"""
Risk Manager für Kalshi Prediction Markets.

Kalshi-Besonderheiten:
- Preise in Cent (1–99), kein Leverage, kein Liquidationsrisiko
- 1 Contract = $1 Notional, Max-Verlust = Kaufpreis
- Exposure = count × price_cents / 100 USD

Dry-Run-Modus:
- Keine API-Abfragen – Positionen werden rein in-memory getracked
- Positionen laufen automatisch ab wenn close_time verstreicht
- Positions-Status wird in data/positions.json gespeichert (für UI)
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from api.client import KalshiClient

logger = logging.getLogger(__name__)

POSITIONS_FILE = Path("data/positions.json")


class RiskManager:
    def __init__(self, client: KalshiClient, config: dict):
        self._client  = client
        self._dry_run = bool(config.get("dry_run", False))
        self._config  = config
        risk = config.get("risk", {})
        self.max_position_usd       = float(risk.get("max_position_usd", 50.0))
        self.max_total_exposure_usd = float(risk.get("max_total_exposure_usd", 200.0))
        self.max_open_positions     = int(risk.get("max_open_positions", 0))  # 0 = kein Limit
        self._positions: dict[str, float] = {}   # ticker → exposure USD
        self._expiry:    dict[str, str]   = {}   # ticker → ISO close_time (dry-run only)
        self._details:   dict[str, dict]  = {}   # ticker → volle Positions-Details
        if self._dry_run:
            self._load_positions_from_file()

    def _load_positions_from_file(self):
        """Stellt Dry-Run-Positionen nach Container-Restart aus positions.json wieder her."""
        try:
            if not POSITIONS_FILE.exists():
                return
            data = json.loads(POSITIONS_FILE.read_text())
            if not data.get("dry_run"):
                return  # Nur Dry-Run-Daten laden
            detail_keys = [
                "side", "price_cents", "count", "rule_name", "title", "event_title",
                "reason", "entered_at", "category", "event_ticker", "sub_title", "image_url",
                "system",
            ]
            loaded = 0
            for p in data.get("positions", []):
                ticker = p.get("ticker", "")
                if not ticker:
                    continue
                self._positions[ticker] = float(p.get("exposure_usd", 0.0))
                if p.get("close_time"):
                    self._expiry[ticker] = p["close_time"]
                self._details[ticker] = {k: p[k] for k in detail_keys if k in p}
                loaded += 1
            if loaded:
                logger.info(f"[Risk/DryRun] {loaded} Positionen aus positions.json wiederhergestellt")
        except Exception as e:
            logger.warning(f"[Risk] Konnte positions.json nicht laden: {e}")

    def refresh_positions(self):
        if self._dry_run:
            # Abgelaufene Märkte ausbuchen (kein API-Call)
            now = datetime.now(timezone.utc)
            expired = [
                t for t, ct_str in self._expiry.items()
                if self._is_expired(ct_str, now)
            ]
            for t in expired:
                exposure = self._positions.pop(t, 0.0)
                self._expiry.pop(t, None)
                self._details.pop(t, None)
                if exposure > 0:
                    logger.info(f"[Risk/DryRun] Markt abgelaufen, Position freigegeben: {t} (${exposure:.2f})")
            self._save_positions()
            return

        try:
            self._positions = {}
            self._details   = {}

            # Gefüllte Positionen
            for p in self._client.get_positions():
                ticker  = p.get("ticker", "")
                mkt_val = abs(float(p.get("market_exposure", 0))) / 100
                if ticker and int(p.get("position", 0)) != 0:
                    self._positions[ticker] = mkt_val

            # Offene (ruhende) Limit-Orders dazuzählen → verhindert Doppel-Orders
            for o in self._client.get_orders(status="resting"):
                ticker    = o.get("ticker", "")
                remaining = int(o.get("remaining_count", o.get("count", 0)))
                yes_price = int(o.get("yes_price", 50))
                side      = o.get("side", "yes")
                price_paid = yes_price if side == "yes" else (100 - yes_price)
                exposure   = remaining * price_paid / 100
                if ticker and remaining > 0:
                    self._positions[ticker] = self._positions.get(ticker, 0.0) + exposure

            self._save_positions()

        except Exception as e:
            logger.warning(f"[Risk] Positions-Refresh fehlgeschlagen: {e}")

    def _is_expired(self, ct_str: str, now: datetime) -> bool:
        try:
            ct = datetime.fromisoformat(ct_str.replace("Z", "+00:00"))
            return ct <= now
        except Exception:
            return False

    def _save_positions(self):
        """Schreibt aktuellen Positions-Status als JSON-Datei für das UI."""
        try:
            POSITIONS_FILE.parent.mkdir(parents=True, exist_ok=True)
            now_iso = datetime.now(timezone.utc).isoformat()
            positions_list = []
            for ticker, exposure_usd in self._positions.items():
                detail = self._details.get(ticker, {})
                positions_list.append({
                    "ticker": ticker,
                    "exposure_usd": round(exposure_usd, 4),
                    "close_time": self._expiry.get(ticker, ""),
                    **detail,
                })
            data = {
                "updated_at": now_iso,
                "dry_run": self._dry_run,
                "positions": positions_list,
                "summary": {
                    "open_positions": len(self._positions),
                    "total_exposure_usd": round(sum(self._positions.values()), 4),
                    "max_exposure_usd": self.max_total_exposure_usd,
                    "max_positions": self.max_open_positions,
                },
            }
            tmp = POSITIONS_FILE.with_suffix(".tmp")
            tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
            tmp.replace(POSITIONS_FILE)
        except Exception as e:
            logger.debug(f"[Risk] positions.json konnte nicht geschrieben werden: {e}")

    def get_total_exposure(self) -> float:
        return sum(self._positions.values())

    def get_open_count(self) -> int:
        return len(self._positions)

    def _system_exposure(self, system: str) -> float:
        """Berechnet die aktuelle Exposure eines Systems."""
        return sum(
            exp for tkr, exp in self._positions.items()
            if self._details.get(tkr, {}).get("system") == system
        )

    def check_order_allowed(self, ticker: str, count: int, price_cents: int,
                            system: str = "") -> tuple[bool, str]:
        exposure_usd = count * price_cents / 100
        current      = self._positions.get(ticker, 0.0)

        # Im Dry-Run: bereits positionierte Märkte nicht nochmals bespielen.
        # Entspricht dem Verhalten einer noch ruhenden Limit-Order im echten Betrieb.
        if self._dry_run and current > 0:
            return False, f"Bereits positioniert (${current:.2f}) – warte auf Settlement"

        if current + exposure_usd > self.max_position_usd:
            return False, (f"Max-Position überschritten: {current:.2f} + "
                           f"{exposure_usd:.2f} > {self.max_position_usd:.2f} USD")

        # System-Budget prüfen (wenn konfiguriert)
        if system:
            sys_cfg     = self._config.get("systems", {}).get(system, {})
            sys_max_usd = float(sys_cfg.get("max_exposure_usd", self.max_total_exposure_usd))
            sys_current = self._system_exposure(system)
            if sys_current + exposure_usd > sys_max_usd:
                return False, (f"System-Budget '{system}' überschritten: "
                               f"${sys_current:.2f} + ${exposure_usd:.2f} > ${sys_max_usd:.2f}")

        total = self.get_total_exposure()
        if total + exposure_usd > self.max_total_exposure_usd:
            return False, (f"Max-Exposure überschritten: {total:.2f} + "
                           f"{exposure_usd:.2f} > {self.max_total_exposure_usd:.2f} USD")

        if (self.max_open_positions > 0 and ticker not in self._positions
                and self.get_open_count() >= self.max_open_positions):
            return False, (f"Max. offene Positionen erreicht: "
                           f"{self.get_open_count()} >= {self.max_open_positions}")

        return True, ""

    def record_order(self, ticker: str, count: int, price_cents: int, is_buy: bool,
                     close_time: str = "", *, side: str = "", rule_name: str = "",
                     title: str = "", event_title: str = "", reason: str = "", entered_at: str = "",
                     category: str = "", event_ticker: str = "", sub_title: str = "",
                     image_url: str = "", system: str = ""):
        exposure = count * price_cents / 100
        if is_buy:
            self._positions[ticker] = self._positions.get(ticker, 0.0) + exposure
            if self._dry_run and close_time and ticker not in self._expiry:
                self._expiry[ticker] = close_time
            if ticker not in self._details:
                self._details[ticker] = {
                    "side": side,
                    "price_cents": price_cents,
                    "count": count,
                    "rule_name": rule_name,
                    "title": title,
                    "event_title": event_title,
                    "reason": reason,
                    "entered_at": entered_at or datetime.now(timezone.utc).isoformat(),
                    "category": category,
                    "event_ticker": event_ticker,
                    "sub_title": sub_title,
                    "image_url": image_url,
                    "system": system,
                }
        else:
            # Exit = Position vollständig schließen, unabhängig vom Sell-Preis.
            # (Sell-Preis < Entry-Preis bei Stop-Loss würde sonst fälschlich
            # eine Teil-Exposure übrig lassen und den Exit neu triggern.)
            self._positions.pop(ticker, None)
            self._expiry.pop(ticker, None)
            self._details.pop(ticker, None)
        self._save_positions()

    def update_detail(self, ticker: str, **kwargs):
        """Füllt fehlende Felder in _details nach (z.B. event_title für Altpositionen)."""
        if ticker not in self._details:
            return
        changed = False
        for key, val in kwargs.items():
            if val and not self._details[ticker].get(key):
                self._details[ticker][key] = val
                changed = True
        if changed:
            self._save_positions()

    def get_summary(self) -> dict:
        return {
            "open_positions": self.get_open_count(),
            "total_exposure_usd": round(self.get_total_exposure(), 2),
            "positions": dict(self._positions),
        }
