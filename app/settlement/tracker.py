"""
Settlement Tracker für Kalshi Prediction Markets.

Ablauf nach jedem Scan-Zyklus:
  1. Liest alle TRADE-Einträge ohne passendes SETTLEMENT
  2. Prüft ob close_time + Delay verstrichen ist
  3. Ruft GET /markets/{ticker} für das Ergebnis ab (result: "yes" | "no")
  4. Berechnet P&L inkl. Gebühren (fee_pct% des Einsatzes bei Gewinn)
  5. Schreibt SETTLEMENT-Eintrag in trades.jsonl
  6. Aktualisiert balance.json

Gebühren-Modell (Kalshi-Standard):
  fee = invested_usd × fee_pct / 100  (nur bei gewonnenem Trade)
  Kalshi berechnet ~1% des Einsatzes auf der Gewinnerseite.
"""

import asyncio
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from api.client import KalshiClient
from logger.trade_logger import TradeLogger

logger = logging.getLogger(__name__)

BALANCE_FILE = Path("data/balance.json")


class SettlementTracker:
    def __init__(self, client: KalshiClient, trade_logger: TradeLogger, config: dict):
        self._client = client
        self._logger = trade_logger
        s_cfg = config.get("settlement", {})
        self._fee_pct   = float(s_cfg.get("fee_pct", 1.0))
        self._delay_s   = int(s_cfg.get("delay_seconds", 300))   # 5 Min nach Marktschluss
        self._start_bal = float(s_cfg.get("starting_balance_usd", 200.0))
        self._balance   = self._load_or_init_balance()
        self._settled   = self._load_settled_tickers()

    # ── Balance ────────────────────────────────────────────────────────── #

    def _load_or_init_balance(self) -> dict:
        if BALANCE_FILE.exists():
            try:
                return json.loads(BALANCE_FILE.read_text())
            except Exception:
                pass
        bal = {
            "starting_usd":       self._start_bal,
            "current_usd":        self._start_bal,
            "total_invested_usd": 0.0,
            "total_returned_usd": 0.0,
            "total_fees_usd":     0.0,
            "total_pnl_usd":      0.0,
            "trades_settled":     0,
            "wins":               0,
            "losses":             0,
            "updated_at":         datetime.now(timezone.utc).isoformat(),
        }
        self._write_balance(bal)
        return bal

    def _write_balance(self, bal: dict):
        BALANCE_FILE.parent.mkdir(parents=True, exist_ok=True)
        bal["updated_at"] = datetime.now(timezone.utc).isoformat()
        BALANCE_FILE.write_text(json.dumps(bal, ensure_ascii=False, indent=2))

    def get_balance(self) -> dict:
        return dict(self._balance)

    # ── Trade-Erkennung ────────────────────────────────────────────────── #

    def _load_settled_tickers(self) -> set:
        return {e.get("ticker", "") for e in self._logger.read_all("SETTLEMENT")}

    def _exited_tickers(self) -> set:
        """Tickers die manuell per Exit-Order (Stop-Loss / Take-Profit) geschlossen wurden."""
        return {
            e.get("ticker", "")
            for e in self._logger.read_all("TRADE")
            if e.get("status") == "dry_run_exit"
        }

    def _pending_trades(self, now: datetime) -> list[dict]:
        """
        Entry-Trades die noch kein SETTLEMENT haben, nicht exited wurden
        und deren close_time + Delay abgelaufen ist.
        """
        exited = self._exited_tickers()
        # Alle Entry-Buys: eine Zeile pro Ticker reicht (kein Averaging in Dry-Run)
        seen: dict[str, dict] = {}
        for e in self._logger.read_all("TRADE"):
            if e.get("status") not in ("dry_run", "resting", "filled", "partially_filled"):
                continue
            ticker = e.get("ticker", "")
            if not ticker or ticker in self._settled or ticker in exited:
                continue
            ct_str = e.get("close_time", "")
            if not ct_str:
                continue   # Kein close_time → kann nicht geprüft werden
            if ticker not in seen:
                seen[ticker] = {**e, "_count_total": 0}
            seen[ticker]["_count_total"] += int(e.get("count", 0))

        result = []
        for ticker, t in seen.items():
            try:
                ct = datetime.fromisoformat(t["close_time"].replace("Z", "+00:00"))
            except Exception:
                continue
            if (now - ct).total_seconds() < self._delay_s:
                continue   # Noch zu früh – Kalshi braucht Zeit zum Settleln
            t["count"] = t["_count_total"]
            result.append(t)
        return result

    # ── Haupt-Check ────────────────────────────────────────────────────── #

    async def check(self):
        """Im Scan-Zyklus aufrufen – prüft Exits und abgelaufene Märkte."""
        now = datetime.now(timezone.utc)
        # 1. Exit-P&L verbuchen (Take-Profit / Stop-Loss die bereits ausgeführt wurden)
        self._process_exits()
        # 2. Abgelaufene Märkte via API abrechnen
        pending = self._pending_trades(now)
        if not pending:
            return
        logger.info(f"[Settlement] {len(pending)} abgelaufene Position(en) werden geprüft")
        loop = asyncio.get_event_loop()
        for trade in pending:
            ticker = trade.get("ticker", "")
            try:
                market = await loop.run_in_executor(
                    None, lambda t=ticker: self._client.get_market(t)
                )
                result = market.get("result")
                if result not in ("yes", "no"):
                    logger.debug(f"[Settlement] {ticker} noch nicht settled (result={result!r})")
                    continue
                self._settle(trade, result)
            except Exception as e:
                logger.warning(f"[Settlement] {ticker} API-Fehler: {e}")

    def _process_exits(self):
        """
        Verbucht P&L für Positionen die aktiv geschlossen wurden (Exit-Trades).
        Entry-Preis vs. Exit-Preis → realisierter P&L, keine Gebühr (kein Settlement).
        """
        all_trades = self._logger.read_all("TRADE")
        entries = {
            t["ticker"]: t for t in all_trades
            if t.get("status") in ("dry_run", "resting", "filled", "partially_filled")
        }
        for ex in all_trades:
            if ex.get("status") != "dry_run_exit":
                continue
            ticker = ex.get("ticker", "")
            if ticker in self._settled:
                continue
            entry = entries.get(ticker)
            if not entry:
                continue
            entry_px  = int(entry.get("price_cents", 0))
            entry_cnt = int(entry.get("count", 0))
            exit_px   = int(ex.get("price_cents", 0))
            exit_cnt  = int(ex.get("count", 0))
            if not entry_px or not entry_cnt:
                continue
            invested_usd   = round(entry_cnt * entry_px / 100, 4)
            received_usd   = round(exit_cnt  * exit_px  / 100, 4)
            pnl_usd        = round(received_usd - invested_usd, 4)
            won            = pnl_usd > 0

            bal = self._balance
            bal_before = round(bal["current_usd"], 4)
            bal["current_usd"]        = round(bal["current_usd"]        + pnl_usd,       4)
            bal["total_invested_usd"] = round(bal["total_invested_usd"] + invested_usd,  4)
            bal["total_returned_usd"] = round(bal["total_returned_usd"] + received_usd,  4)
            bal["total_pnl_usd"]      = round(bal["total_pnl_usd"]      + pnl_usd,        4)
            bal["trades_settled"]     += 1
            bal["wins" if won else "losses"] += 1
            self._write_balance(bal)

            self._logger.log_settlement(
                ticker=ticker, side=entry.get("side", ""),
                price_cents=entry_px, count=entry_cnt,
                result="exit", won=won,
                invested_usd=invested_usd,
                gross_return_usd=received_usd,
                fee_usd=0.0,
                net_return_usd=received_usd,
                pnl_usd=pnl_usd,
                rule=entry.get("rule", ""),
                reason=ex.get("reason", ex.get("rule", "")),
                title=entry.get("title", ""),
                close_time=entry.get("close_time", ""),
                balance_before_usd=bal_before,
                balance_after_usd=bal["current_usd"],
            )
            self._settled.add(ticker)

    # ── Abrechnung ─────────────────────────────────────────────────────── #

    def _settle(self, trade: dict, result: str):
        ticker      = trade["ticker"]
        side        = trade.get("side", "yes")
        price_cents = int(trade.get("price_cents", 50))
        count       = int(trade.get("count", 0))
        rule        = trade.get("rule", "")
        reason      = trade.get("reason", "")
        title       = trade.get("title", "")
        close_time  = trade.get("close_time", "")

        # Kernrechnung
        invested_usd     = round(count * price_cents / 100, 4)
        won              = (result == side)
        gross_return_usd = round(count * 1.0, 4) if won else 0.0
        # Gebühr: fee_pct% des Einsatzes bei Gewinn (Kalshi Standard ~1%)
        fee_usd          = round(invested_usd * self._fee_pct / 100, 4) if won else 0.0
        net_return_usd   = round(gross_return_usd - fee_usd, 4)
        pnl_usd          = round(net_return_usd - invested_usd, 4)

        # Balance aktualisieren
        bal = self._balance
        bal_before = round(bal["current_usd"], 4)
        bal["current_usd"]        = round(bal["current_usd"]        + pnl_usd,          4)
        bal["total_invested_usd"] = round(bal["total_invested_usd"] + invested_usd,      4)
        bal["total_returned_usd"] = round(bal["total_returned_usd"] + net_return_usd,    4)
        bal["total_fees_usd"]     = round(bal["total_fees_usd"]     + fee_usd,           4)
        bal["total_pnl_usd"]      = round(bal["total_pnl_usd"]      + pnl_usd,           4)
        bal["trades_settled"]     += 1
        bal["wins" if won else "losses"] += 1
        self._write_balance(bal)

        self._logger.log_settlement(
            ticker=ticker, side=side, price_cents=price_cents, count=count,
            result=result, won=won,
            invested_usd=invested_usd, gross_return_usd=gross_return_usd,
            fee_usd=fee_usd, net_return_usd=net_return_usd, pnl_usd=pnl_usd,
            rule=rule, reason=reason, title=title, close_time=close_time,
            balance_before_usd=bal_before, balance_after_usd=bal["current_usd"],
        )
        self._settled.add(ticker)
