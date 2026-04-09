"""Trade Executor für Kalshi Prediction Markets."""

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone

from api.client import KalshiClient
from logger.trade_logger import TradeLogger
from risk.manager import RiskManager


@dataclass
class Signal:
    ticker:      str
    rule_name:   str
    side:        str        # "yes" | "no"
    action:      str        # "buy" | "sell"
    price_cents: int        # Limit-Preis in Cent
    count:       int        # Anzahl Contracts
    reason:      str
    meta:        dict = field(default_factory=dict)
    track:       str = ""   # Scanner-Track (crypto_15min, crypto, arb, prediction)

logger = logging.getLogger(__name__)

# Prioritäts-Score pro Scanner-Track
_TRACK_SCORES: dict[str, float] = {
    "crypto_15min": 1.00,  # Mean-Reversion, 10–15 Min Restlaufzeit
    "crypto":       0.80,  # BTC/ETH Tages-Leiter
    "arb":          0.75,  # Leiter-Arbitrage (echte Preisumkehr)
    "pred_zone":    0.72,  # Prediction-Ladder Zone-Bet (garantierter Edge ohne vol_ratio)
    "zone":         0.70,  # Zone/Spread-Bet (Niedrig-Vol Edge)
    "weather":      0.65,  # Wetter-Märkte (Ensemble-Edge)
    "politisch":    0.50,  # Politische/wirtschaftliche Märkte
}


class TradeExecutor:
    def __init__(self, client: KalshiClient, risk: RiskManager,
                 trade_logger: TradeLogger, config: dict):
        self._client   = client
        self._risk     = risk
        self._logger   = trade_logger
        self._dry_run  = bool(config.get("dry_run", False))
        self._queue: asyncio.Queue[Signal] = asyncio.Queue()
        self._stop_event = asyncio.Event()
        self._pending: list[Signal] = []   # Buy-Signal-Puffer pro Scan-Zyklus

        p_cfg = config.get("priority", {})
        self._min_score   = float(p_cfg.get("min_score", 0.30))
        self._max_signals = int(p_cfg.get("max_signals_per_cycle", 10))

        # Lock: macht check_order_allowed + record_order atomar (Dry-Run-Pfad).
        # Signale werden sequenziell über die Queue verarbeitet (single-consumer).
        # Lock schützt nur gegen theoretische Interleaving-Szenarien bei zukünftiger
        # Parallelisierung. Im Live-Pfad wird Lock vor API-Call freigegeben.
        self._order_lock = asyncio.Lock()
        if self._dry_run:
            logger.info("[Executor] *** DRY-RUN MODUS – keine echten Orders ***")

    async def handle_signal(self, signal: Signal):
        if signal.action == "sell":
            # Exit-Signale sofort in Queue (keine Verzögerung)
            await self._queue.put(signal)
        else:
            # Buy-Signale puffern – am Zyklusende priorisiert verarbeiten
            self._pending.append(signal)

    async def handle_cycle_end(self):
        """
        Wird vom Scanner am Ende jedes Scan-Zyklus aufgerufen.
        Sortiert gesammelte Buy-Signale nach Priorität und gibt die besten
        in die Ausführungs-Queue – beste Trades zuerst, bis Kapital erschöpft.
        """
        pending = self._pending
        self._pending = []

        if not pending:
            return

        # Schneller Kapital-Check: wenn Portfolio voll, nichts in Queue einreihen
        available_usd = self._risk.max_total_exposure_usd - self._risk.get_total_exposure()
        at_pos_limit  = (
            self._risk.max_open_positions > 0
            and self._risk.get_open_count() >= self._risk.max_open_positions
        )
        if available_usd < 0.10 or at_pos_limit:
            reason = (
                f"Max. Positionen ({self._risk.max_open_positions}) erreicht"
                if at_pos_limit
                else f"Kapital erschöpft (verfügbar: ${available_usd:.2f})"
            )
            logger.debug(f"[Executor] Zyklus übersprungen – {reason}")
            return

        scored = [(self._compute_priority(s), s) for s in pending]

        # Min-Score-Filter + absteigend sortieren (bester zuerst)
        scored = [(sc, s) for sc, s in scored if sc >= self._min_score]
        scored.sort(key=lambda x: x[0], reverse=True)

        taken   = scored[:self._max_signals]
        skipped = len(scored) - len(taken)

        if scored:
            logger.info(
                f"[Executor] Zyklus-Ende: {len(scored)} qualifizierte Signal(e) | "
                f"top {len(taken)} werden verarbeitet | verfügbar: ${available_usd:.2f}"
                + (f" | {skipped} übersprungen (max_signals_per_cycle)" if skipped else "")
            )

        for score, signal in taken:
            logger.debug(
                f"[Executor] Prio={score:.3f} | {signal.ticker} "
                f"[{signal.track or '?'}] @ {signal.price_cents}¢ | {signal.rule_name}"
            )
            await self._queue.put(signal)

    def _compute_priority(self, signal: Signal) -> float:
        """
        Gewichteter Prioritäts-Score für Buy-Signale (0.0 – 1.0).

        Gewichtung (abgestimmt auf kurzfristige Präferenz):
          35% time_score  – Bevorzugt Märkte die bald ablaufen
          30% track_score – crypto_15min > crypto > arb > politisch
          20% spread_score – Enger Spread = hohe Liquidität = besserer Fill
          15% edge_score  – Sweet-Spot 55–82¢, Longshots bekommen Malus
        """
        # ─── 35% Time Score ─────────────────────────────────────────────────
        time_score = 0.50
        close_time_str = signal.meta.get("close_time", "")
        if close_time_str:
            try:
                ct = datetime.fromisoformat(close_time_str.replace("Z", "+00:00"))
                hours_left = max(0.0, (ct - datetime.now(timezone.utc)).total_seconds() / 3600)
                if hours_left <= 2:
                    time_score = 1.00
                elif hours_left <= 24:
                    time_score = 0.85
                elif hours_left <= 72:
                    time_score = 0.60
                elif hours_left <= 168:   # 7 Tage
                    time_score = 0.35
                else:                     # > 7 Tage (z.B. Time Decay – 14d+)
                    time_score = 0.15
            except Exception:
                pass

        # ─── 30% Track Score ────────────────────────────────────────────────
        track_score = _TRACK_SCORES.get(signal.track, 0.50)

        # ─── 20% Spread Score ───────────────────────────────────────────────
        spread = signal.meta.get("spread")
        if spread is None:
            yes_ask = signal.meta.get("yes_ask") or 0
            yes_bid = signal.meta.get("yes_bid") or 0
            spread = (yes_ask - yes_bid) if yes_ask and yes_bid else 10
        # spread 1¢ = optimal (1.0), 11¢+ = schlecht (0.0)
        spread_score = max(0.0, min(1.0, 1.0 - (float(spread) - 1) / 10))

        # ─── 15% Edge Score ─────────────────────────────────────────────────
        p = signal.price_cents
        if 55 <= p <= 82:
            edge_score = 1.0    # Sweet-Spot: beste Kalibrierung
        elif 83 <= p <= 92:
            edge_score = 0.8    # Noch guter Edge
        elif 93 <= p <= 99:
            edge_score = 0.6    # Hoher Preis, geringes Aufwärtspotential
        elif 35 <= p <= 54:
            edge_score = 0.7    # Unter 50¢, aber nicht Longshot
        elif 15 <= p <= 34:
            edge_score = 0.5    # Longshot-Bereich
        else:
            edge_score = 0.2    # <15¢ – Lotterie-Ticket

        return round(
            0.35 * time_score +
            0.30 * track_score +
            0.20 * spread_score +
            0.15 * edge_score,
            4,
        )

    async def start(self):
        logger.info("[Executor] Gestartet")
        while not self._stop_event.is_set():
            try:
                signal = await asyncio.wait_for(self._queue.get(), timeout=1.0)
                try:
                    await self._process(signal)
                finally:
                    self._queue.task_done()
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception(f"[Executor] Fehler: {e}")
                self._logger.log_error("Executor", str(e))
        logger.info("[Executor] Gestoppt")

    async def stop(self):
        self._stop_event.set()

    async def _process(self, signal: Signal):
        is_buy = signal.action == "buy"

        # Exit-Signale (sell) direkt weiterleiten – keine Risk-Checks
        if not is_buy:
            await self._execute_exit(signal)
            return

        system = signal.meta.get("system", "")

        # Lock: atomare Sequenz aus check → (record | order+record)
        async with self._order_lock:
            self._risk.refresh_positions()
            allowed, reason = self._risk.check_order_allowed(
                signal.ticker, signal.count, signal.price_cents,
                system=system,
                event_ticker=signal.meta.get("event_ticker", ""),
            )
            if not allowed:
                logger.debug(f"[Executor] Abgelehnt | {signal.ticker}: {reason}")
                _expected = ("Bereits positioniert", "Bereits abgerechnet",
                             "Max-Position", "Max-Exposure",
                             "System-Budget", "System-Limit", "Event-Ticker Duplikat",
                             "Max. offene Positionen")
                if not any(reason.startswith(p) for p in _expected):
                    self._logger.log_error("RiskBlock", reason, extra={"ticker": signal.ticker})
                return

            logger.info(
                f"[Executor] {'[DRY] ' if self._dry_run else ''}Order | "
                f"[{system or '?'}] {signal.ticker} {signal.action} {signal.side} "
                f"{signal.count}x @ {signal.price_cents}¢ | {signal.rule_name}"
            )

            if self._dry_run:
                self._logger.log_trade(
                    ticker=signal.ticker,
                    side=signal.side,
                    price_cents=signal.price_cents,
                    count=signal.count,
                    order_id="DRY_RUN",
                    status="dry_run",
                    rule_name=signal.rule_name,
                    extra={
                        "reason":     signal.reason,
                        "dry_run":    True,
                        "close_time": signal.meta.get("close_time", ""),
                        "title":      signal.meta.get("title", ""),
                        "system":     system,
                    },
                )
                # Simulierte Position verbuchen – verhindert Duplikate in Folge-Zyklen
                self._risk.record_order(
                    signal.ticker, signal.count, signal.price_cents, True,
                    close_time  = signal.meta.get("close_time", ""),
                    side        = signal.side,
                    rule_name   = signal.rule_name,
                    title       = signal.meta.get("title", ""),
                    event_title = signal.meta.get("event_title", ""),
                    reason      = signal.reason,
                    category    = signal.meta.get("category", ""),
                    event_ticker= signal.meta.get("event_ticker", ""),
                    sub_title   = signal.meta.get("sub_title", ""),
                    image_url   = signal.meta.get("image_url", ""),
                    system      = system,
                )
                summary = self._risk.get_summary()
                logger.info(
                    f"[Executor/DryRun] Positionen: {summary['open_positions']} | "
                    f"Exposure: ${summary['total_exposure_usd']:.2f} / "
                    f"${self._risk.max_total_exposure_usd:.0f}"
                )
                return

        # Live-Pfad: Lock vor API-Call freigeben (kann Sekunden dauern)
        loop = asyncio.get_running_loop()
        try:
            result = await loop.run_in_executor(
                None,
                lambda: self._client.place_order(
                    ticker=signal.ticker,
                    side=signal.side,
                    order_type="limit",
                    count=signal.count,
                    limit_price=signal.price_cents,
                    action="buy",
                ),
            )
        except Exception as e:
            logger.error(f"[Executor] Order fehlgeschlagen {signal.ticker}: {e}")
            self._logger.log_error("OrderFailed", str(e), extra={"ticker": signal.ticker})
            return

        order    = result.get("order", {})
        order_id = order.get("id")
        status   = order.get("status", "unknown")

        async with self._order_lock:
            if status in ("resting", "filled", "partially_filled"):
                self._risk.record_order(
                    signal.ticker, signal.count, signal.price_cents, True,
                    close_time  = signal.meta.get("close_time", ""),
                    side        = signal.side,
                    rule_name   = signal.rule_name,
                    title       = signal.meta.get("title", ""),
                    event_title = signal.meta.get("event_title", ""),
                    reason      = signal.reason,
                    category    = signal.meta.get("category", ""),
                    event_ticker= signal.meta.get("event_ticker", ""),
                    sub_title   = signal.meta.get("sub_title", ""),
                    image_url   = signal.meta.get("image_url", ""),
                    system      = system,
                )

        self._logger.log_trade(
            ticker=signal.ticker,
            side=signal.side,
            price_cents=signal.price_cents,
            count=signal.count,
            order_id=order_id,
            status=status,
            rule_name=signal.rule_name,
            extra={"reason": signal.reason},
        )

    async def _execute_exit(self, signal: Signal):
        """Schließt eine Position (sell-Signal vom Exit-Scanner)."""
        prefix = "[DRY] " if self._dry_run else ""
        logger.info(
            f"[Executor] {prefix}EXIT | "
            f"{signal.ticker} sell {signal.side} "
            f"{signal.count}x @ {signal.price_cents}¢ | {signal.rule_name}"
        )

        if self._dry_run:
            # Duplikat-Guard: bereits exitete Tickers nicht nochmals loggen
            already_exited = {
                e.get("ticker", "")
                for e in self._logger.read_all("TRADE")
                if e.get("status") == "dry_run_exit"
            }
            if signal.ticker in already_exited:
                logger.debug(f"[Executor/DryRun] EXIT-Duplikat ignoriert: {signal.ticker}")
                return
            self._logger.log_trade(
                ticker=signal.ticker,
                side=signal.side,
                price_cents=signal.price_cents,
                count=signal.count,
                order_id="DRY_EXIT",
                status="dry_run_exit",
                rule_name=signal.rule_name,
                extra={
                    "reason": signal.reason,
                    "dry_run": True,
                    "exit": True,
                    "entry_price_cents": signal.meta.get("entry_price_cents", 0),
                    "close_time": signal.meta.get("close_time", ""),
                },
            )
            # Position aus Risk-Manager entfernen
            self._risk.record_order(
                signal.ticker, signal.count, signal.price_cents, False  # is_buy=False
            )
            summary = self._risk.get_summary()
            logger.info(
                f"[Executor/DryRun] Position geschlossen | "
                f"Positionen: {summary['open_positions']} | "
                f"Exposure: ${summary['total_exposure_usd']:.2f}"
            )
            return

        loop = asyncio.get_running_loop()
        try:
            result = await loop.run_in_executor(
                None,
                lambda: self._client.place_order(
                    ticker=signal.ticker,
                    side=signal.side,
                    order_type="limit",
                    count=signal.count,
                    limit_price=signal.price_cents,
                    action="sell",
                ),
            )
        except Exception as e:
            logger.error(f"[Executor] Exit-Order fehlgeschlagen {signal.ticker}: {e}")
            self._logger.log_error("ExitFailed", str(e), extra={"ticker": signal.ticker})
            return

        order    = result.get("order", {})
        order_id = order.get("id")
        status   = order.get("status", "unknown")

        if status in ("resting", "filled", "partially_filled"):
            self._risk.record_order(signal.ticker, signal.count, signal.price_cents, False)

        self._logger.log_trade(
            ticker=signal.ticker,
            side=signal.side,
            price_cents=signal.price_cents,
            count=signal.count,
            order_id=order_id,
            status=status,
            rule_name=signal.rule_name,
            extra={"reason": signal.reason, "exit": True},
        )

