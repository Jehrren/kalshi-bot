"""
Prediction Market Scanner (System 1).

Scannt politische, wirtschaftliche und allgemeine Märkte auf Kalshi.
Hat eigene Entry- und Exit-Logik – komplett getrennt vom Crypto-System.

Exit-Bedingungen:
  1. Take-Profit : Bid ≥ 1.8× Einstieg (Prediction Markets sind träger)
  2. Zeit-Stop   : < 30 Min, Position hat > 60% des Einsatzes verloren
  3. (Kein Spot-Stop-Loss – keine Crypto-Preisbezüge)
"""

import asyncio
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Optional

from api.client import KalshiClient
from logger.trade_logger import TradeLogger
from prediction.rules import PredictionRuleEngine, PredictionSignal
from trader.executor import Signal

logger = logging.getLogger(__name__)

SYSTEM = "prediction"


def _to_executor_signal(ps: PredictionSignal) -> Signal:
    """Konvertiert PredictionSignal in das shared Signal-Format des Executors."""
    return Signal(
        ticker      = ps.ticker,
        rule_name   = ps.rule_name,
        side        = ps.side,
        action      = ps.action,
        price_cents = ps.price_cents,
        count       = ps.count,
        reason      = ps.reason,
        meta        = {**ps.meta, "system": SYSTEM},
        track       = ps.track,
    )


class PredictionScanner:
    def __init__(
        self,
        client:       KalshiClient,
        trade_logger: TradeLogger,
        config:       dict,
        on_signal:    Callable,
        on_meta:      Optional[Callable] = None,
        on_cycle_end: Optional[Callable] = None,
    ):
        self._client       = client
        self._logger       = trade_logger
        self._config       = config
        self._on_signal    = on_signal
        self._on_meta      = on_meta
        self._on_cycle_end = on_cycle_end

        scan_cfg             = config.get("prediction_scanner", {})
        self._interval_s     = int(scan_cfg.get("interval_seconds", 60))
        self._max_close_days = scan_cfg.get("max_close_days", 30)
        self._min_volume     = float(scan_cfg.get("min_volume_usd", 0))
        self._categories     = set(c.lower() for c in scan_cfg.get("categories", []))

        sys_cfg          = config.get("systems", {}).get(SYSTEM, {})
        self._bankroll   = float(sys_cfg.get("max_exposure_usd", 80.0))

        self._rules      = PredictionRuleEngine(config)
        self._stop_event = asyncio.Event()
        self._exit_pending: set[str] = set()

        logger.info(
            f"[Prediction/Scanner] Gestartet | Intervall: {self._interval_s}s | "
            f"Budget: ${self._bankroll:.0f}"
        )

    async def start(self):
        while not self._stop_event.is_set():
            t0 = time.monotonic()
            try:
                await self._scan_cycle()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception(f"[Prediction/Scanner] Fehler: {e}")
                self._logger.log_error("PredictionScanner", str(e))
            elapsed = time.monotonic() - t0
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(),
                    timeout=max(0.0, self._interval_s - elapsed),
                )
            except asyncio.TimeoutError:
                pass

    async def stop(self):
        self._stop_event.set()

    # ── Scan-Zyklus ──────────────────────────────────────────────────── #

    async def _scan_cycle(self):
        loop = asyncio.get_running_loop()
        signals_total = 0

        signals_total += await self._scan_entries(loop)
        signals_total += await self._scan_exits(loop)

        if signals_total:
            logger.info(f"[Prediction/Scanner] {signals_total} Signal(e)")
        else:
            logger.debug("[Prediction/Scanner] Keine Signale")

        if self._on_cycle_end:
            await self._on_cycle_end()

    # ── Entry-Scan ───────────────────────────────────────────────────── #

    async def _scan_entries(self, loop) -> int:
        events = await loop.run_in_executor(
            None,
            lambda: self._client.get_all_events(status="open", with_nested_markets=True),
        )
        allowed = [e for e in events if (e.get("category") or "").lower() in self._categories]

        markets: list[dict] = []
        now_utc  = datetime.now(timezone.utc)
        max_secs = (self._max_close_days or 30) * 86400

        for event in allowed:
            series_ticker = (event.get("series_ticker") or "").upper()
            image_url     = (event.get("image_url") or "").strip()
            event_title   = (event.get("title") or "").strip()
            if not image_url and series_ticker:
                image_url = (
                    f"https://kalshi-public-docs.s3.amazonaws.com/"
                    f"series-images-webp/{series_ticker}.webp"
                )
            for m in event.get("markets", []):
                if m.get("status", "active") not in ("active", "open"):
                    continue
                ct_str = m.get("close_time", "")
                if ct_str:
                    try:
                        ct = datetime.fromisoformat(ct_str.replace("Z", "+00:00"))
                        if (ct - now_utc).total_seconds() > max_secs:
                            continue
                    except Exception:
                        pass
                if self._min_volume > 0:
                    if float(m.get("volume_24h_fp", 0) or 0) < self._min_volume:
                        continue
                m["category"]    = (event.get("category") or "").lower()
                m["sub_title"]   = (event.get("sub_title") or "").strip()
                m["image_url"]   = image_url
                m["event_title"] = event_title
                markets.append(m)

        logger.info(f"[Prediction/Scanner] {len(markets)} Märkte gescannt")

        signals = 0
        for market in markets:
            if self._on_meta:
                self._on_meta(
                    market.get("ticker", ""),
                    event_title=market.get("event_title", ""),
                    image_url=market.get("image_url", ""),
                    sub_title=market.get("sub_title", ""),
                )
            for ps in self._rules.evaluate(market, bankroll_usd=self._bankroll):
                await self._on_signal(_to_executor_signal(ps))
                signals += 1
        return signals

    # ── Exit-Scan ────────────────────────────────────────────────────── #

    async def _scan_exits(self, loop) -> int:
        """
        Prediction-Market Exit-Logik:
          1. Take-Profit : Bid ≥ 1.8× Einstieg
          2. Zeit-Stop   : < 30 Min + mehr als 60% des Einsatzes verloren
        """
        try:
            data      = json.loads(Path("data/positions.json").read_text())
            positions = [p for p in data.get("positions", []) if p.get("system") == SYSTEM]
        except Exception:
            return 0

        current_tickers = {p.get("ticker", "") for p in positions}
        self._exit_pending = {t for t in self._exit_pending if t in current_tickers}

        now   = datetime.now(timezone.utc)
        exits = 0

        for pos in positions:
            ticker    = pos.get("ticker", "")
            side      = str(pos.get("side", "")).lower()
            entry_px  = int(pos.get("price_cents", 0))
            count     = int(pos.get("count", 0))
            close_str = pos.get("close_time", "")

            if not ticker or not entry_px or not count:
                continue
            if ticker in self._exit_pending:
                continue

            mins_left = float("inf")
            try:
                ct        = datetime.fromisoformat(close_str.replace("Z", "+00:00"))
                mins_left = max(0.0, (ct - now).total_seconds() / 60)
            except Exception:
                pass

            if mins_left < 0:
                continue

            try:
                market = await loop.run_in_executor(
                    None, lambda t=ticker: self._client.get_market(t)
                )
            except Exception:
                continue

            if not market:
                continue

            def _pc(key: str) -> Optional[int]:
                v = market.get(key + "_dollars") or market.get(key)
                if v is None:
                    return None
                f = float(v)
                return int(round(f * 100)) if f <= 1.0 else int(round(f))

            yes_bid = _pc("yes_bid")
            no_bid  = _pc("no_bid")

            exit_reason: Optional[str] = None
            sell_price: int = max(1, entry_px - 1)

            if side == "no":
                # Take-Profit: NO Bid ≥ 1.8× Einstieg
                tp_target = int(entry_px * 1.8)
                if no_bid and no_bid >= tp_target:
                    exit_reason = f"Take-Profit: NO bid {no_bid}¢ ≥ 1.8× {entry_px}¢"
                    sell_price  = max(1, no_bid - 1)
                # Zeit-Stop: < 30 Min + mehr als 60% Verlust
                elif mins_left < 30 and no_bid and no_bid < int(entry_px * 0.4):
                    exit_reason = (
                        f"Zeit-Stop: {mins_left:.0f}min verbl. | "
                        f"NO bid {no_bid}¢ < 40% von Einstieg {entry_px}¢"
                    )
                    sell_price = max(1, no_bid or 1)

            elif side == "yes":
                # Take-Profit: YES Bid ≥ 1.8× Einstieg (cap 95¢)
                tp_target = min(95, int(entry_px * 1.8))
                if yes_bid and yes_bid >= tp_target:
                    exit_reason = f"Take-Profit: YES bid {yes_bid}¢ ≥ {tp_target}¢"
                    sell_price  = max(1, yes_bid - 1)
                # Zeit-Stop: < 30 Min + mehr als 60% Verlust
                elif mins_left < 30 and yes_bid and yes_bid < int(entry_px * 0.4):
                    exit_reason = (
                        f"Zeit-Stop: {mins_left:.0f}min verbl. | "
                        f"YES bid {yes_bid}¢ < 40% von Einstieg {entry_px}¢"
                    )
                    sell_price = max(1, yes_bid or 1)

            if not exit_reason:
                continue

            logger.info(
                f"[Prediction/Exit] {ticker} EXIT {side.upper()} ×{count} "
                f"@ {sell_price}¢ | {exit_reason}"
            )
            self._exit_pending.add(ticker)
            await self._on_signal(Signal(
                ticker      = ticker,
                rule_name   = f"Exit: {exit_reason[:60]}",
                side        = side,
                action      = "sell",
                price_cents = sell_price,
                count       = count,
                reason      = exit_reason,
                meta        = {
                    "close_time":        close_str,
                    "exit":              True,
                    "entry_price_cents": entry_px,
                    "system":            SYSTEM,
                },
            ))
            exits += 1

        return exits
