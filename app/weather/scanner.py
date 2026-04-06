"""
Weather Market Scanner (System 3).

Scannt Temperatur-Märkte auf Kalshi (Daily High / Daily Low).
Nutzt Open-Meteo GFS 31-Member Ensemble als externe Datenquelle.

Edge-Quellen:
  1. Ensemble-Probability vs. Marktpreis (Kern-Edge)
  2. Station-Bias-Korrektur (NWS ≠ GFS-Gitterpunkt)
  3. Modell-Update-Timing (GFS alle 6h)

Exit-Bedingungen:
  1. Take-Profit : Bid ≥ 2.0× Einstieg
  2. Zeit-Stop   : < 30 Min, Position hat > 60% des Einsatzes verloren
"""

import asyncio
import json
import logging
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Optional

from api.client import KalshiClient
from feeds.weather_feed import (
    WeatherFeed, WEATHER_SERIES_MAP, series_to_city, series_to_type,
)
from logger.trade_logger import TradeLogger
from trader.executor import Signal
from weather.rules import WeatherRuleEngine, WeatherSignal

logger = logging.getLogger(__name__)

SYSTEM = "weather"

# Serien-Prefixes für Weather-Märkte
_WEATHER_PREFIXES = frozenset({
    "KXHIGH", "KXLOW", "KXRAIN",
})


def _to_executor_signal(ws: WeatherSignal) -> Signal:
    return Signal(
        ticker      = ws.ticker,
        rule_name   = ws.rule_name,
        side        = ws.side,
        action      = ws.action,
        price_cents = ws.price_cents,
        count       = ws.count,
        reason      = ws.reason,
        meta        = {**ws.meta, "system": SYSTEM},
        track       = ws.track,
    )


def _series_from_ticker(ticker: str) -> str:
    """Extrahiert Serien-Prefix aus Ticker (z.B. KXHIGHNY aus KXHIGHNY-26APR05-T72)."""
    parts = ticker.split("-")
    return parts[0] if parts else ""


class WeatherScanner:
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

        scan_cfg             = config.get("weather_scanner", {})
        self._interval_s     = int(scan_cfg.get("interval_seconds", 300))
        self._max_close_hours = int(scan_cfg.get("max_close_hours", 48))
        self._min_vol        = float(scan_cfg.get("min_volume_usd", 25))
        self._max_concurrent = int(scan_cfg.get("max_concurrent_per_city", 2))

        sys_cfg          = config.get("systems", {}).get(SYSTEM, {})
        self._bankroll   = float(sys_cfg.get("max_exposure_usd", 40.0))

        self._rules = WeatherRuleEngine(config)

        # Ein Feed pro Stadt (dedupliziert über Koordinaten)
        self._feeds: dict[str, WeatherFeed] = {}
        seen_coords: set[str] = set()
        for series, (city, lat, lon, mtype) in WEATHER_SERIES_MAP.items():
            coord_key = f"{lat:.2f},{lon:.2f}"
            if coord_key not in seen_coords:
                self._feeds[city] = WeatherFeed(
                    city=city, lat=lat, lon=lon,
                    refresh_interval_s=int(scan_cfg.get("feed_refresh_seconds", 300)),
                )
                seen_coords.add(coord_key)

        self._stop_event           = asyncio.Event()
        self._exit_pending:  set[str] = set()
        self._exchange_trading: bool  = True
        self._last_exchange_check: float = 0.0

        logger.info(
            f"[Weather/Scanner] Gestartet | Intervall: {self._interval_s}s | "
            f"Städte: {len(self._feeds)} | Budget: ${self._bankroll:.0f}"
        )

    async def start(self):
        while not self._stop_event.is_set():
            t0 = time.monotonic()
            try:
                await self._scan_cycle()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception(f"[Weather/Scanner] Fehler: {e}")
                self._logger.log_error("WeatherScanner", str(e))
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

        # Exchange-Status alle 5 Min prüfen
        now_ts = time.monotonic()
        if now_ts - self._last_exchange_check > 300:
            try:
                status = await loop.run_in_executor(None, self._client.get_exchange_status)
                self._exchange_trading = bool(status.get("trading_active", True))
                self._last_exchange_check = now_ts
            except Exception as e:
                logger.debug(f"[Weather/Scanner] Exchange-Status fehlgeschlagen: {e}")
        if not self._exchange_trading:
            return

        # Feeds aktualisieren
        for feed in self._feeds.values():
            await loop.run_in_executor(None, feed.refresh)

        signals = 0
        signals += await self._scan_entries(loop)
        signals += await self._scan_exits(loop)

        if signals:
            logger.info(f"[Weather/Scanner] {signals} Signal(e)")
        else:
            logger.debug("[Weather/Scanner] Keine Signale")

        if self._on_cycle_end:
            await self._on_cycle_end()

    # ── Entry-Scan ───────────────────────────────────────────────────── #

    async def _scan_entries(self, loop) -> int:
        now_utc  = datetime.now(timezone.utc)
        max_secs = self._max_close_hours * 3600

        # Events per Weather-Serie abrufen
        all_markets: list[dict] = []
        for series, (city, lat, lon, mtype) in WEATHER_SERIES_MAP.items():
            try:
                events = await loop.run_in_executor(
                    None,
                    lambda s=series: self._client.get_events(
                        series_ticker=s, with_nested_markets=True, status="open",
                    ),
                )
            except Exception as e:
                logger.debug(f"[Weather/Scanner] {series} Fetch fehlgeschlagen: {e}")
                continue

            if not events:
                continue

            for ev in events:
                et          = ev.get("event_ticker", "")
                image_url   = (ev.get("image_url") or "").strip()
                event_title = (ev.get("title") or "").strip()

                for m in ev.get("markets", []):
                    if m.get("status", "active") not in ("active", "open"):
                        continue
                    ct_str = m.get("close_time", "")
                    if ct_str:
                        try:
                            ct = datetime.fromisoformat(ct_str.replace("Z", "+00:00"))
                            secs_left = (ct - now_utc).total_seconds()
                            if secs_left < 0 or secs_left > max_secs:
                                continue
                        except Exception:
                            continue
                    m["_series"]       = series
                    m["_city"]         = city
                    m["_market_type"]  = mtype
                    m["category"]      = "climate and weather"
                    m["event_title"]   = event_title
                    m["event_ticker"]  = et
                    if not m.get("image_url"):
                        m["image_url"] = image_url
                    all_markets.append(m)

        if not all_markets:
            return 0

        # Nach Volumen sortieren
        all_markets.sort(key=lambda m: float(m.get("volume_24h_fp", 0) or 0), reverse=True)

        # Positionen laden: Event-Dedup + Concurrent-Limit
        active_event_tickers: set[str] = set()
        concurrent_by_city: dict[str, int] = defaultdict(int)
        try:
            pos_data = json.loads(Path("data/positions.json").read_text())
            for p in pos_data.get("positions", []):
                et_pos = p.get("event_ticker", "")
                if et_pos:
                    active_event_tickers.add(et_pos)
                if p.get("system") == SYSTEM:
                    # Stadt aus Ticker-Prefix ableiten
                    series_pfx = _series_from_ticker(p.get("ticker", ""))
                    city_name = series_to_city(series_pfx)
                    if city_name:
                        concurrent_by_city[city_name] += 1
        except Exception:
            pass

        # Feed-Status loggen
        ready_cities = [c for c, f in self._feeds.items() if f.is_ready()]
        logger.info(
            f"[Weather/Scanner] {len(all_markets)} Märkte | "
            f"Feeds bereit: {len(ready_cities)}/{len(self._feeds)} "
            f"({', '.join(ready_cities[:5])}{'...' if len(ready_cities) > 5 else ''})"
        )

        signals = 0
        for market in all_markets:
            ticker = market.get("ticker", "")
            city   = market.get("_city", "")
            mtype  = market.get("_market_type", "high")
            et_mkt = market.get("event_ticker", "")

            # Event-Dedup (systemübergreifend)
            if et_mkt and et_mkt in active_event_tickers:
                continue

            # Concurrent-Limit pro Stadt
            if concurrent_by_city.get(city, 0) >= self._max_concurrent:
                continue

            # Feed-Kontext für diese Stadt
            feed = self._feeds.get(city)
            ctx  = feed.context(mtype) if feed and feed.is_ready() else {}
            ctx["bankroll_usd"] = self._bankroll

            if self._on_meta:
                self._on_meta(
                    ticker,
                    event_title=market.get("event_title", ""),
                    image_url=market.get("image_url", ""),
                    sub_title=market.get("sub_title", ""),
                )

            for ws in self._rules.evaluate(market, ctx, feed=feed):
                await self._on_signal(_to_executor_signal(ws))
                signals += 1

        return signals

    # ── Exit-Scan ────────────────────────────────────────────────────── #

    async def _scan_exits(self, loop) -> int:
        """
        Weather-Market Exit-Logik:
          1. Take-Profit : Bid ≥ 2.0× Einstieg
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
                mins_left = (ct - now).total_seconds() / 60
            except Exception:
                pass

            if mins_left <= 0:
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
                tp_target = int(entry_px * 2.0)
                if no_bid and no_bid >= tp_target:
                    exit_reason = f"Take-Profit: NO bid {no_bid}¢ ≥ 2× {entry_px}¢"
                    sell_price  = max(1, no_bid - 1)
                elif mins_left < 30 and no_bid and no_bid < int(entry_px * 0.4):
                    exit_reason = (
                        f"Zeit-Stop: {mins_left:.0f}min verbl. | "
                        f"NO bid {no_bid}¢ < 40% von {entry_px}¢"
                    )
                    sell_price = max(1, no_bid or 1)

            elif side == "yes":
                tp_target = min(95, int(entry_px * 2.0))
                if yes_bid and yes_bid >= tp_target:
                    exit_reason = f"Take-Profit: YES bid {yes_bid}¢ ≥ {tp_target}¢"
                    sell_price  = max(1, yes_bid - 1)
                elif mins_left < 30 and yes_bid and yes_bid < int(entry_px * 0.4):
                    exit_reason = (
                        f"Zeit-Stop: {mins_left:.0f}min verbl. | "
                        f"YES bid {yes_bid}¢ < 40% von {entry_px}¢"
                    )
                    sell_price = max(1, yes_bid or 1)

            if not exit_reason:
                continue

            exit_bid = no_bid if side == "no" else yes_bid
            if not exit_bid or exit_bid < 2:
                continue

            logger.info(
                f"[Weather/Exit] {ticker} EXIT {side.upper()} ×{count} "
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
