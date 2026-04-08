"""
Crypto Market Scanner (System 2).

Drei Sub-Tracks:
  A) Tages-Leiter (BTC, ETH, SOL, ...): btc_ladder_rules
  B) Leiter-Arbitrage: Preisumkehr-Detection
  C) 15-Min Mean Reversion: crypto_15min_rules

Exit-Logik (alle Fixes gegenüber altem scanner.py):
  Stop-Loss : < 30min + < 0.3% Abstand zur Schwelle (war: 2h + 0.5%)
  Zeit-Stop NO: < 10min + YES ≥ 90¢ (war: 15min + 85¢)
  Zeit-Stop YES: < 10min + bid < entry × 0.5 (war: absolut ≤ 20¢)
  Take-Profit NO: bid ≥ 2.5× Einstieg (war: 2×)
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
from api.ws_client import KalshiWSFeed
from crypto.rules import CryptoLadderRuleEngine, Crypto15MinRuleEngine, CryptoZoneRuleEngine, CryptoSignal
from feeds.bingx_feed import BingXFeed, SERIES_SYMBOL_MAP, series_to_symbol
from logger.trade_logger import TradeLogger
from trader.executor import Signal

logger = logging.getLogger(__name__)

SYSTEM         = "crypto"
_LADDER_SERIES = frozenset({"KXBTCD", "KXETHD", "KXSOLD", "KXXRPD", "KXDOGED", "KXBNBD"})


def _to_executor_signal(cs: CryptoSignal) -> Signal:
    return Signal(
        ticker      = cs.ticker,
        rule_name   = cs.rule_name,
        side        = cs.side,
        action      = cs.action,
        price_cents = cs.price_cents,
        count       = cs.count,
        reason      = cs.reason,
        meta        = {**cs.meta, "system": SYSTEM},
        track       = cs.track,
    )


def _cents(val) -> int:
    f = float(val)
    return int(round(f * 100)) if f <= 1.0 else int(round(f))


class CryptoScanner:
    def __init__(
        self,
        client:       KalshiClient,
        trade_logger: TradeLogger,
        config:       dict,
        on_signal:    Callable,
        on_meta:      Optional[Callable] = None,
        on_cycle_end: Optional[Callable] = None,
    ):
        self._client      = client
        self._logger      = trade_logger
        self._config      = config
        self._on_signal   = on_signal
        self._on_meta     = on_meta
        self._on_cycle_end = on_cycle_end

        scan_cfg          = config.get("crypto_scanner", {})
        self._interval_s  = int(scan_cfg.get("interval_seconds", 30))
        self._ladder_on   = bool(scan_cfg.get("ladder_enabled", True))
        self._min15_on    = bool(scan_cfg.get("min15_enabled", True))
        self._min_vol            = float(scan_cfg.get("min_volume_usd", 25))
        self._min_vol_by_series  = dict(scan_cfg.get("min_volume_by_series", {}))
        self._max_concurrent      = int(scan_cfg.get("max_concurrent_per_asset", 2))
        self._max_concurrent_event = int(scan_cfg.get("max_concurrent_per_event", 2))
        self._zone_on             = bool(scan_cfg.get("zone_enabled", True))
        self._min15_series        = list(scan_cfg.get("min15_series", []))

        sys_cfg           = config.get("systems", {}).get(SYSTEM, {})
        self._bankroll    = float(sys_cfg.get("max_exposure_usd", 80.0))

        self._ladder_rules = CryptoLadderRuleEngine(config)
        self._min15_rules  = Crypto15MinRuleEngine(config)
        self._zone_rules   = CryptoZoneRuleEngine(config)

        self._ws_feed: Optional[KalshiWSFeed] = None
        if self._min15_on:
            self._ws_feed = KalshiWSFeed(client)

        # BingX Feeds: ein Feed pro Symbol
        self._bingx_feeds: dict[str, BingXFeed] = {}
        seen: set[str] = set()
        for symbol in SERIES_SYMBOL_MAP.values():
            if symbol not in seen:
                self._bingx_feeds[symbol] = BingXFeed(symbol=symbol, refresh_interval_s=30)
                seen.add(symbol)

        self._stop_event          = asyncio.Event()
        self._ladder_event_tickers: list[str] = []
        self._exit_pending:        set[str]   = set()
        self._exchange_trading:    bool       = True
        self._last_exchange_check: float      = 0.0

        logger.info(
            f"[Crypto/Scanner] Gestartet | Intervall: {self._interval_s}s | "
            f"Leiter: {self._ladder_on} | 15-Min: {self._min15_on} | "
            f"Budget: ${self._bankroll:.0f}"
        )

    async def start(self):
        ws_active = False
        if self._ws_feed:
            ws_active = await self._ws_feed.start()
            logger.info(f"[Crypto/Scanner] WebSocket: {'aktiv' if ws_active else 'REST-Fallback'}")

        while not self._stop_event.is_set():
            t0 = time.monotonic()
            try:
                await self._scan_cycle()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception(f"[Crypto/Scanner] Fehler: {e}")
                self._logger.log_error("CryptoScanner", str(e))
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
        if self._ws_feed:
            await self._ws_feed.stop()

    # ── Scan-Zyklus ──────────────────────────────────────────────────── #

    async def _scan_cycle(self):
        loop    = asyncio.get_running_loop()
        signals = 0

        # Exchange-Status alle 5 Minuten
        now_ts = time.monotonic()
        if now_ts - self._last_exchange_check > 300:
            try:
                status = await loop.run_in_executor(None, self._client.get_exchange_status)
                self._exchange_trading  = bool(status.get("trading_active", True))
                self._last_exchange_check = now_ts
            except Exception as e:
                logger.debug(f"[Crypto/Scanner] Exchange-Status fehlgeschlagen: {e}")
        if not self._exchange_trading:
            return

        # BingX Feeds aktualisieren
        for feed in self._bingx_feeds.values():
            await loop.run_in_executor(None, feed.refresh)

        # Positions-State einmal laden und an alle Sub-Scans durchreichen
        cross_blocked_events: set[str]       = set()
        crypto_event_count:   dict[str, int] = defaultdict(int)
        try:
            pos_data = json.loads(Path("data/positions.json").read_text())
            for p in pos_data.get("positions", []):
                et_pos = p.get("event_ticker", "")
                if et_pos:
                    if p.get("system") == SYSTEM:
                        crypto_event_count[et_pos] += 1
                    else:
                        cross_blocked_events.add(et_pos)
        except Exception:
            pass

        if self._ladder_on:
            signals += await self._scan_ladder(loop, cross_blocked_events, crypto_event_count)
            signals += await self._scan_arb(loop)
            if self._zone_on:
                signals += await self._scan_zone(loop, cross_blocked_events, crypto_event_count)

        signals += await self._scan_exits(loop)

        if signals:
            logger.info(f"[Crypto/Scanner] {signals} Signal(e)")
        else:
            logger.debug("[Crypto/Scanner] Keine Signale")

        if self._on_cycle_end:
            await self._on_cycle_end()

    # ── Track A: Tages-Leiter ─────────────────────────────────────────── #

    async def _scan_ladder(self, loop,
                           cross_blocked_events: set[str],
                           crypto_event_count: dict[str, int]) -> int:
        bankroll_usd = self._bankroll

        sym_ctxs: dict[str, dict] = {}
        for sym, feed in self._bingx_feeds.items():
            if feed.is_ready():
                sym_ctxs[sym] = {**feed.context(), "bankroll_usd": bankroll_usd}

        def _thr_val(ticker: str) -> float:
            for sep in ("-T", "-B"):
                if sep in ticker:
                    try:
                        return float(ticker.split(sep)[-1])
                    except Exception:
                        pass
            return 0.0

        def _pc(m: dict, key: str) -> Optional[int]:
            v = m.get(key + "_dollars") or m.get(key)
            if v is None:
                return None
            f = float(v)
            return int(round(f * 100)) if f <= 1.0 else int(round(f))

        crypto_events = await loop.run_in_executor(None, self._client.get_all_crypto_events)
        if not crypto_events:
            crypto_events = []

        # 15M-Series werden von get_all_crypto_events() nicht zurückgegeben →
        # explizit per series_ticker abfragen (konfigurierbar in crypto_scanner.min15_series)
        if self._min15_on and self._min15_series:
            for series_ticker in self._min15_series:
                try:
                    min15_evs = await loop.run_in_executor(
                        None,
                        lambda s=series_ticker: self._client.get_events(
                            series_ticker=s, with_nested_markets=True, status="open"
                        ),
                    )
                    if min15_evs:
                        crypto_events.extend(min15_evs)
                        logger.info(
                            f"[Crypto/Scanner] {series_ticker}: {len(min15_evs)} 15M-Event(s) geladen"
                        )
                except Exception as e:
                    logger.debug(f"[Crypto/Scanner] 15M-Fetch {series_ticker} fehlgeschlagen: {e}")

        if not crypto_events:
            return 0

        now            = datetime.now(timezone.utc)
        markets:       list[dict] = []
        min15_markets: list[dict] = []
        active_15m_tickers: set[str] = set()
        self._ladder_event_tickers = []

        # Volumen-Schwellenwert aus aktivierten 15M-Regeln ermitteln.
        # HINWEIS: volume_24h_fp ist für 15M-Märkte immer 0 (Markt lebt nur 15 Min).
        # Deshalb ist die Mindest-Volumen-Regel standardmäßig deaktiviert → default 0.
        min15_vol = float(next(
            (r["condition"].get("threshold", 0)
             for r in self._config.get("crypto_15min_rules", [])
             if r.get("enabled", True) and r.get("condition", {}).get("type") == "min_volume_usd"),
            0,
        ))

        for ev in crypto_events:
            et     = ev.get("event_ticker", "")
            series = (ev.get("series_ticker") or et.split("-")[0]).upper()
            is_15m = "15M" in series

            if not any(series.startswith(pfx) for pfx in SERIES_SYMBOL_MAP):
                continue
            if series in _LADDER_SERIES and et not in self._ladder_event_tickers:
                self._ladder_event_tickers.append(et)

            for m in ev.get("markets", []):
                if m.get("status", "active") not in ("active", "open"):
                    continue
                m["_series"]      = series
                m["category"]     = "crypto"
                m["event_title"]  = (ev.get("title") or "").strip()
                m["event_ticker"] = et
                if not m.get("image_url"):
                    m["image_url"] = (ev.get("image_url") or "").strip()

                if is_15m:
                    if not self._min15_on:
                        continue
                    close = m.get("close_time", "")
                    try:
                        ct        = datetime.fromisoformat(close.replace("Z", "+00:00"))
                        mins_left = (ct - now).total_seconds() / 60
                        if mins_left < 10 or mins_left > 15:
                            continue
                    except Exception:
                        continue
                    vol = float(m.get("volume_24h_fp", 0) or 0)
                    if min15_vol > 0 and vol < min15_vol:
                        continue
                    active_15m_tickers.add(m.get("ticker", ""))
                    min15_markets.append(m)
                else:
                    if series not in _LADDER_SERIES:
                        continue
                    vol            = float(m.get("volume_24h_fp", 0) or 0)
                    series_min_vol = self._min_vol_by_series.get(series, self._min_vol)
                    if vol < series_min_vol:
                        continue
                    # Distanz-Filter
                    sym = series_to_symbol(series)
                    spot_ctx = sym_ctxs.get(sym, {}) if sym else {}
                    spot_p   = spot_ctx.get("btc_price")
                    if spot_p and spot_p > 0:
                        thr = _thr_val(m.get("ticker", ""))
                        if thr > 0:
                            change_abs   = abs(spot_ctx.get("btc_change_15min", 0) or 0)
                            adaptive_pct = 2.0 + min(change_abs * 0.5, 2.0)
                            # Asymmetrischer Filter:
                            # – 5% unter Spot: erfasst YES>85¢-Zone (threshold 2–5% unter Spot)
                            # – adaptiv über Spot: verhindert hoffnungslos niedrige YES-Preise
                            lo = spot_p * (1 - 5.0 / 100)
                            hi = spot_p * (1 + adaptive_pct / 100)
                            if not (lo <= thr <= hi):
                                continue
                    markets.append(m)

        # MVE Combo ausschließen
        markets = [m for m in markets if not m.get("ticker", "").startswith("KXMVECROSSCATEGORY")]
        # Mindest-NO-Preis ≥ 5¢
        markets = [m for m in markets if (_pc(m, "no_ask") or 0) >= 5]
        # Nach Volumen sortieren – höchste Liquidität zuerst
        markets.sort(key=lambda m: float(m.get("volume_24h_fp", 0) or 0), reverse=True)

        # Concurrent-Limit + Cross-Track-Hedge aus positions.json (bereits geladen)
        concurrent_by_series: dict[str, int] = defaultdict(int)
        active_15m_symbols:   set[str]       = set()
        try:
            pos_data = json.loads(Path("data/positions.json").read_text())
            for p in pos_data.get("positions", []):
                if p.get("system") != SYSTEM:
                    continue
                if p.get("track") == "crypto_15min":
                    sym_15m = series_to_symbol(p.get("ticker", "").split("-")[0])
                    if sym_15m:
                        active_15m_symbols.add(sym_15m)
                else:
                    series_key = p.get("ticker", "").split("-")[0]
                    concurrent_by_series[series_key] += 1
        except Exception:
            pass

        # Per-Serie Aufschlüsselung + Feed-Status
        series_counts: dict[str, int] = defaultdict(int)
        for m in markets:
            series_counts[m.get("_series", "?")] += 1
        feed_info = " | ".join(
            f"{sym.split('-')[0]}={feed.current_price() or '?'}"
            for sym, feed in self._bingx_feeds.items()
            if feed.is_ready()
        )
        logger.info(
            f"[Crypto/Scanner] {len(markets)} Leiter-Märkte | "
            + " ".join(f"{s}:{n}" for s, n in sorted(series_counts.items()))
            + (f" | Feeds: {feed_info}" if feed_info else " | Feeds: nicht bereit")
        )

        signals = 0
        signals_by_series: dict[str, int] = defaultdict(int)
        skipped_concurrent = skipped_event = skipped_hedge = 0

        for market in markets:
            series_key = market.get("_series", "")
            sym        = series_to_symbol(series_key) or "BTC-USDT"

            # Concurrent-Limit: max. N offene Positionen pro Serie
            if concurrent_by_series.get(series_key, 0) >= self._max_concurrent:
                skipped_concurrent += 1
                continue

            # Event-Ticker Dedup: andere Systeme sperren komplett; Crypto max. N pro Event
            et_mkt = market.get("event_ticker", "")
            if et_mkt and et_mkt in cross_blocked_events:
                skipped_event += 1
                continue
            if et_mkt and crypto_event_count.get(et_mkt, 0) >= self._max_concurrent_event:
                skipped_event += 1
                continue

            # Cross-Track Hedge-Check: kein Leiter-Einstieg wenn 15-Min-Position auf gleichem Asset
            if sym in active_15m_symbols:
                skipped_hedge += 1
                continue

            ctx = sym_ctxs.get(sym, {"bankroll_usd": bankroll_usd})
            mkt_signals = self._ladder_rules.evaluate(market, ctx)
            for cs in mkt_signals:
                await self._on_signal(_to_executor_signal(cs))
                signals += 1
                signals_by_series[series_key] += 1
            if mkt_signals and et_mkt:
                crypto_event_count[et_mkt] += 1

        skip_info = []
        if skipped_concurrent: skip_info.append(f"concurrent:{skipped_concurrent}")
        if skipped_event:      skip_info.append(f"event-dedup:{skipped_event}")
        if skipped_hedge:      skip_info.append(f"hedge:{skipped_hedge}")
        if signals:
            logger.info(
                f"[Crypto/Leiter] {signals} Signal(e): "
                + " ".join(f"{s}:{n}" for s, n in sorted(signals_by_series.items()))
                + (f" | übersprungen: {', '.join(skip_info)}" if skip_info else "")
            )
        elif skip_info:
            logger.debug(f"[Crypto/Leiter] 0 Signale | übersprungen: {', '.join(skip_info)}")

        # 15-Min Märkte
        if min15_markets and self._min15_on:
            if self._ws_feed and self._ws_feed.is_connected():
                await self._ws_feed.subscribe(list(active_15m_tickers))
                await self._ws_feed.unsubscribe_stale(active_15m_tickers)
                enriched = []
                for m in min15_markets:
                    ws_data = self._ws_feed.get_market(m.get("ticker", ""))
                    enriched.append({**m, **ws_data} if ws_data else m)
                min15_markets = enriched

            by_sym: dict[str, list] = defaultdict(list)
            for m in min15_markets:
                sym = series_to_symbol(m.get("_series", "")) or "BTC-USDT"
                by_sym[sym].append(m)

            for sym, sym_markets in by_sym.items():
                ctx = sym_ctxs.get(sym, {"bankroll_usd": bankroll_usd})
                for m in sym_markets:
                    for cs in self._min15_rules.evaluate(m, ctx):
                        await self._on_signal(_to_executor_signal(cs))
                        signals += 1

        return signals

    # ── Track B: Leiter-Arbitrage ─────────────────────────────────────── #

    async def _scan_arb(self, loop) -> int:
        if not self._ladder_event_tickers:
            return 0

        def _price(m, key):
            v = m.get(key + "_dollars") or m.get(key)
            if v is None:
                return None
            f = float(v)
            return int(round(f * 100)) if f <= 1.0 else int(round(f))

        def _threshold(ticker: str) -> float:
            try:
                return float(ticker.split("-T")[-1].replace(".99", ""))
            except Exception:
                return 0.0

        signals = 0
        for evt_ticker in self._ladder_event_tickers:
            try:
                mkt_data = await loop.run_in_executor(
                    None,
                    lambda t=evt_ticker: self._client.get_markets(
                        event_ticker=t, status="open", limit=60
                    ),
                )
            except Exception:
                continue

            valid = []
            for m in mkt_data.get("markets", []):
                ya  = _price(m, "yes_ask")
                na  = _price(m, "no_ask")
                vol = float(m.get("volume_24h_fp", 0) or 0)
                if ya and na and vol >= 500:
                    valid.append((m, _threshold(m.get("ticker", "")), ya, na))

            if len(valid) < 2:
                continue
            valid.sort(key=lambda x: x[1])

            for i in range(len(valid) - 1):
                m_low, thr_low, ya_low, _ = valid[i]
                m_high, thr_high, ya_high, na_high = valid[i + 1]
                if thr_high <= thr_low or ya_high <= ya_low:
                    continue
                arb_cost = ya_low + na_high
                if 100 - arb_cost < 1:
                    continue

                series = evt_ticker.split("-")[0]
                profit_min = 100 - arb_cost
                logger.info(
                    f"[Crypto/ARB] {series}: Preisumkehr "
                    f"T{thr_low:.0f} YES={ya_low}¢ < T{thr_high:.0f} YES={ya_high}¢ | "
                    f"min +{profit_min}¢/Paar"
                )
                for sig_side, m_target, px_target, track in [
                    ("yes", m_low,  ya_low  + 1, "arb"),
                    ("no",  m_high, na_high + 1, "arb"),
                ]:
                    await self._on_signal(Signal(
                        ticker      = m_target["ticker"],
                        rule_name   = f"ARB – {'YES günstig' if sig_side == 'yes' else 'NO teuer'}",
                        side        = sig_side,
                        action      = "buy",
                        price_cents = px_target,
                        count       = 5,
                        reason      = f"Arb {series}: min +{profit_min}¢/Paar",
                        meta        = {
                            "close_time": m_target.get("close_time", ""),
                            "title":      (m_target.get("title") or "").strip(),
                            "event_ticker": evt_ticker,
                            "category":   "crypto",
                            "sub_title":  "",
                            "image_url":  "",
                            "system":     SYSTEM,
                        },
                        track       = track,
                    ))
                    signals += 1
        return signals

    # ── Track C: Zone/Spread-Bets ─────────────────────────────────────── #

    async def _scan_zone(self, loop,
                         cross_blocked_events: set[str],
                         crypto_event_count: dict[str, int]) -> int:
        """
        Zone/Spread-Bet innerhalb desselben Events (Track C).

        Kauft YES auf niedrigere + NO auf höhere Schwelle.
        Edge: vol_ratio < 0.8 → aktuelle Volatilität < markt-implizierte Volatilität
        → reale Zone-Wahrscheinlichkeit > Markt-Implied → positiver EV.

        Payoff: in-Zone +($2-cost), außer-Zone +($1-cost).
        Mit combined ≤ 95¢ sind alle Szenarien profitabel (kein Breakeven nötig).
        Edge tritt auf wenn reale P_zone > Markt-Implied (weil momentan weniger Volatilität).
        """
        if not self._ladder_event_tickers:
            return 0

        bankroll_usd = self._bankroll
        sym_ctxs: dict[str, dict] = {}
        for sym, feed in self._bingx_feeds.items():
            if feed.is_ready():
                sym_ctxs[sym] = {**feed.context(), "bankroll_usd": bankroll_usd}

        def _price(m, key):
            v = m.get(key + "_dollars") or m.get(key)
            if v is None:
                return None
            f = float(v)
            return int(round(f * 100)) if f <= 1.0 else int(round(f))

        def _threshold(ticker: str) -> float:
            for sep in ("-T", "-B"):
                if sep in ticker:
                    try:
                        return float(ticker.split(sep)[-1])
                    except Exception:
                        pass
            return 0.0

        signals = 0
        for evt_ticker in self._ladder_event_tickers:
            # Events mit Fremdpositionen oder bereits voll besetzten Crypto-Positionen überspringen
            if evt_ticker in cross_blocked_events:
                continue
            if crypto_event_count.get(evt_ticker, 0) >= self._max_concurrent_event:
                continue

            try:
                mkt_data = await loop.run_in_executor(
                    None,
                    lambda t=evt_ticker: self._client.get_markets(
                        event_ticker=t, status="open", limit=60
                    ),
                )
            except Exception:
                continue

            # vol_ratio vorab prüfen – spart Iteration wenn kein Edge
            series = evt_ticker.split("-")[0]
            sym    = series_to_symbol(series)
            ctx    = sym_ctxs.get(sym, {"bankroll_usd": bankroll_usd}) if sym else {"bankroll_usd": bankroll_usd}
            vol_ratio_raw = ctx.get("bingx_vol_ratio")
            vol_ratio     = float(vol_ratio_raw) if vol_ratio_raw is not None else 1.0
            if vol_ratio >= 0.8:  # Schnell-Exit: Zone-Regeln verlangen < 0.8
                continue

            # Märkte filtern und nach Schwelle sortieren
            valid = []
            for m in mkt_data.get("markets", []):
                ya  = _price(m, "yes_ask")
                na  = _price(m, "no_ask")
                vol = float(m.get("volume_24h_fp", 0) or 0)
                thr = _threshold(m.get("ticker", ""))
                if ya is not None and na is not None and vol >= 500 and thr > 0:
                    m["event_ticker"] = evt_ticker
                    valid.append((m, thr, ya, na))

            if len(valid) < 2:
                continue

            valid.sort(key=lambda x: x[1])  # aufsteigend nach Schwelle

            # Nur direkt benachbarte Paare prüfen (1 Strike Abstand)
            for i in range(len(valid) - 1):
                m_low,  thr_low,  ya_low,  _       = valid[i]
                m_high, thr_high, ya_high, na_high = valid[i + 1]

                if thr_high <= thr_low:
                    continue

                pair_signals = self._zone_rules.evaluate_pair(m_low, m_high, ctx)
                for cs in pair_signals:
                    await self._on_signal(_to_executor_signal(cs))
                    signals += 1

                if pair_signals:
                    combined = ya_low + na_high
                    logger.info(
                        f"[Crypto/Zone] {evt_ticker}: "
                        f"T{thr_low:.0f}(YES@{ya_low}¢) + T{thr_high:.0f}(NO@{na_high}¢) "
                        f"= {combined}¢ | vol_ratio={vol_ratio:.2f}"
                    )
                    break  # Maximal ein Paar pro Event pro Zyklus

        if signals:
            logger.info(f"[Crypto/Zone] {signals} Signal(e)")
        return signals

    # ── Exit-Logik (ALLE FIXES) ───────────────────────────────────────── #

    async def _scan_exits(self, loop) -> int:
        """
        Reparierte Exit-Logik für Crypto-Positionen.

        Fixes gegenüber altem scanner.py:
          Stop-Loss   : < 30min (war 2h) + < 0.3% Abstand (war 0.5%)
          Zeit-Stop NO: < 10min + YES ≥ 90¢ (war 15min + 85¢)
          Zeit-Stop YES: < 10min + bid < entry × 0.5 (war absolut ≤ 20¢)
          Take-Profit NO: bid ≥ 2.5× Einstieg (war 2×)
        """
        try:
            data      = json.loads(Path("data/positions.json").read_text())
            positions = [p for p in data.get("positions", []) if p.get("system") == SYSTEM]
        except Exception:
            return 0

        current_tickers = {p.get("ticker", "") for p in positions}
        self._exit_pending = {t for t in self._exit_pending if t in current_tickers}

        def _spot_price(ticker: str) -> Optional[float]:
            sym  = series_to_symbol(ticker)
            if sym:
                feed = self._bingx_feeds.get(sym)
                if feed and feed.is_ready():
                    return feed.current_price()
            return None

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

            yes_ask = _pc("yes_ask")
            yes_bid = _pc("yes_bid")
            no_ask  = _pc("no_ask")
            no_bid  = _pc("no_bid")

            exit_reason: Optional[str] = None
            sell_price:  int           = entry_px

            if side == "no":
                # Take-Profit: NO bid ≥ 1.7× Einstieg (Daten zeigen: früher Exit ist besser)
                tp_target = int(entry_px * 1.7)
                if no_bid and no_bid >= tp_target:
                    exit_reason = f"Take-Profit: NO bid {no_bid}¢ ≥ 1.7× {entry_px}¢"
                    sell_price  = max(1, no_bid - 1)

                # Stop-Loss: Spot nah an Schwelle – NUR wenn < 30min (FIX: war 2h)
                # UND Abstand < 0.3% (FIX: war 0.5%)
                elif (spot_price := _spot_price(ticker)) and mins_left < 30:
                    try:
                        thr      = float(ticker.split("-T")[-1])
                        pct_away = abs(spot_price - thr) / thr * 100
                        if pct_away < 0.3:
                            exit_reason = (
                                f"Stop-Loss: ${spot_price:,.2f} nur {pct_away:.2f}% "
                                f"von Schwelle ${thr:,.0f} | {mins_left:.0f}min verbl."
                            )
                            sell_price = max(1, (no_bid or 1))
                    except Exception:
                        pass

                # Zeit-Stop: < 10min (FIX: war 15min) + YES ≥ 90¢ (FIX: war 85¢)
                if not exit_reason and mins_left < 10 and yes_ask and yes_ask >= 90:
                    exit_reason = (
                        f"Zeit-Stop: {mins_left:.0f}min verbl. | "
                        f"YES {yes_ask}¢ ≥ 90¢ → NO verliert"
                    )
                    sell_price = max(1, no_bid or 1)

            elif side == "yes":
                # Take-Profit: YES bid ≥ 1.7× Einstieg (cap 95¢)
                tp_target = min(95, int(entry_px * 1.7))
                if yes_bid and yes_bid >= tp_target:
                    exit_reason = f"Take-Profit: YES bid {yes_bid}¢ ≥ {tp_target}¢"
                    sell_price  = max(1, yes_bid - 1)

                # Zeit-Stop: < 10min + bid < entry × 0.5 (Analyse bestätigt: nur Tot-Trades)
                elif mins_left < 10 and yes_bid and yes_bid < int(entry_px * 0.5):
                    exit_reason = (
                        f"Zeit-Stop: {mins_left:.0f}min verbl. | "
                        f"YES bid {yes_bid}¢ < 50% von Einstieg {entry_px}¢"
                    )
                    sell_price = max(1, yes_bid or 1)

            if not exit_reason:
                continue

            # Exit-Guard: kein Sell wenn bid < 2¢ (illiquide – warten bis Liquidität zurück)
            exit_bid = no_bid if side == "no" else yes_bid
            if not exit_bid or exit_bid < 2:
                logger.debug(
                    f"[Crypto/Exit] {ticker} {side.upper()} bid {exit_bid}¢ < 2¢ "
                    f"– illiquide, überspringe"
                )
                continue

            logger.info(
                f"[Crypto/Exit] {ticker} EXIT {side.upper()} ×{count} "
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
