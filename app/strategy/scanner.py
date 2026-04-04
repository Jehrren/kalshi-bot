"""
Market Scanner für Kalshi Prediction Markets.

Drei parallele Scan-Tracks:
  1. Politische / wirtschaftliche Märkte (Events-API, nach Kategorie)
  2. BTC Tages-Leiter      (KXBTCD Serie, 50 Preisstufen, täglich)
  3. BTC 15-Min UP/DOWN    (KXBTC15M Serie, Mean-Reversion)
     → Preise via WebSocket (ticker channel), Discovery via REST

BTC-Preis, 15min-Änderung und technische Indikatoren kommen alle
aus einem einzigen BingXFeed (BTC-USDT, 1-min Candles, öffentliche API).
"""

import asyncio
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Optional

from api.client import KalshiClient
from api.ws_client import KalshiWSFeed
from feeds.bingx_feed import BingXFeed, SERIES_SYMBOL_MAP, series_to_symbol
from logger.trade_logger import TradeLogger
from strategy.rules import RuleEngine, Signal

logger = logging.getLogger(__name__)


class MarketScanner:
    def __init__(
        self,
        client: KalshiClient,
        trade_logger: TradeLogger,
        config: dict,
        on_signal: Callable,
    ):
        self._client    = client
        self._logger    = trade_logger
        self._config    = config
        self._on_signal = on_signal

        scanner = config.get("scanner", {})
        self._interval_s  = int(scanner.get("interval_seconds", 60))
        self._max_markets = int(scanner.get("max_markets_per_scan", 200))
        self._min_volume  = float(scanner.get("min_volume_usd", 0))
        self._max_close_days = scanner.get("max_close_days", None)  # None = kein Limit
        if self._max_close_days is not None:
            self._max_close_days = int(self._max_close_days)
        self._categories  = set(c.lower() for c in scanner.get("categories", []))
        self._btc_ladder  = bool(scanner.get("btc_ladder_enabled", False))
        self._btc_15min   = bool(scanner.get("btc_15min_enabled", False))

        # Separate Regel-Engines für jeden Track
        self._political_rules  = RuleEngine(config, rule_key="rules")
        self._btc_ladder_rules = RuleEngine(config, rule_key="btc_ladder_rules")
        self._btc_15min_rules  = RuleEngine(config, rule_key="btc_15min_rules")

        # WebSocket Feed für BTC 15-Min Real-time Preise
        self._ws_feed: Optional[KalshiWSFeed] = None
        if self._btc_15min:
            self._ws_feed = KalshiWSFeed(client)

        # BingX Feeds: Ein Feed pro Crypto-Symbol.
        # Distanz-Filter, 15-Min-Signale und Stop-Loss nutzen den richtigen
        # Spot-Preis pro Asset (BTC, ETH, SOL, XRP, DOGE, BNB, HYPE, ...).
        self._bingx_feeds: dict[str, BingXFeed] = {}
        if self._btc_15min or self._btc_ladder:
            seen: set[str] = set()
            for symbol in SERIES_SYMBOL_MAP.values():
                if symbol not in seen:
                    self._bingx_feeds[symbol] = BingXFeed(symbol=symbol, refresh_interval_s=30)
                    seen.add(symbol)

        self._stop_event = asyncio.Event()
        # Event-Ticker aller Crypto-Tages-Leiter-Events (für ARB-Scan)
        self._ladder_event_tickers: list[str] = []
        # Tickers für die bereits ein Exit-Signal gesendet wurde.
        # Verhindert Doppel-Exits wenn Executor die Position noch nicht aus
        # positions.json entfernt hat bevor der nächste Scan-Zyklus läuft.
        self._exit_pending: set[str] = set()

    async def start(self):
        ws_active = False
        if self._ws_feed:
            ws_active = await self._ws_feed.start()
            logger.info(
                f"[Scanner] WebSocket: {'aktiv' if ws_active else 'nicht verfügbar (REST-Fallback)'}"
            )

        logger.info(
            f"[Scanner] Gestartet | Intervall: {self._interval_s}s | "
            f"BTC-Leiter: {self._btc_ladder} | BTC-15Min: {self._btc_15min} | "
            f"WS: {ws_active}"
        )
        while not self._stop_event.is_set():
            t0 = time.monotonic()
            try:
                await self._scan_cycle()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception(f"[Scanner] Fehler: {e}")
                self._logger.log_error("Scanner", str(e))

            elapsed = time.monotonic() - t0
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(),
                    timeout=max(0.0, self._interval_s - elapsed),
                )
            except asyncio.TimeoutError:
                pass
        logger.info("[Scanner] Gestoppt")

    async def stop(self):
        self._stop_event.set()
        if self._ws_feed:
            await self._ws_feed.stop()

    # ------------------------------------------------------------------ #
    #  Haupt-Scan-Zyklus                                                  #
    # ------------------------------------------------------------------ #

    async def _scan_cycle(self):
        loop = asyncio.get_event_loop()

        # BingX Feeds aktualisieren (rate-limited intern, ~14 Requests/30s für 7 Symbole)
        for feed in self._bingx_feeds.values():
            await loop.run_in_executor(None, feed.refresh)

        signals_total = 0

        # Track 1: Politische / wirtschaftliche Märkte
        if self._categories:
            signals_total += await self._scan_political(loop)

        # Track 2: Crypto-Märkte (dynamisch über Events-API entdeckt, keine Hardcodierung)
        if self._btc_ladder:
            signals_total += await self._scan_crypto(loop)
            # Track 2b: Leiter-Arbitrage (nutzt in _scan_crypto gespeicherten Event-Ticker)
            signals_total += await self._scan_ladder_arb(loop)

        # Track 4: Exit-Scan (läuft immer wenn irgendein Track aktiv)
        if self._btc_ladder or self._btc_15min or self._categories:
            signals_total += await self._scan_exits(loop)

        if signals_total:
            logger.info(f"[Scanner] {signals_total} Signal(e) erzeugt")
        else:
            logger.debug("[Scanner] Keine Signale")

    # ------------------------------------------------------------------ #
    #  Track 1: Politische Märkte                                         #
    # ------------------------------------------------------------------ #

    async def _scan_political(self, loop) -> int:
        # with_nested_markets=True: ein einziger API-Call statt N+1
        events = await loop.run_in_executor(
            None, lambda: self._client.get_events(
                status="open", limit=200, with_nested_markets=True
            )
        )
        allowed = [e for e in events if (e.get("category") or "").lower() in self._categories]

        markets: list[dict] = []
        for event in allowed:
            cat           = (event.get("category") or "").lower()
            sub_title     = (event.get("sub_title") or "").strip()
            series_ticker = (event.get("series_ticker") or "").upper()
            image_url     = (
                f"https://kalshi-public-docs.s3.amazonaws.com/series-images-webp/{series_ticker}.webp"
                if series_ticker else ""
            )
            for m in event.get("markets", []):
                if m.get("status", "active") not in ("active", "open"):
                    continue
                m["category"]  = cat
                m["sub_title"] = sub_title
                m["image_url"] = image_url
                markets.append(m)

        if self._min_volume > 0:
            markets = [m for m in markets if float(m.get("volume_24h_fp", 0) or 0) >= self._min_volume]

        if self._max_close_days is not None:
            now_utc = datetime.now(timezone.utc)
            max_secs = self._max_close_days * 86400
            def _within_limit(m: dict) -> bool:
                ct_str = m.get("close_time", "")
                if not ct_str:
                    return True
                try:
                    ct = datetime.fromisoformat(ct_str.replace("Z", "+00:00"))
                    return (ct - now_utc).total_seconds() <= max_secs
                except Exception:
                    return True
            before = len(markets)
            markets = [m for m in markets if _within_limit(m)]
            filtered = before - len(markets)
            if filtered:
                logger.info(f"[Scanner/Politisch] {filtered} Märkte mit Laufzeit > {self._max_close_days}d gefiltert")

        logger.info(f"[Scanner/Politisch] {len(markets)} Märkte")
        return await self._emit_signals(markets, self._political_rules, "politisch")

    # ------------------------------------------------------------------ #
    #  Track 2: Crypto-Märkte (dynamisch)                                 #
    # ------------------------------------------------------------------ #

    async def _scan_crypto(self, loop) -> int:
        """
        Scannt alle Kalshi Crypto-Märkte dynamisch (Kategorie: 'Crypto').
        Keine hardcodierten Serien – Entdeckung via paginierende Events-API.

        Gruppen:
        - 15-Min (KX*15M):     btc_15min_rules + BingX-Kontext + WS-Enrichment
        - BTC-Preisstufen:     btc_ladder_rules + OB-Filter + Distanz-Filter (±BTC-Preis)
        - Alle anderen Crypto: btc_ladder_rules (ohne Distanz- und OB-Filter)
        Keine zeitliche Begrenzung für Crypto-Märkte.
        """
        # ── Spot-Kontext pro Symbol vorab laden (Preis + Indikatoren via BingX) ────
        bankroll_usd = float(self._config.get("risk", {}).get("max_total_exposure_usd", 200.0))
        sym_ctxs: dict[str, dict] = {}
        for sym, feed in self._bingx_feeds.items():
            if feed.is_ready():
                sym_ctxs[sym] = {**feed.context(), "bankroll_usd": bankroll_usd}
        # BTC-Kontext für Momentum-Scan und Kelly-Sizing (Fallback: nur bankroll_usd)
        btc_ctx = sym_ctxs.get("BTC-USDT", {"bankroll_usd": bankroll_usd})

        # ── Threshold-Parser ────────────────────────────────────────────
        def _thr_ticker_val(ticker: str) -> float:
            """Schwellenwert aus Ticker: ...T65799.99 oder ...B78550 → float"""
            for sep in ("-T", "-B"):
                if sep in ticker:
                    try:
                        return float(ticker.split(sep)[-1])
                    except Exception:
                        pass
            return 0.0

        # ── Alle Crypto-Events dynamisch laden (paginiert) ─────────────
        crypto_events: list[dict] = []
        cursor = ""
        for _ in range(30):
            r = await loop.run_in_executor(
                None,
                lambda c=cursor: self._client._get("/events", {
                    "status": "open", "limit": 200,
                    **({"cursor": c} if c else {}),
                }),
            )
            batch = r.get("events", [])
            if not batch:
                break
            crypto_events.extend(
                e for e in batch if (e.get("category") or "").lower() == "crypto"
            )
            cursor = r.get("cursor", "")
            if not cursor:
                break

        if not crypto_events:
            logger.debug("[Scanner/Crypto] Keine Crypto-Events gefunden")
            return 0

        logger.debug(f"[Scanner/Crypto] {len(crypto_events)} Crypto-Events entdeckt")

        # ── Märkte für alle Events laden ──────────────────────────────
        now = datetime.now(timezone.utc)
        markets: list[dict] = []
        min15_markets: list[dict] = []
        active_15m_tickers: set[str] = set()

        # Volumen-Schwelle für 15-Min aus Regelwerk lesen
        min15_vol = float(next(
            (rule["condition"].get("threshold", 100)
             for rule in self._config.get("btc_15min_rules", [])
             if rule.get("condition", {}).get("type") == "min_volume_usd"),
            100,
        ))

        # Leiter Event-Ticker für ARB-Scan zurücksetzen
        self._ladder_event_tickers = []

        for ev in crypto_events:
            et     = ev.get("event_ticker", "")
            series = (ev.get("series_ticker") or et.split("-")[0]).upper()
            is_15m = "15M" in series

            # Leiter-Event für ARB-Scan merken (alle Tages-Preisstufen-Serien)
            _LADDER_SERIES = {"KXBTCD", "KXETHD", "KXSOLD", "KXXRPD", "KXDOGED", "KXBNBD"}
            if series in _LADDER_SERIES and et not in self._ladder_event_tickers:
                self._ladder_event_tickers.append(et)

            try:
                r = await loop.run_in_executor(
                    None,
                    lambda t=et: self._client.get_markets(
                        status="open", event_ticker=t, limit=200
                    ),
                )
                mkts = r.get("markets", [])
            except Exception as e:
                logger.debug(f"[Scanner/Crypto] {et}: {e}")
                continue

            for m in mkts:
                if m.get("status", "active") not in ("active", "open"):
                    continue
                m["_series"] = series

                if is_15m:
                    if not self._btc_15min:
                        continue
                    close = m.get("close_time", "")
                    try:
                        ct = datetime.fromisoformat(close.replace("Z", "+00:00"))
                        mins_left = (ct - now).total_seconds() / 60
                        if mins_left < 10 or mins_left > 15:
                            continue
                    except Exception:
                        continue
                    if float(m.get("volume_24h_fp", 0) or 0) >= min15_vol:
                        active_15m_tickers.add(m.get("ticker", ""))
                        min15_markets.append(m)
                else:
                    vol = float(m.get("volume_24h_fp", 0) or 0)
                    if self._min_volume > 0 and vol < self._min_volume:
                        continue
                    # Distanz-Filter für alle Crypto-Preisstufen (BTC, ETH, SOL, ...)
                    sym = series_to_symbol(series)
                    spot_ctx = sym_ctxs.get(sym, {}) if sym else {}
                    spot_price_for_filter = spot_ctx.get("btc_price")
                    if spot_price_for_filter and spot_price_for_filter > 0:
                        thr_val = _thr_ticker_val(m.get("ticker", ""))
                        if thr_val > 0:
                            change_abs = abs(spot_ctx.get("btc_change_15min", 0) or 0)
                            adaptive_dist = 2.0 + min(change_abs * 0.5, 2.0)
                            lo = spot_price_for_filter * (1 - adaptive_dist / 100)
                            hi = spot_price_for_filter * (1 + adaptive_dist / 100)
                            if not (lo <= thr_val <= hi):
                                continue
                    markets.append(m)

        # ── Hilfsfunktionen ─────────────────────────────────────────────
        def _thr(m: dict) -> float:
            return _thr_ticker_val(m.get("ticker", ""))

        def _price_cents(m: dict, key: str) -> int | None:
            v = m.get(key + "_dollars") or m.get(key)
            if v is None:
                return None
            f = float(v)
            return int(round(f * 100)) if f <= 1.0 else int(round(f))

        # ── Filter: MVE Combo Contracts ausschließen (keine Exit-Liquidität) ──
        before  = len(markets)
        markets = [m for m in markets if not m.get("ticker", "").startswith("KXMVECROSSCATEGORY")]
        if len(markets) < before:
            logger.debug(f"[Scanner/Crypto] MVE-Filter: {before} → {len(markets)} Märkte")

        btc_price = btc_ctx.get("btc_price")

        # ── Filter 3: Mindest-NO-Preis ≥5¢ (kein 1–2¢-Lotterie) ────────
        before  = len(markets)
        markets = [m for m in markets if (_price_cents(m, "no_ask") or 0) >= 5]
        if before != len(markets):
            logger.info(
                f"[Scanner/Crypto] Min-NO-Filter (≥5¢): {before} → {len(markets)} Märkte"
            )

        # ── Filter 4: Kalshi Orderbuch – Adverse Selection + Markt-Spread ──
        #
        # Kalshi-Besonderheit: Die meisten Märkte werden von einem zentralen
        # Market Maker (MM) quotiert. Der MM erscheint NICHT im Orderbuch-Endpoint –
        # seine Quotes werden direkt in den Marktdaten (yes_ask/bid) geliefert.
        # Ein leeres Orderbuch = normaler MM-Markt, kein Warnsignal.
        #
        # Wir prüfen:
        # A) Markt-Spread (aus REST-Daten): yes_ask – yes_bid > 5¢ → MM unsicher
        # B) Participant-Orders (aus OB): große YES-Wand bei ≥97¢ → informierter Käufer

        def _ob_cents(val) -> int:
            f = float(val)
            return int(round(f * 100)) if f <= 1.0 else int(round(f))

        ob_passed: list[dict] = []
        for m in markets:
            ticker_m = m.get("ticker", "")
            # OB-Filter nur für BTC-Preisstufen-Märkte (teurer API-Call)
            if not m.get("_series", "").startswith("KXBTC") or _thr_ticker_val(ticker_m) == 0:
                ob_passed.append(m)
                continue
            ya_m     = _price_cents(m, "yes_ask") or 50
            yb_m     = _price_cents(m, "yes_bid") or 0
            na_m     = _price_cents(m, "no_ask")  or 50

            # Check A: Gibt es aktive YES-Bidder? (= Counterparty für unsere NO-Order)
            # YES ask=100¢ ist normal bei fast sicheren Märkten – kein Fehler.
            # Entscheidend ist: YES bid > 0 (jemand kauft YES → wir können NO kaufen)
            if not yb_m or yb_m < 1:
                logger.info(
                    f"[Scanner/OB] {ticker_m} – Keine YES-Bidder (bid={yb_m}¢), übersprungen"
                )
                continue

            # Check B: Participant Orderbuch – Adverse Selection
            try:
                ob_raw = await loop.run_in_executor(
                    None, lambda t=ticker_m: self._client.get_orderbook(t, depth=10)
                )
                ob = ob_raw.get("orderbook", {})
                yes_bids = [[_ob_cents(p), int(q)] for p, q in ob.get("yes", [])]
                no_bids  = [[_ob_cents(p), int(q)] for p, q in ob.get("no", [])]

                yes_total = sum(q for _, q in yes_bids)
                no_total  = sum(q for _, q in no_bids)

                if yes_total == 0 and no_total == 0:
                    # Leeres OB = normaler MM-Markt → durchlassen
                    logger.debug(f"[Scanner/OB] {ticker_m} – MM-Markt (OB leer), OK")
                    ob_passed.append(m)
                    continue

                # Participant-Orders vorhanden: Adverse Selection prüfen
                # Große YES-Wand bei ≥97¢ = jemand ist SEHR sicher dass YES gewinnt
                high_yes_wall = sum(q for p, q in yes_bids if p >= 97)
                if high_yes_wall > 200:
                    logger.info(
                        f"[Scanner/OB] {ticker_m} – Adverse Selection: "
                        f"YES-Wand {high_yes_wall}ct @ ≥97¢, übersprungen"
                    )
                    continue

                best_yes_b = yes_bids[0][0] if yes_bids else 0
                best_no_b  = no_bids[0][0] if no_bids else 0
                logger.debug(
                    f"[Scanner/OB] {ticker_m} ✓ Mkt-Spread={ya_m - yb_m}¢ | "
                    f"OB YES={yes_total}ct @ {best_yes_b}¢ | NO={no_total}ct @ {best_no_b}¢"
                )
                ob_passed.append({**m, "_ob_yes_total": yes_total, "_ob_no_total": no_total})

            except Exception as e:
                logger.debug(f"[Scanner/OB] {ticker_m} OB-Fetch fehlgeschlagen: {e}")
                ob_passed.append(m)   # bei API-Fehler durchlassen

        before_ob = len(markets)
        markets   = ob_passed
        logger.info(
            f"[Scanner/Crypto] {len(markets)} Märkte nach Filterung"
            + (f" (OB-Filter: −{before_ob - len(markets)})" if before_ob != len(markets) else "")
        )
        signals = await self._emit_signals(
            markets, self._btc_ladder_rules, "crypto", context=btc_ctx
        )

        # ── Bonus: YES-Momentum (RSI überverkauft) ──────────────────────
        rsi        = btc_ctx.get("bingx_rsi")
        btc_change = btc_ctx.get("btc_change_15min", 0) or 0
        rsi_thr    = float(self._config.get("scanner", {}).get("yes_momentum_rsi_threshold", 35))

        if btc_price and rsi is not None and rsi <= rsi_thr and btc_change <= -0.3:
            # Suche Stufen leicht ÜBER aktuellem BTC (YES 25–65¢):
            # Markt glaubt 25–65% Chance; RSI überverkauft → Erholung erwartet
            candidates = [
                m for m in markets
                if 25 <= (_price_cents(m, "yes_ask") or 0) <= 65
            ]
            # Nächste Stufen zuerst (höchste YES-Chance = am ehesten erreichbar)
            candidates.sort(key=lambda m: _price_cents(m, "yes_ask") or 0, reverse=True)
            for m in candidates[:3]:
                ya  = _price_cents(m, "yes_ask") or 50
                thr = _thr(m)
                logger.info(
                    f"[Scanner/BTC-Leiter/YES-Momentum] RSI={rsi:.0f} ≤ {rsi_thr:.0f} | "
                    f"Δ15min={btc_change:.2f}% → YES auf ${thr:,.0f} ({ya}¢)"
                )
                await self._on_signal(Signal(
                    ticker      = m["ticker"],
                    rule_name   = "BTC-Leiter – RSI Überverkauft → YES Momentum",
                    side        = "yes",
                    action      = "buy",
                    price_cents = min(99, ya + 1),
                    count       = 5,
                    reason      = (
                        f"RSI={rsi:.0f} ≤ {rsi_thr:.0f} / "
                        f"BTC Δ15min={btc_change:.2f}% / YES={ya}¢ → Erholung erwartet"
                    ),
                    meta={
                        "close_time":   m.get("close_time", ""),
                        "title":        m.get("title", "")[:80],
                        "event_ticker": m.get("event_ticker", ""),
                    },
                ))
                signals += 1

        # ── 15-Min Märkte abarbeiten (WS-Enrichment + btc_15min_rules) ──
        if min15_markets and self._btc_15min:
            # WebSocket: abonnieren + Cache-Daten einmischen
            if self._ws_feed and self._ws_feed.is_connected():
                await self._ws_feed.subscribe(list(active_15m_tickers))
                await self._ws_feed.unsubscribe_stale(active_15m_tickers)
                enriched = []
                for m in min15_markets:
                    ws_data = self._ws_feed.get_market(m.get("ticker", ""))
                    enriched.append({**m, **ws_data} if ws_data else m)
                min15_markets = enriched

            # Pro Symbol: korrekten Spot-Kontext für Mean-Reversion verwenden
            from collections import defaultdict
            by_sym: dict[str, list] = defaultdict(list)
            for m in min15_markets:
                sym = series_to_symbol(m.get("_series", "")) or "BTC-USDT"
                by_sym[sym].append(m)

            for sym, sym_markets in by_sym.items():
                ctx  = sym_ctxs.get(sym, {"bankroll_usd": bankroll_usd})
                coin = sym.split("-")[0]
                rsi_str   = f" | RSI={ctx['bingx_rsi']:.0f}" if ctx.get("bingx_rsi") else ""
                trend_str = f" | Trend={ctx.get('bingx_trend','?')}" if ctx.get("bingx_trend") else ""
                logger.info(
                    f"[Scanner/Crypto-15Min] {len(sym_markets)} {coin} Fenster | "
                    f"{coin} ${ctx.get('btc_price', 0):,.2f} | "
                    f"Δ15min: {ctx.get('btc_change_15min', 0):.2f}%"
                    f"{rsi_str}{trend_str}"
                )
                signals += await self._emit_signals(
                    sym_markets, self._btc_15min_rules, "crypto_15min", context=ctx
                )

        return signals

    # ------------------------------------------------------------------ #
    #  Track 2b: Leiter-Arbitrage                                         #
    # ------------------------------------------------------------------ #

    async def _scan_ladder_arb(self, loop) -> int:
        """
        Erkennt Preisumkehrungen auf allen Crypto-Tages-Leitern (BTC, ETH, SOL, ...).

        Logische Bedingung: P(Asset > $X) ≥ P(Asset > $Y) wenn X < Y.
        Wenn YES(T_höher) > YES(T_tiefer): mathematischer Fehler → Arbitrage.
          → Kaufe YES auf T_tiefer (unterbewertet)
          → Kaufe NO  auf T_höher  (überbewertet)
          → In JEDEM Szenario profitabel.
        """
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
                part = ticker.split("-T")[-1].replace(".99", "")
                return float(part)
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
            except Exception as e:
                logger.debug(f"[Scanner/ARB] {evt_ticker}: {e}")
                continue

            markets = mkt_data.get("markets", [])

            valid = []
            for m in markets:
                ya  = _price(m, "yes_ask")
                vol = float(m.get("volume_24h_fp", 0) or 0)
                if ya and vol >= 500:
                    valid.append((m, _threshold(m.get("ticker", "")), ya))

            if len(valid) < 2:
                continue

            valid.sort(key=lambda x: x[1])

            for i in range(len(valid) - 1):
                m_low,  thr_low,  ya_low  = valid[i]
                m_high, thr_high, ya_high = valid[i + 1]

                if thr_high <= thr_low:
                    continue
                if ya_high <= ya_low:
                    continue

                arb_cost       = ya_low + (100 - ya_high)
                arb_profit_min = 100 - arb_cost

                if arb_profit_min < 1:
                    continue

                series = evt_ticker.split("-")[0]
                logger.info(
                    f"[Scanner/ARB] {series}: Preisumkehr! "
                    f"T{thr_low:.0f} YES={ya_low}¢ < T{thr_high:.0f} YES={ya_high}¢ | "
                    f"Mindestgewinn: {arb_profit_min}¢/Paar"
                )

                from strategy.rules import Signal
                count = 5

                await self._on_signal(Signal(
                    ticker      = m_low["ticker"],
                    rule_name   = "ARB – YES günstige Stufe",
                    side        = "yes",
                    action      = "buy",
                    price_cents = ya_low + 1,
                    count       = count,
                    reason      = (f"Arb {series}: T{thr_low:.0f} YES={ya_low}¢ < "
                                   f"T{thr_high:.0f} YES={ya_high}¢ → min +{arb_profit_min}¢/Paar"),
                    meta        = {"close_time": m_low.get("close_time", ""),
                                   "title": (m_low.get("title") or "").strip(),
                                   "event_ticker": evt_ticker,
                                   "category": "crypto", "sub_title": "", "image_url": "",
                                   "arb": True, "arb_profit_min_cents": arb_profit_min},
                ))
                await self._on_signal(Signal(
                    ticker      = m_high["ticker"],
                    rule_name   = "ARB – NO teure Stufe",
                    side        = "no",
                    action      = "buy",
                    price_cents = (100 - ya_high) + 1,
                    count       = count,
                    reason      = (f"Arb {series}: T{thr_high:.0f} YES={ya_high}¢ > "
                                   f"T{thr_low:.0f} YES={ya_low}¢ → min +{arb_profit_min}¢/Paar"),
                    meta        = {"close_time": m_high.get("close_time", ""),
                                   "title": (m_high.get("title") or "").strip(),
                                   "event_ticker": evt_ticker,
                                   "category": "crypto", "sub_title": "", "image_url": "",
                                   "arb": True, "arb_profit_min_cents": arb_profit_min},
                ))
                signals += 2

        return signals

    # (Track 3 – 15-Min – ist in _scan_crypto integriert)

    # ------------------------------------------------------------------ #
    #  Track 4: Exit-Scan (früher Track 4)                                #
    # ------------------------------------------------------------------ #

    # ------------------------------------------------------------------ #
    #  Track 4: Exit-Scan                                                 #
    # ------------------------------------------------------------------ #

    def _load_open_positions(self) -> list[dict]:
        """Liest offene Positionen aus positions.json (geschrieben von RiskManager)."""
        try:
            data = json.loads(Path("data/positions.json").read_text())
            return data.get("positions", [])
        except Exception:
            return []

    async def _scan_exits(self, loop) -> int:
        """
        Prüft jede offene Position auf Exit-Bedingungen:

        1. Take-Profit  : Aktueller Bid ≥ 2× Einstiegspreis (NO oder YES)
        2. Stop-Loss    : BTC innerhalb 0.5% der Schwelle + < 2h Restlaufzeit
        3. Zeit-Stop    : < 15 Min + Position wahrscheinlich verlierend

        Sendet Signal(action='sell') – Executor schließt die Position.
        """
        positions = self._load_open_positions()
        if not positions:
            self._exit_pending.clear()
            return 0

        # Tickers aus _exit_pending entfernen die bereits aus positions.json verschwunden sind
        current_tickers = {p.get("ticker", "") for p in positions}
        self._exit_pending = {t for t in self._exit_pending if t in current_tickers}

        # Spot-Preise für Stop-Loss (pro Symbol)
        def _spot_price(ticker: str) -> Optional[float]:
            sym = series_to_symbol(ticker)
            if sym:
                feed = self._bingx_feeds.get(sym)
                if feed and feed.is_ready():
                    return feed.current_price()
            return None

        now = datetime.now(timezone.utc)
        exits = 0

        for pos in positions:
            ticker    = pos.get("ticker", "")
            side      = str(pos.get("side", "")).lower()
            entry_px  = int(pos.get("price_cents", 0))
            count     = int(pos.get("count", 0))
            close_str = pos.get("close_time", "")

            if not ticker or not entry_px or not count:
                continue

            # Bereits ein Exit-Signal für diesen Ticker gesendet → überspringen
            if ticker in self._exit_pending:
                continue

            # Restlaufzeit berechnen
            mins_left = float("inf")
            try:
                ct = datetime.fromisoformat(close_str.replace("Z", "+00:00"))
                mins_left = (ct - now).total_seconds() / 60
            except Exception:
                pass

            if mins_left < 0:
                continue   # bereits abgelaufen

            # Aktuellen Markt-Status laden
            try:
                market = await loop.run_in_executor(
                    None, lambda t=ticker: self._client.get_market(t)
                )
            except Exception as e:
                logger.debug(f"[Scanner/Exit] get_market {ticker} fehlgeschlagen: {e}")
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
            sell_price: int = entry_px   # Fallback

            if side == "no":
                # Exit 1: Take-Profit – NO Bid ≥ 2× Einstieg (Position wächst)
                tp_target = entry_px * 2
                if no_bid and no_bid >= tp_target:
                    exit_reason = (
                        f"Take-Profit: NO bid {no_bid}¢ ≥ 2× Einstieg {entry_px}¢"
                    )
                    sell_price = max(1, no_bid - 1)

                # Exit 2: Stop-Loss – Spot-Preis nah an Schwelle + kurze Restlaufzeit
                elif (spot_price := _spot_price(ticker)) and mins_left < 120:
                    try:
                        thr       = float(ticker.split("-T")[-1])
                        pct_away  = abs(spot_price - thr) / thr * 100
                        if pct_away < 0.5:
                            exit_reason = (
                                f"Stop-Loss: ${spot_price:,.2f} nur {pct_away:.1f}% "
                                f"von Schwelle ${thr:,.0f} | {mins_left:.0f}min verbl."
                            )
                            sell_price = max(1, (no_bid or entry_px) - 1)
                    except Exception:
                        pass

                # Exit 3: Zeit-Stop – < 15 Min, YES stark gestiegen → Position verliert
                if not exit_reason and mins_left < 15 and yes_ask and yes_ask >= 85:
                    exit_reason = (
                        f"Zeit-Stop: {mins_left:.0f}min verbl. | "
                        f"YES {yes_ask}¢ ≥ 85¢ → NO verliert"
                    )
                    sell_price = max(1, (no_bid or 1))

            elif side == "yes":
                # Exit 1: Take-Profit – YES Bid ≥ 2× Einstieg (gedeckelt 95¢)
                tp_target = min(95, entry_px * 2)
                if yes_bid and yes_bid >= tp_target:
                    exit_reason = (
                        f"Take-Profit: YES bid {yes_bid}¢ ≥ Ziel {tp_target}¢ "
                        f"(2× Einstieg {entry_px}¢, cap 95¢)"
                    )
                    sell_price = max(1, yes_bid - 1)

                # Exit 3: Zeit-Stop – < 15 Min, YES stark gefallen
                elif mins_left < 15 and yes_bid and yes_bid <= 20:
                    exit_reason = (
                        f"Zeit-Stop: {mins_left:.0f}min verbl. | "
                        f"YES bid {yes_bid}¢ ≤ 20¢ → Verlust stoppen"
                    )
                    sell_price = max(1, yes_bid or 1)

            if not exit_reason:
                continue

            # ── Orderbuch-Check: Participant-Liquidität für Exit ────────
            # Kalshi: MM bietet immer zwei Seiten → Exit ist fast immer möglich.
            # OB-Endpoint zeigt NUR Participant-Orders (nicht MM-Quotes).
            # Wenn OB leer → MM-Markt → Exit zu MM-Preis (kein Blocking).
            # Wenn Participant-Orders vorhanden → Teilverkauf wenn nötig.
            exit_count = count
            try:
                ob_raw = await loop.run_in_executor(
                    None, lambda t=ticker: self._client.get_orderbook(t, depth=10)
                )
                ob = ob_raw.get("orderbook", {})

                def _obc(val) -> int:
                    f = float(val)
                    return int(round(f * 100)) if f <= 1.0 else int(round(f))

                if side == "no":
                    bids_ob = [[_obc(p), int(q)] for p, q in ob.get("no", [])]
                else:
                    bids_ob = [[_obc(p), int(q)] for p, q in ob.get("yes", [])]

                part_available = sum(q for p, q in bids_ob if p >= sell_price)
                ob_total = sum(q for _, q in bids_ob)

                if ob_total == 0:
                    # Leeres OB = MM-Markt → Exit zu MM-Preis, Volles count behalten
                    logger.debug(f"[Scanner/Exit] {ticker} – MM-Markt, Exit zu {sell_price}¢")
                elif part_available < exit_count:
                    # Participant-Liquidität reicht nicht → MM ergänzt den Rest
                    logger.debug(
                        f"[Scanner/Exit] {ticker} – {part_available}/{count}ct als "
                        f"Participant-Orders, MM deckt Rest"
                    )
                    # Nicht reduzieren: MM bietet immer an → vollen count senden
            except Exception as e:
                logger.debug(f"[Scanner/Exit] OB-Check {ticker} fehlgeschlagen: {e}")

            logger.info(
                f"[Scanner/Exit] {ticker} EXIT {side.upper()} ×{exit_count} "
                f"@ {sell_price}¢ | {exit_reason}"
            )
            self._exit_pending.add(ticker)
            await self._on_signal(Signal(
                ticker      = ticker,
                rule_name   = f"Exit: {exit_reason[:60]}",
                side        = side,
                action      = "sell",
                price_cents = sell_price,
                count       = exit_count,
                reason      = exit_reason,
                meta        = {
                    "close_time":        close_str,
                    "exit":              True,
                    "entry_price_cents": entry_px,
                },
            ))
            exits += 1

        if exits:
            logger.info(f"[Scanner/Exit] {exits} Exit-Signal(e)")
        return exits

    # ------------------------------------------------------------------ #
    #  Signal-Emission                                                    #
    # ------------------------------------------------------------------ #

    async def _emit_signals(
        self,
        markets: list[dict],
        rule_engine: RuleEngine,
        track: str,
        context: dict | None = None,
    ) -> int:
        count = 0
        for market in markets:
            signals = rule_engine.evaluate(market, context=context)
            for signal in signals:
                self._logger.log_signal(
                    ticker=signal.ticker,
                    rule_name=f"[{track}] {signal.rule_name}",
                    side=signal.side,
                    price_cents=signal.price_cents,
                    count=signal.count,
                    reason=signal.reason,
                    extra={**signal.meta, "track": track},
                )
                await self._on_signal(signal)
                count += 1
        return count
