"""
Prediction Market Scanner (System 1).

Scannt politische, wirtschaftliche und allgemeine Märkte auf Kalshi.
Hat eigene Entry- und Exit-Logik – komplett getrennt vom Crypto-System.

Exit-Bedingungen:
  1. Take-Profit : Bid ≥ 2.2× Einstieg (Prediction Markets sind träger)
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

        self._rules                 = PredictionRuleEngine(config)
        self._stop_event            = asyncio.Event()
        self._exit_pending:  set[str]       = set()
        self._prev_yes_ask:  dict[str, int] = {}  # für Überreaktions-Erkennung
        self._price_history: dict[str, list[tuple[float, int]]] = {}  # 2h Preis-Historie pro Ticker
        self._exchange_trading:     bool    = True
        self._last_exchange_check:  float   = 0.0

        self._zone_on               = bool(scan_cfg.get("zone_enabled", False))
        self._zone_max_combined     = int(scan_cfg.get("zone_max_combined_cents", 95))
        self._zone_min_hours        = float(scan_cfg.get("zone_min_hours_remaining", 3.0))
        self._zone_count            = int(scan_cfg.get("zone_count", 3))
        self._zone_yes_min          = int(scan_cfg.get("zone_yes_leg_min", 55))
        self._zone_yes_max          = int(scan_cfg.get("zone_yes_leg_max", 88))
        self._zone_no_ya_min        = int(scan_cfg.get("zone_no_yes_ask_min", 85))

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
        loop   = asyncio.get_running_loop()
        now_ts = time.monotonic()

        # Exchange-Status alle 5 Minuten prüfen
        if now_ts - self._last_exchange_check > 300:
            try:
                status = await asyncio.wait_for(
                    loop.run_in_executor(None, self._client.get_exchange_status),
                    timeout=20.0,
                )
                self._exchange_trading      = bool(status.get("trading_active", True))
                self._last_exchange_check   = now_ts
            except Exception as e:
                logger.debug(f"[Prediction/Scanner] Exchange-Status fehlgeschlagen: {e}")
        if not self._exchange_trading:
            return

        events = await self._fetch_events(loop)

        # Positions-State einmal laden und an Sub-Scans durchreichen
        active_event_tickers: set[str] = set()
        try:
            pos_data = json.loads(Path("data/positions.json").read_text())
            for p in pos_data.get("positions", []):
                et_pos = p.get("event_ticker", "")
                if et_pos:
                    active_event_tickers.add(et_pos)
        except Exception:
            pass

        signals_total = 0
        signals_total += await self._scan_entries(loop, events, active_event_tickers)
        if self._zone_on:
            signals_total += await self._scan_zone_pairs(events, active_event_tickers)
        signals_total += await self._scan_exits(loop)

        if signals_total:
            logger.info(f"[Prediction/Scanner] {signals_total} Signal(e)")
        else:
            logger.debug("[Prediction/Scanner] Keine Signale")

        if self._on_cycle_end:
            await self._on_cycle_end()

    # ── Event-Fetch ──────────────────────────────────────────────────── #

    async def _fetch_events(self, loop) -> list:
        """Holt alle offenen Events gefiltert nach konfigurierten Kategorien."""
        try:
            events = await asyncio.wait_for(
                loop.run_in_executor(
                    None,
                    lambda: self._client.get_all_events(status="open", with_nested_markets=True),
                ),
                timeout=45.0,
            )
        except asyncio.TimeoutError:
            logger.warning("[Prediction/Scanner] get_all_events Timeout – Zyklus übersprungen")
            return []
        return [e for e in events if (e.get("category") or "").lower() in self._categories]

    # ── Entry-Scan ───────────────────────────────────────────────────── #

    async def _scan_entries(self, loop, events: list,
                            active_event_tickers: set[str]) -> int:
        markets: list[dict] = []
        now_utc  = datetime.now(timezone.utc)
        max_secs = (self._max_close_days or 30) * 86400

        for event in events:
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
                        secs_remaining = (ct - now_utc).total_seconds()
                        # Markt bereits abgelaufen → überspringen (Bug-Fix)
                        if secs_remaining <= 0:
                            continue
                        if secs_remaining > max_secs:
                            continue
                    except Exception:
                        continue
                if self._min_volume > 0:
                    if float(m.get("volume_24h_fp", 0) or 0) < self._min_volume:
                        continue
                m["category"]     = (event.get("category") or "").lower()
                m["sub_title"]    = (event.get("sub_title") or "").strip()
                m["image_url"]    = image_url
                m["event_title"]  = event_title
                m["event_ticker"] = event.get("event_ticker", "")
                markets.append(m)

        logger.info(f"[Prediction/Scanner] {len(markets)} Märkte gescannt")

        signals = 0
        for market in markets:
            ticker = market.get("ticker", "")

            # Event-Ticker Dedup: max. 1 Position pro Event (verhindert Range-Exposure)
            et_mkt = market.get("event_ticker", "")
            if et_mkt and et_mkt in active_event_tickers:
                continue

            # Überreaktions-Delta + 2h Preis-Historie berechnen und ins market-Dict injizieren
            ya_raw = market.get("yes_ask_dollars") or market.get("yes_ask")
            if ya_raw is not None:
                try:
                    ya_f       = float(ya_raw)
                    ya         = int(round(ya_f * 100)) if ya_f <= 1.0 else int(round(ya_f))
                    now_ts_abs = time.time()
                    # Überreaktions-Delta
                    prev = self._prev_yes_ask.get(ticker)
                    if prev is not None:
                        market = {**market, "_overreaction_delta": ya - prev}
                    self._prev_yes_ask[ticker] = ya
                    # 2h Preis-Historie (Regime-Filter)
                    cutoff  = now_ts_abs - 7200
                    history = [e for e in self._price_history.get(ticker, []) if e[0] >= cutoff]
                    history.append((now_ts_abs, ya))
                    self._price_history[ticker] = history
                    if len(history) >= 2:
                        prices = [e[1] for e in history]
                        market = {**market, "_price_change_2h": max(prices) - min(prices)}
                except (ValueError, TypeError):
                    pass

            if self._on_meta:
                self._on_meta(
                    ticker,
                    event_title=market.get("event_title", ""),
                    image_url=market.get("image_url", ""),
                    sub_title=market.get("sub_title", ""),
                )
            for ps in self._rules.evaluate(market, bankroll_usd=self._bankroll):
                await self._on_signal(_to_executor_signal(ps))
                signals += 1
        return signals

    # ── Zone-Bet-Scan ────────────────────────────────────────────────── #

    async def _scan_zone_pairs(self, events: list,
                               active_event_tickers: set[str]) -> int:
        """
        Zone-Bet für Prediction-Ladder-Märkte (Gas, CPI, etc.).
        Kauft YES am unteren + NO am oberen Schwellenwert desselben Events.
        Edge: combined_cost < 95¢ → in allen Szenarien profitabel (kein vol_ratio nötig).

          Szenario A (Preis in Zone):           beide lösen aus → +200 − combined
          Szenario B (Preis außerhalb Zone):    ein Leg löst aus  → +100 − combined
        """
        now_utc  = datetime.now(timezone.utc)
        min_secs = self._zone_min_hours * 3600

        def _price(m: dict, key: str) -> Optional[int]:
            v = m.get(key + "_dollars") or m.get(key)
            if v is None:
                return None
            try:
                f = float(v)
                return int(round(f * 100)) if f <= 1.0 else int(round(f))
            except (ValueError, TypeError):
                return None

        signals = 0
        for event in events:
            et = event.get("event_ticker", "")
            if not et:
                continue

            # Bereits aktive Positionen in diesem Event → überspringen
            if et in active_event_tickers:
                continue

            raw_markets = [
                m for m in event.get("markets", [])
                if m.get("status", "active") in ("active", "open")
            ]
            if len(raw_markets) < 2:
                continue

            # Zeitfilter + Preise lesen: (market, yes_ask, no_ask)
            valid: list[tuple[dict, int, int]] = []
            for m in raw_markets:
                ct_str = m.get("close_time", "")
                if not ct_str:
                    continue
                try:
                    ct = datetime.fromisoformat(ct_str.replace("Z", "+00:00"))
                    if (ct - now_utc).total_seconds() < min_secs:
                        continue
                except Exception:
                    continue
                ya = _price(m, "yes_ask")
                na = _price(m, "no_ask")
                if ya is not None and na is not None:
                    valid.append((m, ya, na))

            if len(valid) < 2:
                continue

            # Aufsteigend nach yes_ask sortieren:
            # niedrigster YES-Ask (= höchster Schwellenwert, weit über Spot) zuerst,
            # höchster YES-Ask (= niedrigster Schwellenwert, tief im Geld) zuletzt.
            valid.sort(key=lambda x: x[1])

            # Suche erstes gültiges Paar: m_high hat hohen YES-Ask (YES-Leg),
            # m_low hat niedrigen YES-Ask → teueren NO (NO-Leg).
            for i in range(len(valid) - 1):
                m_no_leg,  ya_no_leg,  na_no_leg  = valid[i]      # hoher Schwellenwert, niedriger YES
                m_yes_leg, ya_yes_leg, na_yes_leg = valid[i + 1]  # niedriger Schwellenwert, hoher YES

                # YES-Leg: YES-Ask muss im konfigurierten Preisfenster liegen
                if not (self._zone_yes_min <= ya_yes_leg <= self._zone_yes_max):
                    continue
                # NO-Leg: YES-Ask muss hoch genug sein (= NO-Preis ist günstig)
                # Nein: das NO-Leg hat den NIEDRIGEREN YES-Ask → dessen NO ist teuer.
                # Korrektur: NO-Leg = m_no_leg (hoher Schwellenwert).
                # Dessen YES-Ask ist niedrig → NO-Ask = na_no_leg (direkt lesen!).
                # Wir wollen: YES-Ask des NO-Legs ≥ zone_no_ya_min (damit NO günstig)
                # → aber m_no_leg hat den niedrigsten YES-Ask. Das passt nicht.
                # Richtige Logik: m_yes_leg ist das YES-Kauf-Leg (hoher YES-Ask),
                # m_no_leg ist das NO-Kauf-Leg (dessen YES ist hoch → NO günstig).
                # → m_no_leg.yes_ask sollte ≥ zone_no_ya_min sein.
                # Aber bei aufsteigender Sortierung hat valid[i] den niedrigeren YES.
                # Also: valid[-1] hat den höchsten YES (= YES-Kauf-Leg),
                # valid[-2] hat den zweithöchsten YES (= NO-Kauf-Leg, dessen YES ≥ 85).
                # Wir iterieren von unten: i und i+1 benachbart.
                # valid[i+1] hat höheren YES → YES-Leg.
                # valid[i] hat niedrigeren YES → NO-Leg (dessen YES muss ≥ 85 sein für günstiges NO)
                if ya_no_leg < self._zone_no_ya_min:
                    continue

                # Kosten: actual prices + 1¢ Slippage
                cost_yes = ya_yes_leg + 1
                cost_no  = na_no_leg + 1
                combined = cost_yes + cost_no

                if combined >= self._zone_max_combined:
                    continue

                ticker_yes = m_yes_leg.get("ticker", "")
                ticker_no  = m_no_leg.get("ticker", "")
                if not ticker_yes or not ticker_no:
                    continue

                logger.info(
                    f"[Prediction/ZoneBet] {et}: "
                    f"YES@{ticker_yes}({ya_yes_leg}¢) + NO@{ticker_no}(no_ask={na_no_leg}¢) "
                    f"→ combined={combined}¢ (max={self._zone_max_combined}¢)"
                )

                zone_meta_base = {
                    "system":        SYSTEM,
                    "combined_cost": combined,
                    "event_ticker":  et,
                }

                sig_yes = _to_executor_signal(PredictionSignal(
                    ticker      = ticker_yes,
                    rule_name   = "ZoneBet:YES",
                    side        = "yes",
                    action      = "buy",
                    price_cents = cost_yes,
                    count       = self._zone_count,
                    reason      = (
                        f"ZoneBet YES leg | combined={combined}¢ < "
                        f"{self._zone_max_combined}¢ | pair={ticker_no}"
                    ),
                    meta        = {**zone_meta_base, "zone_pair": ticker_no},
                    track       = "pred_zone",
                ))
                sig_no = _to_executor_signal(PredictionSignal(
                    ticker      = ticker_no,
                    rule_name   = "ZoneBet:NO",
                    side        = "no",
                    action      = "buy",
                    price_cents = cost_no,
                    count       = self._zone_count,
                    reason      = (
                        f"ZoneBet NO leg | combined={combined}¢ < "
                        f"{self._zone_max_combined}¢ | pair={ticker_yes}"
                    ),
                    meta        = {**zone_meta_base, "zone_pair": ticker_yes},
                    track       = "pred_zone",
                ))

                # Beide Legs zusammen senden — Executor prüft beide atomar
                await self._on_signal(sig_yes)
                await self._on_signal(sig_no)
                signals += 2
                break   # Nur 1 Paar pro Event

        return signals

    # ── Exit-Scan ────────────────────────────────────────────────────── #

    async def _scan_exits(self, loop) -> int:
        """
        Prediction-Market Exit-Logik:
          1. Take-Profit : Bid ≥ 2.2× Einstieg
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
                market = await asyncio.wait_for(
                    loop.run_in_executor(None, lambda t=ticker: self._client.get_market(t)),
                    timeout=15.0,
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
                # Take-Profit: NO Bid ≥ 1.7× Einstieg
                tp_target = int(entry_px * 1.7)
                if no_bid and no_bid >= tp_target:
                    exit_reason = f"Take-Profit: NO bid {no_bid}¢ ≥ 1.7× {entry_px}¢"
                    sell_price  = max(1, no_bid - 1)
                # Zeit-Stop: < 30 Min + bid < 50% (Analyse zeigt: Type-1-Trades bis 50%)
                elif mins_left < 30 and no_bid and no_bid < int(entry_px * 0.5):
                    exit_reason = (
                        f"Zeit-Stop: {mins_left:.0f}min verbl. | "
                        f"NO bid {no_bid}¢ < 50% von Einstieg {entry_px}¢"
                    )
                    sell_price = max(1, no_bid or 1)

            elif side == "yes":
                # Take-Profit: YES Bid ≥ 1.7× Einstieg (cap 95¢)
                tp_target = min(95, int(entry_px * 1.7))
                if yes_bid and yes_bid >= tp_target:
                    exit_reason = f"Take-Profit: YES bid {yes_bid}¢ ≥ {tp_target}¢"
                    sell_price  = max(1, yes_bid - 1)
                # Zeit-Stop: < 30 Min + bid < 50%
                elif mins_left < 30 and yes_bid and yes_bid < int(entry_px * 0.5):
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
                    f"[Prediction/Exit] {ticker} {side.upper()} bid {exit_bid}¢ < 2¢ "
                    f"– illiquide, überspringe"
                )
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
