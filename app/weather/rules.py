"""
Weather Market Regelwerk (System 3).

Verwaltet Entry-Signale für Temperatur-Märkte (Daily High / Daily Low).

Edge-Quellen:
  1. GFS 31-Member Ensemble vs. Marktpreis (Probability-Gap)
  2. NWS Station-Bias-Korrektur (GFS ≠ Settlement-Station)
  3. Stadt-Selektion (warme stabile Klimata > volatile Nordstädte)
  4. Modell-Update-Timing (GFS alle 6h, Märkte reprisen mit Verzögerung)

Regeln (weather_rules):
  forecast_above    : Forecast sicher über Schwelle → YES kaufen (oder NO bei knappem Margin)
  forecast_below    : Forecast sicher unter Schwelle → NO kaufen
  ensemble_edge     : Ensemble-Probability vs. Marktpreis-Gap > Schwelle
  min_volume_usd    : SKIP-Filter
  title_not_contains: SKIP-Filter
"""

import logging
import math
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

from feeds.weather_feed import WeatherFeed, _normal_cdf, CITY_STD_F, _is_summer_month

logger = logging.getLogger(__name__)

SYSTEM = "weather"


@dataclass
class WeatherSignal:
    ticker:      str
    rule_name:   str
    side:        str
    action:      str
    price_cents: int
    count:       int
    reason:      str
    system:      str = SYSTEM
    meta:        dict = field(default_factory=dict)
    track:       str = "weather"


def weather_corrected_prob(yes_ask_cents: int) -> float:
    """
    Kalibrierungskorrektur für Wetter-Märkte.

    Wetter-Märkte haben ähnliche Overconfidence-Muster wie Crypto:
      ≥92%   → -3% (Markt überschätzt Sicherheit bei Tail-Risiko)
      ≥85%   → -2%
      73–82% → -1%
    """
    p = yes_ask_cents / 100.0
    if p >= 0.92:
        return max(0.01, p - 0.03)
    if p >= 0.85:
        return max(0.01, p - 0.02)
    if 0.73 <= p <= 0.82:
        return p - 0.01
    return p


def kelly_count(price_cents: int, true_prob_win: float,
                bankroll_usd: float, fraction: float = 0.25,
                min_count: int = 1, max_count: int = 8,
                fee_pct: float = 1.0) -> int:
    """Quarter-Kelly mit Gebühren-Abzug. Gibt 0 zurück wenn kein Edge."""
    cost = price_cents / 100.0
    if cost <= 0 or cost >= 1 or true_prob_win <= 0:
        return min_count
    fee = fee_pct / 100.0
    b = (1.0 - cost) * (1.0 - fee) / cost
    q = 1.0 - true_prob_win
    f_star = (true_prob_win * b - q) / b
    if f_star <= 0:
        return 0
    if f_star * fraction < 0.01:
        return 0
    bet_usd = f_star * fraction * bankroll_usd
    count = int(bet_usd / cost)
    return max(min_count, min(max_count, count))


class WeatherRuleEngine:
    def __init__(self, config: dict):
        self._rules = [
            r for r in config.get("weather_rules", [])
            if r.get("enabled", True)
        ]
        logger.info(f"[Weather/Rules] {len(self._rules)} aktive Regeln geladen")

    def evaluate(self, market: dict, context: dict | None = None,
                 feed: WeatherFeed | None = None) -> list[WeatherSignal]:
        ticker    = market.get("ticker", "")
        yes_ask   = self._price(market, "yes_ask")
        no_ask    = self._price(market, "no_ask")
        volume    = float(market.get("volume_24h_fp", 0) or 0)
        title     = str(market.get("title", "")).lower()
        close_str = market.get("close_time", "")
        hours_left = self._hours_remaining(close_str)
        ctx       = context or {}

        forecast_temp = ctx.get("forecast_temp_f")
        confidence    = ctx.get("forecast_confidence", 0.0)
        spread_f      = ctx.get("ensemble_spread_f", 99.0)
        city          = ctx.get("city", "")
        bankroll      = float(ctx.get("bankroll_usd", 40.0))

        # Threshold aus Ticker lesen (-T oder -B Suffix)
        threshold_val = self._ticker_threshold(ticker)

        # ── SKIP-Filter ──────────────────────────────────────────────
        for rule in self._rules:
            act = rule.get("action", {})
            if act.get("side") != "SKIP":
                continue
            cond = rule.get("condition", {})
            t    = cond.get("type", "")
            if t == "min_volume_usd" and volume < float(cond.get("threshold", 0)):
                return []
            if t == "title_not_contains":
                keyword = str(cond.get("value", "")).lower()
                if keyword in title:
                    return []
            if t == "yes_ask_between":
                low  = int(cond.get("threshold_low", 0))
                high = int(cond.get("threshold_high", 100))
                if yes_ask is not None and low <= yes_ask <= high:
                    return []

        # ── Trading-Regeln ───────────────────────────────────────────
        signals: list[WeatherSignal] = []
        for rule in self._rules:
            act = rule.get("action", {})
            if act.get("side") == "SKIP":
                continue
            sig = self._eval_rule(
                rule, ticker, yes_ask, no_ask, close_str, hours_left,
                forecast_temp, threshold_val, confidence, spread_f,
                city, bankroll, market, feed, ctx,
            )
            if sig:
                signals.append(sig)
        return signals

    def _eval_rule(
        self, rule: dict, ticker: str,
        yes_ask: Optional[int], no_ask: Optional[int],
        close_str: str, hours_left: float,
        forecast_temp: Optional[float], threshold_val: float,
        confidence: float, spread_f: float,
        city: str, bankroll: float,
        market: dict, feed: Optional[WeatherFeed],
        ctx: dict,
    ) -> Optional[WeatherSignal]:
        name   = rule.get("name", "Unbenannt")
        cond   = rule.get("condition", {})
        act    = rule.get("action", {})
        t      = cond.get("type", "")
        side   = act.get("side", "no")
        count  = int(act.get("count", 3))
        offset = int(act.get("limit_offset_cents", 1))

        matched, reason = False, ""

        if t == "ensemble_edge" and feed is not None and yes_ask is not None:
            min_edge     = int(cond.get("min_edge_cents", 15))
            min_hours    = float(cond.get("min_hours_remaining", 2.0))
            max_hours    = float(cond.get("max_hours_remaining", 48))
            min_conf     = float(cond.get("min_confidence", 0.3))
            max_spread   = float(cond.get("max_spread_f", 5.0))
            market_type  = market.get("_market_type", "high")

            if hours_left < min_hours or (max_hours > 0 and hours_left > max_hours):
                return None
            if confidence < min_conf:
                return None
            if spread_f > max_spread:
                return None
            if threshold_val <= 0:
                return None

            # Ensemble-Probability: P(temp >= threshold)
            ens_prob = feed.ensemble_probability(threshold_val, market_type)
            # YES-Ask = Markt-implied P(temp >= threshold) bei "above"-Typ
            market_yes_p = yes_ask / 100.0

            # Edge-Berechnung
            # Wenn Ensemble sagt "wahrscheinlicher als Markt denkt" → YES kaufen
            # Wenn Ensemble sagt "unwahrscheinlicher als Markt denkt" → NO kaufen
            edge_yes = int(round((ens_prob - market_yes_p) * 100))
            edge_no  = -edge_yes

            if edge_yes >= min_edge:
                matched = True
                side    = "yes"
                reason  = (
                    f"Ensemble P={ens_prob:.0%} vs. Markt {yes_ask}¢ | "
                    f"Edge +{edge_yes}¢ | {city} {threshold_val:.0f}°F | "
                    f"σ={spread_f:.1f}°F conf={confidence:.2f}"
                )
            elif edge_no >= min_edge:
                matched = True
                side    = "no"
                reason  = (
                    f"Ensemble P(NO)={1-ens_prob:.0%} vs. NO-Ask {no_ask}¢ | "
                    f"Edge +{edge_no}¢ | {city} {threshold_val:.0f}°F | "
                    f"σ={spread_f:.1f}°F conf={confidence:.2f}"
                )

        elif t == "yes_ask_above" and yes_ask is not None:
            thr       = int(cond.get("threshold", 88))
            min_hours = float(cond.get("min_hours_remaining", 2.0))
            max_hours = float(cond.get("max_hours_remaining", 48))
            min_margin = float(cond.get("forecast_min_margin_f", 3.0))

            if yes_ask > thr:
                if hours_left < min_hours or (max_hours > 0 and hours_left > max_hours):
                    return None
                if forecast_temp is not None and threshold_val > 0:
                    margin = forecast_temp - threshold_val
                    if abs(margin) < min_margin:
                        matched = True
                        side    = "no"
                        reason  = (
                            f"YES {yes_ask}¢ > {thr}¢ aber Forecast {forecast_temp:.1f}°F "
                            f"nur {margin:+.1f}°F von Schwelle {threshold_val:.0f}°F | "
                            f"{city} | conf={confidence:.2f}"
                        )

        elif t == "yes_ask_between" and yes_ask is not None:
            low       = int(cond.get("threshold_low", 73))
            high      = int(cond.get("threshold_high", 82))
            min_hours = float(cond.get("min_hours_remaining", 4.0))
            max_spread = float(cond.get("max_spread_f", 4.0))

            if low <= yes_ask <= high:
                if hours_left < min_hours:
                    return None
                if spread_f > max_spread:
                    return None
                matched = True
                side    = act.get("side", "yes")
                reason  = (
                    f"YES {yes_ask}¢ in [{low}–{high}]¢ | {city} | "
                    f"Forecast {forecast_temp:.1f}°F | conf={confidence:.2f}"
                    if forecast_temp else
                    f"YES {yes_ask}¢ in [{low}–{high}]¢ | {city}"
                )

        if not matched:
            return None

        # Limit-Preis
        if side == "yes":
            px = max(1, min(99, (yes_ask or 50) + offset))
        else:
            px = max(1, min(99, (no_ask or 50) + offset))

        # Kelly Sizing
        if act.get("kelly_sizing"):
            fraction = float(act.get("kelly_fraction", 0.25))
            min_cnt  = int(act.get("min_count", 1))
            max_cnt  = int(act.get("max_count", 8))
            # Bei ensemble_edge kennen wir die echte Probability bereits
            if t == "ensemble_edge" and feed is not None and threshold_val > 0:
                market_type = market.get("_market_type", "high")
                ens_p = feed.ensemble_probability(threshold_val, market_type)
                if side == "yes":
                    true_p = ens_p
                else:
                    true_p = 1.0 - ens_p
            elif side == "no":
                edge_p = no_ask or px
                true_p = 1.0 - weather_corrected_prob(100 - edge_p)
            else:
                edge_p = yes_ask or px
                true_p = weather_corrected_prob(edge_p)
            kelly_c = kelly_count(px, true_p, bankroll, fraction, min_cnt, max_cnt)
            if kelly_c == 0:
                logger.debug(f"[Weather/Kelly] {ticker} – kein Edge bei {px}¢ (true_p={true_p:.2%})")
                return None
            count  = kelly_c
            reason += f" · Kelly={count}ct"

        return WeatherSignal(
            ticker      = ticker,
            rule_name   = name,
            side        = side,
            action      = "buy",
            price_cents = px,
            count       = count,
            reason      = reason,
            meta        = {
                "yes_ask":       yes_ask,
                "no_ask":        no_ask,
                "title":         market.get("title", "")[:80],
                "event_title":   (market.get("event_title") or "")[:120],
                "event_ticker":  market.get("event_ticker", ""),
                "close_time":    close_str,
                "category":      "climate and weather",
                "sub_title":     "",
                "image_url":     (market.get("image_url") or "").strip(),
                "system":        SYSTEM,
                "city":          city,
                "forecast_temp": forecast_temp,
                "threshold":     threshold_val,
                "confidence":    confidence,
            },
        )

    def _price(self, market: dict, key: str) -> Optional[int]:
        v = market.get(key + "_dollars") or market.get(key)
        if v is None:
            return None
        try:
            f = float(v)
            return int(round(f * 100)) if f <= 1.0 else int(round(f))
        except (ValueError, TypeError):
            return None

    def _hours_remaining(self, close_str: str) -> float:
        if not close_str:
            return float("inf")
        try:
            ct = datetime.fromisoformat(close_str.replace("Z", "+00:00"))
            return max(0.0, (ct - datetime.now(timezone.utc)).total_seconds() / 3600)
        except Exception:
            return float("inf")

    def _ticker_threshold(self, ticker: str) -> float:
        for sep in ("-T", "-B"):
            if sep in ticker:
                try:
                    return float(ticker.split(sep)[-1])
                except Exception:
                    pass
        return 0.0
