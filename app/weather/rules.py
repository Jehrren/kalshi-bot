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
import re
from dataclasses import dataclass, field
from typing import Optional

from feeds.weather_feed import WeatherFeed
from utils.kelly import kelly_count
from utils.market import ticker_threshold, hours_remaining as market_hours_remaining, parse_price_cents

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


# kelly_count wird aus utils.kelly importiert (max_count default=8 für Weather-System)


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
            if t == "ticker_not_contains":
                fragment = str(cond.get("value", "")).upper()
                if fragment in ticker.upper():
                    logger.debug(f"[Weather] {ticker} SKIP – Ticker enthält '{fragment}'")
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
        is_bracket = "-B" in ticker

        if t == "ensemble_edge" and feed is not None and yes_ask is not None:
            if is_bracket:  # ensemble_edge berechnet P(≥X), nicht P(bracket) → skip
                return None
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

            # Guard: Nur echte Ensemble-Daten verwenden (≥5 Mitglieder)
            n_members = ctx.get("ensemble_members", 0)
            if n_members < 5:
                logger.debug(
                    f"[Weather/Ensemble] {ticker} – nur {n_members} Ensemble-Mitglieder "
                    f"(< 5), Signal übersprungen"
                )
                return None

            # Richtungskorrektur: "<X°"- oder "below"-Kontrakte → YES = P(temp < X)
            title_text = market.get("title", "").lower()
            # Robuste Erkennung: explizit "< Zahl" oder "below" im Titel
            yes_is_below = bool(re.search(r"<\s*\d", title_text) or " below " in title_text)

            # ensemble_probability() gibt immer P(temp >= threshold) zurück
            # Bei "<"-Kontrakten ist das P(NO), nicht P(YES) → invertieren
            ens_above_prob = feed.ensemble_probability(threshold_val, market_type)
            ens_yes_prob = (1.0 - ens_above_prob) if yes_is_below else ens_above_prob

            market_yes_p = yes_ask / 100.0

            # Edge-Berechnung
            edge_yes = int(round((ens_yes_prob - market_yes_p) * 100))
            edge_no  = -edge_yes

            if edge_yes >= min_edge:
                # YES-Seite deaktiviert: 10.3% WR bei 29 Trades, -$33.44 P&L.
                # Ensemble-Kalibrierung ist systematisch zu optimistisch für YES.
                # Nur NO-Seite hat Edge (57.1% WR).
                logger.debug(
                    f"[Weather/Ensemble] {ticker} YES-Edge +{edge_yes}¢ ignoriert "
                    f"(YES-Seite deaktiviert – historisch 10% WR)"
                )
            elif edge_no >= min_edge:
                matched = True
                side    = "no"
                reason  = (
                    f"Ensemble P(NO)={1-ens_yes_prob:.0%} vs. NO-Ask {no_ask}¢ | "
                    f"Edge +{edge_no}¢ | {city} {threshold_val:.0f}°F | "
                    f"σ={spread_f:.1f}°F conf={confidence:.2f}"
                )

        elif t == "bracket_edge" and feed is not None and yes_ask is not None:
            if not is_bracket:  # bracket_edge nur für Bracket-Märkte (-B Ticker)
                return None
            bracket_width = float(cond.get("bracket_width_f", 2.0))
            min_edge      = int(cond.get("min_edge_cents", 15))
            min_hours     = float(cond.get("min_hours_remaining", 2.0))
            max_hours     = float(cond.get("max_hours_remaining", 48))
            min_conf      = float(cond.get("min_confidence", 0.4))
            max_spread    = float(cond.get("max_spread_f", 3.0))
            market_type   = market.get("_market_type", "high")

            if hours_left < min_hours or (max_hours > 0 and hours_left > max_hours):
                return None
            if confidence < min_conf:
                return None
            if spread_f > max_spread:
                return None
            if threshold_val <= 0:
                return None

            upper_f = threshold_val + bracket_width
            ens_prob     = feed.bracket_probability(threshold_val, upper_f, market_type)
            market_yes_p = yes_ask / 100.0

            edge_yes = int(round((ens_prob - market_yes_p) * 100))
            edge_no  = -edge_yes

            if edge_yes >= min_edge:
                matched = True
                side    = "yes"
                reason  = (
                    f"Bracket P={ens_prob:.0%} vs. Markt {yes_ask}¢ | "
                    f"Edge +{edge_yes}¢ | {city} {threshold_val:.0f}–{upper_f:.0f}°F | "
                    f"σ={spread_f:.1f}°F conf={confidence:.2f}"
                )
            elif edge_no >= min_edge:
                matched = True
                side    = "no"
                reason  = (
                    f"Bracket P(NO)={1-ens_prob:.0%} vs. NO-Ask {no_ask}¢ | "
                    f"Edge +{edge_no}¢ | {city} {threshold_val:.0f}–{upper_f:.0f}°F | "
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

        # Mindest-Payout-Schutz: NO bei 91¢+ → absoluter Gewinn zu gering
        if side == "no":
            max_no_ask = int(act.get("max_no_ask_cents", 0))
            if max_no_ask > 0 and (no_ask or 0) > max_no_ask:
                logger.debug(
                    f"[Weather] {ticker} – NO {no_ask}¢ > max {max_no_ask}¢ "
                    f"(ROI zu gering, Einsatz:{(no_ask or 0)/100:.2f}$, Win:<{(100-(no_ask or 0))/100:.2f}$)"
                )
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
                title_text = market.get("title", "").lower()
                # Konsistente robuste Erkennung (identisch mit Signal-Generierung oben)
                yes_is_below = bool(re.search(r"<\s*\d", title_text) or " below " in title_text)
                ens_above_p = feed.ensemble_probability(threshold_val, market_type)
                ens_yes_p = (1.0 - ens_above_p) if yes_is_below else ens_above_p
                true_p = ens_yes_p if side == "yes" else (1.0 - ens_yes_p)
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
        return parse_price_cents(market, key)

    def _hours_remaining(self, close_str: str) -> float:
        return market_hours_remaining(close_str)

    def _ticker_threshold(self, ticker: str) -> float:
        return ticker_threshold(ticker)
