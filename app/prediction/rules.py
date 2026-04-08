"""
Prediction Market Regelwerk (System 1).

Basiert auf Polymarket-Kalibrierungsdaten:
  - Märkte sind im Schnitt 2% zu optimistisch (YES überschätzt)
  - Stärkster Bias: >90% YES (3–5% Überschätzung) und 50/50-Zone (7%)
  - Beste Kalibrierung: 73–82% YES → fast perfekt, Trend folgen
  - NO ist bei 58–70% oft leicht unterbewertet

Bedingungstypen:
  yes_ask_above       : YES ask > Schwelle → NO kaufen
  yes_ask_between     : YES ask in [low, high] → YES oder NO kaufen
                        Optionales Feld: min_hours_remaining (Float) → Filter
  time_decay_no       : YES 18–62¢ mit langer Laufzeit → NO kaufen
  min_volume_usd      : SKIP-Filter
  min_open_interest_usd: SKIP-Filter
  title_not_contains  : SKIP-Filter
  ticker_not_contains : SKIP-Filter (prüft Ticker-String, z.B. "-B" für Bracket)
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)

SYSTEM = "prediction"


# ── Kelly Utilities ──────────────────────────────────────────────────── #

def polymarket_corrected_prob(yes_ask_cents: int) -> float:
    """
    Korrigiert den systematischen Overconfidence-Bias auf Kalshi/Polymarket.

    Kalibrierungsdaten (Polymarket-Forschung):
      >90%    → -3% bis -5% Überschätzung (Certainty-Overweighting)
      73–82%  → fast perfekt kalibriert (±1%)
      58–70%  → ~2% Überschätzung
      43–57%  → bis zu 7% Überschätzung (schlechteste Zone)

    Globale Korrektur: -2% auf alle Werte als Basislinie.
    """
    p = yes_ask_cents / 100.0
    if p >= 0.90:
        return max(0.01, p - 0.04)
    if p >= 0.73:
        return p - 0.01   # fast perfekt – minimale Korrektur
    if p >= 0.58:
        return p - 0.02
    return max(0.01, p - 0.02)  # Basiskorrektur


def kelly_count(price_cents: int, true_prob_win: float,
                bankroll_usd: float, fraction: float = 0.25,
                min_count: int = 1, max_count: int = 10,
                fee_pct: float = 1.0) -> int:
    """Quarter-Kelly Position Sizing mit Gebührenabzug. Gibt 0 zurück wenn kein positiver Edge."""
    cost = price_cents / 100.0
    if cost <= 0 or cost >= 1 or true_prob_win <= 0:
        return min_count
    fee    = fee_pct / 100.0
    b      = (1.0 - cost) * (1.0 - fee) / cost
    q      = 1.0 - true_prob_win
    f_star = (true_prob_win * b - q) / b
    if f_star <= 0:
        return 0
    if f_star * fraction < 0.01:
        return 0
    bet_usd = f_star * fraction * bankroll_usd
    count   = int(bet_usd / cost)
    return max(min_count, min(max_count, count))


# ── Signal ───────────────────────────────────────────────────────────── #

@dataclass
class PredictionSignal:
    ticker:      str
    rule_name:   str
    side:        str        # "yes" | "no"
    action:      str        # "buy" | "sell"
    price_cents: int
    count:       int
    reason:      str
    system:      str = SYSTEM
    meta:        dict = field(default_factory=dict)
    track:       str = "prediction"


# ── Rule Engine ──────────────────────────────────────────────────────── #

class PredictionRuleEngine:
    def __init__(self, config: dict):
        self._rules = [
            r for r in config.get("prediction_rules", [])
            if r.get("enabled", True)
        ]
        logger.info(f"[Prediction/Rules] {len(self._rules)} aktive Regeln geladen")

    def evaluate(self, market: dict, bankroll_usd: float = 200.0) -> list[PredictionSignal]:
        ticker    = market.get("ticker", "")
        yes_ask   = self._price(market, "yes_ask")
        yes_bid   = self._price(market, "yes_bid")
        no_ask    = self._price(market, "no_ask")
        volume    = float(market.get("volume_24h_fp", 0) or 0)
        oi        = float(market.get("open_interest_fp", 0) or 0)
        close_str = market.get("close_time", "")
        title     = str(market.get("title", "")).lower()
        hours_left = self._hours_remaining(close_str)

        # ── SKIP-Filter zuerst ────────────────────────────────────────
        for rule in self._rules:
            act = rule.get("action", {})
            if act.get("side") != "SKIP":
                continue
            cond = rule.get("condition", {})
            t    = cond.get("type", "")
            if t == "min_volume_usd" and volume < float(cond.get("threshold", 0)):
                logger.debug(f"[Prediction] {ticker} SKIP – Volumen ${volume:.0f} < ${cond['threshold']}")
                return []
            if t == "min_open_interest_usd" and oi < float(cond.get("threshold", 0)):
                logger.debug(f"[Prediction] {ticker} SKIP – OI ${oi:.0f} < ${cond['threshold']}")
                return []
            if t == "title_not_contains":
                keyword = str(cond.get("value", "")).lower()
                if keyword in title:
                    logger.debug(f"[Prediction] {ticker} SKIP – Titel enthält '{keyword}'")
                    return []
            if t == "ticker_not_contains":
                fragment = str(cond.get("value", "")).upper()
                if fragment in ticker.upper():
                    logger.debug(f"[Prediction] {ticker} SKIP – Ticker enthält '{fragment}'")
                    return []
            if t == "yes_ask_between":
                low  = int(cond.get("threshold_low", 0))
                high = int(cond.get("threshold_high", 100))
                if yes_ask is not None and low <= yes_ask <= high:
                    logger.debug(f"[Prediction] {ticker} SKIP – YES ask {yes_ask}¢ in [{low}–{high}]")
                    return []

        # ── Trading-Regeln ────────────────────────────────────────────
        signals: list[PredictionSignal] = []
        for rule in self._rules:
            act = rule.get("action", {})
            if act.get("side") == "SKIP":
                continue
            sig = self._eval_rule(rule, ticker, yes_ask, yes_bid, no_ask,
                                  close_str, hours_left, bankroll_usd, market)
            if sig:
                signals.append(sig)
        return signals

    def _eval_rule(self, rule: dict, ticker: str,
                   yes_ask: Optional[int], yes_bid: Optional[int], no_ask: Optional[int],
                   close_str: str, hours_left: float,
                   bankroll_usd: float, market: dict) -> Optional[PredictionSignal]:
        name   = rule.get("name", "Unbenannt")
        cond   = rule.get("condition", {})
        act    = rule.get("action", {})
        t      = cond.get("type", "")
        side   = act.get("side", "yes")
        action = "buy"
        count  = int(act.get("count", 5))
        offset = int(act.get("limit_offset_cents", 1))

        matched, reason = False, ""

        if t == "yes_ask_above":
            thr       = int(cond.get("threshold", 90))
            thr_max   = int(cond.get("threshold_max", 100))
            min_hours = float(cond.get("min_hours_remaining", 0.0))
            if yes_ask is not None and thr < yes_ask <= thr_max:
                # Bug-Fix: Markt muss genug Restlaufzeit haben (sonst bereits abgerechnet)
                if min_hours > 0 and hours_left < min_hours:
                    logger.debug(
                        f"[Prediction] {ticker} – YES {yes_ask}¢ > {thr}¢ blockiert: "
                        f"nur {hours_left:.1f}h verbleibend (mind. {min_hours}h nötig)"
                    )
                    return None
                matched = True
                reason  = f"YES ask {yes_ask}¢ > {thr}¢ (≤ {thr_max}¢) → NO"
                side    = "no"

        elif t == "yes_ask_between":
            low       = int(cond.get("threshold_low", 0))
            high      = int(cond.get("threshold_high", 100))
            min_hours = float(cond.get("min_hours_remaining", 0.0))
            if yes_ask is not None and low <= yes_ask <= high:
                if min_hours > 0 and hours_left < min_hours:
                    logger.debug(
                        f"[Prediction] {ticker} – YES {yes_ask}¢ passt Regel '{name}', "
                        f"aber nur {hours_left:.1f}h verbleibend (mind. {min_hours}h nötig)"
                    )
                    return None
                matched = True
                reason  = f"YES ask {yes_ask}¢ in [{low}–{high}]¢"
                side = act.get("side", "yes")

        elif t == "time_decay_no":
            ask_min  = int(cond.get("yes_ask_min", 18))
            ask_max  = int(cond.get("yes_ask_max", 62))
            min_days = float(cond.get("min_days_remaining", 14))
            if yes_ask is not None and ask_min <= yes_ask <= ask_max:
                try:
                    ct   = datetime.fromisoformat(close_str.replace("Z", "+00:00"))
                    days = (ct - datetime.now(timezone.utc)).days
                    if days >= min_days:
                        matched = True
                        reason  = f"Time-Decay: YES={yes_ask}¢ | {days}d verbleibend"
                        side    = "no"
                except Exception:
                    pass

        elif t == "overreaction":
            delta       = int(market.get("_overreaction_delta", 0))
            min_delta   = int(cond.get("delta_cents", 8))
            overbought  = int(cond.get("overbought_threshold", 68))
            oversold    = int(cond.get("oversold_threshold", 32))
            if delta >= min_delta and yes_ask is not None and yes_ask >= overbought:
                matched = True
                side    = "no"
                reason  = f"Überreaktion: YES {yes_ask}¢ (+{delta}¢) → NO kontrarian"
            elif delta <= -min_delta and yes_ask is not None and yes_ask <= oversold:
                matched = True
                side    = "yes"
                reason  = f"Überreaktion: YES {yes_ask}¢ (-{abs(delta)}¢) → YES kontrarian"

        if not matched:
            return None

        # Limit-Preis
        if side == "yes":
            px = max(1, min(99, (yes_ask or 50) + offset))
        else:
            px = max(1, min(99, (no_ask or 50) + offset))

        # Kelly Sizing (wenn in Regel aktiviert)
        if act.get("kelly_sizing"):
            fraction  = float(act.get("kelly_fraction", 0.25))
            min_cnt   = int(act.get("min_count", 1))
            max_cnt   = int(act.get("max_count", 10))
            if side == "no":
                edge_p = no_ask or px
                true_p = 1.0 - polymarket_corrected_prob(100 - edge_p)
            else:
                edge_p = yes_ask or px
                true_p = polymarket_corrected_prob(edge_p)
            kelly_c = kelly_count(edge_p, true_p, bankroll_usd, fraction, min_cnt, max_cnt)
            if kelly_c == 0:
                logger.debug(f"[Prediction/Kelly] {ticker} – kein Edge bei {edge_p}¢, Signal verworfen")
                return None
            count  = kelly_c
            reason += f" · Kelly={count}ct"

        return PredictionSignal(
            ticker      = ticker,
            rule_name   = name,
            side        = side,
            action      = action,
            price_cents = px,
            count       = count,
            reason      = reason,
            meta        = {
                "yes_ask":     yes_ask,
                "yes_bid":     yes_bid,
                "no_ask":      no_ask,
                "title":       market.get("title", "")[:80],
                "event_title": (market.get("event_title") or "")[:120],
                "event_ticker":market.get("event_ticker", ""),
                "close_time":  close_str,
                "category":    (market.get("category") or "").lower(),
                "sub_title":   (market.get("sub_title") or "").strip(),
                "image_url":   (market.get("image_url") or "").strip(),
                "system":      SYSTEM,
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
