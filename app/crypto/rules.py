"""
Crypto Market Regelwerk (System 2).

Verwaltet Entry-Signale für BTC/ETH/SOL/XRP Tages-Leiter und 15-Min-Märkte.

Ladder-Regeln (crypto_ladder_rules):
  yes_ask_above     : YES > Schwelle → NO kaufen
                      Pflicht-Felder: spot_min_overshoot_pct (%), min_hours_remaining
  yes_ask_between   : YES in Zone → YES kaufen
                      Pflicht-Felder: min_hours_remaining, optional require_rsi_oversold
  title_not_contains: SKIP-Filter
  min_volume_usd    : SKIP-Filter
  yes_ask_between (SKIP): 50/50-Filter

15-Min-Regeln (crypto_15min_rules):
  btc_15min_mean_reversion : RSI-gestützte Mean-Reversion
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)

SYSTEM = "crypto"


# ── Kelly Utilities ──────────────────────────────────────────────────── #

def crypto_corrected_yes_prob(yes_ask_cents: int) -> float:
    """
    Kalibrierungskorrektur für Crypto-Threshold-Märkte.

    Crypto-Leiter: BTC/ETH können intraday ±2–3% schwingen.
    Bei >85% YES ist das Tail-Risiko (Threshold-Bruch) systematisch unterschätzt.

      ≥95%   → -4% (extreme Certainty-Overweighting)
      ≥90%   → -3%
      ≥85%   → -2.5%
      73–82% → fast korrekt, -1%
      55–65% → -1%
    """
    p = yes_ask_cents / 100.0
    if p >= 0.95:
        return max(0.01, p - 0.04)
    if p >= 0.90:
        return max(0.01, p - 0.03)
    if p >= 0.85:
        return max(0.01, p - 0.025)
    if 0.73 <= p <= 0.82:
        return p - 0.01
    if 0.55 <= p <= 0.65:
        return p - 0.01
    return p


def kelly_count(price_cents: int, true_prob_win: float,
                bankroll_usd: float, fraction: float = 0.25,
                min_count: int = 1, max_count: int = 15,
                fee_pct: float = 1.0) -> int:
    """Quarter-Kelly Position Sizing mit Gebühren-Abzug.

    fee_pct: Kalshi Settlement-Fee in Prozent (default 1.0 = 1%).
    Reduziert den effektiven Gewinn und damit den Kelly-Faktor.
    Gibt 0 zurück wenn kein positiver Edge nach Gebühren besteht.
    """
    cost = price_cents / 100.0
    if cost <= 0 or cost >= 1 or true_prob_win <= 0:
        return min_count
    fee   = fee_pct / 100.0
    # Netto-Gewinn nach Fee: (1 - cost) * (1 - fee)
    b       = (1.0 - cost) * (1.0 - fee) / cost
    q       = 1.0 - true_prob_win
    f_star  = (true_prob_win * b - q) / b
    if f_star <= 0:
        return 0
    # Untergrenze: wenn Kelly < 1% empfiehlt, kein Trade
    if f_star * fraction < 0.01:
        return 0
    bet_usd = f_star * fraction * bankroll_usd
    count   = int(bet_usd / cost)
    return max(min_count, min(max_count, count))


# ── Signal ───────────────────────────────────────────────────────────── #

@dataclass
class CryptoSignal:
    ticker:      str
    rule_name:   str
    side:        str
    action:      str
    price_cents: int
    count:       int
    reason:      str
    system:      str = SYSTEM
    meta:        dict = field(default_factory=dict)
    track:       str = "crypto"


# ── Rule Engine ──────────────────────────────────────────────────────── #

class CryptoLadderRuleEngine:
    def __init__(self, config: dict):
        self._rules = [
            r for r in config.get("crypto_ladder_rules", [])
            if r.get("enabled", True)
        ]
        logger.info(f"[Crypto/LadderRules] {len(self._rules)} aktive Regeln geladen")

    def evaluate(self, market: dict, context: dict | None = None) -> list[CryptoSignal]:
        ticker    = market.get("ticker", "")
        yes_ask   = self._price(market, "yes_ask")
        yes_bid   = self._price(market, "yes_bid")
        no_ask    = self._price(market, "no_ask")
        volume    = float(market.get("volume_24h_fp", 0) or 0)
        title     = str(market.get("title", "")).lower()
        close_str = market.get("close_time", "")
        hours_left = self._hours_remaining(close_str)
        ctx       = context or {}
        spot      = ctx.get("btc_price")  # Spot-Preis (BTC, ETH, etc.) – key heißt immer btc_price
        rsi       = ctx.get("bingx_rsi")
        bankroll  = float(ctx.get("bankroll_usd", 80.0))

        # ── SKIP-Filter ───────────────────────────────────────────────
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
                    logger.debug(f"[Crypto/Ladder] {ticker} SKIP – Titel enthält '{keyword}'")
                    return []
            if t == "yes_ask_between":
                low  = int(cond.get("threshold_low", 0))
                high = int(cond.get("threshold_high", 100))
                if yes_ask is not None and low <= yes_ask <= high:
                    return []

        # ── Threshold aus Ticker lesen ────────────────────────────────
        threshold_val = self._ticker_threshold(ticker)

        # ── Trading-Regeln ────────────────────────────────────────────
        signals: list[CryptoSignal] = []
        for rule in self._rules:
            act = rule.get("action", {})
            if act.get("side") == "SKIP":
                continue
            sig = self._eval_rule(
                rule, ticker, yes_ask, yes_bid, no_ask,
                close_str, hours_left, spot, rsi, threshold_val, bankroll, market,
                ctx=ctx,
            )
            if sig:
                signals.append(sig)
        return signals

    def _eval_rule(self, rule: dict, ticker: str,
                   yes_ask: Optional[int], yes_bid: Optional[int], no_ask: Optional[int],
                   close_str: str, hours_left: float,
                   spot: Optional[float], rsi: Optional[float],
                   threshold_val: float, bankroll: float,
                   market: dict, ctx: dict | None = None) -> Optional[CryptoSignal]:
        ctx = ctx or {}
        name   = rule.get("name", "Unbenannt")
        cond   = rule.get("condition", {})
        act    = rule.get("action", {})
        t      = cond.get("type", "")
        side   = act.get("side", "no")
        count  = int(act.get("count", 5))
        offset = int(act.get("limit_offset_cents", 1))

        matched, reason = False, ""

        if t == "yes_ask_above":
            thr         = int(cond.get("threshold", 85))
            min_hours   = float(cond.get("min_hours_remaining", 2.0))
            min_over    = float(cond.get("spot_min_overshoot_pct", 1.0)) / 100

            if yes_ask is not None and yes_ask > thr:
                # Zeit-Check: mindestens X Stunden verbleibend
                if hours_left < min_hours:
                    logger.debug(
                        f"[Crypto/Ladder] {ticker} – YES {yes_ask}¢ > {thr}¢ blockiert: "
                        f"nur {hours_left:.1f}h verbleibend (mind. {min_hours}h)"
                    )
                    return None
                # Spot-Distanz-Check: Spot muss > Threshold + X% sein
                if spot and spot > 0 and threshold_val > 0:
                    overshoot = (spot - threshold_val) / threshold_val
                    if overshoot < min_over:
                        logger.debug(
                            f"[Crypto/Ladder] {ticker} – NO-Kauf blockiert: "
                            f"Spot ${spot:,.0f} nur {overshoot:.2%} über Threshold "
                            f"${threshold_val:,.0f} (mind. {min_over:.2%})"
                        )
                        return None
                matched = True
                reason  = (
                    f"YES {yes_ask}¢ > {thr}¢ | {hours_left:.1f}h verbl. | "
                    f"Spot ${spot:,.0f} > Schwelle ${threshold_val:,.0f}"
                    if spot and threshold_val else f"YES {yes_ask}¢ > {thr}¢"
                )
                side = "no"

        elif t == "yes_ask_between":
            low          = int(cond.get("threshold_low", 73))
            high         = int(cond.get("threshold_high", 82))
            min_hours    = float(cond.get("min_hours_remaining", 4.0))
            req_rsi_os   = bool(cond.get("require_rsi_oversold", False))
            rsi_os_thr   = float(cond.get("rsi_oversold_threshold", 40))

            if yes_ask is not None and low <= yes_ask <= high:
                # Zeit-Check
                if hours_left < min_hours:
                    logger.debug(
                        f"[Crypto/Ladder] {ticker} – YES {yes_ask}¢ blockiert: "
                        f"nur {hours_left:.1f}h verbleibend (mind. {min_hours}h)"
                    )
                    return None
                # RSI-Check (wenn Pflicht)
                if req_rsi_os:
                    if rsi is None or rsi > rsi_os_thr:
                        logger.debug(
                            f"[Crypto/Ladder] {ticker} – YES {yes_ask}¢ blockiert: "
                            f"RSI={rsi} nicht überverkauft (< {rsi_os_thr} nötig)"
                        )
                        return None
                matched = True
                reason  = f"YES {yes_ask}¢ in [{low}–{high}]¢ | {hours_left:.1f}h verbl."
                if rsi:
                    reason += f" | RSI={rsi:.0f}"
                side = act.get("side", "yes")

        if not matched:
            return None

        # ── Trend-Filter für YES-Käufe ────────────────────────────────
        if side == "yes":
            trend = ctx.get("bingx_trend")
            if trend and trend == "down":
                logger.debug(
                    f"[Crypto/Ladder] {ticker} – YES-Kauf blockiert: Trend='{trend}' (abwärts)"
                )
                return None

        # ── OB-Imbalance-Filter für NO-Käufe ─────────────────────────
        if side == "no":
            ob_imb = ctx.get("bingx_ob_imbalance")
            if ob_imb and ob_imb > 1.2:
                logger.debug(
                    f"[Crypto/Ladder] {ticker} – NO-Kauf blockiert: "
                    f"OB-Imbalance={ob_imb:.2f} > 1.2 (starker Kaufdruck)"
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
            max_cnt  = int(act.get("max_count", 15))
            if side == "no":
                edge_p = no_ask or px
                true_p = 1.0 - crypto_corrected_yes_prob(100 - edge_p)
            else:
                edge_p = yes_ask or px
                true_p = crypto_corrected_yes_prob(edge_p)
            kelly_c = kelly_count(edge_p, true_p, bankroll, fraction, min_cnt, max_cnt)
            if kelly_c == 0:
                logger.debug(f"[Crypto/Kelly] {ticker} – kein Edge bei {edge_p}¢")
                return None
            count  = kelly_c
            reason += f" · Kelly={count}ct"

        return CryptoSignal(
            ticker      = ticker,
            rule_name   = name,
            side        = side,
            action      = "buy",
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
                "close_time":  market.get("close_time", ""),
                "category":    "crypto",
                "sub_title":   "",
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

    def _ticker_threshold(self, ticker: str) -> float:
        for sep in ("-T", "-B"):
            if sep in ticker:
                try:
                    return float(ticker.split(sep)[-1])
                except Exception:
                    pass
        return 0.0


class Crypto15MinRuleEngine:
    """Eigenständige Rule Engine für BTC/ETH 15-Min Mean-Reversion-Märkte."""

    def __init__(self, config: dict):
        self._rules = [
            r for r in config.get("crypto_15min_rules", [])
            if r.get("enabled", True)
        ]
        logger.info(f"[Crypto/15MinRules] {len(self._rules)} aktive Regeln geladen")

    def evaluate(self, market: dict, context: dict | None = None) -> list[CryptoSignal]:
        ticker  = market.get("ticker", "")
        yes_ask = self._price(market, "yes_ask")
        volume  = float(market.get("volume_24h_fp", 0) or 0)
        ctx     = context or {}

        for rule in self._rules:
            act = rule.get("action", {})
            if act.get("side") != "SKIP":
                continue
            cond = rule.get("condition", {})
            if cond.get("type") == "min_volume_usd":
                if volume < float(cond.get("threshold", 0)):
                    return []

        signals: list[CryptoSignal] = []
        for rule in self._rules:
            act = rule.get("action", {})
            if act.get("side") == "SKIP":
                continue
            sig = self._eval_mean_reversion(rule, ticker, yes_ask, market, ctx)
            if sig:
                signals.append(sig)
        return signals

    def _eval_mean_reversion(self, rule: dict, ticker: str,
                              yes_ask: Optional[int], market: dict,
                              ctx: dict) -> Optional[CryptoSignal]:
        cond       = rule.get("condition", {})
        act        = rule.get("action", {})
        if cond.get("type") != "btc_15min_mean_reversion":
            return None

        change     = ctx.get("btc_change_15min")
        rsi        = ctx.get("bingx_rsi")
        vol_ratio  = ctx.get("bingx_vol_ratio", 1.0)
        if change is None:
            return None

        bias_thr      = int(float(cond.get("bias_threshold", 0.65)) * 100)
        change_thr    = float(cond.get("change_threshold_pct", 0.6))
        rsi_ob        = float(cond.get("rsi_overbought", 72))
        rsi_os        = float(cond.get("rsi_oversold",   28))
        vol_ratio_min = float(cond.get("vol_ratio_min",  1.1))
        count         = int(act.get("count", 5))
        offset        = int(act.get("limit_offset_cents", 1))

        side, matched, reason = "no", False, ""

        if rsi is not None:
            if rsi >= rsi_ob and yes_ask is not None and yes_ask >= bias_thr and vol_ratio >= vol_ratio_min:
                matched = True
                reason  = f"RSI={rsi:.0f} überkauft / UP={yes_ask}¢ → DOWN fade | Vol={vol_ratio:.2f}"
                side    = "no"
            elif rsi <= rsi_os and yes_ask is not None and yes_ask <= (100 - bias_thr) and vol_ratio >= vol_ratio_min:
                matched = True
                reason  = f"RSI={rsi:.0f} überverkauft / UP={yes_ask}¢ → UP fade | Vol={vol_ratio:.2f}"
                side    = "yes"
            else:
                logger.debug(
                    f"[15-Min] {ticker} kein Signal: RSI={rsi:.0f} "
                    f"(OB≥{rsi_ob}, OS≤{rsi_os}), Vol={vol_ratio:.2f} (min {vol_ratio_min})"
                )
        else:
            if change >= change_thr and yes_ask is not None and yes_ask >= bias_thr:
                matched = True
                reason  = f"BTC +{change:.2f}% / UP={yes_ask}¢ → DOWN fade"
                side    = "no"
            elif change <= -change_thr and yes_ask is not None and yes_ask <= (100 - bias_thr):
                matched = True
                reason  = f"BTC {change:.2f}% / UP={yes_ask}¢ → UP fade"
                side    = "yes"

        if not matched:
            return None

        no_ask  = self._price(market, "no_ask")
        if side == "yes":
            px = max(1, min(99, (yes_ask or 50) + offset))
        else:
            px = max(1, min(99, (no_ask or 50) + offset))

        return CryptoSignal(
            ticker      = ticker,
            rule_name   = rule.get("name", "15-Min Mean Reversion"),
            side        = side,
            action      = "buy",
            price_cents = px,
            count       = count,
            reason      = reason,
            meta        = {
                "yes_ask":    yes_ask,
                "no_ask":     no_ask,
                "close_time": market.get("close_time", ""),
                "title":      market.get("title", "")[:80],
                "system":     SYSTEM,
            },
            track       = "crypto_15min",
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
