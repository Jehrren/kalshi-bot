"""
Regelbasierte Strategien für Kalshi Prediction Markets.

Preise in Cent (1–99). 50¢ = 50% Wahrscheinlichkeit.

Bedingungstypen:
  yes_ask_below       : Kaufe YES wenn Ask < Schwelle (Cent)
  yes_ask_above       : Kaufe NO  wenn YES-Ask > Schwelle (Cent)
  no_ask_below        : Kaufe NO  wenn NO-Ask < Schwelle (Cent)
  spread_wide         : Handle wenn Spread > Schwelle (Cent)
  min_open_interest_usd : Filter – überspringe wenn Open Interest < Schwelle
  min_volume_usd      : Filter – überspringe wenn 24h-Volumen < Schwelle
  ticker_filter       : Filter – nur bestimmte Ticker-Prefixe handeln
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------ #
#  Kelly Sizing Utilities                                              #
# ------------------------------------------------------------------ #

def bias_corrected_yes_prob(yes_ask_cents: int) -> float:
    """
    Schätzt die wahre YES-Wahrscheinlichkeit nach Korrektur des
    Overconfidence-Bias (Longshot-Effekt / Certainty-Overweighting).

    Kalshi BTC-Tages-Ladder: Systematische Überschätzung von Sicherheit.
    BTC kann intraday ±2-3% schwingen → Märkte bei 95%+ unterschätzen Tail-Risiko.

      ≥95%   → 3% Überschätzung (einheitlich für 95–100%)
      90–95% → 4% Überschätzung (stärkste Zone)
      85–90% → 3% Überschätzung
      73–82% → 1% Unterschätzung (gut kalibrierte Zone)
      55–65% → 1% Überschätzung (kleiner Bias)
    """
    p = yes_ask_cents / 100.0
    if p >= 0.95:
        return p - 0.03   # Einheitlich für 95–100%: Tail-Risiko unterschätzt
    if p >= 0.90:
        return p - 0.04   # Stärkster Edge-Bereich
    if p >= 0.85:
        return p - 0.03
    if 0.73 <= p <= 0.82:
        return p + 0.01   # gut kalibriert → leicht unterbewertet
    if 0.55 <= p <= 0.65:
        return p - 0.01   # kleiner Bias
    return p


def kelly_count(price_cents: int, true_prob_win: float,
                bankroll_usd: float, fraction: float = 0.25,
                min_count: int = 1, max_count: int = 20) -> int:
    """
    Fractional Kelly Position Sizing.

    price_cents   : Was wir pro Contract zahlen (z.B. NO zu 7¢)
    true_prob_win : Geschätzte Wahrscheinlichkeit, dass unsere Seite gewinnt
    bankroll_usd  : Verfügbares Kapital
    fraction      : Kelly-Fraktion (0.25 = Quarter-Kelly → konservativ)

    Formel: f* = (p × b − q) / b  wobei b = Gewinn/Einsatz = (100-p_cents)/p_cents
    """
    cost = price_cents / 100.0
    if cost <= 0 or cost >= 1 or true_prob_win <= 0:
        return min_count
    b = (1.0 - cost) / cost          # Auszahlungsverhältnis
    q = 1.0 - true_prob_win
    f_star = (true_prob_win * b - q) / b
    if f_star <= 0:
        logger.debug(
            f"[Kelly] Kein Edge: p_win={true_prob_win:.3f}, b={b:.2f}, f*={f_star:.4f}"
        )
        return 0
    bet_usd = f_star * fraction * bankroll_usd
    count   = int(bet_usd / cost)
    result  = max(min_count, min(max_count, count))
    logger.debug(
        f"[Kelly] p_win={true_prob_win:.3f} b={b:.2f} f*={f_star:.2%} "
        f"× {fraction:.0%} × ${bankroll_usd:.0f} = ${bet_usd:.2f} / {cost:.2f} = {count} → {result}"
    )
    return result


@dataclass
class Signal:
    ticker: str
    rule_name: str
    side: str           # "yes" | "no"
    action: str         # "buy" | "sell"
    price_cents: int    # Limit-Preis in Cent
    count: int          # Anzahl Contracts
    reason: str
    meta: dict = field(default_factory=dict)


class RuleEngine:
    def __init__(self, config: dict, rule_key: str = "rules"):
        self._rules = [r for r in config.get(rule_key, []) if r.get("enabled", True)]
        logger.info(f"[Rules/{rule_key}] {len(self._rules)} aktive Regeln geladen")

    def evaluate(self, market: dict, context: dict | None = None) -> list[Signal]:
        """
        Evaluiert alle Regeln gegen einen Kalshi-Markt.
        market = dict aus der API (ticker, yes_ask, yes_bid, no_ask, no_bid, ...)
        """
        ticker = market.get("ticker", "")

        # Preise aus API (in Cent)
        yes_ask = self._price(market, "yes_ask")
        yes_bid = self._price(market, "yes_bid")
        no_ask  = self._price(market, "no_ask")
        no_bid  = self._price(market, "no_bid")
        spread  = (yes_ask - yes_bid) if yes_ask and yes_bid else None

        # Volumen / Open Interest
        volume_usd = float(market.get("volume_24h_fp", 0) or 0)
        oi_usd     = float(market.get("open_interest_fp", 0) or 0)

        # Filter-Regeln zuerst
        for rule in self._rules:
            if rule.get("action", {}).get("side") != "SKIP":
                continue
            cond = rule.get("condition", {})
            if cond.get("type") == "yes_ask_between":
                low  = int(cond.get("threshold_low", 0))
                high = int(cond.get("threshold_high", 100))
                if yes_ask is not None and low <= yes_ask <= high:
                    logger.debug(f"[Rules] {ticker} SKIP (yes_ask={yes_ask}¢ in [{low}–{high}])")
                    return []
                continue
            if not self._check_filter(cond, ticker, volume_usd, oi_usd):
                logger.debug(f"[Rules] {ticker} SKIP durch '{rule.get('name')}'")
                return []

        # Trading-Regeln
        signals: list[Signal] = []
        for rule in self._rules:
            act = rule.get("action", {})
            if act.get("side") == "SKIP":
                continue
            sig = self._eval_rule(rule, ticker, yes_ask, yes_bid, no_ask, no_bid, spread, market, context)
            if sig:
                signals.append(sig)

        return signals

    def _price(self, market: dict, key: str) -> Optional[int]:
        """Extrahiert Preis in Cent aus den verschiedenen API-Formaten."""
        # API gibt manchmal USD-String, manchmal Cent-Int
        v = market.get(key + "_dollars") or market.get(key)
        if v is None:
            return None
        try:
            f = float(v)
            # Wenn < 1.0 → USD-Format → in Cent umrechnen
            return int(round(f * 100)) if f <= 1.0 else int(round(f))
        except (ValueError, TypeError):
            return None

    def _check_filter(self, cond: dict, ticker: str, volume: float, oi: float) -> bool:
        t = cond.get("type", "")
        if t == "min_open_interest_usd":
            return oi >= float(cond.get("threshold", 0))
        if t == "min_volume_usd":
            return volume >= float(cond.get("threshold", 0))
        if t == "ticker_filter":
            prefixes = cond.get("prefixes", [])
            return not prefixes or any(ticker.startswith(p) for p in prefixes)
        return True

    def _eval_rule(self, rule: dict, ticker: str,
                   yes_ask: Optional[int], yes_bid: Optional[int],
                   no_ask: Optional[int], no_bid: Optional[int],
                   spread: Optional[int], market: dict,
                   context: dict | None = None) -> Optional[Signal]:
        name    = rule.get("name", "Unbenannt")
        cond    = rule.get("condition", {})
        act     = rule.get("action", {})
        t       = cond.get("type", "")
        side    = act.get("side", "yes")          # "yes" | "no"
        action  = act.get("action", "buy")        # "buy" | "sell"
        count   = int(act.get("count", 5))
        offset  = int(act.get("limit_offset_cents", 1))
        otype   = act.get("order_type", "limit")

        matched, reason = False, ""

        if t == "yes_ask_below":
            thr = int(cond.get("threshold", 10))
            if yes_ask is not None and yes_ask < thr:
                matched = True
                reason  = f"YES ask {yes_ask}¢ < {thr}¢"

        elif t == "yes_ask_above":
            thr = int(cond.get("threshold", 90))
            if yes_ask is not None and yes_ask > thr:
                matched = True
                reason  = f"YES ask {yes_ask}¢ > {thr}¢"
                side, action = "no", "buy"

        elif t == "no_ask_below":
            thr = int(cond.get("threshold", 10))
            if no_ask is not None and no_ask < thr:
                matched = True
                reason  = f"NO ask {no_ask}¢ < {thr}¢"
                side, action = "no", "buy"

        elif t == "yes_ask_between":
            low  = int(cond.get("threshold_low", 0))
            high = int(cond.get("threshold_high", 100))
            if yes_ask is not None and low <= yes_ask <= high:
                matched = True
                reason  = f"YES ask {yes_ask}¢ in [{low}¢–{high}¢]"

        elif t == "btc_15min_mean_reversion":
            # Faded Retail-Überreaktion nach starker BTC-Bewegung.
            # Primär: RSI-Signal von BingX (präziser)
            # Fallback: Nur Preis-Änderung wenn BingX nicht bereit
            ctx        = context or {}
            change     = ctx.get("btc_change_15min")
            rsi        = ctx.get("bingx_rsi")
            vol_ratio  = ctx.get("bingx_vol_ratio", 1.0)
            if change is None:
                return None

            bias_thr   = int(float(cond.get("bias_threshold", 0.65)) * 100)
            change_thr = float(cond.get("change_threshold_pct", 0.3))
            rsi_ob     = float(cond.get("rsi_overbought", 68))   # RSI > X = überkauft
            rsi_os     = float(cond.get("rsi_oversold",   32))   # RSI < X = überverkauft

            # Mit RSI: präziseres Signal (RSI bestätigt Preis-Bewegung)
            if rsi is not None:
                if (rsi >= rsi_ob and yes_ask is not None and yes_ask >= bias_thr
                        and vol_ratio >= 0.8):
                    matched = True
                    reason  = (f"RSI={rsi:.0f} überkauft / UP={yes_ask}¢ ≥ {bias_thr}¢ "
                               f"/ Vol×{vol_ratio:.1f} → DOWN fade")
                    side, action = "no", "buy"
                elif (rsi <= rsi_os and yes_ask is not None and yes_ask <= (100 - bias_thr)
                        and vol_ratio >= 0.8):
                    matched = True
                    reason  = (f"RSI={rsi:.0f} überverkauft / UP={yes_ask}¢ ≤ {100-bias_thr}¢ "
                               f"/ Vol×{vol_ratio:.1f} → UP fade")
                    side, action = "yes", "buy"
            else:
                # Fallback: nur Preis-Änderung (BingX noch nicht bereit)
                if change >= change_thr and yes_ask is not None and yes_ask >= bias_thr:
                    matched = True
                    reason  = f"BTC +{change:.2f}% / UP={yes_ask}¢ ≥ {bias_thr}¢ → DOWN fade"
                    side, action = "no", "buy"
                elif change <= -change_thr and yes_ask is not None and yes_ask <= (100 - bias_thr):
                    matched = True
                    reason  = f"BTC {change:.2f}% / UP={yes_ask}¢ ≤ {100-bias_thr}¢ → UP fade"
                    side, action = "yes", "buy"

        elif t == "time_decay_no":
            min_days = float(cond.get("min_days_remaining", 14))
            ask_min  = int(cond.get("yes_ask_min", 18))
            ask_max  = int(cond.get("yes_ask_max", 62))
            ct_str   = market.get("close_time", "")
            if yes_ask is not None and ask_min <= yes_ask <= ask_max and ct_str:
                try:
                    ct   = datetime.fromisoformat(ct_str.replace("Z", "+00:00"))
                    days = (ct - datetime.now(timezone.utc)).days
                    if days >= min_days:
                        matched = True
                        reason  = f"Time-Decay: YES={yes_ask}¢ | {days}d verbleibend → tägl. Verfall"
                        side, action = "no", "buy"
                except Exception:
                    pass

        elif t == "spread_wide":
            thr = int(cond.get("threshold", 5))
            if spread is not None and spread > thr:
                matched = True
                reason  = f"Spread {spread}¢ > {thr}¢"

        if not matched:
            return None

        # Limit-Preis bestimmen
        if otype == "limit":
            if side == "yes":
                px = (yes_ask or 50) + offset
            else:
                px = (no_ask or 50) + offset
            px = max(1, min(99, px))
        else:
            px = yes_ask or no_ask or 50

        # Kelly Sizing (optional, wenn in der Regel aktiviert)
        if act.get("kelly_sizing") and context:
            bankroll  = float(context.get("bankroll_usd", 200.0))
            fraction  = float(act.get("kelly_fraction", 0.25))
            min_cnt   = int(act.get("min_count", 1))
            max_cnt   = int(act.get("max_count", 20))
            true_yes  = bias_corrected_yes_prob(yes_ask or 50)
            # Edge auf Marktpreis (ask) berechnen, nicht auf Limit-Preis (ask+offset).
            # Der Offset verbessert Fill-Wahrscheinlichkeit, aber die Edge basiert
            # auf dem Preis den der Markt anbietet, nicht auf unserem Aufschlag.
            if side == "no":
                edge_price = no_ask or px
                true_p_win = 1.0 - true_yes
                kelly_c = kelly_count(edge_price, true_p_win, bankroll, fraction, min_cnt, max_cnt)
            else:
                edge_price = yes_ask or px
                kelly_c = kelly_count(edge_price, true_yes, bankroll, fraction, min_cnt, max_cnt)
            if kelly_c == 0:
                logger.debug(f"[Rules/Kelly] {ticker} – kein Edge bei {edge_price}¢, Signal verworfen")
                return None
            count = kelly_c
            reason += f" · Kelly={count}ct ({edge_price}¢ → f={fraction:.0%})"

        return Signal(
            ticker=ticker,
            rule_name=name,
            side=side,
            action=action,
            price_cents=px,
            count=count,
            reason=reason,
            meta={
                "yes_ask": yes_ask, "yes_bid": yes_bid,
                "no_ask": no_ask, "no_bid": no_bid,
                "spread": spread,
                "title": market.get("title", "")[:80],
                "event_ticker": market.get("event_ticker", ""),
                "close_time": market.get("close_time", ""),
                "category":  (market.get("category") or "").lower(),
                "sub_title": (market.get("sub_title") or "").strip(),
                "image_url": (market.get("image_url") or "").strip(),
            },
        )
