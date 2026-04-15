"""
15-Minuten-Märkte Rule Engine (System 2 – Crypto).

Eigenständiges Modul, aus crypto/rules.py ausgelagert.

Unterstützte Regel-Typen:
  btc_15min_spot_convergence : Spot-Distance Convergence
"""

import logging
from datetime import datetime, timezone
from typing import Optional

from crypto.models import CryptoSignal, SYSTEM
from utils.kelly import kelly_count
from utils.market import ticker_threshold, hours_remaining as market_hours_remaining, parse_price_cents

logger = logging.getLogger(__name__)


class Crypto15MinRuleEngine:
    """
    Rule Engine für BTC/ETH/SOL/XRP/DOGE/BNB 15-Min-Märkte.

    Unterstützte Regel-Typen:
      - btc_15min_spot_convergence: Spot-Distance Convergence

    Spot-Distance Convergence Logik:
      1. Parse Threshold aus Ticker (z.B. KXBTC15M-...-T71500 → 71500)
      2. Hole BingX Spot-Preis
      3. Berechne signierten Abstand: (spot - threshold) / threshold
      4. Wenn |Abstand| > min_distance_pct UND Restlaufzeit im Fenster:
         - Spot ÜBER Schwelle → kauf YES (markt-in-the-money)
         - Spot UNTER Schwelle → kauf NO (markt-in-the-money)
      5. Kelly-Sizing basierend auf Abstand (größerer Abstand = höhere Konfidenz)
    """

    def __init__(self, config: dict):
        self._rules = [
            r for r in config.get("crypto_15min_rules", [])
            if r.get("enabled", True)
        ]
        logger.info(f"[Crypto/15MinRules] {len(self._rules)} aktive Regeln geladen")

    def evaluate(self, market: dict, context: dict | None = None) -> list[CryptoSignal]:
        ticker  = market.get("ticker", "")
        yes_ask = self._price(market, "yes_ask")
        no_ask  = self._price(market, "no_ask")
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
            cond_type = rule.get("condition", {}).get("type", "")
            if cond_type == "btc_15min_spot_convergence":
                sig = self._eval_spot_convergence(rule, ticker, yes_ask, no_ask, market, ctx)
            else:
                sig = None
            if sig:
                signals.append(sig)
        return signals

    def _eval_spot_convergence(
        self, rule: dict, ticker: str,
        yes_ask: Optional[int], no_ask: Optional[int],
        market: dict, ctx: dict,
    ) -> Optional[CryptoSignal]:
        """
        Spot-Distance Convergence für 15-Min-Märkte.

        Nutzt BingX Spot-Preis vs. Kalshi-Schwelle. Bei großem Abstand
        zum Settlement-Zeitpunkt ist die in-the-money-Seite fast sicher.
        """
        cond = rule.get("condition", {})
        act  = rule.get("action", {})
        name = rule.get("name", "15M Spot-Convergence")

        # Threshold aus Ticker (-T Suffix) oder floor_strike (15M-Märkte ohne -T)
        threshold = ticker_threshold(ticker)
        if threshold <= 0:
            fs = market.get("floor_strike") or market.get("floor_strike_dollars")
            threshold = float(fs) if fs else 0.0
        if threshold <= 0:
            return None

        # Spot-Preis aus BingX-Kontext (spot_price bevorzugt – asset-agnostisch)
        spot = ctx.get("spot_price") or ctx.get("btc_price")
        if not spot or spot <= 0:
            return None

        # Restlaufzeit (in Minuten)
        close_str = market.get("close_time", "")
        mins_left = market_hours_remaining(close_str) * 60 if close_str else 999.0

        min_mins = float(cond.get("min_mins_remaining", 2.0))
        max_mins = float(cond.get("max_mins_remaining", 12.0))
        if mins_left < min_mins or mins_left > max_mins:
            logger.debug(
                f"[15M/Spot] {ticker} außerhalb Zeitfenster: {mins_left:.1f}min "
                f"(erlaubt {min_mins}-{max_mins}min)"
            )
            return None

        # Stunden-Blacklist (UTC): bekannte toxische Zeitfenster
        blocked_hours = cond.get("blocked_hours_utc", [])
        if blocked_hours:
            now_hour = datetime.now(timezone.utc).hour
            if now_hour in blocked_hours:
                logger.debug(
                    f"[15M/Spot] {ticker} blockiert: Stunde {now_hour}:00 UTC auf Blacklist"
                )
                return None

        # Signierter Abstand: positiv = Spot über Schwelle (YES wahrscheinlich)
        distance_pct = (spot - threshold) / threshold * 100
        min_distance_pct = float(cond.get("min_distance_pct", 0.4))

        if abs(distance_pct) < min_distance_pct:
            logger.info(
                f"[15M/Spot] {ticker} Abstand {distance_pct:+.2f}% < {min_distance_pct}% Schwelle "
                f"(spot={spot:.2f} vs thr={threshold:.2f}) – skip"
            )
            return None

        # Seite + Preis bestimmen
        max_yes = int(cond.get("max_yes_price", 88))
        min_yes = int(cond.get("min_yes_price", 12))
        offset  = int(act.get("limit_offset_cents", 1))

        if distance_pct > 0:
            # Spot ist ÜBER Schwelle → YES ist "in-the-money" → kauf YES
            if yes_ask is None or yes_ask >= max_yes or yes_ask < min_yes:
                return None
            side   = "yes"
            edge_p = yes_ask
            px     = max(1, min(99, yes_ask + offset))
        else:
            # Spot ist UNTER Schwelle → NO ist "in-the-money" → kauf NO
            # no_ask < min_yes würde YES > (100 - min_yes)¢ bedeuten → ROI zu gering
            if no_ask is None or no_ask >= max_yes or no_ask < min_yes:
                return None
            side   = "no"
            edge_p = no_ask
            px     = max(1, min(99, no_ask + offset))

        # Echte Wahrscheinlichkeit basierend auf Abstand + Zeit
        # Kalibriert nach 29 Trades: historische YES-WR=41%, NO-WR=71%.
        # Alte Werte (96/92/88/84%) waren zu optimistisch → Kelly gab max_count.
        abs_dist = abs(distance_pct)
        if abs_dist >= 1.0:
            true_p = 0.82
        elif abs_dist >= 0.7:
            true_p = 0.78
        elif abs_dist >= 0.5:
            true_p = 0.74
        else:  # 0.4-0.5%
            true_p = 0.70

        # Zeit-Bonus: bei <5 Min Restlaufzeit ist das Ergebnis fast festgelegt
        if mins_left <= 3:
            true_p = min(0.90, true_p + 0.04)
        elif mins_left <= 5:
            true_p = min(0.90, true_p + 0.02)

        # Kelly-Sizing
        bankroll = float(ctx.get("bankroll_usd", 75.0))
        fraction = float(act.get("kelly_fraction", 0.30))
        min_cnt  = int(act.get("min_count", 1))
        max_cnt  = int(act.get("max_count", 10))
        kelly_c  = kelly_count(edge_p, true_p, bankroll, fraction, min_cnt, max_cnt)

        # Preis-abhängiger Count-Cap: bei hohen Preisen max. 3 Contracts.
        # Verhindert $8.50-Verluste (85¢×10) auf 15-Min-Märkten.
        # Historisch: alle 13 Zeit-Stop-Verluste waren bei ≥75¢ mit Count ≥5.
        if px >= 75:
            kelly_c = min(kelly_c, 3)
        elif px >= 60:
            kelly_c = min(kelly_c, 5)

        if kelly_c == 0:
            logger.debug(f"[15M/Spot] {ticker} kein Edge bei {edge_p}¢ (true_p={true_p:.2%})")
            return None

        reason = (
            f"Spot {spot:,.2f} vs Schwelle {threshold:,.2f} = {distance_pct:+.2f}% | "
            f"{mins_left:.0f}min verbl. | {side.upper()} @ {edge_p}¢ "
            f"(P≈{true_p:.0%}) · Kelly={kelly_c}ct"
        )

        return CryptoSignal(
            ticker      = ticker,
            rule_name   = name,
            side        = side,
            action      = "buy",
            price_cents = px,
            count       = kelly_c,
            reason      = reason,
            meta        = {
                "yes_ask":      yes_ask,
                "no_ask":       no_ask,
                "close_time":   market.get("close_time", ""),
                "title":        market.get("title", "")[:80],
                "spot_price":   spot,
                "threshold":    threshold,
                "distance_pct": round(distance_pct, 3),
                "mins_left":    round(mins_left, 1),
                "system":       SYSTEM,
            },
            track       = "crypto_15min",
        )

    def _price(self, market: dict, key: str) -> Optional[int]:
        return parse_price_cents(market, key)
