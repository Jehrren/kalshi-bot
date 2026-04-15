"""
Market-Utilities – shared helper functions für Kalshi-Markt-Daten.

Funktionen die in mehreren Rule-Engines und Scannern identisch verwendet werden.
"""

from datetime import datetime, timezone
from typing import Optional


def ticker_threshold(ticker: str) -> float:
    """Parst numerische Schwelle aus Ticker-Symbol.

    Beispiele:
      KXBTC-T71500   → 71500.0
      KXETH-B3000    → 3000.0
      KXWTMP-LA-T85  → 85.0

    Returns:
        Schwellen-Wert als float, 0.0 wenn nicht parsebar.
    """
    for sep in ("-T", "-B"):
        if sep in ticker:
            try:
                return float(ticker.split(sep)[-1])
            except Exception:
                pass
    return 0.0


def hours_remaining(close_str: str) -> float:
    """Berechnet verbleibende Stunden bis Marktschluss.

    Args:
        close_str: ISO-8601 Zeitstring (z.B. '2026-04-14T20:00:00Z').

    Returns:
        Stunden bis close_time, float('inf') bei fehlendem/ungültigem Input.
    """
    if not close_str:
        return float("inf")
    try:
        ct = datetime.fromisoformat(close_str.replace("Z", "+00:00"))
        return max(0.0, (ct - datetime.now(timezone.utc)).total_seconds() / 3600)
    except Exception:
        return float("inf")


def parse_price_cents(market: dict, key: str) -> Optional[int]:
    """Liest Marktpreis und normalisiert auf Cents (1–99).

    Unterstützt sowohl Dollar-Brüche (0.85) als auch Cent-Ganzzahlen (85).

    Args:
        market: Kalshi-Markt-Dict.
        key:    Preis-Feld ohne Suffix (z.B. 'yes_ask').

    Returns:
        Preis in Cents oder None wenn nicht verfügbar.
    """
    v = market.get(key + "_dollars") or market.get(key)
    if v is None:
        return None
    try:
        f = float(v)
        return int(round(f * 100)) if f <= 1.0 else int(round(f))
    except (ValueError, TypeError):
        return None
