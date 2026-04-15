"""
Kelly-Criterion Position Sizing – shared utility.

Beide Handelssysteme (Crypto + Weather) verwenden dieselbe Quarter-Kelly-Logik.
"""


def kelly_count(
    price_cents: int,
    true_prob_win: float,
    bankroll_usd: float,
    fraction: float = 0.25,
    min_count: int = 1,
    max_count: int = 15,
    fee_pct: float = 1.0,
) -> int:
    """Quarter-Kelly Position Sizing mit Kalshi-Gebühren-Abzug.

    Args:
        price_cents:   Kaufpreis in Cents (1–99).
        true_prob_win: Geschätzte Gewinn-Wahrscheinlichkeit (0–1).
        bankroll_usd:  Verfügbares Kapital in USD.
        fraction:      Kelly-Fraktion (default 0.25 = Quarter-Kelly).
        min_count:     Minimum Contracts bei positivem Edge.
        max_count:     Maximum Contracts.
        fee_pct:       Kalshi Settlement-Fee in % des Payouts (default 1.0).

    Returns:
        Anzahl Contracts. 0 wenn kein positiver Edge oder ungültige Inputs.
    """
    cost = price_cents / 100.0
    # Ungültige Inputs → kein Trade
    if cost <= 0 or cost >= 1 or true_prob_win <= 0:
        return 0
    fee = fee_pct / 100.0
    # Netto-Gewinn nach Fee: (1 - cost) * (1 - fee)
    b = (1.0 - cost) * (1.0 - fee) / cost
    q = 1.0 - true_prob_win
    f_star = (true_prob_win * b - q) / b
    if f_star <= 0:
        return 0
    if f_star * fraction < 0.001:
        return 0
    bet_usd = f_star * fraction * bankroll_usd
    count = int(bet_usd / cost)
    return max(min_count, min(max_count, count))
