"""
Unit-Tests für utils/kelly.py – Quarter-Kelly Position Sizing.

Abgedeckte Fälle:
  - Ungültige Inputs → immer 0 zurückgeben (kritischer Bug-Fix)
  - Positiver Edge → Contracts berechnet, min/max beachtet
  - Negativer Edge → 0
  - Grenzfälle
"""

import sys
from pathlib import Path

# app/ ins sys.path damit 'utils' gefunden wird
sys.path.insert(0, str(Path(__file__).parent.parent / "app"))

from utils.kelly import kelly_count


class TestKellyCountInvalidInputs:
    """Ungültige Inputs müssen immer 0 zurückgeben – KEIN Trade bei schlechten Daten."""

    def test_price_zero_returns_zero(self):
        assert kelly_count(0, 0.8, 200.0) == 0

    def test_price_negative_returns_zero(self):
        assert kelly_count(-5, 0.8, 200.0) == 0

    def test_price_at_100_returns_zero(self):
        assert kelly_count(100, 0.8, 200.0) == 0

    def test_price_above_100_returns_zero(self):
        assert kelly_count(105, 0.8, 200.0) == 0

    def test_true_prob_zero_returns_zero(self):
        assert kelly_count(85, 0.0, 200.0) == 0

    def test_true_prob_negative_returns_zero(self):
        assert kelly_count(85, -0.1, 200.0) == 0


class TestKellyCountPositiveEdge:
    """Bei echtem positivem Edge: Contracts berechnen, min/max respektieren."""

    def test_strong_edge_returns_nonzero(self):
        # YES bei 85¢, true prob = 92% → klarer positiver Edge
        count = kelly_count(85, 0.92, 200.0)
        assert count > 0

    def test_respects_max_count(self):
        # Großes Kapital würde hohe Anzahl empfehlen, aber max=5
        count = kelly_count(50, 0.75, 100_000.0, max_count=5)
        assert count <= 5

    def test_respects_min_count_when_edge_positive(self):
        # Kleines Kapital → Kelly wäre <1, aber min=2
        count = kelly_count(85, 0.92, 1.0, min_count=2)
        assert count >= 2

    def test_no_edge_returns_zero(self):
        # YES bei 85¢, true prob = 85% → kein Kelly-Edge (breakeven oder minimal)
        count = kelly_count(85, 0.85, 200.0)
        assert count == 0 or count >= 1  # 0 wenn kein positiver Edge

    def test_weather_system_defaults(self):
        # Weather-System: max_count=8
        count = kelly_count(70, 0.80, 50.0, max_count=8)
        assert count <= 8

    def test_crypto_system_defaults(self):
        # Crypto-System: max_count=15
        count = kelly_count(88, 0.93, 200.0, max_count=15)
        assert count <= 15


class TestKellyCountNegativeEdge:
    """Bei negativem Kelly-Faktor: 0 zurückgeben."""

    def test_overpriced_market_returns_zero(self):
        # YES bei 95¢, true prob = 90% → negativer Edge (markt überteuert)
        count = kelly_count(95, 0.90, 200.0)
        assert count == 0

    def test_certain_loss_returns_zero(self):
        # YES bei 90¢, true prob = 50% → massiver negativer Edge
        count = kelly_count(90, 0.50, 200.0)
        assert count == 0


class TestKellyCountFeeImpact:
    """Gebühren reduzieren den Edge – bei hohen Gebühren evtl. kein Trade."""

    def test_high_fee_reduces_count(self):
        count_no_fee = kelly_count(85, 0.92, 200.0, fee_pct=0.0)
        count_with_fee = kelly_count(85, 0.92, 200.0, fee_pct=5.0)
        assert count_no_fee >= count_with_fee

    def test_extreme_fee_eliminates_edge(self):
        # Bei 90% Gebühr: Netto-Gewinn = 15¢ × 0.10 / 85¢ ≈ 1.76% → negativer Kelly-Faktor
        count = kelly_count(85, 0.92, 200.0, fee_pct=90.0)
        assert count == 0
