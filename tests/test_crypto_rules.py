"""
Unit-Tests für crypto/rules.py und crypto/models.py.

Abgedeckte Fälle:
  - crypto_corrected_yes_prob: Kalibrierungskorrektur
  - CryptoSignal: Dataclass-Konstruktion
  - utils.market: ticker_threshold, hours_remaining
"""

import sys
from pathlib import Path
from datetime import datetime, timezone, timedelta

sys.path.insert(0, str(Path(__file__).parent.parent / "app"))

from crypto.rules import crypto_corrected_yes_prob
from crypto.models import CryptoSignal, SYSTEM
from utils.market import ticker_threshold, hours_remaining


class TestCryptoCorrectedYesProb:
    """Kalibrierungskorrektur für Crypto-Threshold-Märkte."""

    def test_extreme_high_price_reduced(self):
        # ≥95¢ → -4%
        result = crypto_corrected_yes_prob(97)
        assert abs(result - 0.93) < 0.001

    def test_high_price_90_reduced(self):
        # ≥90¢ → -3%
        result = crypto_corrected_yes_prob(92)
        assert abs(result - 0.89) < 0.001

    def test_high_price_85_reduced(self):
        # ≥85¢ → -2.5%
        result = crypto_corrected_yes_prob(87)
        assert abs(result - 0.845) < 0.001

    def test_mid_range_73_82_reduced(self):
        # 73–82¢ → -1%
        result = crypto_corrected_yes_prob(78)
        assert abs(result - 0.77) < 0.001

    def test_low_range_55_65_reduced(self):
        # 55–65¢ → -1%
        result = crypto_corrected_yes_prob(60)
        assert abs(result - 0.59) < 0.001

    def test_neutral_range_unchanged(self):
        # Außerhalb aller Korrekturbänder → unverändert
        result = crypto_corrected_yes_prob(50)
        assert abs(result - 0.50) < 0.001

    def test_minimum_floor(self):
        # Extreme Korrekturen: minimum 0.01
        result = crypto_corrected_yes_prob(2)  # 2¢ = 2% - 4% wäre negativ
        assert result >= 0.01


class TestCryptoSignal:
    """CryptoSignal Dataclass: korrekte Defaults und Konstruktion."""

    def test_default_system(self):
        sig = CryptoSignal(
            ticker="KXBTC-T80000",
            rule_name="test",
            side="no",
            action="buy",
            price_cents=88,
            count=3,
            reason="test signal",
        )
        assert sig.system == SYSTEM
        assert sig.system == "crypto"

    def test_default_track(self):
        sig = CryptoSignal(
            ticker="KXBTC-T80000",
            rule_name="test",
            side="no",
            action="buy",
            price_cents=88,
            count=3,
            reason="test signal",
        )
        assert sig.track == "crypto"

    def test_custom_track(self):
        sig = CryptoSignal(
            ticker="KXBTC15M-T80000",
            rule_name="15m test",
            side="yes",
            action="buy",
            price_cents=45,
            count=5,
            reason="15min signal",
            track="crypto_15min",
        )
        assert sig.track == "crypto_15min"


class TestTickerThreshold:
    """Threshold-Parsing aus Ticker-Symbolen."""

    def test_btc_t_suffix(self):
        assert ticker_threshold("KXBTC-T80000") == 80000.0

    def test_eth_t_suffix(self):
        assert ticker_threshold("KXETH-T3000") == 3000.0

    def test_b_suffix(self):
        assert ticker_threshold("KXWTMP-LA-B85") == 85.0

    def test_15min_t_suffix(self):
        assert ticker_threshold("KXBTC15M-25APR14-T83500") == 83500.0

    def test_no_suffix_returns_zero(self):
        assert ticker_threshold("KXBTCMAXMON") == 0.0

    def test_invalid_suffix_returns_zero(self):
        assert ticker_threshold("KXBTC-TXYZ") == 0.0


class TestHoursRemaining:
    """Verbleibende Stunden bis Marktschluss."""

    def test_future_close_time(self):
        future = (datetime.now(timezone.utc) + timedelta(hours=3)).isoformat()
        h = hours_remaining(future)
        assert 2.9 < h < 3.1

    def test_past_close_time_returns_zero(self):
        past = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
        h = hours_remaining(past)
        assert h == 0.0

    def test_empty_string_returns_inf(self):
        assert hours_remaining("") == float("inf")

    def test_none_returns_inf(self):
        # Leerer String (None würde TypeError sein, daher "" als Grenzfall)
        assert hours_remaining("") == float("inf")

    def test_z_suffix_handled(self):
        future = (datetime.now(timezone.utc) + timedelta(hours=5)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        h = hours_remaining(future)
        assert 4.9 < h < 5.1
