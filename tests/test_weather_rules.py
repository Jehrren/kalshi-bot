"""
Unit-Tests für weather/rules.py.

Abgedeckte Fälle:
  - weather_corrected_prob: Kalibrierungskorrektur
  - kelly_count (weather defaults): max_count=8
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "app"))

# weather_corrected_prob direkt aus dem Modul
# (WeatherRuleEngine braucht WeatherFeed-Import → zu komplex für Unit-Test)
import importlib.util
spec = importlib.util.spec_from_file_location(
    "weather_rules",
    Path(__file__).parent.parent / "app" / "weather" / "rules.py",
)


class TestWeatherCorrectedProb:
    """Kalibrierungskorrektur für Wetter-Märkte."""

    def _get_weather_corrected_prob(self):
        """Lädt weather_corrected_prob ohne WeatherFeed-Abhängigkeit."""
        import importlib
        import unittest.mock as mock
        # WeatherFeed mocken damit der Import nicht fehlschlägt
        with mock.patch.dict("sys.modules", {
            "feeds.weather_feed": mock.MagicMock(),
        }):
            import importlib.util
            spec = importlib.util.spec_from_file_location(
                "weather_rules_test",
                Path(__file__).parent.parent / "app" / "weather" / "rules.py",
            )
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            return mod.weather_corrected_prob

    def test_extreme_high_price_reduced(self):
        fn = self._get_weather_corrected_prob()
        # ≥92¢ → -3%
        result = fn(95)
        assert abs(result - 0.92) < 0.001

    def test_high_price_85_reduced(self):
        fn = self._get_weather_corrected_prob()
        # ≥85¢ → -2%
        result = fn(88)
        assert abs(result - 0.86) < 0.001

    def test_mid_range_73_82_reduced(self):
        fn = self._get_weather_corrected_prob()
        # 73–82¢ → -1%
        result = fn(75)
        assert abs(result - 0.74) < 0.001

    def test_low_price_unchanged(self):
        fn = self._get_weather_corrected_prob()
        # Außerhalb Korrekturbänder → unverändert
        result = fn(50)
        assert abs(result - 0.50) < 0.001

    def test_minimum_floor(self):
        fn = self._get_weather_corrected_prob()
        # Minimum 0.01
        result = fn(1)
        assert result >= 0.01


class TestKellyWeatherDefaults:
    """kelly_count mit Weather-System Defaults (max_count=8)."""

    def test_weather_max_count_respected(self):
        from utils.kelly import kelly_count
        count = kelly_count(70, 0.85, 1_000.0, max_count=8)
        assert count <= 8

    def test_weather_min_count_respected(self):
        from utils.kelly import kelly_count
        # Kleines Kapital, aber positiver Edge → min_count=1
        count = kelly_count(70, 0.85, 0.5, min_count=1, max_count=8)
        assert count >= 1

    def test_invalid_input_returns_zero(self):
        from utils.kelly import kelly_count
        # Bug-Fix Verifikation: 0 statt min_count bei ungültigen Inputs
        assert kelly_count(0, 0.85, 200.0, min_count=1) == 0
        assert kelly_count(85, 0.0, 200.0, min_count=1) == 0
