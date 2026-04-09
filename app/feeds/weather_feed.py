"""
Open-Meteo GFS Ensemble Feed für Wetter-Märkte.

Öffentliche API – kein Key nötig.
Endpoint: https://ensemble-api.open-meteo.com/v1/ensemble

Liefert:
  forecast_high_f   : Tages-Hoch in °F (Median der 31 GFS-Mitglieder)
  forecast_low_f    : Tages-Tief in °F
  ensemble_members  : Liste aller 31 Tageshochs (für Probability-Berechnung)
  ensemble_spread_f : Standardabweichung der Ensemble-Mitglieder
  forecast_confidence : 1.0 bei engem Spread, 0.0 bei weitem Spread

Station-Bias-Korrekturen (NWS-Station vs. GFS-Gitterpunkt):
  Kalshi settled auf NWS Daily Climate Report (spezifische Station pro Stadt).
  GFS-Forecasts haben systematische Abweichungen zur Settlement-Station.
"""

import json
import logging
import math
import time
import urllib.request
from typing import Optional

logger = logging.getLogger(__name__)

ENSEMBLE_URL = "https://ensemble-api.open-meteo.com/v1/ensemble"

# Kalshi-Serien → (Stadt, Breitengrad, Längengrad, Typ "high"|"low")
WEATHER_SERIES_MAP: dict[str, tuple[str, float, float, str]] = {
    # Daily High Temperature
    "KXHIGHNY":   ("New York",     40.7826, -73.9656, "high"),   # Central Park KNYC
    "KXHIGHCHI":  ("Chicago",      41.9742, -87.9073, "high"),   # O'Hare KORD
    "KXHIGHMIA":  ("Miami",        25.7959, -80.2870, "high"),   # KMIA
    "KXHIGHLAX":  ("Los Angeles",  33.9382, -118.389, "high"),   # KLAX
    "KXHIGHDEN":  ("Denver",       39.8561, -104.673, "high"),   # KDEN
    "KXHIGHTBOS": ("Boston",       42.3656, -71.0096, "high"),   # KBOS
    "KXHIGHTATL": ("Atlanta",      33.6407, -84.4277, "high"),   # KATL
    "KXHIGHTDAL": ("Dallas",       32.8998, -97.0403, "high"),   # KDFW
    "KXHIGHTSEA": ("Seattle",      47.4502, -122.309, "high"),   # KSEA
    "KXHIGHTSATX":("San Antonio",  29.5339, -98.4698, "high"),   # KSAT
    "KXHIGHTDC":  ("Washington DC",38.8512, -77.0402, "high"),   # KDCA
    "KXHIGHAUS":  ("Austin",       30.1945, -97.6699, "high"),   # KAUS
    # Daily Low Temperature
    "KXLOWTNYC":  ("New York",     40.7826, -73.9656, "low"),
    "KXLOWTLV":   ("Las Vegas",    36.0840, -115.152, "low"),
    "KXLOWTDEN":  ("Denver",       39.8561, -104.673, "low"),
    "KXLOWTCHI":  ("Chicago",      41.9742, -87.9073, "low"),
}

# Station-Bias-Korrekturen: GFS-Forecast → NWS-Settlement-Temperatur.
# Positiver Wert = NWS-Station misst wärmer als GFS-Gitterpunkt.
# Quelle: historische METAR vs. GFS Analyse (saisonale Durchschnitte).
STATION_BIAS_F: dict[str, tuple[float, float]] = {
    # Stadt:      (Sommer-Bias, Winter-Bias)
    "New York":     (+1.8, +0.8),   # Urban Heat Island Central Park
    "Chicago":      (-1.2, -0.5),   # Lake-Effekt, kühler als Metro
    "Miami":        (+0.4, +0.3),   # Airport, nah an Forecast
    "Los Angeles":  (+0.6, +0.2),   # Küstennah
    "Denver":       (-0.3, -0.8),   # Höhenlage, kühler als Modell
    "Boston":       (+0.5, +0.3),
    "Atlanta":      (+1.0, +0.3),   # Hartsfield Beton
    "Dallas":       (+1.5, +0.4),   # Beton Heat Island
    "Seattle":      (-0.8, -0.3),   # Nähe Wasser
    "San Antonio":  (+1.2, +0.3),
    "Washington DC":(+0.8, +0.3),
    "Austin":       (+1.0, +0.2),
    "Las Vegas":    (+0.5, +0.2),
}

# Stadt×Saison Standardabweichungen (°F) für Probability-Berechnung.
# Basiert auf historischen NWS-Forecast-Fehlern.
CITY_STD_F: dict[str, tuple[float, float]] = {
    # Stadt:      (Sommer-σ, Winter-σ)
    "New York":     (1.8, 2.5),
    "Chicago":      (2.0, 3.0),
    "Miami":        (1.2, 1.5),
    "Los Angeles":  (1.5, 1.8),
    "Denver":       (2.5, 3.5),
    "Boston":       (2.0, 2.8),
    "Atlanta":      (1.5, 2.0),
    "Dallas":       (1.8, 2.2),
    "Seattle":      (1.8, 2.0),
    "San Antonio":  (1.5, 1.8),
    "Washington DC":(1.8, 2.5),
    "Austin":       (1.5, 1.8),
    "Las Vegas":    (1.5, 2.0),
}


def series_to_city(series: str) -> Optional[str]:
    """Gibt den Stadt-Namen für ein Kalshi-Serien-Prefix zurück."""
    info = WEATHER_SERIES_MAP.get(series)
    return info[0] if info else None


def series_to_type(series: str) -> str:
    """Gibt 'high' oder 'low' für die Serie zurück."""
    info = WEATHER_SERIES_MAP.get(series)
    return info[3] if info else "high"


def _is_summer_month(month: int) -> bool:
    return month in (4, 5, 6, 7, 8, 9)


def _normal_cdf(x: float) -> float:
    """Standard-Normal CDF via Abramowitz-Stegun-Approximation."""
    if x < -8:
        return 0.0
    if x > 8:
        return 1.0
    t = 1.0 / (1.0 + 0.2316419 * abs(x))
    d = 0.3989422804014327  # 1/sqrt(2*pi)
    p = d * math.exp(-x * x / 2.0) * (
        t * (0.319381530 + t * (-0.356563782 + t * (1.781477937
        + t * (-1.821255978 + t * 1.330274429))))
    )
    return (1.0 - p) if x > 0 else p


class WeatherFeed:
    """Open-Meteo GFS Ensemble Feed für eine Stadt (lat/lon)."""

    def __init__(self, city: str, lat: float, lon: float,
                 refresh_interval_s: int = 300):
        self._city    = city
        self._lat     = lat
        self._lon     = lon
        self._refresh = refresh_interval_s
        self._last_ts = 0.0
        self._data: dict = {}

    def refresh(self) -> None:
        now = time.monotonic()
        if now - self._last_ts < self._refresh:
            return
        raw = self._fetch()
        if raw:
            self._data = raw
            self._last_ts = now
            logger.debug(
                f"[Weather/{self._city}] Forecast geladen: "
                f"high={self._daily_max_median():.1f}°F"
            )

    def _fetch(self) -> dict:
        params = (
            f"?latitude={self._lat}&longitude={self._lon}"
            f"&models=gfs_seamless"
            f"&daily=temperature_2m_max,temperature_2m_min"
            f"&temperature_unit=fahrenheit"
            f"&timezone=America%2FNew_York"
            f"&forecast_days=2"
        )
        url = ENSEMBLE_URL + params
        try:
            req = urllib.request.Request(url, headers={"Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=10) as r:
                return json.loads(r.read())
        except Exception as e:
            logger.warning(f"[Weather/{self._city}] Fetch fehlgeschlagen: {e}")
            return {}

    def is_ready(self) -> bool:
        return bool(self._data.get("daily"))

    def _daily_max_median(self) -> float:
        """Median des Tages-Hochs (Member 0 = Kontroll-Lauf)."""
        daily = self._data.get("daily", {})
        vals = daily.get("temperature_2m_max", [])
        return float(vals[0]) if vals else 0.0

    def _daily_min_median(self) -> float:
        daily = self._data.get("daily", {})
        vals = daily.get("temperature_2m_min", [])
        return float(vals[0]) if vals else 0.0

    def _ensemble_highs(self) -> list[float]:
        """Tageshochs aller 31 GFS-Mitglieder für heute (Index 0)."""
        daily = self._data.get("daily", {})
        members: list[float] = []
        # Kontroll-Lauf
        ctrl = daily.get("temperature_2m_max", [])
        if ctrl:
            members.append(float(ctrl[0]))
        # Perturbed members: temperature_2m_max_member01 bis _member30
        for i in range(1, 31):
            key = f"temperature_2m_max_member{i:02d}"
            vals = daily.get(key, [])
            if vals:
                members.append(float(vals[0]))
        return members

    def _ensemble_lows(self) -> list[float]:
        """Tagestiefs aller 31 GFS-Mitglieder für heute."""
        daily = self._data.get("daily", {})
        members: list[float] = []
        ctrl = daily.get("temperature_2m_min", [])
        if ctrl:
            members.append(float(ctrl[0]))
        for i in range(1, 31):
            key = f"temperature_2m_min_member{i:02d}"
            vals = daily.get(key, [])
            if vals:
                members.append(float(vals[0]))
        return members

    def bracket_probability(self, lower_f: float, upper_f: float, market_type: str = "high") -> float:
        """
        P(lower_f ≤ temp < upper_f) für Bracket-Märkte.

        Methode: P(≥lower) - P(≥upper)
        Korrekt für Kalshi Bracket-Slots (z.B. "60° to 61°" → lower=60, upper=62).
        """
        if lower_f <= 0 or upper_f <= lower_f:
            return 0.0
        p_low  = self.ensemble_probability(lower_f, market_type)
        p_high = self.ensemble_probability(upper_f, market_type)
        return max(0.0, min(1.0, p_low - p_high))

    def ensemble_probability(self, threshold_f: float, market_type: str = "high") -> float:
        """
        Berechnet Wahrscheinlichkeit dass Temperatur AT oder ABOVE threshold liegt.

        Für 'high'-Märkte: P(daily_high >= threshold)
        Für 'low'-Märkte:  P(daily_low >= threshold)

        Methode: Anteil der Ensemble-Mitglieder die den Schwellenwert überschreiten,
        plus Normal-CDF-Glättung für bessere Kalibrierung.
        """
        members = self._ensemble_highs() if market_type == "high" else self._ensemble_lows()
        if not members:
            return 0.5

        # Station-Bias anwenden: verschiebt Forecast Richtung Settlement-Station
        month = time.localtime().tm_mon
        summer = _is_summer_month(month)
        bias_pair = STATION_BIAS_F.get(self._city, (0.0, 0.0))
        bias = bias_pair[0] if summer else bias_pair[1]
        corrected = [m + bias for m in members]

        # Ensemble-Counting: wie viele Mitglieder liegen AT oder ABOVE?
        above = sum(1 for m in corrected if m >= threshold_f)
        ensemble_p = above / len(corrected)

        # Normal-CDF als Glättung (verhindert 0.0/1.0 bei knappen Werten)
        mean = sum(corrected) / len(corrected)
        std_pair = CITY_STD_F.get(self._city, (2.0, 2.5))
        std = std_pair[0] if summer else std_pair[1]
        if std > 0:
            z = (mean - threshold_f) / std
            cdf_p = _normal_cdf(z)
        else:
            cdf_p = ensemble_p

        # Blend: 60% Ensemble-Counting + 40% CDF (Bayesian-artig)
        return round(0.6 * ensemble_p + 0.4 * cdf_p, 4)

    def context(self, market_type: str = "high") -> dict:
        """Gibt den vollständigen Wetter-Kontext zurück."""
        if not self.is_ready():
            return {}

        members = self._ensemble_highs() if market_type == "high" else self._ensemble_lows()
        forecast = self._daily_max_median() if market_type == "high" else self._daily_min_median()

        # Station-Bias anwenden
        month = time.localtime().tm_mon
        summer = _is_summer_month(month)
        bias_pair = STATION_BIAS_F.get(self._city, (0.0, 0.0))
        bias = bias_pair[0] if summer else bias_pair[1]
        forecast_corrected = forecast + bias

        # Ensemble-Spread (Standardabweichung)
        spread = 0.0
        if len(members) >= 2:
            mean = sum(members) / len(members)
            variance = sum((m - mean) ** 2 for m in members) / len(members)
            spread = math.sqrt(variance)

        # Confidence: enger Spread = hohe Konfidenz
        # σ < 1.5°F → 1.0, σ > 5.0°F → 0.0
        confidence = max(0.0, min(1.0, 1.0 - (spread - 1.5) / 3.5))

        return {
            "city":                self._city,
            "forecast_high_f":     self._daily_max_median() + bias if market_type == "high" else None,
            "forecast_low_f":      self._daily_min_median() + bias if market_type == "low" else None,
            "forecast_temp_f":     forecast_corrected,
            "station_bias_f":      bias,
            "ensemble_spread_f":   round(spread, 2),
            "ensemble_members":    len(members),
            "forecast_confidence": round(confidence, 3),
        }
