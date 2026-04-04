"""
BTC Preis-Feed.

Primär: Binance REST API (kostenlos, kein Auth, sehr zuverlässig)
Fallback: CoinGecko (kostenlos, kein Auth – bei 429 wird Primär bevorzugt)

Sammelt alle 30 Sekunden den aktuellen BTC-Preis und hält
eine In-Memory-Historie für Momentum-Berechnungen.
"""

import logging
import time
from collections import deque
from typing import Optional

import requests

logger = logging.getLogger(__name__)

BINANCE_URL   = "https://api.binance.com/api/v3/ticker/price"
COINGECKO_URL = "https://api.coingecko.com/api/v3/simple/price"


class BTCPriceFeed:
    """Hält BTC-Preis-Historie und berechnet Momentum."""

    def __init__(self, refresh_interval_s: int = 30):
        self._interval  = refresh_interval_s
        # Tuple (unix_timestamp, price_usd) – maxlen = 120 Einträge = 60 Min @ 30s
        self._history: deque = deque(maxlen=120)
        self._last_refresh   = 0.0
        self._enabled        = True

    # ------------------------------------------------------------------ #
    #  Datenabruf                                                          #
    # ------------------------------------------------------------------ #

    def refresh(self) -> None:
        """Ruft aktuellen BTC-Preis ab (rate-limited auf self._interval)."""
        if not self._enabled:
            return
        now = time.monotonic()
        if now - self._last_refresh < self._interval:
            return
        price = self._fetch_binance() or self._fetch_coingecko()
        if price:
            self._history.append((time.time(), price))
            self._last_refresh = now
            logger.debug(f"[BTCFeed] ${price:,.2f}")
        # Kein Hard-Fehler – nächster Versuch beim nächsten Zyklus

    def _fetch_binance(self) -> Optional[float]:
        """Binance public ticker (kein Auth, kein praktisches Rate Limit)."""
        try:
            r = requests.get(
                BINANCE_URL,
                params={"symbol": "BTCUSDT"},
                timeout=5,
            )
            r.raise_for_status()
            return float(r.json()["price"])
        except Exception as e:
            logger.debug(f"[BTCFeed] Binance fehlgeschlagen: {e}")
            return None

    def _fetch_coingecko(self) -> Optional[float]:
        """CoinGecko Fallback."""
        try:
            r = requests.get(
                COINGECKO_URL,
                params={"ids": "bitcoin", "vs_currencies": "usd"},
                timeout=5,
            )
            r.raise_for_status()
            return float(r.json()["bitcoin"]["usd"])
        except Exception as e:
            logger.warning(f"[BTCFeed] Alle Quellen fehlgeschlagen (letzte: {e})")
            return None

    # ------------------------------------------------------------------ #
    #  Zugriff                                                             #
    # ------------------------------------------------------------------ #

    def current_price(self) -> Optional[float]:
        return self._history[-1][1] if self._history else None

    def change_pct(self, minutes: int = 15) -> Optional[float]:
        """
        Prozentuale Preisänderung über die letzten N Minuten.
        Gibt None zurück wenn nicht genug Historie vorhanden.
        """
        if len(self._history) < 2:
            return None

        now_ts, now_price = self._history[-1]
        target_ts = now_ts - (minutes * 60)

        # Letzter bekannter Preis vor dem Zielzeitpunkt
        past_price: Optional[float] = None
        for ts, price in reversed(self._history):
            if ts <= target_ts:
                past_price = price
                break

        # Fallback: ältester verfügbarer Datenpunkt
        if past_price is None:
            if len(self._history) < 4:   # Zu wenig Daten für sinnvolle Schätzung
                return None
            past_price = self._history[0][1]

        if past_price == 0:
            return None

        return (now_price - past_price) / past_price * 100

    def context(self) -> dict:
        """Preis-Kontext für die Regel-Engine."""
        return {
            "btc_price":        self.current_price(),
            "btc_change_5min":  self.change_pct(5),
            "btc_change_15min": self.change_pct(15),
        }

    def is_ready(self) -> bool:
        """True wenn genug Historie für 15-Min-Berechnungen vorhanden."""
        return len(self._history) >= 4
