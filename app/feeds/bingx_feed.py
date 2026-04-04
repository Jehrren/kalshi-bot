"""
BingX OHLCV Feed für BTC technische Indikatoren.

Öffentliche API – kein Key nötig für Marktdaten.
Endpoints:
  /openApi/spot/v2/market/kline  – OHLCV Candles
  /openApi/spot/v1/market/depth  – L2 Orderbuch (Top-20 Levels)

Liefert:
  rsi_14          : RSI auf 1-Minuten-Candles (Überkauft >70, Überverkauft <30)
  ema_9/ema_21    : Kurzfristige Trendrichtung
  vol_ratio       : Aktuelles Volumen vs. 20-Perioden-Durchschnitt (>1 = erhöht)
  trend           : "up" | "down" | "flat"
  ob_imbalance    : Bid/Ask Imbalance (>1 = Kaufdruck, <1 = Verkaufsdruck)
  ob_bid_wall_usd : Größter Bid-Block in USD (Unterstützung)
  ob_ask_wall_usd : Größter Ask-Block in USD (Widerstand)
"""

import json
import logging
import time
import urllib.request
from collections import deque
from typing import Optional

logger = logging.getLogger(__name__)

BINGX_URL       = "https://open-api.bingx.com/openApi/spot/v2/market/kline"
BINGX_DEPTH_URL = "https://open-api.bingx.com/openApi/spot/v1/market/depth"

# Kalshi-Serien-Prefix → BingX-Symbol (für Distanz-Filter und 15-Min-Signale)
SERIES_SYMBOL_MAP: dict[str, str] = {
    "KXBTC":   "BTC-USDT",
    "KXSAT":   "BTC-USDT",   # Satoshi/BTC-Milestone-Märkte
    "KXETH":   "ETH-USDT",
    "KXSOL":   "SOL-USDT",
    "KXSOLE":  "SOL-USDT",
    "KXSOLD":  "SOL-USDT",
    "KXXRP":   "XRP-USDT",
    "KXDOGE":  "DOGE-USDT",
    "KXBNB":   "BNB-USDT",
    "KXHYPE":  "HYPE-USDT",
    "KXSHIBA": "SHIB-USDT",
}


def series_to_symbol(series: str) -> str | None:
    """Gibt das BingX-Handelssymbol für einen Kalshi-Serien-Prefix zurück."""
    for prefix, symbol in SERIES_SYMBOL_MAP.items():
        if series.startswith(prefix):
            return symbol
    return None


class BingXFeed:
    def __init__(self, symbol: str = "BTC-USDT", interval: str = "1m",
                 refresh_interval_s: int = 30):
        self._symbol      = symbol
        self._interval    = interval
        self._refresh     = refresh_interval_s
        self._last_ts     = 0.0
        self._last_ob_ts  = 0.0
        # [open, high, low, close, volume] – neueste zuletzt
        self._candles: deque = deque(maxlen=50)
        self._orderbook: dict = {}   # {"bids": [[price, qty],...], "asks": [...]}

    # ------------------------------------------------------------------ #
    #  Refresh                                                             #
    # ------------------------------------------------------------------ #

    def refresh(self) -> None:
        now = time.monotonic()
        if now - self._last_ts >= self._refresh:
            candles = self._fetch(limit=30)
            if candles:
                self._candles.clear()
                self._candles.extend(candles)
                self._last_ts = now
                logger.debug(f"[BingX] {len(candles)} Candles | close={candles[-1][3]:.2f}")
        # Orderbuch alle 15 Sekunden aktualisieren (häufiger als OHLCV)
        if now - self._last_ob_ts >= 15:
            ob = self._fetch_orderbook()
            if ob:
                self._orderbook = ob
                self._last_ob_ts = now

    def _fetch_orderbook(self, depth: int = 20) -> dict:
        """Lädt L2 Orderbuch (Top-N Bid/Ask Levels)."""
        url = f"{BINGX_DEPTH_URL}?symbol={self._symbol}&depth={depth}"
        try:
            with urllib.request.urlopen(url, timeout=8) as r:
                data = json.loads(r.read())
            ob = data.get("data", {})
            bids = [[float(b[0]), float(b[1])] for b in ob.get("bids", [])]
            asks = [[float(a[0]), float(a[1])] for a in ob.get("asks", [])]
            return {"bids": bids, "asks": asks}
        except Exception as e:
            logger.debug(f"[BingX] Orderbuch-Fetch fehlgeschlagen: {e}")
            return {}

    def _fetch(self, limit: int = 30) -> list:
        url = (f"{BINGX_URL}?symbol={self._symbol}"
               f"&interval={self._interval}&limit={limit}")
        try:
            with urllib.request.urlopen(url, timeout=8) as r:
                data = json.loads(r.read())
            raw = data.get("data", [])
            # Format: [open_time, open, high, low, close, volume, close_time, quote_vol]
            candles = []
            for c in raw:
                candles.append([
                    float(c[1]),  # open
                    float(c[2]),  # high
                    float(c[3]),  # low
                    float(c[4]),  # close
                    float(c[5]),  # volume
                ])
            # BingX gibt neueste zuerst zurück → umkehren
            return list(reversed(candles))
        except Exception as e:
            logger.warning(f"[BingX] Fetch fehlgeschlagen: {e}")
            return []

    # ------------------------------------------------------------------ #
    #  Indikatoren                                                         #
    # ------------------------------------------------------------------ #

    def is_ready(self) -> bool:
        return len(self._candles) >= 30

    def rsi(self, period: int = 14) -> Optional[float]:
        closes = [c[3] for c in self._candles]
        if len(closes) < period + 1:
            return None
        gains, losses = [], []
        for i in range(1, len(closes)):
            delta = closes[i] - closes[i - 1]
            gains.append(max(delta, 0))
            losses.append(max(-delta, 0))
        # Wilder's smoothed RSI: SMA für erste avg_gain/avg_loss, dann exponentiell glätten
        avg_gain = sum(gains[:period]) / period
        avg_loss = sum(losses[:period]) / period
        for i in range(period, len(gains)):
            avg_gain = (avg_gain * (period - 1) + gains[i]) / period
            avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        if avg_loss == 0:
            return 100.0
        rs = avg_gain / avg_loss
        return round(100 - (100 / (1 + rs)), 2)

    def ema(self, period: int) -> Optional[float]:
        closes = [c[3] for c in self._candles]
        if len(closes) < period:
            return None
        k = 2 / (period + 1)
        ema_val = sum(closes[:period]) / period
        for price in closes[period:]:
            ema_val = price * k + ema_val * (1 - k)
        return round(ema_val, 2)

    def change_pct(self, minutes: int = 15) -> Optional[float]:
        """
        Prozentuale BTC-Preisänderung der letzten N Minuten (via 1-min Candles).
        Entspricht btc_change_15min aus dem alten BTCPriceFeed.
        """
        closes = [c[3] for c in self._candles]
        if len(closes) < minutes + 1:
            return None
        past = closes[-(minutes + 1)]
        current = closes[-1]
        if past == 0:
            return None
        return round((current - past) / past * 100, 4)

    def vol_ratio(self) -> Optional[float]:
        """Aktuelles Volumen vs. 20-Perioden-Durchschnitt."""
        vols = [c[4] for c in self._candles]
        if len(vols) < 22:
            return None
        ref = vols[-21:-1]   # 20 Candles EXKL. aktueller
        avg = sum(ref) / len(ref)
        if avg == 0:
            return None
        return round(vols[-1] / avg, 2)

    def orderbook_imbalance(self) -> Optional[float]:
        """
        Bid/Ask Imbalance der Top-5 Levels (nach USD-Wert).
        > 1.0 = mehr Kaufdruck (bullish)
        < 1.0 = mehr Verkaufsdruck (bearish)
        """
        bids = self._orderbook.get("bids", [])[:5]
        asks = self._orderbook.get("asks", [])[:5]
        if not bids or not asks:
            return None
        bid_usd = sum(p * q for p, q in bids)
        ask_usd = sum(p * q for p, q in asks)
        if ask_usd == 0:
            return None
        return round(bid_usd / ask_usd, 3)

    def orderbook_walls(self) -> tuple[Optional[float], Optional[float]]:
        """
        Größte Einzel-Blöcke im Bid/Ask (USD-Wert).
        Gibt (größter_bid_usd, größter_ask_usd) zurück.
        Große Wände = starke Unterstützung/Widerstand.
        """
        bids = self._orderbook.get("bids", [])
        asks = self._orderbook.get("asks", [])
        if not bids or not asks:
            return None, None
        bid_wall = max((p * q for p, q in bids), default=0)
        ask_wall = max((p * q for p, q in asks), default=0)
        return round(bid_wall, 0), round(ask_wall, 0)

    def current_price(self) -> Optional[float]:
        if not self._candles:
            return None
        return self._candles[-1][3]

    def context(self) -> dict:
        """Gibt den vollständigen Indikator-Kontext zurück."""
        if not self.is_ready():
            return {}
        rsi_val  = self.rsi(14)
        ema9     = self.ema(9)
        ema21    = self.ema(21)
        vol_r    = self.vol_ratio()
        price    = self.current_price()
        ob_imb   = self.orderbook_imbalance()
        bid_wall, ask_wall = self.orderbook_walls()

        trend = "flat"
        if ema9 and ema21:
            if ema9 > ema21 * 1.0005:
                trend = "up"
            elif ema9 < ema21 * 0.9995:
                trend = "down"

        ctx: dict = {
            # BTC Preis (kompatibel mit altem BTCPriceFeed-Interface)
            "btc_price":        price,
            "btc_change_5min":  self.change_pct(5),
            "btc_change_15min": self.change_pct(15),
            # BingX-spezifische Indikatoren
            "bingx_price":     price,
            "bingx_rsi":       rsi_val,
            "bingx_ema9":      ema9,
            "bingx_ema21":     ema21,
            "bingx_vol_ratio": vol_r,
            "bingx_trend":     trend,
        }
        if ob_imb is not None:
            ctx["bingx_ob_imbalance"] = ob_imb
        if bid_wall is not None:
            ctx["bingx_ob_bid_wall_usd"] = bid_wall
        if ask_wall is not None:
            ctx["bingx_ob_ask_wall_usd"] = ask_wall
        return ctx
