"""
Kalshi API Client
REST-API mit RSA-PSS Signatur-Authentifizierung.

Auth-Schema:
  KALSHI-ACCESS-KEY       : API Key ID (UUID)
  KALSHI-ACCESS-SIGNATURE : base64(RSA-PSS-SHA256(timestamp + METHOD + /trade-api/v2/path))
  KALSHI-ACCESS-TIMESTAMP : Unix-Millisekunden als String

Preise auf Kalshi sind in USD-Cent (0–100).
  50 Cent = 50% implizite Wahrscheinlichkeit = $0.50 pro Contract
  1 Contract settlet auf $1.00 (YES gewinnt) oder $0.00 (NO gewinnt)
"""

import base64
import logging
import time
from typing import Optional

import requests
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

logger = logging.getLogger(__name__)

BASE_URL    = "https://api.elections.kalshi.com"
PATH_PREFIX = "/trade-api/v2"


class KalshiClient:
    def __init__(self, api_key_id: str, private_key_pem: str):
        self._key_id = api_key_id
        self._private_key = serialization.load_pem_private_key(
            private_key_pem.encode() if isinstance(private_key_pem, str) else private_key_pem,
            password=None,
        )
        logger.info(f"[Client] Kalshi verbunden | Key-ID: {api_key_id[:8]}...")

    # ------------------------------------------------------------------ #
    #  Auth                                                                #
    # ------------------------------------------------------------------ #

    def _sign(self, method: str, path: str) -> dict:
        """Erzeugt die Auth-Header für einen REST-Request (PATH_PREFIX wird vorangestellt)."""
        ts  = str(int(time.time() * 1000))
        msg = (ts + method.upper() + PATH_PREFIX + path).encode()
        return self._sign_raw(ts, msg)

    def _sign_path(self, method: str, full_path: str) -> dict:
        """Wie _sign, aber full_path wird direkt verwendet (kein PATH_PREFIX). Für WebSocket."""
        ts  = str(int(time.time() * 1000))
        msg = (ts + method.upper() + full_path).encode()
        return self._sign_raw(ts, msg)

    def _sign_raw(self, ts: str, msg: bytes) -> dict:
        """Interne Signatur-Logik."""
        sig = self._private_key.sign(
            msg,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.DIGEST_LENGTH,
            ),
            hashes.SHA256(),
        )
        return {
            "KALSHI-ACCESS-KEY":       self._key_id,
            "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode(),
            "KALSHI-ACCESS-TIMESTAMP": ts,
        }

    def _get(self, path: str, params: Optional[dict] = None) -> dict:
        headers = self._sign("GET", path)
        r = requests.get(
            BASE_URL + PATH_PREFIX + path,
            headers=headers,
            params=params,
            timeout=10,
        )
        r.raise_for_status()
        return r.json()

    def _post(self, path: str, body: dict) -> dict:
        headers = self._sign("POST", path)
        headers["Content-Type"] = "application/json"
        r = requests.post(
            BASE_URL + PATH_PREFIX + path,
            headers=headers,
            json=body,
            timeout=10,
        )
        r.raise_for_status()
        return r.json()

    def _delete(self, path: str) -> dict:
        headers = self._sign("DELETE", path)
        r = requests.delete(
            BASE_URL + PATH_PREFIX + path,
            headers=headers,
            timeout=10,
        )
        r.raise_for_status()
        return r.json()

    # ------------------------------------------------------------------ #
    #  Account                                                             #
    # ------------------------------------------------------------------ #

    def get_balance(self) -> dict:
        """Balance in USD-Cent. balance / 100 = USD."""
        return self._get("/portfolio/balance")

    def get_positions(self) -> list[dict]:
        positions = []
        cursor = ""
        while True:
            params = {"limit": 100}
            if cursor:
                params["cursor"] = cursor
            data = self._get("/portfolio/positions", params)
            positions.extend(data.get("market_positions", []))
            cursor = data.get("cursor", "")
            if not cursor:
                break
        return positions

    def get_fills(self, limit: int = 50) -> list[dict]:
        return self._get("/portfolio/fills", {"limit": limit}).get("fills", [])

    def get_orders(self, status: str = "resting") -> list[dict]:
        return self._get("/portfolio/orders", {"status": status}).get("orders", [])

    # ------------------------------------------------------------------ #
    #  Märkte                                                              #
    # ------------------------------------------------------------------ #

    def get_markets(
        self,
        status: str = "open",
        limit: int = 200,
        cursor: str = "",
        event_ticker: Optional[str] = None,
        series_ticker: Optional[str] = None,
        min_close_ts: Optional[int] = None,
        max_close_ts: Optional[int] = None,
    ) -> dict:
        """Gibt Märkte zurück. Pagination über cursor."""
        params: dict = {"status": status, "limit": limit}
        if cursor:
            params["cursor"] = cursor
        if event_ticker:
            params["event_ticker"] = event_ticker
        if series_ticker:
            params["series_ticker"] = series_ticker
        if min_close_ts is not None:
            params["min_close_ts"] = min_close_ts
        if max_close_ts is not None:
            params["max_close_ts"] = max_close_ts
        return self._get("/markets", params)

    def get_all_open_markets(self, max_markets: int = 500) -> list[dict]:
        """Lädt alle offenen Märkte (paginiert)."""
        markets = []
        cursor  = ""
        while len(markets) < max_markets:
            batch_limit = min(200, max_markets - len(markets))
            data   = self.get_markets(status="open", limit=batch_limit, cursor=cursor)
            batch  = data.get("markets", [])
            markets.extend(batch)
            cursor = data.get("cursor", "")
            if not cursor or not batch:
                break
        return markets

    def get_market(self, ticker: str) -> dict:
        """Einzelnen Markt abrufen."""
        return self._get(f"/markets/{ticker}").get("market", {})

    def get_orderbook(self, ticker: str, depth: int = 10) -> dict:
        """L2 Orderbuch für einen Markt."""
        return self._get(f"/markets/{ticker}/orderbook", {"depth": depth})

    def get_events(
        self,
        status: str = "open",
        limit: int = 200,
        series_ticker: Optional[str] = None,
        with_nested_markets: bool = False,
        min_close_ts: Optional[int] = None,
    ) -> list[dict]:
        """Übergeordnete Events. with_nested_markets=True liefert Märkte direkt inline (spart N Folge-Calls)."""
        params: dict = {"status": status, "limit": limit}
        if series_ticker:
            params["series_ticker"] = series_ticker
        if with_nested_markets:
            params["with_nested_markets"] = "true"
        if min_close_ts is not None:
            params["min_close_ts"] = min_close_ts
        return self._get("/events", params).get("events", [])

    def get_event_metadata(self, event_ticker: str) -> dict:
        """Metadata inkl. image_url für ein Event (GET /events/{ticker}/metadata)."""
        return self._get(f"/events/{event_ticker}/metadata")

    # ------------------------------------------------------------------ #
    #  Orders                                                              #
    # ------------------------------------------------------------------ #

    def place_order(
        self,
        ticker: str,
        side: str,           # "yes" | "no"
        order_type: str,     # "limit" | "market"
        count: int,          # Anzahl Contracts (1 Contract = $1 Notional)
        limit_price: Optional[int] = None,  # in Cent (1–99)
        action: str = "buy",  # "buy" | "sell"
    ) -> dict:
        """
        Platziert eine Order.
        count     = Anzahl Contracts
        limit_price = Preis in Cent (z.B. 45 = $0.45 = 45% YES)
        """
        body: dict = {
            "ticker":     ticker,
            "side":       side,
            "type":       order_type,
            "count":      count,
            "action":     action,
        }
        if order_type == "limit" and limit_price is not None:
            yes_cents = limit_price if side == "yes" else 100 - limit_price
            # _dollars Format: unterstützt Subpenny-Preise (0.001$ Schritte an den Enden)
            body["yes_price_dollars"] = f"{yes_cents / 100:.4f}"

        result = self._post("/portfolio/orders", body)
        logger.debug(f"[Client] Order | {ticker} {action} {side} {count}x @ {limit_price}¢ | {result}")
        return result

    def cancel_order(self, order_id: str) -> dict:
        result = self._delete(f"/portfolio/orders/{order_id}")
        logger.debug(f"[Client] Cancel | {order_id} | {result}")
        return result

    def cancel_all_orders(self) -> int:
        orders = self.get_orders(status="resting")
        cancelled = 0
        for o in orders:
            try:
                self.cancel_order(o["id"])
                cancelled += 1
            except Exception as e:
                logger.warning(f"[Client] Cancel fehlgeschlagen {o['id']}: {e}")
        return cancelled

    # ------------------------------------------------------------------ #
    #  Exchange Status                                                     #
    # ------------------------------------------------------------------ #

    def get_exchange_status(self) -> dict:
        """Exchange-Status (öffentlich, kein Auth nötig laut Doku – nutzt aber _get für Konsistenz)."""
        return self._get("/exchange/status")
