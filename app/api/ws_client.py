"""
Kalshi WebSocket Feed – Real-time Ticker-Updates.

Hält eine persistente WSS-Verbindung und cached die neuesten Marktpreise.
Fällt automatisch auf REST-Fallback zurück wenn WS nicht verfügbar ist.

Channels:
  ticker  – Best-Bid/Ask-Updates für abonnierte Märkte (send_initial_snapshot=True)
"""

import asyncio
import json
import logging
import time
from typing import Optional

import websockets
from websockets.exceptions import ConnectionClosed

logger = logging.getLogger(__name__)

WS_URL   = "wss://api.elections.kalshi.com/trade-api/ws/v2"
WS_PATH  = "/trade-api/ws/v2"   # Signing-Pfad (abweichend von REST /trade-api/v2)


class KalshiWSFeed:
    """Real-time Preis-Cache via Kalshi WebSocket."""

    def __init__(self, kalshi_client):
        self._client        = kalshi_client
        self._cache: dict   = {}        # ticker → latest market dict (from WS)
        self._subscribed: set = set()
        self._ws            = None
        self._cmd_id        = 0
        self._connected     = False
        self._stop_event    = asyncio.Event()
        self._listen_task   = None

    # ------------------------------------------------------------------ #
    #  Verbindung                                                          #
    # ------------------------------------------------------------------ #

    async def start(self) -> bool:
        """
        Baut WS-Verbindung auf, startet Listen-Task.
        Gibt True zurück wenn erfolgreich.
        """
        try:
            # WS-Pfad weicht von REST ab: /trade-api/ws/v2
            headers = self._client._sign_path("GET", WS_PATH)
            self._ws = await asyncio.wait_for(
                websockets.connect(WS_URL, additional_headers=headers),
                timeout=10,
            )
            self._connected = True
            logger.info("[WSFeed] Verbunden mit Kalshi WebSocket")
            self._listen_task = asyncio.create_task(self._listen(), name="ws_listen")
            return True
        except Exception as e:
            logger.warning(f"[WSFeed] Verbindung fehlgeschlagen – Fallback auf REST: {e}")
            self._connected = False
            return False

    async def stop(self) -> None:
        self._stop_event.set()
        if self._ws:
            await self._ws.close()
        if self._listen_task:
            self._listen_task.cancel()
            try:
                await self._listen_task
            except asyncio.CancelledError:
                pass
        self._connected = False
        logger.info("[WSFeed] Gestoppt")

    # ------------------------------------------------------------------ #
    #  Subscription                                                        #
    # ------------------------------------------------------------------ #

    async def subscribe(self, market_tickers: list[str]) -> None:
        """Abonniert neue Ticker (ignoriert bereits abonnierte)."""
        if not self._connected or not self._ws:
            return
        new = [t for t in market_tickers if t not in self._subscribed]
        if not new:
            return
        self._cmd_id += 1
        cmd = {
            "id": self._cmd_id,
            "cmd": "subscribe",
            "params": {
                "channels": ["ticker"],
                "market_tickers": new,
                "send_initial_snapshot": True,
            },
        }
        try:
            await self._ws.send(json.dumps(cmd))
            self._subscribed.update(new)
            logger.debug(f"[WSFeed] Abonniert: {new}")
        except Exception as e:
            logger.warning(f"[WSFeed] Subscribe fehlgeschlagen: {e}")
            self._connected = False

    async def unsubscribe_stale(self, active_tickers: set[str]) -> None:
        """Entfernt abgelaufene Ticker aus der internen Cache."""
        stale = self._subscribed - active_tickers
        for t in stale:
            self._subscribed.discard(t)
            self._cache.pop(t, None)

    # ------------------------------------------------------------------ #
    #  Listen-Loop                                                         #
    # ------------------------------------------------------------------ #

    async def _listen(self) -> None:
        """Verarbeitet eingehende WS-Nachrichten und updated den Cache."""
        try:
            async for raw in self._ws:
                if self._stop_event.is_set():
                    break
                try:
                    msg = json.loads(raw)
                except json.JSONDecodeError:
                    continue

                msg_type = msg.get("type", "")

                if msg_type == "ticker":
                    market = msg.get("msg", {})
                    ticker = market.get("ticker", "")
                    if ticker:
                        self._cache[ticker] = {**market, "_ws_ts": time.time()}

                elif msg_type == "subscribed":
                    logger.debug(f"[WSFeed] Subscription bestätigt: {msg.get('msg', {})}")

                elif msg_type == "error":
                    err = msg.get("msg", {})
                    logger.warning(f"[WSFeed] Server-Fehler: {err}")

        except ConnectionClosed as e:
            logger.warning(f"[WSFeed] Verbindung getrennt: {e}")
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.warning(f"[WSFeed] Listen-Fehler: {e}")
        finally:
            self._connected = False

    # ------------------------------------------------------------------ #
    #  Cache-Zugriff                                                       #
    # ------------------------------------------------------------------ #

    def get_market(self, ticker: str) -> Optional[dict]:
        """Gibt gecachte Marktdaten zurück, None wenn nicht vorhanden."""
        return self._cache.get(ticker)

    def is_connected(self) -> bool:
        return self._connected

    def cache_size(self) -> int:
        return len(self._cache)
