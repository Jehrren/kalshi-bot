"""Trade Executor für Kalshi Prediction Markets."""

import asyncio
import logging

from api.client import KalshiClient
from logger.trade_logger import TradeLogger
from risk.manager import RiskManager
from strategy.rules import Signal

logger = logging.getLogger(__name__)


class TradeExecutor:
    def __init__(self, client: KalshiClient, risk: RiskManager,
                 trade_logger: TradeLogger, config: dict):
        self._client   = client
        self._risk     = risk
        self._logger   = trade_logger
        self._dry_run  = bool(config.get("dry_run", False))
        self._queue: asyncio.Queue[Signal] = asyncio.Queue()
        self._stop_event = asyncio.Event()
        if self._dry_run:
            logger.info("[Executor] *** DRY-RUN MODUS – keine echten Orders ***")

    async def handle_signal(self, signal: Signal):
        await self._queue.put(signal)

    async def start(self):
        logger.info("[Executor] Gestartet")
        while not self._stop_event.is_set():
            try:
                signal = await asyncio.wait_for(self._queue.get(), timeout=1.0)
                await self._process(signal)
                self._queue.task_done()
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception(f"[Executor] Fehler: {e}")
                self._logger.log_error("Executor", str(e))
        logger.info("[Executor] Gestoppt")

    async def stop(self):
        self._stop_event.set()

    async def _process(self, signal: Signal):
        is_buy = signal.action == "buy"

        # Exit-Signale (sell) direkt weiterleiten – keine Risk-Checks
        if not is_buy:
            await self._execute_exit(signal)
            return

        self._risk.refresh_positions()
        allowed, reason = self._risk.check_order_allowed(
            signal.ticker, signal.count, signal.price_cents
        )
        if not allowed:
            logger.debug(f"[Executor] Abgelehnt | {signal.ticker}: {reason}")
            # "Bereits positioniert" ist im Dry-Run erwartetes Verhalten – kein echter Fehler
            if not reason.startswith("Bereits positioniert"):
                self._logger.log_error("RiskBlock", reason, extra={"ticker": signal.ticker})
            return

        logger.info(
            f"[Executor] {'[DRY] ' if self._dry_run else ''}Order | "
            f"{signal.ticker} {signal.action} {signal.side} "
            f"{signal.count}x @ {signal.price_cents}¢ | {signal.rule_name}"
        )

        if self._dry_run:
            self._logger.log_trade(
                ticker=signal.ticker,
                side=signal.side,
                price_cents=signal.price_cents,
                count=signal.count,
                order_id="DRY_RUN",
                status="dry_run",
                rule_name=signal.rule_name,
                extra={
                    "reason": signal.reason,
                    "dry_run": True,
                    "close_time": signal.meta.get("close_time", ""),
                    "title": signal.meta.get("title", ""),
                },
            )
            # Simulierte Position verbuchen – verhindert Duplikate in Folge-Zyklen
            self._risk.record_order(
                signal.ticker, signal.count, signal.price_cents, True,
                close_time=signal.meta.get("close_time", ""),
                side=signal.side,
                rule_name=signal.rule_name,
                title=signal.meta.get("title", ""),
                reason=signal.reason,
                category=signal.meta.get("category", ""),
                event_ticker=signal.meta.get("event_ticker", ""),
                sub_title=signal.meta.get("sub_title", ""),
                image_url=signal.meta.get("image_url", ""),
            )
            summary = self._risk.get_summary()
            logger.info(
                f"[Executor/DryRun] Positionen: {summary['open_positions']} | "
                f"Exposure: ${summary['total_exposure_usd']:.2f} / "
                f"${self._risk.max_total_exposure_usd:.0f}"
            )
            return

        loop = asyncio.get_event_loop()
        try:
            result = await loop.run_in_executor(
                None,
                lambda: self._client.place_order(
                    ticker=signal.ticker,
                    side=signal.side,
                    order_type="limit",
                    count=signal.count,
                    limit_price=signal.price_cents,
                    action="buy",
                ),
            )
        except Exception as e:
            logger.error(f"[Executor] Order fehlgeschlagen {signal.ticker}: {e}")
            self._logger.log_error("OrderFailed", str(e), extra={"ticker": signal.ticker})
            return

        order    = result.get("order", {})
        order_id = order.get("id")
        status   = order.get("status", "unknown")

        if status in ("resting", "filled", "partially_filled"):
            self._risk.record_order(signal.ticker, signal.count, signal.price_cents, True)

        self._logger.log_trade(
            ticker=signal.ticker,
            side=signal.side,
            price_cents=signal.price_cents,
            count=signal.count,
            order_id=order_id,
            status=status,
            rule_name=signal.rule_name,
            extra={"reason": signal.reason},
        )

    async def _execute_exit(self, signal: Signal):
        """Schließt eine Position (sell-Signal vom Exit-Scanner)."""
        prefix = "[DRY] " if self._dry_run else ""
        logger.info(
            f"[Executor] {prefix}EXIT | "
            f"{signal.ticker} sell {signal.side} "
            f"{signal.count}x @ {signal.price_cents}¢ | {signal.rule_name}"
        )

        if self._dry_run:
            self._logger.log_trade(
                ticker=signal.ticker,
                side=signal.side,
                price_cents=signal.price_cents,
                count=signal.count,
                order_id="DRY_EXIT",
                status="dry_run_exit",
                rule_name=signal.rule_name,
                extra={
                    "reason": signal.reason,
                    "dry_run": True,
                    "exit": True,
                    "entry_price_cents": signal.meta.get("entry_price_cents", 0),
                    "close_time": signal.meta.get("close_time", ""),
                },
            )
            # Position aus Risk-Manager entfernen
            self._risk.record_order(
                signal.ticker, signal.count, signal.price_cents, False  # is_buy=False
            )
            summary = self._risk.get_summary()
            logger.info(
                f"[Executor/DryRun] Position geschlossen | "
                f"Positionen: {summary['open_positions']} | "
                f"Exposure: ${summary['total_exposure_usd']:.2f}"
            )
            return

        loop = asyncio.get_event_loop()
        try:
            result = await loop.run_in_executor(
                None,
                lambda: self._client.place_order(
                    ticker=signal.ticker,
                    side=signal.side,
                    order_type="limit",
                    count=signal.count,
                    limit_price=signal.price_cents,
                    action="sell",
                ),
            )
        except Exception as e:
            logger.error(f"[Executor] Exit-Order fehlgeschlagen {signal.ticker}: {e}")
            self._logger.log_error("ExitFailed", str(e), extra={"ticker": signal.ticker})
            return

        order    = result.get("order", {})
        order_id = order.get("id")
        status   = order.get("status", "unknown")

        if status in ("resting", "filled", "partially_filled"):
            self._risk.record_order(signal.ticker, signal.count, signal.price_cents, False)

        self._logger.log_trade(
            ticker=signal.ticker,
            side=signal.side,
            price_cents=signal.price_cents,
            count=signal.count,
            order_id=order_id,
            status=status,
            rule_name=signal.rule_name,
            extra={"reason": signal.reason, "exit": True},
        )
