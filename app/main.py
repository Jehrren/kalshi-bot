"""
Kalshi Prediction Market Trading Bot
Regelbasiertes automatisiertes Trading – Einstiegspunkt
"""

import asyncio
import json
import logging
import os
import signal
import sys
from pathlib import Path

from dotenv import load_dotenv
from pathlib import Path
load_dotenv(Path(__file__).parent.parent / ".env")

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("main")

from api.client import KalshiClient
from logger.trade_logger import TradeLogger
from risk.manager import RiskManager
from settlement.tracker import SettlementTracker
from strategy.scanner import MarketScanner
from trader.executor import TradeExecutor


def load_config() -> dict:
    with open(Path(__file__).parent / "config.json") as f:
        return json.load(f)


def validate_config(config: dict) -> list[str]:
    errors = []
    risk = config.get("risk", {})
    if risk.get("max_position_usd", 50) <= 0:
        errors.append("risk.max_position_usd muss > 0 sein")
    if risk.get("max_total_exposure_usd", 200) <= 0:
        errors.append("risk.max_total_exposure_usd muss > 0 sein")
    if config.get("scanner", {}).get("interval_seconds", 60) < 10:
        errors.append("scanner.interval_seconds muss >= 10 sein")
    return errors


async def main():
    config = load_config()
    errors = validate_config(config)
    if errors:
        for e in errors: logger.error(f"[Config] {e}")
        raise SystemExit(f"Config ungültig: {len(errors)} Fehler")

    api_key_id   = os.getenv("KALSHI_API_KEY_ID", "")
    private_key  = os.getenv("KALSHI_PRIVATE_KEY", "")

    if not api_key_id or not private_key:
        logger.error("KALSHI_API_KEY_ID und KALSHI_PRIVATE_KEY müssen in .env gesetzt sein!")
        sys.exit(1)

    logger.info("=" * 55)
    logger.info("  Kalshi Prediction Market Bot")
    logger.info("=" * 55)

    # Exchange-Status prüfen
    client = KalshiClient(api_key_id, private_key)
    try:
        status = client.get_exchange_status()
        logger.info(f"[Main] Exchange: active={status.get('exchange_active')} "
                    f"trading={status.get('trading_active')}")
    except Exception as e:
        logger.warning(f"[Main] Exchange-Status fehlgeschlagen: {e}")

    log_cfg = config.get("logging", {})
    trade_logger = TradeLogger(
        trades_file  = log_cfg.get("trades_file", "data/trades.jsonl"),
        signals_file = log_cfg.get("signals_file", "data/signals.jsonl"),
        errors_file  = log_cfg.get("errors_file", "data/errors.jsonl"),
    )

    # Balance anzeigen
    try:
        bal = client.get_balance()
        balance_usd = bal.get("balance", 0) / 100
        logger.info(f"[Main] Balance: ${balance_usd:.2f}")
        if balance_usd < 1.0:
            trade_logger.log_warning("Balance", f"Kalshi-Konto-Guthaben niedrig: ${balance_usd:.2f} – bitte aufladen")
    except Exception as e:
        trade_logger.log_warning("Balance", f"Balance-Abfrage fehlgeschlagen: {e}")

    risk       = RiskManager(client, config)
    executor   = TradeExecutor(client, risk, trade_logger, config)
    settlement = SettlementTracker(client, trade_logger, config)
    scanner    = MarketScanner(
        client=client, trade_logger=trade_logger,
        config=config, on_signal=executor.handle_signal,
    )

    trade_logger.log_system("BOOT", "Bot startet", extra={"config": config})

    shutdown_event = asyncio.Event()
    loop = asyncio.get_event_loop()

    def handle_shutdown(sig_name):
        logger.info(f"[Main] {sig_name} – fahre herunter...")
        trade_logger.log_system("SHUTDOWN", sig_name)
        shutdown_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, lambda s=sig.name: handle_shutdown(s))

    async def settlement_loop():
        """Läuft unabhängig vom Scanner – prüft Settlements jede Minute."""
        while not shutdown_event.is_set():
            try:
                await settlement.check()
            except Exception as e:
                logger.warning(f"[Main/Settlement] Fehler: {e}")
            try:
                await asyncio.wait_for(shutdown_event.wait(), timeout=60.0)
            except asyncio.TimeoutError:
                pass

    tasks = [
        asyncio.create_task(scanner.start(),     name="scanner"),
        asyncio.create_task(executor.start(),    name="executor"),
        asyncio.create_task(settlement_loop(),   name="settlement"),
        asyncio.create_task(shutdown_event.wait(), name="shutdown"),
    ]

    logger.info("[Main] Bot läuft.")
    await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)

    await scanner.stop()
    await executor.stop()
    for t in tasks:
        t.cancel()
        try: await t
        except asyncio.CancelledError: pass

    trade_logger.log_system("STOPPED", "Bot beendet")
    logger.info("[Main] Bot beendet.")


if __name__ == "__main__":
    asyncio.run(main())
