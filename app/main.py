"""
Kalshi Trading Bot – Dual-System-Architektur
  System 1: Prediction Markets (politisch, wirtschaftlich)
  System 2: Crypto Markets (BTC/ETH Leiter + 15-Min)
"""

import asyncio
import json
import logging
import os
import signal
import sys
from pathlib import Path

from dotenv import load_dotenv
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
from crypto.scanner import CryptoScanner
from logger.trade_logger import TradeLogger
from prediction.scanner import PredictionScanner
from risk.manager import RiskManager
from settlement.tracker import SettlementTracker
from trader.executor import TradeExecutor


def load_config() -> dict:
    config_path = Path(__file__).parent / "config.json"
    try:
        with open(config_path) as f:
            return json.load(f)
    except FileNotFoundError:
        logger.error(f"[Config] config.json nicht gefunden: {config_path}")
        raise SystemExit(1)
    except json.JSONDecodeError as e:
        logger.error(f"[Config] config.json ungültiges JSON: {e}")
        raise SystemExit(1)


def validate_config(config: dict) -> list[str]:
    errors = []
    systems = config.get("systems", {})
    for sys_name in ("prediction", "crypto"):
        s = systems.get(sys_name, {})
        if float(s.get("max_exposure_usd", 80)) <= 0:
            errors.append(f"systems.{sys_name}.max_exposure_usd muss > 0 sein")
    scan_s = config.get("prediction_scanner", {}).get("interval_seconds", 60)
    if scan_s < 10:
        errors.append("prediction_scanner.interval_seconds muss >= 10 sein")
    return errors


async def main():
    config = load_config()
    errors = validate_config(config)
    if errors:
        for e in errors:
            logger.error(f"[Config] {e}")
        raise SystemExit(f"Config ungültig: {len(errors)} Fehler")

    api_key_id  = os.getenv("KALSHI_API_KEY_ID", "")
    private_key = os.getenv("KALSHI_PRIVATE_KEY", "")
    if not api_key_id or not private_key:
        logger.error("KALSHI_API_KEY_ID und KALSHI_PRIVATE_KEY müssen in .env gesetzt sein!")
        sys.exit(1)

    logger.info("=" * 60)
    logger.info("  Kalshi Trading Bot – Dual-System")
    logger.info("  System 1: Prediction Markets")
    logger.info("  System 2: Crypto Markets")
    logger.info("=" * 60)

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

    try:
        bal = client.get_balance()
        balance_usd = bal.get("balance", 0) / 100
        logger.info(f"[Main] Balance: ${balance_usd:.2f}")
        if balance_usd < 1.0:
            trade_logger.log_warning("Balance", f"Guthaben niedrig: ${balance_usd:.2f}")
    except Exception as e:
        trade_logger.log_warning("Balance", f"Balance-Abfrage fehlgeschlagen: {e}")

    # ── Shared Infrastruktur ──────────────────────────────────────────
    risk       = RiskManager(client, config)
    executor   = TradeExecutor(client, risk, trade_logger, config)
    settlement = SettlementTracker(client, trade_logger, config, risk=risk)

    systems_cfg = config.get("systems", {})

    # ── System 1: Prediction Scanner ─────────────────────────────────
    prediction_scanner = None
    if systems_cfg.get("prediction", {}).get("enabled", True):
        prediction_scanner = PredictionScanner(
            client=client,
            trade_logger=trade_logger,
            config=config,
            on_signal=executor.handle_signal,
            on_meta=risk.update_detail,
            on_cycle_end=executor.handle_cycle_end,
        )
        logger.info("[Main] System 1 (Prediction Markets) aktiv")

    # ── System 2: Crypto Scanner ──────────────────────────────────────
    crypto_scanner = None
    if systems_cfg.get("crypto", {}).get("enabled", True):
        crypto_scanner = CryptoScanner(
            client=client,
            trade_logger=trade_logger,
            config=config,
            on_signal=executor.handle_signal,
            on_meta=risk.update_detail,
            on_cycle_end=executor.handle_cycle_end,
        )
        logger.info("[Main] System 2 (Crypto Markets) aktiv")

    trade_logger.log_system("BOOT", "Dual-System Bot startet")

    shutdown_event = asyncio.Event()
    loop           = asyncio.get_running_loop()

    def handle_shutdown(sig_name):
        logger.info(f"[Main] {sig_name} – fahre herunter...")
        trade_logger.log_system("SHUTDOWN", sig_name)
        shutdown_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, lambda s=sig.name: handle_shutdown(s))

    async def settlement_loop():
        while not shutdown_event.is_set():
            try:
                await settlement.check()
            except Exception as e:
                logger.warning(f"[Main/Settlement] Fehler: {e}")
            try:
                await asyncio.wait_for(shutdown_event.wait(), timeout=60.0)
            except asyncio.TimeoutError:
                pass

    async def heartbeat_loop():
        while not shutdown_event.is_set():
            try:
                risk._save_positions()
            except Exception as e:
                logger.debug(f"[Main/Heartbeat] Fehler: {e}")
            try:
                await asyncio.wait_for(shutdown_event.wait(), timeout=60.0)
            except asyncio.TimeoutError:
                pass

    tasks = [
        asyncio.create_task(executor.start(),       name="executor"),
        asyncio.create_task(settlement_loop(),       name="settlement"),
        asyncio.create_task(heartbeat_loop(),        name="heartbeat"),
        asyncio.create_task(shutdown_event.wait(),   name="shutdown"),
    ]
    if prediction_scanner:
        tasks.append(asyncio.create_task(prediction_scanner.start(), name="prediction"))
    if crypto_scanner:
        tasks.append(asyncio.create_task(crypto_scanner.start(), name="crypto"))

    logger.info(f"[Main] Bot läuft mit {len(tasks) - 4} aktivem/n Scanner-System(en).")
    await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)

    # Sauber stoppen
    if prediction_scanner:
        await prediction_scanner.stop()
    if crypto_scanner:
        await crypto_scanner.stop()
    await executor.stop()

    for t in tasks:
        if not t.done():
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass

    try:
        cancelled = await asyncio.get_running_loop().run_in_executor(
            None, client.cancel_all_orders
        )
        if cancelled:
            logger.info(f"[Main] {cancelled} offene Order(s) storniert")
    except Exception as e:
        logger.warning(f"[Main] cancel_all_orders fehlgeschlagen: {e}")

    trade_logger.log_system("STOPPED", "Bot beendet")
    logger.info("[Main] Bot beendet.")


if __name__ == "__main__":
    asyncio.run(main())
