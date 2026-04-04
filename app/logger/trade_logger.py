"""JSONL Trade Logger – analog zu BingX/Hyperliquid Projekten."""

import json
import logging
import atexit
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, List

logger = logging.getLogger(__name__)

MAX_FILE_SIZE  = 10 * 1024 * 1024
MAX_KEEP_FILES = 3
_FILE_HANDLES: dict[str, object] = {}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _rotate_if_needed(filepath: str):
    path = Path(filepath)
    if not path.exists() or path.stat().st_size < MAX_FILE_SIZE:
        return
    handle = _FILE_HANDLES.pop(filepath, None)
    if handle:
        try: handle.flush(); handle.close()
        except Exception: pass
    oldest = Path(f"{filepath}.{MAX_KEEP_FILES}")
    if oldest.exists():
        oldest.unlink()
    for i in range(MAX_KEEP_FILES - 1, 0, -1):
        src, dst = Path(f"{filepath}.{i}"), Path(f"{filepath}.{i+1}")
        if src.exists(): src.rename(dst)
    path.rename(Path(f"{filepath}.1"))


def _get_handle(filepath: str):
    if filepath not in _FILE_HANDLES:
        Path(filepath).parent.mkdir(parents=True, exist_ok=True)
        _FILE_HANDLES[filepath] = open(filepath, "a", encoding="utf-8")
        atexit.register(lambda: [h.close() for h in _FILE_HANDLES.values()])
    return _FILE_HANDLES[filepath]


def _write(filepath: str, record: dict):
    _rotate_if_needed(filepath)
    h = _get_handle(filepath)
    h.write(json.dumps(record, ensure_ascii=False) + "\n")
    h.flush()


class TradeLogger:
    def __init__(self, trades_file="data/trades.jsonl",
                 signals_file="data/signals.jsonl", errors_file="data/errors.jsonl"):
        self._trades  = trades_file
        self._signals = signals_file
        self._errors  = errors_file

    def log_signal(self, ticker: str, rule_name: str, side: str, price_cents: int,
                   count: int, reason: str, extra: Optional[dict] = None):
        record = {"ts": _now_iso(), "type": "SIGNAL", "ticker": ticker,
                  "rule": rule_name, "side": side, "price_cents": price_cents,
                  "count": count, "reason": reason}
        if extra: record.update(extra)
        _write(self._signals, record)
        logger.info(f"[Signal] {ticker} | {rule_name} | {side} @ {price_cents}¢ x{count}")

    def log_trade(self, ticker: str, side: str, price_cents: int, count: int,
                  order_id: Optional[str], status: str, rule_name: str,
                  extra: Optional[dict] = None):
        record = {"ts": _now_iso(), "type": "TRADE", "ticker": ticker,
                  "side": side, "price_cents": price_cents, "count": count,
                  "order_id": order_id, "status": status, "rule": rule_name}
        if extra: record.update(extra)
        _write(self._trades, record)
        logger.info(f"[Trade] {status} | {ticker} {side} @ {price_cents}¢ x{count} oid={order_id}")

    def log_warning(self, context: str, message: str, extra: Optional[dict] = None):
        record = {"ts": _now_iso(), "type": "WARNING", "context": context, "message": message}
        if extra: record.update(extra)
        _write(self._errors, record)
        logger.warning(f"[Warning] {context}: {message}")

    def log_error(self, context: str, message: str, extra: Optional[dict] = None):
        record = {"ts": _now_iso(), "type": "ERROR", "context": context, "message": message}
        if extra: record.update(extra)
        _write(self._errors, record)
        logger.error(f"[Error] {context}: {message}")

    def log_system(self, event: str, message: str, extra: Optional[dict] = None):
        record = {"ts": _now_iso(), "type": "SYSTEM", "event": event, "message": message}
        if extra: record.update(extra)
        _write(self._trades, record)
        logger.info(f"[System] {event}: {message}")

    def log_settlement(self, ticker: str, side: str, price_cents: int, count: int,
                       result: str, won: bool, invested_usd: float,
                       gross_return_usd: float, fee_usd: float,
                       net_return_usd: float, pnl_usd: float,
                       rule: str = "", reason: str = "", title: str = "",
                       close_time: str = "", balance_before_usd: float = 0.0,
                       balance_after_usd: float = 0.0):
        record = {
            "ts": _now_iso(), "type": "SETTLEMENT",
            "ticker": ticker, "side": side,
            "price_cents": price_cents, "count": count,
            "result": result, "won": won,
            "invested_usd": round(invested_usd, 4),
            "gross_return_usd": round(gross_return_usd, 4),
            "fee_usd": round(fee_usd, 4),
            "net_return_usd": round(net_return_usd, 4),
            "pnl_usd": round(pnl_usd, 4),
            "rule": rule, "reason": reason, "title": title,
            "close_time": close_time,
            "balance_before_usd": round(balance_before_usd, 4),
            "balance_after_usd": round(balance_after_usd, 4),
        }
        _write(self._trades, record)
        sign = "+" if pnl_usd >= 0 else ""
        outcome = "WIN " if won else "LOSS"
        logger.info(
            f"[Settlement] {outcome} | {ticker} {side}@{price_cents}¢×{count} "
            f"result={result} | P&L {sign}{pnl_usd:.2f}$ | "
            f"Balance {balance_before_usd:.2f}$→{balance_after_usd:.2f}$"
        )

    def read_all(self, entry_type: Optional[str] = None) -> List[dict]:
        """Liest alle Einträge aus trades.jsonl inkl. rotierter Dateien (älteste zuerst)."""
        files = [self._trades]
        for i in range(1, MAX_KEEP_FILES + 1):
            rotated = f"{self._trades}.{i}"
            if Path(rotated).exists():
                files.append(rotated)
        entries: List[dict] = []
        for fp in reversed(files):   # älteste zuerst
            try:
                with open(fp, encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            entry = json.loads(line)
                            if entry_type is None or entry.get("type") == entry_type:
                                entries.append(entry)
                        except Exception:
                            pass
            except FileNotFoundError:
                pass
        return entries
