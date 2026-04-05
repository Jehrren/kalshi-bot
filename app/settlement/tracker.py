"""
Settlement Tracker für Kalshi Prediction Markets.

Ablauf nach jedem Scan-Zyklus:
  1. Liest alle TRADE-Einträge ohne passendes SETTLEMENT
  2. Prüft ob close_time + Delay verstrichen ist
  3. Ruft GET /markets/{ticker} für das Ergebnis ab (result: "yes" | "no")
  4. Berechnet P&L inkl. Gebühren (fee_pct% des Einsatzes bei Gewinn)
  5. Schreibt SETTLEMENT-Eintrag in trades.jsonl
  6. Aktualisiert balance.json

Gebühren-Modell (Kalshi-Standard):
  fee = gross_return_usd × fee_pct / 100  (nur bei gewonnenem Trade)
  Kalshi berechnet ~1% des Brutto-Auszahlungsbetrags (count × $1) auf der Gewinnerseite.
"""

import asyncio
import json
import logging
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from api.client import KalshiClient
from logger.trade_logger import TradeLogger
from risk.manager import RiskManager

logger = logging.getLogger(__name__)

BALANCE_FILE = Path("data/balance.json")


class SettlementTracker:
    def __init__(self, client: KalshiClient, trade_logger: TradeLogger, config: dict,
                 risk: RiskManager | None = None):
        self._client = client
        self._logger = trade_logger
        self._risk   = risk
        s_cfg = config.get("settlement", {})
        self._fee_pct   = float(s_cfg.get("fee_pct", 1.0))
        self._delay_s   = int(s_cfg.get("delay_seconds", 300))   # 5 Min nach Marktschluss
        self._start_bal = float(s_cfg.get("starting_balance_usd", 200.0))
        self._balance   = self._load_or_init_balance()
        self._settled   = self._load_settled_tickers()
        # RiskManager über bereits abgerechnete Ticker informieren (Zombie-Schutz)
        if self._risk and self._settled:
            self._risk.set_settled_tickers(self._settled)
        # Verlorene Positionen wiederherstellen (Geister-Schutz)
        if self._risk:
            self._reconcile_ghosts()

    # ── Reconciliation ─────────────────────────────────────────────────── #

    def _reconcile_ghosts(self):
        """
        Findet Geister-Trades: im TRADE-Log vorhanden, aber weder in
        positions.json (offen) noch in settlements (abgeschlossen).
        Stellt diese Positionen im RiskManager wieder her, damit sie
        korrekt getrackt und später settled werden können.
        """
        # Log einmal lesen → konsistenter Snapshot (kein TOCTOU)
        all_trades = self._logger.read_all("TRADE")
        exited = {
            t.get("ticker", "") for t in all_trades
            if t.get("status") == "dry_run_exit"
        }
        entry_trades: dict[str, dict] = {}
        for t in all_trades:
            if t.get("status") not in ("dry_run", "resting", "filled", "partially_filled"):
                continue
            ticker = t.get("ticker", "")
            if ticker and ticker not in entry_trades:
                entry_trades[ticker] = t

        # Geister = im Log, nicht settled, nicht exited, nicht im RiskManager
        open_tickers = self._risk.get_open_tickers()
        ghosts = set(entry_trades.keys()) - self._settled - exited - open_tickers

        if not ghosts:
            return

        restored = 0
        skipped_ghosts: list[str] = []
        for ticker in ghosts:
            trade = entry_trades[ticker]
            count = int(trade.get("count", 0))
            price_cents = int(trade.get("price_cents", 0))
            if not count or not price_cents:
                skipped_ghosts.append(ticker)
                continue
            self._risk.record_order(
                ticker, count, price_cents, True,
                close_time  = trade.get("close_time", ""),
                side        = trade.get("side", ""),
                rule_name   = trade.get("rule", ""),
                title       = trade.get("title", ""),
                event_title = trade.get("event_title", ""),
                reason      = trade.get("reason", ""),
                category    = trade.get("category", ""),
                event_ticker= trade.get("event_ticker", ""),
                sub_title   = trade.get("sub_title", ""),
                image_url   = trade.get("image_url", ""),
                system      = trade.get("system", ""),
            )
            restored += 1

        if restored:
            logger.info(f"[Settlement] {restored} Geister-Position(en) wiederhergestellt: "
                        f"{sorted(ghosts - set(skipped_ghosts))}")
        if skipped_ghosts:
            logger.warning(f"[Settlement] {len(skipped_ghosts)} Geister-Trade(s) nicht "
                           f"wiederherstellbar (count/price=0): {skipped_ghosts}")

    # ── Balance ────────────────────────────────────────────────────────── #

    def _load_or_init_balance(self) -> dict:
        if BALANCE_FILE.exists():
            try:
                data = json.loads(BALANCE_FILE.read_text())
                # Altes Format (balance_usd statt current_usd) migrieren
                if "current_usd" not in data:
                    starting = float(data.get("balance_usd", self._start_bal))
                    data = {
                        "starting_usd":       starting,
                        "current_usd":        starting,
                        "total_invested_usd": 0.0,
                        "total_returned_usd": 0.0,
                        "total_fees_usd":     0.0,
                        "total_pnl_usd":      0.0,
                        "trades_settled":     0,
                        "wins":               0,
                        "losses":             0,
                        "updated_at":         None,
                    }
                    self._write_balance(data)
                return data
            except Exception as e:
                logger.warning(f"[Settlement] balance.json unlesbar, wird neu erstellt: {e}")
        bal = {
            "starting_usd":       self._start_bal,
            "current_usd":        self._start_bal,
            "total_invested_usd": 0.0,
            "total_returned_usd": 0.0,
            "total_fees_usd":     0.0,
            "total_pnl_usd":      0.0,
            "trades_settled":     0,
            "wins":               0,
            "losses":             0,
            "updated_at":         datetime.now(timezone.utc).isoformat(),
        }
        self._write_balance(bal)
        return bal

    def _write_balance(self, bal: dict):
        BALANCE_FILE.parent.mkdir(parents=True, exist_ok=True)
        bal["updated_at"] = datetime.now(timezone.utc).isoformat()
        content = json.dumps(bal, ensure_ascii=False, indent=2)
        tmp = BALANCE_FILE.with_suffix(".tmp")
        tmp.write_text(content, encoding="utf-8")
        tmp.replace(BALANCE_FILE)

    def get_balance(self) -> dict:
        return dict(self._balance)

    # ── Trade-Erkennung ────────────────────────────────────────────────── #

    def _load_settled_tickers(self) -> set:
        return {e.get("ticker", "") for e in self._logger.read_all("SETTLEMENT")}

    def _pending_trades(self, now: datetime, all_trades: list) -> list[dict]:
        """
        Entry-Trades die noch kein SETTLEMENT haben, nicht exited wurden
        und deren close_time + Delay abgelaufen ist.
        """
        exited = {
            e.get("ticker", "") for e in all_trades
            if e.get("status") == "dry_run_exit"
        }
        # Alle Entry-Buys: eine Zeile pro Ticker reicht (kein Averaging in Dry-Run)
        seen: dict[str, dict] = {}
        for e in all_trades:
            if e.get("status") not in ("dry_run", "resting", "filled", "partially_filled"):
                continue
            ticker = e.get("ticker", "")
            if not ticker or ticker in self._settled or ticker in exited:
                continue
            ct_str = e.get("close_time", "")
            if not ct_str:
                continue   # Kein close_time → kann nicht geprüft werden
            if ticker not in seen:
                seen[ticker] = {**e, "_count_total": 0}
            seen[ticker]["_count_total"] += int(e.get("count", 0))

        result = []
        for ticker, t in seen.items():
            try:
                ct = datetime.fromisoformat(t["close_time"].replace("Z", "+00:00"))
            except Exception:
                continue
            if (now - ct).total_seconds() < self._delay_s:
                continue   # Noch zu früh – Kalshi braucht Zeit zum Settleln
            t["count"] = t["_count_total"]
            result.append(t)
        return result

    # ── Haupt-Check ────────────────────────────────────────────────────── #

    async def check(self):
        """Im Scan-Zyklus aufrufen – prüft Exits und abgelaufene Märkte."""
        now = datetime.now(timezone.utc)
        # Einmal lesen → konsistenter Snapshot für alle Sub-Methoden
        all_trades = self._logger.read_all("TRADE")
        # 1. Exit-P&L verbuchen (Take-Profit / Stop-Loss die bereits ausgeführt wurden)
        self._process_exits(all_trades)
        # 2. Abgelaufene Märkte via API abrechnen
        pending = self._pending_trades(now, all_trades)
        if not pending:
            return
        logger.info(f"[Settlement] {len(pending)} abgelaufene Position(en) werden geprüft")
        loop = asyncio.get_running_loop()
        for trade in pending:
            ticker = trade.get("ticker", "")
            try:
                market = await loop.run_in_executor(
                    None, lambda t=ticker: self._client.get_market(t)
                )
                result = market.get("result")
                if result not in ("yes", "no"):
                    logger.debug(f"[Settlement] {ticker} noch nicht settled (result={result!r})")
                    continue
                self._settle(trade, result)
            except Exception as e:
                logger.warning(f"[Settlement] {ticker} API-Fehler: {e}")

    def _process_exits(self, all_trades: list):
        """
        Verbucht P&L für Positionen die aktiv geschlossen wurden (Exit-Trades).
        Entry-Preis vs. Exit-Preis → realisierter P&L, keine Gebühr (kein Settlement).
        """
        entries = {
            t["ticker"]: t for t in all_trades
            if t.get("status") in ("dry_run", "resting", "filled", "partially_filled")
        }
        for ex in all_trades:
            if ex.get("status") != "dry_run_exit":
                continue
            ticker = ex.get("ticker", "")
            if ticker in self._settled:
                continue
            # Guard sofort setzen → verhindert Doppelverarbeitung bei Duplikaten im Log
            self._settled.add(ticker)
            entry = entries.get(ticker)
            if not entry:
                # Kein Entry-Trade gefunden → trotzdem aus RiskManager entfernen (Zombie-Schutz)
                if self._risk:
                    self._risk.mark_settled(ticker)
                continue
            entry_px  = int(entry.get("price_cents", 0))
            entry_cnt = int(entry.get("count", 0))
            exit_px   = int(ex.get("price_cents", 0))
            exit_cnt  = int(ex.get("count", 0))
            if not entry_px or not entry_cnt:
                continue
            invested_usd   = round(exit_cnt * entry_px / 100, 4)
            received_usd   = round(exit_cnt  * exit_px  / 100, 4)
            pnl_usd        = round(received_usd - invested_usd, 4)
            won            = pnl_usd > 0

            bal_before = round(self._balance["current_usd"], 4)
            new_bal = {
                **self._balance,
                "current_usd":        round(self._balance["current_usd"]        + pnl_usd,       4),
                "total_invested_usd": round(self._balance["total_invested_usd"] + invested_usd,  4),
                "total_returned_usd": round(self._balance["total_returned_usd"] + received_usd,  4),
                "total_pnl_usd":      round(self._balance["total_pnl_usd"]      + pnl_usd,       4),
                "trades_settled":     self._balance["trades_settled"] + 1,
                "wins":               self._balance["wins"]   + (1 if won else 0),
                "losses":             self._balance["losses"] + (0 if won else 1),
            }
            self._write_balance(new_bal)
            self._balance = new_bal

            self._logger.log_settlement(
                ticker=ticker, side=entry.get("side", ""),
                price_cents=entry_px, count=entry_cnt,
                result="exit", won=won,
                invested_usd=invested_usd,
                gross_return_usd=received_usd,
                fee_usd=0.0,
                net_return_usd=received_usd,
                pnl_usd=pnl_usd,
                rule=entry.get("rule", ""),
                reason=ex.get("reason", ex.get("rule", "")),
                title=entry.get("title", ""),
                close_time=entry.get("close_time", ""),
                balance_before_usd=bal_before,
                balance_after_usd=new_bal["current_usd"],
            )
            if self._risk:
                self._risk.mark_settled(ticker)

    # ── Abrechnung ─────────────────────────────────────────────────────── #

    def _settle(self, trade: dict, result: str):
        ticker      = trade["ticker"]
        side        = trade.get("side", "yes")
        price_cents = int(trade.get("price_cents", 50))
        count       = int(trade.get("count", 0))
        rule        = trade.get("rule", "")
        reason      = trade.get("reason", "")
        title       = trade.get("title", "")
        close_time  = trade.get("close_time", "")

        # Kernrechnung
        invested_usd     = round(count * price_cents / 100, 4)
        won              = (result == side)
        gross_return_usd = round(count * 1.0, 4) if won else 0.0
        # Gebühr: fee_pct% des Brutto-Auszahlungsbetrags bei Gewinn (Kalshi ~1% des Payouts)
        fee_usd          = round(gross_return_usd * self._fee_pct / 100, 4) if won else 0.0
        net_return_usd   = round(gross_return_usd - fee_usd, 4)
        pnl_usd          = round(net_return_usd - invested_usd, 4)

        # Balance aktualisieren (immutable – neues Dict statt in-place Mutation)
        bal_before = round(self._balance["current_usd"], 4)
        new_bal = {
            **self._balance,
            "current_usd":        round(self._balance["current_usd"]        + pnl_usd,          4),
            "total_invested_usd": round(self._balance["total_invested_usd"] + invested_usd,      4),
            "total_returned_usd": round(self._balance["total_returned_usd"] + net_return_usd,    4),
            "total_fees_usd":     round(self._balance["total_fees_usd"]     + fee_usd,           4),
            "total_pnl_usd":      round(self._balance["total_pnl_usd"]      + pnl_usd,           4),
            "trades_settled":     self._balance["trades_settled"] + 1,
            "wins":               self._balance["wins"]   + (1 if won else 0),
            "losses":             self._balance["losses"] + (0 if won else 1),
        }
        self._write_balance(new_bal)
        self._balance = new_bal

        self._logger.log_settlement(
            ticker=ticker, side=side, price_cents=price_cents, count=count,
            result=result, won=won,
            invested_usd=invested_usd, gross_return_usd=gross_return_usd,
            fee_usd=fee_usd, net_return_usd=net_return_usd, pnl_usd=pnl_usd,
            rule=rule, reason=reason, title=title, close_time=close_time,
            balance_before_usd=bal_before, balance_after_usd=new_bal["current_usd"],
        )
        self._settled.add(ticker)
        if self._risk:
            self._risk.mark_settled(ticker)

    # ── Statistiken ────────────────────────────────────────────────────── #

    def get_stats(self) -> dict:
        """
        Berechnet konsistente Handelsstatistiken aus trades.jsonl + positions.json.
        Einzige Quelle der Wahrheit – alle Dashboards/UIs sollten diese Methode nutzen.
        """
        all_trades = self._logger.read_all("TRADE")
        settlements = self._logger.read_all("SETTLEMENT")

        # Entry-Trades: einzigartige Ticker (= tatsächlich eröffnete Positionen)
        entry_tickers: set[str] = set()
        for t in all_trades:
            if t.get("status") in ("dry_run", "resting", "filled", "partially_filled"):
                ticker = t.get("ticker", "")
                if ticker:
                    entry_tickers.add(ticker)

        # Settled Ticker (abgeschlossen per Markt-Settlement oder Exit)
        settled_tickers: set[str] = set()
        pnl_list: list[float] = []
        for s in settlements:
            ticker = s.get("ticker", "")
            if ticker:
                settled_tickers.add(ticker)
                pnl_list.append(float(s.get("pnl_usd", 0.0)))

        # Offene Positionen aus RiskManager (live) oder Differenz
        if self._risk:
            open_count = self._risk.get_open_count()
            open_exposure = round(self._risk.get_total_exposure(), 2)
            max_profit_open = self._risk.get_max_profit_open_usd()
        else:
            open_count = len(entry_tickers - settled_tickers)
            open_exposure = 0.0
            max_profit_open = 0.0

        return {
            "trades_total": len(entry_tickers),
            "trades_open": open_count,
            "trades_closed": len(settled_tickers),
            "open_exposure_usd": open_exposure,
            "max_profit_open_usd": max_profit_open,
            "best_single_pnl_usd": round(max(pnl_list), 4) if pnl_list else 0.0,
            "worst_single_pnl_usd": round(min(pnl_list), 4) if pnl_list else 0.0,
            "balance": dict(self._balance),
        }
