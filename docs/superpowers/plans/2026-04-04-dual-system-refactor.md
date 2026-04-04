# Dual-System Refactor: Prediction Markets + Crypto Markets

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Den Bot in zwei vollständig getrennte, eigenständige Handelssysteme aufteilen – System 1 für allgemeine Prediction Markets (Polymarket-basierte Regeln), System 2 für Crypto-Märkte (RSI/Distanz-basierte Regeln) – mit gemeinsamer Infrastruktur (API, Logging, Risk, Executor).

**Architecture:** Jedes System hat ein eigenes `rules.py` (Regeln) und `scanner.py` (Scanner + Exit-Logik). Beide Scanner senden Signale an denselben `TradeExecutor` und denselben `RiskManager`, aber mit eigenem Exposure-Budget. Die alte `app/strategy/` bleibt bis alle Tasks abgeschlossen sind, wird dann gelöscht.

**Tech Stack:** Python 3.11, asyncio, Kalshi REST/WS API, BingX Feed, pytest

---

## Datei-Map (Überblick)

### Neue Dateien
| Datei | Verantwortung |
|---|---|
| `app/prediction/__init__.py` | Package-Marker |
| `app/prediction/rules.py` | Polymarket-basiertes Regelwerk (Overconfidence-Korrektur, NO-Bias, Zeit-Filter) |
| `app/prediction/scanner.py` | Scanner für politische/wirtschaftliche Märkte + Exit-Logik |
| `app/crypto/__init__.py` | Package-Marker |
| `app/crypto/rules.py` | Crypto-Regelwerk (RSI, Distanz-Filter, Range-Fix, Stop-Loss-Fix) |
| `app/crypto/scanner.py` | Scanner für BTC/ETH Leiter + 15-Min Märkte + Exit-Logik |

### Geänderte Dateien
| Datei | Was ändert sich |
|---|---|
| `app/config.json` | Komplett neu strukturiert: `prediction_rules`, `crypto_ladder_rules`, `crypto_15min_rules`, getrennte Budgets |
| `app/risk/manager.py` | System-Tag (`prediction` / `crypto`) in Positionen; getrennte Exposure-Limits |
| `app/trader/executor.py` | System-Tag im Logging und `record_order` |
| `app/main.py` | Startet beide Scanner parallel; alte MarketScanner-Referenz entfernt |

### Gelöschte Dateien (nach Migration)
| Datei | Grund |
|---|---|
| `app/strategy/rules.py` | Durch `prediction/rules.py` + `crypto/rules.py` ersetzt |
| `app/strategy/scanner.py` | Durch `prediction/scanner.py` + `crypto/scanner.py` ersetzt |

---

## Task 1: Config.json neu strukturieren

**Ziel:** Eine saubere Konfiguration, die beide Systeme vollständig trennt. Alle bekannten Bugs (Range-Filter fehlt bei Crypto, Stop-Loss zu eng) werden hier konfigurativ gelöst.

**Files:**
- Modify: `app/config.json` (vollständig neu schreiben)

- [ ] **Schritt 1.1: Config sichern und neu schreiben**

```bash
cp app/config.json app/config.json.bak
```

Inhalt von `app/config.json` komplett ersetzen mit:

```json
{
    "dry_run": true,

    "settlement": {
        "fee_pct": 1.0,
        "delay_seconds": 300,
        "starting_balance_usd": 200.0
    },

    "systems": {
        "prediction": {
            "enabled": true,
            "max_exposure_usd": 80.0,
            "max_position_usd": 20.0,
            "max_open_positions": 15
        },
        "crypto": {
            "enabled": true,
            "max_exposure_usd": 80.0,
            "max_position_usd": 20.0,
            "max_open_positions": 10
        }
    },

    "prediction_scanner": {
        "interval_seconds": 60,
        "max_close_days": 30,
        "min_volume_usd": 100,
        "categories": [
            "economics", "financials", "politics", "elections", "companies",
            "world", "health", "science and technology", "entertainment",
            "social", "climate and weather", "transportation"
        ]
    },

    "prediction_rules": [
        {
            "name": "Filter – 50/50-Zone (43–57¢)",
            "description": "Schlechteste Kalibrierung: bis zu 7% YES-Bias, kein Edge",
            "enabled": true,
            "condition": { "type": "yes_ask_between", "threshold_low": 43, "threshold_high": 57 },
            "action": { "side": "SKIP" }
        },
        {
            "name": "Filter – Min. Volumen $100",
            "description": "Tote Märkte ohne aktiven Counterparty überspringen",
            "enabled": true,
            "condition": { "type": "min_volume_usd", "threshold": 100 },
            "action": { "side": "SKIP" }
        },
        {
            "name": "Filter – Min. Open Interest $500",
            "description": "Kein Markt ohne gebundenes Kapital – sonst kein Fill",
            "enabled": true,
            "condition": { "type": "min_open_interest_usd", "threshold": 500 },
            "action": { "side": "SKIP" }
        },
        {
            "name": "Filter – Range/Bracket-Kontrakte ausschließen",
            "description": "Range-Kontrakte haben anderes Risikoprofil als Richtungswetten",
            "enabled": true,
            "condition": { "type": "title_not_contains", "value": "range" },
            "action": { "side": "SKIP" }
        },
        {
            "name": "Stark überschätztes YES (>90%) → NO kaufen",
            "description": "~3–5% Overconfidence-Bias. Kelly-Sizing mit -2% Korrektur.",
            "enabled": true,
            "condition": { "type": "yes_ask_above", "threshold": 90 },
            "action": {
                "side": "no", "order_type": "limit", "limit_offset_cents": 1,
                "kelly_sizing": true, "kelly_fraction": 0.25,
                "min_count": 1, "max_count": 10
            }
        },
        {
            "name": "Leicht überschätztes YES (58–70%) → NO kaufen",
            "description": "~1–2% Bias. Kleinere Position, da Edge geringer.",
            "enabled": true,
            "condition": { "type": "yes_ask_between", "threshold_low": 58, "threshold_high": 70 },
            "action": {
                "side": "no", "order_type": "limit", "count": 3, "limit_offset_cents": 1
            }
        },
        {
            "name": "Gut kalibrierte Zone (73–82%) → YES kaufen (mind. 4h Restlaufzeit)",
            "description": "Beste Kalibrierung laut Polymarket-Daten. NUR wenn > 4h verbleibend – verhindert Einstieg nahe Ablauf.",
            "enabled": true,
            "condition": {
                "type": "yes_ask_between",
                "threshold_low": 73,
                "threshold_high": 82,
                "min_hours_remaining": 4.0
            },
            "action": {
                "side": "yes", "order_type": "limit", "count": 5, "limit_offset_cents": 1
            }
        },
        {
            "name": "Time Decay – Überschätztes YES mit langer Laufzeit → NO",
            "description": "Märkte mit 14+ Tagen: YES 18–62¢ verfällt tägl. ~0.2% wenn Ereignis ausbleibt.",
            "enabled": true,
            "condition": {
                "type": "time_decay_no",
                "min_days_remaining": 14,
                "yes_ask_min": 18,
                "yes_ask_max": 62
            },
            "action": { "side": "no", "order_type": "limit", "count": 3, "limit_offset_cents": 1 }
        }
    ],

    "crypto_scanner": {
        "interval_seconds": 30,
        "ladder_enabled": true,
        "min15_enabled": true,
        "min_volume_usd": 25,
        "ladder_min_volume_usd": 100
    },

    "crypto_ladder_rules": [
        {
            "name": "Filter – Range/Bracket-Kontrakte ausschließen",
            "description": "PFLICHT-FIX: Range-Kontrakte landen im Crypto-Track aber haben anderes Risikoprofil",
            "enabled": true,
            "condition": { "type": "title_not_contains", "value": "range" },
            "action": { "side": "SKIP" }
        },
        {
            "name": "Filter – Mindest-Volumen $100",
            "enabled": true,
            "condition": { "type": "min_volume_usd", "threshold": 100 },
            "action": { "side": "SKIP" }
        },
        {
            "name": "Filter – 50/50-Zone (45–55¢)",
            "enabled": true,
            "condition": { "type": "yes_ask_between", "threshold_low": 45, "threshold_high": 55 },
            "action": { "side": "SKIP" }
        },
        {
            "name": "Leiter – YES >85% → NO kaufen (Tail-Risiko wird unterschätzt)",
            "description": "NUR wenn Spot > Schwelle + 1% UND > 2h verbleibend. Verhindert Einstieg nahe Ablauf oder wenn Schwelle bereits gefährdet ist.",
            "enabled": true,
            "condition": {
                "type": "yes_ask_above",
                "threshold": 85,
                "spot_min_overshoot_pct": 1.0,
                "min_hours_remaining": 2.0
            },
            "action": {
                "side": "no", "order_type": "limit", "limit_offset_cents": 1,
                "kelly_sizing": true, "kelly_fraction": 0.25,
                "min_count": 1, "max_count": 15
            }
        },
        {
            "name": "Leiter – YES 73–82% → YES kaufen (RSI überverkauft + mind. 4h)",
            "description": "NUR wenn RSI-Signal überverkauft UND > 4h verbleibend. Verhindert Near-Expiry-Falle.",
            "enabled": true,
            "condition": {
                "type": "yes_ask_between",
                "threshold_low": 73,
                "threshold_high": 82,
                "min_hours_remaining": 4.0,
                "require_rsi_oversold": true,
                "rsi_oversold_threshold": 40
            },
            "action": {
                "side": "yes", "order_type": "limit", "count": 5, "limit_offset_cents": 1
            }
        }
    ],

    "crypto_15min_rules": [
        {
            "name": "15-Min – Mindest-Volumen $1000",
            "enabled": true,
            "condition": { "type": "min_volume_usd", "threshold": 1000 },
            "action": { "side": "SKIP" }
        },
        {
            "name": "15-Min – Mean Reversion",
            "description": "Faded Retail-Überreaktion. RSI-bestätigt. Kleinere Position (5ct statt 10ct).",
            "enabled": true,
            "condition": {
                "type": "btc_15min_mean_reversion",
                "change_threshold_pct": 0.4,
                "bias_threshold": 0.65,
                "rsi_overbought": 68,
                "rsi_oversold": 32
            },
            "action": { "order_type": "limit", "count": 5, "limit_offset_cents": 1 }
        }
    ],

    "logging": {
        "trades_file": "data/trades.jsonl",
        "signals_file": "data/signals.jsonl",
        "errors_file": "data/errors.jsonl"
    }
}
```

- [ ] **Schritt 1.2: JSON-Syntax validieren**

```bash
python3 -c "import json; json.load(open('app/config.json')); print('OK')"
```

Erwartete Ausgabe: `OK`

- [ ] **Schritt 1.3: Commit**

```bash
git add app/config.json
git commit -m "refactor: config in zwei getrennte Systeme aufteilen (prediction + crypto)"
```

---

## Task 2: Package-Skelette anlegen

**Files:**
- Create: `app/prediction/__init__.py`
- Create: `app/crypto/__init__.py`

- [ ] **Schritt 2.1: Packages erstellen**

`app/prediction/__init__.py`:
```python
"""System 1: Prediction Markets (politisch, wirtschaftlich, allgemein)."""
```

`app/crypto/__init__.py`:
```python
"""System 2: Crypto Markets (BTC/ETH Leiter + 15-Min Mean Reversion)."""
```

- [ ] **Schritt 2.2: Commit**

```bash
git add app/prediction/__init__.py app/crypto/__init__.py
git commit -m "feat: prediction und crypto Packages anlegen"
```

---

## Task 3: Prediction Rules

**Ziel:** Polymarket-basiertes Regelwerk mit -2%-Korrektur, Zeit-Filter für YES-Positionen (mind. 4h), Kelly-Sizing.

**Files:**
- Create: `app/prediction/rules.py`

- [ ] **Schritt 3.1: `app/prediction/rules.py` erstellen**

```python
"""
Prediction Market Regelwerk (System 1).

Basiert auf Polymarket-Kalibrierungsdaten:
  - Märkte sind im Schnitt 2% zu optimistisch (YES überschätzt)
  - Stärkster Bias: >90% YES (3–5% Überschätzung) und 50/50-Zone (7%)
  - Beste Kalibrierung: 73–82% YES → fast perfekt, Trend folgen
  - NO ist bei 58–70% oft leicht unterbewertet

Bedingungstypen:
  yes_ask_above       : YES ask > Schwelle → NO kaufen
  yes_ask_between     : YES ask in [low, high] → YES oder NO kaufen
                        Optionales Feld: min_hours_remaining (Float) → Filter
  time_decay_no       : YES 18–62¢ mit langer Laufzeit → NO kaufen
  min_volume_usd      : SKIP-Filter
  min_open_interest_usd: SKIP-Filter
  title_not_contains  : SKIP-Filter
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)

SYSTEM = "prediction"


# ── Kelly Utilities ──────────────────────────────────────────────────── #

def polymarket_corrected_prob(yes_ask_cents: int) -> float:
    """
    Korrigiert den systematischen Overconfidence-Bias auf Kalshi/Polymarket.

    Kalibrierungsdaten (Polymarket-Forschung):
      >90%    → -3% bis -5% Überschätzung (Certainty-Overweighting)
      73–82%  → fast perfekt kalibriert (±1%)
      58–70%  → ~2% Überschätzung
      43–57%  → bis zu 7% Überschätzung (schlechteste Zone)

    Globale Korrektur: -2% auf alle Werte als Basislinie.
    """
    p = yes_ask_cents / 100.0
    if p >= 0.90:
        return max(0.01, p - 0.04)
    if p >= 0.73:
        return p - 0.01   # fast perfekt – minimale Korrektur
    if p >= 0.58:
        return p - 0.02
    return max(0.01, p - 0.02)  # Basiskorrektur


def kelly_count(price_cents: int, true_prob_win: float,
                bankroll_usd: float, fraction: float = 0.25,
                min_count: int = 1, max_count: int = 10) -> int:
    """Quarter-Kelly Position Sizing. Gibt 0 zurück wenn kein positiver Edge."""
    cost = price_cents / 100.0
    if cost <= 0 or cost >= 1 or true_prob_win <= 0:
        return min_count
    b       = (1.0 - cost) / cost
    q       = 1.0 - true_prob_win
    f_star  = (true_prob_win * b - q) / b
    if f_star <= 0:
        return 0
    bet_usd = f_star * fraction * bankroll_usd
    count   = int(bet_usd / cost)
    return max(min_count, min(max_count, count))


# ── Signal ───────────────────────────────────────────────────────────── #

@dataclass
class PredictionSignal:
    ticker:      str
    rule_name:   str
    side:        str        # "yes" | "no"
    action:      str        # "buy" | "sell"
    price_cents: int
    count:       int
    reason:      str
    system:      str = SYSTEM
    meta:        dict = field(default_factory=dict)
    track:       str = "prediction"


# ── Rule Engine ──────────────────────────────────────────────────────── #

class PredictionRuleEngine:
    def __init__(self, config: dict):
        self._rules = [
            r for r in config.get("prediction_rules", [])
            if r.get("enabled", True)
        ]
        logger.info(f"[Prediction/Rules] {len(self._rules)} aktive Regeln geladen")

    def evaluate(self, market: dict, bankroll_usd: float = 200.0) -> list[PredictionSignal]:
        ticker    = market.get("ticker", "")
        yes_ask   = self._price(market, "yes_ask")
        yes_bid   = self._price(market, "yes_bid")
        no_ask    = self._price(market, "no_ask")
        volume    = float(market.get("volume_24h_fp", 0) or 0)
        oi        = float(market.get("open_interest_fp", 0) or 0)
        close_str = market.get("close_time", "")
        title     = str(market.get("title", "")).lower()
        hours_left = self._hours_remaining(close_str)

        # ── SKIP-Filter zuerst ────────────────────────────────────────
        for rule in self._rules:
            act = rule.get("action", {})
            if act.get("side") != "SKIP":
                continue
            cond = rule.get("condition", {})
            t    = cond.get("type", "")
            if t == "min_volume_usd" and volume < float(cond.get("threshold", 0)):
                logger.debug(f"[Prediction] {ticker} SKIP – Volumen ${volume:.0f} < ${cond['threshold']}")
                return []
            if t == "min_open_interest_usd" and oi < float(cond.get("threshold", 0)):
                logger.debug(f"[Prediction] {ticker} SKIP – OI ${oi:.0f} < ${cond['threshold']}")
                return []
            if t == "title_not_contains":
                keyword = str(cond.get("value", "")).lower()
                if keyword in title:
                    logger.debug(f"[Prediction] {ticker} SKIP – Titel enthält '{keyword}'")
                    return []
            if t == "yes_ask_between":
                low  = int(cond.get("threshold_low", 0))
                high = int(cond.get("threshold_high", 100))
                if yes_ask is not None and low <= yes_ask <= high:
                    logger.debug(f"[Prediction] {ticker} SKIP – YES ask {yes_ask}¢ in [{low}–{high}]")
                    return []

        # ── Trading-Regeln ────────────────────────────────────────────
        signals: list[PredictionSignal] = []
        for rule in self._rules:
            act = rule.get("action", {})
            if act.get("side") == "SKIP":
                continue
            sig = self._eval_rule(rule, ticker, yes_ask, yes_bid, no_ask,
                                  close_str, hours_left, bankroll_usd, market)
            if sig:
                signals.append(sig)
        return signals

    def _eval_rule(self, rule: dict, ticker: str,
                   yes_ask: Optional[int], yes_bid: Optional[int], no_ask: Optional[int],
                   close_str: str, hours_left: float,
                   bankroll_usd: float, market: dict) -> Optional[PredictionSignal]:
        name   = rule.get("name", "Unbenannt")
        cond   = rule.get("condition", {})
        act    = rule.get("action", {})
        t      = cond.get("type", "")
        side   = act.get("side", "yes")
        action = "buy"
        count  = int(act.get("count", 5))
        offset = int(act.get("limit_offset_cents", 1))

        matched, reason = False, ""

        if t == "yes_ask_above":
            thr = int(cond.get("threshold", 90))
            if yes_ask is not None and yes_ask > thr:
                matched = True
                reason  = f"YES ask {yes_ask}¢ > {thr}¢ → NO"
                side    = "no"

        elif t == "yes_ask_between":
            low       = int(cond.get("threshold_low", 0))
            high      = int(cond.get("threshold_high", 100))
            min_hours = float(cond.get("min_hours_remaining", 0.0))
            if yes_ask is not None and low <= yes_ask <= high:
                if min_hours > 0 and hours_left < min_hours:
                    logger.debug(
                        f"[Prediction] {ticker} – YES {yes_ask}¢ passt Regel '{name}', "
                        f"aber nur {hours_left:.1f}h verbleibend (mind. {min_hours}h nötig)"
                    )
                    return None
                matched = True
                reason  = f"YES ask {yes_ask}¢ in [{low}–{high}]¢"
                # Seite aus Regel bestimmen (default: YES für 73–82% Zone, NO für 58–70%)
                side = act.get("side", "yes")

        elif t == "time_decay_no":
            ask_min  = int(cond.get("yes_ask_min", 18))
            ask_max  = int(cond.get("yes_ask_max", 62))
            min_days = float(cond.get("min_days_remaining", 14))
            if yes_ask is not None and ask_min <= yes_ask <= ask_max:
                try:
                    ct   = datetime.fromisoformat(close_str.replace("Z", "+00:00"))
                    days = (ct - datetime.now(timezone.utc)).days
                    if days >= min_days:
                        matched = True
                        reason  = f"Time-Decay: YES={yes_ask}¢ | {days}d verbleibend"
                        side    = "no"
                except Exception:
                    pass

        if not matched:
            return None

        # Limit-Preis
        if side == "yes":
            px = max(1, min(99, (yes_ask or 50) + offset))
        else:
            px = max(1, min(99, (no_ask or 50) + offset))

        # Kelly Sizing (wenn in Regel aktiviert)
        if act.get("kelly_sizing"):
            fraction  = float(act.get("kelly_fraction", 0.25))
            min_cnt   = int(act.get("min_count", 1))
            max_cnt   = int(act.get("max_count", 10))
            if side == "no":
                edge_p = no_ask or px
                true_p = 1.0 - polymarket_corrected_prob(100 - edge_p)
            else:
                edge_p = yes_ask or px
                true_p = polymarket_corrected_prob(edge_p)
            kelly_c = kelly_count(edge_p, true_p, bankroll_usd, fraction, min_cnt, max_cnt)
            if kelly_c == 0:
                logger.debug(f"[Prediction/Kelly] {ticker} – kein Edge bei {edge_p}¢, Signal verworfen")
                return None
            count  = kelly_c
            reason += f" · Kelly={count}ct"

        return PredictionSignal(
            ticker      = ticker,
            rule_name   = name,
            side        = side,
            action      = action,
            price_cents = px,
            count       = count,
            reason      = reason,
            meta        = {
                "yes_ask":     yes_ask,
                "yes_bid":     yes_bid,
                "no_ask":      no_ask,
                "title":       market.get("title", "")[:80],
                "event_title": (market.get("event_title") or "")[:120],
                "event_ticker":market.get("event_ticker", ""),
                "close_time":  close_str,
                "category":    (market.get("category") or "").lower(),
                "sub_title":   (market.get("sub_title") or "").strip(),
                "image_url":   (market.get("image_url") or "").strip(),
                "system":      SYSTEM,
            },
        )

    def _price(self, market: dict, key: str) -> Optional[int]:
        v = market.get(key + "_dollars") or market.get(key)
        if v is None:
            return None
        try:
            f = float(v)
            return int(round(f * 100)) if f <= 1.0 else int(round(f))
        except (ValueError, TypeError):
            return None

    def _hours_remaining(self, close_str: str) -> float:
        if not close_str:
            return float("inf")
        try:
            ct = datetime.fromisoformat(close_str.replace("Z", "+00:00"))
            return max(0.0, (ct - datetime.now(timezone.utc)).total_seconds() / 3600)
        except Exception:
            return float("inf")
```

- [ ] **Schritt 3.2: Smoke-Test**

```bash
cd /home/ubuntu/kalshi/app
python3 -c "
from prediction.rules import PredictionRuleEngine
import json
cfg = json.load(open('config.json'))
eng = PredictionRuleEngine(cfg)
market = {
    'ticker': 'TEST-001',
    'yes_ask': 0.94,
    'yes_bid': 0.90,
    'no_ask': 0.07,
    'no_bid': 0.06,
    'volume_24h_fp': 500,
    'open_interest_fp': 1000,
    'close_time': '2026-04-20T20:00:00Z',
    'title': 'Will candidate X win?',
}
sigs = eng.evaluate(market, bankroll_usd=200.0)
print(f'Signale: {len(sigs)}')
for s in sigs:
    print(f'  {s.rule_name} | {s.side} {s.count}x @ {s.price_cents}¢')
"
```

Erwartete Ausgabe:
```
[Prediction/Rules] 6 aktive Regeln geladen
Signale: 1
  Stark überschätztes YES (>90%) → NO kaufen | no Xx @ Yc
```

- [ ] **Schritt 3.3: Commit**

```bash
git add app/prediction/rules.py
git commit -m "feat(prediction): Polymarket-basiertes Regelwerk mit Kelly + Zeit-Filter"
```

---

## Task 4: Prediction Scanner

**Ziel:** Eigenständiger Scanner für politische/wirtschaftliche Märkte mit eigener Exit-Logik. Schickt `PredictionSignal`-Objekte (werden im nächsten Schritt an den Executor adaptiert).

**Files:**
- Create: `app/prediction/scanner.py`

- [ ] **Schritt 4.1: `app/prediction/scanner.py` erstellen**

```python
"""
Prediction Market Scanner (System 1).

Scannt politische, wirtschaftliche und allgemeine Märkte auf Kalshi.
Hat eigene Entry- und Exit-Logik – komplett getrennt vom Crypto-System.

Exit-Bedingungen:
  1. Take-Profit : Bid ≥ 1.8× Einstieg (Prediction Markets sind träger)
  2. Zeit-Stop   : < 30 Min, Position hat > 50% des Einsatzes verloren
  3. (Kein Spot-Stop-Loss – keine Crypto-Preisbezüge)
"""

import asyncio
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Optional

from api.client import KalshiClient
from logger.trade_logger import TradeLogger
from prediction.rules import PredictionRuleEngine, PredictionSignal
from trader.executor import Signal

logger = logging.getLogger(__name__)

SYSTEM = "prediction"


def _to_executor_signal(ps: PredictionSignal) -> Signal:
    """Konvertiert PredictionSignal in das shared Signal-Format des Executors."""
    return Signal(
        ticker      = ps.ticker,
        rule_name   = ps.rule_name,
        side        = ps.side,
        action      = ps.action,
        price_cents = ps.price_cents,
        count       = ps.count,
        reason      = ps.reason,
        meta        = {**ps.meta, "system": SYSTEM},
        track       = ps.track,
    )


class PredictionScanner:
    def __init__(
        self,
        client:       KalshiClient,
        trade_logger: TradeLogger,
        config:       dict,
        on_signal:    Callable,
        on_meta:      Optional[Callable] = None,
        on_cycle_end: Optional[Callable] = None,
    ):
        self._client      = client
        self._logger      = trade_logger
        self._config      = config
        self._on_signal   = on_signal
        self._on_meta     = on_meta
        self._on_cycle_end = on_cycle_end

        scan_cfg          = config.get("prediction_scanner", {})
        self._interval_s  = int(scan_cfg.get("interval_seconds", 60))
        self._max_close_days = scan_cfg.get("max_close_days", 30)
        self._min_volume  = float(scan_cfg.get("min_volume_usd", 0))
        self._categories  = set(c.lower() for c in scan_cfg.get("categories", []))

        sys_cfg           = config.get("systems", {}).get(SYSTEM, {})
        self._bankroll    = float(sys_cfg.get("max_exposure_usd", 80.0))

        self._rules       = PredictionRuleEngine(config)
        self._stop_event  = asyncio.Event()
        self._exit_pending: set[str] = set()

        logger.info(
            f"[Prediction/Scanner] Gestartet | Intervall: {self._interval_s}s | "
            f"Budget: ${self._bankroll:.0f}"
        )

    async def start(self):
        while not self._stop_event.is_set():
            t0 = time.monotonic()
            try:
                await self._scan_cycle()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception(f"[Prediction/Scanner] Fehler: {e}")
                self._logger.log_error("PredictionScanner", str(e))
            elapsed = time.monotonic() - t0
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(),
                    timeout=max(0.0, self._interval_s - elapsed),
                )
            except asyncio.TimeoutError:
                pass

    async def stop(self):
        self._stop_event.set()

    # ── Scan-Zyklus ──────────────────────────────────────────────────── #

    async def _scan_cycle(self):
        loop = asyncio.get_running_loop()
        signals_total = 0

        # Entry-Scan
        signals_total += await self._scan_entries(loop)

        # Exit-Scan
        signals_total += await self._scan_exits(loop)

        if signals_total:
            logger.info(f"[Prediction/Scanner] {signals_total} Signal(e)")
        else:
            logger.debug("[Prediction/Scanner] Keine Signale")

        if self._on_cycle_end:
            await self._on_cycle_end()

    # ── Entry-Scan ───────────────────────────────────────────────────── #

    async def _scan_entries(self, loop) -> int:
        events = await loop.run_in_executor(
            None,
            lambda: self._client.get_all_events(status="open", with_nested_markets=True),
        )
        allowed = [e for e in events if (e.get("category") or "").lower() in self._categories]

        markets: list[dict] = []
        now_utc  = datetime.now(timezone.utc)
        max_secs = (self._max_close_days or 30) * 86400

        for event in allowed:
            series_ticker = (event.get("series_ticker") or "").upper()
            image_url     = (event.get("image_url") or "").strip()
            event_title   = (event.get("title") or "").strip()
            if not image_url and series_ticker:
                image_url = (
                    f"https://kalshi-public-docs.s3.amazonaws.com/"
                    f"series-images-webp/{series_ticker}.webp"
                )
            for m in event.get("markets", []):
                if m.get("status", "active") not in ("active", "open"):
                    continue
                ct_str = m.get("close_time", "")
                if ct_str:
                    try:
                        ct = datetime.fromisoformat(ct_str.replace("Z", "+00:00"))
                        if (ct - now_utc).total_seconds() > max_secs:
                            continue
                    except Exception:
                        pass
                if self._min_volume > 0:
                    if float(m.get("volume_24h_fp", 0) or 0) < self._min_volume:
                        continue
                m["category"]    = (event.get("category") or "").lower()
                m["sub_title"]   = (event.get("sub_title") or "").strip()
                m["image_url"]   = image_url
                m["event_title"] = event_title
                markets.append(m)

        logger.info(f"[Prediction/Scanner] {len(markets)} Märkte gescannt")

        signals = 0
        for market in markets:
            if self._on_meta:
                self._on_meta(
                    market.get("ticker", ""),
                    event_title=market.get("event_title", ""),
                    image_url=market.get("image_url", ""),
                    sub_title=market.get("sub_title", ""),
                )
            for ps in self._rules.evaluate(market, bankroll_usd=self._bankroll):
                await self._on_signal(_to_executor_signal(ps))
                signals += 1
        return signals

    # ── Exit-Scan ────────────────────────────────────────────────────── #

    async def _scan_exits(self, loop) -> int:
        """
        Prediction-Market Exit-Logik:
          1. Take-Profit : Bid ≥ 1.8× Einstieg
          2. Zeit-Stop   : < 30 Min + mehr als 50% des Einsatzes verloren
        """
        try:
            data      = json.loads(Path("data/positions.json").read_text())
            positions = [p for p in data.get("positions", []) if p.get("system") == SYSTEM]
        except Exception:
            return 0

        current_tickers = {p.get("ticker", "") for p in positions}
        self._exit_pending = {t for t in self._exit_pending if t in current_tickers}

        now   = datetime.now(timezone.utc)
        exits = 0

        for pos in positions:
            ticker    = pos.get("ticker", "")
            side      = str(pos.get("side", "")).lower()
            entry_px  = int(pos.get("price_cents", 0))
            count     = int(pos.get("count", 0))
            close_str = pos.get("close_time", "")

            if not ticker or not entry_px or not count:
                continue
            if ticker in self._exit_pending:
                continue

            mins_left = float("inf")
            try:
                ct        = datetime.fromisoformat(close_str.replace("Z", "+00:00"))
                mins_left = max(0.0, (ct - now).total_seconds() / 60)
            except Exception:
                pass

            if mins_left < 0:
                continue

            try:
                market = await loop.run_in_executor(
                    None, lambda t=ticker: self._client.get_market(t)
                )
            except Exception:
                continue

            if not market:
                continue

            def _pc(key: str) -> Optional[int]:
                v = market.get(key + "_dollars") or market.get(key)
                if v is None:
                    return None
                f = float(v)
                return int(round(f * 100)) if f <= 1.0 else int(round(f))

            yes_bid = _pc("yes_bid")
            no_bid  = _pc("no_bid")

            exit_reason: Optional[str] = None
            sell_price: int = max(1, entry_px - 1)

            if side == "no":
                # Take-Profit: NO Bid ≥ 1.8× Einstieg
                tp_target = int(entry_px * 1.8)
                if no_bid and no_bid >= tp_target:
                    exit_reason = f"Take-Profit: NO bid {no_bid}¢ ≥ 1.8× {entry_px}¢"
                    sell_price  = max(1, no_bid - 1)
                # Zeit-Stop: < 30 Min + mehr als 60% Verlust
                elif mins_left < 30 and no_bid and no_bid < int(entry_px * 0.4):
                    exit_reason = (
                        f"Zeit-Stop: {mins_left:.0f}min verbl. | "
                        f"NO bid {no_bid}¢ < 40% von Einstieg {entry_px}¢"
                    )
                    sell_price = max(1, no_bid or 1)

            elif side == "yes":
                # Take-Profit: YES Bid ≥ 1.8× Einstieg (cap 95¢)
                tp_target = min(95, int(entry_px * 1.8))
                if yes_bid and yes_bid >= tp_target:
                    exit_reason = f"Take-Profit: YES bid {yes_bid}¢ ≥ {tp_target}¢"
                    sell_price  = max(1, yes_bid - 1)
                # Zeit-Stop: < 30 Min + mehr als 60% Verlust
                elif mins_left < 30 and yes_bid and yes_bid < int(entry_px * 0.4):
                    exit_reason = (
                        f"Zeit-Stop: {mins_left:.0f}min verbl. | "
                        f"YES bid {yes_bid}¢ < 40% von Einstieg {entry_px}¢"
                    )
                    sell_price = max(1, yes_bid or 1)

            if not exit_reason:
                continue

            logger.info(
                f"[Prediction/Exit] {ticker} EXIT {side.upper()} ×{count} "
                f"@ {sell_price}¢ | {exit_reason}"
            )
            self._exit_pending.add(ticker)
            await self._on_signal(Signal(
                ticker      = ticker,
                rule_name   = f"Exit: {exit_reason[:60]}",
                side        = side,
                action      = "sell",
                price_cents = sell_price,
                count       = count,
                reason      = exit_reason,
                meta        = {
                    "close_time":        close_str,
                    "exit":              True,
                    "entry_price_cents": entry_px,
                    "system":            SYSTEM,
                },
            ))
            exits += 1

        return exits
```

- [ ] **Schritt 4.2: Import-Test**

```bash
cd /home/ubuntu/kalshi/app
python3 -c "from prediction.scanner import PredictionScanner; print('OK')"
```

Erwartete Ausgabe: `OK`

- [ ] **Schritt 4.3: Commit**

```bash
git add app/prediction/scanner.py
git commit -m "feat(prediction): Scanner mit Entry-/Exit-Logik (Zeit-Stop: -60% + 30min)"
```

---

## Task 5: Crypto Rules

**Ziel:** Crypto-spezifisches Regelwerk. Fixes für alle 5 identifizierten Probleme:
- Range-Filter vorhanden ✓
- NO nur wenn Spot > Threshold + 1% UND > 2h ✓
- YES in 73-82% nur wenn > 4h verbleibend ✓
- RSI-Pflicht für YES-Käufe ✓
- Position-Sizing reduziert (5 statt 10 für 15-Min) ✓

**Files:**
- Create: `app/crypto/rules.py`

- [ ] **Schritt 5.1: `app/crypto/rules.py` erstellen**

```python
"""
Crypto Market Regelwerk (System 2).

Verwaltet Entry-Signale für BTC/ETH/SOL/XRP Tages-Leiter und 15-Min-Märkte.

Ladder-Regeln (crypto_ladder_rules):
  yes_ask_above     : YES > Schwelle → NO kaufen
                      Pflicht-Felder: spot_min_overshoot_pct (%), min_hours_remaining
  yes_ask_between   : YES in Zone → YES kaufen
                      Pflicht-Felder: min_hours_remaining, optional require_rsi_oversold
  title_not_contains: SKIP-Filter
  min_volume_usd    : SKIP-Filter
  yes_ask_between (SKIP): 50/50-Filter

15-Min-Regeln (crypto_15min_rules):
  btc_15min_mean_reversion : RSI-gestützte Mean-Reversion
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)

SYSTEM = "crypto"


# ── Kelly Utilities ──────────────────────────────────────────────────── #

def crypto_corrected_yes_prob(yes_ask_cents: int) -> float:
    """
    Kalibrierungskorrektur für Crypto-Threshold-Märkte.

    Crypto-Leiter: BTC/ETH können intraday ±2–3% schwingen.
    Bei >85% YES ist das Tail-Risiko (Threshold-Bruch) systematisch unterschätzt.

      ≥95%   → -4% (extreme Certainty-Overweighting)
      ≥90%   → -3%
      ≥85%   → -2.5%
      73–82% → fast korrekt, -1%
      55–65% → -1%
    """
    p = yes_ask_cents / 100.0
    if p >= 0.95:
        return max(0.01, p - 0.04)
    if p >= 0.90:
        return max(0.01, p - 0.03)
    if p >= 0.85:
        return max(0.01, p - 0.025)
    if 0.73 <= p <= 0.82:
        return p - 0.01
    if 0.55 <= p <= 0.65:
        return p - 0.01
    return p


def kelly_count(price_cents: int, true_prob_win: float,
                bankroll_usd: float, fraction: float = 0.25,
                min_count: int = 1, max_count: int = 15) -> int:
    """Quarter-Kelly Position Sizing."""
    cost = price_cents / 100.0
    if cost <= 0 or cost >= 1 or true_prob_win <= 0:
        return min_count
    b       = (1.0 - cost) / cost
    q       = 1.0 - true_prob_win
    f_star  = (true_prob_win * b - q) / b
    if f_star <= 0:
        return 0
    bet_usd = f_star * fraction * bankroll_usd
    count   = int(bet_usd / cost)
    return max(min_count, min(max_count, count))


# ── Signal ───────────────────────────────────────────────────────────── #

@dataclass
class CryptoSignal:
    ticker:      str
    rule_name:   str
    side:        str
    action:      str
    price_cents: int
    count:       int
    reason:      str
    system:      str = SYSTEM
    meta:        dict = field(default_factory=dict)
    track:       str = "crypto"


# ── Rule Engine ──────────────────────────────────────────────────────── #

class CryptoLadderRuleEngine:
    def __init__(self, config: dict):
        self._rules = [
            r for r in config.get("crypto_ladder_rules", [])
            if r.get("enabled", True)
        ]
        logger.info(f"[Crypto/LadderRules] {len(self._rules)} aktive Regeln geladen")

    def evaluate(self, market: dict, context: dict | None = None) -> list[CryptoSignal]:
        ticker    = market.get("ticker", "")
        yes_ask   = self._price(market, "yes_ask")
        yes_bid   = self._price(market, "yes_bid")
        no_ask    = self._price(market, "no_ask")
        volume    = float(market.get("volume_24h_fp", 0) or 0)
        title     = str(market.get("title", "")).lower()
        close_str = market.get("close_time", "")
        hours_left = self._hours_remaining(close_str)
        ctx       = context or {}
        spot      = ctx.get("btc_price")  # Spot-Preis (BTC, ETH, etc.) – key heißt immer btc_price
        rsi       = ctx.get("bingx_rsi")
        bankroll  = float(ctx.get("bankroll_usd", 80.0))

        # ── SKIP-Filter ───────────────────────────────────────────────
        for rule in self._rules:
            act = rule.get("action", {})
            if act.get("side") != "SKIP":
                continue
            cond = rule.get("condition", {})
            t    = cond.get("type", "")
            if t == "min_volume_usd" and volume < float(cond.get("threshold", 0)):
                return []
            if t == "title_not_contains":
                keyword = str(cond.get("value", "")).lower()
                if keyword in title:
                    logger.debug(f"[Crypto/Ladder] {ticker} SKIP – Titel enthält '{keyword}'")
                    return []
            if t == "yes_ask_between":
                low  = int(cond.get("threshold_low", 0))
                high = int(cond.get("threshold_high", 100))
                if yes_ask is not None and low <= yes_ask <= high:
                    return []

        # ── Threshold aus Ticker lesen ────────────────────────────────
        threshold_val = self._ticker_threshold(ticker)

        # ── Trading-Regeln ────────────────────────────────────────────
        signals: list[CryptoSignal] = []
        for rule in self._rules:
            act = rule.get("action", {})
            if act.get("side") == "SKIP":
                continue
            sig = self._eval_rule(
                rule, ticker, yes_ask, yes_bid, no_ask,
                close_str, hours_left, spot, rsi, threshold_val, bankroll, market
            )
            if sig:
                signals.append(sig)
        return signals

    def _eval_rule(self, rule: dict, ticker: str,
                   yes_ask: Optional[int], yes_bid: Optional[int], no_ask: Optional[int],
                   close_str: str, hours_left: float,
                   spot: Optional[float], rsi: Optional[float],
                   threshold_val: float, bankroll: float,
                   market: dict) -> Optional[CryptoSignal]:
        name   = rule.get("name", "Unbenannt")
        cond   = rule.get("condition", {})
        act    = rule.get("action", {})
        t      = cond.get("type", "")
        side   = act.get("side", "no")
        count  = int(act.get("count", 5))
        offset = int(act.get("limit_offset_cents", 1))

        matched, reason = False, ""

        if t == "yes_ask_above":
            thr         = int(cond.get("threshold", 85))
            min_hours   = float(cond.get("min_hours_remaining", 2.0))
            min_over    = float(cond.get("spot_min_overshoot_pct", 1.0)) / 100

            if yes_ask is not None and yes_ask > thr:
                # Zeit-Check: mindestens X Stunden verbleibend
                if hours_left < min_hours:
                    logger.debug(
                        f"[Crypto/Ladder] {ticker} – YES {yes_ask}¢ > {thr}¢ blockiert: "
                        f"nur {hours_left:.1f}h verbleibend (mind. {min_hours}h)"
                    )
                    return None
                # Spot-Distanz-Check: Spot muss > Threshold + X% sein
                if spot and spot > 0 and threshold_val > 0:
                    overshoot = (spot - threshold_val) / threshold_val
                    if overshoot < min_over:
                        logger.debug(
                            f"[Crypto/Ladder] {ticker} – NO-Kauf blockiert: "
                            f"Spot ${spot:,.0f} nur {overshoot:.2%} über Threshold "
                            f"${threshold_val:,.0f} (mind. {min_over:.2%})"
                        )
                        return None
                matched = True
                reason  = (
                    f"YES {yes_ask}¢ > {thr}¢ | {hours_left:.1f}h verbl. | "
                    f"Spot ${spot:,.0f} > Schwelle ${threshold_val:,.0f}"
                    if spot and threshold_val else f"YES {yes_ask}¢ > {thr}¢"
                )
                side = "no"

        elif t == "yes_ask_between":
            low          = int(cond.get("threshold_low", 73))
            high         = int(cond.get("threshold_high", 82))
            min_hours    = float(cond.get("min_hours_remaining", 4.0))
            req_rsi_os   = bool(cond.get("require_rsi_oversold", False))
            rsi_os_thr   = float(cond.get("rsi_oversold_threshold", 40))

            if yes_ask is not None and low <= yes_ask <= high:
                # Zeit-Check
                if hours_left < min_hours:
                    logger.debug(
                        f"[Crypto/Ladder] {ticker} – YES {yes_ask}¢ blockiert: "
                        f"nur {hours_left:.1f}h verbleibend (mind. {min_hours}h)"
                    )
                    return None
                # RSI-Check (wenn Pflicht)
                if req_rsi_os:
                    if rsi is None or rsi > rsi_os_thr:
                        logger.debug(
                            f"[Crypto/Ladder] {ticker} – YES {yes_ask}¢ blockiert: "
                            f"RSI={rsi} nicht überverkauft (< {rsi_os_thr} nötig)"
                        )
                        return None
                matched = True
                reason  = f"YES {yes_ask}¢ in [{low}–{high}]¢ | {hours_left:.1f}h verbl."
                if rsi:
                    reason += f" | RSI={rsi:.0f}"
                side = act.get("side", "yes")

        if not matched:
            return None

        # Limit-Preis
        if side == "yes":
            px = max(1, min(99, (yes_ask or 50) + offset))
        else:
            px = max(1, min(99, (no_ask or 50) + offset))

        # Kelly Sizing
        if act.get("kelly_sizing"):
            fraction = float(act.get("kelly_fraction", 0.25))
            min_cnt  = int(act.get("min_count", 1))
            max_cnt  = int(act.get("max_count", 15))
            if side == "no":
                edge_p = no_ask or px
                true_p = 1.0 - crypto_corrected_yes_prob(100 - edge_p)
            else:
                edge_p = yes_ask or px
                true_p = crypto_corrected_yes_prob(edge_p)
            kelly_c = kelly_count(edge_p, true_p, bankroll, fraction, min_cnt, max_cnt)
            if kelly_c == 0:
                logger.debug(f"[Crypto/Kelly] {ticker} – kein Edge bei {edge_p}¢")
                return None
            count  = kelly_c
            reason += f" · Kelly={count}ct"

        return CryptoSignal(
            ticker      = ticker,
            rule_name   = name,
            side        = side,
            action      = "buy",
            price_cents = px,
            count       = count,
            reason      = reason,
            meta        = {
                "yes_ask":     yes_ask,
                "yes_bid":     yes_bid,
                "no_ask":      no_ask,
                "title":       market.get("title", "")[:80],
                "event_title": (market.get("event_title") or "")[:120],
                "event_ticker":market.get("event_ticker", ""),
                "close_time":  market.get("close_time", ""),
                "category":    "crypto",
                "sub_title":   "",
                "image_url":   (market.get("image_url") or "").strip(),
                "system":      SYSTEM,
            },
        )

    def _price(self, market: dict, key: str) -> Optional[int]:
        v = market.get(key + "_dollars") or market.get(key)
        if v is None:
            return None
        try:
            f = float(v)
            return int(round(f * 100)) if f <= 1.0 else int(round(f))
        except (ValueError, TypeError):
            return None

    def _hours_remaining(self, close_str: str) -> float:
        if not close_str:
            return float("inf")
        try:
            ct = datetime.fromisoformat(close_str.replace("Z", "+00:00"))
            return max(0.0, (ct - datetime.now(timezone.utc)).total_seconds() / 3600)
        except Exception:
            return float("inf")

    def _ticker_threshold(self, ticker: str) -> float:
        for sep in ("-T", "-B"):
            if sep in ticker:
                try:
                    return float(ticker.split(sep)[-1])
                except Exception:
                    pass
        return 0.0


class Crypto15MinRuleEngine:
    """Eigenständige Rule Engine für BTC/ETH 15-Min Mean-Reversion-Märkte."""

    def __init__(self, config: dict):
        self._rules = [
            r for r in config.get("crypto_15min_rules", [])
            if r.get("enabled", True)
        ]
        logger.info(f"[Crypto/15MinRules] {len(self._rules)} aktive Regeln geladen")

    def evaluate(self, market: dict, context: dict | None = None) -> list[CryptoSignal]:
        ticker  = market.get("ticker", "")
        yes_ask = self._price(market, "yes_ask")
        volume  = float(market.get("volume_24h_fp", 0) or 0)
        ctx     = context or {}

        for rule in self._rules:
            act = rule.get("action", {})
            if act.get("side") != "SKIP":
                continue
            cond = rule.get("condition", {})
            if cond.get("type") == "min_volume_usd":
                if volume < float(cond.get("threshold", 0)):
                    return []

        signals: list[CryptoSignal] = []
        for rule in self._rules:
            act = rule.get("action", {})
            if act.get("side") == "SKIP":
                continue
            sig = self._eval_mean_reversion(rule, ticker, yes_ask, market, ctx)
            if sig:
                signals.append(sig)
        return signals

    def _eval_mean_reversion(self, rule: dict, ticker: str,
                              yes_ask: Optional[int], market: dict,
                              ctx: dict) -> Optional[CryptoSignal]:
        cond       = rule.get("condition", {})
        act        = rule.get("action", {})
        if cond.get("type") != "btc_15min_mean_reversion":
            return None

        change     = ctx.get("btc_change_15min")
        rsi        = ctx.get("bingx_rsi")
        vol_ratio  = ctx.get("bingx_vol_ratio", 1.0)
        if change is None:
            return None

        bias_thr   = int(float(cond.get("bias_threshold", 0.65)) * 100)
        change_thr = float(cond.get("change_threshold_pct", 0.4))
        rsi_ob     = float(cond.get("rsi_overbought", 68))
        rsi_os     = float(cond.get("rsi_oversold",   32))
        count      = int(act.get("count", 5))
        offset     = int(act.get("limit_offset_cents", 1))

        side, matched, reason = "no", False, ""

        if rsi is not None:
            if rsi >= rsi_ob and yes_ask is not None and yes_ask >= bias_thr and vol_ratio >= 0.8:
                matched = True
                reason  = f"RSI={rsi:.0f} überkauft / UP={yes_ask}¢ → DOWN fade"
                side    = "no"
            elif rsi <= rsi_os and yes_ask is not None and yes_ask <= (100 - bias_thr) and vol_ratio >= 0.8:
                matched = True
                reason  = f"RSI={rsi:.0f} überverkauft / UP={yes_ask}¢ → UP fade"
                side    = "yes"
        else:
            if change >= change_thr and yes_ask is not None and yes_ask >= bias_thr:
                matched = True
                reason  = f"BTC +{change:.2f}% / UP={yes_ask}¢ → DOWN fade"
                side    = "no"
            elif change <= -change_thr and yes_ask is not None and yes_ask <= (100 - bias_thr):
                matched = True
                reason  = f"BTC {change:.2f}% / UP={yes_ask}¢ → UP fade"
                side    = "yes"

        if not matched:
            return None

        no_ask  = self._price(market, "no_ask")
        if side == "yes":
            px = max(1, min(99, (yes_ask or 50) + offset))
        else:
            px = max(1, min(99, (no_ask or 50) + offset))

        return CryptoSignal(
            ticker      = ticker,
            rule_name   = rule.get("name", "15-Min Mean Reversion"),
            side        = side,
            action      = "buy",
            price_cents = px,
            count       = count,
            reason      = reason,
            meta        = {
                "yes_ask":    yes_ask,
                "no_ask":     no_ask,
                "close_time": market.get("close_time", ""),
                "title":      market.get("title", "")[:80],
                "system":     SYSTEM,
            },
            track       = "crypto_15min",
        )

    def _price(self, market: dict, key: str) -> Optional[int]:
        v = market.get(key + "_dollars") or market.get(key)
        if v is None:
            return None
        try:
            f = float(v)
            return int(round(f * 100)) if f <= 1.0 else int(round(f))
        except (ValueError, TypeError):
            return None
```

- [ ] **Schritt 5.2: Smoke-Test Ladder-Regel**

```bash
cd /home/ubuntu/kalshi/app
python3 -c "
from crypto.rules import CryptoLadderRuleEngine
import json
cfg = json.load(open('config.json'))
eng = CryptoLadderRuleEngine(cfg)

# Test 1: NO-Kauf mit guten Bedingungen (2.5% über Threshold, 4h verbleibend)
market = {
    'ticker': 'KXBTCD-26APR04-T67000',
    'yes_ask': 0.94,
    'yes_bid': 0.91,
    'no_ask': 0.07,
    'no_bid': 0.06,
    'volume_24h_fp': 500,
    'close_time': '2026-04-04T23:59:00Z',   # 4h+ verbleibend
    'title': 'Bitcoin price on Apr 4, 2026?',
}
context = {'btc_price': 68685.0, 'bankroll_usd': 80.0}  # 2.5% über 67000
sigs = eng.evaluate(market, context)
print(f'Test 1 (gut): {len(sigs)} Signal(e) – erwartet: 1')
for s in sigs: print(f'  {s.rule_name} | {s.side} {s.count}x @ {s.price_cents}¢')

# Test 2: Blockiert wegen zu wenig Restlaufzeit (< 2h)
import json as j2
market2 = dict(market)
market2['close_time'] = '2026-04-04T21:30:00Z'  # < 2h verbleibend
sigs2 = eng.evaluate(market2, context)
print(f'Test 2 (blockiert Zeit): {len(sigs2)} Signal(e) – erwartet: 0')

# Test 3: Blockiert wegen Range-Kontrakt
market3 = dict(market)
market3['title'] = 'Bitcoin price range on Apr 4, 2026?'
sigs3 = eng.evaluate(market3, context)
print(f'Test 3 (Range-Filter): {len(sigs3)} Signal(e) – erwartet: 0')
"
```

Erwartete Ausgabe:
```
[Crypto/LadderRules] 5 aktive Regeln geladen
Test 1 (gut): 1 Signal(e) – erwartet: 1
  Leiter – YES >85% → NO kaufen ... | no Xx @ Yc
Test 2 (blockiert Zeit): 0 Signal(e) – erwartet: 0
Test 3 (Range-Filter): 0 Signal(e) – erwartet: 0
```

- [ ] **Schritt 5.3: Commit**

```bash
git add app/crypto/rules.py
git commit -m "feat(crypto): Regelwerk mit Range-Fix, Spot-Distanz-Check und Zeit-Filter"
```

---

## Task 6: Crypto Scanner

**Ziel:** Scanner für BTC/ETH Leiter + 15-Min Märkte mit reparierter Exit-Logik.

**Fixes gegenüber altem scanner.py:**
- Stop-Loss: nur bei < 30min verbleibend (war 2h) + Schwelle 0.3% (war 0.5%)
- Zeit-Stop YES: bid < entry × 0.5 relativ (war absolut ≤ 20¢)
- Zeit-Stop NO: YES ≥ 90¢ (war 85¢)

**Files:**
- Create: `app/crypto/scanner.py`

- [ ] **Schritt 6.1: `app/crypto/scanner.py` erstellen**

```python
"""
Crypto Market Scanner (System 2).

Drei Sub-Tracks:
  A) Tages-Leiter (BTC, ETH, SOL, ...): btc_ladder_rules
  B) Leiter-Arbitrage: Preisumkehr-Detection
  C) 15-Min Mean Reversion: crypto_15min_rules

Exit-Logik (alle Fixes gegenüber altem scanner.py):
  Stop-Loss : < 30min + < 0.3% Abstand zur Schwelle (war: 2h + 0.5%)
  Zeit-Stop NO: < 10min + YES ≥ 90¢ (war: 15min + 85¢)
  Zeit-Stop YES: < 10min + bid < entry × 0.5 (war: absolut ≤ 20¢)
  Take-Profit NO: bid ≥ 2.5× Einstieg (war: 2×)
"""

import asyncio
import json
import logging
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Optional

from api.client import KalshiClient
from api.ws_client import KalshiWSFeed
from crypto.rules import CryptoLadderRuleEngine, Crypto15MinRuleEngine, CryptoSignal
from feeds.bingx_feed import BingXFeed, SERIES_SYMBOL_MAP, series_to_symbol
from logger.trade_logger import TradeLogger
from trader.executor import Signal

logger = logging.getLogger(__name__)

SYSTEM         = "crypto"
_LADDER_SERIES = frozenset({"KXBTCD", "KXETHD", "KXSOLD", "KXXRPD", "KXDOGED", "KXBNBD"})


def _to_executor_signal(cs: CryptoSignal) -> Signal:
    return Signal(
        ticker      = cs.ticker,
        rule_name   = cs.rule_name,
        side        = cs.side,
        action      = cs.action,
        price_cents = cs.price_cents,
        count       = cs.count,
        reason      = cs.reason,
        meta        = {**cs.meta, "system": SYSTEM},
        track       = cs.track,
    )


def _cents(val) -> int:
    f = float(val)
    return int(round(f * 100)) if f <= 1.0 else int(round(f))


class CryptoScanner:
    def __init__(
        self,
        client:       KalshiClient,
        trade_logger: TradeLogger,
        config:       dict,
        on_signal:    Callable,
        on_meta:      Optional[Callable] = None,
        on_cycle_end: Optional[Callable] = None,
    ):
        self._client      = client
        self._logger      = trade_logger
        self._config      = config
        self._on_signal   = on_signal
        self._on_meta     = on_meta
        self._on_cycle_end = on_cycle_end

        scan_cfg          = config.get("crypto_scanner", {})
        self._interval_s  = int(scan_cfg.get("interval_seconds", 30))
        self._ladder_on   = bool(scan_cfg.get("ladder_enabled", True))
        self._min15_on    = bool(scan_cfg.get("min15_enabled", True))
        self._min_vol     = float(scan_cfg.get("min_volume_usd", 25))

        sys_cfg           = config.get("systems", {}).get(SYSTEM, {})
        self._bankroll    = float(sys_cfg.get("max_exposure_usd", 80.0))

        self._ladder_rules = CryptoLadderRuleEngine(config)
        self._min15_rules  = Crypto15MinRuleEngine(config)

        self._ws_feed: Optional[KalshiWSFeed] = None
        if self._min15_on:
            self._ws_feed = KalshiWSFeed(client)

        # BingX Feeds: ein Feed pro Symbol
        self._bingx_feeds: dict[str, BingXFeed] = {}
        seen: set[str] = set()
        for symbol in SERIES_SYMBOL_MAP.values():
            if symbol not in seen:
                self._bingx_feeds[symbol] = BingXFeed(symbol=symbol, refresh_interval_s=30)
                seen.add(symbol)

        self._stop_event          = asyncio.Event()
        self._ladder_event_tickers: list[str] = []
        self._exit_pending:        set[str]   = set()
        self._exchange_trading:    bool       = True
        self._last_exchange_check: float      = 0.0

        logger.info(
            f"[Crypto/Scanner] Gestartet | Intervall: {self._interval_s}s | "
            f"Leiter: {self._ladder_on} | 15-Min: {self._min15_on} | "
            f"Budget: ${self._bankroll:.0f}"
        )

    async def start(self):
        ws_active = False
        if self._ws_feed:
            ws_active = await self._ws_feed.start()
            logger.info(f"[Crypto/Scanner] WebSocket: {'aktiv' if ws_active else 'REST-Fallback'}")

        while not self._stop_event.is_set():
            t0 = time.monotonic()
            try:
                await self._scan_cycle()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception(f"[Crypto/Scanner] Fehler: {e}")
                self._logger.log_error("CryptoScanner", str(e))
            elapsed = time.monotonic() - t0
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(),
                    timeout=max(0.0, self._interval_s - elapsed),
                )
            except asyncio.TimeoutError:
                pass

    async def stop(self):
        self._stop_event.set()
        if self._ws_feed:
            await self._ws_feed.stop()

    # ── Scan-Zyklus ──────────────────────────────────────────────────── #

    async def _scan_cycle(self):
        loop    = asyncio.get_running_loop()
        signals = 0

        # Exchange-Status alle 5 Minuten
        now_ts = time.monotonic()
        if now_ts - self._last_exchange_check > 300:
            try:
                status = await loop.run_in_executor(None, self._client.get_exchange_status)
                self._exchange_trading  = bool(status.get("trading_active", True))
                self._last_exchange_check = now_ts
            except Exception as e:
                logger.debug(f"[Crypto/Scanner] Exchange-Status fehlgeschlagen: {e}")
        if not self._exchange_trading:
            return

        # BingX Feeds aktualisieren
        for feed in self._bingx_feeds.values():
            await loop.run_in_executor(None, feed.refresh)

        if self._ladder_on:
            signals += await self._scan_ladder(loop)
            signals += await self._scan_arb(loop)

        signals += await self._scan_exits(loop)

        if signals:
            logger.info(f"[Crypto/Scanner] {signals} Signal(e)")
        else:
            logger.debug("[Crypto/Scanner] Keine Signale")

        if self._on_cycle_end:
            await self._on_cycle_end()

    # ── Track A: Tages-Leiter ─────────────────────────────────────────── #

    async def _scan_ladder(self, loop) -> int:
        bankroll_usd = self._bankroll

        sym_ctxs: dict[str, dict] = {}
        for sym, feed in self._bingx_feeds.items():
            if feed.is_ready():
                sym_ctxs[sym] = {**feed.context(), "bankroll_usd": bankroll_usd}

        btc_ctx = sym_ctxs.get("BTC-USDT", {"bankroll_usd": bankroll_usd})

        def _thr_val(ticker: str) -> float:
            for sep in ("-T", "-B"):
                if sep in ticker:
                    try:
                        return float(ticker.split(sep)[-1])
                    except Exception:
                        pass
            return 0.0

        def _pc(m: dict, key: str) -> Optional[int]:
            v = m.get(key + "_dollars") or m.get(key)
            if v is None:
                return None
            f = float(v)
            return int(round(f * 100)) if f <= 1.0 else int(round(f))

        crypto_events = await loop.run_in_executor(None, self._client.get_all_crypto_events)
        if not crypto_events:
            return 0

        now            = datetime.now(timezone.utc)
        markets:       list[dict] = []
        min15_markets: list[dict] = []
        active_15m_tickers: set[str] = set()
        self._ladder_event_tickers = []

        min15_vol = float(next(
            (r["condition"].get("threshold", 1000)
             for r in self._config.get("crypto_15min_rules", [])
             if r.get("condition", {}).get("type") == "min_volume_usd"),
            1000,
        ))

        for ev in crypto_events:
            et     = ev.get("event_ticker", "")
            series = (ev.get("series_ticker") or et.split("-")[0]).upper()
            is_15m = "15M" in series

            if not any(series.startswith(pfx) for pfx in SERIES_SYMBOL_MAP):
                continue
            if series in _LADDER_SERIES and et not in self._ladder_event_tickers:
                self._ladder_event_tickers.append(et)

            for m in ev.get("markets", []):
                if m.get("status", "active") not in ("active", "open"):
                    continue
                m["_series"]     = series
                m["category"]    = "crypto"
                m["event_title"] = (ev.get("title") or "").strip()
                if not m.get("image_url"):
                    m["image_url"] = (ev.get("image_url") or "").strip()

                if is_15m:
                    if not self._min15_on:
                        continue
                    close = m.get("close_time", "")
                    try:
                        ct        = datetime.fromisoformat(close.replace("Z", "+00:00"))
                        mins_left = (ct - now).total_seconds() / 60
                        if mins_left < 10 or mins_left > 15:
                            continue
                    except Exception:
                        continue
                    if float(m.get("volume_24h_fp", 0) or 0) >= min15_vol:
                        active_15m_tickers.add(m.get("ticker", ""))
                        min15_markets.append(m)
                else:
                    vol = float(m.get("volume_24h_fp", 0) or 0)
                    if self._min_vol > 0 and vol < self._min_vol:
                        continue
                    # Distanz-Filter
                    sym = series_to_symbol(series)
                    spot_ctx = sym_ctxs.get(sym, {}) if sym else {}
                    spot_p   = spot_ctx.get("btc_price")
                    if spot_p and spot_p > 0:
                        thr = _thr_val(m.get("ticker", ""))
                        if thr > 0:
                            change_abs   = abs(spot_ctx.get("btc_change_15min", 0) or 0)
                            adaptive_pct = 2.0 + min(change_abs * 0.5, 2.0)
                            lo = spot_p * (1 - adaptive_pct / 100)
                            hi = spot_p * (1 + adaptive_pct / 100)
                            if not (lo <= thr <= hi):
                                continue
                    markets.append(m)

        # MVE Combo ausschließen
        markets = [m for m in markets if not m.get("ticker", "").startswith("KXMVECROSSCATEGORY")]
        # Mindest-NO-Preis ≥ 5¢
        markets = [m for m in markets if (_pc(m, "no_ask") or 0) >= 5]

        logger.info(f"[Crypto/Scanner] {len(markets)} Leiter-Märkte")

        signals = 0
        for market in markets:
            sym     = series_to_symbol(market.get("_series", "")) or "BTC-USDT"
            ctx     = sym_ctxs.get(sym, {"bankroll_usd": bankroll_usd})
            for cs in self._ladder_rules.evaluate(market, ctx):
                await self._on_signal(_to_executor_signal(cs))
                signals += 1

        # 15-Min Märkte
        if min15_markets and self._min15_on:
            if self._ws_feed and self._ws_feed.is_connected():
                await self._ws_feed.subscribe(list(active_15m_tickers))
                await self._ws_feed.unsubscribe_stale(active_15m_tickers)
                enriched = []
                for m in min15_markets:
                    ws_data = self._ws_feed.get_market(m.get("ticker", ""))
                    enriched.append({**m, **ws_data} if ws_data else m)
                min15_markets = enriched

            by_sym: dict[str, list] = defaultdict(list)
            for m in min15_markets:
                sym = series_to_symbol(m.get("_series", "")) or "BTC-USDT"
                by_sym[sym].append(m)

            for sym, sym_markets in by_sym.items():
                ctx = sym_ctxs.get(sym, {"bankroll_usd": bankroll_usd})
                for m in sym_markets:
                    for cs in self._min15_rules.evaluate(m, ctx):
                        await self._on_signal(_to_executor_signal(cs))
                        signals += 1

        return signals

    # ── Track B: Leiter-Arbitrage ─────────────────────────────────────── #

    async def _scan_arb(self, loop) -> int:
        if not self._ladder_event_tickers:
            return 0

        def _price(m, key):
            v = m.get(key + "_dollars") or m.get(key)
            if v is None:
                return None
            f = float(v)
            return int(round(f * 100)) if f <= 1.0 else int(round(f))

        def _threshold(ticker: str) -> float:
            try:
                return float(ticker.split("-T")[-1].replace(".99", ""))
            except Exception:
                return 0.0

        signals = 0
        for evt_ticker in self._ladder_event_tickers:
            try:
                mkt_data = await loop.run_in_executor(
                    None,
                    lambda t=evt_ticker: self._client.get_markets(
                        event_ticker=t, status="open", limit=60
                    ),
                )
            except Exception:
                continue

            valid = []
            for m in mkt_data.get("markets", []):
                ya  = _price(m, "yes_ask")
                na  = _price(m, "no_ask")
                vol = float(m.get("volume_24h_fp", 0) or 0)
                if ya and na and vol >= 500:
                    valid.append((m, _threshold(m.get("ticker", "")), ya, na))

            if len(valid) < 2:
                continue
            valid.sort(key=lambda x: x[1])

            for i in range(len(valid) - 1):
                m_low, thr_low, ya_low, _ = valid[i]
                m_high, thr_high, ya_high, na_high = valid[i + 1]
                if thr_high <= thr_low or ya_high <= ya_low:
                    continue
                arb_cost = ya_low + na_high
                if 100 - arb_cost < 1:
                    continue

                series = evt_ticker.split("-")[0]
                profit_min = 100 - arb_cost
                logger.info(
                    f"[Crypto/ARB] {series}: Preisumkehr "
                    f"T{thr_low:.0f} YES={ya_low}¢ < T{thr_high:.0f} YES={ya_high}¢ | "
                    f"min +{profit_min}¢/Paar"
                )
                for sig_side, m_target, px_target, track in [
                    ("yes", m_low,  ya_low  + 1, "arb"),
                    ("no",  m_high, na_high + 1, "arb"),
                ]:
                    await self._on_signal(Signal(
                        ticker      = m_target["ticker"],
                        rule_name   = f"ARB – {'YES günstig' if sig_side == 'yes' else 'NO teuer'}",
                        side        = sig_side,
                        action      = "buy",
                        price_cents = px_target,
                        count       = 5,
                        reason      = f"Arb {series}: min +{profit_min}¢/Paar",
                        meta        = {
                            "close_time": m_target.get("close_time", ""),
                            "title":      (m_target.get("title") or "").strip(),
                            "event_ticker": evt_ticker,
                            "category":   "crypto",
                            "sub_title":  "",
                            "image_url":  "",
                            "system":     SYSTEM,
                        },
                        track       = track,
                    ))
                    signals += 1
        return signals

    # ── Exit-Logik (ALLE FIXES) ───────────────────────────────────────── #

    async def _scan_exits(self, loop) -> int:
        """
        Reparierte Exit-Logik für Crypto-Positionen.

        Fixes gegenüber altem scanner.py:
          Stop-Loss   : < 30min (war 2h) + < 0.3% Abstand (war 0.5%)
          Zeit-Stop NO: < 10min + YES ≥ 90¢ (war 15min + 85¢)
          Zeit-Stop YES: < 10min + bid < entry × 0.5 (war absolut ≤ 20¢)
          Take-Profit NO: bid ≥ 2.5× Einstieg (war 2×)
        """
        try:
            data      = json.loads(Path("data/positions.json").read_text())
            positions = [p for p in data.get("positions", []) if p.get("system") == SYSTEM]
        except Exception:
            return 0

        current_tickers = {p.get("ticker", "") for p in positions}
        self._exit_pending = {t for t in self._exit_pending if t in current_tickers}

        def _spot_price(ticker: str) -> Optional[float]:
            sym  = series_to_symbol(ticker)
            if sym:
                feed = self._bingx_feeds.get(sym)
                if feed and feed.is_ready():
                    return feed.current_price()
            return None

        now   = datetime.now(timezone.utc)
        exits = 0

        for pos in positions:
            ticker    = pos.get("ticker", "")
            side      = str(pos.get("side", "")).lower()
            entry_px  = int(pos.get("price_cents", 0))
            count     = int(pos.get("count", 0))
            close_str = pos.get("close_time", "")

            if not ticker or not entry_px or not count:
                continue
            if ticker in self._exit_pending:
                continue

            mins_left = float("inf")
            try:
                ct        = datetime.fromisoformat(close_str.replace("Z", "+00:00"))
                mins_left = max(0.0, (ct - now).total_seconds() / 60)
            except Exception:
                pass

            if mins_left < 0:
                continue

            try:
                market = await loop.run_in_executor(
                    None, lambda t=ticker: self._client.get_market(t)
                )
            except Exception:
                continue
            if not market:
                continue

            def _pc(key: str) -> Optional[int]:
                v = market.get(key + "_dollars") or market.get(key)
                if v is None:
                    return None
                f = float(v)
                return int(round(f * 100)) if f <= 1.0 else int(round(f))

            yes_ask = _pc("yes_ask")
            yes_bid = _pc("yes_bid")
            no_ask  = _pc("no_ask")
            no_bid  = _pc("no_bid")

            exit_reason: Optional[str] = None
            sell_price:  int           = entry_px

            if side == "no":
                # Take-Profit: NO bid ≥ 2.5× Einstieg (mehr Geduld als 2×)
                tp_target = int(entry_px * 2.5)
                if no_bid and no_bid >= tp_target:
                    exit_reason = f"Take-Profit: NO bid {no_bid}¢ ≥ 2.5× {entry_px}¢"
                    sell_price  = max(1, no_bid - 1)

                # Stop-Loss: Spot nah an Schwelle – NUR wenn < 30min (FIX: war 2h)
                # UND Abstand < 0.3% (FIX: war 0.5%)
                elif (spot_price := _spot_price(ticker)) and mins_left < 30:
                    try:
                        thr      = float(ticker.split("-T")[-1])
                        pct_away = abs(spot_price - thr) / thr * 100
                        if pct_away < 0.3:
                            exit_reason = (
                                f"Stop-Loss: ${spot_price:,.2f} nur {pct_away:.2f}% "
                                f"von Schwelle ${thr:,.0f} | {mins_left:.0f}min verbl."
                            )
                            sell_price = max(1, (no_bid or 1))
                    except Exception:
                        pass

                # Zeit-Stop: < 10min (FIX: war 15min) + YES ≥ 90¢ (FIX: war 85¢)
                if not exit_reason and mins_left < 10 and yes_ask and yes_ask >= 90:
                    exit_reason = (
                        f"Zeit-Stop: {mins_left:.0f}min verbl. | "
                        f"YES {yes_ask}¢ ≥ 90¢ → NO verliert"
                    )
                    sell_price = max(1, no_bid or 1)

            elif side == "yes":
                # Take-Profit: YES bid ≥ 1.8× Einstieg (cap 95¢)
                tp_target = min(95, int(entry_px * 1.8))
                if yes_bid and yes_bid >= tp_target:
                    exit_reason = f"Take-Profit: YES bid {yes_bid}¢ ≥ {tp_target}¢"
                    sell_price  = max(1, yes_bid - 1)

                # Zeit-Stop: < 10min + bid < entry × 0.5 (FIX: war absolut ≤ 20¢)
                elif mins_left < 10 and yes_bid and yes_bid < int(entry_px * 0.5):
                    exit_reason = (
                        f"Zeit-Stop: {mins_left:.0f}min verbl. | "
                        f"YES bid {yes_bid}¢ < 50% von Einstieg {entry_px}¢"
                    )
                    sell_price = max(1, yes_bid or 1)

            if not exit_reason:
                continue

            logger.info(
                f"[Crypto/Exit] {ticker} EXIT {side.upper()} ×{count} "
                f"@ {sell_price}¢ | {exit_reason}"
            )
            self._exit_pending.add(ticker)
            await self._on_signal(Signal(
                ticker      = ticker,
                rule_name   = f"Exit: {exit_reason[:60]}",
                side        = side,
                action      = "sell",
                price_cents = sell_price,
                count       = count,
                reason      = exit_reason,
                meta        = {
                    "close_time":        close_str,
                    "exit":              True,
                    "entry_price_cents": entry_px,
                    "system":            SYSTEM,
                },
            ))
            exits += 1

        return exits
```

- [ ] **Schritt 6.2: Import-Test**

```bash
cd /home/ubuntu/kalshi/app
python3 -c "from crypto.scanner import CryptoScanner; print('OK')"
```

Erwartete Ausgabe: `OK`

- [ ] **Schritt 6.3: Commit**

```bash
git add app/crypto/scanner.py
git commit -m "feat(crypto): Scanner mit repariertem Stop-Loss (0.3%/30min) + Zeit-Stop (relativ)"
```

---

## Task 7: Risk Manager – System-Awareness

**Ziel:** Positionen mit `system`-Tag versehen, getrennte Exposure-Limits pro System einhalten.

**Files:**
- Modify: `app/risk/manager.py`

- [ ] **Schritt 7.1: `record_order` um `system`-Parameter erweitern**

In `app/risk/manager.py`, Methode `record_order` (Zeile ~180), die Signatur ändern:

```python
def record_order(self, ticker: str, count: int, price_cents: int, is_buy: bool,
                 close_time: str = "", *, side: str = "", rule_name: str = "",
                 title: str = "", event_title: str = "", reason: str = "", entered_at: str = "",
                 category: str = "", event_ticker: str = "", sub_title: str = "",
                 image_url: str = "", system: str = ""):
```

Im `if is_buy:` Block die `_details`-Speicherung erweitern:

```python
if ticker not in self._details:
    self._details[ticker] = {
        "side": side,
        "price_cents": price_cents,
        "count": count,
        "rule_name": rule_name,
        "title": title,
        "event_title": event_title,
        "reason": reason,
        "entered_at": entered_at or datetime.now(timezone.utc).isoformat(),
        "category": category,
        "event_ticker": event_ticker,
        "sub_title": sub_title,
        "image_url": image_url,
        "system": system,          # ← NEU
    }
```

- [ ] **Schritt 7.2: `_load_positions_from_file` um `system` erweitern**

In der Methode `_load_positions_from_file` (Zeile ~48), die `detail_keys` Liste erweitern:

```python
detail_keys = [
    "side", "price_cents", "count", "rule_name", "title", "event_title",
    "reason", "entered_at", "category", "event_ticker", "sub_title", "image_url",
    "system",    # ← NEU
]
```

- [ ] **Schritt 7.3: `check_order_allowed` um System-Budgets erweitern**

Neue Hilfsmethode `_system_exposure` und Update in `check_order_allowed` hinzufügen:

```python
def _system_exposure(self, system: str) -> float:
    """Berechnet die aktuelle Exposure eines Systems."""
    return sum(
        exp for tkr, exp in self._positions.items()
        if self._details.get(tkr, {}).get("system") == system
    )

def check_order_allowed(self, ticker: str, count: int, price_cents: int,
                        system: str = "") -> tuple[bool, str]:
    exposure_usd = count * price_cents / 100
    current      = self._positions.get(ticker, 0.0)

    if self._dry_run and current > 0:
        return False, f"Bereits positioniert (${current:.2f}) – warte auf Settlement"

    if current + exposure_usd > self.max_position_usd:
        return False, (f"Max-Position überschritten: {current:.2f} + "
                       f"{exposure_usd:.2f} > {self.max_position_usd:.2f} USD")

    # System-Budget prüfen (wenn konfiguriert)
    if system:
        sys_cfg     = self._config.get("systems", {}).get(system, {})
        sys_max_usd = float(sys_cfg.get("max_exposure_usd", self.max_total_exposure_usd))
        sys_current = self._system_exposure(system)
        if sys_current + exposure_usd > sys_max_usd:
            return False, (f"System-Budget '{system}' überschritten: "
                           f"${sys_current:.2f} + ${exposure_usd:.2f} > ${sys_max_usd:.2f}")

    total = self.get_total_exposure()
    if total + exposure_usd > self.max_total_exposure_usd:
        return False, (f"Max-Exposure überschritten: {total:.2f} + "
                       f"{exposure_usd:.2f} > {self.max_total_exposure_usd:.2f} USD")

    if (self.max_open_positions > 0 and ticker not in self._positions
            and self.get_open_count() >= self.max_open_positions):
        return False, (f"Max. offene Positionen erreicht: "
                       f"{self.get_open_count()} >= {self.max_open_positions}")

    return True, ""
```

Außerdem: `__init__` muss `config` speichern. Am Anfang von `__init__` hinzufügen:

```python
self._config = config   # ← NEU (direkt nach self._dry_run = ...)
```

- [ ] **Schritt 7.4: Smoke-Test**

```bash
cd /home/ubuntu/kalshi/app
python3 -c "
from risk.manager import RiskManager
from api.client import KalshiClient
import json
cfg = json.load(open('config.json'))

# Mock-Client (kein API-Call nötig)
class MockClient: pass
rm = RiskManager(MockClient(), cfg)

# Test: System-Budget
rm.record_order('TEST-A', 5, 80, True, system='prediction')
rm.record_order('TEST-B', 3, 70, True, system='crypto')
ok, reason = rm.check_order_allowed('TEST-C', 10, 80, system='prediction')
print(f'Prediction-Budget nach 1 Trade: OK={ok} | {reason or \"innerhalb Limit\"}')
exp_p = rm._system_exposure('prediction')
exp_c = rm._system_exposure('crypto')
print(f'Prediction-Exposure: \${exp_p:.2f} | Crypto-Exposure: \${exp_c:.2f}')
"
```

Erwartete Ausgabe:
```
Prediction-Budget nach 1 Trade: OK=True | innerhalb Limit
Prediction-Exposure: $4.00 | Crypto-Exposure: $2.10
```

- [ ] **Schritt 7.5: Commit**

```bash
git add app/risk/manager.py
git commit -m "feat(risk): System-Tag in Positionen + getrennte Budgets pro System"
```

---

## Task 8: Executor – System-Tag weiterleiten

**Ziel:** Den `system`-Wert aus `signal.meta` an `record_order` und `check_order_allowed` weiterleiten.

**Files:**
- Modify: `app/trader/executor.py`

- [ ] **Schritt 8.1: `_process`-Methode anpassen**

In `app/trader/executor.py`, in der Methode `_process` (ab Zeile ~185), die Zeilen wo `check_order_allowed` und `record_order` aufgerufen werden anpassen:

```python
async def _process(self, signal: Signal):
    is_buy = signal.action == "buy"

    if not is_buy:
        await self._execute_exit(signal)
        return

    self._risk.refresh_positions()

    # System-Tag aus Meta lesen
    system = signal.meta.get("system", "")

    allowed, reason = self._risk.check_order_allowed(
        signal.ticker, signal.count, signal.price_cents, system=system
    )
    if not allowed:
        logger.debug(f"[Executor] Abgelehnt | {signal.ticker}: {reason}")
        _expected = ("Bereits positioniert", "Max-Position", "Max-Exposure",
                     "System-Budget", "Max. offene Positionen")
        if not any(reason.startswith(p) for p in _expected):
            self._logger.log_error("RiskBlock", reason, extra={"ticker": signal.ticker})
        return

    logger.info(
        f"[Executor] {'[DRY] ' if self._dry_run else ''}Order | "
        f"[{system or '?'}] {signal.ticker} {signal.action} {signal.side} "
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
                "reason":     signal.reason,
                "dry_run":    True,
                "close_time": signal.meta.get("close_time", ""),
                "title":      signal.meta.get("title", ""),
                "system":     system,
            },
        )
        self._risk.record_order(
            signal.ticker, signal.count, signal.price_cents, True,
            close_time  = signal.meta.get("close_time", ""),
            side        = signal.side,
            rule_name   = signal.rule_name,
            title       = signal.meta.get("title", ""),
            event_title = signal.meta.get("event_title", ""),
            reason      = signal.reason,
            category    = signal.meta.get("category", ""),
            event_ticker= signal.meta.get("event_ticker", ""),
            sub_title   = signal.meta.get("sub_title", ""),
            image_url   = signal.meta.get("image_url", ""),
            system      = system,   # ← NEU
        )
        # ... (rest unverändert)
```

- [ ] **Schritt 8.2: Smoke-Test Import**

```bash
cd /home/ubuntu/kalshi/app
python3 -c "from trader.executor import TradeExecutor; print('OK')"
```

- [ ] **Schritt 8.3: Commit**

```bash
git add app/trader/executor.py
git commit -m "feat(executor): system-Tag aus Signal.meta an RiskManager weiterleiten"
```

---

## Task 9: main.py – Beide Systeme starten

**Ziel:** `main.py` startet beide Scanner parallel, alte `MarketScanner`-Referenz entfernen.

**Files:**
- Modify: `app/main.py`

- [ ] **Schritt 9.1: `main.py` komplett ersetzen**

```python
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
    risk     = RiskManager(client, config)
    executor = TradeExecutor(client, risk, trade_logger, config)
    settlement = SettlementTracker(client, trade_logger, config)

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

    logger.info(f"[Main] Bot läuft mit {len(tasks) - 3} aktivem/n Scanner-System(en).")
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
```

- [ ] **Schritt 9.2: Import-Test (alle Module)**

```bash
cd /home/ubuntu/kalshi/app
python3 -c "
import json
cfg = json.load(open('config.json'))
from prediction.rules import PredictionRuleEngine
from prediction.scanner import PredictionScanner
from crypto.rules import CryptoLadderRuleEngine, Crypto15MinRuleEngine
from crypto.scanner import CryptoScanner
from risk.manager import RiskManager
from trader.executor import TradeExecutor
print('Alle Imports OK')
print(f'Prediction-Regeln: {len(PredictionRuleEngine(cfg)._rules)}')
print(f'Crypto-Leiter-Regeln: {len(CryptoLadderRuleEngine(cfg)._rules)}')
print(f'Crypto-15Min-Regeln: {len(Crypto15MinRuleEngine(cfg)._rules)}')
"
```

Erwartete Ausgabe:
```
Alle Imports OK
Prediction-Regeln: 6
Crypto-Leiter-Regeln: 5
Crypto-15Min-Regeln: 2
```

- [ ] **Schritt 9.3: Commit**

```bash
git add app/main.py
git commit -m "feat: dual-system main.py – Prediction + Crypto Scanner parallel"
```

---

## Task 10: Alte strategy/ entfernen + Dry-Run validieren

**Ziel:** Aufräumen und validieren, dass der Bot im Dry-Run korrekt startet.

**Files:**
- Delete: `app/strategy/rules.py`
- Delete: `app/strategy/scanner.py`

- [ ] **Schritt 10.1: Prüfen, ob strategy/ noch importiert wird**

```bash
cd /home/ubuntu/kalshi/app
grep -r "from strategy" . --include="*.py"
grep -r "import strategy" . --include="*.py"
```

Erwartete Ausgabe: **keine Treffer** (wenn alle Tasks abgeschlossen)

- [ ] **Schritt 10.2: strategy/ löschen**

```bash
rm app/strategy/rules.py app/strategy/scanner.py
# __init__.py behalten (leer) – verhindert Importfehler falls externe Referenz
```

- [ ] **Schritt 10.3: Bot-Start im Dry-Run testen**

```bash
cd /home/ubuntu/kalshi
python3 app/main.py 2>&1 | head -30
```

Erwartete Ausgabe (nach ~3 Sekunden mit Ctrl+C):
```
[Main] Exchange: active=True trading=True
[Main] Balance: $200.00
[Prediction/Rules] 6 aktive Regeln geladen
[Prediction/Scanner] Gestartet | Intervall: 60s | Budget: $80
[Crypto/LadderRules] 5 aktive Regeln geladen
[Crypto/15MinRules] 2 aktive Regeln geladen
[Crypto/Scanner] Gestartet | Intervall: 30s | Budget: $80
[Main] Bot läuft mit 2 aktivem/n Scanner-System(en).
```

- [ ] **Schritt 10.4: Abschluss-Commit**

```bash
git add -A
git commit -m "refactor: alte strategy/ entfernt – Dual-System vollständig aktiv"
```

---

## Selbst-Review

### Spec-Abdeckung

| Anforderung | Task |
|---|---|
| Prediction Markets mit Polymarket-Regelwerk | Task 3+4 |
| Crypto Markets mit eigenem Regelwerk | Task 5+6 |
| Range-Filter in Crypto (war Hauptfehler) | Task 1 + Task 5 |
| YES-Positionen nur mit Mindest-Restlaufzeit (>4h) | Task 1 + Task 5 |
| Stop-Loss auf 0.3%/30min korrigiert | Task 6 |
| Zeit-Stop relativ (50% des Entry-Preises) | Task 6 |
| Getrennte Budgets ($80/$80) | Task 1 + Task 7 |
| System-Tag in Positionen | Task 7+8 |
| Beide Scanner laufen parallel | Task 9 |
| Alte strategy/ entfernt | Task 10 |

### Bekannte Lücken

- `strategy/__init__.py` bleibt erhalten (harmlos, kein Code)
- `btc_price.py` in feeds/ wurde nicht migriert – wird nicht mehr direkt verwendet
- `update_detail`-Callback in den neuen Scannern wird aktuell nicht genutzt (low priority)
