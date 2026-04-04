# Kalshi Prediction Market Trading Bot

Regelbasierter Algo-Trading-Bot für [Kalshi](https://kalshi.com) – eine regulierte US-Prediction-Market-Plattform, auf der binäre Kontrakte (YES/NO) auf reale Ereignisse gehandelt werden (Politik, Wirtschaft, Krypto, Sport).

Inklusive **Live-Dashboard** (React + Node.js) für Positionen, Settlements und Systemstatus.

---

## Features

- **3 unabhängige Trading-Tracks** mit eigenen Regelwerken
- **Multi-Asset BingX Feed** für technische Indikatoren (BTC, ETH, SOL, XRP, DOGE, BNB, HYPE, SHIB)
- **Kalshi WebSocket** für Echtzeit-Preise bei 15-Min-Fenstern
- **Dry-Run-Modus** mit persistenten In-Memory-Positionen
- **Kelly-Criterion Positionsgröße** für Crypto-Ladder
- **Auto-Settlement** mit P&L-Tracking und Gebührenberechnung
- **Live-Dashboard** mit mobiler Optimierung

---

## Trading-Strategie

### Grundlage: Markt-Kalibrierungsforschung

Kalshi-Märkte sind systematisch zu optimistisch bei YES-Chancen. Das erzeugt statistisch verwertbare Edges:

| Marktzone | Beobachtung | Aktion |
|---|---|---|
| YES-Ask **45–55 ¢** | Bis zu 7 % YES-Bias, kein Edge | SKIP |
| YES-Ask **55–65 ¢** | ~1–2 % Überschätzung | NO kaufen (klein) |
| YES-Ask **73–82 ¢** | Beste Kalibrierung, kaum Bias | YES kaufen |
| YES-Ask **> 90 ¢** | ~5 % Overconfidence-Bias | NO kaufen (groß) |

### Track 1 – Politische & Wirtschaftliche Märkte

Scannt alle Kategorien (Economics, Politics, Financials, Health, Science usw.) nach Märkten mit Laufzeit ≤ 30 Tage, filtert nach Mindestvolumen und Open Interest, wendet die Kalibrierungsregeln an.

### Track 2 – Crypto Preis-Leiter

Entdeckt Crypto-Preisstufen-Märkte dynamisch via Kalshi Events-API (KXBTC, KXETH, KXSOL usw.). Kombiniert Kalibrierungsregeln mit BingX-Indikatoren (RSI, EMA, Distanz zur Schwelle). Kelly-Sizing mit Quarter-Kelly.

### Track 3 – BTC 15-Min Mean Reversion

Tradet kurzfristige Fenster-Märkte (z. B. "BTC über $67.000 um 15:15 Uhr?"). Einstieg nur in den ersten 5 Minuten eines Fensters bei RSI-Überkauf/-Überverkauf und starker BTC-Preisbewegung.

---

## Architektur

```
kalshi/
├── app/
│   ├── main.py                  # Einstiegspunkt, asyncio-Orchestrierung
│   ├── config.json              # Regelwerke & Risikoparameter
│   ├── api/
│   │   ├── client.py            # Kalshi REST API (RSA-PSS-SHA256 Auth)
│   │   └── ws_client.py         # Kalshi WebSocket Feed
│   ├── feeds/
│   │   └── bingx_feed.py        # BingX OHLCV + Orderbuch (8 Assets)
│   ├── strategy/
│   │   ├── scanner.py           # 3-Track Market Scanner
│   │   └── rules.py             # Regel-Engine & Signal-Generierung
│   ├── trader/
│   │   └── executor.py          # Order-Ausführung (async Queue)
│   ├── risk/
│   │   └── manager.py           # Positions- & Risikoverwaltung
│   ├── settlement/
│   │   └── tracker.py           # Auto-Settlement & P&L-Berechnung
│   └── logger/
│       └── trade_logger.py      # JSONL-Logging mit Rotation
├── data/                        # Laufzeitdaten (nicht in Git)
│   ├── trades.jsonl             # Trades + Settlements
│   ├── signals.jsonl            # Generierte Signale
│   ├── errors.jsonl             # Warnungen & Fehler
│   ├── positions.json           # Aktive Positionen (für UI)
│   └── balance.json             # P&L-Tracking
├── Dockerfile
├── docker-compose.yml
└── requirements.txt
```

---

## Dashboard (trading-ui)

Separates Docker-Image mit Node.js-Backend und React-Frontend:

- **Aktive Wetten** – Gruppert nach Event mit Bildern, Laufzeiten, Preis-Bars
- **Abgerechnete Trades** – Settlement-History mit P&L, horizontale Scroll-Tabelle
- **System und Fehler** – Warnungen und Fehler aus `errors.jsonl`, immer sichtbar
- **Statistik-Panel** – Winrate, Gesamtexposure, bester/schlechtester Trade
- **Mobil-optimiert** – Gestapelte Card-Ansicht für Tabellen auf kleinen Bildschirmen

---

## Risiko-Management

```json
{
  "max_position_usd":       50.0,   // Max Exposure pro Ticker
  "max_total_exposure_usd": 200.0,  // Max Gesamtexposure
  "max_open_positions":     0       // 0 = kein Limit
}
```

- **Dry-Run**: Positionen werden in `data/positions.json` persistiert und nach Container-Restart wiederhergestellt
- **Auto-Expiry**: Abgelaufene Märkte werden automatisch aus dem Portfolio gebucht
- **Kelly-Sizing**: Optionales Quarter-Kelly für Crypto-Leiter-Märkte

---

## Setup

### Voraussetzungen

- Docker & Docker Compose
- Kalshi-Account mit API-Zugang
- (Optional) BingX-Account – nicht nötig, da öffentliche Market-Daten

### 1. Repository klonen

```bash
git clone https://github.com/Jehrren/kalshi-bot.git
cd kalshi-bot
```

### 2. API-Schlüssel einrichten

Im Kalshi-Account unter **Settings → API** ein RSA-Schlüsselpaar anlegen.

```bash
cp .env.example .env
# .env bearbeiten:
KALSHI_API_KEY_ID=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
KALSHI_PRIVATE_KEY="-----BEGIN RSA PRIVATE KEY-----
...
-----END RSA PRIVATE KEY-----"
LOG_LEVEL=INFO
```

### 3. Bot starten

```bash
docker compose up --build -d
docker compose logs -f kalshi
```

### 4. Dashboard starten

```bash
cd trading-ui
docker compose up --build -d
# Erreichbar unter http://localhost:4173
```

---

## Konfiguration

Alle Handelsregeln in `app/config.json`:

```json
{
  "dry_run": true,
  "scanner": {
    "interval_seconds": 30,
    "btc_ladder_enabled": true,
    "btc_15min_enabled": true,
    "max_close_days": 30,
    "min_volume_usd": 25,
    "categories": ["economics", "politics", "financials", "...]
  },
  "risk": {
    "max_position_usd": 50.0,
    "max_total_exposure_usd": 200.0
  }
}
```

Zum Live-Betrieb `"dry_run": false` setzen. Mit `"dry_run": true` werden keine echten Orders platziert.

---

## Datenfluss

```
MarketScanner (alle 30s)
  ├─ Track 1: Kalshi Events API → Politische Märkte
  ├─ Track 2: Kalshi Events API → Crypto-Leiter-Märkte
  │            + BingX OHLCV/Orderbuch (RSI, EMA, Distanz)
  └─ Track 3: Kalshi WS-Feed → BTC 15-Min-Fenster
                              + BingX Echtzeit-Preis
        │
        ▼ Signal → asyncio.Queue
TradeExecutor
  ├─ RiskManager.check_order_allowed()
  ├─ KalshiClient.place_order()  (oder DryRun-Simulation)
  └─ TradeLogger.log_trade()

SettlementTracker (jede 60s)
  ├─ Ablaufende Positionen erkennen
  ├─ Settlement-Ergebnis von Kalshi holen
  └─ P&L berechnen + in trades.jsonl schreiben
```

---

## Authentifizierung

Kalshi nutzt **RSA-PSS-SHA256-Signaturen**. Jeder API-Request wird mit dem Private Key signiert. Die Signatur enthält Timestamp + HTTP-Methode + Pfad. Implementiert in `api/client.py` via `cryptography`-Bibliothek.

---

## BingX Feed

Öffentliche REST-API, kein Account nötig:

| Indikator | Beschreibung |
|---|---|
| `rsi_14` | RSI auf 1-Min-Candles |
| `ema_9 / ema_21` | Kurzfristige Trendrichtung |
| `vol_ratio` | Volumen vs. 20-Perioden-Durchschnitt |
| `trend` | `up` / `down` / `flat` |
| `ob_imbalance` | Bid/Ask-Imbalance Top-5 Levels |
| `btc_change_5min/15min` | Prozentuale Preisänderung |

Unterstützte Assets: **BTC, ETH, SOL, XRP, DOGE, BNB, HYPE, SHIB**

---

## Logs

```bash
# Live-Trades verfolgen
docker exec kalshi tail -f data/trades.jsonl

# Signale
docker exec kalshi tail -f data/signals.jsonl

# Bot-Logs
docker compose logs -f kalshi

# Aktive Positionen
docker exec kalshi cat data/positions.json
```

Alle Log-Dateien rotieren automatisch bei 10 MB (max. 3 Generationen).

---

## Hinweise

- Preise in **Cent** (1–99). 50 ¢ = 50 % Wahrscheinlichkeit.
- Ein Kontrakt kostet den Ask-Preis und zahlt **100 ¢** bei Gewinn.
- Max-Verlust pro Kontrakt = Kaufpreis. Kein Leverage, kein Liquidationsrisiko.
- Standardmäßig ist `dry_run: true` – vor dem ersten echten Lauf prüfen.
