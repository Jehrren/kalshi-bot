# Kalshi Trading Bot – Vollständiges Regelwerk
Stand: 2026-04-03

---

## Überblick

Regelbasierter Bot für Kalshi Prediction Markets. Binary YES/NO Märkte, Settlement $1.00 pro Contract, Preise in Cent = implizite Wahrscheinlichkeit. Kapital: ~$200. Drei parallele Scan-Tracks alle 30 Sekunden.

**Edge-These**: Kalshi-Märkte zeigen systematische Kalibrierungsfehler (Overconfidence-Bias / Longshot-Effekt). Märkte mit YES>90% überschätzen die Sicherheit um 3–4%. Zusätzlich entstehen kurzzeitige Preisumkehrungen auf der BTC-Leiter (Arbitrage) und Überreaktionen nach BTC-Preisbewegungen (Mean Reversion).

---

## Datenpipeline

Einzige externe Datenquelle: **BingXFeed** (BTC-USDT, 1-Minuten-Candles, öffentliche API, kein Key)

| Signal            | Herkunft                          | Refresh |
|-------------------|-----------------------------------|---------|
| `btc_price`       | BingX letzte Close-Candle         | 30s     |
| `btc_change_5min` | change_pct(5) aus 1-min Candles   | 30s     |
| `btc_change_15min`| change_pct(15) aus 1-min Candles  | 30s     |
| `bingx_rsi`       | RSI(14) auf 1-min Closes          | 30s     |
| `bingx_ema9/21`   | EMA(9), EMA(21)                   | 30s     |
| `bingx_trend`     | "up"/"down"/"flat" via EMA-Kreuz  | 30s     |
| `bingx_vol_ratio` | aktuelles Vol / 20-Perioden-Ø     | 30s     |
| `bingx_ob_imbalance` | Bid/Ask USD-Imbalance Top-5    | 15s     |
| `bingx_ob_bid_wall_usd` | Größte Einzelposition Bids  | 15s     |
| `bingx_ob_ask_wall_usd` | Größte Einzelposition Asks  | 15s     |

Kalshi-Preise kommen aus der REST-API + optionalem WebSocket (nur für Track 3).

---

## Risiko-Limits

```
max_position_usd       = $50    # Pro Ticker (alle Contracts zusammen)
max_total_exposure_usd = $200   # Gesamtes offenes Risiko
max_open_positions     = 20     # Anzahl Positionen gleichzeitig
```

---

## Track 1: Politische / Wirtschaftliche Märkte

**Aktivierung**: `scanner.categories` muss Kategorien enthalten (aktuell leer → Track deaktiviert)

**Filter-Kette (SKIP wenn)**:
1. 24h-Volumen < $100
2. Open Interest < $500
3. YES ask im Bereich 45–55¢ (50/50-Zone, kein Edge)

**Handels-Regeln**:

| Bedingung          | Aktion      | Contracts | Begründung |
|--------------------|-------------|-----------|------------|
| YES ask > 90¢      | NO kaufen   | 5         | ~5% Overconfidence-Bias bei hoher Sicherheit |
| YES ask 55–65¢     | NO kaufen   | 3         | ~1–2% Bias, kleinere Position wegen geringerem Edge |
| YES ask 73–82¢     | YES kaufen  | 5         | Beste Kalibrierungszone laut Daten, leicht unterbewertet |

Limit-Orders: ask + 1¢ (verbessert Fill-Chance ohne Edge zu kosten).

---

## Track 2: BTC Tages-Leiter (KXBTCD)

**Serie**: KXBTCD – täglich, ~50 Preisstufen (z.B. T65799 = "BTC ≥ $65.799,99 um Marktschluss")

### Filter-Kette (5 Stufen, in Reihenfolge)

**Filter 1 – Mindest-Restlaufzeit + Volumen**
- Überspringe Stufen mit < 10 Min bis Ablauf
- Überspringe Stufen mit 24h-Volumen < $1.000

**Filter 2 – Distanz zum aktuellen BTC-Preis**
- Basis: ±2% um aktuellen BTC-Preis
- Adaptiv: Bei hoher Intraday-Vola (|Δ15min|%) wird der Bereich ausgeweitet (max +2%, also bis ±4%)
- Stufen außerhalb dieses Fensters haben keinen Informationsgehalt für uns

**Filter 3 – MVE Combo Contracts ausschließen**
- Ticker-Prefix `KXMVECROSSCATEGORY` → immer überspringen
- Keine Exit-Liquidität, zu komplexe Auszahlungsstruktur

**Filter 4 – Mindest-NO-Preis ≥ 5¢**
- Stufen mit NO ask < 5¢ (= YES > 95¢) ausschließen
- 1–2¢ NO sind Lotterie-Tickets, keine sinnvolle Kelly-Berechnung möglich

**Filter 5 – Kalshi Orderbuch (Adverse Selection)**
- Check A: `yes_bid > 0`? Wenn nein → kein Counterparty, überspringen
- Check B: Participant-Orderbook abfragen. Wenn OB leer → normaler MM-Markt, durchlassen.
  Wenn OB nicht leer und YES-Wand von > 200 Contracts bei ≥97¢ → informierter Käufer vorhanden, überspringen (adverse selection)

### Handels-Regeln (btc_ladder_rules) – mit Kelly Sizing

| Bedingung          | Aktion     | Kelly-Fraktion | Max Contracts | Begründung |
|--------------------|------------|---------------|---------------|------------|
| YES ask > 90¢      | NO kaufen  | 25% (Quarter) | 20            | Stärkster Edge: Overconfidence bei Sicherheit |
| YES ask 55–65¢     | NO kaufen  | 25% (Quarter) | 10            | Geringer Edge, konservativeres Maximum |
| YES ask 73–82¢     | YES kaufen | 25% (Quarter) | 15            | Gut kalibrierte Zone, leichte Unterbewertung |

### Kelly-Formel

```
f* = (p_win × b − q) / b
     wobei: b = (1 − Preis_cents/100) / (Preis_cents/100)
            q = 1 − p_win

Position_USD = f* × 0.25 × Bankroll_USD
Contracts    = int(Position_USD / Preis_USD)
               min: 1, max: je nach Regel (10–20)
```

**Bias-Korrektur** (vor Kelly): `bias_corrected_yes_prob(yes_ask_cents)`

```
YES ask ≥ 95¢        → wahre YES-Wahrscheinlichkeit = ask/100 − 3%
YES ask 90–94¢       → − 4%   (stärkste Overconfidence-Zone)
YES ask 85–89¢       → − 3%
YES ask 73–82¢       → + 1%   (gut kalibriert, leicht unterbewertet)
YES ask 55–65¢       → − 1%   (kleiner Bias)
sonst               → unverändert
```

**Wichtig**: Kelly berechnet immer auf `no_ask` (Marktpreis), nicht auf `no_ask + 1¢` (unser Limit-Preis). Der +1¢ Offset verbessert nur die Fill-Wahrscheinlichkeit, darf den Edge-Preis nicht verfälschen.

### Bonus: YES-Momentum (RSI überverkauft)

**Bedingung**: RSI ≤ 35 UND BTC Δ15min ≤ −0.3%
**Aktion**: Kaufe YES auf die 3 Stufen leicht über aktuellem BTC-Preis mit YES ask 25–65¢
**Preis**: ask + 1¢, 5 Contracts (festes Sizing, kein Kelly da Mean-Reversion-Bet)
**Logik**: Wenn RSI überverkauft und BTC fällt, erwarten Märkte zu starken Einbruch → Erholung wahrscheinlich → niedrigere Stufen werden billiger als gerechtfertigt

---

## Track 2b: Leiter-Arbitrage

**Logik**: Für X < Y gilt immer: P(BTC > X) ≥ P(BTC > Y). Wenn YES(T_höher) > YES(T_tiefer): Preisumkehr = garantierter Gewinn unabhängig vom BTC-Ausgang.

**Bedingungen**:
- Mindest-Volumen: $1.000 pro Stufe
- Mindestgewinn: > 1¢ pro Contractpaar (nach Spread)
- Nur aktuellstes KXBTCD-Event

**Berechnung**:
```
Arb-Kosten     = YES(tiefer) + NO(höher) = ya_low + (100 − ya_high)
Mindestgewinn  = 100 − Arb-Kosten  [Cent pro Paar]
```

**Signale** (2 pro erkannter Umkehr):
1. Signal 1: YES auf niedrigere Stufe zu `ya_low + 1¢`, 5 Contracts
2. Signal 2: NO auf höhere Stufe zu `(100 − ya_high) + 1¢`, 5 Contracts

**In JEDEM Szenario profitabel**: Ob BTC über, zwischen oder unter beiden Schwellen schließt – immer gewinnt mindestens ein Leg genug, um das andere zu kompensieren plus Mindestgewinn.

---

## Track 3: BTC 15-Min UP/DOWN (KXBTC15M)

**Marktstruktur**: 15-Minuten-Fenster, Binary UP/DOWN (BTC über/unter dem Eröffnungspreis bei Ablauf)

**Timing-Filter**: Nur Fenster mit 10–15 Min Restlaufzeit = **erste 5 Minuten** des Fensters. Spätere Einträge haben zu viel information gegen uns.

**Volumen-Filter**: $1.000 Mindest-Volumen

**Preise**: WebSocket (KalshiWSFeed) wenn verbunden, sonst REST-Fallback

**Mean-Reversion-Regel (btc_15min_mean_reversion)**:

Parameter:
```
change_threshold_pct = 0.3%    # Mindest-Bewegung für Signal
bias_threshold       = 0.65    # = 65¢ (Markt preist ≥65% für eine Seite)
rsi_overbought       = 68
rsi_oversold         = 32
vol_ratio_min        = 0.8     # Mindest-Volumen (kein sterbender Markt)
```

**Mit RSI (BingX bereit)**:
| Bedingung | Aktion | Logik |
|-----------|--------|-------|
| RSI ≥ 68 AND YES ask ≥ 65¢ AND vol_ratio ≥ 0.8 | NO kaufen | Überkauft → Kursrückgang erwartet |
| RSI ≤ 32 AND YES ask ≤ 35¢ AND vol_ratio ≥ 0.8 | YES kaufen | Überverkauft → Erholung erwartet |

**Fallback ohne RSI** (BingX noch nicht warm):
| Bedingung | Aktion |
|-----------|--------|
| Δ15min ≥ +0.3% AND YES ask ≥ 65¢ | NO kaufen |
| Δ15min ≤ −0.3% AND YES ask ≤ 35¢ | YES kaufen |

10 Contracts, Limit ask + 1¢.

---

## Track 4: Exit-Mechanismus

Läuft bei jedem Scan-Zyklus. Liest offene Positionen aus `data/positions.json`.

### Exit-Trigger für NO-Positionen

| Trigger | Bedingung | Sell-Preis |
|---------|-----------|------------|
| Take-Profit | NO bid ≥ 2× Einstiegspreis | no_bid − 1¢ |
| Stop-Loss | BTC-Preis < 0.5% von Schwelle UND < 120 Min Restlaufzeit | no_bid − 1¢ |
| Zeit-Stop | < 15 Min Restlaufzeit UND YES ask ≥ 85¢ | no_bid |

### Exit-Trigger für YES-Positionen

| Trigger | Bedingung | Sell-Preis |
|---------|-----------|------------|
| Take-Profit | YES bid ≥ min(95¢, 2× Einstiegspreis) | yes_bid − 1¢ |
| Zeit-Stop | < 15 Min Restlaufzeit UND YES bid ≤ 20¢ | yes_bid |

### Exit-Liquiditäts-Check

Vor dem Exit: Kalshi Orderbook prüfen.
- OB leer → normaler MM-Markt → Exit zu MM-Preis immer möglich, volles Count behalten.
- OB hat Participant-Orders → MM deckt trotzdem Rest → kein Reduzieren des Counts.

---

## Was wir NICHT handeln und warum

| Ausschluss | Grund |
|------------|-------|
| YES ask 45–55¢ (50/50-Zone) | Schlechteste Kalibrierung, bis zu 7% YES-Bias, kein Edge |
| NO ask < 5¢ (YES > 95¢) | Kelly liefert 0–1 Contracts, Kosten übersteigen Edge |
| Stufen außerhalb ±2–4% BTC-Preis | Keine Entscheidungsrelevanz, binden Kapital ohne Informationsgehalt |
| KXMVECROSSCATEGORY | Komplexe Auszahlung, keine Exit-Liquidität |
| Märkte mit YES bid = 0 | Kein Counterparty vorhanden |
| Adverse Selection: YES-Wand > 200ct @ ≥97¢ | Informierter Gegenspieler = wir haben keinen Edge |
| Polymarket, Manifold, Metaculus | Geo-geblockt oder keine API |

---

## Modus

- **dry_run: true** → Alle Orders werden simuliert, keine echten Kalshi-Orders
- **dry_run: false** → Live-Trading, erfordert ausgefüllte `.env` mit Key

---

---

## Zukunftshinweis: Atomic Execution bei Leiter-Arbitrage

**Aktueller Stand (bis ~$500 Kapital): kein Handlungsbedarf.**

Die Leiter-Arbitrage (Track 2b) sendet zwei unabhängige Signals – eines für YES auf Stufe A, eines für NO auf Stufe B. Falls nur ein Leg gefüllt wird (Partial Fill), entsteht eine einseitige Direktionalposition. Das ist **kein Totalverlust** – wir haben einfach eine normale gerichtete Wette statt einer hedged Arb. Bei $200 Kapital ist das Risiko eines einzelnen ungefüllten Legs klein.

**Ab ~$5.000 Kapital sinnvoll**:
- Order-ID Tracking implementieren: Wenn Leg 1 nicht gefüllt wird, Leg 2 sofort stornieren
- Implementierungsort: `trader/executor.py`, neue `_execute_arb_pair()` Methode
- Benötigt: Order-Status-Polling (Kalshi REST: GET `/orders/{order_id}`) oder Kalshi-Orderbook-Subscription via WebSocket

**Theoretisch atomare Ausführung** (Cross-Platform, z.B. Kalshi + Polymarket):
- Erfordert Jito Bundles (Solana-spezifisch) oder ähnliche Blockchain-Infrastruktur
- Erst ab $50.000+ relevant und technisch sehr komplex
- Für uns irrelevant (geoblocked von Polymarket; single-platform Arb hat ohnehin kein Gegenpartei-Risiko da Kalshi selbst Settlement garantiert)

