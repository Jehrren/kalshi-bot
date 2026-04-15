"""
Diagnose-Script: Warum feuern keine Crypto-Ladder-Signale?

Führt einen vollständigen Simulations-Scan durch und zeigt für jeden Markt
den genauen Blockierungs-Grund.
"""
import json
import os
import sys
import asyncio
from pathlib import Path
from datetime import datetime, timezone

# Projekt-Root zum sys.path hinzufügen
sys.path.insert(0, str(Path(__file__).parent))

from api.client import KalshiClient
from feeds.bingx_feed import BingXFeed, SERIES_SYMBOL_MAP, series_to_symbol
from crypto.rules import CryptoLadderRuleEngine

# ── Config laden ────────────────────────────────────────────────────── #
config = json.loads(Path("config.json").read_text())
scan_cfg = config.get("crypto_scanner", {})
MIN_VOL_BY_SERIES = dict(scan_cfg.get("min_volume_by_series", {}))
MIN_VOL_DEFAULT   = float(scan_cfg.get("min_volume_usd", 25))
_LADDER_SERIES    = frozenset({"KXBTCD", "KXETHD", "KXSOLD", "KXXRPD", "KXDOGED", "KXBNBD"})

RULE_ENGINE = CryptoLadderRuleEngine(config)


def _cents(m, key):
    v = m.get(key + "_dollars") or m.get(key)
    if v is None:
        return None
    try:
        f = float(v)
        return int(round(f * 100)) if f <= 1.0 else int(round(f))
    except (ValueError, TypeError):
        return None


def _thr(ticker):
    for sep in ("-T", "-B"):
        if sep in ticker:
            try:
                return float(ticker.split(sep)[-1])
            except Exception:
                pass
    return 0.0


def _hours(close_str):
    if not close_str:
        return float("inf")
    try:
        ct = datetime.fromisoformat(close_str.replace("Z", "+00:00"))
        return max(0.0, (ct - datetime.now(timezone.utc)).total_seconds() / 3600)
    except Exception:
        return float("inf")


def check_ladder_rule(market, ctx, rules):
    """Liefert (reason_str, signal_count) für einen Markt."""
    yes_ask = _cents(market, "yes_ask")
    no_ask  = _cents(market, "no_ask")
    vol     = float(market.get("volume_24h_fp", 0) or 0)
    title   = str(market.get("title", "")).lower()
    ticker  = market.get("ticker", "")
    thr_val = _thr(ticker)
    hours   = _hours(market.get("close_time", ""))
    spot    = ctx.get("spot_price") or ctx.get("btc_price")
    rsi     = ctx.get("bingx_rsi")
    ob_imb  = ctx.get("bingx_ob_imbalance")

    reasons = []

    # SKIP-Filter
    if "range" in title:
        return "SKIP: title contains 'range'", 0
    if vol < 100:
        return f"SKIP: volume ${vol:.0f} < $100", 0
    if yes_ask is not None and 45 <= yes_ask <= 55:
        return f"SKIP: 50/50-Zone (YES={yes_ask}¢)", 0

    if (no_ask or 0) < 5:
        return f"SKIP: no_ask={no_ask}¢ < 5¢", 0

    # YES-above check (NO-buy)
    for rule in rules:
        act = rule.get("action", {})
        if act.get("side") == "SKIP":
            continue
        cond = rule.get("condition", {})
        t = cond.get("type", "")
        if t == "yes_ask_above":
            thr     = int(cond.get("threshold", 85))
            thr_max = int(cond.get("threshold_max", 0))
            min_hrs = float(cond.get("min_hours_remaining", 2.0))
            max_hrs = float(cond.get("max_hours_remaining", 0))
            min_over = float(cond.get("spot_min_overshoot_pct", 0)) / 100

            if yes_ask is None:
                reasons.append(f"[yes_ask_above] yes_ask=None")
                continue
            if yes_ask <= thr:
                reasons.append(f"[yes_ask_above] YES={yes_ask}¢ <= {thr}¢ (zu niedrig)")
                continue
            if thr_max > 0 and yes_ask > thr_max:
                reasons.append(f"[yes_ask_above] YES={yes_ask}¢ > threshold_max={thr_max}¢")
                continue
            if max_hrs > 0 and hours > max_hrs:
                reasons.append(f"[yes_ask_above] {hours:.1f}h > max {max_hrs:.0f}h")
                continue
            if hours < min_hrs:
                reasons.append(f"[yes_ask_above] {hours:.1f}h < min {min_hrs:.1f}h")
                continue
            if min_over > 0 and thr_val <= 0:
                reasons.append(f"[yes_ask_above] Threshold nicht parsebar aus Ticker")
                continue
            if min_over > 0 and (not spot or spot <= 0):
                reasons.append(f"[yes_ask_above] Kein Spot-Preis")
                continue
            if spot and spot > 0 and thr_val > 0:
                overshoot = (spot - thr_val) / thr_val
                if overshoot < min_over:
                    reasons.append(
                        f"[yes_ask_above] Overshoot {overshoot:.2%} < {min_over:.2%} "
                        f"(spot ${spot:,.0f} vs thr ${thr_val:,.0f})"
                    )
                    continue
            # OB-Imbalance
            if ob_imb and ob_imb > 1.2:
                reasons.append(f"[yes_ask_above] OB-Imbalance={ob_imb:.2f} > 1.2 BLOCKIERT")
                continue
            reasons.append(f"[yes_ask_above] MATCH! YES={yes_ask}¢, spot=${spot}, thr=${thr_val}")

        elif t == "yes_ask_between":
            low  = int(cond.get("threshold_low", 73))
            high = int(cond.get("threshold_high", 82))
            min_hrs = float(cond.get("min_hours_remaining", 4.0))
            max_hrs = float(cond.get("max_hours_remaining", 0))
            req_rsi = bool(cond.get("require_rsi_oversold", False))
            rsi_thr = float(cond.get("rsi_oversold_threshold", 40))

            if yes_ask is None:
                reasons.append(f"[yes_ask_between {low}-{high}] yes_ask=None")
                continue
            if not (low <= yes_ask <= high):
                reasons.append(f"[yes_ask_between {low}-{high}] YES={yes_ask}¢ außerhalb")
                continue
            if max_hrs > 0 and hours > max_hrs:
                reasons.append(f"[yes_ask_between {low}-{high}] {hours:.1f}h > max {max_hrs:.0f}h")
                continue
            if hours < min_hrs:
                reasons.append(f"[yes_ask_between {low}-{high}] {hours:.1f}h < min {min_hrs:.1f}h")
                continue
            if req_rsi:
                if rsi is None or rsi > rsi_thr:
                    reasons.append(
                        f"[yes_ask_between {low}-{high}] RSI={rsi} > {rsi_thr} (nicht überverkauft)"
                    )
                    continue
            reasons.append(f"[yes_ask_between {low}-{high}] MATCH! YES={yes_ask}¢")

    return "; ".join(reasons) if reasons else "Kein aktiver Regel-Match", 0


async def main():
    from dotenv import load_dotenv
    load_dotenv()  # .env im aktuellen Verzeichnis oder Parent suchen
    key_id  = os.getenv("KALSHI_API_KEY_ID", "")
    key_pem = os.getenv("KALSHI_PRIVATE_KEY", "")

    client = KalshiClient(api_key_id=key_id, private_key_pem=key_pem)
    loop   = asyncio.get_running_loop()

    # BingX Feeds aufwärmen (nur BTC und ETH für Diagnose)
    feeds: dict[str, BingXFeed] = {}
    print("=== BingX Feed Warmup (30 Kerzen) ===")
    for sym in set(SERIES_SYMBOL_MAP.values()):
        f = BingXFeed(symbol=sym, refresh_interval_s=30)
        feeds[sym] = f
        try:
            await asyncio.wait_for(loop.run_in_executor(None, f.refresh), timeout=20.0)
            ready = f.is_ready()
            ctx   = f.context() if ready else {}
            print(
                f"  {sym}: ready={ready} | price={ctx.get('spot_price') or ctx.get('btc_price')} | "
                f"RSI={ctx.get('bingx_rsi', 'N/A'):.1f} | "
                f"OB-Imb={ctx.get('bingx_ob_imbalance', 'N/A')} | "
                f"trend={ctx.get('bingx_trend', 'N/A')}"
                if ready else f"  {sym}: NICHT BEREIT ({len(f._candles)}/30 Kerzen)"
            )
        except Exception as e:
            print(f"  {sym}: Fehler {e}")

    # Märkte laden
    print("\n=== Crypto Ladder Märkte ===")
    try:
        events = await asyncio.wait_for(
            loop.run_in_executor(None, client.get_all_crypto_events),
            timeout=30.0,
        )
    except Exception as e:
        print(f"Fehler beim Laden der Events: {e}")
        return

    now = datetime.now(timezone.utc)
    rules = [r for r in config.get("crypto_ladder_rules", []) if r.get("enabled", True)]

    # Auswertung
    total = skipped_vol = skipped_dist = in_range = 0
    sym_ctxs: dict[str, dict] = {}
    bankroll_usd = float(scan_cfg.get("bankroll_usd", 75.0))
    for sym, f in feeds.items():
        if f.is_ready():
            sym_ctxs[sym] = {**f.context(), "bankroll_usd": bankroll_usd}

    for ev in (events or []):
        et     = ev.get("event_ticker", "")
        series = (ev.get("series_ticker") or et.split("-")[0]).upper()
        if series not in _LADDER_SERIES:
            continue

        for m in ev.get("markets", []):
            if m.get("status", "active") not in ("active", "open"):
                continue
            ticker = m.get("ticker", "")
            vol    = float(m.get("volume_24h_fp", 0) or 0)
            series_min = MIN_VOL_BY_SERIES.get(series, MIN_VOL_DEFAULT)

            total += 1
            if vol < series_min:
                skipped_vol += 1
                continue

            # Distanz-Filter
            sym = series_to_symbol(series) or "BTC-USDT"
            ctx = sym_ctxs.get(sym, {"bankroll_usd": 75.0})
            spot = ctx.get("spot_price") or ctx.get("btc_price")
            thr_val = _thr(ticker)
            yes_ask = _cents(m, "yes_ask")
            no_ask  = _cents(m, "no_ask")
            hours   = _hours(m.get("close_time", ""))

            if spot and spot > 0 and thr_val > 0:
                change_abs = abs(ctx.get("spot_change_15min") or ctx.get("btc_change_15min", 0) or 0)
                adaptive_pct = 2.0 + min(change_abs * 0.5, 2.0)
                lo = spot * 0.95
                hi = spot * (1 + adaptive_pct / 100)
                if not (lo <= thr_val <= hi):
                    skipped_dist += 1
                    continue

            in_range += 1
            reason, _ = check_ladder_rule(m, ctx, rules)
            overshoot = ""
            if spot and thr_val > 0:
                ov = (spot - thr_val) / thr_val * 100
                overshoot = f" | overshoot={ov:+.1f}%"
            print(
                f"  {ticker:<45} YES={yes_ask:>3}¢ NO={no_ask:>3}¢ "
                f"vol=${vol:>7.0f} {hours:.1f}h{overshoot}"
            )
            print(f"    → {reason}")

    print(f"\n=== Zusammenfassung ===")
    print(f"  Gesamt Märkte:        {total}")
    print(f"  Vol-Filter (zu klein):{skipped_vol}")
    print(f"  Distanz-Filter:       {skipped_dist}")
    print(f"  Im Scan-Fenster:      {in_range}")


if __name__ == "__main__":
    asyncio.run(main())
