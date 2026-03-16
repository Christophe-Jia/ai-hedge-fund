"""
Analyze Polymarket geopolitical signal accuracy vs BTC/ETH volatility — Feb 2026.

Focus: US/Israel-Iran strike event around 2026-02-28.

Pipeline:
  1. Load market price histories from data/polymarket/price_history_feb2026/
  2. Compute anomaly flags (z-score of hourly probability change > 1.5σ)
  3. Load BTC/ETH 1h OHLCV from data/polymarket/{btc,eth}_1h_feb2026.csv
  4. Event-align: for each anomaly, extract ±2h pre / ±8h post BTC/ETH volatility window
  5. Zoomed view: highlight events in Feb 24–Mar 2 window around the Feb 28 event
  6. Statistical test: Mann-Whitney U (anomaly vs non-anomaly windows)
  7. Print report + save data/polymarket/analysis_iran_feb2026.json

Usage:
  poetry run python scripts/polymarket/analyze_iran_feb2026.py
  poetry run python scripts/polymarket/analyze_iran_feb2026.py --min-vol 100000
"""

import argparse
import json
import sys
from pathlib import Path
from datetime import datetime, timezone
from typing import NamedTuple

try:
    import numpy as np
    import pandas as pd
    from scipy import stats
except ImportError as exc:
    raise SystemExit(
        "Required: numpy, pandas, scipy.\n"
        "Run: poetry add pandas scipy  (numpy already present via pandas)"
    ) from exc

DATA_DIR = Path(__file__).parent.parent.parent / "data" / "polymarket"
PRICE_HISTORY_DIR = DATA_DIR / "price_history_feb2026"
MARKETS_FILE = DATA_DIR / "markets_feb2026.json"

# ── anomaly detection ─────────────────────────────────────────────────────────
ZSCORE_THRESHOLD = 1.5      # lower than default (2.0) to catch subtler pre-event moves
ROLLING_WINDOW = 20         # hours for rolling baseline

# ── event window ─────────────────────────────────────────────────────────────
PRE_HOURS = 2
POST_HOURS = 8              # extended to see full crypto reaction after strike

# ── zoomed focus window around Feb 28 event ───────────────────────────────────
ZOOM_START = datetime(2026, 2, 24, tzinfo=timezone.utc)
ZOOM_END = datetime(2026, 3, 2, tzinfo=timezone.utc)


# ── data loading ──────────────────────────────────────────────────────────────
def load_market_meta(min_volume: float) -> dict[str, dict]:
    """Return {conditionId: metadata_dict} filtered by volume."""
    if not MARKETS_FILE.exists():
        print(f"[!] {MARKETS_FILE} not found — run fetch_markets_feb2026.py first")
        return {}
    markets = json.loads(MARKETS_FILE.read_text())
    result = {}
    for m in markets:
        cid = m.get("conditionId") or m.get("id", "")
        if not cid:
            continue
        vol = float(m.get("volume", m.get("volumeNum", 0)) or 0)
        if vol >= min_volume:
            result[cid] = m
    return result


def load_price_history(condition_id: str) -> pd.Series | None:
    """Return a pd.Series of YES probability indexed by UTC datetime."""
    path = PRICE_HISTORY_DIR / f"{condition_id}.json"
    if not path.exists():
        return None
    raw = json.loads(path.read_text())
    if not raw:
        return None
    # shape: [{"t": unix_sec, "p": 0.42}, ...]  or  [ts, p] list-of-lists
    records = []
    for item in raw:
        if isinstance(item, dict):
            ts = item.get("t") or item.get("timestamp")
            p = item.get("p") or item.get("price")
        elif isinstance(item, (list, tuple)) and len(item) >= 2:
            ts, p = item[0], item[1]
        else:
            continue
        if ts and p is not None:
            records.append((int(ts), float(p)))
    if not records:
        return None
    idx = pd.to_datetime([r[0] for r in records], unit="s", utc=True)
    series = pd.Series([r[1] for r in records], index=idx, name=condition_id)
    series = series.sort_index().loc[~series.index.duplicated()]
    return series


def load_crypto(symbol: str) -> pd.DataFrame | None:
    """Load BTC or ETH 1h OHLCV from the feb2026 CSV. symbol in {'btc', 'eth'}"""
    path = DATA_DIR / f"{symbol}_1h_feb2026.csv"
    if not path.exists():
        print(f"[!] {path} not found — run fetch_crypto_feb2026.py first")
        return None
    df = pd.read_csv(path, parse_dates=["datetime_utc"])
    df["datetime_utc"] = pd.to_datetime(df["datetime_utc"], utc=True)
    df = df.set_index("datetime_utc").sort_index()
    # hourly return and intrabar volatility
    df["ret"] = df["close"].pct_change()
    df["intrabar_vol"] = (df["high"] - df["low"]) / df["open"].replace(0, np.nan) * 100
    return df


# ── anomaly detection ─────────────────────────────────────────────────────────
def compute_anomaly_flags(series: pd.Series) -> pd.DataFrame:
    """
    Given hourly YES probability series, return DataFrame with anomaly flags.
    Uses ZSCORE_THRESHOLD = 1.5 (lower than default) for this analysis.
    """
    df = series.to_frame("prob")
    df["dp"] = df["prob"].diff()
    roll = df["dp"].rolling(ROLLING_WINDOW, min_periods=5)
    df["dp_mean"] = roll.mean()
    df["dp_std"] = roll.std()
    df["dp_z"] = (df["dp"] - df["dp_mean"]) / (df["dp_std"] + 1e-9)
    df["anomaly"] = df["dp_z"].abs() > ZSCORE_THRESHOLD
    df["direction"] = np.where(df["dp"] > 0, "YES_surge", "NO_surge")
    return df


# ── event-window analysis ─────────────────────────────────────────────────────
class WindowResult(NamedTuple):
    event_time: datetime
    direction: str
    market_question: str
    condition_id: str
    dp_z: float
    btc_vol_pre: float          # mean intrabar_vol in [t-PRE, t)
    btc_vol_post: float         # mean intrabar_vol in (t, t+POST]
    btc_cum_ret_1h: float
    btc_cum_ret_2h: float
    btc_cum_ret_4h: float
    btc_cum_ret_8h: float
    eth_vol_post: float
    eth_cum_ret_4h: float
    eth_cum_ret_8h: float
    in_zoom_window: bool        # True if event_time is in Feb 24–Mar 2


def extract_windows(
    anomaly_df: pd.DataFrame,
    btc: pd.DataFrame,
    eth: pd.DataFrame,
    condition_id: str,
    question: str,
) -> list[WindowResult]:
    results = []
    anomaly_times = anomaly_df[anomaly_df["anomaly"]].index

    for t in anomaly_times:
        direction = anomaly_df.loc[t, "direction"]
        dp_z = float(anomaly_df.loc[t, "dp_z"])
        in_zoom = ZOOM_START <= t.to_pydatetime() <= ZOOM_END

        def window_stats(crypto_df: pd.DataFrame, pre: int, post: int):
            t_pre = t - pd.Timedelta(hours=pre)
            t_post = t + pd.Timedelta(hours=post)
            pre_mask = (crypto_df.index >= t_pre) & (crypto_df.index < t)
            post_mask = (crypto_df.index > t) & (crypto_df.index <= t_post)
            pre_vol = float(crypto_df.loc[pre_mask, "intrabar_vol"].mean()) if pre_mask.any() else np.nan
            post_vol = float(crypto_df.loc[post_mask, "intrabar_vol"].mean()) if post_mask.any() else np.nan

            def cum_ret(hours: int) -> float:
                t_h = t + pd.Timedelta(hours=hours)
                close_at_t = crypto_df["close"].asof(t) if not crypto_df.empty else np.nan
                close_at_h = crypto_df["close"].asof(t_h) if not crypto_df.empty else np.nan
                if np.isnan(close_at_t) or np.isnan(close_at_h) or close_at_t == 0:
                    return np.nan
                return (close_at_h - close_at_t) / close_at_t * 100

            return pre_vol, post_vol, cum_ret(1), cum_ret(2), cum_ret(4), cum_ret(8)

        btc_pre_vol, btc_post_vol, btc_r1, btc_r2, btc_r4, btc_r8 = window_stats(btc, PRE_HOURS, POST_HOURS)
        _, eth_post_vol, _, _, eth_r4, eth_r8 = window_stats(eth, PRE_HOURS, POST_HOURS)

        results.append(WindowResult(
            event_time=t.to_pydatetime(),
            direction=direction,
            market_question=question,
            condition_id=condition_id,
            dp_z=dp_z,
            btc_vol_pre=btc_pre_vol,
            btc_vol_post=btc_post_vol,
            btc_cum_ret_1h=btc_r1,
            btc_cum_ret_2h=btc_r2,
            btc_cum_ret_4h=btc_r4,
            btc_cum_ret_8h=btc_r8,
            eth_vol_post=eth_post_vol,
            eth_cum_ret_4h=eth_r4,
            eth_cum_ret_8h=eth_r8,
            in_zoom_window=in_zoom,
        ))
    return results


# ── statistical tests ─────────────────────────────────────────────────────────
def run_stats_report(
    anomaly_windows: list[WindowResult],
    btc: pd.DataFrame,
    eth: pd.DataFrame,
) -> dict:
    """
    Compare anomaly-window BTC/ETH vol vs baseline (all non-anomaly hours).
    Returns a dict with key statistics.
    """
    baseline_btc_vol = btc["intrabar_vol"].dropna().values
    baseline_eth_vol = eth["intrabar_vol"].dropna().values

    anomaly_btc_post = np.array([w.btc_vol_post for w in anomaly_windows
                                  if not np.isnan(w.btc_vol_post)])
    anomaly_eth_post = np.array([w.eth_vol_post for w in anomaly_windows
                                  if not np.isnan(w.eth_vol_post)])

    def mw_test(a: np.ndarray, b: np.ndarray) -> tuple[float, float]:
        if len(a) < 3 or len(b) < 3:
            return np.nan, np.nan
        stat, p = stats.mannwhitneyu(a, b, alternative="greater")
        return float(stat), float(p)

    btc_stat, btc_p = mw_test(anomaly_btc_post, baseline_btc_vol)
    eth_stat, eth_p = mw_test(anomaly_eth_post, baseline_eth_vol)

    yes_surge = [w for w in anomaly_windows if w.direction == "YES_surge"]
    no_surge = [w for w in anomaly_windows if w.direction == "NO_surge"]
    zoom_windows = [w for w in anomaly_windows if w.in_zoom_window]

    def safe_mean(vals):
        v = [x for x in vals if not np.isnan(x)]
        return float(np.mean(v)) if v else np.nan

    result = {
        "n_anomalies": len(anomaly_windows),
        "n_yes_surge": len(yes_surge),
        "n_no_surge": len(no_surge),
        "n_in_zoom_window_feb24_mar2": len(zoom_windows),
        "btc": {
            "anomaly_post_vol_mean": safe_mean([w.btc_vol_post for w in anomaly_windows]),
            "baseline_vol_mean": float(np.mean(baseline_btc_vol)) if len(baseline_btc_vol) else np.nan,
            "mw_statistic": btc_stat,
            "mw_p_value": btc_p,
            "significant_at_0.05": btc_p < 0.05 if not np.isnan(btc_p) else False,
        },
        "eth": {
            "anomaly_post_vol_mean": safe_mean([w.eth_vol_post for w in anomaly_windows]),
            "baseline_vol_mean": float(np.mean(baseline_eth_vol)) if len(baseline_eth_vol) else np.nan,
            "mw_statistic": eth_stat,
            "mw_p_value": eth_p,
            "significant_at_0.05": eth_p < 0.05 if not np.isnan(eth_p) else False,
        },
        "btc_cum_ret_8h_by_direction": {
            "YES_surge_mean_%": safe_mean([w.btc_cum_ret_8h for w in yes_surge]),
            "NO_surge_mean_%": safe_mean([w.btc_cum_ret_8h for w in no_surge]),
        },
        "zoom_window_feb24_mar2": {
            "n_events": len(zoom_windows),
            "btc_vol_post_mean": safe_mean([w.btc_vol_post for w in zoom_windows]),
            "btc_cum_ret_8h_mean_%": safe_mean([w.btc_cum_ret_8h for w in zoom_windows]),
            "eth_cum_ret_8h_mean_%": safe_mean([w.eth_cum_ret_8h for w in zoom_windows]),
        },
    }
    return result


def _fmt(v, fmt=".4f"):
    if isinstance(v, float) and np.isnan(v):
        return "N/A"
    try:
        return format(v, fmt)
    except (TypeError, ValueError):
        return str(v)


# ── main ──────────────────────────────────────────────────────────────────────
def main(min_volume: float = 10_000) -> None:
    print("=== Polymarket Iran Strike Analysis — Feb 2026 ===\n")

    # load crypto
    print("Loading BTC/ETH price data …")
    btc = load_crypto("btc")
    eth = load_crypto("eth")
    if btc is None or eth is None:
        print("[!] Missing crypto data. Run fetch_crypto_feb2026.py first.")
        sys.exit(1)
    print(f"  BTC: {len(btc)} hourly candles  ({btc.index[0].date()} → {btc.index[-1].date()})")
    print(f"  ETH: {len(eth)} hourly candles  ({eth.index[0].date()} → {eth.index[-1].date()})\n")

    # load markets
    print(f"Loading market metadata (min_volume=${min_volume:,.0f}) …")
    market_meta = load_market_meta(min_volume)
    print(f"  {len(market_meta)} qualifying markets\n")

    if not market_meta:
        print("[!] No markets found. Run fetch_markets_feb2026.py first, or lower --min-vol.")
        sys.exit(1)

    # process each market
    all_windows: list[WindowResult] = []
    market_summaries = []

    for cid, meta in market_meta.items():
        question = meta.get("question", "")[:80]
        series = load_price_history(cid)
        if series is None or len(series) < ROLLING_WINDOW + 5:
            print(f"  [skip] {question[:50]} — insufficient price history")
            continue

        adf = compute_anomaly_flags(series)
        n_anomalies = int(adf["anomaly"].sum())
        vol = float(meta.get("volume", meta.get("volumeNum", 0)) or 0)

        print(f"  {question[:60]}")
        print(f"    vol=${vol:>10,.0f}  ticks={len(series)}  anomalies={n_anomalies}")

        if n_anomalies == 0:
            continue

        windows = extract_windows(adf, btc, eth, cid, question)
        all_windows.extend(windows)
        market_summaries.append({
            "condition_id": cid,
            "question": question,
            "volume": vol,
            "n_ticks": len(series),
            "n_anomalies": n_anomalies,
        })

    print(f"\nTotal anomaly events across all markets: {len(all_windows)}")

    if not all_windows:
        print("[!] No anomaly windows to analyze. Check price history files.")
        sys.exit(0)

    # statistical analysis
    print("\n=== Statistical Results ===\n")
    stats_report = run_stats_report(all_windows, btc, eth)

    print(f"N anomalies total : {stats_report['n_anomalies']}")
    print(f"  YES_surge       : {stats_report['n_yes_surge']}")
    print(f"  NO_surge        : {stats_report['n_no_surge']}")
    print(f"  In zoom window  : {stats_report['n_in_zoom_window_feb24_mar2']}  (Feb 24–Mar 2)")
    print()

    btc_r = stats_report["btc"]
    eth_r = stats_report["eth"]
    print("BTC volatility (intrabar high-low/open %):")
    print(f"  Anomaly window mean : {_fmt(btc_r['anomaly_post_vol_mean'])}%")
    print(f"  Baseline mean       : {_fmt(btc_r['baseline_vol_mean'])}%")
    print(f"  Mann-Whitney p      : {_fmt(btc_r['mw_p_value'])}  {'** SIGNIFICANT **' if btc_r['significant_at_0.05'] else '(not significant)'}")
    print()
    print("ETH volatility (intrabar high-low/open %):")
    print(f"  Anomaly window mean : {_fmt(eth_r['anomaly_post_vol_mean'])}%")
    print(f"  Baseline mean       : {_fmt(eth_r['baseline_vol_mean'])}%")
    print(f"  Mann-Whitney p      : {_fmt(eth_r['mw_p_value'])}  {'** SIGNIFICANT **' if eth_r['significant_at_0.05'] else '(not significant)'}")
    print()

    dr = stats_report["btc_cum_ret_8h_by_direction"]
    print("BTC 8h cumulative return by event direction:")
    print(f"  YES_surge events (war escalation?) : {_fmt(dr['YES_surge_mean_%'])}%")
    print(f"  NO_surge events (de-escalation?)   : {_fmt(dr['NO_surge_mean_%'])}%")
    print()

    zr = stats_report["zoom_window_feb24_mar2"]
    print("=== Zoomed Window: Feb 24–Mar 2 (around Feb 28 strike event) ===")
    print(f"  N events        : {zr['n_events']}")
    print(f"  BTC post vol    : {_fmt(zr['btc_vol_post_mean'])}%")
    print(f"  BTC 8h ret mean : {_fmt(zr['btc_cum_ret_8h_mean_%'])}%")
    print(f"  ETH 8h ret mean : {_fmt(zr['eth_cum_ret_8h_mean_%'])}%")

    # top anomaly events — all
    print("\n=== Top 20 Anomaly Events (by |z-score|) ===\n")
    sorted_windows = sorted(all_windows, key=lambda w: abs(w.dp_z), reverse=True)[:20]
    print(f"{'Time (UTC)':<20} {'Dir':<12} {'Z':>6} {'BTC vol%':>9} {'BTC 8h%':>8}  {'Zoom':>4}  Question")
    print("-" * 115)
    for w in sorted_windows:
        t_str = w.event_time.strftime("%Y-%m-%d %H:%M")
        zoom_flag = " [Z]" if w.in_zoom_window else "    "
        print(
            f"{t_str:<20} {w.direction:<12} {_fmt(w.dp_z, '.2f'):>6} "
            f"{_fmt(w.btc_vol_post, '.3f'):>9} {_fmt(w.btc_cum_ret_8h, '.3f'):>8}  "
            f"{zoom_flag}  {w.market_question[:50]}"
        )

    # zoomed window events sorted by time
    zoom_events = sorted(
        [w for w in all_windows if w.in_zoom_window],
        key=lambda w: (w.event_time, -abs(w.dp_z)),
    )
    if zoom_events:
        print(f"\n=== All Anomaly Events in Zoom Window Feb 24–Mar 2 (chronological) ===\n")
        print(f"{'Time (UTC)':<20} {'Dir':<12} {'Z':>6} {'BTC vol%':>9} {'BTC 1h%':>8} {'BTC 4h%':>8} {'BTC 8h%':>8}  Question")
        print("-" * 120)
        for w in zoom_events:
            t_str = w.event_time.strftime("%Y-%m-%d %H:%M")
            print(
                f"{t_str:<20} {w.direction:<12} {_fmt(w.dp_z, '.2f'):>6} "
                f"{_fmt(w.btc_vol_post, '.3f'):>9} {_fmt(w.btc_cum_ret_1h, '.3f'):>8} "
                f"{_fmt(w.btc_cum_ret_4h, '.3f'):>8} {_fmt(w.btc_cum_ret_8h, '.3f'):>8}  "
                f"{w.market_question[:50]}"
            )

    # save results
    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "event_focus": "US/Israel-Iran strike 2026-02-28",
        "parameters": {
            "zscore_threshold": ZSCORE_THRESHOLD,
            "rolling_window_hours": ROLLING_WINDOW,
            "pre_hours": PRE_HOURS,
            "post_hours": POST_HOURS,
            "min_volume_usd": min_volume,
            "zoom_start": ZOOM_START.isoformat(),
            "zoom_end": ZOOM_END.isoformat(),
        },
        "markets": market_summaries,
        "statistics": stats_report,
        "anomaly_events": [
            {
                "event_time": w.event_time.isoformat(),
                "direction": w.direction,
                "market_question": w.market_question,
                "condition_id": w.condition_id,
                "dp_z": w.dp_z,
                "in_zoom_window": w.in_zoom_window,
                "btc_vol_pre": w.btc_vol_pre,
                "btc_vol_post": w.btc_vol_post,
                "btc_cum_ret_1h_%": w.btc_cum_ret_1h,
                "btc_cum_ret_2h_%": w.btc_cum_ret_2h,
                "btc_cum_ret_4h_%": w.btc_cum_ret_4h,
                "btc_cum_ret_8h_%": w.btc_cum_ret_8h,
                "eth_vol_post": w.eth_vol_post,
                "eth_cum_ret_4h_%": w.eth_cum_ret_4h,
                "eth_cum_ret_8h_%": w.eth_cum_ret_8h,
            }
            for w in all_windows
        ],
    }
    out_path = DATA_DIR / "analysis_iran_feb2026.json"
    out_path.write_text(json.dumps(output, indent=2, default=str))
    print(f"\nFull results saved → {out_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Polymarket Iran strike analysis — Feb 2026")
    parser.add_argument(
        "--min-vol",
        type=float,
        default=10_000,
        help="Minimum market volume in USD (default: 10000)",
    )
    args = parser.parse_args()
    main(min_volume=args.min_vol)
