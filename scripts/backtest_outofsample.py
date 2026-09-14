#!/usr/bin/env python3
"""Out-of-sample backtest of the two-event-signal configuration.

The weekend_gap + funding_rate combination was validated as the optimal
two-signal config on the 2023-04-01 -> 2026-09-10 training window
(commit 07e03d0: +16.07% total, Sharpe 0.423, MDD -10.35%). Both signals'
thresholds (funding high +50% ann. / low -5% ann.; weekend |move| >= 5%)
were quantile-calibrated on 2023+ data — so 2021-01-01 -> 2023-03-02,
whose funding history has just been backfilled, is a TRUE out-of-sample
window. It spans the 2021 bull top, the 2021-05-19 crash, the full 2022
bear and the FTX collapse.

Windows:
  - out_of_sample: 2021-01-01 -> 2023-03-02 (inclusive)
  - full:          2021-01-01 -> 2026-09-11 (inclusive, continuous view)

Scenarios per window: combined 2-signal portfolio, each signal alone,
plus benchmarks (equal-weight basket B&H, BTC B&H, QQQ B&H).

Conventions (matching scripts/backtest_combined_signals.py, the canonical
07e03d0 version, except where noted):
  - Signals evaluated with as_of = trading day D (data strictly before
    D); trades execute at D's close. No look-ahead.
  - Target exposure = combined score (signed fraction of equity, capped
    at +/-1); |score| <= threshold (0.15) -> cash.
  - Rebalance when |target - net exposure| >= band (0.25).
  - IBKR costs: $0.005/share, min $1/order, 5bps half-spread.
  - DIFFERENCE vs canonical: the tradable universe on each day is the
    UNION of the fired signals' metadata["targets"] (equal weight across
    available names), not a fixed 4-symbol basket. Symbols without data
    on the day (e.g. COIN before its 2021-04-14 IPO) are skipped and
    counted; a universe change also triggers a rebalance so newly listed
    names actually enter.
  - Shorting allowed (score < 0); borrow costs not modeled.

This script only READS src/signals/* — it does not modify any signal
definitions or the canonical backtest.

Usage:
    poetry run python scripts/backtest_outofsample.py
"""

from __future__ import annotations

import json
import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import sqlalchemy as sa

from src.data.historical_store import HistoricalOHLCVStore
from src.data.nasdaq_store import NasdaqDailyStore
from src.selection.costs import IbkrCostModel
from src.signals import FundingRateSignal, Signal, SignalCombiner, SignalOutput, WeekendGapSignal

BASKET = ("COIN", "MSTR", "MARA", "RIOT")
BTC_SYMBOL = "BTC/USDT"
FUNDING_SYMBOL = "BTC/USDT:USDT"

OOS_START, OOS_END = "2021-01-01", "2023-03-03"          # [start, end) -> last day 2023-03-02
FULL_START, FULL_END = "2021-01-01", "2026-09-12"        # [start, end) -> last day 2026-09-11
THRESHOLD = 0.15
BAND = 0.25
INITIAL = 100_000.0

# Training-window reference (commit 07e03d0, window 2023-04-01..2026-09-10).
# The two-signal combo there is the "ablation_without_onchain_fundamental" row.
TRAINING_REF = {
    "source": "commit 07e03d0, reports/combined_signals.json, window 2023-04-01..2026-09-10",
    "combined_2sig": {"total_return_pct": 16.07, "cagr_pct": 4.45, "sharpe": 0.423, "max_drawdown_pct": -10.35},
    "single_weekend_gap": {"total_return_pct": 6.90, "sharpe": 0.253, "max_drawdown_pct": -10.35},
    "single_funding_rate": {"total_return_pct": 8.58, "sharpe": 0.364, "max_drawdown_pct": -6.88},
    "benchmarks": {
        "basket_buy_hold": {"total_return_pct": 172.63, "sharpe": 0.759, "max_drawdown_pct": -68.72},
        "btc_buy_hold": {"total_return_pct": 175.22, "sharpe": 0.870, "max_drawdown_pct": -52.97},
    },
}


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------


def load_stock_closes(start: str, end: str) -> pd.DataFrame:
    """Basket daily closes, tz-naive UTC index, forward-filled gaps."""
    store = NasdaqDailyStore(assetclass="stocks")
    frames = {}
    for sym in BASKET:
        df = store.get_daily(sym, start, end)
        if df.empty:
            raise RuntimeError(f"no data for {sym} in [{start}, {end}]")
        s = df["close"].copy()
        s.index = s.index.tz_localize(None)
        frames[sym] = s
    return pd.DataFrame(frames).sort_index().ffill()


def load_btc_closes(start: str, end: str) -> pd.Series:
    """BTC/USDT spot daily closes, tz-naive UTC date index (7 days/week)."""
    store = HistoricalOHLCVStore(allow_fetch=False)

    def ms(day: str) -> int:
        return int(pd.Timestamp(day).value // 1_000_000)

    df = store.get_ohlcv(BTC_SYMBOL, "spot", "1d", ms(start), ms(end))
    if df.empty:
        raise RuntimeError("no BTC spot data")
    s = df["close"].set_axis(pd.to_datetime(df["ts"], unit="ms").dt.normalize())
    return s.sort_index()


def load_qqq_closes(start: str, end: str) -> pd.Series:
    """QQQ daily closes (price-only, dividends not reinvested)."""
    store = NasdaqDailyStore(assetclass="etf")
    df = store.get_daily("QQQ", start, end)
    if df.empty:
        raise RuntimeError("no QQQ data")
    s = df["close"].copy()
    s.index = s.index.tz_localize(None)
    return s.sort_index()


def load_funding_daily_annualized(start: str, end: str) -> pd.Series:
    """Daily mean annualized funding (%) for the distribution analysis."""
    store = HistoricalOHLCVStore(allow_fetch=False)

    def ms(day: str) -> int:
        return int(pd.Timestamp(day).value // 1_000_000)

    sql = sa.text(
        "SELECT ts, rate FROM funding_rates "
        "WHERE symbol = :symbol AND ts >= :start AND ts < :end ORDER BY ts ASC"
    )
    with store._engine.connect() as conn:
        rows = conn.execute(
            sql, {"symbol": FUNDING_SYMBOL, "start": ms(start), "end": ms(end)}
        ).fetchall()
    if not rows:
        return pd.Series(dtype=float)
    frame = pd.DataFrame(rows, columns=["ts", "rate"])
    dates = pd.to_datetime(frame["ts"], unit="ms").dt.tz_localize(None).dt.normalize()
    ann = frame["rate"].astype(float) * 3 * 365 * 100
    return ann.groupby(dates).mean().sort_index()


# ---------------------------------------------------------------------------
# Signal caching (same pattern as the canonical script)
# ---------------------------------------------------------------------------


class _CachedSignal(Signal):
    """Serves precomputed SignalOutputs so every scenario reuses them."""

    def __init__(self, name: str, outputs: dict) -> None:
        self.name = name
        self.description = ""
        self._outputs = outputs

    def generate(self, as_of: pd.Timestamp) -> SignalOutput | None:
        return self._outputs.get(pd.Timestamp(as_of))

    def data_health(self) -> dict:
        return {"status": "ok", "cached": True}


def precompute_signals(signals: list[Signal], dates: pd.DatetimeIndex) -> dict[str, dict]:
    """{signal_name: {as_of: SignalOutput | None}} over the trading days."""
    cache: dict[str, dict] = {}
    for sig in signals:
        outputs: dict = {}
        for day in dates:
            try:
                outputs[day] = sig.generate(day)
            except Exception as exc:  # noqa: BLE001 — a broken signal must not kill the run
                print(f"  [warn] {sig.name} failed on {day.date()}: {exc}")
                outputs[day] = None
        cache[sig.name] = outputs
    return cache


# ---------------------------------------------------------------------------
# Portfolio simulation
# ---------------------------------------------------------------------------


def run_portfolio(
    cached: dict[str, dict],
    names: list[str],
    prices: pd.DataFrame,
    threshold: float,
    band: float,
    cost_model: IbkrCostModel,
    initial: float = INITIAL,
    universe_mode: str = "targets",
) -> dict:
    """Banded-rebalance portfolio over the union of fired signals' targets.

    Per trading day D:
      1. contributing signals = those with a cached output on D;
      2. combined score = equal-weight mean of contributing scores
         (SignalCombiner semantics — weights renormalize over contributors);
      3. target = score if |score| > threshold else 0.0 (cash);
      4. universe = union of contributing signals' metadata["targets"]
         (universe_mode="targets"), or the fixed 4-symbol basket when in
         market (universe_mode="fixed_basket", bridging to the canonical
         07e03d0 convention); restricted to symbols with price data on D
         (skipped symbols recorded); equal weight across survivors;
      5. rebalance when |target - net exposure| >= band OR the active
         universe changed since the last executed rebalance.
    """
    signals = [_CachedSignal(n, cached[n]) for n in names]
    combiner = SignalCombiner(signals, threshold=threshold)

    all_syms = list(prices.columns)
    first_valid = {s: prices[s].first_valid_index() for s in all_syms}

    cash = initial
    shares = {s: 0.0 for s in all_syms}
    last_target_universe: frozenset = frozenset()   # universe at last rebalance event
    equity_values: list[float] = []
    net_exposures: list[float] = []
    targets_hist: list[float] = []
    n_rebalances = 0
    turnover_notional = 0.0
    total_costs = 0.0
    skipped: dict[str, list[str]] = {}      # symbol -> [dates skipped]
    empty_universe_days = 0
    equity_min = initial
    equity_min_day = None
    first_nonpositive_day = None

    for day in prices.index:
        px = prices.loc[day]
        pos_value = sum(shares[s] * float(px[s]) for s in all_syms if not pd.isna(px[s]))
        equity_before = cash + pos_value
        net_frac_before = pos_value / equity_before if equity_before > 0 else 0.0

        out = combiner.combined_score(day)
        target = out.score if out.direction != "flat" else 0.0

        # Universe: fired signals' target union, or the fixed basket when
        # in market (canonical bridge mode). Restricted to listed names.
        universe: set[str] = set()
        if universe_mode == "fixed_basket":
            if target != 0.0:
                universe = set(BASKET)
        else:
            for n in names:
                o = cached[n].get(day)
                if o is not None:
                    universe.update(o.metadata.get("targets", []))
        active = {s for s in universe if s in first_valid and first_valid[s] is not None and first_valid[s] <= day}
        for s in sorted(universe - active):
            skipped.setdefault(s, []).append(day.date().isoformat())
        if universe and not active:
            empty_universe_days += 1

        held = {s for s in all_syms if abs(shares[s]) > 1e-9}
        universe_changed = frozenset(active) != last_target_universe
        if abs(target - net_frac_before) >= band or universe_changed:
            traded = False
            # Trade every name we hold OR want: names not in the active
            # universe (incl. a signal that went flat -> empty universe)
            # get unwound to zero.
            for s in sorted(active | held):
                price = float(px[s])
                if pd.isna(price) or price <= 0:
                    continue
                if s in active and target != 0.0:
                    per_symbol_notional = target * equity_before / len(active)
                    desired_q = per_symbol_notional / price
                else:
                    desired_q = 0.0
                delta = desired_q - shares[s]
                notional = abs(delta) * price
                if notional <= 1.0:  # dust
                    continue
                cost = cost_model.trade_cost(abs(delta), price)
                cash -= delta * price + cost
                shares[s] = desired_q
                turnover_notional += notional
                total_costs += cost
                traded = True
            if traded:
                n_rebalances += 1
            last_target_universe = frozenset(active)

        pos_value = sum(shares[s] * float(px[s]) for s in all_syms if not pd.isna(px[s]))
        equity = cash + pos_value
        equity_values.append(equity)
        if equity < equity_min:
            equity_min, equity_min_day = equity, day
        if first_nonpositive_day is None and equity <= 0:
            first_nonpositive_day = day
        net_exposures.append(pos_value / equity if equity > 0 else 0.0)
        targets_hist.append(target)

    equity = pd.Series(equity_values, index=prices.index, name="equity")
    ret = equity.pct_change().dropna()
    in_market = [(t, e) for t, e in zip(targets_hist[1:], ret) if abs(t) > 1e-9]
    return {
        "equity": equity,
        "metrics": metrics(equity, annual_days=252),
        "win_rate_daily_pct": round(float((ret > 0).mean() * 100), 2) if len(ret) else 0.0,
        "win_rate_in_market_pct": round(
            float(sum(1 for _, r in in_market if r > 0) / len(in_market) * 100), 2
        ) if in_market else 0.0,
        "avg_net_exposure": round(sum(net_exposures) / len(net_exposures), 4),
        "pct_days_in_market": round(
            sum(1 for t in targets_hist if abs(t) > 1e-9) / len(targets_hist), 4
        ),
        "n_rebalances": n_rebalances,
        "turnover_notional": round(turnover_notional, 2),
        "total_costs": round(total_costs, 2),
        "final_equity": round(float(equity.iloc[-1]), 2),
        "skipped_symbol_days": {s: {"days": len(d), "first": d[0], "last": d[-1]}
                                for s, d in sorted(skipped.items())},
        "empty_universe_days": empty_universe_days,
        "equity_min_pct_of_initial": round(equity_min / initial * 100, 2),
        "equity_min_day": equity_min_day.date().isoformat() if equity_min_day is not None else None,
        "first_nonpositive_equity_day": (
            first_nonpositive_day.date().isoformat() if first_nonpositive_day is not None else None
        ),
    }


def metrics(equity: pd.Series, annual_days: int) -> dict:
    ret = equity.pct_change().dropna()
    total_return = equity.iloc[-1] / equity.iloc[0] - 1.0
    n_days = len(equity)
    years = n_days / annual_days
    cagr = (equity.iloc[-1] / equity.iloc[0]) ** (1.0 / years) - 1.0 if years > 0 else 0.0
    sharpe = float(ret.mean() / ret.std() * math.sqrt(annual_days)) if ret.std() > 0 else 0.0
    drawdown = equity / equity.cummax() - 1.0
    return {
        "total_return_pct": round(total_return * 100, 2),
        "cagr_pct": round(cagr * 100, 2),
        "sharpe": round(sharpe, 3),
        "max_drawdown_pct": round(float(drawdown.min()) * 100, 2),
        "n_days": n_days,
    }


# ---------------------------------------------------------------------------
# Benchmarks
# ---------------------------------------------------------------------------


def basket_buy_hold(prices: pd.DataFrame, initial: float = INITIAL) -> dict:
    """Equal-weight B&H over symbols with data on the first trading day."""
    day0 = prices.index[0]
    syms = [s for s in prices.columns if not pd.isna(prices[s].iloc[0])]
    shares = {s: initial / len(syms) / float(prices.iloc[0][s]) for s in syms}
    equity = pd.Series(
        [sum(shares[s] * float(row[s]) for s in syms) for _, row in prices.iterrows()],
        index=prices.index,
    )
    return {"equity": equity, "metrics": metrics(equity, annual_days=252), "symbols": syms}


def series_buy_hold(s: pd.Series, annual_days: int, initial: float = INITIAL) -> dict:
    equity = s / float(s.iloc[0]) * initial
    return {"equity": equity, "metrics": metrics(equity, annual_days=annual_days)}


# ---------------------------------------------------------------------------
# Per-year decomposition
# ---------------------------------------------------------------------------


def yearly_metrics(equity: pd.Series, years: list[int]) -> dict[str, dict]:
    """Calendar-year return and intra-year max drawdown."""
    out: dict[str, dict] = {}
    for yr in years:
        seg = equity[equity.index.year == yr]
        if seg.empty:
            continue
        ret = seg.iloc[-1] / seg.iloc[0] - 1.0
        mdd = float((seg / seg.cummax() - 1.0).min())
        out[str(yr)] = {
            "return_pct": round(ret * 100, 2),
            "max_drawdown_pct": round(mdd * 100, 2),
            "n_days": len(seg),
        }
    return out


def yearly_trigger_stats(cached: dict[str, dict], years: list[int]) -> dict[str, dict]:
    """Per-signal per-year trigger counts (and funding regimes)."""
    out: dict[str, dict] = {}
    for name, outputs in cached.items():
        per_year: dict[int, dict] = {}
        for day, o in outputs.items():
            if o is None:
                continue
            yr = day.year
            if yr not in years:
                continue
            agg = per_year.setdefault(
                yr, {"days_fired": 0, "high_days": 0, "low_days": 0,
                     "new_extreme_episodes": 0, "long_days": 0, "short_days": 0}
            )
            agg["days_fired"] += 1
            if name == "funding_rate":
                regime = o.metadata.get("regime")
                if regime == "high":
                    agg["high_days"] += 1
                elif regime == "low":
                    agg["low_days"] += 1
                if o.metadata.get("is_new_extreme"):
                    agg["new_extreme_episodes"] += 1
            if o.direction == "long":
                agg["long_days"] += 1
            elif o.direction == "short":
                agg["short_days"] += 1
        out[name] = {str(yr): v for yr, v in sorted(per_year.items())}
    return out


# ---------------------------------------------------------------------------
# Funding distribution analysis (threshold transferability)
# ---------------------------------------------------------------------------


def funding_distribution(funding_daily: pd.Series, ma_window: int = 3) -> dict:
    """Percentiles and threshold-hit counts of the 3d funding MA per period.

    The signal fires on the 3-day MA of daily-mean annualized funding vs
    the +50 / -5 thresholds, so the MA distribution is what determines
    whether the calibrated thresholds transfer to another regime.
    """
    ma = funding_daily.rolling(ma_window).mean().dropna()

    def block(s: pd.Series) -> dict:
        if s.empty:
            return {}
        q = s.quantile([0.01, 0.05, 0.25, 0.50, 0.75, 0.95, 0.99])
        return {
            "n_days": int(len(s)),
            "p01": round(float(q.loc[0.01]), 2),
            "p05": round(float(q.loc[0.05]), 2),
            "p25": round(float(q.loc[0.25]), 2),
            "p50": round(float(q.loc[0.50]), 2),
            "p75": round(float(q.loc[0.75]), 2),
            "p95": round(float(q.loc[0.95]), 2),
            "p99": round(float(q.loc[0.99]), 2),
            "days_ma_above_50": int((s > 50).sum()),
            "days_ma_below_neg5": int((s < -5).sum()),
            "pct_ma_above_50": round(float((s > 50).mean() * 100), 3),
            "pct_ma_below_neg5": round(float((s < -5).mean() * 100), 3),
        }

    out: dict[str, dict] = {}
    for yr in sorted(ma.index.year.unique()):
        out[f"year_{yr}"] = block(ma[ma.index.year == yr])
    out["oos_2021_01_01__2023_03_02"] = block(ma[ma.index <= "2023-03-02"])
    out["training_2023_03_03__2026_09_14"] = block(ma[ma.index >= "2023-03-03"])
    return out


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def scenario_json(res: dict) -> dict:
    out = dict(res["metrics"])
    out.update({
        "win_rate_daily_pct": res["win_rate_daily_pct"],
        "win_rate_in_market_pct": res["win_rate_in_market_pct"],
        "avg_net_exposure": res["avg_net_exposure"],
        "pct_days_in_market": res["pct_days_in_market"],
        "n_rebalances": res["n_rebalances"],
        "turnover_notional": res["turnover_notional"],
        "total_costs_usd": res["total_costs"],
        "final_equity": res["final_equity"],
        "skipped_symbol_days": res["skipped_symbol_days"],
        "empty_universe_days": res["empty_universe_days"],
        "equity_min_pct_of_initial": res["equity_min_pct_of_initial"],
        "equity_min_day": res["equity_min_day"],
        "first_nonpositive_equity_day": res["first_nonpositive_equity_day"],
    })
    return out


def print_row(label: str, m: dict, extra: str = "") -> None:
    print(f"  {label:36s} 总收益 {m['total_return_pct']:+9.2f}% | "
          f"CAGR {m['cagr_pct']:+7.2f}% | Sharpe {m['sharpe']:7.3f} | "
          f"最大回撤 {m['max_drawdown_pct']:8.2f}%{extra}")


def run_window(
    label: str,
    start: str,
    end: str,
    cached: dict[str, dict],
    sig_names: list[str],
    cost_model: IbkrCostModel,
) -> dict:
    prices = load_stock_closes(start, end)
    btc = load_btc_closes(start, end)
    qqq = load_qqq_closes(start, end)
    dates = prices.index
    years = sorted(dates.year.unique())

    print("\n" + "=" * 92)
    print(f"  窗口: {label}  ({dates[0].date()} ~ {dates[-1].date()}, {len(dates)} 个交易日)")
    print("=" * 92)

    # --- signal fire summary over this window -----------------------------
    for name in sig_names:
        outs = {d: o for d, o in cached[name].items() if d in dates}
        fired = [o for o in outs.values() if o is not None]
        print(f"  {name:20s} 触发 {len(fired):4d}/{len(dates)} 天 "
              f"({len(fired)/len(dates)*100:4.1f}%)")

    # --- scenarios ---------------------------------------------------------
    scenarios: dict[str, dict] = {}
    print("\n  --- 策略（信号 targets 并集，等权；跳过无数据标的）---")
    scenarios["combined_2sig"] = run_portfolio(
        cached, sig_names, prices, THRESHOLD, BAND, cost_model)
    print_row("组合（weekend_gap + funding_rate）", scenarios["combined_2sig"]["metrics"])
    for name in sig_names:
        key = f"single_{name}"
        scenarios[key] = run_portfolio(cached, [name], prices, THRESHOLD, BAND, cost_model)
        print_row(f"单信号: {name}", scenarios[key]["metrics"])

    # --- benchmarks ----------------------------------------------------------
    print("\n  --- 基准 ---")
    bench_basket = basket_buy_hold(prices)
    print_row(f"等权篮子 B&H ({','.join(bench_basket['symbols'])})",
              bench_basket["metrics"], " (COIN 窗口初未上市则剔除)")
    bench_btc = series_buy_hold(btc, annual_days=365)
    print_row("BTC B&H", bench_btc["metrics"], " (365天年化)")
    bench_qqq = series_buy_hold(qqq, annual_days=252)
    print_row("QQQ B&H", bench_qqq["metrics"], " (价格未含股息)")

    # --- per-year decomposition ---------------------------------------------
    yearly: dict[str, dict] = {"years": [str(y) for y in years]}
    print(f"\n  --- 分年分解（当年收益% / 当年内最大回撤%）---")
    header = f"  {'年份':6s}"
    cols = [("组合", scenarios["combined_2sig"]),
            ("weekend_gap", scenarios["single_weekend_gap"]),
            ("funding_rate", scenarios["single_funding_rate"]),
            ("篮子B&H", bench_basket), ("BTC", bench_btc), ("QQQ", bench_qqq)]
    for cname, _ in cols:
        header += f" | {cname:>17s}"
    print(header)
    yearly["combined_2sig"] = yearly_metrics(scenarios["combined_2sig"]["equity"], years)
    yearly["single_weekend_gap"] = yearly_metrics(scenarios["single_weekend_gap"]["equity"], years)
    yearly["single_funding_rate"] = yearly_metrics(scenarios["single_funding_rate"]["equity"], years)
    yearly["bench_basket"] = yearly_metrics(bench_basket["equity"], years)
    yearly["bench_btc"] = yearly_metrics(bench_btc["equity"], years)
    yearly["bench_qqq"] = yearly_metrics(bench_qqq["equity"], years)
    for yr in years:
        row = f"  {yr:<6d}"
        for cname, _ in cols:
            ym = yearly[{"组合": "combined_2sig", "weekend_gap": "single_weekend_gap",
                        "funding_rate": "single_funding_rate", "篮子B&H": "bench_basket",
                        "BTC": "bench_btc", "QQQ": "bench_qqq"}[cname]].get(str(yr), {})
            row += f" | {ym.get('return_pct', float('nan')):+8.1f}/{ym.get('max_drawdown_pct', float('nan')):7.1f}"
        print(row)

    # --- per-year trigger counts --------------------------------------------
    trig = yearly_trigger_stats({n: {d: o for d, o in cached[n].items() if d in dates}
                                 for n in sig_names}, years)
    print("\n  --- 分年信号触发统计 ---")
    for name in sig_names:
        for yr in years:
            t = trig[name].get(str(yr))
            if not t:
                print(f"  {name:14s} {yr}: 0 次触发")
                continue
            if name == "funding_rate":
                print(f"  {name:14s} {yr}: 触发 {t['days_fired']:3d} 天 "
                      f"(high {t['high_days']}, low {t['low_days']}, "
                      f"新极值事件 {t['new_extreme_episodes']})")
            else:
                print(f"  {name:14s} {yr}: 触发 {t['days_fired']:3d} 天 "
                      f"(long {t['long_days']}, short {t['short_days']})")

    # --- skipped symbols ------------------------------------------------------
    skipped = {}
    for key in ("combined_2sig", "single_weekend_gap", "single_funding_rate"):
        for sym, info in scenarios[key]["skipped_symbol_days"].items():
            tag = skipped.setdefault(sym, {})
            tag[key] = info
    if skipped:
        print("\n  --- 跳过的无数据标的 ---")
        for sym, info in skipped.items():
            keys = list(info.values())[0]
            print(f"  {sym}: {info} (如 {keys['days']} 天, {keys['first']} ~ {keys['last']})")

    return {
        "range": [str(dates[0].date()), str(dates[-1].date())],
        "trading_days": len(dates),
        "scenarios": {k: scenario_json(v) for k, v in scenarios.items()},
        "benchmarks": {
            "basket_buy_hold": {**bench_basket["metrics"], "symbols": bench_basket["symbols"]},
            "btc_buy_hold": {**bench_btc["metrics"],
                             "note": "annualized on 365 calendar days; BTC trades 7d/wk"},
            "qqq_buy_hold": {**bench_qqq["metrics"],
                             "note": "price-only close series, dividends not reinvested"},
        },
        "yearly": yearly,
        "trigger_stats": trig,
        "skipped_symbols": skipped,
    }


def main() -> None:
    print("=" * 92)
    print("  双事件信号（weekend_gap + funding_rate）样本外回测")
    print(f"  阈值: {THRESHOLD} | 调仓带宽: {BAND} | IBKR 成本 | 初始资金 ${INITIAL:,.0f}")
    print(f"  训练窗参考 (07e03d0): 组合 +16.07% / Sharpe 0.423 / MDD -10.35% "
          f"(2023-04-01 ~ 2026-09-10)")
    print("=" * 92)

    # Precompute signal outputs ONCE on the full-window trading calendar;
    # each window's simulation only iterates its own price index.
    full_prices = load_stock_closes(FULL_START, FULL_END)
    dates = full_prices.index
    print(f"\n全窗口交易日: {len(dates)} 天 ({dates[0].date()} ~ {dates[-1].date()})")
    print("评估信号（每日，缓存复用）...")
    signals: list[Signal] = [WeekendGapSignal(), FundingRateSignal()]
    cached = precompute_signals(signals, dates)
    sig_names = [s.name for s in signals]

    cost_model = IbkrCostModel()

    oos = run_window("样本外 2021-01-01 ~ 2023-03-02", OOS_START, OOS_END,
                     cached, sig_names, cost_model)
    full = run_window("全窗口 2021-01-01 ~ 2026-09-11", FULL_START, FULL_END,
                      cached, sig_names, cost_model)

    # --- funding distribution (threshold transferability) --------------------
    funding_daily = load_funding_daily_annualized(FULL_START, "2026-09-15")
    funding_dist = funding_distribution(funding_daily)
    print("\n  --- funding 3日均线（年化%）分布：阈值可迁移性 ---")
    print(f"  {'期间':32s} {'天数':>5s} {'p01':>7s} {'p50':>7s} {'p99':>7s} "
          f"{'>50天数':>8s} {'<-5天数':>8s}")
    for period, b in funding_dist.items():
        print(f"  {period:32s} {b['n_days']:5d} {b['p01']:7.1f} {b['p50']:7.1f} "
              f"{b['p99']:7.1f} {b['days_ma_above_50']:8d} {b['days_ma_below_neg5']:8d}")

    # --- convention bridge: attribute decay to the window, not the rules ----
    # Rerun the combined portfolio on the TRAINING window under both universe
    # conventions, and the OOS window under the canonical fixed-basket one,
    # so the training->OOS decay can be compared like-for-like.
    print("\n" + "=" * 92)
    print("  约定桥接：训练窗复跑（两种 universe 约定）+ 样本外固定篮子变体")
    print("=" * 92)
    tr_prices = load_stock_closes("2023-04-01", "2026-09-10")
    tr_dates = tr_prices.index
    tr_cached = {n: {d: o for d, o in cached[n].items() if d in tr_dates} for n in sig_names}
    bridge: dict[str, dict] = {"canonical_reference_07e03d0": TRAINING_REF["combined_2sig"]}

    tr_t = run_portfolio(tr_cached, sig_names, tr_prices, THRESHOLD, BAND, cost_model)
    bridge["training_targets_union"] = scenario_json(tr_t)
    print_row("训练窗 + targets并集约定", tr_t["metrics"])

    tr_f = run_portfolio(tr_cached, sig_names, tr_prices, THRESHOLD, BAND, cost_model,
                         universe_mode="fixed_basket")
    bridge["training_fixed_basket"] = scenario_json(tr_f)
    print_row("训练窗 + 固定篮子约定", tr_f["metrics"])

    oos_prices = load_stock_closes(OOS_START, OOS_END)
    oos_dates = oos_prices.index
    oos_cached = {n: {d: o for d, o in cached[n].items() if d in oos_dates} for n in sig_names}
    oos_f = run_portfolio(oos_cached, sig_names, oos_prices, THRESHOLD, BAND, cost_model,
                          universe_mode="fixed_basket")
    bridge["oos_fixed_basket"] = scenario_json(oos_f)
    print_row("样本外 + 固定篮子约定", oos_f["metrics"])

    # --- JSON output -----------------------------------------------------------
    report = {
        "config": {
            "signals": sig_names,
            "threshold": THRESHOLD,
            "rebalance_band": BAND,
            "initial_capital": INITIAL,
            "cost_model": {
                "commission_per_share": cost_model.commission_per_share,
                "min_commission_per_order": cost_model.min_commission_per_order,
                "spread_bps": cost_model.spread_bps,
            },
            "training_reference": TRAINING_REF,
        },
        "windows": {
            "out_of_sample": oos,
            "full": full,
        },
        "funding_distribution_3d_ma": funding_dist,
        "convention_bridge": bridge,
        "conventions": {
            "look_ahead": "signals evaluated with data strictly before as_of; trades at as_of close",
            "sizing": "target exposure = combined score (signed fraction of equity, cap +/-1)",
            "flat_rule": f"|combined score| <= {THRESHOLD} -> cash",
            "rebalance_rule": f"trade when |target - net exposure| >= {BAND}, or when the active universe changes",
            "universe": "union of fired signals' metadata targets, equal weight; symbols without data on the day are skipped and counted (COIN IPO 2021-04-14)",
            "difference_vs_canonical": "canonical 07e03d0 used a fixed COIN/MSTR/MARA/RIOT basket for sizing; this script sizes only the fired signals' target union",
            "win_rate": "win_rate_daily_pct = % of trading days with positive daily equity return; win_rate_in_market_pct = same, restricted to days with nonzero target",
            "shorting": "allowed; borrow costs not modeled",
            "costs": "IBKR Pro tiered: $0.005/share min $1/order + 5bps half-spread per trade",
            "benchmarks": "basket B&H over symbols with data at window start; QQQ price-only (no dividends)",
        },
    }

    out_path = Path("reports/outofsample_backtest.json")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n")
    print(f"\nJSON 已写入: {out_path}")


if __name__ == "__main__":
    main()
