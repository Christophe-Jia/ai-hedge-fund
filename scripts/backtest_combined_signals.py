#!/usr/bin/env python3
"""Combined-signal backtest: 3-signal portfolio vs single signals vs benchmarks.

Compares, on the crypto-proxy stock basket (COIN/MSTR/MARA/RIOT):
  - the combined 3-signal portfolio (SignalCombiner, equal weights)
  - each signal alone
  - each leave-one-out pair (ablation)
  - buy-and-hold benchmarks (equal-weight basket, BTC)

Conventions (no look-ahead):
  - Signals are evaluated with as_of = trading day D, i.e. on data
    STRICTLY BEFORE D's close; trades execute at D's close.
  - Target exposure = combined score (signed fraction of equity, capped
    at +/-1); flat (|score| <= threshold) means go to cash.
  - Rebalance only when |target - current net exposure| >= band (limits
    churn); every trade pays IBKR costs (commission + half-spread).
  - Shorting is allowed (score < 0); borrow costs are NOT modeled.

Signal outputs are computed once per (signal, day) and cached — all
scenarios share the exact same per-signal scores.

Usage:
    poetry run python scripts/backtest_combined_signals.py
    poetry run python scripts/backtest_combined_signals.py --threshold 0.2 --band 0.25
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd

from src.data.historical_store import HistoricalOHLCVStore
from src.data.nasdaq_store import NasdaqDailyStore
from src.selection.costs import IbkrCostModel
from src.signals import (
    FundingRateSignal,
    OnchainFundamentalSignal,
    SignalCombiner,
    SignalOutput,
    Signal,
    WeekendGapSignal,
)

BASKET = ("COIN", "MSTR", "MARA", "RIOT")
BTC_SYMBOL = "BTC/USDT"
DEFAULT_START = "2023-04-01"   # BTC spot + funding data begin 2023-03
DEFAULT_END = "2026-09-10"     # last stock trading day in the store


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


# ---------------------------------------------------------------------------
# Signal caching
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
    initial: float = 100_000.0,
) -> dict:
    """Daily-rebalance (banded) portfolio driven by the combined signal."""
    signals = [_CachedSignal(n, cached[n]) for n in names]
    combiner = SignalCombiner(signals, threshold=threshold)

    cash = initial
    shares = {sym: 0.0 for sym in BASKET}
    equity_values: list[float] = []
    net_exposures: list[float] = []
    targets: list[float] = []
    n_rebalances = 0
    turnover_notional = 0.0
    total_costs = 0.0

    for day in prices.index:
        px = prices.loc[day]
        equity_before = cash + sum(shares[s] * float(px[s]) for s in BASKET)
        net_frac_before = (
            sum(shares[s] * float(px[s]) for s in BASKET) / equity_before
            if equity_before > 0
            else 0.0
        )

        out = combiner.combined_score(day)
        target = out.score if out.direction != "flat" else 0.0

        if abs(target - net_frac_before) >= band:
            n_rebalances += 1
            per_symbol_notional = target * equity_before / len(BASKET)
            for s in BASKET:
                price = float(px[s])
                if price <= 0:
                    continue
                desired_q = per_symbol_notional / price
                delta = desired_q - shares[s]
                notional = abs(delta) * price
                if notional <= 1.0:  # dust
                    continue
                cost = cost_model.trade_cost(abs(delta), price)
                cash -= delta * price + cost
                shares[s] = desired_q
                turnover_notional += notional
                total_costs += cost

        px = prices.loc[day]
        equity = cash + sum(shares[s] * float(px[s]) for s in BASKET)
        equity_values.append(equity)
        net_exposures.append(
            sum(shares[s] * float(px[s]) for s in BASKET) / equity if equity > 0 else 0.0
        )
        targets.append(target)

    equity = pd.Series(equity_values, index=prices.index, name="equity")
    return {
        "equity": equity,
        "metrics": _metrics(equity, annual_days=252),
        "avg_net_exposure": sum(net_exposures) / len(net_exposures),
        "pct_days_in_market": sum(1 for t in targets if abs(t) > 1e-9) / len(targets),
        "n_rebalances": n_rebalances,
        "turnover_notional": turnover_notional,
        "total_costs": total_costs,
        "final_equity": float(equity.iloc[-1]),
    }


def _metrics(equity: pd.Series, annual_days: int) -> dict:
    ret = equity.pct_change().dropna()
    total_return = equity.iloc[-1] / equity.iloc[0] - 1.0
    n_days = len(equity)
    years = n_days / annual_days
    cagr = (equity.iloc[-1] / equity.iloc[0]) ** (1.0 / years) - 1.0 if years > 0 else 0.0
    sharpe = (
        float(ret.mean() / ret.std() * math.sqrt(annual_days)) if ret.std() > 0 else 0.0
    )
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


def basket_buy_hold(prices: pd.DataFrame, initial: float = 100_000.0) -> dict:
    shares = {s: initial / len(BASKET) / float(prices.iloc[0][s]) for s in BASKET}
    equity = pd.Series(
        [sum(shares[s] * float(row[s]) for s in BASKET) for _, row in prices.iterrows()],
        index=prices.index,
    )
    return {"equity": equity, "metrics": _metrics(equity, annual_days=252)}


def btc_buy_hold(btc: pd.Series, initial: float = 100_000.0) -> dict:
    equity = btc / float(btc.iloc[0]) * initial
    return {"equity": equity, "metrics": _metrics(equity, annual_days=365)}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Combined-signal backtest")
    p.add_argument("--start", default=DEFAULT_START)
    p.add_argument("--end", default=DEFAULT_END)
    p.add_argument("--threshold", type=float, default=0.15,
                   help="|combined score| below this = flat/cash [default 0.15]")
    p.add_argument("--band", type=float, default=0.25,
                   help="rebalance when |target-current exposure| >= band [default 0.25]")
    p.add_argument("--initial", type=float, default=100_000.0)
    p.add_argument("--out", default="reports/combined_signals.json",
                   help="output JSON path [default reports/combined_signals.json]")
    return p.parse_args()


def main() -> None:
    args = parse_args()

    print("=" * 78)
    print("  组合信号回测：三信号组合 vs 单信号 vs 消融 vs 基准")
    print(f"  标的: {', '.join(BASKET)} | 窗口: {args.start} ~ {args.end}")
    print(f"  阈值: {args.threshold} | 调仓带宽: {args.band} | IBKR 成本模型")
    print("=" * 78)

    prices = load_stock_closes(args.start, args.end)
    btc = load_btc_closes(args.start, args.end)
    dates = prices.index
    print(f"\n交易日: {len(dates)} 天 ({dates[0].date()} ~ {dates[-1].date()})")

    # --- precompute every signal once on every trading day ------------------
    signals: list[Signal] = [
        WeekendGapSignal(),
        FundingRateSignal(),
        OnchainFundamentalSignal(),
    ]
    print("评估信号（每日，缓存复用）...")
    cached = precompute_signals(signals, dates)
    sig_names = [s.name for s in signals]
    for name in sig_names:
        outs = cached[name]
        fired = [o for o in outs.values() if o is not None]
        avg_abs = sum(abs(o.score) for o in fired) / len(fired) if fired else 0.0
        print(f"  {name:20s} 触发 {len(fired):4d}/{len(dates)} 天 "
              f"({len(fired)/len(dates)*100:4.1f}%) | 平均|score| {avg_abs:.3f}")

    # --- correlation matrix (pairwise-complete over trading days) -----------
    scores = pd.DataFrame(
        {
            name: {day: (o.score if o is not None else None) for day, o in outs.items()}
            for name, outs in cached.items()
        }
    )
    corr = scores.corr()

    # --- scenarios ----------------------------------------------------------
    cost_model = IbkrCostModel()
    scenarios: dict[str, dict] = {}

    def run(names: list[str], label: str) -> dict:
        res = run_portfolio(cached, names, prices, args.threshold, args.band,
                            cost_model, args.initial)
        m = res["metrics"]
        print(f"  {label:34s} 总收益 {m['total_return_pct']:+8.2f}% | "
              f"CAGR {m['cagr_pct']:+6.2f}% | Sharpe {m['sharpe']:6.3f} | "
              f"最大回撤 {m['max_drawdown_pct']:7.2f}% | "
              f"调仓 {res['n_rebalances']:3d} 次 | 成本 ${res['total_costs']:,.0f}")
        return res

    print("\n--- 组合 vs 单信号 ---")
    scenarios["combined_all3"] = run(sig_names, "组合（全部三信号）")
    for name in sig_names:
        scenarios[f"single_{name}"] = run([name], f"单信号: {name}")

    print("\n--- 消融实验（去掉一个信号）---")
    for dropped in sig_names:
        remaining = [n for n in sig_names if n != dropped]
        scenarios[f"ablation_without_{dropped}"] = run(
            remaining, f"去掉 {dropped}（{'+'.join(remaining)}）"
        )

    print("\n--- 基准 ---")
    bench_basket = basket_buy_hold(prices, args.initial)
    m = bench_basket["metrics"]
    print(f"  {'等权篮子买入持有':34s} 总收益 {m['total_return_pct']:+8.2f}% | "
          f"CAGR {m['cagr_pct']:+6.2f}% | Sharpe {m['sharpe']:6.3f} | "
          f"最大回撤 {m['max_drawdown_pct']:7.2f}%")
    bench_btc = btc_buy_hold(btc, args.initial)
    m = bench_btc["metrics"]
    print(f"  {'BTC 买入持有':34s} 总收益 {m['total_return_pct']:+8.2f}% | "
          f"CAGR {m['cagr_pct']:+6.2f}% | Sharpe {m['sharpe']:6.3f} | "
          f"最大回撤 {m['max_drawdown_pct']:7.2f}% (365天年化)")

    # --- correlation matrix --------------------------------------------------
    print("\n--- 信号相关性矩阵（交易日, pairwise-complete）---")
    print(corr.round(3).to_string())

    # --- JSON output ----------------------------------------------------------
    def scenario_json(res: dict) -> dict:
        out = dict(res["metrics"])
        out.update({
            "avg_net_exposure": round(res["avg_net_exposure"], 3),
            "pct_days_in_market": round(res["pct_days_in_market"], 3),
            "n_rebalances": res["n_rebalances"],
            "turnover_notional": round(res["turnover_notional"], 2),
            "total_costs_usd": round(res["total_costs"], 2),
            "final_equity": round(res["final_equity"], 2),
        })
        return out

    report = {
        "config": {
            "basket": list(BASKET),
            "start": args.start,
            "end": args.end,
            "trading_days": len(dates),
            "threshold": args.threshold,
            "rebalance_band": args.band,
            "initial_capital": args.initial,
            "cost_model": {
                "commission_per_share": cost_model.commission_per_share,
                "min_commission_per_order": cost_model.min_commission_per_order,
                "spread_bps": cost_model.spread_bps,
            },
        },
        "scenarios": {k: scenario_json(v) for k, v in scenarios.items()},
        "benchmarks": {
            "basket_buy_hold": bench_basket["metrics"],
            "btc_buy_hold": {**bench_btc["metrics"],
                             "note": "annualized on 365 calendar days; BTC trades 7d/wk"},
        },
        "signal_stats": {
            name: {
                "days_fired": sum(1 for o in cached[name].values() if o is not None),
                "pct_days_fired": round(
                    sum(1 for o in cached[name].values() if o is not None) / len(dates), 4
                ),
                "avg_abs_score_when_fired": round(
                    sum(abs(o.score) for o in cached[name].values() if o is not None)
                    / max(1, sum(1 for o in cached[name].values() if o is not None)),
                    4,
                ),
            }
            for name in sig_names
        },
        "correlation_matrix": {
            row: {col: (None if pd.isna(corr.loc[row, col]) else round(float(corr.loc[row, col]), 4))
                  for col in corr.columns}
            for row in corr.index
        },
        "conventions": {
            "look_ahead": "signals evaluated with data strictly before as_of; trades at as_of close",
            "sizing": "target exposure = combined score (signed fraction of equity, cap +/-1)",
            "flat_rule": f"|combined score| <= {args.threshold} -> cash",
            "rebalance_rule": f"trade when |target - net exposure| >= {args.band}",
            "shorting": "allowed; borrow costs not modeled",
            "costs": "IBKR Pro tiered: $0.005/share min $1/order + 5bps half-spread per trade",
        },
    }

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n")
    print(f"\nJSON 已写入: {out_path}")


if __name__ == "__main__":
    main()
