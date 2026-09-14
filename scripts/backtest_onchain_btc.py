#!/usr/bin/env python3
"""OnchainFundamentalSignal backtest on BTC directly (not crypto stocks).

The signal is a BTC mean-reversion fundamental signal (MVRV valuation /
price-vs-usage divergence / exchange net flows). It previously executed on
a crypto-proxy stock basket and lost -57.6% (2023-2026 bull market, short
the basket for 111 consecutive days). Here the same signal trades BTC
itself via its native instrument, extending the test window to the full
on-chain history (2019-07-01 -> 2026-09-13, ~7.2 years — the stock basket
only has 3.5 years).

Conventions (no look-ahead, mirrors scripts/backtest_combined_signals.py):
  - Signal is evaluated with as_of = bar date D, i.e. on on-chain data
    STRICTLY BEFORE D (store queries are ts < as_of); trades execute at
    D's close.
  - Target exposure = signal score (signed fraction of equity, capped
    +/-1); |score| <= threshold -> cash.
  - Rebalance only when |target - current exposure| >= band; every trade
    pays fee_bps on traded notional (turnover).
  - Shorting is allowed (score < 0). BTC trades 7 days/week; annualized
    on 365 days.

Price series: BTC/USDT spot 1d closes from data/btc_history.db
(HistoricalOHLCVStore). Spot stands in for the perp execution target
(BTC/USDT:USDT): daily closes differ only by basis, and funding flows are
NOT modeled (per spec, costs are 5bps on turnover only — conservative for
a signal that is net-short in high-funding bull regimes, generous in
low/negative-funding regimes).

Note: pre-2023-03 spot bars were backfilled into data/btc_history.db from
data-api.binance.vision (Binance public market-data mirror).

Usage:
    poetry run python scripts/backtest_onchain_btc.py
    poetry run python scripts/backtest_onchain_btc.py --threshold 0.0 --band 0.0
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
from src.signals import OnchainFundamentalSignal

BTC_SYMBOL = "BTC/USDT"
DEFAULT_START = "2019-07-01"   # after 180d on-chain warmup (data from 2019-01-01)
DEFAULT_END = "2026-09-13"     # last full on-chain daily metric date
ANNUAL_DAYS = 365              # BTC trades 7d/wk


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="OnchainFundamentalSignal -> BTC backtest")
    p.add_argument("--start", default=DEFAULT_START)
    p.add_argument("--end", default=DEFAULT_END)
    p.add_argument("--threshold", type=float, default=0.15,
                   help="|score| below this = flat/cash [default 0.15]")
    p.add_argument("--band", type=float, default=0.25,
                   help="rebalance when |target-current exposure| >= band [default 0.25]")
    p.add_argument("--fee-bps", type=float, default=5.0,
                   help="cost per unit of turnover, in bps [default 5]")
    p.add_argument("--initial", type=float, default=100_000.0)
    p.add_argument("--out", default="reports/onchain_btc_backtest.json")
    return p.parse_args()


def load_btc_closes(start: str, end: str) -> pd.Series:
    """BTC/USDT spot 1d closes, tz-naive UTC date index (inclusive of end)."""
    store = HistoricalOHLCVStore(allow_fetch=False)

    def ms(day: str) -> int:
        return int(pd.Timestamp(day).value // 1_000_000)

    # end + 1 day so the bar dated `end` is included (ts is bar open).
    df = store.get_ohlcv(BTC_SYMBOL, "spot", "1d", ms(start), ms(end) + 86_400_000)
    if df.empty:
        raise RuntimeError("no BTC spot 1d data — run the data refresh first")
    s = df["close"].set_axis(pd.to_datetime(df["ts"], unit="ms").dt.normalize())
    s = s.sort_index()
    s = s[s.index <= pd.Timestamp(end)]
    # continuity check (BTC trades daily; gaps would silently skew returns)
    gaps = s.index.to_series().diff().dt.days.dropna()
    if len(gaps) and gaps.max() > 1:
        print(f"  [warn] BTC series has gaps up to {int(gaps.max())} days")
    return s


def metrics_from_equity(equity: pd.Series) -> dict:
    ret = equity.pct_change().dropna()
    n_days = len(equity)
    years = n_days / ANNUAL_DAYS
    cagr = (equity.iloc[-1] / equity.iloc[0]) ** (1.0 / years) - 1.0 if years > 0 else 0.0
    sharpe = float(ret.mean() / ret.std() * math.sqrt(ANNUAL_DAYS)) if ret.std() > 0 else 0.0
    drawdown = equity / equity.cummax() - 1.0
    return {
        "total_return_pct": round(equity.iloc[-1] / equity.iloc[0] * 100 - 100, 2),
        "cagr_pct": round(cagr * 100, 2),
        "sharpe": round(sharpe, 3),
        "max_drawdown_pct": round(float(drawdown.min()) * 100, 2),
        "n_days": n_days,
    }


def run_backtest(
    closes: pd.Series,
    threshold: float,
    band: float,
    fee_bps: float,
    initial: float,
) -> dict:
    """Daily-loop simulation: signal(D) -> position at D's close -> D+1's return."""
    sig = OnchainFundamentalSignal()
    fee = fee_bps / 10_000.0

    equity = initial
    exposure = 0.0
    n_rebalances = 0
    turnover = 0.0
    total_costs = 0.0
    n_days_fired = 0
    rows: list[dict] = []

    closes_arr = closes.to_numpy()
    dates = closes.index

    for i, day in enumerate(dates):
        # 1. mark-to-market: exposure set at the previous close earns today's return
        if i > 0:
            r = closes_arr[i] / closes_arr[i - 1] - 1.0
            equity *= 1.0 + exposure * r

        # 2. signal on data strictly before `day`; trade at `day`'s close
        out = sig.generate(day)
        if out is None:
            target = 0.0
        else:
            n_days_fired += 1
            target = out.score if out.direction != "flat" else 0.0
            if abs(target) <= threshold:
                target = 0.0

        if abs(target - exposure) >= band or (band <= 0.0 and target != exposure):
            traded = abs(target - exposure)
            cost = traded * equity * fee
            equity -= cost
            turnover += traded
            total_costs += cost
            n_rebalances += 1
            exposure = target

        rows.append({
            "date": day,
            "close": closes_arr[i],
            "exposure": exposure,
            "equity": equity,
            "score": None if out is None else out.score,
        })

    df = pd.DataFrame(rows).set_index("date")
    equity_s = df["equity"]
    exposure_s = df["exposure"]

    long_days = int((exposure_s > 1e-9).sum())
    short_days = int((exposure_s < -1e-9).sum())
    flat_days = int((exposure_s.abs() <= 1e-9).sum())

    return {
        "equity": equity_s,
        "exposure": exposure_s,
        "scores": df["score"],
        "metrics": metrics_from_equity(equity_s),
        "avg_exposure": round(float(exposure_s.mean()), 3),
        "avg_abs_exposure": round(float(exposure_s.abs().mean()), 3),
        "pct_days_in_market": round(1.0 - flat_days / len(exposure_s), 4),
        "days_long": long_days,
        "days_short": short_days,
        "days_flat": flat_days,
        "n_days_fired": n_days_fired,
        "n_rebalances": n_rebalances,
        "turnover": round(turnover, 2),
        "total_costs_usd": round(total_costs, 2),
        "final_equity": round(float(equity_s.iloc[-1]), 2),
    }


def yearly_table(res: dict, closes: pd.Series) -> list[dict]:
    """Per-calendar-year strategy vs BTC buy-and-hold."""
    years = []
    for year, eq in res["equity"].groupby(res["equity"].index.year):
        px = closes[closes.index.year == year]
        strat = metrics_from_equity(eq)
        bh_equity = px / float(px.iloc[0]) * 100_000.0
        bh = metrics_from_equity(bh_equity)
        expo = res["exposure"][res["exposure"].index.year == year]
        years.append({
            "year": int(year),
            "strategy_return_pct": strat["total_return_pct"],
            "strategy_sharpe": strat["sharpe"],
            "strategy_mdd_pct": strat["max_drawdown_pct"],
            "btc_return_pct": bh["total_return_pct"],
            "excess_pct": round(strat["total_return_pct"] - bh["total_return_pct"], 2),
            "avg_exposure": round(float(expo.mean()), 3),
            "days_long": int((expo > 1e-9).sum()),
            "days_short": int((expo < -1e-9).sum()),
            "n_days": len(eq),
        })
    return years


def main() -> None:
    args = parse_args()

    print("=" * 78)
    print("  OnchainFundamentalSignal -> BTC 直接交易回测")
    print(f"  标的: BTC/USDT (spot 1d 收盘价, 永续的代理) | 窗口: {args.start} ~ {args.end}")
    print(f"  阈值: {args.threshold} | 调仓带宽: {args.band} | 成本: {args.fee_bps}bps × 换手")
    print("=" * 78)

    closes = load_btc_closes(args.start, args.end)
    print(f"\n交易日: {len(closes)} 天 ({closes.index[0].date()} ~ {closes.index[-1].date()})")

    res = run_backtest(closes, args.threshold, args.band, args.fee_bps, args.initial)
    m = res["metrics"]
    bh_equity = closes / float(closes.iloc[0]) * args.initial
    bh = metrics_from_equity(bh_equity)

    print(f"\n--- 全期 ({closes.index[0].date()} ~ {closes.index[-1].date()}, "
          f"{m['n_days']} 天, 365天年化) ---")
    print(f"  {'信号→BTC':24s} 总收益 {m['total_return_pct']:+9.2f}% | "
          f"CAGR {m['cagr_pct']:+7.2f}% | Sharpe {m['sharpe']:6.3f} | "
          f"最大回撤 {m['max_drawdown_pct']:8.2f}%")
    print(f"  {'BTC 买入持有':24s} 总收益 {bh['total_return_pct']:+9.2f}% | "
          f"CAGR {bh['cagr_pct']:+7.2f}% | Sharpe {bh['sharpe']:6.3f} | "
          f"最大回撤 {bh['max_drawdown_pct']:8.2f}%")
    print(f"\n  平均敞口 {res['avg_exposure']:+.3f} (|敞口| {res['avg_abs_exposure']:.3f}) | "
          f"在场 {res['pct_days_in_market']*100:.1f}% | "
          f"多 {res['days_long']} / 空 {res['days_short']} / 平 {res['days_flat']} 天 | "
          f"调仓 {res['n_rebalances']} 次 | 换手 {res['turnover']:.1f}x | "
          f"成本 ${res['total_costs_usd']:,.0f}")

    years = yearly_table(res, closes)
    print("\n--- 分年表现 ---")
    print(f"  {'年份':6s}{'信号%':>10s}{'BTC%':>10s}{'超额%':>9s}"
          f"{'Sharpe':>8s}{'MDD%':>9s}{'敞口':>8s}{'多/空天数':>12s}")
    for y in years:
        print(f"  {y['year']:<6d}{y['strategy_return_pct']:>+10.2f}"
              f"{y['btc_return_pct']:>+10.2f}{y['excess_pct']:>+9.2f}"
              f"{y['strategy_sharpe']:>8.2f}{y['strategy_mdd_pct']:>9.2f}"
              f"{y['avg_exposure']:>+8.3f}"
              f"{y['days_long']:>7d}/{y['days_short']:<4d}")

    scores = res["scores"].dropna()
    report = {
        "config": {
            "signal": "onchain_fundamental",
            "targets": ["BTC/USDT:USDT"],
            "instrument": "perp (proxied by BTC/USDT spot 1d closes)",
            "start": args.start,
            "end": args.end,
            "threshold": args.threshold,
            "rebalance_band": args.band,
            "fee_bps_on_turnover": args.fee_bps,
            "initial_capital": args.initial,
            "annualization_days": ANNUAL_DAYS,
        },
        "overall": {
            "strategy": {
                **res["metrics"],
                "avg_exposure": res["avg_exposure"],
                "avg_abs_exposure": res["avg_abs_exposure"],
                "pct_days_in_market": res["pct_days_in_market"],
                "days_long": res["days_long"],
                "days_short": res["days_short"],
                "days_flat": res["days_flat"],
                "n_days_fired": res["n_days_fired"],
                "n_rebalances": res["n_rebalances"],
                "turnover": res["turnover"],
                "total_costs_usd": res["total_costs_usd"],
                "final_equity": res["final_equity"],
            },
            "btc_buy_hold": bh,
        },
        "yearly": years,
        "signal_stats": {
            "days_fired": int(len(scores)),
            "avg_score": round(float(scores.mean()), 4) if len(scores) else None,
            "avg_abs_score": round(float(scores.abs().mean()), 4) if len(scores) else None,
            "pct_days_short_score": round(
                float((scores < 0).mean()), 4) if len(scores) else None,
        },
        "conventions": {
            "look_ahead": "signal as_of = bar date D uses on-chain data strictly before D; trades at D's close",
            "sizing": "target exposure = signal score (signed fraction of equity, cap +/-1)",
            "flat_rule": f"|score| <= {args.threshold} -> cash",
            "rebalance_rule": f"trade when |target - exposure| >= {args.band}",
            "shorting": "allowed (perp); borrow/funding costs NOT modeled",
            "costs": f"{args.fee_bps} bps on traded notional (turnover)",
            "price_note": "spot daily closes stand in for perp; basis and funding not modeled",
            "data_note": "BTC spot 1d pre-2023-03 backfilled from data-api.binance.vision; on-chain metrics from CoinMetrics (2019-01-01+)",
        },
    }

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n")
    print(f"\nJSON 已写入: {out_path}")


if __name__ == "__main__":
    main()
