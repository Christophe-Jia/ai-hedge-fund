#!/usr/bin/env python3
"""Pure DCA baseline backtest for crypto spot (BTC/ETH/SOL default).

Buys a fixed USDT amount on a schedule (monthly or weekly), split equally
across symbols, at daily close prices, paying realistic taker fees via the
repo's Binance CostModel. Reports MOIC, XIRR, max drawdown, and a lump-sum
comparison — the "do nothing smart" baseline every strategy must beat.

Data: data/btc_history.db (spot daily OHLCV, backfilled via
scripts/backfill_perp_ohlcv.py --market spot).

Usage:
    poetry run python scripts/run_dca_baseline.py
    poetry run python scripts/run_dca_baseline.py --monthly 5000 --symbols BTC,ETH
    poetry run python scripts/run_dca_baseline.py --freq weekly --monthly 1750
    poetry run python scripts/run_dca_baseline.py --no-costs   # frictionless
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd

from src.backtesting.cost_model import CostModel, VipTier
from src.data.historical_store import HistoricalOHLCVStore

DEFAULT_DB = Path(__file__).resolve().parents[1] / "data" / "btc_history.db"
OUT_DIR = Path(__file__).resolve().parents[1] / "outputs" / "dca_baseline"


# ---------------------------------------------------------------------------
# XIRR (money-weighted annualized return), Newton with bisection fallback
# ---------------------------------------------------------------------------

def xirr(cashflows: list[tuple[pd.Timestamp, float]], guess: float = 0.10) -> float | None:
    """cashflows: [(date, amount)], negative = invested, final value positive."""
    if len(cashflows) < 2:
        return None
    t0 = cashflows[0][0]

    def npv(rate: float) -> float:
        return sum(
            amt / ((1.0 + rate) ** ((ts - t0).days / 365.25)) for ts, amt in cashflows
        )

    lo, hi = -0.9999, 10.0
    f_lo, f_hi = npv(lo), npv(hi)
    if f_lo * f_hi > 0:
        return None
    for _ in range(200):
        mid = (lo + hi) / 2
        f_mid = npv(mid)
        if abs(f_mid) < 1e-7:
            return mid
        if f_lo * f_mid < 0:
            hi, f_hi = mid, f_mid
        else:
            lo, f_lo = mid, f_mid
    return (lo + hi) / 2


# ---------------------------------------------------------------------------
# DCA simulation
# ---------------------------------------------------------------------------

def load_prices(store: HistoricalOHLCVStore, symbol: str, start: str, end: str) -> pd.Series:
    start_ts = int(datetime.fromisoformat(start).replace(tzinfo=timezone.utc).timestamp() * 1000)
    end_ts = int(datetime.fromisoformat(end).replace(tzinfo=timezone.utc).timestamp() * 1000)
    df = store.get_ohlcv(symbol, "spot", "1d", start_ts, end_ts)
    if df.empty:
        raise SystemExit(f"No spot daily data for {symbol} in [{start}, {end}] — run backfill first")
    s = df.set_index(pd.to_datetime(df["ts"], unit="ms", utc=True))["close"].astype(float)
    return s[~s.index.duplicated(keep="last")].sort_index()


def build_schedule(prices: pd.Series, freq: str, contribution_day: int) -> list[pd.Timestamp]:
    """First available trading day on/after each target contribution date."""
    if freq == "weekly":
        targets = pd.date_range(prices.index[0], prices.index[-1], freq="W-MON")
    else:
        targets = pd.date_range(prices.index[0], prices.index[-1], freq="MS") + pd.Timedelta(days=contribution_day - 1)
    schedule: list[pd.Timestamp] = []
    for t in targets:
        valid = prices.index[prices.index >= t]
        if len(valid):
            schedule.append(valid[0])
    return schedule


def run_dca(
    prices: dict[str, pd.Series],
    per_purchase_usdt: float,
    freq: str,
    contribution_day: int,
    cost_model: CostModel | None,
) -> dict:
    all_idx = sorted(set().union(*[s.index for s in prices.values()]))
    price_df = pd.DataFrame({sym: s.reindex(all_idx) for sym, s in prices.items()})
    price_df = price_df.ffill().dropna(how="all")

    schedule = build_schedule(price_df["close" if "close" in price_df else price_df.columns[0]] if False else price_df.iloc[:, 0], freq, contribution_day)
    schedule_set = set(schedule)

    syms = list(prices.keys())
    qty = {s: 0.0 for s in syms}
    invested = {s: 0.0 for s in syms}
    total_invested = 0.0
    total_costs = 0.0

    rows = []
    for ts, row in price_df.iterrows():
        if ts in schedule_set:
            budget = per_purchase_usdt / len(syms)
            for s in syms:
                p = row[s]
                if pd.isna(p) or p <= 0:
                    continue  # symbol not listed yet — skip its share this round
                cost = cost_model.compute_trade_cost(budget, "spot") if cost_model else 0.0
                net = budget - cost
                q = net / p
                qty[s] += q
                invested[s] += budget
                total_invested += budget
                total_costs += cost
        value = sum(qty[s] * row[s] for s in syms if not pd.isna(row[s]))
        rows.append({"ts": ts, "invested": total_invested, "value": value})
        for s in syms:
            rows[-1][f"qty_{s}"] = qty[s]

    ledger = pd.DataFrame(rows).set_index("ts")
    return {
        "ledger": ledger,
        "qty": qty,
        "invested": invested,
        "total_invested": total_invested,
        "total_costs": total_costs,
        "n_purchases": len(schedule) * len(syms),
        "price_df": price_df,
    }


def max_drawdown(equity: pd.Series) -> float:
    peak = equity.cummax()
    dd = (equity - peak) / peak
    return float(dd.min())


def annualized_vol_sharpe(equity: pd.Series) -> tuple[float, float]:
    eq = equity[equity > 0].replace([float("inf")], pd.NA).dropna()
    rets = eq.pct_change().replace([float("inf")], pd.NA).dropna()
    if len(rets) < 2 or rets.std() == 0:
        return 0.0, 0.0
    vol = float(rets.std() * (365 ** 0.5))  # crypto trades 24/7
    ann_ret = float((eq.iloc[-1] / eq.iloc[0]) ** (365 / max(len(eq), 1)) - 1)
    # rough excess: crypto "risk-free" ≈ USDT yield ~4% (DeFi/stable yield proxy)
    sharpe = (ann_ret - 0.04) / vol if vol > 0 else 0.0
    return vol, sharpe


def main() -> None:
    p = argparse.ArgumentParser(description="Crypto spot pure-DCA baseline backtest")
    p.add_argument("--monthly", type=float, default=3000.0, help="total USDT per period (split across symbols)")
    p.add_argument("--freq", choices=["monthly", "weekly"], default="monthly")
    p.add_argument("--contribution-day", type=int, default=1, help="day of month (monthly freq)")
    p.add_argument("--symbols", type=str, default="BTC,ETH,SOL", help="comma list: BTC,ETH,SOL")
    p.add_argument("--start", type=str, default="2023-09-12")
    p.add_argument("--end", type=str, default=None)
    p.add_argument("--db", type=str, default=None)
    p.add_argument("--no-costs", action="store_true", help="zero fees/slippage")
    args = p.parse_args()

    end = args.end or datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
    syms = [s.strip().upper() for s in args.symbols.split(",")]
    ccxt_syms = [f"{s}/USDT" for s in syms]

    db_path = args.db or str(DEFAULT_DB)
    store = HistoricalOHLCVStore(db_path=db_path)

    prices = {}
    for s in ccxt_syms:
        prices[s] = load_prices(store, s, args.start, end)

    cost_model = None if args.no_costs else CostModel(vip_tier=VipTier.VIP0)
    per_purchase = args.monthly if args.freq == "monthly" else args.monthly

    result = run_dca(prices, per_purchase, args.freq, args.contribution_day, cost_model)
    ledger = result["ledger"]
    equity = ledger["value"]
    final_value = float(equity.iloc[-1])

    # metrics
    moic = final_value / result["total_invested"] if result["total_invested"] else 0.0
    cashflows = [
        (ts, -float(row["invested"])) for ts, row in ledger.iterrows() if row["invested"] > 0
    ]
    # collapse daily invested into actual purchase dates for XIRR accuracy
    cf_map: dict[pd.Timestamp, float] = {}
    prev_inv = 0.0
    for ts, row in ledger.iterrows():
        if row["invested"] > prev_inv:
            cf_map[ts] = -(float(row["invested"]) - prev_inv)
            prev_inv = float(row["invested"])
    cashflows = sorted(cf_map.items()) + [(ledger.index[-1], final_value)]
    xirr_val = xirr(cashflows)
    mdd = max_drawdown(equity)
    vol, sharpe = annualized_vol_sharpe(equity)

    # lump-sum comparison: same total, invested on day 1
    price_df = result["price_df"]
    lump = {}
    for s in ccxt_syms:
        p0 = price_df[s].dropna()
        if len(p0):
            budget = result["total_invested"] / len(ccxt_syms)
            cost = cost_model.compute_trade_cost(budget, "spot") if cost_model else 0.0
            lump[s] = (budget - cost) / float(p0.iloc[0])
    lump_value = sum(lump[s] * float(price_df[s].iloc[-1]) for s in lump)

    years = (ledger.index[-1] - ledger.index[0]).days / 365.25
    n_months = round(years * 12)

    # ------------------------------------------------------------------ print
    print("\n" + "=" * 64)
    print(f"  纯 DCA 基准回测  |  {args.freq} {args.monthly:.0f} USDT × {len(ccxt_syms)} 币")
    print(f"  区间: {ledger.index[0].date()} ~ {ledger.index[-1].date()}  ({years:.1f} 年 / {n_months} 个月)")
    print(f"  成本: {'无 (理想化)' if args.no_costs else 'Binance现货 VIP0 taker 0.10%'}")
    print("=" * 64)

    print(f"\n  投入总额      : {result['total_invested']:>14,.0f} USDT")
    print(f"  交易成本合计  : {result['total_costs']:>14,.2f} USDT")
    print(f"  期末市值      : {final_value:>14,.0f} USDT")
    print(f"  净收益        : {final_value - result['total_invested']:>14,.0f} USDT")
    print(f"  收益倍数 MOIC : {moic:>14.3f}x")

    print(f"\n  XIRR 年化     : {xirr_val * 100 if xirr_val is not None else float('nan'):>14.2f}%")
    print(f"  最大回撤      : {mdd * 100:>14.2f}%")
    print(f"  年化波动率    : {vol * 100:>14.2f}%")
    print(f"  Sharpe(rf=4%) : {sharpe:>14.2f}")

    print(f"\n  一次性投入对照: 第 1 天全仓买入同样金额")
    lump_mult = lump_value / result["total_invested"] if result["total_invested"] else 0.0
    print(f"    期末市值    : {lump_value:>14,.0f} USDT  ({lump_mult:.3f}x)")
    print(f"    DCA 相对    : {((moic / lump_mult - 1) * 100 if lump_mult else 0.0):>+14.2f}%")

    print("\n  分币种:")
    print(f"  {'币种':<14}{'数量':>12}{'投入':>12}{'市值':>12}{'均价':>12}{'现价':>12}{'P&L%':>9}")
    for s in ccxt_syms:
        q, inv = result["qty"][s], result["invested"][s]
        px_last = float(price_df[s].iloc[-1])
        val = q * px_last
        avg = inv / q if q else 0.0
        pnl = (val / inv - 1) * 100 if inv else 0.0
        print(f"  {s:<14}{q:>12.5f}{inv:>12,.0f}{val:>12,.0f}{avg:>12,.1f}{px_last:>12,.1f}{pnl:>+8.1f}%")

    # ------------------------------------------------------------------ save
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M")
    ledger.to_csv(OUT_DIR / f"ledger_{stamp}.csv")
    summary = {
        "config": {
            "freq": args.freq, "per_period_usdt": args.monthly, "symbols": ccxt_syms,
            "start": str(ledger.index[0].date()), "end": str(ledger.index[-1].date()),
            "costs": "none" if args.no_costs else "spot VIP0 taker 10bps",
        },
        "total_invested": result["total_invested"],
        "total_costs": result["total_costs"],
        "final_value": final_value,
        "moic": moic,
        "xirr": xirr_val,
        "max_drawdown": mdd,
        "annualized_vol": vol,
        "sharpe_rf4": sharpe,
        "lump_sum_value": lump_value,
        "per_symbol": {
            s: {
                "qty": result["qty"][s], "invested": result["invested"][s],
                "value": result["qty"][s] * float(price_df[s].iloc[-1]),
            } for s in ccxt_syms
        },
    }
    with open(OUT_DIR / f"summary_{stamp}.json", "w") as f:
        json.dump(summary, f, indent=2, default=str)
    print(f"\n  已保存: {OUT_DIR}/summary_{stamp}.json + ledger_{stamp}.csv\n")


if __name__ == "__main__":
    main()
