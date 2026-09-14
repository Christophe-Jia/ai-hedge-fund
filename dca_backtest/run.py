"""CLI runner for the DCA backtest framework.

Examples:
    poetry run python -m dca_backtest.run --strategy both
    poetry run python -m dca_backtest.run --weights "QQQ:0.5,VOO:0.4,TQQQ:0.1" --rebalance annual
    poetry run python -m dca_backtest.run --strategy tqqq-20 --years 40 --horizons 15,20,25
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import pandas as pd
from rich.console import Console
from rich.table import Table

from .config import BacktestConfig, with_overrides
from .data import build_fx, build_growth_levels, fetch_raw
from .engine import run_backtest
from .metrics import max_drawdown, rolling_target_analysis, xirr
from .strategy import PRESETS, FixedWeights

console = Console()


def _parse_weights(s: str) -> dict[str, float]:
    w = {}
    for part in s.split(","):
        k, v = part.split(":")
        w[k.strip().upper()] = float(v)
    return w


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="QQQ/VOO/TQQQ monthly DCA backtest (RMB contributions)")
    p.add_argument("--monthly-rmb", type=float, default=20_000, help="monthly contribution in RMB")
    p.add_argument("--target-rmb", type=float, default=20_000_000, help="target portfolio value in RMB")
    p.add_argument("--years", type=int, default=40, help="backtest length in years (default 40)")
    p.add_argument("--start", type=str, default=None, help="explicit start date YYYY-MM-DD (overrides --years)")
    p.add_argument("--end", type=str, default=None, help="explicit end date YYYY-MM-DD")
    p.add_argument("--strategy", type=str, default="both", choices=sorted(PRESETS), help="preset allocation")
    p.add_argument("--weights", type=str, default=None, help='custom weights, e.g. "QQQ:0.5,VOO:0.5"')
    p.add_argument("--rebalance", type=str, default="none", choices=["none", "monthly", "annual"])
    p.add_argument("--contribution-day", type=int, default=1)
    p.add_argument("--contribution-growth", type=float, default=0.0, help="annual growth of monthly amount, e.g. 0.05")
    p.add_argument("--fx-spread-bps", type=float, default=20.0, help="RMB->USD conversion cost in bps")
    p.add_argument("--wht", type=float, default=0.10, help="US dividend withholding tax (0.10 treaty rate)")
    p.add_argument("--tqqq-financing-spread", type=float, default=None, help="override synthetic TQQQ financing spread (annual)")
    p.add_argument("--ignore-fx", action="store_true", help="pure USD simulation (no FX conversion)")
    p.add_argument("--horizons", type=str, default="10,15,20,25,30", help="rolling-analysis horizons in years")
    p.add_argument("--skip-rolling", action="store_true", help="skip the rolling start-date probability analysis")
    p.add_argument("--refresh-data", action="store_true", help="re-download price data")
    p.add_argument("--no-plot", action="store_true")
    p.add_argument("--out-dir", type=str, default="outputs/dca_backtest")
    return p.parse_args()


def _fmt_rmb(x: float) -> str:
    return f"{x:,.0f}"


def _print_assumptions(args, cfg: BacktestConfig, instruments, provenance: pd.DataFrame) -> None:
    t = Table(title="假设 / Assumptions", show_lines=False)
    t.add_column("项目")
    t.add_column("取值", justify="right")
    t.add_row("每月定投 (RMB)", _fmt_rmb(cfg.monthly_contribution_rmb))
    if cfg.contribution_growth_annual:
        t.add_row("定投金额年增长", f"{cfg.contribution_growth_annual:.1%}")
    t.add_row("换汇成本", f"{cfg.fx_spread_bps:.0f} bps")
    t.add_row("美股红利预扣税", f"{cfg.dividend_withholding_tax:.0%}")
    t.add_row("再平衡", cfg.rebalance)
    for spec in instruments.values():
        t.add_row(
            f"{spec.symbol} 成本",
            f"点差 {spec.spread_bps:.0f}bps / 合成段ER {spec.er_annual:.2%}"
            + (f" / 融资利差 {spec.financing_spread:.2%}" if spec.financing_spread else ""),
        )
    console.print(t)

    prov = provenance.copy()
    for c in ("start", "end"):
        prov[c] = prov[c].dt.date
    t2 = Table(title="数据来源链 / Data provenance (新旧顺序拼接)")
    for c in ("symbol", "source", "kind", "start", "end"):
        t2.add_column(c)
    for _, r in prov.iterrows():
        t2.add_row(*[str(v) for v in r])
    console.print(t2)


def _print_results(res, irr: float | None, mdd: float) -> None:
    t = Table(title=f"回测结果 / Result — {res.strategy_name}", show_lines=False)
    t.add_column("指标")
    t.add_column("数值", justify="right")
    t.add_row("区间", f"{res.start.date()} → {res.end.date()}")
    t.add_row("定投月数", f"{res.months}")
    t.add_row("累计投入 (RMB)", _fmt_rmb(res.total_contrib_rmb))
    t.add_row("期末净值 (RMB)", _fmt_rmb(res.final_nav_rmb))
    t.add_row("期末净值 (USD)", f"{res.final_nav_usd:,.0f}")
    t.add_row("净值 / 投入倍数", f"{res.multiple:.2f}x")
    if irr is not None:
        t.add_row("XIRR (年化资金加权收益)", f"{irr:.2%}")
    t.add_row("最大回撤 (NAV口径, 含定投)", f"{mdd:.1%}")
    if res.hit_date is not None:
        t.add_row("达到目标", f"[green]{res.hit_date.date()}[/green]")
    else:
        t.add_row("达到目标", "未达到 (该区间内)")
    console.print(t)


def _print_rolling(stats: dict) -> None:
    t = Table(title="滚动起点分析 / Rolling start-date analysis — 达到目标的概率")
    t.add_column("期限 (年)", justify="right")
    t.add_column("可用起点数", justify="right")
    t.add_column("达成数", justify="right")
    t.add_column("达成概率", justify="right")
    for h, s in stats["horizons"].items():
        prob = s["probability"]
        color = "green" if prob >= 0.8 else ("yellow" if prob >= 0.5 else "red")
        t.add_row(str(h), f"{s['eligible_starts']}", f"{s['hits']}", f"[{color}]{prob:.1%}[/{color}]")
    console.print(t)

    o = stats["overall"]

    def _y(v):
        return "—" if v is None else f"{v:.1f}"

    t2 = Table(title="全部历史起点统计 (不限期限, 截至 数据结束)")
    t2.add_column("指标")
    t2.add_column("数值", justify="right")
    t2.add_row("起点总数", f"{o['total_starts']}")
    t2.add_row("最终达成目标数", f"{o['ever_hit']}")
    t2.add_row("最终达成率", f"{o['ever_hit_rate']:.1%}")
    t2.add_row("达成用时 中位数 (年)", _y(o["median_years"]))
    t2.add_row("达成用时 P10 / P90 (年)", f"{_y(o['p10_years'])} / {_y(o['p90_years'])}")
    t2.add_row("达成用时 最快 / 最慢 (年)", f"{_y(o['min_years'])} / {_y(o['max_years'])}")
    console.print(t2)


def _plot(res, target_rmb: float, out: Path) -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    daily = res.daily
    fig, ax = plt.subplots(figsize=(11, 6))
    ax.plot(daily.index, daily["nav_rmb"], label="Portfolio NAV (RMB)", lw=1.2)
    ax.plot(daily.index, daily["cum_contrib_rmb"], label="Cumulative contributions (RMB)", lw=1.0)
    ax.axhline(target_rmb, ls="--", color="red", lw=1, label=f"Target {target_rmb:,.0f}")
    ax.set_yscale("log")
    ax.set_title(f"DCA backtest — {res.strategy_name}")
    ax.set_ylabel("RMB (log scale)")
    ax.grid(alpha=0.3)
    ax.legend()
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    console.print(f"[dim]chart saved to {out}[/dim]")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    args = _parse_args()

    cfg = BacktestConfig(
        monthly_contribution_rmb=args.monthly_rmb,
        contribution_day=args.contribution_day,
        contribution_growth_annual=args.contribution_growth,
        fx_spread_bps=args.fx_spread_bps,
        dividend_withholding_tax=args.wht,
        rebalance=args.rebalance,
    )
    instruments = with_overrides(tqqq_financing_spread=args.tqqq_financing_spread)

    if args.weights:
        strategy = FixedWeights(_parse_weights(args.weights))
    else:
        strategy = FixedWeights(PRESETS[args.strategy], name=args.strategy)

    end = args.end or None
    if args.start:
        start = args.start
    else:
        start = (pd.Timestamp.today() - pd.DateOffset(years=args.years)).strftime("%Y-%m-%d")

    with console.status("fetching price data (cached in dca_backtest/cache/) ..."):
        raw = fetch_raw(refresh=args.refresh_data)
    levels, provenance = build_growth_levels(raw, instruments, cfg.dividend_withholding_tax)
    fx = build_fx(raw)
    if args.ignore_fx:
        fx = pd.Series(1.0, index=levels.index)

    win = levels.loc[start:end]
    if win.dropna(how="any").empty:
        console.print("[red]no data in the requested window[/red]")
        raise SystemExit(1)

    _print_assumptions(args, cfg, instruments, provenance)

    res = run_backtest(levels, fx, cfg, strategy, instruments, start=start, end=end, target_rmb=args.target_rmb)
    irr = None
    mdd = 0.0
    if res.daily is not None:
        flows = [
            (d, -float(r["contrib_rmb"]))
            for d, r in res.daily.iterrows()
            if r["contrib_rmb"] > 0
        ]
        flows.append((res.end, res.final_nav_rmb))
        irr = xirr(flows)
        mdd = max_drawdown(res.daily["nav_rmb"])
    _print_results(res, irr, mdd)

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    if res.daily is not None:
        csv = out_dir / f"dca_{strategy.name.replace(' ', '_').replace('/', '-')}_daily.csv"
        res.daily.to_csv(csv)
        console.print(f"[dim]daily NAV saved to {csv}[/dim]")
        if not args.no_plot:
            _plot(res, args.target_rmb, out_dir / "equity_curve.png")

    if not args.skip_rolling:
        horizons = tuple(int(h) for h in args.horizons.split(","))
        with console.status("rolling start-date analysis (simulating every historical start month) ...") as st:
            def prog(done, total):
                st.update(f"rolling analysis: {done}/{total} start months simulated")

            _, stats = rolling_target_analysis(
                levels, fx, cfg, strategy, instruments, args.target_rmb, horizons, progress=prog
            )
        _print_rolling(stats)


if __name__ == "__main__":
    main()
