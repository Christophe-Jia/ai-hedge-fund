#!/usr/bin/env python3
"""Leveraged-ETF decay identity — no model, no fitting, no hypothesis test.

The one result in this repo that needs no validation is an *identity*: a fund
that resets ``L`` times the underlying's daily return each day loses, to second
order in the daily log return,

    drag_per_day  =  (1/2) * L * (L - 1) * sigma_daily^2          (identity)

where ``sigma_daily`` is the standard deviation of the UNDERLYING's daily
simple return.  For L = 2 this collapses to ``sigma_daily^2`` per day.

Derivation (why it is an identity and not a model).  With ``r_t`` the
underlying's daily simple return and a perfectly implemented daily-reset L-times
fund::

    ln(1 + R_product) = sum_t ln(1 + L r_t)      approx  L sum_t r_t - (L^2/2) sum_t r_t^2
    ln(1 + L R_underlying) = L sum_t ln(1 + r_t) approx  L sum_t r_t - (L/2)   sum_t r_t^2

Subtracting, the gap in log growth is ``(L^2 - L)/2 * sum_t r_t^2``, i.e. per
day ``(1/2) L (L - 1) r_t^2``; replacing ``r_t^2`` by its expectation
``sigma_daily^2`` (zero-mean assumption) gives the stated drag.  So the identity
is the EXPECTATION of an exactly-known second-order term: for a realized path the
exact second-order drag is ``(1/2) L (L - 1) * sum_t r_t^2``, which this script
also reports so the approximation error is visible rather than asserted.

HONEST BOUNDARY — the identity is only the rebalancing/variance term.  A real
2x product's return is at least::

    r_p = L * r_u  -  drag_rebalance  -  (L - 1) * r_financing  -  fee  -  tracking_error

The ``(L - 1) * r_financing`` term is funded borrowing and is reported here from
the repo-resident short rate (data/fred/dtb3.csv).  The fee and tracking-error
terms cannot be computed from anything in this repo and are listed as data gaps.

Usage::

    poetry run python scripts/leveraged_decay.py
    poetry run python scripts/leveraged_decay.py --leveraged 2 --out reports/leveraged_decay.json
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

from src.data.historical_store import HistoricalOHLCVStore  # noqa: E402

DEFAULT_OUT = "reports/leveraged_decay.json"
DTB3_PATH = "data/fred/dtb3.csv"

TRADING_DAYS = 252

#: label -> (symbol, market_type).  The user cares about MU, MRVL and COIN.
INSTRUMENTS: tuple[tuple[str, str, str], ...] = (
    ("MU", "MU", "stocks"),
    ("MRVL", "MRVL", "stocks"),
    ("COIN", "COIN", "stocks"),
)


# ---------------------------------------------------------------------------
# data
# ---------------------------------------------------------------------------


def load_closes(symbol: str, market_type: str) -> pd.Series:
    """Daily closes from the local store; empty series when the symbol is absent.

    ``allow_fetch=False`` keeps the run offline and deterministic.  A missing
    symbol must surface as an explicit data gap, not as a silent short history.
    """
    store = HistoricalOHLCVStore(allow_fetch=False)
    end_ms = int(pd.Timestamp("2100-01-01").value // 1_000_000)
    df = store.get_ohlcv(symbol, market_type, "1d", 0, end_ms)
    if df.empty:
        return pd.Series(dtype=float)
    idx = pd.to_datetime(df["ts"], unit="ms", utc=True).dt.tz_localize(None).dt.normalize()
    s = pd.Series(df["close"].to_numpy(dtype=float), index=idx, name=symbol).sort_index()
    return s[~s.index.duplicated(keep="last")]


def load_short_rate() -> pd.Series:
    """3-month T-bill (FRED DTB3, %) as a decimal annual rate; empty if absent."""
    path = Path(DTB3_PATH)
    if not path.exists():
        return pd.Series(dtype=float)
    df = pd.read_csv(path)
    df.columns = [c.strip() for c in df.columns]
    date_col = df.columns[0]
    rate_col = "DTB3" if "DTB3" in df.columns else df.columns[1]
    s = pd.Series(
        pd.to_numeric(df[rate_col], errors="coerce").to_numpy(dtype=float),
        index=pd.to_datetime(df[date_col], errors="coerce"),
    ).dropna()
    return (s / 100.0).sort_index()


# ---------------------------------------------------------------------------
# the identity
# ---------------------------------------------------------------------------


def identity_drag_per_day(sigma_daily: float, leveraged: float) -> float:
    """(1/2) L (L-1) sigma_daily^2 — the daily rebalancing drag (identity)."""
    return 0.5 * leveraged * (leveraged - 1.0) * sigma_daily**2


def annualize_arithmetic(drag_per_day: float, trading_days: int = TRADING_DAYS) -> float:
    """Arithmetic annualisation: trading_days * daily drag."""
    return drag_per_day * trading_days


def annualize_compound(drag_per_day: float, trading_days: int = TRADING_DAYS) -> float:
    """Compound annualisation: 1 - exp(-trading_days * daily drag).

    The drag compounds like a negative yield, so the arithmetic figure slightly
    overstates the multiplicative loss.
    """
    return 1.0 - float(np.exp(-annualize_arithmetic(drag_per_day, trading_days)))


def second_order_realized_drag(
    returns: np.ndarray, leveraged: float, *, log_gap: bool = True
) -> float:
    """Exact realized second-order term for the path.

    ``log_gap=True``  -> (1/2) L (L-1) * sum(r^2)     (exact log-growth gap)
    ``log_gap=False`` -> the same expressed as a simple-return gap,
                         ``1 - exp(-sum)``.
    """
    acc = 0.5 * leveraged * (leveraged - 1.0) * float(np.sum(returns**2))
    return acc if log_gap else 1.0 - float(np.exp(-acc))


def simulated_product_returns(returns: np.ndarray, leveraged: float) -> np.ndarray:
    """Daily returns of an IDEAL daily-reset L-times fund: L * r_t, no costs."""
    return leveraged * returns


def path_gap(returns: np.ndarray, leveraged: float) -> dict[str, float]:
    """Realized gap between an ideal daily-reset product and an L-times-log-growth exposure.

    The benchmark must be an exposure that delivers ``L`` times the underlying's
    LOG growth, i.e. cumulative return ``exp(L * sum ln(1+r_t)) - 1``.  (Using
    ``L`` times the underlying's *simple* cumulative return is a different, badly
    behaved object — it conflates the drag with ordinary compounding and can
    exceed the product by hundreds of percent on a trending path.)  With this
    benchmark the log gap is exactly

        sum_t ln(1 + L r_t) - L sum_t ln(1 + r_t)  ~=  -(1/2) L (L-1) sum_t r_t^2

    i.e. minus the realized second-order drag, so the identity can be checked
    against an exact quantity instead of asserted.
    """
    log_growth = float(np.sum(np.log1p(returns)))
    benchmark_geo = float(np.exp(leveraged * log_growth)) - 1.0
    benchmark_prat = leveraged * (float(np.prod(1.0 + returns)) - 1.0)
    ideal = float(np.prod(1.0 + leveraged * returns)) - 1.0
    return {
        # practitioner framing: "L times the underlying's cumulative return".
        # NOT the right object for isolating the drag — it conflates the drag with
        # ordinary compounding, so it can be hundreds of percent away on a trend.
        "benchmark_Lx_cumulative_simple": benchmark_prat,
        "gap_vs_Lx_cumulative_simple": ideal - benchmark_prat,
        # geometric framing: an exposure delivering L times the underlying's LOG
        # growth. This is the benchmark for which the drag decomposition is exact.
        "benchmark_L_times_loggrowth": benchmark_geo,
        "ideal_daily_reset": ideal,
        "gap_simple": ideal - benchmark_geo,
        "gap_log": (
            float(np.log1p(ideal) - np.log1p(benchmark_geo))
            if ideal > -1.0 and benchmark_geo > -1.0
            else np.nan
        ),
    }


def per_year_table(
    closes: pd.Series,
    *,
    leveraged: float,
    short_rate: pd.Series | None = None,
) -> list[dict[str, Any]]:
    """One row per calendar year: realized vol, identity drag, realized gap."""
    rets = closes.pct_change().dropna()
    rows: list[dict[str, Any]] = []
    for year, grp in rets.groupby(rets.index.year):
        r = grp.to_numpy(dtype=float)
        if r.size < 20:
            continue
        sigma_d = float(np.std(r, ddof=1))
        drag_d = identity_drag_per_day(sigma_d, leveraged)
        row: dict[str, Any] = {
            "year": int(year),
            "n_days": int(r.size),
            "sigma_daily": sigma_d,
            "sigma_annualized": sigma_d * float(np.sqrt(TRADING_DAYS)),
            "drag_per_day": drag_d,
            "annual_drag_arithmetic": annualize_arithmetic(drag_d),
            "annual_drag_compound": annualize_compound(drag_d),
            "realized_second_order_drag_log": second_order_realized_drag(r, leveraged, log_gap=True),
            "realized_second_order_drag_compound": second_order_realized_drag(r, leveraged, log_gap=False),
        }
        row.update({f"path_{k}": v for k, v in path_gap(r, leveraged).items()})
        row["identity_vs_realized_log_gap_diff"] = (
            row["realized_second_order_drag_log"] - row["annual_drag_arithmetic"]
        )
        # exact accounting check: realized log gap should equal -(2nd-order term)
        # up to the neglected 3rd-and-higher order terms
        row["identity_residual_3rd_order"] = (
            row["realized_second_order_drag_log"] + row["path_gap_log"]
        )
        if short_rate is not None and not short_rate.empty:
            yr_rate = short_rate[short_rate.index.year == year]
            if not yr_rate.empty:
                avg_rate = float(yr_rate.mean())
                row["short_rate_avg_dtb3"] = avg_rate
                row["financing_drag_annual"] = (leveraged - 1.0) * avg_rate
        rows.append(row)
    return rows


def summarize(rows: list[dict[str, Any]], *, leveraged: float) -> dict[str, Any]:
    """Full-period pooled statistics plus the worst-volatility year."""
    if not rows:
        return {}
    sigma_d = np.array([r["sigma_daily"] for r in rows], dtype=float)
    drag_d = identity_drag_per_day(float(sigma_d.mean()), leveraged)
    peak = max(rows, key=lambda r: r["sigma_daily"])
    return {
        "n_years": len(rows),
        "mean_sigma_daily": float(sigma_d.mean()),
        "median_sigma_daily": float(np.median(sigma_d)),
        "mean_sigma_annualized": float(sigma_d.mean() * np.sqrt(TRADING_DAYS)),
        "full_period_drag_per_day_from_mean_sigma": drag_d,
        "full_period_annual_drag_arithmetic": annualize_arithmetic(drag_d),
        "full_period_annual_drag_compound": annualize_compound(drag_d),
        "mean_of_annual_drags_arithmetic": float(np.mean([r["annual_drag_arithmetic"] for r in rows])),
        "mean_of_annual_realized_second_order_log": float(
            np.mean([r["realized_second_order_drag_log"] for r in rows])
        ),
        "highest_volatility_year": {
            "year": peak["year"],
            "sigma_daily": peak["sigma_daily"],
            "sigma_annualized": peak["sigma_annualized"],
            "annual_drag_arithmetic": peak["annual_drag_arithmetic"],
            "annual_drag_compound": peak["annual_drag_compound"],
        },
        "amplification_note": (
            "drag scales as L(L-1): the 2x drag is 2x the 1x drag only in the sense "
            "that 0.5*1*0 = 0 for L=1 — an UNLEVERED fund has ZERO rebalancing drag, so "
            "the whole term is created by the leverage."
        ),
    }


# ---------------------------------------------------------------------------
# report
# ---------------------------------------------------------------------------


def build_report(leveraged: float) -> dict[str, Any]:
    short_rate = load_short_rate()
    instruments: dict[str, Any] = {}
    for label, symbol, market_type in INSTRUMENTS:
        closes = load_closes(symbol, market_type)
        if closes.empty:
            instruments[label] = {
                "status": "DATA_GAP",
                "symbol": symbol,
                "market_type": market_type,
                "message": (
                    f"'{symbol}' is not present in data/btc_history.db (ohlcv, {market_type}, 1d). "
                    "The identity cannot be evaluated for this instrument until the series is "
                    "backfilled; do NOT substitute a different ticker or a shorter proxy."
                ),
                "data_needed": [
                    f"daily closes for {symbol} ({market_type}) in data/btc_history.db",
                    "corporate-action-adjusted (splits/dividends) — the store's Nasdaq series is split-adjusted",
                ],
            }
            continue
        rows = per_year_table(closes, leveraged=leveraged, short_rate=short_rate)
        instruments[label] = {
            "status": "OK",
            "symbol": symbol,
            "market_type": market_type,
            "first_bar": str(closes.index[0].date()),
            "last_bar": str(closes.index[-1].date()),
            "n_bars": int(closes.shape[0]),
            "per_year": rows,
            "full_period": summarize(rows, leveraged=leveraged),
        }

    # repo-resident financing proxy
    fin: dict[str, Any] = {
        "proxy": "FRED DTB3 (3-month Treasury bill, secondary market, %/yr) from data/fred/dtb3.csv",
        "why_proxy": (
            "A daily-reset 2x ETF obtains its exposure through a total-return swap: it pays "
            "the financing leg on the borrowed (L-1)x notional. The swap rate is a short "
            "money-market rate plus a dealer spread; DTB3 is the repo-resident observable "
            "proxy for that base. The spread is NOT in this repo."
        ),
    }
    if not short_rate.empty:
        fin["dtb3_last"] = float(short_rate.iloc[-1])
        fin["dtb3_last_date"] = str(short_rate.index[-1].date())
        fin["dtb3_mean_2023_2026"] = float(short_rate[short_rate.index.year >= 2023].mean())
        for L in (2.0, 3.0):
            fin[f"annual_financing_drag_L{int(L)}_at_last_dtb3"] = (L - 1.0) * float(short_rate.iloc[-1])
    else:
        fin["status"] = "DATA_GAP"

    extra_costs = {
        "1_expense_ratio": {
            "status": "DATA_GAP (not in repo)",
            "formula": "flat annual %, charged on NAV",
            "magnitude_hint": (
                "daily-reset single-stock leveraged ETFs are typically quoted in the "
                "0.9%-1.3%/yr range, but the exact number must come from the issuer's "
                "prospectus/NAV page — it must NOT be guessed here"
            ),
            "needed": "issuer prospectus / expense ratio for the specific product",
        },
        "2_tracking_error": {
            "status": "DATA_GAP (not in repo)",
            "formula": "realized product return minus the ideal daily-reset L*x_underlying return",
            "magnitude_hint": (
                "driven by swap bid/ask, futures basis, rebalance timing and cash drag; "
                "typically small (10-50bp/yr) but only measurable, never assumable"
            ),
            "needed": (
                "daily NAV total-return series for the actual product, aligned to the "
                "underlying — then this is computed directly, not modelled"
            ),
        },
        "3_financing_swap_cost": {
            "status": "PARTIALLY COMPUTED (base rate only)",
            "formula": "(L - 1) * r_financing  per year",
            "computed_from": fin.get("proxy"),
            "magnitude": (
                f"{(leveraged - 1.0) * float(short_rate.iloc[-1]):.4f}/yr at the last DTB3 "
                f"({float(short_rate.iloc[-1]):.4%})"
                if not short_rate.empty
                else None
            ),
            "needed": "the actual dealer swap spread over the base rate",
        },
        "4_path_dependence": {
            "status": "COMPUTED ex post",
            "meaning": (
                "the identity is an expectation over sigma; a realized path's gap between "
                "the product and the benchmark is path-dependent. TWO benchmarks are "
                "reported per year, both aligned to the same realized returns: "
                "(a) benchmark_Lx_cumulative_simple = L times the underlying's cumulative "
                "simple return (the practitioner framing, gap_vs_Lx_cumulative_simple); "
                "(b) benchmark_L_times_loggrowth = exp(L * sum ln(1+r_t)) - 1 (the geometric "
                "framing, gap_log) against which the drag decomposition is exact. Use "
                "gap_log to reason about the drag; the simple-return gap also contains "
                "ordinary compounding and can be enormous on a trending path."
            ),
            "exactness_check": (
                "identity_residual_3rd_order = realized_second_order_drag_log + "
                "path_gap_log: this is the part of the realized gap NOT explained by the "
                "second-order identity, i.e. the neglected 3rd-and-higher order terms. It "
                "is reported so the approximation error is visible rather than asserted."
            ),
            "point": (
                "path dependence does not cancel: for the same cumulative underlying move, "
                "a choppier path produces a strictly larger gap"
            ),
        },
    }
    conclusion: dict[str, Any] = {}
    coin = instruments.get("COIN", {})
    if coin.get("status") == "OK":
        fp = coin["full_period"]
        fin_rate = fin.get("dtb3_last")
        comp = fp["full_period_annual_drag_compound"]
        arith = fp["full_period_annual_drag_arithmetic"]
        concl = {
            "question": "这类 2x 产品在 COIN 这种波动率下，长期期望被吃掉多少？",
            "committed_part": {
                "period": f"{coin['first_bar']}..{coin['last_bar']}",
                "mean_sigma_annualized": fp["mean_sigma_annualized"],
                "drag_annual_arithmetic": arith,
                "drag_annual_compound": comp,
                "worst_year": fp["highest_volatility_year"],
            },
            "answer": (
                f"COIN 的已实现年化波动率在这段样本里平均 {fp['mean_sigma_annualized']:.0%}。"
                f"对 L=2 的每日再平衡产品，仅『再平衡拖累』这一项（恒等式）就是约 "
                f"{arith:.0%}/年（算术口径）或 {comp:.0%}/年（复利口径）；波动最高的 "
                f"{fp['highest_volatility_year']['year']} 年（年化波动 "
                f"{fp['highest_volatility_year']['sigma_annualized']:.0%}）单年拖累约 "
                f"{fp['highest_volatility_year']['annual_drag_arithmetic']:.0%}/年。"
                + (
                    f"再叠加融资/掉期成本 (L-1)*r ≈ {fin_rate:.1%}/年，以及管理费"
                    "（本仓无数据，需 issuer 文件确认，通常 1% 上下）与跟踪误差，"
                    f"长期期望被吃掉的量级约为 {comp + (fin_rate or 0):.0%}–"
                    f"{arith + (fin_rate or 0):.0%}/年。"
                    if fin_rate is not None
                    else "再叠加融资/掉期成本、管理费与跟踪误差（均需额外数据）。"
                )
                + " 换言之：在 COIN 这种波动率下，2x 产品长期看被波动率本身吃掉的远多于"
                "被费用吃掉的——杠杆放大的是方差，而方差是恒等式意义上的损耗。"
            ),
            "caveat": (
                "这是期望/恒等式口径（用 sigma 代替 r^2），不是任何具体产品的业绩预测；"
                "实际产品的差额还包含管理费、跟踪误差、融资价差与路径依赖，见 "
                "extra_costs_beyond_the_identity。"
            ),
        }
        conclusion = concl
    report_conclusion = conclusion

    return {
        "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "leveraged": leveraged,
        "trading_days_per_year": TRADING_DAYS,
        "conclusion": report_conclusion,
        "identity": {
            "formula": "drag_per_day = 0.5 * L * (L - 1) * sigma_daily^2",
            "is_identity_not_model": (
                "exact to second order in the daily log return; the only assumption is "
                "replacing r_t^2 by its expectation sigma_daily^2 (zero-mean)"
            ),
            "derivation": (
                "log-growth of a daily-reset L-times fund minus log-growth of an L-times "
                "static exposure = (L^2-L)/2 * sum_t r_t^2, i.e. 0.5*L*(L-1)*sigma_daily^2 "
                "per day in expectation"
            ),
            "arithmetic": "annual = trading_days * drag_per_day",
            "compound": "annual = 1 - exp(-trading_days * drag_per_day)",
        },
        "instruments": instruments,
        "financing": fin,
        "extra_costs_beyond_the_identity": extra_costs,
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--leveraged", type=float, default=2.0, help="leverage factor L [2.0]")
    p.add_argument("--out", default=DEFAULT_OUT)
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    report = build_report(args.leveraged)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding="utf-8")

    print(f"L={args.leveraged:g}  drag/day = 0.5*L*(L-1)*sigma_daily^2")
    for label, block in report["instruments"].items():
        if block.get("status") != "OK":
            print(f"  {label}: {block.get('status')}")
            continue
        fp = block["full_period"]
        print(
            f"  {label}: {block['first_bar']}..{block['last_bar']}  "
            f"mean sigma_ann={fp['mean_sigma_annualized']:.1%}  "
            f"annual drag {fp['full_period_annual_drag_arithmetic']:.1%} arith / "
            f"{fp['full_period_annual_drag_compound']:.1%} comp  "
            f"worst year {fp['highest_volatility_year']['year']} "
            f"({fp['highest_volatility_year']['annual_drag_arithmetic']:.1%})"
        )
    print(f"wrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
