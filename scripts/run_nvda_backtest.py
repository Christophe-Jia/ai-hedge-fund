"""
NVDA EMA 回测脚本 — scripts/run_nvda_backtest.py

数据来源优先级：
  1. tests/fixtures/api/prices/<TICKER>_<start>_<end>.json（本地文件，离线运行）
  2. Yahoo Finance yfinance（需联网，首次运行自动下载）

策略：EMA 双均线交叉（可调参数）

用法：
    poetry run python scripts/run_nvda_backtest.py
    poetry run python scripts/run_nvda_backtest.py --ticker NVDA --start-date 2024-01-01 --end-date 2024-12-31
    poetry run python scripts/run_nvda_backtest.py --ticker AAPL --fast 8 --slow 21 --capital 50000

下载并保存实际数据：
    poetry run python scripts/download_stock_data.py --ticker NVDA --start 2024-01-01 --end 2024-12-31

输出：
    - 每日交易明细表格
    - Sharpe ratio、Sortino ratio、最大回撤
    - 总收益率 vs SPY 基准
"""

import argparse
import json
import sys
from pathlib import Path
from unittest.mock import patch

# 确保项目根目录在 Python 路径中
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import pandas as pd

from src.backtesting.engine import BacktestEngine
from src.strategies.ema_strategy import EmaStrategy
from src.data.models import Price

FIXTURES_DIR = ROOT / "tests" / "fixtures" / "api" / "prices"


# ─────────────────────────────────────────────────────────────────────────────
# 数据加载：本地 fixture 优先，其次 yfinance
# ─────────────────────────────────────────────────────────────────────────────

_df_cache: dict[str, pd.DataFrame] = {}


def _find_fixture(ticker: str, start: str, end: str) -> Path | None:
    """找到覆盖请求日期范围的 fixture 文件。"""
    for p in sorted(FIXTURES_DIR.glob(f"{ticker}_*.json")):
        try:
            parts = p.stem.split("_")
            _, fstart, fend = parts[0], parts[1], parts[2]
            if not (end < fstart or start > fend):
                return p
        except Exception:
            continue
    return None


def _load_fixture(path: Path) -> pd.DataFrame:
    """从 JSON fixture 加载为 DataFrame。"""
    with path.open() as f:
        data = json.load(f)
    df = pd.DataFrame(data["prices"])
    df["Date"] = pd.to_datetime(df["time"]).dt.tz_convert("UTC")
    df.set_index("Date", inplace=True)
    for col in ("open", "close", "high", "low", "volume"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df.sort_index()


def _load_yfinance(ticker: str) -> pd.DataFrame:
    """从 yfinance 下载全年数据（仅当 fixture 不存在时调用）。"""
    try:
        import yfinance as yf
        t = yf.Ticker(ticker)
        df = t.history(start="2023-01-01", end="2025-01-01", auto_adjust=True)
        df.index = df.index.tz_convert("UTC")
        df.columns = [c.lower() for c in df.columns]
        return df
    except Exception as e:
        raise RuntimeError(
            f"无法获取 {ticker} 数据：{e}\n"
            f"请先下载 fixture：\n"
            f"  poetry run python scripts/download_stock_data.py --ticker {ticker} --start 2024-01-01 --end 2024-12-31"
        ) from e


def _get_df(ticker: str, start: str, end: str) -> pd.DataFrame:
    """获取价格 DataFrame，先查本地 fixture，再查 yfinance 缓存。"""
    cache_key = ticker
    if cache_key not in _df_cache:
        fixture = _find_fixture(ticker, start, end)
        if fixture:
            _df_cache[cache_key] = _load_fixture(fixture)
        else:
            print(f"  [提示] 未找到 {ticker} 本地 fixture，尝试从 Yahoo Finance 下载...")
            _df_cache[cache_key] = _load_yfinance(ticker)

    full = _df_cache[cache_key]
    start_ts = pd.to_datetime(start).tz_localize("UTC")
    end_ts = pd.to_datetime(end).tz_localize("UTC")
    return full.loc[(full.index >= start_ts) & (full.index <= end_ts)]


# ─────────────────────────────────────────────────────────────────────────────
# API 适配器（供 monkeypatch 使用）
# ─────────────────────────────────────────────────────────────────────────────

def _fake_get_prices(ticker: str, start_date: str, end_date: str, api_key=None) -> list:
    df = _get_df(ticker, start_date, end_date)
    if df.empty:
        return []
    prices = []
    for ts, row in df.iterrows():
        prices.append(Price(
            ticker=ticker,
            open=float(row["open"]),
            close=float(row["close"]),
            high=float(row["high"]),
            low=float(row["low"]),
            volume=int(row["volume"]),
            time=ts.isoformat(),
        ))
    return prices


def _fake_get_price_data(ticker: str, start_date: str, end_date: str, api_key=None) -> pd.DataFrame:
    df = _get_df(ticker, start_date, end_date)
    if df.empty:
        return pd.DataFrame()
    return df[["open", "close", "high", "low", "volume"]].copy()


# ─────────────────────────────────────────────────────────────────────────────
# 主程序
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="使用 EMA 交叉策略回测股票（优先读取本地 fixture，无 fixture 则从 yfinance 下载）",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--ticker", default="NVDA", help="股票代码")
    parser.add_argument("--start-date", default="2024-01-01", help="回测起始日期 YYYY-MM-DD")
    parser.add_argument("--end-date", default="2024-12-31", help="回测截止日期 YYYY-MM-DD")
    parser.add_argument("--fast", type=int, default=10, help="快线 EMA 周期")
    parser.add_argument("--slow", type=int, default=30, help="慢线 EMA 周期")
    parser.add_argument("--quantity-pct", type=float, default=0.10, help="每次开仓占现金比例（0~1）")
    parser.add_argument("--capital", type=float, default=100_000.0, help="初始资金（美元）")
    args = parser.parse_args()

    # 判断数据来源
    nvda_fixture = _find_fixture(args.ticker, args.start_date, args.end_date)
    spy_fixture = _find_fixture("SPY", args.start_date, args.end_date)
    source = "本地 fixture" if nvda_fixture else "Yahoo Finance (yfinance)"

    print(f"\n{'=' * 60}")
    print(f"  EMA 回测：{args.ticker}")
    print(f"  区间：{args.start_date} → {args.end_date}")
    print(f"  参数：EMA({args.fast}, {args.slow})，开仓比例 {args.quantity_pct:.0%}")
    print(f"  初始资金：${args.capital:,.0f}")
    print(f"  数据来源：{source}")
    print(f"{'=' * 60}\n")

    strategy = EmaStrategy(
        fast_period=args.fast,
        slow_period=args.slow,
        quantity_pct=args.quantity_pct,
    )

    # 用本地数据替换所有 financialdatasets.ai API 调用
    with (
        patch("src.backtesting.engine.get_prices", side_effect=lambda *a, **k: None),
        patch("src.backtesting.engine.get_price_data", side_effect=_fake_get_price_data),
        patch("src.backtesting.engine.get_financial_metrics", return_value=[]),
        patch("src.backtesting.engine.get_insider_trades", return_value=[]),
        patch("src.backtesting.engine.get_company_news", return_value=[]),
        patch("src.strategies.ema_strategy.get_prices", side_effect=_fake_get_prices),
        patch("src.tools.api.get_prices", side_effect=_fake_get_prices),
        patch("src.tools.api.get_price_data", side_effect=_fake_get_price_data),
        patch("src.backtesting.benchmarks.get_price_data", side_effect=_fake_get_price_data),
    ):
        engine = BacktestEngine(
            agent=strategy,
            tickers=[args.ticker],
            start_date=args.start_date,
            end_date=args.end_date,
            initial_capital=args.capital,
            model_name="",
            model_provider="",
            selected_analysts=None,
            initial_margin_requirement=0.0,
        )

        metrics = engine.run_backtest()

    # 打印汇总
    print(f"\n{'=' * 60}")
    print("  回测结果汇总")
    print(f"{'=' * 60}")
    sharpe = metrics.get("sharpe_ratio")
    sortino = metrics.get("sortino_ratio")
    drawdown = metrics.get("max_drawdown")
    print(f"  Sharpe Ratio   : {sharpe:.4f}" if sharpe is not None else "  Sharpe Ratio   : N/A")
    print(f"  Sortino Ratio  : {sortino:.4f}" if sortino is not None else "  Sortino Ratio  : N/A")
    # max_drawdown is already stored as a percentage (e.g. -3.02 means -3.02%)
    print(f"  Max Drawdown   : {drawdown:.2f}%" if drawdown is not None else "  Max Drawdown   : N/A")
    print(f"{'=' * 60}\n")


if __name__ == "__main__":
    main()
