"""
下载股票 OHLCV 数据并保存为 fixture 格式 — scripts/download_stock_data.py

依赖：yfinance（需单独安装）
    pip install yfinance
    # 或者
    poetry add yfinance --group dev

用法：
    # 下载 NVDA 2024 全年数据（financialdatasets.ai 免费 ticker，也可用 yfinance 对比）
    poetry run python scripts/download_stock_data.py --ticker NVDA --start 2024-01-01 --end 2024-12-31

    # 下载多个 ticker
    poetry run python scripts/download_stock_data.py --ticker AAPL --start 2024-01-01 --end 2024-06-30
    poetry run python scripts/download_stock_data.py --ticker SPY  --start 2024-01-01 --end 2024-12-31

输出路径：
    tests/fixtures/api/prices/<TICKER>_<start>_<end>.json

格式与 conftest.py monkeypatch 兼容：
    {
      "ticker": "NVDA",
      "prices": [
        {"ticker": "NVDA", "open": 495.0, "close": 510.0, "high": 515.0, "low": 490.0,
         "volume": 50000000, "time": "2024-01-02T05:00:00Z", "time_milliseconds": 1704171600000}
        ...
      ]
    }
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

# 确保项目根目录在 Python 路径中
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

FIXTURES_DIR = ROOT / "tests" / "fixtures" / "api" / "prices"


def download_with_yfinance(ticker: str, start: str, end: str) -> list[dict]:
    """使用 yfinance 下载 OHLCV 数据，返回 fixture 格式的 price list。"""
    try:
        import yfinance as yf
    except ImportError:
        print("错误：yfinance 未安装。")
        print("请运行：pip install yfinance  或  poetry add yfinance --group dev")
        sys.exit(1)

    print(f"正在从 Yahoo Finance 下载 {ticker} ({start} → {end})...")
    df = yf.download(ticker, start=start, end=end, auto_adjust=True, progress=False)

    if df.empty:
        print(f"错误：未能获取到 {ticker} 的数据，请检查 ticker 名称和日期范围。")
        sys.exit(1)

    # yfinance 返回多级列索引时需要降级
    if hasattr(df.columns, "levels"):
        df.columns = df.columns.get_level_values(0)

    prices = []
    for ts, row in df.iterrows():
        # pandas Timestamp → ISO 8601 UTC 字符串
        time_str = ts.strftime("%Y-%m-%dT%H:%M:%SZ")
        # 转为 UTC+0 时区偏移的毫秒时间戳（yfinance 返回的是纽约 00:00 UTC 时间）
        time_ms = int(ts.timestamp() * 1000)

        prices.append({
            "ticker": ticker,
            "open": round(float(row["Open"]), 4),
            "close": round(float(row["Close"]), 4),
            "high": round(float(row["High"]), 4),
            "low": round(float(row["Low"]), 4),
            "volume": int(row["Volume"]),
            "time": time_str,
            "time_milliseconds": time_ms,
        })

    return prices


def save_fixture(ticker: str, start: str, end: str, prices: list[dict]) -> Path:
    """保存为 JSON fixture 文件。"""
    FIXTURES_DIR.mkdir(parents=True, exist_ok=True)
    output_path = FIXTURES_DIR / f"{ticker}_{start}_{end}.json"

    payload = {"ticker": ticker, "prices": prices}
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)

    return output_path


def main() -> None:
    parser = argparse.ArgumentParser(
        description="下载股票 OHLCV 数据并保存为 backtesting fixture 格式",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--ticker", required=True, help="股票代码，例如 NVDA / AAPL / SPY"
    )
    parser.add_argument("--start", required=True, help="起始日期 YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="截止日期 YYYY-MM-DD")
    args = parser.parse_args()

    ticker = args.ticker.upper()
    prices = download_with_yfinance(ticker, args.start, args.end)
    output_path = save_fixture(ticker, args.start, args.end, prices)

    print(f"已下载 {len(prices)} 条数据记录")
    print(f"已保存至：{output_path.relative_to(ROOT)}")
    print()
    print("现在可以运行回测（数据会自动从 fixture 加载，无需 API key）：")
    print(f"  poetry run python scripts/run_nvda_backtest.py --ticker {ticker} --start-date {args.start} --end-date {args.end}")


if __name__ == "__main__":
    main()
