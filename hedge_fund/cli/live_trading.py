"""
Live trading CLI entry point.

Run crypto live trading:
    python -m src.cli.live_trading --market crypto --tickers BTC/USDT --interval 60 --paper

Run US stock live trading:
    python -m src.cli.live_trading --market alpaca --tickers AAPL,MSFT --interval 30 --paper
"""

import argparse
import sys
from colorama import Fore, Style, init as colorama_init

colorama_init()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Live trading scheduler – runs the multi-agent workflow periodically.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Paper-trade BTC every 60 minutes:
  python -m src.cli.live_trading --market crypto --tickers BTC/USDT --interval 60 --paper

  # Live-trade with Alpaca every 15 minutes:
  python -m src.cli.live_trading --market alpaca --tickers AAPL,MSFT --interval 15

  # Monitor positions only (no trades):
  python -m src.cli.live_trading --market crypto --tickers BTC/USDT --monitor-only
""",
    )
    parser.add_argument(
        "--market",
        choices=["crypto", "alpaca"],
        default="crypto",
        help="Market type: 'crypto' (CCXT) or 'alpaca' (US stocks). Default: crypto",
    )
    parser.add_argument(
        "--tickers",
        type=str,
        default="BTC/USDT",
        help="Comma-separated list of symbols. Crypto: 'BTC/USDT,ETH/USDT'. Stocks: 'AAPL,MSFT'.",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=60,
        help="Workflow execution interval in minutes. Default: 60",
    )
    parser.add_argument(
        "--exchange",
        type=str,
        default=None,
        help="CCXT exchange id (e.g. 'binance', 'okx'). Uses CCXT_EXCHANGE env var if not set.",
    )
    parser.add_argument(
        "--paper",
        action="store_true",
        default=True,
        help="Use paper/sandbox trading mode (default: True).",
    )
    parser.add_argument(
        "--live",
        action="store_true",
        default=False,
        help="Use REAL live trading (overrides --paper). Requires valid API credentials.",
    )
    parser.add_argument(
        "--monitor-only",
        action="store_true",
        default=False,
        help="Start the position monitor without placing any orders.",
    )
    parser.add_argument(
        "--monitor-interval",
        type=int,
        default=30,
        help="Position polling interval in seconds. Default: 30",
    )
    parser.add_argument(
        "--model",
        type=str,
        default=None,
        help="LLM model name to use for agent reasoning (e.g. gpt-4o, deepseek-chat).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    paper = not args.live  # --live disables paper mode; default is paper

    tickers = [t.strip() for t in args.tickers.split(",") if t.strip()]
    if not tickers:
        print(f"{Fore.RED}Error: No tickers provided.{Style.RESET_ALL}")
        sys.exit(1)

    print(f"\n{Fore.CYAN}{'=' * 50}")
    print(f"  AI Hedge Fund – Live Trading Mode")
    print(f"{'=' * 50}{Style.RESET_ALL}")
    print(f"  Market:    {Fore.GREEN}{args.market.upper()}{Style.RESET_ALL}")
    print(f"  Tickers:   {Fore.GREEN}{', '.join(tickers)}{Style.RESET_ALL}")
    print(f"  Interval:  {Fore.GREEN}{args.interval} min{Style.RESET_ALL}")
    print(f"  Mode:      {Fore.YELLOW + 'PAPER' if paper else Fore.RED + 'LIVE'}{Style.RESET_ALL}")
    if not paper:
        confirm = input(f"\n{Fore.RED}WARNING: LIVE trading is enabled. Real money will be at risk.{Style.RESET_ALL}\nType 'YES' to confirm: ")
        if confirm.strip() != "YES":
            print("Aborted.")
            sys.exit(0)
    print()

    from src.trading.executor import TradeExecutor
    from src.live.monitor import LiveMonitor
    from src.live.scheduler import LiveTradingScheduler

    executor = TradeExecutor(market=args.market, exchange_id=args.exchange, paper=paper)

    # Always start the monitor
    monitor = LiveMonitor(executor, poll_interval_sec=args.monitor_interval)
    monitor.start()

    if args.monitor_only:
        print(f"{Fore.YELLOW}Monitor-only mode. Press Ctrl-C to exit.{Style.RESET_ALL}")
        try:
            import time
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\nStopped.")
        return

    scheduler = LiveTradingScheduler(
        market=args.market,
        tickers=tickers,
        interval_minutes=args.interval,
        paper=paper,
        exchange_id=args.exchange,
    )
    scheduler.start()


if __name__ == "__main__":
    main()
