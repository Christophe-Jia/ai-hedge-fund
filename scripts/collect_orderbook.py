"""
WebSocket tick-by-tick order book and trade collector using ccxt.pro.

Streams real-time trades and L2 order book snapshots from a crypto exchange
into a local SQLite database (data/orderbook_trades.db).

Architecture: single asyncio event loop, one stream_trades + one
stream_order_book coroutine per symbol, plus a status_loop.

Usage:
    poetry run python scripts/collect_orderbook.py
    poetry run python scripts/collect_orderbook.py --symbols BTC/USD,ETH/USD --exchange kraken
    poetry run python scripts/collect_orderbook.py --symbols BTC/USDT --exchange binance --ob-depth 20

Background:
    nohup poetry run python scripts/collect_orderbook.py > logs/orderbook.log 2>&1 &
"""

from __future__ import annotations

import argparse
import asyncio
import os
import signal
import sys
import time
from datetime import datetime, timezone

# Ensure project root is on path when run directly
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

try:
    import ccxt.pro as ccxtpro
    import ccxt
except ImportError as exc:
    raise SystemExit(
        "ccxt[pro] is required. Run: poetry install\n"
        "(ccxt.pro is bundled with ccxt >= 4.0)"
    ) from exc

from src.data.orderbook_store import OrderBookTradeStore


# ---------------------------------------------------------------------------
# Stream coroutines
# ---------------------------------------------------------------------------

async def stream_trades(
    exchange: ccxtpro.Exchange,
    symbol: str,
    store: OrderBookTradeStore,
    stop_event: asyncio.Event,
) -> None:
    """Subscribe to watch_trades and persist each batch to DB."""
    print(f"[trades/{symbol}] Starting stream ...")
    while not stop_event.is_set():
        try:
            trades = await exchange.watch_trades(symbol)
            records = []
            for t in trades:
                trade_id = t.get("id")
                if not trade_id:
                    # Generate synthetic ID from ts+price when exchange omits it
                    trade_id = f"{t.get('timestamp', 0)}_{t.get('price', 0)}_{t.get('amount', 0)}"
                records.append({
                    "trade_id": str(trade_id),
                    "ts_ms": t.get("timestamp") or int(time.time() * 1000),
                    "symbol": symbol,
                    "price": float(t.get("price", 0)),
                    "amount": float(t.get("amount", 0)),
                    "side": t.get("side") or "unknown",
                })
            if records:
                n = store.insert_trades(records)
                if n > 0:
                    print(
                        f"[trades/{symbol}] +{n} trades  "
                        f"(latest price={records[-1]['price']:.2f}  "
                        f"ts={datetime.fromtimestamp(records[-1]['ts_ms']/1000, tz=timezone.utc).strftime('%H:%M:%S')})"
                    )
        except ccxt.NetworkError as e:
            print(f"[trades/{symbol}] NetworkError: {e} — reconnecting in 5s ...")
            await asyncio.sleep(5)
        except ccxt.ExchangeError as e:
            print(f"[trades/{symbol}] ExchangeError: {e} — retrying in 10s ...")
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"[trades/{symbol}] Unexpected error: {e} — retrying in 5s ...")
            await asyncio.sleep(5)

    print(f"[trades/{symbol}] Stream stopped.")


async def stream_order_book(
    exchange: ccxtpro.Exchange,
    symbol: str,
    store: OrderBookTradeStore,
    stop_event: asyncio.Event,
    depth: int = 20,
    snapshot_interval: float = 5.0,
) -> None:
    """
    Subscribe to watch_order_book and persist periodic snapshots.

    OB updates arrive very frequently; snapshot_interval (seconds) throttles
    how often we write to DB to avoid runaway storage.
    """
    print(f"[ob/{symbol}] Starting stream (depth={depth}, snapshot every {snapshot_interval}s) ...")
    last_saved = 0.0
    while not stop_event.is_set():
        try:
            ob = await exchange.watch_order_book(symbol, depth)
            now = asyncio.get_event_loop().time()
            if now - last_saved >= snapshot_interval:
                ts_ms = ob.get("timestamp") or int(time.time() * 1000)
                bids = ob.get("bids", [])[:depth]
                asks = ob.get("asks", [])[:depth]
                rows = store.insert_order_book_snapshot(ts_ms, symbol, bids, asks)
                last_saved = now
                best_bid = bids[0][0] if bids else 0
                best_ask = asks[0][0] if asks else 0
                print(
                    f"[ob/{symbol}] snapshot +{rows} rows  "
                    f"bid={best_bid:.2f}  ask={best_ask:.2f}  "
                    f"spread={best_ask - best_bid:.2f}"
                )
        except ccxt.NetworkError as e:
            print(f"[ob/{symbol}] NetworkError: {e} — reconnecting in 5s ...")
            await asyncio.sleep(5)
        except ccxt.ExchangeError as e:
            print(f"[ob/{symbol}] ExchangeError: {e} — retrying in 10s ...")
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"[ob/{symbol}] Unexpected error: {e} — retrying in 5s ...")
            await asyncio.sleep(5)

    print(f"[ob/{symbol}] Stream stopped.")


async def status_loop(
    store: OrderBookTradeStore,
    symbols: list[str],
    stop_event: asyncio.Event,
    interval: int = 600,
) -> None:
    """Print collection status every `interval` seconds (default 10 min)."""
    while not stop_event.is_set():
        try:
            await asyncio.wait_for(
                asyncio.shield(stop_event.wait()),
                timeout=interval,
            )
        except asyncio.TimeoutError:
            pass

        if stop_event.is_set():
            break

        ts = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        print(f"\n[status] {ts}")
        for sym in symbols:
            trades = store.get_trade_count(sym)
            ob_rows = store.get_snapshot_count(sym)
            latest_ts = store.get_latest_trade_ts(sym)
            latest_str = (
                datetime.fromtimestamp(latest_ts / 1000, tz=timezone.utc).strftime("%H:%M:%S")
                if latest_ts else "N/A"
            )
            print(
                f"  {sym:12s}  trades={trades:>10,}  ob_rows={ob_rows:>10,}  "
                f"latest_trade={latest_str}"
            )
        print()


# ---------------------------------------------------------------------------
# Main async entrypoint
# ---------------------------------------------------------------------------

async def main_async(
    symbols: list[str],
    exchange_id: str,
    db_path: str,
    ob_depth: int,
    snapshot_interval: float,
) -> None:
    store = OrderBookTradeStore(db_path)

    print(f"\n=== WebSocket Tick Collector ===")
    print(f"  Exchange  : {exchange_id}")
    print(f"  Symbols   : {', '.join(symbols)}")
    print(f"  OB depth  : {ob_depth} levels")
    print(f"  OB save   : every {snapshot_interval}s")
    print(f"  DB path   : {db_path}")
    print(f"  Started   : {datetime.now(tz=timezone.utc).isoformat()}")
    print(f"  Stop      : Ctrl+C or SIGTERM\n")

    # Build exchange instance
    exchange_cls = getattr(ccxtpro, exchange_id, None)
    if exchange_cls is None:
        raise SystemExit(f"Exchange '{exchange_id}' not found in ccxt.pro. "
                         f"Available: {', '.join(dir(ccxtpro)[:10])} ...")
    exchange: ccxtpro.Exchange = exchange_cls({
        "enableRateLimit": True,
        "apiKey": os.environ.get("CCXT_API_KEY", ""),
        "secret": os.environ.get("CCXT_API_SECRET", ""),
    })

    # Pre-load markets so watch_* calls don't trigger a blocking REST fetch
    # during the async event loop. Retry up to 3 times on network errors.
    for attempt in range(1, 4):
        try:
            print(f"  Loading markets ({exchange_id}) ...")
            await exchange.load_markets()
            print(f"  Markets loaded ({len(exchange.markets)} symbols)")
            break
        except Exception as e:
            print(f"  [WARN] load_markets attempt {attempt}/3 failed: {e}")
            if attempt < 3:
                await asyncio.sleep(5)
            else:
                print("  [WARN] Proceeding without pre-loaded markets — streams will load on demand")

    stop_event = asyncio.Event()

    # Graceful shutdown on SIGINT / SIGTERM
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except (NotImplementedError, RuntimeError):
            # Windows or restricted environment — rely on KeyboardInterrupt
            pass

    tasks: list[asyncio.Task] = []
    for sym in symbols:
        tasks.append(asyncio.create_task(
            stream_trades(exchange, sym, store, stop_event),
            name=f"trades-{sym}",
        ))
        tasks.append(asyncio.create_task(
            stream_order_book(exchange, sym, store, stop_event, ob_depth, snapshot_interval),
            name=f"ob-{sym}",
        ))
    tasks.append(asyncio.create_task(
        status_loop(store, symbols, stop_event),
        name="status",
    ))

    try:
        await asyncio.gather(*tasks, return_exceptions=True)
    except KeyboardInterrupt:
        stop_event.set()
    finally:
        # Cancel any still-running tasks
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await exchange.close()
        print("\n[collector] Shutdown complete.")

        # Final status
        print("\n[final status]")
        for sym in symbols:
            trades = store.get_trade_count(sym)
            ob_rows = store.get_snapshot_count(sym)
            print(f"  {sym:12s}  trades={trades:>10,}  ob_rows={ob_rows:>10,}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="WebSocket tick-by-tick order book and trade collector"
    )
    parser.add_argument(
        "--symbols",
        default="BTC/USDT,ETH/USDT",
        help="Comma-separated trading pairs (default: BTC/USDT,ETH/USDT)",
    )
    parser.add_argument(
        "--exchange",
        default="gate",
        help="ccxt.pro exchange ID (default: gate; kraken/binance may be geo-blocked)",
    )
    parser.add_argument(
        "--ob-depth",
        type=int,
        default=25,
        help="Order book depth to subscribe and store (default: 25; Kraken accepts 10/25/100/500/1000)",
    )
    parser.add_argument(
        "--db",
        default=os.path.join(
            os.path.dirname(__file__), "..", "data", "orderbook_trades.db"
        ),
        help="SQLite database path (default: data/orderbook_trades.db)",
    )
    parser.add_argument(
        "--snapshot-interval",
        type=float,
        default=5.0,
        help="Seconds between OB snapshot saves (default: 5.0)",
    )
    args = parser.parse_args()

    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    db_path = os.path.abspath(args.db)

    # Create logs directory if running in background
    os.makedirs(os.path.join(os.path.dirname(__file__), "..", "logs"), exist_ok=True)

    try:
        asyncio.run(
            main_async(
                symbols=symbols,
                exchange_id=args.exchange,
                db_path=db_path,
                ob_depth=args.ob_depth,
                snapshot_interval=args.snapshot_interval,
            )
        )
    except KeyboardInterrupt:
        print("\n[collector] Interrupted.")


if __name__ == "__main__":
    main()
