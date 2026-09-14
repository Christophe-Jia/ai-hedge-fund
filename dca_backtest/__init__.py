"""DCA (定投) backtesting framework for QQQ / VOO / TQQQ with realistic costs.

Modules:
    config   -- instrument cost specs and backtest configuration
    data     -- price data fetching / caching / 40-year series construction
    strategy -- pluggable strategy interface (returns target weights)
    engine   -- daily NAV simulation with monthly RMB contributions
    metrics  -- XIRR, max drawdown, rolling target-hit probability
    run      -- CLI entry point
"""
