# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

### Python (Poetry)
```bash
# Install dependencies
poetry install

# Run all tests
poetry run pytest

# Run a single test
poetry run pytest tests/backtesting/test_portfolio.py::test_apply_long_buy_basic -v

# Lint and format
poetry run black src/
poetry run isort src/
poetry run flake8 src/
```

### CLI Entry Points
```bash
# Run hedge fund analysis (CLI)
poetry run python src/main.py --ticker AAPL,MSFT,NVDA

# Run backtester
poetry run backtester --tickers AAPL,MSFT --start-date 2024-01-01 --end-date 2024-12-31

# Run live trading (paper mode)
poetry run live-trading --market crypto --tickers BTC/USDT --interval 60 --paper
```

### Web Application
```bash
# Start both frontend and backend
./run.sh

# Backend only (FastAPI, port 8000)
cd app/backend && poetry run uvicorn main:app --reload

# Frontend only (Vite, port 5173)
cd app/frontend && npm install && npm run dev
```

## Architecture

The system has three interfaces built on a shared agent/data layer:

### 1. Multi-Agent Graph (Core)

The main LangGraph workflow in `src/main.py` and `src/graph/`:
- **State**: `AgentState` (TypedDict in `src/graph/state.py`) carries messages, `data` (portfolio/signals), and `metadata`
- **Flow**: `start_node` → parallel analyst agents → `risk_management_agent` → `portfolio_manager` → END
- **Analysts** write to `data["analyst_signals"][agent_id][ticker] = {signal, confidence}`
- **Portfolio manager** reads all signals + risk data and outputs final `decisions`

18 agents live in `src/agents/`: famous investor personas (warren_buffett, charlie_munger, etc.) plus analysis agents (fundamentals, technicals, sentiment, valuation). Crypto-specific agents are in `src/agents/crypto/`. Agent registry is in `src/utils/analysts.py`.

### 2. Backtesting Engine (`src/backtesting/`)

`BacktestEngine` in `engine.py` orchestrates:
- `_prefetch_data()` — fetches prices for all tickers + SPY benchmark via `get_price_data()`
- `run_backtest()` — iterates dates, calls agent, executes trades, returns `PerformanceMetrics`

Supporting modules: `portfolio.py` (Portfolio state), `trader.py` (TradeExecutor), `metrics.py` (Sharpe/Sortino/drawdown), `benchmarks.py` (SPY comparison).

**Testing**: `tests/backtesting/integration/conftest.py` monkeypatches all API calls to load from JSON fixtures at `tests/fixtures/api/`. The fixtures directory is currently empty — integration tests will fail until fixtures are created.

### 3. Web Application (`app/`)

- **Backend** (`app/backend/`): FastAPI + SQLAlchemy. Key route: `routes/hedge_fund.py` streams agent decisions via SSE. `services/graph.py` converts a React Flow graph (from frontend) into a LangGraph workflow.
- **Frontend** (`app/frontend/`): React + Vite + React Flow visual graph builder. Users construct agent pipelines visually.

### 4. Live Trading (`src/live/`, `src/trading/`)

`LiveTradingScheduler` runs agent workflows on a schedule. `src/trading/executor.py` routes orders to either CCXT (crypto) or Alpaca (US stocks).

### 5. Data Layer (`src/data/`, `src/tools/`)

- `src/tools/api.py`: `get_prices()`, `get_financial_metrics()`, `get_company_news()`, `get_insider_trades()` — all backed by financialdatasets.ai. Free tickers: AAPL, GOOGL, MSFT, NVDA, TSLA.
- `src/data/crypto.py`: `get_crypto_ticker(symbol)` and `get_crypto_prices(symbol, start, end)` via CCXT/Binance (no API key needed for public endpoints).
- `src/data/cache.py`: In-memory cache layer used by `src/tools/api.py`.

## Security Rules (PUBLIC REPO — read before every commit)

This repository is **publicly visible**. The following rules are non-negotiable:

### Never commit secrets
- API keys, tokens, passwords, secrets must NEVER appear in any committed file
- This includes: `FINANCIAL_DATASETS_API_KEY`, `OPENAI_API_KEY`, `ANTHROPIC_API_KEY`,
  `GROQ_API_KEY`, `CCXT_API_KEY`, `CCXT_API_SECRET`, `ALPACA_API_KEY`,
  `ALPACA_SECRET_KEY`, `GITHUB_TOKEN`, any personal access tokens
- `.env` is gitignored — keep all secrets there, never anywhere else

### GitHub Actions secrets
- Secrets used in workflows must be stored in **GitHub Actions Secrets**
  (repo Settings → Secrets and variables → Actions), referenced as
  `${{ secrets.MY_SECRET }}` — never hardcoded in `.yml` files
- The current workflows use only public endpoints and require no secrets;
  if exchange API keys are ever needed, add them as GitHub Secrets only

### Safe patterns
```python
# CORRECT — read from environment
api_key = os.environ.get("MY_API_KEY", "")

# WRONG — never hardcode
api_key = "sk-abc123..."
```

### Git remote URL
- Never use token-embedded URLs like `https://<token>@github.com/...` in
  committed config. Use them only in the local terminal session, never in
  any file tracked by git (including shell scripts, Makefiles, docs)

### Pre-commit checklist
Before every `git add` / `git commit`:
1. Run `git diff --cached` and scan for any key-like strings (long random alphanumerics)
2. Confirm no `.env` file or credential file is staged (`git status`)
3. Confirm no hardcoded secrets in newly added scripts under `scripts/`

### If a secret is accidentally committed
1. Immediately rotate/revoke the exposed key (GitHub, OpenAI, exchange, etc.)
2. Remove from history: `git filter-repo` or contact the service provider
3. Force-push only as a last resort and with awareness of downstream clones

---

## Known Issues

- **`calculate_adx()` in `src/agents/technicals.py` mutates its input DataFrame** — adds 10+ columns (`high_low`, `tr`, `+di`, `-di`, `adx`, etc.) as side effects.
- **`tests/fixtures/api/` is empty** — all integration tests in `tests/backtesting/integration/` will fail with `AssertionError: Missing price fixture for {ticker}`.
- **`calculate_trend_signals()`** requires at least 55 data points (uses EMA-55).

## Environment Variables

Copy `.env.example` to `.env`. Key variables:
```
FINANCIAL_DATASETS_API_KEY=   # Required for non-free tickers
OPENAI_API_KEY=               # or ANTHROPIC_API_KEY / GROQ_API_KEY
CCXT_EXCHANGE=binance         # Default crypto exchange
CCXT_API_KEY=                 # Only needed for order execution
ALPACA_API_KEY=               # Only needed for US stock live trading
```
