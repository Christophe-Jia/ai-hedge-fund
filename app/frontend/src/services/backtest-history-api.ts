const API_BASE_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';

export interface BacktestRunListItem {
  id: number;
  name: string | null;
  engine_type: string;
  created_at: string;
  tickers: string[] | null;
  start_date: string | null;
  end_date: string | null;
  initial_capital: number | null;
  model_name: string | null;
  sharpe_ratio: number | null;
  max_drawdown: number | null;
  total_return: number | null;
}

export interface PortfolioValuePoint {
  Date: string;
  'Portfolio Value': number;
  [key: string]: any;
}

export interface BacktestRunDetail extends BacktestRunListItem {
  portfolio_value_series: PortfolioValuePoint[] | null;
  performance_metrics: Record<string, number | null> | null;
  final_portfolio: any | null;
  selected_analysts: string[] | null;
  extra_params: any | null;
}

export const backtestHistoryApi = {
  async list(limit = 50, offset = 0): Promise<BacktestRunListItem[]> {
    const res = await fetch(
      `${API_BASE_URL}/backtests?limit=${limit}&offset=${offset}`,
    );
    if (!res.ok) throw new Error(`Failed to list backtests: ${res.statusText}`);
    return res.json();
  },

  async get(id: number): Promise<BacktestRunDetail> {
    const res = await fetch(`${API_BASE_URL}/backtests/${id}`);
    if (!res.ok) throw new Error(`Failed to get backtest ${id}: ${res.statusText}`);
    return res.json();
  },

  async delete(id: number): Promise<void> {
    const res = await fetch(`${API_BASE_URL}/backtests/${id}`, { method: 'DELETE' });
    if (!res.ok) throw new Error(`Failed to delete backtest ${id}: ${res.statusText}`);
  },

  async rename(id: number, name: string): Promise<void> {
    const res = await fetch(`${API_BASE_URL}/backtests/${id}/name`, {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name }),
    });
    if (!res.ok) throw new Error(`Failed to rename backtest ${id}: ${res.statusText}`);
  },
};
