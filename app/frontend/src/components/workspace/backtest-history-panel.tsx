import { useState, useEffect, useCallback } from 'react';
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Legend,
} from 'recharts';
import { Button } from '@/components/ui/button';
import {
  backtestHistoryApi,
  BacktestRunListItem,
  BacktestRunDetail,
  PortfolioValuePoint,
} from '@/services/backtest-history-api';
import { Trash2, RefreshCw, Tag } from 'lucide-react';
import { cn } from '@/lib/utils';

// Colour palette for chart lines
const LINE_COLORS = [
  '#3b82f6', '#22c55e', '#f59e0b', '#ef4444', '#a855f7',
];

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
function fmtDate(d: string | null): string {
  if (!d) return '—';
  return d.split('T')[0];
}

function fmtNum(n: number | null | undefined, decimals = 2): string {
  if (n == null) return '—';
  return n.toFixed(decimals);
}

// Merge multiple portfolio_value_series into a single array for recharts.
// Each series becomes a key "<id>_<name>" in each data point.
function mergeSeriesForChart(
  details: Map<number, BacktestRunDetail>,
): { date: string; [key: string]: any }[] {
  const dateMap = new Map<string, Record<string, number>>();

  for (const [id, detail] of details.entries()) {
    const key = `run_${id}`;
    for (const pt of detail.portfolio_value_series ?? []) {
      const date = String(pt.Date).split('T')[0];
      if (!dateMap.has(date)) dateMap.set(date, { date });
      dateMap.get(date)![key] = pt['Portfolio Value'];
    }
  }

  return Array.from(dateMap.values()).sort((a, b) =>
    a.date < b.date ? -1 : 1,
  );
}

// ---------------------------------------------------------------------------
// Sub-components
// ---------------------------------------------------------------------------
function MetricsCompareTable({
  runs,
  details,
}: {
  runs: BacktestRunListItem[];
  details: Map<number, BacktestRunDetail>;
}) {
  const metricKeys = ['sharpe_ratio', 'sortino_ratio', 'max_drawdown', 'total_return'];
  const metricLabels: Record<string, string> = {
    sharpe_ratio: 'Sharpe',
    sortino_ratio: 'Sortino',
    max_drawdown: 'Max Drawdown',
    total_return: 'Total Return',
  };

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-xs">
        <thead>
          <tr className="border-b border-border text-muted-foreground">
            <th className="text-left py-1.5 font-medium pr-3">Metric</th>
            {runs.map((r) => (
              <th key={r.id} className="text-right py-1.5 font-medium px-2">
                {r.name ?? `Run #${r.id}`}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {metricKeys.map((mk) => (
            <tr key={mk} className="border-b border-border/50">
              <td className="py-1 text-muted-foreground pr-3">{metricLabels[mk] ?? mk}</td>
              {runs.map((r) => {
                const detail = details.get(r.id);
                const val = detail?.performance_metrics?.[mk];
                return (
                  <td key={r.id} className="py-1 text-right font-mono px-2">
                    {fmtNum(val as number | null)}
                  </td>
                );
              })}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Main panel
// ---------------------------------------------------------------------------
export function BacktestHistoryPanel() {
  const [runs, setRuns] = useState<BacktestRunListItem[]>([]);
  const [selected, setSelected] = useState<Set<number>>(new Set());
  const [details, setDetails] = useState<Map<number, BacktestRunDetail>>(new Map());
  const [loading, setLoading] = useState(false);
  const [editingId, setEditingId] = useState<number | null>(null);
  const [editName, setEditName] = useState('');

  const loadRuns = useCallback(async () => {
    setLoading(true);
    try {
      const data = await backtestHistoryApi.list(50, 0);
      setRuns(data);
    } catch (e) {
      console.error('[BacktestHistoryPanel] load error:', e);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    loadRuns();
  }, [loadRuns]);

  // Load detail (portfolio series + full metrics) when a run is selected
  const toggleSelect = async (id: number) => {
    const next = new Set(selected);
    if (next.has(id)) {
      next.delete(id);
      const nextDetails = new Map(details);
      nextDetails.delete(id);
      setDetails(nextDetails);
    } else {
      if (next.size >= 5) return; // max 5 comparison lines
      next.add(id);
      if (!details.has(id)) {
        try {
          const detail = await backtestHistoryApi.get(id);
          setDetails((prev) => new Map(prev).set(id, detail));
        } catch (e) {
          console.error('[BacktestHistoryPanel] detail load error:', e);
        }
      }
    }
    setSelected(next);
  };

  const handleDelete = async (id: number, e: React.MouseEvent) => {
    e.stopPropagation();
    await backtestHistoryApi.delete(id);
    selected.delete(id);
    setSelected(new Set(selected));
    setDetails((prev) => {
      const m = new Map(prev);
      m.delete(id);
      return m;
    });
    await loadRuns();
  };

  const handleRename = async (id: number) => {
    if (!editName.trim()) return;
    await backtestHistoryApi.rename(id, editName.trim());
    setEditingId(null);
    await loadRuns();
  };

  const selectedRuns = runs.filter((r) => selected.has(r.id));
  const chartData = selectedRuns.length > 0 ? mergeSeriesForChart(details) : [];

  return (
    <div className="space-y-4">
      {/* Header */}
      <div className="flex items-center justify-between">
        <h2 className="text-sm font-medium text-foreground">Backtest History</h2>
        <Button
          variant="ghost"
          size="sm"
          onClick={loadRuns}
          disabled={loading}
          className="h-7 px-2 text-xs gap-1"
        >
          <RefreshCw size={11} className={loading ? 'animate-spin' : ''} />
          Refresh
        </Button>
      </div>

      {/* Run list */}
      <div className="border border-border rounded-md overflow-hidden">
        {runs.length === 0 ? (
          <p className="text-xs text-muted-foreground py-6 text-center">
            No backtest history yet. Run a backtest to save results.
          </p>
        ) : (
          <div className="max-h-48 overflow-y-auto">
            {runs.map((run, idx) => (
              <div
                key={run.id}
                onClick={() => toggleSelect(run.id)}
                className={cn(
                  'flex items-center gap-2 px-3 py-2 cursor-pointer border-b border-border/50 hover:bg-muted/50 transition-colors',
                  selected.has(run.id) && 'bg-muted/30',
                )}
              >
                {/* Colour swatch */}
                <div
                  className="h-2.5 w-2.5 rounded-full flex-shrink-0"
                  style={{
                    background: selected.has(run.id)
                      ? LINE_COLORS[
                          Array.from(selected).indexOf(run.id) % LINE_COLORS.length
                        ]
                      : '#6b7280',
                  }}
                />

                <div className="flex-1 min-w-0">
                  {editingId === run.id ? (
                    <input
                      autoFocus
                      value={editName}
                      onChange={(e) => setEditName(e.target.value)}
                      onBlur={() => handleRename(run.id)}
                      onKeyDown={(e) => e.key === 'Enter' && handleRename(run.id)}
                      onClick={(e) => e.stopPropagation()}
                      className="w-full bg-background border border-input rounded px-1 text-xs"
                    />
                  ) : (
                    <span className="text-xs font-medium truncate">
                      {run.name ?? `Run #${run.id}`}
                    </span>
                  )}
                  <div className="text-xs text-muted-foreground">
                    {run.tickers?.join(', ')} · {fmtDate(run.start_date)}~{fmtDate(run.end_date)} ·{' '}
                    Sharpe {fmtNum(run.sharpe_ratio)}
                  </div>
                </div>

                <div className="flex gap-1 flex-shrink-0">
                  <button
                    onClick={(e) => {
                      e.stopPropagation();
                      setEditingId(run.id);
                      setEditName(run.name ?? '');
                    }}
                    className="p-0.5 text-muted-foreground hover:text-foreground"
                    title="Rename"
                  >
                    <Tag size={11} />
                  </button>
                  <button
                    onClick={(e) => handleDelete(run.id, e)}
                    className="p-0.5 text-muted-foreground hover:text-red-400"
                    title="Delete"
                  >
                    <Trash2 size={11} />
                  </button>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>

      {/* Equity curve chart */}
      {chartData.length > 0 && (
        <div>
          <p className="text-xs text-muted-foreground mb-2">
            Portfolio Value Comparison (select up to 5 runs)
          </p>
          <ResponsiveContainer width="100%" height={200}>
            <LineChart data={chartData}>
              <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.05)" />
              <XAxis
                dataKey="date"
                tick={{ fontSize: 10, fill: '#6b7280' }}
                tickFormatter={(d) => d.slice(5)}
              />
              <YAxis
                tick={{ fontSize: 10, fill: '#6b7280' }}
                tickFormatter={(v) => `$${(v / 1000).toFixed(0)}k`}
              />
              <Tooltip
                contentStyle={{
                  background: '#1c1c1c',
                  border: '1px solid #333',
                  fontSize: 11,
                }}
                formatter={(val: any) => [`$${Number(val).toLocaleString()}`, '']}
              />
              <Legend wrapperStyle={{ fontSize: 10 }} />
              {Array.from(selected).map((id, i) => {
                const run = runs.find((r) => r.id === id);
                return (
                  <Line
                    key={id}
                    type="monotone"
                    dataKey={`run_${id}`}
                    name={run?.name ?? `Run #${id}`}
                    stroke={LINE_COLORS[i % LINE_COLORS.length]}
                    dot={false}
                    strokeWidth={1.5}
                    connectNulls
                  />
                );
              })}
            </LineChart>
          </ResponsiveContainer>
        </div>
      )}

      {/* Metrics comparison table */}
      {selectedRuns.length > 0 && (
        <div>
          <p className="text-xs text-muted-foreground mb-2">Metrics Comparison</p>
          <MetricsCompareTable runs={selectedRuns} details={details} />
        </div>
      )}
    </div>
  );
}
