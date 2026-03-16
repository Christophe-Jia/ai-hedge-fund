import { useState } from 'react';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Input } from '@/components/ui/input';
import { useLiveTrading } from '@/hooks/useLiveTrading';
import { LiveTradingConfig } from '@/services/live-trading-api';
import { Activity, RefreshCw, Play, Square } from 'lucide-react';
import { cn } from '@/lib/utils';
import { AlertPanel } from './AlertPanel';

// ---------------------------------------------------------------------------
// Status badge
// ---------------------------------------------------------------------------
function StatusBadge({ status }: { status: string | undefined }) {
  const label = status ?? 'stopped';
  const colorClass =
    label === 'running'
      ? 'bg-green-500/20 text-green-400 border-green-500/30'
      : label === 'error'
        ? 'bg-red-500/20 text-red-400 border-red-500/30'
        : 'bg-zinc-500/20 text-zinc-400 border-zinc-500/30';

  return (
    <span
      className={cn(
        'inline-flex items-center gap-1.5 px-2 py-0.5 rounded-full border text-xs font-medium',
        colorClass,
      )}
    >
      <span
        className={cn(
          'h-1.5 w-1.5 rounded-full',
          label === 'running' && 'bg-green-400 animate-pulse',
          label === 'error' && 'bg-red-400',
          label === 'stopped' && 'bg-zinc-400',
        )}
      />
      {label.charAt(0).toUpperCase() + label.slice(1)}
    </span>
  );
}

// ---------------------------------------------------------------------------
// Strategy config panel
// ---------------------------------------------------------------------------
function StrategyConfigPanel({
  onStart,
  isLoading,
}: {
  onStart: (cfg: LiveTradingConfig) => void;
  isLoading: boolean;
}) {
  const [market, setMarket] = useState('crypto');
  const [tickers, setTickers] = useState('BTC/USDT');
  const [interval, setInterval] = useState(60);
  const [paper, setPaper] = useState(true);
  const [modelName, setModelName] = useState('gpt-4o');

  const handleStart = () => {
    onStart({
      market,
      tickers: tickers.split(',').map((t) => t.trim()).filter(Boolean),
      interval_minutes: interval,
      paper,
      model_name: modelName,
      model_provider: 'openai',
    });
  };

  return (
    <div className="space-y-3">
      <div className="grid grid-cols-2 gap-3">
        <div>
          <label className="text-xs text-muted-foreground mb-1 block">Market</label>
          <select
            value={market}
            onChange={(e) => setMarket(e.target.value)}
            className="w-full h-8 px-2 text-xs rounded-md border border-input bg-background text-foreground"
          >
            <option value="crypto">Crypto</option>
            <option value="alpaca">Stocks (Alpaca)</option>
          </select>
        </div>
        <div>
          <label className="text-xs text-muted-foreground mb-1 block">Tickers (comma-sep)</label>
          <Input
            value={tickers}
            onChange={(e) => setTickers(e.target.value)}
            className="h-8 text-xs"
            placeholder="BTC/USDT,ETH/USDT"
          />
        </div>
        <div>
          <label className="text-xs text-muted-foreground mb-1 block">Interval (min)</label>
          <Input
            type="number"
            value={interval}
            onChange={(e) => setInterval(Number(e.target.value))}
            className="h-8 text-xs"
            min={1}
          />
        </div>
        <div>
          <label className="text-xs text-muted-foreground mb-1 block">Model</label>
          <Input
            value={modelName}
            onChange={(e) => setModelName(e.target.value)}
            className="h-8 text-xs"
            placeholder="gpt-4o"
          />
        </div>
      </div>
      <div className="flex items-center gap-2">
        <label className="text-xs text-muted-foreground">Paper mode</label>
        <button
          onClick={() => setPaper(!paper)}
          className={cn(
            'relative inline-flex h-4 w-8 items-center rounded-full transition-colors',
            paper ? 'bg-green-500' : 'bg-zinc-600',
          )}
        >
          <span
            className={cn(
              'inline-block h-3 w-3 transform rounded-full bg-white transition-transform',
              paper ? 'translate-x-4' : 'translate-x-1',
            )}
          />
        </button>
        <span className="text-xs text-muted-foreground">{paper ? 'Paper (safe)' : 'LIVE'}</span>
      </div>
      <Button
        size="sm"
        onClick={handleStart}
        disabled={isLoading}
        className="w-full h-8 text-xs gap-1.5"
      >
        <Play size={12} />
        Start Trading
      </Button>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Positions table
// ---------------------------------------------------------------------------
function PositionsTable({ positions }: { positions: Record<string, any> }) {
  const entries = Object.values(positions);
  if (!entries.length) {
    return (
      <p className="text-xs text-muted-foreground py-4 text-center">No open positions</p>
    );
  }
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-xs">
        <thead>
          <tr className="text-muted-foreground border-b border-border">
            <th className="text-left py-1.5 font-medium">Symbol</th>
            <th className="text-right py-1.5 font-medium">Qty</th>
            <th className="text-right py-1.5 font-medium">Market Value</th>
          </tr>
        </thead>
        <tbody>
          {entries.map((pos: any, i) => (
            <tr key={i} className="border-b border-border/50">
              <td className="py-1.5 font-mono">{pos.symbol ?? Object.keys(positions)[i]}</td>
              <td className="py-1.5 text-right font-mono">
                {typeof pos.qty === 'number' ? pos.qty.toFixed(6) : pos.qty ?? '—'}
              </td>
              <td className="py-1.5 text-right font-mono">
                {pos.market_value != null ? `$${Number(pos.market_value).toLocaleString()}` : '—'}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Order history table
// ---------------------------------------------------------------------------
function OrderHistoryTable({ orders }: { orders: any[] }) {
  if (!orders.length) {
    return (
      <p className="text-xs text-muted-foreground py-4 text-center">No orders placed yet</p>
    );
  }
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-xs">
        <thead>
          <tr className="text-muted-foreground border-b border-border">
            <th className="text-left py-1.5 font-medium">Time</th>
            <th className="text-left py-1.5 font-medium">Symbol</th>
            <th className="text-left py-1.5 font-medium">Side</th>
            <th className="text-right py-1.5 font-medium">Qty</th>
            <th className="text-left py-1.5 font-medium">Status</th>
          </tr>
        </thead>
        <tbody>
          {[...orders].reverse().slice(0, 100).map((order: any, i) => (
            <tr key={i} className="border-b border-border/50">
              <td className="py-1.5 text-muted-foreground">
                {order.timestamp
                  ? new Date(order.timestamp).toLocaleTimeString()
                  : '—'}
              </td>
              <td className="py-1.5 font-mono">{order.symbol ?? '—'}</td>
              <td className="py-1.5">
                <span
                  className={cn(
                    'text-xs',
                    order.side === 'buy' ? 'text-green-400' : 'text-red-400',
                  )}
                >
                  {order.side ?? '—'}
                </span>
              </td>
              <td className="py-1.5 text-right font-mono">
                {order.quantity ?? order.qty ?? '—'}
              </td>
              <td className="py-1.5">
                <span
                  className={cn(
                    'text-xs',
                    order.error ? 'text-red-400' : 'text-green-400',
                  )}
                >
                  {order.error ? 'failed' : order.status ?? 'filled'}
                </span>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Main page
// ---------------------------------------------------------------------------
export function LiveTradingPage() {
  const { status, positions, orders, isLoading, error, start, stop, refresh } = useLiveTrading();
  const [activeTab, setActiveTab] = useState<'positions' | 'orders'>('positions');
  const isRunning = status?.status === 'running';

  return (
    <div className="h-full flex flex-col overflow-hidden">
      <div className="flex-1 overflow-y-auto">
        <div className="max-w-4xl mx-auto px-6 pt-6 pb-4 space-y-4">
          {/* Header */}
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-2">
              <Activity size={16} className="text-muted-foreground" />
              <h1 className="text-lg font-semibold text-foreground">Live Trading</h1>
              <StatusBadge status={status?.status} />
            </div>
            <Button
              variant="ghost"
              size="sm"
              onClick={refresh}
              disabled={isLoading}
              className="h-7 px-2 text-xs gap-1"
            >
              <RefreshCw size={11} className={isLoading ? 'animate-spin' : ''} />
              Refresh
            </Button>
          </div>

          {error && (
            <div className="bg-red-500/10 border border-red-500/20 rounded-md px-3 py-2 text-xs text-red-400">
              {error}
            </div>
          )}

          {/* Session info */}
          {isRunning && status?.config && (
            <div className="bg-green-500/5 border border-green-500/20 rounded-md px-3 py-2 text-xs text-muted-foreground flex items-center justify-between">
              <span>
                {status.config.market} · {status.config.tickers?.join(', ')} ·{' '}
                {status.config.interval_minutes}min ·{' '}
                {status.config.paper ? '📄 Paper' : '⚡ Live'}
              </span>
              <Button
                size="sm"
                variant="destructive"
                onClick={stop}
                disabled={isLoading}
                className="h-6 px-2 text-xs gap-1"
              >
                <Square size={10} />
                Stop
              </Button>
            </div>
          )}

          {/* Config panel (shown when stopped) */}
          {!isRunning && (
            <div className="border border-border rounded-md p-4">
              <h2 className="text-sm font-medium text-foreground mb-3">Start Strategy</h2>
              <StrategyConfigPanel onStart={start} isLoading={isLoading} />
            </div>
          )}

          {/* Positions / Orders */}
          <div className="border border-border rounded-md overflow-hidden">
            <div className="flex border-b border-border">
              {(['positions', 'orders'] as const).map((tab) => (
                <button
                  key={tab}
                  onClick={() => setActiveTab(tab)}
                  className={cn(
                    'px-4 py-2 text-xs font-medium transition-colors',
                    activeTab === tab
                      ? 'border-b-2 border-primary text-foreground'
                      : 'text-muted-foreground hover:text-foreground',
                  )}
                >
                  {tab === 'positions' ? 'Positions' : `Orders (${orders.length})`}
                </button>
              ))}
            </div>
            <div className="p-3">
              {activeTab === 'positions' ? (
                <PositionsTable positions={positions} />
              ) : (
                <OrderHistoryTable orders={orders} />
              )}
            </div>
          </div>

          {/* Alert panel */}
          <div className="border border-border rounded-md p-4">
            <AlertPanel sessionId="main" />
          </div>
        </div>
      </div>
    </div>
  );
}
