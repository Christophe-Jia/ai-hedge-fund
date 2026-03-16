import { useState, useEffect, useCallback } from 'react';
import { LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer } from 'recharts';
import { Button } from '@/components/ui/button';
import { RefreshCw, TrendingDown, Activity, Minus } from 'lucide-react';
import { cn } from '@/lib/utils';

const API_BASE_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';

interface Market {
  token_id: string;
  condition_id: string;
  question: string;
  last_seen: number;
}

interface TickPoint {
  ts: number;
  price: number;
}

interface SignalResult {
  signal: string;
  confidence: number;
  data_available: boolean;
  reasoning?: {
    risk_level?: string;
    markets_scanned?: number;
    top_markets?: any[];
  };
}

// ---------------------------------------------------------------------------
// Signal badge
// ---------------------------------------------------------------------------
function SignalBadge({ signal, confidence }: { signal: string; confidence: number }) {
  const isBearish = signal === 'bearish';
  const isNeutral = signal === 'neutral';
  return (
    <span
      className={cn(
        'inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-medium border',
        isBearish && 'bg-red-500/15 text-red-400 border-red-500/25',
        isNeutral && 'bg-zinc-500/15 text-zinc-400 border-zinc-500/25',
        !isBearish && !isNeutral && 'bg-green-500/15 text-green-400 border-green-500/25',
      )}
    >
      {isBearish ? <TrendingDown size={11} /> : isNeutral ? <Minus size={11} /> : <Activity size={11} />}
      {signal.charAt(0).toUpperCase() + signal.slice(1)}{' '}
      {confidence > 0 && <span className="opacity-70">({confidence}%)</span>}
    </span>
  );
}

// ---------------------------------------------------------------------------
// Mini price chart for a single market
// ---------------------------------------------------------------------------
function MiniPriceChart({ tokenId }: { tokenId: string }) {
  const [ticks, setTicks] = useState<TickPoint[]>([]);

  useEffect(() => {
    fetch(`${API_BASE_URL}/data-collection/polymarket/markets/${encodeURIComponent(tokenId)}/ticks`)
      .then((r) => r.ok ? r.json() : null)
      .then((d) => d && setTicks(d.ticks || []))
      .catch(() => {});
  }, [tokenId]);

  if (!ticks.length) return <div className="h-10 flex items-center justify-center text-xs text-muted-foreground">No data</div>;

  const data = ticks.map((t) => ({ time: t.ts, price: Math.round(t.price * 100) }));

  return (
    <ResponsiveContainer width="100%" height={36}>
      <LineChart data={data}>
        <YAxis domain={[0, 100]} hide />
        <XAxis dataKey="time" hide />
        <Tooltip
          contentStyle={{ background: '#1c1c1c', border: '1px solid #333', fontSize: 10 }}
          formatter={(v: any) => [`${v}%`, 'Prob']}
        />
        <Line type="monotone" dataKey="price" stroke="#f59e0b" dot={false} strokeWidth={1.2} />
      </LineChart>
    </ResponsiveContainer>
  );
}

// ---------------------------------------------------------------------------
// Main panel
// ---------------------------------------------------------------------------
export function PolymarketPanel() {
  const [markets, setMarkets] = useState<Market[]>([]);
  const [signal, setSignal] = useState<SignalResult | null>(null);
  const [loading, setLoading] = useState(false);

  const refresh = useCallback(async () => {
    setLoading(true);
    try {
      const [mRes, sRes] = await Promise.all([
        fetch(`${API_BASE_URL}/data-collection/polymarket/markets`),
        fetch(`${API_BASE_URL}/data-collection/polymarket/signal`),
      ]);
      if (mRes.ok) {
        const d = await mRes.json();
        setMarkets(d.markets || []);
      }
      if (sRes.ok) {
        const d = await sRes.json();
        setSignal(d.signal?.['BTC/USDT'] ?? d.signal ?? null);
      }
    } catch (e) {
      console.error('[PolymarketPanel] refresh error:', e);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    refresh();
  }, [refresh]);

  return (
    <div className="space-y-3">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <h3 className="text-sm font-medium text-foreground">Polymarket Signals</h3>
          {signal && (
            <SignalBadge
              signal={signal.signal}
              confidence={signal.confidence}
            />
          )}
        </div>
        <Button
          variant="ghost"
          size="sm"
          onClick={refresh}
          disabled={loading}
          className="h-6 px-2 text-xs gap-1"
        >
          <RefreshCw size={10} className={loading ? 'animate-spin' : ''} />
          Refresh
        </Button>
      </div>

      {/* Signal details */}
      {signal?.reasoning && (
        <div className="text-xs text-muted-foreground flex gap-3">
          <span>Risk: {signal.reasoning.risk_level ?? '—'}</span>
          <span>Markets scanned: {signal.reasoning.markets_scanned ?? 0}</span>
          {!signal.data_available && (
            <span className="text-amber-500">No recent data</span>
          )}
        </div>
      )}

      {/* Market list */}
      {markets.length === 0 ? (
        <p className="text-xs text-muted-foreground py-3 text-center">
          No Polymarket data yet. Start the <strong>Collect Polymarket Ticks</strong> daemon above.
        </p>
      ) : (
        <div className="space-y-2">
          {markets.slice(0, 8).map((m) => (
            <div key={m.token_id} className="border border-border rounded-md p-2.5">
              <div className="flex items-start justify-between gap-2">
                <p className="text-xs text-foreground line-clamp-2 flex-1">{m.question}</p>
              </div>
              <MiniPriceChart tokenId={m.token_id} />
            </div>
          ))}
          {markets.length > 8 && (
            <p className="text-xs text-muted-foreground text-center">
              +{markets.length - 8} more markets
            </p>
          )}
        </div>
      )}
    </div>
  );
}
