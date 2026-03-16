import { useState, useEffect, useCallback, useRef } from 'react';
import {
  liveTradingApi,
  LiveTradingConfig,
  LiveTradingStatus,
  Position,
  OrderRecord,
} from '@/services/live-trading-api';

const DEFAULT_SESSION = 'main';

export interface UseLiveTradingReturn {
  status: LiveTradingStatus | null;
  positions: Record<string, Position>;
  orders: OrderRecord[];
  snapshot: any;
  isLoading: boolean;
  error: string | null;
  start: (config: LiveTradingConfig) => Promise<void>;
  stop: () => Promise<void>;
  refresh: () => void;
}

export function useLiveTrading(sessionId = DEFAULT_SESSION): UseLiveTradingReturn {
  const [status, setStatus] = useState<LiveTradingStatus | null>(null);
  const [positions, setPositions] = useState<Record<string, Position>>({});
  const [orders, setOrders] = useState<OrderRecord[]>([]);
  const [snapshot, setSnapshot] = useState<any>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const sseCleanupRef = useRef<(() => void) | null>(null);

  // ------------------------------------------------------------------
  // Poll status + positions + orders
  // ------------------------------------------------------------------
  const refresh = useCallback(async () => {
    try {
      const [s, p, o] = await Promise.all([
        liveTradingApi.getStatus(sessionId),
        liveTradingApi.getPositions(sessionId),
        liveTradingApi.getOrders(sessionId),
      ]);
      setStatus(s);
      setPositions(p);
      setOrders(o);
      setError(null);
    } catch (err: any) {
      setError(err?.message ?? 'Failed to fetch status');
    }
  }, [sessionId]);

  // Initial load + polling
  useEffect(() => {
    refresh();
    const timer = setInterval(refresh, 30_000);
    return () => clearInterval(timer);
  }, [refresh]);

  // ------------------------------------------------------------------
  // SSE stream for real-time monitor snapshots
  // ------------------------------------------------------------------
  useEffect(() => {
    const cleanup = liveTradingApi.streamUpdates(
      sessionId,
      (snap) => {
        setSnapshot(snap);
        // Merge positions from snapshot if available
        if (snap?.balance?.positions) {
          const posMap: Record<string, Position> = {};
          for (const p of snap.balance.positions) {
            posMap[p.symbol] = p;
          }
          setPositions(posMap);
        }
      },
      (err) => {
        // SSE errors are non-fatal; we fall back to polling
        console.warn('[useLiveTrading] SSE error:', err);
      },
    );
    sseCleanupRef.current = cleanup;
    return () => {
      cleanup();
      sseCleanupRef.current = null;
    };
  }, [sessionId]);

  // ------------------------------------------------------------------
  // Actions
  // ------------------------------------------------------------------
  const start = useCallback(
    async (config: LiveTradingConfig) => {
      setIsLoading(true);
      setError(null);
      try {
        await liveTradingApi.start({ ...config, session_id: sessionId });
        await refresh();
      } catch (err: any) {
        setError(err?.message ?? 'Failed to start');
        throw err;
      } finally {
        setIsLoading(false);
      }
    },
    [sessionId, refresh],
  );

  const stop = useCallback(async () => {
    setIsLoading(true);
    setError(null);
    try {
      await liveTradingApi.stop(sessionId);
      await refresh();
    } catch (err: any) {
      setError(err?.message ?? 'Failed to stop');
      throw err;
    } finally {
      setIsLoading(false);
    }
  }, [sessionId, refresh]);

  return { status, positions, orders, snapshot, isLoading, error, start, stop, refresh };
}
