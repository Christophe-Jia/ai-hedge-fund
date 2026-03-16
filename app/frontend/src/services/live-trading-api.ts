const API_BASE_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';

export interface LiveTradingConfig {
  market: string;
  tickers: string[];
  interval_minutes: number;
  paper: boolean;
  model_name: string;
  model_provider: string;
  exchange_id?: string;
  session_id?: string;
}

export interface LiveTradingStatus {
  session_id: string;
  status: 'stopped' | 'running' | 'error';
  started_at: string | null;
  error_message: string | null;
  config: Partial<LiveTradingConfig> | null;
}

export interface Position {
  symbol: string;
  qty: number;
  market_value: number | null;
}

export interface OrderRecord {
  id?: string;
  symbol: string;
  side?: string;
  quantity?: number;
  status?: string;
  timestamp?: string;
  confidence?: number;
  error?: string;
  [key: string]: any;
}

export const liveTradingApi = {
  async start(config: LiveTradingConfig): Promise<{ message: string; session_id: string }> {
    const res = await fetch(`${API_BASE_URL}/live-trading/start`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(config),
    });
    if (!res.ok) {
      const body = await res.json().catch(() => ({}));
      throw new Error(body.detail || res.statusText);
    }
    return res.json();
  },

  async stop(sessionId = 'main'): Promise<void> {
    const res = await fetch(
      `${API_BASE_URL}/live-trading/stop?session_id=${encodeURIComponent(sessionId)}`,
      { method: 'DELETE' },
    );
    if (!res.ok && res.status !== 404) {
      throw new Error(`Failed to stop: ${res.statusText}`);
    }
  },

  async getStatus(sessionId = 'main'): Promise<LiveTradingStatus> {
    const res = await fetch(
      `${API_BASE_URL}/live-trading/status?session_id=${encodeURIComponent(sessionId)}`,
    );
    if (!res.ok) throw new Error(`Failed to get status: ${res.statusText}`);
    return res.json();
  },

  async getPositions(sessionId = 'main'): Promise<Record<string, Position>> {
    const res = await fetch(
      `${API_BASE_URL}/live-trading/positions?session_id=${encodeURIComponent(sessionId)}`,
    );
    if (!res.ok) throw new Error(`Failed to get positions: ${res.statusText}`);
    const data = await res.json();
    return data.positions || {};
  },

  async getOrders(sessionId = 'main'): Promise<OrderRecord[]> {
    const res = await fetch(
      `${API_BASE_URL}/live-trading/orders?session_id=${encodeURIComponent(sessionId)}`,
    );
    if (!res.ok) throw new Error(`Failed to get orders: ${res.statusText}`);
    const data = await res.json();
    return data.orders || [];
  },

  streamUpdates(
    sessionId = 'main',
    onSnapshot: (snapshot: any) => void,
    onError?: (err: string) => void,
  ): () => void {
    const controller = new AbortController();
    const url = `${API_BASE_URL}/live-trading/stream?session_id=${encodeURIComponent(sessionId)}`;

    (async () => {
      try {
        const res = await fetch(url, { signal: controller.signal });
        if (!res.ok) {
          onError?.(`Stream failed: ${res.statusText}`);
          return;
        }
        const reader = res.body!.getReader();
        const decoder = new TextDecoder();
        let buffer = '';
        while (true) {
          const { done, value } = await reader.read();
          if (done) break;
          buffer += decoder.decode(value, { stream: true });
          const blocks = buffer.split('\n\n');
          buffer = blocks.pop() ?? '';
          for (const block of blocks) {
            if (!block.trim() || block.startsWith(':')) continue;
            for (const line of block.split('\n')) {
              if (line.startsWith('data: ')) {
                try {
                  const payload = JSON.parse(line.slice(6));
                  onSnapshot(payload);
                } catch {
                  // ignore malformed
                }
              }
            }
          }
        }
      } catch (err: any) {
        if (err?.name !== 'AbortError') {
          onError?.(err?.message ?? String(err));
        }
      }
    })();

    return () => controller.abort();
  },
};
