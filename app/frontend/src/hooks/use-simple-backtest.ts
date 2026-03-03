import { useCallback, useRef, useState } from 'react';
import { ModelProvider, BacktestRequest, GraphNode, GraphEdge } from '@/services/types';

const API_BASE_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';

export type BacktestRunStatus = 'idle' | 'running' | 'complete' | 'error';

export interface SimpleBacktestConfig {
  assetType: 'equities' | 'crypto';
  tickers: string;
  startDate: string;
  endDate: string;
  initialCapital: number;
  modelName: string;
  modelProvider: string;
  selectedFactors: string[];
  slippageBps: number;
}

export interface SimpleBacktestResult {
  sharpe_ratio: number | null;
  sortino_ratio: number | null;
  max_drawdown: number | null;
  total_return: number | null;
  final_portfolio_value: number | null;
  initial_capital: number | null;
  total_days: number | null;
}

interface BacktestState {
  status: BacktestRunStatus;
  result: SimpleBacktestResult | null;
  error: string | null;
  progressMessages: string[];
}

// graph.py's extract_base_agent_key() strips a trailing _XXXXXX (6 lower-alnum chars) suffix.
// Every node that is sent to the backend must therefore have a 7th+ char last segment
// OR a deliberate suffix appended.  We always append a fixed suffix to all node IDs so that:
//   extract_base_agent_key("portfolio_manager_wbpmgr") → "portfolio_manager" ✓
//   extract_base_agent_key("ben_graham_node01")        → "ben_graham"        ✓
// Without the suffix, IDs like "ben_graham" are incorrectly stripped to "ben".
const PM_ID = 'portfolio_manager_wbpmgr';
const FACTOR_SUFFIX = 'node01';

function buildBacktestRequest(config: SimpleBacktestConfig): BacktestRequest {
  const tickers = config.tickers
    .split(',')
    .map((t) => t.trim().toUpperCase())
    .filter(Boolean);

  // Each factor gets a stable suffixed ID so extract_base_agent_key() returns the correct key.
  const factorNodeIds = config.selectedFactors.map((factor) => `${factor}_${FACTOR_SUFFIX}`);

  // Minimal graph structure: portfolio-start (UI marker), portfolio manager, and factor agents.
  // Factor nodes must NOT have incoming edges so graph.py auto-wires start_node → each factor.
  const graphNodes: GraphNode[] = [
    { id: 'portfolio-start', type: 'portfolioStart', position: { x: 0, y: 0 } },
    { id: PM_ID, type: 'portfolioManager', position: { x: 800, y: 0 } },
    ...config.selectedFactors.map((factor, idx) => ({
      id: `${factor}_${FACTOR_SUFFIX}`,
      type: 'agentNode',
      position: { x: 200 + idx * 50, y: idx * 80 },
      data: { agentId: factor },
    })),
  ];

  // Only wire factor → portfolio manager. Do NOT add portfolio-start → factor edges:
  // graph.py treats any node without an incoming edge as auto-wired from start_node.
  const graphEdges: GraphEdge[] = [
    ...factorNodeIds.map((factorId) => ({
      id: `edge-${factorId}-pm`,
      source: factorId,
      target: PM_ID,
    })),
  ];

  return {
    tickers,
    start_date: config.startDate,
    end_date: config.endDate,
    initial_capital: config.initialCapital,
    graph_nodes: graphNodes,
    graph_edges: graphEdges,
    model_name: config.modelName,
    model_provider: config.modelProvider as ModelProvider,
    margin_requirement: 0,
  };
}

export function useSimpleBacktest() {
  const [state, setState] = useState<BacktestState>({
    status: 'idle',
    result: null,
    error: null,
    progressMessages: [],
  });

  const abortRef = useRef<AbortController | null>(null);

  const run = useCallback((config: SimpleBacktestConfig) => {
    if (state.status === 'running') return;

    const controller = new AbortController();
    abortRef.current = controller;

    setState({
      status: 'running',
      result: null,
      error: null,
      progressMessages: [],
    });

    const request = buildBacktestRequest(config);

    (async () => {
      try {
        const res = await fetch(`${API_BASE_URL}/hedge-fund/backtest`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(request),
          signal: controller.signal,
        });

        if (!res.ok) {
          const body = await res.text();
          setState((prev) => ({
            ...prev,
            status: 'error',
            error: body || `HTTP ${res.status}`,
          }));
          return;
        }

        const reader = res.body!.getReader();
        const decoder = new TextDecoder();
        let buffer = '';
        let initialCapital = config.initialCapital;

        while (true) {
          const { done, value } = await reader.read();
          if (done) break;
          buffer += decoder.decode(value, { stream: true });

          const blocks = buffer.split('\n\n');
          buffer = blocks.pop() ?? '';

          for (const block of blocks) {
            if (!block.trim()) continue;
            const lines = block.split('\n');
            let eventType = '';
            let dataStr = '';
            for (const l of lines) {
              if (l.startsWith('event: ')) eventType = l.slice(7).trim();
              if (l.startsWith('data: ')) dataStr = l.slice(6).trim();
            }
            if (!dataStr) continue;

            try {
              const payload = JSON.parse(dataStr);

              if (eventType === 'progress') {
                const msg =
                  payload.status
                    ? `[${payload.agent ?? 'backtest'}] ${payload.status}`
                    : null;
                if (msg) {
                  setState((prev) => ({
                    ...prev,
                    progressMessages: [...prev.progressMessages.slice(-49), msg],
                  }));
                }
              } else if (eventType === 'complete') {
                const data = payload.data ?? payload;
                const pm = data.performance_metrics ?? {};
                const fp = data.final_portfolio ?? {};
                const totalReturn =
                  fp.total_value != null && initialCapital > 0
                    ? ((fp.total_value - initialCapital) / initialCapital) * 100
                    : null;

                const result: SimpleBacktestResult = {
                  sharpe_ratio: pm.sharpe_ratio ?? null,
                  sortino_ratio: pm.sortino_ratio ?? null,
                  max_drawdown: pm.max_drawdown ?? null,
                  total_return: totalReturn,
                  final_portfolio_value: fp.total_value ?? null,
                  initial_capital: initialCapital,
                  total_days: data.total_days ?? null,
                };

                setState((prev) => ({
                  ...prev,
                  status: 'complete',
                  result,
                }));
                abortRef.current = null;
                return;
              } else if (eventType === 'error') {
                setState((prev) => ({
                  ...prev,
                  status: 'error',
                  error: payload.message ?? 'Backtest failed',
                }));
                abortRef.current = null;
                return;
              }
            } catch {
              // ignore malformed events
            }
          }
        }

        // Stream ended without complete event
        setState((prev) => {
          if (prev.status === 'running') {
            return { ...prev, status: 'error', error: 'Stream ended unexpectedly' };
          }
          return prev;
        });
      } catch (err: any) {
        if (err?.name === 'AbortError') return;
        setState((prev) => ({
          ...prev,
          status: 'error',
          error: err?.message ?? String(err),
        }));
      } finally {
        abortRef.current = null;
      }
    })();
  }, [state.status]);

  const stop = useCallback(() => {
    if (abortRef.current) {
      abortRef.current.abort();
      abortRef.current = null;
    }
    setState((prev) => ({ ...prev, status: 'idle' }));
  }, []);

  const reset = useCallback(() => {
    setState({ status: 'idle', result: null, error: null, progressMessages: [] });
  }, []);

  return { ...state, run, stop, reset };
}
