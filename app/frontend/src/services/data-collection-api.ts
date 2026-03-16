const API_BASE_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';

export interface StoreRow {
  store: string;
  table: string;
  rows: number | null;
  latest_ts: number | null;
  ts_is_seconds: boolean;
  size_bytes: number | null;
}

export interface DataCollectionStatus {
  stores: StoreRow[];
  timestamp: string;
}

export interface ProcessEntry {
  name: string;
  label: string;
  description: string;
  status: 'idle' | 'running' | 'done' | 'error';
  started_at: string | null;
  finished_at: string | null;
  exit_code: number | null;
}

export interface ProcessesResponse {
  processes: ProcessEntry[];
}

export const dataCollectionApi = {
  async getStatus(): Promise<DataCollectionStatus> {
    const res = await fetch(`${API_BASE_URL}/data-collection/status`);
    if (!res.ok) throw new Error(`Failed to get status: ${res.statusText}`);
    return res.json();
  },

  async getProcesses(): Promise<ProcessesResponse> {
    const res = await fetch(`${API_BASE_URL}/data-collection/processes`);
    if (!res.ok) throw new Error(`Failed to get processes: ${res.statusText}`);
    return res.json();
  },

  async stopScript(name: string): Promise<void> {
    const res = await fetch(`${API_BASE_URL}/data-collection/run/${name}`, {
      method: 'DELETE',
    });
    if (!res.ok && res.status !== 409) {
      throw new Error(`Failed to stop script: ${res.statusText}`);
    }
  },

  runScript(
    name: string,
    args: string[],
    onLog: (line: string) => void,
    onComplete: (exitCode: number, status: string) => void,
    onError: (err: string) => void,
  ): () => void {
    const controller = new AbortController();

    (async () => {
      try {
        const res = await fetch(`${API_BASE_URL}/data-collection/run/${name}`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ args }),
          signal: controller.signal,
        });

        if (!res.ok) {
          const body = await res.text();
          onError(body || res.statusText);
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
              if (eventType === 'log') {
                onLog(payload.line ?? dataStr);
              } else if (eventType === 'complete') {
                onComplete(payload.exit_code ?? -1, payload.status ?? 'done');
              }
            } catch {
              // ignore malformed events
            }
          }
        }
      } catch (err: any) {
        if (err?.name !== 'AbortError') {
          onError(err?.message ?? String(err));
        }
      }
    })();

    return () => controller.abort();
  },
};
