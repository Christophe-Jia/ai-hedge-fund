import { useCallback, useRef, useState } from 'react';
import { dataCollectionApi } from '@/services/data-collection-api';

export type ScriptStatus = 'idle' | 'running' | 'done' | 'error';

const MAX_LOGS = 500;
const MAX_HISTORY = 5;

export interface RunRecord {
  startedAt: string; // ISO
  finishedAt: string | null;
  status: 'running' | 'done' | 'error';
  exitCode: number | null;
}

function historyKey(name: string) {
  return `dc_run_history_${name}`;
}

function loadHistory(name: string): RunRecord[] {
  try {
    const raw = localStorage.getItem(historyKey(name));
    return raw ? JSON.parse(raw) : [];
  } catch {
    return [];
  }
}

function saveHistory(name: string, history: RunRecord[]) {
  try {
    localStorage.setItem(historyKey(name), JSON.stringify(history.slice(-MAX_HISTORY)));
  } catch {
    // ignore
  }
}

interface ScriptRunnerState {
  status: ScriptStatus;
  logs: string[];
  exitCode: number | null;
  startedAt: Date | null;
  runHistory: RunRecord[];
}

export function useScriptRunner(scriptName: string) {
  const [state, setState] = useState<ScriptRunnerState>(() => ({
    status: 'idle',
    logs: [],
    exitCode: null,
    startedAt: null,
    runHistory: loadHistory(scriptName),
  }));

  const abortRef = useRef<(() => void) | null>(null);

  const run = useCallback(
    (args: string[] = []) => {
      if (state.status === 'running') return;

      const startedAt = new Date();
      const newRecord: RunRecord = {
        startedAt: startedAt.toISOString(),
        finishedAt: null,
        status: 'running',
        exitCode: null,
      };

      const history = [...loadHistory(scriptName), newRecord];
      saveHistory(scriptName, history);

      setState({
        status: 'running',
        logs: [],
        exitCode: null,
        startedAt,
        runHistory: history.slice(-MAX_HISTORY),
      });

      const abort = dataCollectionApi.runScript(
        scriptName,
        args,
        (line) => {
          setState((prev) => ({
            ...prev,
            logs:
              prev.logs.length >= MAX_LOGS
                ? [...prev.logs.slice(prev.logs.length - MAX_LOGS + 1), line]
                : [...prev.logs, line],
          }));
        },
        (exitCode, status) => {
          const finalStatus = status === 'done' ? 'done' : 'error';
          const finishedAt = new Date().toISOString();

          const updatedHistory = loadHistory(scriptName).map((r, i, arr) =>
            i === arr.length - 1
              ? { ...r, finishedAt, status: finalStatus as 'done' | 'error', exitCode }
              : r
          );
          saveHistory(scriptName, updatedHistory);

          setState((prev) => ({
            ...prev,
            status: finalStatus,
            exitCode,
            runHistory: updatedHistory.slice(-MAX_HISTORY),
          }));
          abortRef.current = null;
        },
        (err) => {
          const finishedAt = new Date().toISOString();
          const updatedHistory = loadHistory(scriptName).map((r, i, arr) =>
            i === arr.length - 1
              ? { ...r, finishedAt, status: 'error' as const, exitCode: -1 }
              : r
          );
          saveHistory(scriptName, updatedHistory);

          setState((prev) => ({
            ...prev,
            status: 'error',
            logs: [...prev.logs, `[error] ${err}`],
            runHistory: updatedHistory.slice(-MAX_HISTORY),
          }));
          abortRef.current = null;
        },
      );

      abortRef.current = abort;
    },
    [scriptName, state.status],
  );

  const stop = useCallback(async () => {
    if (abortRef.current) {
      abortRef.current();
      abortRef.current = null;
    }
    try {
      await dataCollectionApi.stopScript(scriptName);
    } catch {
      // ignore
    }

    const finishedAt = new Date().toISOString();
    const updatedHistory = loadHistory(scriptName).map((r, i, arr) =>
      i === arr.length - 1 && r.status === 'running'
        ? { ...r, finishedAt, status: 'error' as const, exitCode: -1 }
        : r
    );
    saveHistory(scriptName, updatedHistory);

    setState((prev) => ({
      ...prev,
      status: 'idle',
      runHistory: updatedHistory.slice(-MAX_HISTORY),
    }));
  }, [scriptName]);

  return { ...state, run, stop };
}
