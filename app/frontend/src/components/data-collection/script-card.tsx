import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { useScriptRunner, ScriptStatus, RunRecord } from '@/hooks/use-script-runner';
import { Play, Square } from 'lucide-react';
import { StatusDot } from './status-dot';
import { ElapsedTimer } from './elapsed-timer';
import { LogPanel } from './log-panel';
import { DaemonStatsBar } from './daemon-stats';

// ── helpers ──────────────────────────────────────────────────────────────────

function formatRelative(isoOrDate: string | Date | null): string {
  if (!isoOrDate) return 'never';
  const d = typeof isoOrDate === 'string' ? new Date(isoOrDate) : isoOrDate;
  const diffMs = Date.now() - d.getTime();
  const s = Math.floor(diffMs / 1000);
  if (s < 60) return `${s}s ago`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m ago`;
  const h = Math.floor(m / 60);
  if (h < 24) return `${h}h ago`;
  return `${Math.floor(h / 24)}d ago`;
}

function TypeBadge({ type }: { type: 'one-shot' | 'daemon' }) {
  if (type === 'daemon') {
    return (
      <Badge
        variant="outline"
        className="text-purple-400 border-purple-400/40 bg-purple-400/10 text-[10px] px-1.5 h-5"
      >
        daemon
      </Badge>
    );
  }
  return (
    <Badge
      variant="outline"
      className="text-muted-foreground border-border text-[10px] px-1.5 h-5"
    >
      one-shot
    </Badge>
  );
}

function RunButton({ status, onRun, onStop }: { status: ScriptStatus; onRun: () => void; onStop: () => void }) {
  if (status === 'running') {
    return (
      <Button
        variant="outline"
        size="sm"
        onClick={onStop}
        className="h-7 px-3 text-xs gap-1 border-red-500/50 text-red-400 hover:bg-red-500/10"
      >
        <Square size={12} />
        Stop
      </Button>
    );
  }
  return (
    <Button
      variant="outline"
      size="sm"
      onClick={onRun}
      className="h-7 px-3 text-xs gap-1"
    >
      <Play size={12} />
      Run
    </Button>
  );
}

function LastRunLine({ status, startedAt, runHistory }: {
  status: ScriptStatus;
  exitCode?: number | null;
  startedAt: Date | null;
  runHistory: RunRecord[];
}) {
  if (status === 'running') {
    return <ElapsedTimer startedAt={startedAt} running={true} />;
  }

  const last = runHistory.filter((r) => r.status !== 'running').slice(-1)[0];
  if (!last) {
    return <span className="text-xs text-muted-foreground">Last: never</span>;
  }

  const timeAgo = formatRelative(last.finishedAt ?? last.startedAt);
  if (last.status === 'error') {
    return (
      <span className="text-xs text-red-400">
        Last: {timeAgo} · exit {last.exitCode ?? 1}
      </span>
    );
  }
  return (
    <span className="text-xs text-muted-foreground">
      Last: {timeAgo} · exit {last.exitCode ?? 0}
    </span>
  );
}

function cardBorderClass(status: ScriptStatus): string {
  if (status === 'running') return 'border-blue-500/50 bg-blue-500/5';
  if (status === 'error') return 'border-red-500/30 bg-red-500/5';
  return 'border-border';
}

// ── One-shot Script Card ──────────────────────────────────────────────────────

interface ScriptCardProps {
  name: string;
  label: string;
}

export function ScriptCard({ name, label }: ScriptCardProps) {
  const { status, logs, startedAt, runHistory, run, stop } = useScriptRunner(name);

  return (
    <div className={`bg-card border rounded-xl p-4 ${cardBorderClass(status)}`}>
      {/* Top row */}
      <div className="flex items-center justify-between gap-2">
        <div className="flex items-center gap-2 min-w-0">
          <StatusDot status={status} />
          <span className="text-sm font-semibold text-foreground truncate">{label}</span>
        </div>
        <div className="flex items-center gap-2 shrink-0">
          <TypeBadge type="one-shot" />
          <RunButton status={status} onRun={() => run()} onStop={stop} />
        </div>
      </div>

      {/* Second row — timing info */}
      <div className="mt-1.5 ml-4">
        <LastRunLine
          status={status}
          startedAt={startedAt}
          runHistory={runHistory}
        />
      </div>

      {/* Log panel */}
      {logs.length > 0 && <LogPanel lines={logs} maxVisible={10} />}
    </div>
  );
}

// ── Daemon Script Card ────────────────────────────────────────────────────────

interface DaemonScriptCardProps {
  name: string;
  label: string;
}

function RunHistoryItem({ record }: { record: RunRecord }) {
  const start = new Date(record.startedAt);
  const startStr = start.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
  const endStr = record.finishedAt
    ? new Date(record.finishedAt).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
    : 'now';

  const today = new Date();
  const yesterday = new Date(today);
  yesterday.setDate(today.getDate() - 1);
  const isToday = start.toDateString() === today.toDateString();
  const isYesterday = start.toDateString() === yesterday.toDateString();
  const dateLabel = isToday ? 'today' : isYesterday ? 'yesterday' : formatRelative(record.startedAt);

  if (record.status === 'running') {
    return (
      <span className="text-green-400">
        ● {dateLabel} {startStr}–{endStr} (running)
      </span>
    );
  }
  if (record.status === 'error') {
    return (
      <span className="text-red-400">
        ✕ {dateLabel} {startStr}–{endStr}
      </span>
    );
  }
  return (
    <span className="text-muted-foreground">
      ✓ {dateLabel} {startStr}–{endStr}
    </span>
  );
}

export function DaemonScriptCard({ name, label }: DaemonScriptCardProps) {
  const { status, logs, startedAt, runHistory, run, stop } = useScriptRunner(name);
  const isRunning = status === 'running';

  return (
    <div
      className={`bg-card border rounded-xl p-4 col-span-full lg:col-span-2 ${
        isRunning ? 'border-green-500/40 bg-green-500/5' : cardBorderClass(status)
      }`}
    >
      {/* Top row */}
      <div className="flex items-center justify-between gap-2">
        <div className="flex items-center gap-2 min-w-0">
          <StatusDot status={status} size="md" />
          <span className="text-sm font-semibold text-foreground truncate">{label}</span>
          <TypeBadge type="daemon" />
        </div>
        <div className="flex items-center gap-3 shrink-0">
          {isRunning && (
            <ElapsedTimer startedAt={startedAt} running={true} />
          )}
          <RunButton status={status} onRun={() => run()} onStop={stop} />
        </div>
      </div>

      {/* Second row timing (when not running) */}
      {!isRunning && (
        <div className="mt-1.5 ml-5">
          <LastRunLine
            status={status}
            startedAt={startedAt}
            runHistory={runHistory}
          />
        </div>
      )}

      {/* Live stats bar */}
      <DaemonStatsBar lines={logs} running={isRunning} />

      {/* Log panel — default expanded, 20 lines */}
      {logs.length > 0 && (
        <LogPanel lines={logs} maxVisible={20} defaultExpanded={true} />
      )}

      {/* Run history */}
      {runHistory.length > 0 && (
        <div className="border-t border-border/50 mt-3 pt-3">
          <div className="flex items-center gap-2 flex-wrap">
            <span className="text-xs text-muted-foreground shrink-0">Run history:</span>
            {runHistory.slice(-3).map((record, i) => (
              <span key={i} className="text-xs">
                <RunHistoryItem record={record} />
              </span>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
