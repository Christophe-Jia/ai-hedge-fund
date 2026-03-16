import { useMemo } from 'react';

export interface DaemonStats {
  tradesPerSec: number | null;
  latestPrice: number | null;
  obDepth: number | null;
}

export function parseDaemonStats(lines: string[]): DaemonStats {
  const recent = lines.slice(-50);

  // Parse latest price from lines like: trade buy $84,210.5 or trade sell $84,208.0
  let latestPrice: number | null = null;
  for (let i = recent.length - 1; i >= 0; i--) {
    const m = recent[i].match(/\$([0-9,]+(?:\.[0-9]+)?)/);
    if (m) {
      latestPrice = parseFloat(m[1].replace(/,/g, ''));
      break;
    }
  }

  // Parse ob depth from lines like: depth=50 or ob: 50 levels
  let obDepth: number | null = null;
  for (let i = recent.length - 1; i >= 0; i--) {
    const m = recent[i].match(/depth=(\d+)/i) || recent[i].match(/ob[:\s]+(\d+)\s*levels?/i);
    if (m) {
      obDepth = parseInt(m[1], 10);
      break;
    }
  }

  // Count trade lines in last 5 seconds window (use last ~10 lines as proxy for rate)
  const tradeLines = recent.filter((l) => /trade\s+(buy|sell)/i.test(l));
  // Estimate trades/sec from last 10 trade lines timestamps if available
  // Fall back to: count of trade lines in last 50 / assumed 10s window
  const tradesPerSec = tradeLines.length > 0 ? parseFloat((tradeLines.length / 10).toFixed(1)) : null;

  return { tradesPerSec, latestPrice, obDepth };
}

interface DaemonStatsBarProps {
  lines: string[];
  running: boolean;
}

export function DaemonStatsBar({ lines, running }: DaemonStatsBarProps) {
  const stats = useMemo(() => parseDaemonStats(lines), [lines]);

  if (!running) return null;

  return (
    <div className="bg-muted/30 rounded-lg px-4 py-2 mt-3 flex gap-6 text-xs font-mono">
      <span className="text-muted-foreground">
        trades/sec:{' '}
        <span className="text-foreground font-semibold">
          {stats.tradesPerSec !== null ? stats.tradesPerSec : '—'}
        </span>
      </span>
      <span className="text-muted-foreground">
        latest:{' '}
        <span className="text-foreground font-semibold">
          {stats.latestPrice !== null ? `$${stats.latestPrice.toLocaleString()} BTC` : '—'}
        </span>
      </span>
      <span className="text-muted-foreground">
        ob depth:{' '}
        <span className="text-foreground font-semibold">
          {stats.obDepth !== null ? stats.obDepth : '—'}
        </span>
      </span>
    </div>
  );
}
