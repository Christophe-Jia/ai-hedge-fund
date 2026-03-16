import { Button } from '@/components/ui/button';
import { RefreshCw } from 'lucide-react';
import { StoreRow } from '@/services/data-collection-api';

function formatBytes(bytes: number | null): string {
  if (bytes === null || bytes === undefined) return '—';
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(0)} KB`;
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(1)} GB`;
}

function formatRows(rows: number | null): string {
  if (rows === null || rows === undefined) return '—';
  if (rows >= 1_000_000) return `${(rows / 1_000_000).toFixed(2)}M`;
  if (rows >= 1_000) return `${(rows / 1_000).toFixed(1)}K`;
  return rows.toLocaleString();
}

function getAgeMs(ts: number | null, isSec: boolean): number | null {
  if (ts === null || ts === undefined) return null;
  const ms = isSec ? ts * 1000 : ts;
  return Date.now() - ms;
}

function formatRelativeTime(ageMs: number | null): string {
  if (ageMs === null) return 'never';
  const s = Math.floor(ageMs / 1000);
  if (s < 60) return `${s}s ago`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m ago`;
  const h = Math.floor(m / 60);
  if (h < 24) return `${h}h ago`;
  const d = Math.floor(h / 24);
  return `${d}d ago`;
}

function FreshnessDot({ ageMs }: { ageMs: number | null }) {
  if (ageMs === null) {
    return (
      <span className="inline-flex items-center gap-1.5">
        <span className="w-1.5 h-1.5 rounded-full bg-red-500 inline-block shrink-0" />
        <span className="text-red-400">never</span>
      </span>
    );
  }
  const h = ageMs / (1000 * 60 * 60);
  const dotColor = h < 1 ? 'bg-green-500' : h < 24 ? 'bg-yellow-500' : 'bg-red-500';
  return (
    <span className="inline-flex items-center gap-1.5">
      <span className={`w-1.5 h-1.5 rounded-full ${dotColor} inline-block shrink-0`} />
      <span>{formatRelativeTime(ageMs)}</span>
    </span>
  );
}

function groupByStore(rows: StoreRow[]): Map<string, StoreRow[]> {
  const map = new Map<string, StoreRow[]>();
  for (const row of rows) {
    const existing = map.get(row.store) ?? [];
    existing.push(row);
    map.set(row.store, existing);
  }
  return map;
}

interface StoreStatusTableProps {
  rows: StoreRow[];
  loading: boolean;
  lastRefreshed: Date | null;
  onRefresh: () => void;
}

export function StoreStatusTable({ rows, loading, lastRefreshed, onRefresh }: StoreStatusTableProps) {
  const grouped = groupByStore(rows);
  const now = Date.now();
  const updatedAgo = lastRefreshed
    ? Math.floor((now - lastRefreshed.getTime()) / 1000)
    : null;

  return (
    <div className="border-t border-border shrink-0">
      <div className="px-6 py-3">
        <div className="flex items-center justify-between mb-2">
          <h2 className="text-sm font-semibold text-foreground">Data Stores</h2>
          <div className="flex items-center gap-2">
            {updatedAgo !== null && (
              <span className="text-xs text-muted-foreground">
                Updated {updatedAgo}s ago
              </span>
            )}
            <Button
              variant="ghost"
              size="sm"
              onClick={onRefresh}
              disabled={loading}
              className="h-7 w-7 p-0 text-muted-foreground hover:text-foreground"
            >
              <RefreshCw size={12} className={loading ? 'animate-spin' : ''} />
            </Button>
          </div>
        </div>

        <div className="border border-border rounded-md overflow-hidden">
          <table className="w-full text-xs">
            <thead>
              <tr className="bg-muted/40 border-b border-border">
                <th className="text-left px-3 py-2 text-muted-foreground font-medium">Store</th>
                <th className="text-left px-3 py-2 text-muted-foreground font-medium">Table</th>
                <th className="text-right px-3 py-2 text-muted-foreground font-medium">Rows</th>
                <th className="text-right px-3 py-2 text-muted-foreground font-medium">Latest</th>
                <th className="text-right px-3 py-2 text-muted-foreground font-medium">Size</th>
              </tr>
            </thead>
            <tbody>
              {rows.length === 0 ? (
                <tr>
                  <td colSpan={5} className="px-3 py-4 text-center text-muted-foreground">
                    {loading ? 'Loading…' : 'No data stores found'}
                  </td>
                </tr>
              ) : (
                Array.from(grouped.entries()).flatMap(([storeName, storeRows]) =>
                  storeRows.map((row, idx) => {
                    const ageMs = getAgeMs(row.latest_ts, row.ts_is_seconds);
                    const isStale = row.rows === 0 || ageMs === null;
                    return (
                      <tr
                        key={`${row.store}-${row.table}`}
                        className={`border-b border-border/50 last:border-b-0 ${
                          isStale ? 'bg-red-500/5' : 'hover:bg-muted/20'
                        }`}
                      >
                        <td className="px-3 py-1.5 font-mono text-foreground/80">
                          {idx === 0 ? storeName : ''}
                        </td>
                        <td className="px-3 py-1.5 font-mono text-foreground/60">{row.table}</td>
                        <td className="px-3 py-1.5 text-right tabular-nums text-foreground/80">
                          {formatRows(row.rows)}
                        </td>
                        <td className="px-3 py-1.5 text-right tabular-nums text-foreground/60">
                          <FreshnessDot ageMs={ageMs} />
                        </td>
                        <td className="px-3 py-1.5 text-right tabular-nums text-foreground/60">
                          {formatBytes(row.size_bytes)}
                        </td>
                      </tr>
                    );
                  })
                )
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
