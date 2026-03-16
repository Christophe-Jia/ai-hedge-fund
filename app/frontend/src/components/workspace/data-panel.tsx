import { useCallback } from 'react';
import { Button } from '@/components/ui/button';
import { Play, RefreshCw } from 'lucide-react';
import { useStoreStatus } from '@/hooks/use-store-status';
import { ScriptCard, DaemonScriptCard } from '@/components/data-collection/script-card';
import { StoreStatusTable } from '@/components/data-collection/store-status-table';
import { dataCollectionApi } from '@/services/data-collection-api';

const ONE_SHOT_SCRIPTS = [
  { name: 'seed_btc_history', label: 'Seed BTC History' },
  { name: 'backfill_perp_ohlcv', label: 'Backfill Perp OHLCV' },
  { name: 'backfill_onchain', label: 'Backfill Onchain' },
  { name: 'collect_macro_data', label: 'Collect Macro Data' },
  { name: 'collect_crypto_data', label: 'Collect Crypto Data' },
] as const;

const DAEMON_SCRIPT = { name: 'collect_orderbook', label: 'Collect Orderbook' } as const;

const ALL_SCRIPTS = [
  ...ONE_SHOT_SCRIPTS.map((s) => s.name),
  DAEMON_SCRIPT.name,
];

function ResumeAllButton() {
  const handleResumeAll = useCallback(async () => {
    for (const name of ALL_SCRIPTS) {
      try {
        // We trigger each script via POST; the backend handles idempotency.
        // We check status by attempting to run — if already running it's a no-op server-side.
        dataCollectionApi.runScript(
          name,
          [],
          () => {},
          () => {},
          () => {},
        );
      } catch {
        // continue with others
      }
    }
  }, []);

  return (
    <Button
      variant="default"
      size="sm"
      onClick={handleResumeAll}
      className="h-7 px-3 text-xs gap-1.5"
    >
      <Play size={12} />
      Resume All
    </Button>
  );
}

export function DataPanel() {
  const { status, loading, lastRefreshed, refresh } = useStoreStatus();

  return (
    <div className="h-full flex flex-col overflow-hidden">
      {/* Scrollable top section */}
      <div className="flex-1 overflow-y-auto">
        <div className="px-4 pt-4 pb-3">
          {/* Header */}
          <div className="flex items-center justify-between mb-4">
            <h2 className="text-sm font-semibold text-foreground">数据采集</h2>
            <div className="flex items-center gap-2">
              <Button
                variant="outline"
                size="sm"
                onClick={refresh}
                disabled={loading}
                className="h-7 px-3 text-xs gap-1.5"
              >
                <RefreshCw size={12} className={loading ? 'animate-spin' : ''} />
                Refresh
              </Button>
              <ResumeAllButton />
            </div>
          </div>

          {/* Daemon — full width */}
          <div className="mb-3">
            <DaemonScriptCard name={DAEMON_SCRIPT.name} label={DAEMON_SCRIPT.label} />
          </div>

          {/* One-shot scripts section */}
          <div className="text-xs font-semibold uppercase tracking-wide text-muted-foreground mb-2">
            One-shot 脚本
          </div>
          <div className="grid grid-cols-1 gap-2">
            {ONE_SHOT_SCRIPTS.map((script) => (
              <ScriptCard key={script.name} name={script.name} label={script.label} />
            ))}
          </div>
        </div>
      </div>

      {/* Data stores — pinned at bottom */}
      <StoreStatusTable
        rows={status?.stores ?? []}
        loading={loading}
        lastRefreshed={lastRefreshed}
        onRefresh={refresh}
      />
    </div>
  );
}
