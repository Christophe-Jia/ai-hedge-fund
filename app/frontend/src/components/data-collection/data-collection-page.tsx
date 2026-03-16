import { Button } from '@/components/ui/button';
import { RefreshCw } from 'lucide-react';
import { useStoreStatus } from '@/hooks/use-store-status';
import { ScriptCard, DaemonScriptCard } from './script-card';
import { StoreStatusTable } from './store-status-table';
import { PolymarketPanel } from './PolymarketPanel';

const ONE_SHOT_SCRIPTS = [
  { name: 'seed_btc_history', label: 'Seed BTC History' },
  { name: 'backfill_perp_ohlcv', label: 'Backfill Perp OHLCV' },
  { name: 'backfill_onchain', label: 'Backfill Onchain' },
  { name: 'collect_macro_data', label: 'Collect Macro Data' },
  { name: 'collect_crypto_data', label: 'Collect Crypto Data' },
] as const;

const DAEMON_SCRIPTS = [
  { name: 'collect_orderbook', label: 'Collect Orderbook' },
  { name: 'collect_polymarket_ticks', label: 'Collect Polymarket Ticks' },
] as const;

function formatUpdatedAgo(lastRefreshed: Date | null): string {
  if (!lastRefreshed) return '';
  const s = Math.floor((Date.now() - lastRefreshed.getTime()) / 1000);
  if (s < 60) return `Updated ${s}s ago`;
  return `Updated ${Math.floor(s / 60)}m ago`;
}

export function DataCollectionPage() {
  const { status, loading, lastRefreshed, refresh } = useStoreStatus();

  return (
    <div className="h-full flex flex-col overflow-hidden">
      {/* Scrollable main area */}
      <div className="flex-1 overflow-y-auto">
        <div className="max-w-4xl mx-auto px-6 pt-6 pb-4">
          {/* Page header */}
          <div className="flex items-center justify-between mb-6">
            <h1 className="text-lg font-semibold text-foreground">Data Collection</h1>
            <div className="flex items-center gap-3">
              {lastRefreshed && (
                <span className="text-xs text-muted-foreground">
                  {formatUpdatedAgo(lastRefreshed)}
                </span>
              )}
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
            </div>
          </div>

          {/* One-shot script cards grid */}
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-3 mb-3">
            {ONE_SHOT_SCRIPTS.map((script) => (
              <ScriptCard key={script.name} name={script.name} label={script.label} />
            ))}
          </div>

          {/* Daemon cards */}
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-3 mb-6">
            {DAEMON_SCRIPTS.map((script) => (
              <DaemonScriptCard key={script.name} name={script.name} label={script.label} />
            ))}
          </div>

          {/* Polymarket signals panel */}
          <div className="border border-border rounded-md p-4">
            <PolymarketPanel />
          </div>
        </div>
      </div>

      {/* Data stores — always visible at bottom */}
      <StoreStatusTable
        rows={status?.stores ?? []}
        loading={loading}
        lastRefreshed={lastRefreshed}
        onRefresh={refresh}
      />
    </div>
  );
}

function formatUpdatedAgo(lastRefreshed: Date | null): string {
  if (!lastRefreshed) return '';
  const s = Math.floor((Date.now() - lastRefreshed.getTime()) / 1000);
  if (s < 60) return `Updated ${s}s ago`;
  return `Updated ${Math.floor(s / 60)}m ago`;
}

export function DataCollectionPage() {
  const { status, loading, lastRefreshed, refresh } = useStoreStatus();

  return (
    <div className="h-full flex flex-col overflow-hidden">
      {/* Scrollable main area */}
      <div className="flex-1 overflow-y-auto">
        <div className="max-w-4xl mx-auto px-6 pt-6 pb-4">
          {/* Page header */}
          <div className="flex items-center justify-between mb-6">
            <h1 className="text-lg font-semibold text-foreground">Data Collection</h1>
            <div className="flex items-center gap-3">
              {lastRefreshed && (
                <span className="text-xs text-muted-foreground">
                  {formatUpdatedAgo(lastRefreshed)}
                </span>
              )}
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
            </div>
          </div>

          {/* One-shot script cards grid */}
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-3 mb-3">
            {ONE_SHOT_SCRIPTS.map((script) => (
              <ScriptCard key={script.name} name={script.name} label={script.label} />
            ))}
          </div>

          {/* Daemon card — full width */}
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-3">
            <DaemonScriptCard name={DAEMON_SCRIPT.name} label={DAEMON_SCRIPT.label} />
          </div>
        </div>
      </div>

      {/* Data stores — always visible at bottom */}
      <StoreStatusTable
        rows={status?.stores ?? []}
        loading={loading}
        lastRefreshed={lastRefreshed}
        onRefresh={refresh}
      />
    </div>
  );
}
