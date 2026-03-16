import { useState } from 'react';
import { BacktestPanel } from './backtest-panel';
import { DataPanel } from './data-panel';
import { BacktestHistoryPanel } from './backtest-history-panel';
import { ChevronDown, ChevronRight } from 'lucide-react';

export function WorkspacePage() {
  const [historyOpen, setHistoryOpen] = useState(false);

  return (
    <div className="h-full w-full flex overflow-hidden bg-background">
      {/* Left: Backtest panel — fixed width */}
      <div className="w-[480px] shrink-0 overflow-y-auto">
        <BacktestPanel />

        {/* Collapsible history panel below backtest panel */}
        <div className="border-t border-border">
          <button
            onClick={() => setHistoryOpen(!historyOpen)}
            className="w-full flex items-center gap-2 px-4 py-2.5 text-xs font-medium text-muted-foreground hover:text-foreground transition-colors"
          >
            {historyOpen ? <ChevronDown size={12} /> : <ChevronRight size={12} />}
            Backtest History
          </button>
          {historyOpen && (
            <div className="px-4 pb-4">
              <BacktestHistoryPanel />
            </div>
          )}
        </div>
      </div>

      {/* Right: Data collection panel — flex-1 */}
      <div className="flex-1 min-w-0 overflow-hidden">
        <DataPanel />
      </div>
    </div>
  );
}
