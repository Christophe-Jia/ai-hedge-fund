import { cn } from '@/lib/utils';
import { SimpleBacktestResult } from '@/hooks/use-simple-backtest';

interface BacktestResultsInlineProps {
  result: SimpleBacktestResult;
  className?: string;
}

export function BacktestResultsInline({ result, className }: BacktestResultsInlineProps) {
  const totalReturn = result.total_return;
  const sharpe = result.sharpe_ratio;
  const maxDD = result.max_drawdown;

  return (
    <div className={cn('border-t border-border pt-3 mt-1', className)}>
      <div className="text-xs font-semibold text-muted-foreground uppercase tracking-wide mb-2">
        Results
      </div>

      <div className="grid grid-cols-3 gap-2 mb-3">
        {/* Total Return */}
        <div className="bg-muted/30 rounded-lg p-2 text-center">
          <div className="text-[10px] text-muted-foreground mb-0.5">Total Return</div>
          <div
            className={cn(
              'text-sm font-bold tabular-nums',
              totalReturn == null
                ? 'text-muted-foreground'
                : totalReturn >= 0
                ? 'text-green-500'
                : 'text-red-500'
            )}
          >
            {totalReturn == null
              ? '—'
              : `${totalReturn >= 0 ? '+' : ''}${totalReturn.toFixed(2)}%`}
          </div>
        </div>

        {/* Sharpe Ratio */}
        <div className="bg-muted/30 rounded-lg p-2 text-center">
          <div className="text-[10px] text-muted-foreground mb-0.5">Sharpe</div>
          <div
            className={cn(
              'text-sm font-bold tabular-nums',
              sharpe == null
                ? 'text-muted-foreground'
                : sharpe >= 1
                ? 'text-green-500'
                : sharpe >= 0
                ? 'text-yellow-500'
                : 'text-red-500'
            )}
          >
            {sharpe == null ? '—' : sharpe.toFixed(2)}
          </div>
        </div>

        {/* Max Drawdown */}
        <div className="bg-muted/30 rounded-lg p-2 text-center">
          <div className="text-[10px] text-muted-foreground mb-0.5">Max DD</div>
          <div
            className={cn(
              'text-sm font-bold tabular-nums',
              maxDD == null ? 'text-muted-foreground' : 'text-red-500'
            )}
          >
            {maxDD == null ? '—' : `-${Math.abs(maxDD).toFixed(1)}%`}
          </div>
        </div>
      </div>

      {/* Additional detail row */}
      <div className="flex flex-wrap gap-x-4 gap-y-1 text-xs text-muted-foreground font-mono">
        {result.sortino_ratio != null && (
          <span>
            Sortino:{' '}
            <span className="text-foreground/80">{result.sortino_ratio.toFixed(2)}</span>
          </span>
        )}
        {result.total_days != null && (
          <span>
            Days: <span className="text-foreground/80">{result.total_days}</span>
          </span>
        )}
        {result.final_portfolio_value != null && (
          <span>
            Final:{' '}
            <span className="text-foreground/80">
              ${result.final_portfolio_value.toLocaleString(undefined, { maximumFractionDigits: 0 })}
            </span>
          </span>
        )}
      </div>
    </div>
  );
}
