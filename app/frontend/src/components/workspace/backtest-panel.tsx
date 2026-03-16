import { useCallback, useEffect, useState } from 'react';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Play, Square } from 'lucide-react';
import { cn } from '@/lib/utils';
import { FactorList, FACTOR_GROUPS } from './factor-list';
import { BacktestResultsInline } from './backtest-results-inline';
import { useSimpleBacktest } from '@/hooks/use-simple-backtest';
import { getModels, LanguageModel } from '@/data/models';

type AssetType = 'equities' | 'crypto';

const DEFAULT_EQUITY_TICKERS = 'AAPL,MSFT,NVDA';
const DEFAULT_CRYPTO_TICKERS = 'BTC/USDT';

function getDefaultFactors(assetType: AssetType): Set<string> {
  const keys = new Set<string>();
  FACTOR_GROUPS.forEach((group) => {
    group.factors.forEach((factor) => {
      // Skip equity-only factors for crypto
      if (assetType === 'crypto' && factor.equityOnly) return;
      keys.add(factor.key);
    });
  });
  return keys;
}

export function BacktestPanel() {
  const [assetType, setAssetType] = useState<AssetType>('equities');
  const [tickers, setTickers] = useState(DEFAULT_EQUITY_TICKERS);
  const [startDate, setStartDate] = useState('2024-01-01');
  const [endDate, setEndDate] = useState('2024-12-31');
  const [initialCapital, setInitialCapital] = useState(100000);
  const [slippageBps, setSlippageBps] = useState(5);
  const [selectedFactors, setSelectedFactors] = useState<Set<string>>(getDefaultFactors('equities'));
  const [models, setModels] = useState<LanguageModel[]>([]);
  const [selectedModel, setSelectedModel] = useState<LanguageModel | null>(null);

  const { status, result, error, progressMessages, run, stop } = useSimpleBacktest();

  // Load models on mount
  useEffect(() => {
    getModels()
      .then((m) => {
        setModels(m);
        const defaultModel = m.find((x) => x.model_name === 'gpt-4.1') ?? m[0] ?? null;
        setSelectedModel(defaultModel);
      })
      .catch(() => {
        // leave models empty — user can't select
      });
  }, []);

  // When asset type changes, reset tickers and deselect equity-only factors
  const handleAssetTypeChange = useCallback((type: AssetType) => {
    setAssetType(type);
    setTickers(type === 'equities' ? DEFAULT_EQUITY_TICKERS : DEFAULT_CRYPTO_TICKERS);
    setSelectedFactors(getDefaultFactors(type));
  }, []);

  const handleToggleFactor = useCallback((key: string) => {
    setSelectedFactors((prev) => {
      const next = new Set(prev);
      if (next.has(key)) {
        next.delete(key);
      } else {
        next.add(key);
      }
      return next;
    });
  }, []);

  const handleRun = () => {
    if (!selectedModel) return;
    run({
      assetType,
      tickers,
      startDate,
      endDate,
      initialCapital,
      modelName: selectedModel.model_name,
      modelProvider: selectedModel.provider,
      selectedFactors: Array.from(selectedFactors),
      slippageBps,
    });
  };

  const isRunning = status === 'running';
  const canRun = selectedFactors.size > 0 && tickers.trim().length > 0 && !isRunning;

  return (
    <div className="h-full flex flex-col overflow-hidden border-r border-border">
      {/* Header */}
      <div className="px-4 pt-4 pb-3 border-b border-border shrink-0">
        <h2 className="text-sm font-semibold text-foreground">策略回测</h2>
        <div className="flex gap-1 mt-2">
          <button
            className={cn(
              'px-3 py-1 text-xs rounded-md font-medium transition-colors',
              assetType === 'equities'
                ? 'bg-primary text-primary-foreground'
                : 'text-muted-foreground hover:text-foreground hover:bg-muted/60'
            )}
            onClick={() => handleAssetTypeChange('equities')}
          >
            美股
          </button>
          <button
            className={cn(
              'px-3 py-1 text-xs rounded-md font-medium transition-colors',
              assetType === 'crypto'
                ? 'bg-primary text-primary-foreground'
                : 'text-muted-foreground hover:text-foreground hover:bg-muted/60'
            )}
            onClick={() => handleAssetTypeChange('crypto')}
          >
            加密货币
          </button>
        </div>
      </div>

      {/* Scrollable body */}
      <div className="flex-1 overflow-y-auto px-4 py-3 space-y-3">
        {/* Factor groups */}
        <FactorList
          assetType={assetType}
          selectedFactors={selectedFactors}
          onToggle={handleToggleFactor}
        />

        {/* Parameters */}
        <div className="border border-border rounded-lg p-3 space-y-2">
          <div className="text-xs font-semibold uppercase tracking-wide text-muted-foreground mb-1">
            参数配置
          </div>

          <div className="space-y-1.5">
            <label className="text-xs text-muted-foreground block">标的</label>
            <Input
              value={tickers}
              onChange={(e) => setTickers(e.target.value)}
              placeholder={assetType === 'equities' ? 'AAPL,MSFT,NVDA' : 'BTC/USDT'}
              className="h-7 text-xs font-mono"
            />
          </div>

          <div className="grid grid-cols-2 gap-2">
            <div className="space-y-1.5">
              <label className="text-xs text-muted-foreground block">开始日期</label>
              <Input
                type="date"
                value={startDate}
                onChange={(e) => setStartDate(e.target.value)}
                className="h-7 text-xs"
              />
            </div>
            <div className="space-y-1.5">
              <label className="text-xs text-muted-foreground block">结束日期</label>
              <Input
                type="date"
                value={endDate}
                onChange={(e) => setEndDate(e.target.value)}
                className="h-7 text-xs"
              />
            </div>
          </div>

          <div className="grid grid-cols-2 gap-2">
            <div className="space-y-1.5">
              <label className="text-xs text-muted-foreground block">初始资金 ($)</label>
              <Input
                type="number"
                value={initialCapital}
                onChange={(e) => setInitialCapital(Number(e.target.value))}
                min={1000}
                step={10000}
                className="h-7 text-xs"
              />
            </div>
            <div className="space-y-1.5">
              <label className="text-xs text-muted-foreground block">滑点 (bps)</label>
              <Input
                type="number"
                value={slippageBps}
                onChange={(e) => setSlippageBps(Number(e.target.value))}
                min={0}
                max={100}
                step={1}
                className="h-7 text-xs"
              />
            </div>
          </div>

          <div className="space-y-1.5">
            <label className="text-xs text-muted-foreground block">模型</label>
            <select
              value={selectedModel?.model_name ?? ''}
              onChange={(e) => {
                const m = models.find((x) => x.model_name === e.target.value);
                if (m) setSelectedModel(m);
              }}
              className="w-full h-7 text-xs rounded-md border border-input bg-background px-2 py-0 text-foreground focus:outline-none focus:ring-1 focus:ring-ring"
            >
              {models.length === 0 && (
                <option value="">Loading models...</option>
              )}
              {models.map((m) => (
                <option key={m.model_name} value={m.model_name}>
                  {m.display_name}
                </option>
              ))}
            </select>
          </div>
        </div>

        {/* Run / Stop button */}
        <Button
          onClick={isRunning ? stop : handleRun}
          disabled={!isRunning && !canRun}
          className={cn(
            'w-full h-8 text-xs gap-1.5',
            isRunning && 'border-red-500/50 text-red-400 hover:bg-red-500/10'
          )}
          variant={isRunning ? 'outline' : 'default'}
        >
          {isRunning ? (
            <>
              <Square size={12} />
              Stop
            </>
          ) : (
            <>
              <Play size={12} />
              Run Backtest
            </>
          )}
        </Button>

        {/* Progress messages */}
        {isRunning && progressMessages.length > 0 && (
          <div className="border border-border rounded-lg p-2 max-h-24 overflow-y-auto">
            {progressMessages.slice(-8).map((msg, i) => (
              <div key={i} className="text-[10px] font-mono text-muted-foreground leading-relaxed">
                {msg}
              </div>
            ))}
          </div>
        )}

        {/* Error */}
        {status === 'error' && error && (
          <div className="border border-red-500/30 bg-red-500/5 rounded-lg p-2 text-xs text-red-400 font-mono">
            {error}
          </div>
        )}

        {/* Results */}
        {status === 'complete' && result && (
          <BacktestResultsInline result={result} />
        )}
      </div>
    </div>
  );
}
