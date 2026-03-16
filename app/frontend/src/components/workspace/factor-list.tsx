import { cn } from '@/lib/utils';
import { Checkbox } from '@/components/ui/checkbox';

export interface Factor {
  key: string;
  label: string;
  equityOnly?: boolean;
}

export interface FactorGroup {
  title: string;
  factors: Factor[];
}

export const FACTOR_GROUPS: FactorGroup[] = [
  {
    title: 'Trend / Momentum',
    factors: [
      { key: 'technical_analyst', label: 'Technical Analyst' },
      { key: 'stanley_druckenmiller', label: 'Stanley Druckenmiller' },
      { key: 'cathie_wood', label: 'Cathie Wood' },
    ],
  },
  {
    title: 'Fundamentals / Value',
    factors: [
      { key: 'aswath_damodaran', label: 'Aswath Damodaran', equityOnly: true },
      { key: 'ben_graham', label: 'Ben Graham', equityOnly: true },
      { key: 'bill_ackman', label: 'Bill Ackman', equityOnly: true },
      { key: 'charlie_munger', label: 'Charlie Munger', equityOnly: true },
      { key: 'michael_burry', label: 'Michael Burry', equityOnly: true },
      { key: 'mohnish_pabrai', label: 'Mohnish Pabrai', equityOnly: true },
      { key: 'peter_lynch', label: 'Peter Lynch', equityOnly: true },
      { key: 'phil_fisher', label: 'Phil Fisher', equityOnly: true },
      { key: 'rakesh_jhunjhunwala', label: 'Rakesh Jhunjhunwala', equityOnly: true },
      { key: 'warren_buffett', label: 'Warren Buffett', equityOnly: true },
      { key: 'fundamentals_analyst', label: 'Fundamentals Analyst', equityOnly: true },
      { key: 'growth_analyst', label: 'Growth Analyst', equityOnly: true },
      { key: 'valuation_analyst', label: 'Valuation Analyst', equityOnly: true },
    ],
  },
  {
    title: 'Sentiment / Market Micro',
    factors: [
      { key: 'sentiment_analyst', label: 'Sentiment Analyst' },
      { key: 'news_sentiment_analyst', label: 'News Sentiment' },
      { key: 'ob_signal', label: 'OB Signal' },
      { key: 'polymarket_signal', label: 'Polymarket Signal' },
    ],
  },
];

interface FactorGroupSectionProps {
  group: FactorGroup;
  selectedFactors: Set<string>;
  disabled: boolean;
  onToggle: (key: string) => void;
}

function FactorGroupSection({ group, selectedFactors, disabled, onToggle }: FactorGroupSectionProps) {
  const allChecked = group.factors.every((f) => selectedFactors.has(f.key));
  const someChecked = group.factors.some((f) => selectedFactors.has(f.key));

  const toggleGroup = () => {
    if (disabled) return;
    if (allChecked) {
      group.factors.forEach((f) => {
        if (selectedFactors.has(f.key)) onToggle(f.key);
      });
    } else {
      group.factors.forEach((f) => {
        if (!selectedFactors.has(f.key)) onToggle(f.key);
      });
    }
  };

  return (
    <div className={cn('border border-border rounded-lg p-3', disabled && 'opacity-50')}>
      {/* Group header */}
      <div className="flex items-center gap-2 mb-2">
        <Checkbox
          id={`group-${group.title}`}
          checked={allChecked ? true : someChecked ? 'indeterminate' : false}
          onCheckedChange={toggleGroup}
          disabled={disabled}
          className="h-3.5 w-3.5"
        />
        <label
          htmlFor={`group-${group.title}`}
          className={cn(
            'text-xs font-semibold uppercase tracking-wide text-muted-foreground cursor-pointer select-none',
            disabled && 'cursor-not-allowed'
          )}
        >
          {group.title}
        </label>
        {disabled && (
          <span className="ml-auto text-[10px] text-muted-foreground/60 italic">crypto: disabled</span>
        )}
      </div>

      {/* Factor rows */}
      <div className="space-y-1 pl-1">
        {group.factors.map((factor) => (
          <div key={factor.key} className="flex items-center gap-2">
            <Checkbox
              id={`factor-${factor.key}`}
              checked={selectedFactors.has(factor.key)}
              onCheckedChange={() => !disabled && onToggle(factor.key)}
              disabled={disabled}
              className="h-3.5 w-3.5"
            />
            <label
              htmlFor={`factor-${factor.key}`}
              className={cn(
                'text-xs text-foreground/80 cursor-pointer select-none',
                disabled && 'cursor-not-allowed'
              )}
            >
              {factor.label}
            </label>
          </div>
        ))}
      </div>
    </div>
  );
}

interface FactorListProps {
  assetType: 'equities' | 'crypto';
  selectedFactors: Set<string>;
  onToggle: (key: string) => void;
}

export function FactorList({ assetType, selectedFactors, onToggle }: FactorListProps) {
  return (
    <div className="space-y-2">
      {FACTOR_GROUPS.map((group) => {
        const isFundamentalsGroup = group.title === 'Fundamentals / Value';
        const disabled = assetType === 'crypto' && isFundamentalsGroup;
        return (
          <FactorGroupSection
            key={group.title}
            group={group}
            selectedFactors={selectedFactors}
            disabled={disabled}
            onToggle={onToggle}
          />
        );
      })}
    </div>
  );
}
