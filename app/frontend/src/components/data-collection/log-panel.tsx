import { useEffect, useRef, useState } from 'react';

function getLineColor(line: string): string {
  const upper = line.toUpperCase();
  if (upper.includes('[ERROR]') || upper.includes('ERROR:')) return 'text-red-500';
  if (upper.includes('[WARN]') || upper.includes('WARNING:')) return 'text-yellow-500';
  return 'text-muted-foreground';
}

interface LogPanelProps {
  lines: string[];
  maxVisible?: number;
  defaultExpanded?: boolean;
}

export function LogPanel({ lines, maxVisible = 10, defaultExpanded = false }: LogPanelProps) {
  const bottomRef = useRef<HTMLDivElement>(null);
  const [expanded, setExpanded] = useState(defaultExpanded);

  const visibleLines = expanded ? lines : lines.slice(-maxVisible);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [lines.length]);

  if (lines.length === 0) return null;

  return (
    <div className="border-t border-border/50 mt-3 pt-3">
      <div className="space-y-0.5 overflow-y-auto" style={{ maxHeight: expanded ? '300px' : 'auto' }}>
        {visibleLines.map((line, i) => (
          <div
            key={expanded ? i : lines.length - visibleLines.length + i}
            className={`text-xs font-mono leading-relaxed whitespace-pre-wrap break-all ${getLineColor(line)}`}
          >
            {line}
          </div>
        ))}
        <div ref={bottomRef} />
      </div>
      {lines.length > maxVisible && (
        <button
          onClick={() => setExpanded((v) => !v)}
          className="mt-1.5 text-xs text-blue-400 hover:text-blue-300 transition-colors"
        >
          {expanded ? 'Show less' : `Show all (${lines.length} lines)`}
        </button>
      )}
    </div>
  );
}
