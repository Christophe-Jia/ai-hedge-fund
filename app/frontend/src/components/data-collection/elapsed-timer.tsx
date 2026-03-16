import { useEffect, useRef, useState } from 'react';

function formatElapsed(seconds: number): string {
  if (seconds < 60) return `${seconds}s`;
  const m = Math.floor(seconds / 60);
  const s = seconds % 60;
  if (m < 60) return `${m}m ${s}s`;
  const h = Math.floor(m / 60);
  const rem = m % 60;
  return `${h}h ${rem}m ${s}s`;
}

interface ElapsedTimerProps {
  startedAt: Date | null;
  running: boolean;
  className?: string;
}

export function ElapsedTimer({ startedAt, running, className = '' }: ElapsedTimerProps) {
  const [elapsed, setElapsed] = useState(0);
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null);

  useEffect(() => {
    if (!running || !startedAt) {
      if (intervalRef.current) clearInterval(intervalRef.current);
      return;
    }

    const tick = () => {
      const diff = Math.floor((Date.now() - startedAt.getTime()) / 1000);
      setElapsed(diff);
    };

    tick();
    intervalRef.current = setInterval(tick, 1000);

    return () => {
      if (intervalRef.current) clearInterval(intervalRef.current);
    };
  }, [running, startedAt]);

  if (!running || !startedAt) return null;

  return (
    <span className={`text-xs text-green-400 font-mono tabular-nums ${className}`}>
      {formatElapsed(elapsed)} elapsed
    </span>
  );
}
