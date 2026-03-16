import { ScriptStatus } from '@/hooks/use-script-runner';

interface StatusDotProps {
  status: ScriptStatus;
  size?: 'sm' | 'md';
}

export function StatusDot({ status, size = 'sm' }: StatusDotProps) {
  const dim = size === 'md' ? 'w-2.5 h-2.5' : 'w-2 h-2';

  switch (status) {
    case 'running':
      return <span className={`${dim} rounded-full bg-green-500 animate-pulse inline-block shrink-0`} />;
    case 'done':
      return <span className={`${dim} rounded-full bg-green-500 inline-block shrink-0`} />;
    case 'error':
      return <span className={`${dim} rounded-full bg-red-500 inline-block shrink-0`} />;
    default:
      return <span className={`${dim} rounded-full bg-muted-foreground/40 inline-block shrink-0`} />;
  }
}
