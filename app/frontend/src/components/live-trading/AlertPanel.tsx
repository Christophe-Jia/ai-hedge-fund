import { useState, useEffect, useCallback } from 'react';
import { Button } from '@/components/ui/button';
import { RefreshCw, CheckCircle, AlertTriangle, XCircle, Info } from 'lucide-react';
import { cn } from '@/lib/utils';

const API_BASE_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';

interface AlertItem {
  id: number;
  triggered_at: string;
  session_id: string | null;
  rule_id: string;
  severity: 'INFO' | 'WARNING' | 'CRITICAL';
  message: string | null;
  acknowledged: boolean;
}

interface AlertRule {
  rule_id: string;
  name: string;
  severity: string;
  cooldown_seconds: number;
}

// ---------------------------------------------------------------------------
// Severity icon
// ---------------------------------------------------------------------------
function SeverityIcon({ severity }: { severity: string }) {
  if (severity === 'CRITICAL') return <XCircle size={13} className="text-red-400 flex-shrink-0" />;
  if (severity === 'WARNING') return <AlertTriangle size={13} className="text-amber-400 flex-shrink-0" />;
  return <Info size={13} className="text-blue-400 flex-shrink-0" />;
}

function severityClass(severity: string) {
  if (severity === 'CRITICAL') return 'border-red-500/20 bg-red-500/5';
  if (severity === 'WARNING') return 'border-amber-500/20 bg-amber-500/5';
  return 'border-blue-500/20 bg-blue-500/5';
}

// ---------------------------------------------------------------------------
// Alert panel
// ---------------------------------------------------------------------------
export function AlertPanel({ sessionId = 'main' }: { sessionId?: string }) {
  const [alerts, setAlerts] = useState<AlertItem[]>([]);
  const [rules, setRules] = useState<AlertRule[]>([]);
  const [loading, setLoading] = useState(false);
  const [filter, setFilter] = useState<'ALL' | 'WARNING' | 'CRITICAL'>('ALL');

  const loadAlerts = useCallback(async () => {
    setLoading(true);
    try {
      const params = new URLSearchParams({ session_id: sessionId, limit: '100' });
      if (filter !== 'ALL') params.set('severity', filter);
      const [aRes, rRes] = await Promise.all([
        fetch(`${API_BASE_URL}/alerts?${params}`),
        fetch(`${API_BASE_URL}/alerts/rules`),
      ]);
      if (aRes.ok) setAlerts(await aRes.json());
      if (rRes.ok) {
        const d = await rRes.json();
        setRules(d.rules || []);
      }
    } catch (e) {
      console.error('[AlertPanel] load error:', e);
    } finally {
      setLoading(false);
    }
  }, [sessionId, filter]);

  useEffect(() => {
    loadAlerts();
    const t = setInterval(loadAlerts, 30_000);
    return () => clearInterval(t);
  }, [loadAlerts]);

  const acknowledge = async (id: number) => {
    await fetch(`${API_BASE_URL}/alerts/${id}/acknowledge`, { method: 'PATCH' });
    setAlerts((prev) => prev.map((a) => (a.id === id ? { ...a, acknowledged: true } : a)));
  };

  const unread = alerts.filter((a) => !a.acknowledged).length;

  return (
    <div className="space-y-3">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <h3 className="text-sm font-medium text-foreground">Risk Alerts</h3>
          {unread > 0 && (
            <span className="px-1.5 py-0.5 rounded-full bg-red-500 text-white text-[10px] font-bold">
              {unread}
            </span>
          )}
        </div>
        <div className="flex gap-1">
          {(['ALL', 'WARNING', 'CRITICAL'] as const).map((f) => (
            <button
              key={f}
              onClick={() => setFilter(f)}
              className={cn(
                'px-2 py-0.5 rounded text-xs transition-colors',
                filter === f
                  ? 'bg-muted text-foreground'
                  : 'text-muted-foreground hover:text-foreground',
              )}
            >
              {f}
            </button>
          ))}
          <Button
            variant="ghost"
            size="sm"
            onClick={loadAlerts}
            disabled={loading}
            className="h-6 w-6 p-0"
          >
            <RefreshCw size={10} className={loading ? 'animate-spin' : ''} />
          </Button>
        </div>
      </div>

      {/* Alert rules summary */}
      {rules.length > 0 && (
        <div className="text-xs text-muted-foreground">
          Active rules: {rules.map((r) => r.name).join(' · ')}
        </div>
      )}

      {/* Alert list */}
      {alerts.length === 0 ? (
        <p className="text-xs text-muted-foreground py-4 text-center">
          No alerts yet. Alerts will appear when thresholds are breached.
        </p>
      ) : (
        <div className="space-y-1.5 max-h-64 overflow-y-auto">
          {alerts.map((alert) => (
            <div
              key={alert.id}
              className={cn(
                'flex items-start gap-2 p-2.5 rounded-md border text-xs',
                severityClass(alert.severity),
                alert.acknowledged && 'opacity-40',
              )}
            >
              <SeverityIcon severity={alert.severity} />
              <div className="flex-1 min-w-0">
                <div className="flex items-center gap-1.5">
                  <span className="font-medium">{alert.rule_id}</span>
                  <span className="text-muted-foreground">
                    {new Date(alert.triggered_at).toLocaleTimeString()}
                  </span>
                </div>
                {alert.message && (
                  <p className="text-muted-foreground mt-0.5">{alert.message}</p>
                )}
              </div>
              {!alert.acknowledged && (
                <button
                  onClick={() => acknowledge(alert.id)}
                  className="text-muted-foreground hover:text-green-400 flex-shrink-0"
                  title="Mark as read"
                >
                  <CheckCircle size={12} />
                </button>
              )}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
