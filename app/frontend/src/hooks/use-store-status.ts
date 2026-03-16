import { useCallback, useEffect, useRef, useState } from 'react';
import { dataCollectionApi, DataCollectionStatus } from '@/services/data-collection-api';

const POLL_INTERVAL_MS = 15_000;

export function useStoreStatus() {
  const [status, setStatus] = useState<DataCollectionStatus | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [lastRefreshed, setLastRefreshed] = useState<Date | null>(null);
  const timerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const refresh = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const data = await dataCollectionApi.getStatus();
      setStatus(data);
      setLastRefreshed(new Date());
    } catch (err: any) {
      setError(err?.message ?? String(err));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    refresh();

    const schedule = () => {
      timerRef.current = setTimeout(async () => {
        await refresh();
        schedule();
      }, POLL_INTERVAL_MS);
    };

    schedule();

    return () => {
      if (timerRef.current) clearTimeout(timerRef.current);
    };
  }, [refresh]);

  return { status, loading, error, lastRefreshed, refresh };
}
