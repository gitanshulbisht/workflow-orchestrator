import { useEffect, useState } from 'react';

const API_BASE = import.meta.env.VITE_API_BASE ?? '';

export interface OrchestratorEvent {
  id: number;
  aggregateType: string;
  aggregateId: string;
  eventType: string;
  payload: Record<string, unknown>;
  createdAt: string;
}

export function useEventStream() {
  const [events, setEvents] = useState<OrchestratorEvent[]>([]);
  const [connected, setConnected] = useState(false);

  useEffect(() => {
    const source = new EventSource(`${API_BASE}/api/v1/events/stream`);
    source.onopen = () => setConnected(true);
    source.onerror = () => setConnected(false);
    const handler = (e: MessageEvent) => {
      try {
        const parsed = JSON.parse(e.data);
        if (!parsed.eventType) {
          // 'connected' ping has no eventType
          return;
        }
        setEvents((prev) => [parsed, ...prev].slice(0, 100));
      } catch {
        // ignore malformed events
      }
    };
    // The backend emits named SSE events (event: <EVENT_TYPE>), so they must
    // be listened for by name — the generic 'message' handler never fires.
    source.addEventListener('DAG_RUN_CREATED', handler);
    source.addEventListener('DAG_RUN_STARTED', handler);
    source.addEventListener('DAG_RUN_SUCCESS', handler);
    source.addEventListener('DAG_RUN_FAILED', handler);
    source.addEventListener('DAG_RUN_CANCELLED', handler);
    source.addEventListener('TASK_INSTANCE_SCHEDULED', handler);
    source.addEventListener('TASK_INSTANCE_SUCCEEDED', handler);
    source.addEventListener('TASK_INSTANCE_FAILED', handler);
    source.addEventListener('TASK_INSTANCE_RETRY_SCHEDULED', handler);
    source.addEventListener('TASK_INSTANCE_DEAD_LETTERED', handler);
    source.addEventListener('TASK_INSTANCE_SKIPPED', handler);
    source.addEventListener('DEAD_LETTER_REPLAYED', handler);
    source.addEventListener('WEBHOOK_REGISTERED', handler);
    return () => source.close();
  }, []);

  return { events, connected };
}
