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
    const handler = (e: Event) => {
      const messageEvent = e as MessageEvent;
      if (!messageEvent.data || messageEvent.data.startsWith('{')) {
        // 'connected' ping has JSON but no eventType
        return;
      }
      try {
        const parsed = JSON.parse(messageEvent.data);
        setEvents((prev) => [parsed, ...prev].slice(0, 100));
      } catch {
        // ignore malformed events
      }
    };
    source.addEventListener('message', handler);
    return () => source.close();
  }, []);

  return { events, connected };
}
