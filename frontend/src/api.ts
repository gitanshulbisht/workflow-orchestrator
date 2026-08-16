const API_BASE = import.meta.env.VITE_API_BASE ?? '';

export interface DagTask {
  id: string;
  name: string;
  type: string;
  config: Record<string, unknown>;
  maxRetries: number;
  retryDelaySeconds: number;
  retryBackoff: number;
  timeoutSeconds: number;
  singleton: boolean;
  dependsOn: string[];
}

export interface Dag {
  id: string;
  name: string;
  description: string | null;
  version: number;
  scheduleCron: string | null;
  timezone: string;
  paused: boolean;
  yaml: string;
  tasks: DagTask[];
}

export interface DagRun {
  id: string;
  dagId: string;
  dagVersion: number;
  state: string;
  triggerType: string;
  triggerPayload: string | null;
  startedAt: string | null;
  endedAt: string | null;
  createdAt: string;
}

export interface TaskInstance {
  id: string;
  runId: string;
  dagTaskId: string;
  state: string;
  attemptNo: number;
  queuedAt: string;
  scheduledAt: string | null;
  startedAt: string | null;
  endedAt: string | null;
  claimedBy: string | null;
  errorMessage: string | null;
}

export interface DeadLetter {
  id: string;
  taskInstanceId: string;
  runId: string;
  taskName: string;
  errorPayload: Record<string, unknown>;
  deadLetteredAt: string;
  replayStatus: string;
}

export interface Stats {
  dags: number;
  runs: number;
  tasks: number;
  deadLetters: number;
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${API_BASE}${path}`, {
    headers: { 'Content-Type': 'application/json' },
    ...init,
  });
  if (!response.ok) {
    let detail = response.statusText;
    try {
      const body = await response.json();
      detail = body.detail ?? detail;
    } catch {
      // keep statusText
    }
    throw new Error(detail);
  }
  return response.json() as Promise<T>;
}

export const api = {
  listDags: () => request<Dag[]>('/api/v1/dags'),
  getDag: (id: string) => request<Dag>(`/api/v1/dags/${id}`),
  registerDag: (yaml: string, idempotencyKey?: string) =>
    request<Dag>('/api/v1/dags', {
      method: 'POST',
      body: yaml,
      headers: {
        'Content-Type': 'application/yaml',
        ...(idempotencyKey ? { 'Idempotency-Key': idempotencyKey } : {}),
      },
    }),
  pauseDag: (id: string, paused: boolean) =>
    request<Dag>(`/api/v1/dags/${id}`, {
      method: 'PATCH',
      body: JSON.stringify({ paused }),
    }),
  triggerRun: (dagId: string, idempotencyKey?: string) =>
    request<DagRun>(`/api/v1/dags/${dagId}/runs`, {
      method: 'POST',
      body: '{}',
      ...(idempotencyKey ? { headers: { 'Idempotency-Key': idempotencyKey } } : {}),
    }),
  cancelRun: (dagId: string, runId: string) =>
    request<DagRun>(`/api/v1/dags/${dagId}/runs/${runId}/cancel`, { method: 'POST' }),
  listRuns: (dagId?: string) =>
    request<DagRun[]>(`/api/v1/runs${dagId ? `?dagId=${dagId}` : ''}`),
  getRun: (runId: string) => request<DagRun & { tasks: TaskInstance[] }>(`/api/v1/runs/${runId}`),
  listDeadLetters: () => request<DeadLetter[]>('/api/v1/dead-letters'),
  replayDeadLetter: (id: string) =>
    request<DeadLetter>(`/api/v1/dead-letters/${id}/replay`, { method: 'POST' }),
  stats: () => request<Stats>('/api/v1/stats'),
};
