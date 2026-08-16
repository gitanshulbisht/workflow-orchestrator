import { useCallback, useEffect, useState } from 'react';
import { useNavigate, useParams } from 'react-router-dom';
import { api, type DagRun, type TaskInstance } from '../api';

type RunWithTasks = DagRun & { tasks: TaskInstance[] };

const stateColor: Record<string, string> = {
  PENDING: 'state-pending',
  SCHEDULED: 'state-scheduled',
  RUNNING: 'state-running',
  SUCCESS: 'state-success',
  FAILED: 'state-failed',
  UP_FOR_RETRY: 'state-retry',
  DEAD_LETTERED: 'state-dead',
  SKIPPED: 'state-skipped',
  CANCELLED: 'state-cancelled',
};

export default function RunDetail() {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const [run, setRun] = useState<RunWithTasks | null>(null);
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(async () => {
    if (!id) return;
    try {
      setRun(await api.getRun(id));
    } catch (e) {
      setError(String(e));
    }
  }, [id]);

  useEffect(() => {
    load();
    const interval = setInterval(load, 2000);
    return () => clearInterval(interval);
  }, [load]);

  async function cancel() {
    if (!run) return;
    try {
      await api.cancelRun(run.dagId, run.id);
      await load();
    } catch (e) {
      setError(String(e));
    }
  }

  if (!run) {
    return <div className="panel">{error ?? 'Loading…'}</div>;
  }

  return (
    <div>
      <button onClick={() => navigate('/runs')} className="back">← Runs</button>
      <section className="panel">
        <h2>
          Run {run.id.slice(0, 8)}{' '}
          <span className={stateColor[run.state] ?? ''}>{run.state}</span>
        </h2>
        <p className="muted">
          DAG {run.dagId.slice(0, 8)} v{run.dagVersion} · {run.triggerType} ·
          started {run.startedAt ? new Date(run.startedAt).toLocaleString() : '—'}
        </p>
        {error && <div className="error-banner">{error}</div>}
        {run.state === 'RUNNING' || run.state === 'PENDING' ? (
          <button onClick={cancel}>Cancel run</button>
        ) : null}
      </section>

      <section className="panel">
        <h2>Task timeline</h2>
        <table>
          <thead>
            <tr><th>Task</th><th>State</th><th>Attempt</th><th>Scheduled</th><th>Ended</th><th>Error</th></tr>
          </thead>
          <tbody>
            {run.tasks.map((task) => (
              <tr key={task.id}>
                <td>{task.id.slice(0, 8)}</td>
                <td><span className={stateColor[task.state] ?? ''}>{task.state}</span></td>
                <td>{task.attemptNo}</td>
                <td>{task.scheduledAt ? new Date(task.scheduledAt).toLocaleString() : '—'}</td>
                <td>{task.endedAt ? new Date(task.endedAt).toLocaleString() : '—'}</td>
                <td className="muted">{task.errorMessage ?? ''}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>
    </div>
  );
}
