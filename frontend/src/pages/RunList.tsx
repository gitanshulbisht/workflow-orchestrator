import { useCallback, useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import { api, type DagRun } from '../api';

const stateColor: Record<string, string> = {
  PENDING: 'state-pending',
  RUNNING: 'state-running',
  SUCCESS: 'state-success',
  FAILED: 'state-failed',
  CANCELLED: 'state-cancelled',
};

export default function RunList() {
  const [runs, setRuns] = useState<DagRun[]>([]);
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(async () => {
    try {
      setRuns(await api.listRuns());
    } catch (e) {
      setError(String(e));
    }
  }, []);

  useEffect(() => {
    load();
    const interval = setInterval(load, 3000);
    return () => clearInterval(interval);
  }, [load]);

  return (
    <div>
      <section className="panel">
        <h2>Runs</h2>
        {error && <div className="error-banner">{error}</div>}
        <table>
          <thead>
            <tr><th>Run</th><th>DAG</th><th>Trigger</th><th>State</th><th>Started</th></tr>
          </thead>
          <tbody>
            {runs.map((run) => (
              <tr key={run.id}>
                <td><Link to={`/runs/${run.id}`}>{run.id.slice(0, 8)}</Link></td>
                <td>{run.dagId.slice(0, 8)}</td>
                <td>{run.triggerType}</td>
                <td><span className={stateColor[run.state] ?? ''}>{run.state}</span></td>
                <td>{run.startedAt ? new Date(run.startedAt).toLocaleString() : '—'}</td>
              </tr>
            ))}
            {runs.length === 0 && (
              <tr><td colSpan={5} className="empty">No runs yet.</td></tr>
            )}
          </tbody>
        </table>
      </section>
    </div>
  );
}
