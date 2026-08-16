import { useCallback, useEffect, useState } from 'react';
import { useNavigate, useParams } from 'react-router-dom';
import { api, type Dag } from '../api';

export default function DagDetail() {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const [dag, setDag] = useState<Dag | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [newYaml, setNewYaml] = useState('');
  const [editing, setEditing] = useState(false);

  const load = useCallback(async () => {
    if (!id) return;
    try {
      setDag(await api.getDag(id));
    } catch (e) {
      setError(String(e));
    }
  }, [id]);

  useEffect(() => {
    load();
  }, [load]);

  async function update() {
    if (!id) return;
    try {
      await fetch(`/api/v1/dags/${id}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/yaml' },
        body: newYaml,
      });
      setEditing(false);
      await load();
    } catch (e) {
      setError(String(e));
    }
  }

  if (!dag) {
    return <div className="panel">{error ?? 'Loading…'}</div>;
  }

  return (
    <div>
      <button onClick={() => navigate('/')} className="back">← DAGs</button>
      <section className="panel">
        <h2>{dag.name} <span className="muted">v{dag.version}</span></h2>
        <p>{dag.description}</p>
        <p className="muted">
          schedule: {dag.scheduleCron ?? 'manual'} · timezone: {dag.timezone} ·
          {dag.paused ? ' paused' : ' active'}
        </p>
        {error && <div className="error-banner">{error}</div>}
        <table>
          <thead>
            <tr><th>Task</th><th>Type</th><th>Depends on</th><th>Retries</th><th>Backoff</th><th>Timeout</th></tr>
          </thead>
          <tbody>
            {dag.tasks.map((task) => (
              <tr key={task.id}>
                <td>{task.name}</td>
                <td>{task.type}</td>
                <td>{task.dependsOn.join(', ') || '—'}</td>
                <td>{task.maxRetries}</td>
                <td>{task.retryDelaySeconds}s × {task.retryBackoff}</td>
                <td>{task.timeoutSeconds}s</td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>

      <section className="panel">
        <h2>Definition (YAML)</h2>
        {editing ? (
          <>
            <textarea
              rows={16}
              value={newYaml}
              onChange={(e) => setNewYaml(e.target.value)}
              spellCheck={false}
            />
            <button onClick={update}>Save new version</button>
            <button onClick={() => setEditing(false)}>Cancel</button>
          </>
        ) : (
          <>
            <pre className="yaml-view">{dag.yaml}</pre>
            <button onClick={() => { setNewYaml(dag.yaml); setEditing(true); }}>Edit</button>
          </>
        )}
      </section>
    </div>
  );
}
