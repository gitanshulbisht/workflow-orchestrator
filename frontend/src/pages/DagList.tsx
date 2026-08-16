import { useCallback, useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import { api, type Dag, type Stats } from '../api';

const SAMPLE_YAML = `name: data-pipeline
description: Fetch data in parallel, process, notify
tasks:
  - name: start
    type: delay
    config:
      seconds: 1
  - name: fetch-data
    type: bash
    config:
      command: echo "fetched 42 rows"
    dependsOn: [start]
  - name: fetch-config
    type: bash
    config:
      command: echo "config loaded"
    dependsOn: [start]
  - name: process
    type: bash
    config:
      command: echo "processing..."
    dependsOn: [fetch-data, fetch-config]
  - name: notify
    type: http
    config:
      url: https://example.com/notify
      method: POST
      body: "{\\"status\\":\\"done\\"}"
    dependsOn: [process]
`;

export default function DagList() {
  const [dags, setDags] = useState<Dag[]>([]);
  const [stats, setStats] = useState<Stats | null>(null);
  const [yaml, setYaml] = useState(SAMPLE_YAML);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const load = useCallback(async () => {
    try {
      const [d, s] = await Promise.all([api.listDags(), api.stats()]);
      setDags(d);
      setStats(s);
    } catch (e) {
      setError(String(e));
    }
  }, []);

  useEffect(() => {
    load();
  }, [load]);

  async function register() {
    setBusy(true);
    setError(null);
    try {
      const idempotencyKey = crypto.randomUUID();
      await api.registerDag(yaml, idempotencyKey);
      setYaml(SAMPLE_YAML);
      await load();
    } catch (e) {
      setError(String(e));
    } finally {
      setBusy(false);
    }
  }

  async function togglePause(dag: Dag) {
    try {
      await api.pauseDag(dag.id, !dag.paused);
      await load();
    } catch (e) {
      setError(String(e));
    }
  }

  async function trigger(dag: Dag) {
    try {
      await api.triggerRun(dag.id, crypto.randomUUID());
      await load();
    } catch (e) {
      setError(String(e));
    }
  }

  return (
    <div>
      <div className="stat-cards">
        <div className="stat-card"><strong>{stats?.dags ?? '—'}</strong><span>DAGs</span></div>
        <div className="stat-card"><strong>{stats?.runs ?? '—'}</strong><span>Runs</span></div>
        <div className="stat-card"><strong>{stats?.tasks ?? '—'}</strong><span>Tasks</span></div>
        <div className="stat-card"><strong>{stats?.deadLetters ?? '—'}</strong><span>Dead letters</span></div>
      </div>

      {error && <div className="error-banner">{error}</div>}

      <section className="panel">
        <h2>Register a DAG</h2>
        <textarea
          rows={18}
          value={yaml}
          onChange={(e) => setYaml(e.target.value)}
          spellCheck={false}
        />
        <button onClick={register} disabled={busy}>
          {busy ? 'Registering…' : 'Register DAG'}
        </button>
      </section>

      <section className="panel">
        <h2>Registered DAGs</h2>
        <table>
          <thead>
            <tr>
              <th>Name</th><th>Version</th><th>Schedule</th><th>State</th><th></th>
            </tr>
          </thead>
          <tbody>
            {dags.map((dag) => (
              <tr key={dag.id}>
                <td><Link to={`/dags/${dag.id}`}>{dag.name}</Link></td>
                <td>v{dag.version}</td>
                <td>{dag.scheduleCron ?? 'manual'}</td>
                <td>{dag.paused ? 'paused' : 'active'}</td>
                <td className="actions">
                  <button onClick={() => trigger(dag)}>Trigger</button>
                  <button onClick={() => togglePause(dag)}>
                    {dag.paused ? 'Resume' : 'Pause'}
                  </button>
                </td>
              </tr>
            ))}
            {dags.length === 0 && (
              <tr><td colSpan={5} className="empty">No DAGs yet — register one above.</td></tr>
            )}
          </tbody>
        </table>
      </section>
    </div>
  );
}
