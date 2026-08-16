import { useCallback, useEffect, useState } from 'react';
import { api, type DeadLetter } from '../api';

export default function DeadLetters() {
  const [items, setItems] = useState<DeadLetter[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [replaying, setReplaying] = useState<string | null>(null);

  const load = useCallback(async () => {
    try {
      setItems(await api.listDeadLetters());
    } catch (e) {
      setError(String(e));
    }
  }, []);

  useEffect(() => {
    load();
  }, [load]);

  async function replay(id: string) {
    setReplaying(id);
    setError(null);
    try {
      await api.replayDeadLetter(id);
      await load();
    } catch (e) {
      setError(String(e));
    } finally {
      setReplaying(null);
    }
  }

  return (
    <div>
      <section className="panel">
        <h2>Dead letters</h2>
        {error && <div className="error-banner">{error}</div>}
        <table>
          <thead>
            <tr><th>Task</th><th>Run</th><th>Error</th><th>Dead-lettered at</th><th></th></tr>
          </thead>
          <tbody>
            {items.map((dl) => (
              <tr key={dl.id}>
                <td>{dl.taskName}</td>
                <td>{dl.runId.slice(0, 8)}</td>
                <td className="muted">{String(dl.errorPayload?.error ?? '')}</td>
                <td>{new Date(dl.deadLetteredAt).toLocaleString()}</td>
                <td className="actions">
                  <button
                    onClick={() => replay(dl.id)}
                    disabled={replaying === dl.id || dl.replayStatus === 'REPLAYED'}
                  >
                    {dl.replayStatus === 'REPLAYED' ? 'Replayed' : 'Replay'}
                  </button>
                </td>
              </tr>
            ))}
            {items.length === 0 && (
              <tr><td colSpan={5} className="empty">No dead letters — everything is healthy.</td></tr>
            )}
          </tbody>
        </table>
      </section>
    </div>
  );
}
