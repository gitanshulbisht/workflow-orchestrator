import { useEventStream } from '../useEventStream';

export default function Events() {
  const { events, connected } = useEventStream();

  return (
    <div>
      <section className="panel">
        <h2>Live events {connected ? '●' : '○'}</h2>
        <p className="muted">
          Streamed over SSE from the outbox relay. Newest first.
        </p>
        <table>
          <thead>
            <tr><th>Type</th><th>Aggregate</th><th>Payload</th><th>At</th></tr>
          </thead>
          <tbody>
            {events.map((event) => (
              <tr key={event.id}>
                <td>{event.eventType}</td>
                <td>{event.aggregateType}:{event.aggregateId.slice(0, 8)}</td>
                <td className="muted mono">
                  {JSON.stringify(event.payload).slice(0, 120)}
                </td>
                <td>{new Date(event.createdAt).toLocaleTimeString()}</td>
              </tr>
            ))}
            {events.length === 0 && (
              <tr><td colSpan={4} className="empty">Waiting for events… trigger a DAG run to see the stream.</td></tr>
            )}
          </tbody>
        </table>
      </section>
    </div>
  );
}
