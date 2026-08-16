import { BrowserRouter, NavLink, Route, Routes } from 'react-router-dom';
import DagList from './pages/DagList';
import DagDetail from './pages/DagDetail';
import RunList from './pages/RunList';
import RunDetail from './pages/RunDetail';
import DeadLetters from './pages/DeadLetters';
import Events from './pages/Events';
import { useEventStream } from './useEventStream';
import './App.css';

export default function App() {
  const { connected } = useEventStream();

  return (
    <BrowserRouter>
      <div className="layout">
        <header className="topbar">
          <h1>Orchestrator</h1>
          <span className={`live-indicator ${connected ? 'live' : ''}`}>
            {connected ? '● live' : '○ offline'}
          </span>
          <nav>
            <NavLink to="/" end>DAGs</NavLink>
            <NavLink to="/runs">Runs</NavLink>
            <NavLink to="/dead-letters">Dead letters</NavLink>
            <NavLink to="/events">Events</NavLink>
          </nav>
        </header>
        <main>
          <Routes>
            <Route path="/" element={<DagList />} />
            <Route path="/dags/:id" element={<DagDetail />} />
            <Route path="/runs" element={<RunList />} />
            <Route path="/runs/:id" element={<RunDetail />} />
            <Route path="/dead-letters" element={<DeadLetters />} />
            <Route path="/events" element={<Events />} />
          </Routes>
        </main>
      </div>
    </BrowserRouter>
  );
}
