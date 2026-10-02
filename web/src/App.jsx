import { useEffect, useState } from 'react'
import NodePool from './components/NodePool.jsx'
import AdminPanel from './components/AdminPanel.jsx'
import SubmitForm from './components/SubmitForm.jsx'
import RunLogs from './components/RunLogs.jsx'
import CheckpointAdvice from './components/CheckpointAdvice.jsx'
import JobRuns from './components/JobRuns.jsx'
import Results from './components/Results.jsx'
import Login from './components/Login.jsx'
import { PoolProvider, usePool } from './PoolContext.jsx'
import { getMe, getToken, logout, setUnauthorizedHandler } from './api.js'

// The heartbeat of the page: live counts + a ticking clock. Cheap on purpose —
// the shared pool refresh and a 1s clock tick.
function LiveStrip() {
  const { nodes } = usePool()
  const [now, setNow] = useState(() => new Date())

  useEffect(() => {
    const id = setInterval(() => setNow(new Date()), 1000)
    return () => clearInterval(id)
  }, [])

  const online = nodes.filter((n) => n.online)
  const slots = online.filter((n) => !n.agent_outdated).reduce((s, n) => s + (n.capacity || 0), 0)
  return (
    <div className="livestrip">
      <span className="sys"><span className="sysdot" />SYSTEM LIVE</span>
      <span className="sep">│</span>
      <span>{online.length}/{nodes.length} nodes online</span>
      <span className="sep">│</span>
      <span>{slots} run slots</span>
      <span className="sep">│</span>
      <span className="clock">{now.toLocaleTimeString()}</span>
    </div>
  )
}

export default function App() {
  const [authed, setAuthed] = useState(() => !!getToken())
  const [job, setJob] = useState(null) // { jobId, runIds }
  const [runId, setRunId] = useState(null)
  // Who is signed in (2026-09-07): the admin sees the admin controls, and nobody
  // else sees a control they are not allowed to use.
  const [me, setMe] = useState(null)

  // Any 401 anywhere (expired token, etc.) drops us back to the login screen.
  useEffect(() => {
    setUnauthorizedHandler(() => { setAuthed(false); setJob(null); setRunId(null); setMe(null) })
    return () => setUnauthorizedHandler(null)
  }, [])

  useEffect(() => {
    if (!authed) { setMe(null); return }
    let alive = true, timer
    async function load() {
      try { const user = await getMe(); if (alive) setMe(user) }
      catch { if (alive) timer = setTimeout(load, 3000) }
    }
    load()
    return () => { alive = false; clearTimeout(timer) }
  }, [authed])

  function onSubmitted(result) {
    setJob({ jobId: result.job_id, runIds: result.run_ids })
    setRunId(result.run_ids[0] ?? null)
  }

  // 2026-09-07 (walk 1, row 46): a job's runs and logs can be opened again from the
  // Results panel — after a reload the Runs and Live logs panels used to be empty and
  // stay empty, so no earlier job's log could be read from the interface.
  function onViewRun(jobId, runIds, rid) {
    setJob({ jobId, runIds })
    setRunId(rid ?? runIds[0] ?? null)
    document.getElementById('runs-panel')?.scrollIntoView({ behavior: 'smooth', block: 'start' })
  }

  function onLogout() {
    logout()
    setJob(null)
    setRunId(null)
    setMe(null)
    setAuthed(false)
  }

  if (!authed) return <Login onLoggedIn={() => setAuthed(true)} />

  return (
    <PoolProvider>
    <div className="app">
      {/* Fixed twinkling backdrop — pure CSS, sits behind everything. */}
      <div className="stars" aria-hidden="true" />
      <header>
        <button className="logout" onClick={onLogout}>Sign out</button>
        <h1><span className="sheen">Distributed Training Platform</span></h1>
        <p className="sub">Pool your machines, submit a job, and watch it run across them as it happens.</p>
        <LiveStrip />
      </header>

      {/* The pool is the star — it gets the full page width. */}
      <section className="panel scan">
        <h2>Node pool</h2>
        <NodePool isAdmin={!!me?.is_admin} />
      </section>

      {/* The admin's own panel (2026-09-07, walk 1 row 14): accounts and tiers. */}
      {me?.is_admin && (
        <section className="panel">
          <h2>Admin — users and tiers</h2>
          <AdminPanel />
        </section>
      )}

      {/* Full width (2026-09-07): the form is mostly rows of small fields, and in a
          half-width column every one of them wrapped, which made the tallest panel
          on the page out of the shortest content. */}
      <section className="panel">
        <h2>Submit a job</h2>
        <SubmitForm onSubmitted={onSubmitted} />
      </section>

      {/* Runs beside Results: the run table is five narrow columns and does not want
          the page, while Results' cards do — and the two are read together, one job's
          runs now against every earlier job's. */}
      <div className="grid">
        <section className="panel" id="runs-panel">
          <h2>Runs</h2>
          {job ? (
            /* One row per run: parallel dispatch is visible as several rows
               RUNNING on different nodes at once (the W4 DoD). */
            <>
              {/* One line about the pasted script, above the runs it applies to. */}
              <CheckpointAdvice jobId={job.jobId} />
              <JobRuns key={job.jobId} jobId={job.jobId} />
            </>
          ) : (
            <p className="muted">
              submit a job to watch its runs land here — or pick an earlier job under
              Results and open one of its runs
            </p>
          )}
        </section>

        {/* Unified results view (W6, FR-10): pick any job, compare its runs side by
            side, and download each run's collected output files. */}
        <section className="panel">
          <h2>Results</h2>
          <Results onViewRun={onViewRun} />
        </section>
      </div>

      {job && (
        <section className="panel">
          <h2>Live logs</h2>
          {job.runIds.length > 1 && (
            <div className="runtabs">
              {job.runIds.map((rid, i) => (
                <button
                  key={rid}
                  className={rid === runId ? 'tab active' : 'tab'}
                  onClick={() => setRunId(rid)}
                >
                  run {i + 1}
                </button>
              ))}
            </div>
          )}
          {/* key={runId} remounts the log view (fresh cursor + socket) per run */}
          {runId && <RunLogs key={runId} jobId={job.jobId} runId={runId} />}
        </section>
      )}
    </div>
    </PoolProvider>
  )
}
