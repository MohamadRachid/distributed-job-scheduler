import { usePool } from '../PoolContext.jsx'
import { useEffect, useState } from 'react'
import {
  cancelJob, getJobs, getJobRuns, releaseJobStorage, getRunArtifacts, downloadArtifact, shredJobKey,
} from '../api.js'
import { learnedTitle } from '../learned.js'
import { isTerminal } from '../status.js'

// W6 unified results view (FR-10): pick a job, see its runs SIDE BY SIDE — node,
// status, attempt, exit, duration, W5b failure reason, W5c learned line — each with
// its collected output files, downloadable through the control plane.

function bytes(n) {
  if (n == null) return ''
  if (n < 1024) return `${n} B`
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`
  return `${(n / (1024 * 1024)).toFixed(1)} MB`
}

function duration(r) {
  if (!r.started_at || !r.finished_at) return '—'
  const s = (new Date(r.finished_at) - new Date(r.started_at)) / 1000
  return s >= 0 ? `${s.toFixed(1)}s` : '—'
}

// One run's collected files, with a download button each.
function Artifacts({ runId, status, attempt, refresh, shredded }) {
  const [files, setFiles] = useState(null)
  const [err, setErr] = useState(null)
  const [dlErr, setDlErr] = useState({}) // artifact_id -> the sentence the download failed with

  useEffect(() => {
    let alive = true
    setFiles(null); setErr(null); setDlErr({})
    getRunArtifacts(runId)
      .then((f) => alive && setFiles(f))
      .catch((e) => alive && setErr(e.message))
    return () => { alive = false }
  }, [runId, status, attempt, refresh])

  async function download(a) {
    setDlErr((m) => ({ ...m, [a.artifact_id]: null }))
    try {
      await downloadArtifact(a.artifact_id, a.filename)
    } catch (e) {
      // On the page, in words (walk 1, row 62) — the click used to fail silently.
      setDlErr((m) => ({ ...m, [a.artifact_id]: e.message }))
    }
  }

  if (err) return <p className="muted">artifacts unavailable</p>
  if (files == null) return <p className="muted">loading files…</p>
  if (!files.length) return <p className="muted">no output files</p>
  return (
    <ul className="artifacts">
      {files.map((a) => (
        <li key={a.artifact_id}>
          {shredded ? (
            <span className="muted">🔥 {a.filename} — unreadable, the key was deleted</span>
          ) : (
            <button className="minor" onClick={() => download(a)}>
              ⬇ {a.filename}
            </button>
          )}
          {/* The size STORED, which is the sealed size — a little larger than the
              file you get back (walk 1, row 27). */}
          <span className="muted"> {bytes(a.size)} stored (sealed)</span>
          {dlErr[a.artifact_id] && <div className="err">{dlErr[a.artifact_id]}</div>}
        </li>
      ))}
    </ul>
  )
}

// Crypto-shred: one click deletes a job's key, which makes every sealed copy of its
// data permanently unreadable — the input object in storage, its results and
// checkpoints, anything still staged on a worker, any backup. Since 2026-09-06 that
// reaches the OUTPUTS too, because they are sealed with the same key. Irreversible,
// so it confirms first and says so.
function ShredKey({ jobId, onShredded }) {
  const [state, setState] = useState(null) // null | 'busy' | a result message

  async function shred() {
    // The dialog names everything the key opens — input, results AND checkpoints —
    // the same list the outcome line names (walk 1, row 61).
    const ok = window.confirm(
      'Delete this job’s key?\n\n' +
        'Every sealed copy of its input, its results and its checkpoints becomes ' +
        'permanently unreadable — including to us. This cannot be undone.',
    )
    if (!ok) return
    setState('busy')
    try {
      const r = await shredJobKey(jobId)
      setState(r.detail)
      if (onShredded) onShredded()
    } catch (e) {
      setState(e.message)
    }
  }

  return (
    <div className="shred">
      <button className="minor danger" onClick={shred} disabled={state === 'busy'}>
        🔥 Delete key (crypto-shred)
      </button>
      {state && state !== 'busy' && <span className="muted"> {state}</span>}
    </div>
  )
}

// The job-level stop button (2026-09-07, walk 1 row 64). Shown while any run is still
// in flight; says what it did in the control plane's own words.
function CancelJob({ jobId, runs }) {
  const [state, setState] = useState(null) // null | 'busy' | a sentence
  const live = runs.some((r) => !isTerminal(r))
  if (!live && !state) return null
  async function cancel() {
    if (!window.confirm('Stop this job? Runs that have not started end now; running ones are stopped by their worker.')) return
    setState('busy')
    try {
      const r = await cancelJob(jobId)
      setState(r.detail)
    } catch (e) {
      setState(e.message)
    }
  }
  return (
    <div className="shred">
      {live && (
        <button className="minor danger" onClick={cancel} disabled={state === 'busy'}>
          ■ Cancel job
        </button>
      )}
      {state && state !== 'busy' && <span className="muted"> {state}</span>}
    </div>
  )
}

// One run rendered as a card, so runs sit side by side for comparison.
function RunCard({ run, index, nodeName, privateJob, shredded, onView, refresh }) {
  const failed = run.failure_reason != null
  const inFlight = !isTerminal(run)
  return (
    <div className="runcard">
      <div className="runcard-head">
        <span className="runcard-title">run {index + 1}</span>
        <span className={`pill ${run.status.toLowerCase()}`}>{run.status}</span>
        {/* Open this run in the Runs and Live logs panels (walk 1, row 46). */}
        {onView && (
          <button className="minor" onClick={() => onView(run.run_id)} title="show this run's log above">
            ▤ log
          </button>
        )}
      </div>
      <div className="runcard-body">
        {run.cancel_requested_at && inFlight && (
          <div className="kv col">
            <span className="reason waiting">■ cancel requested</span>
            <span className="muted reasondetail">
              The worker stops it at its next check-in. If the worker never answers, the
              run ends as cancelled when its lease expires.
            </span>
          </div>
        )}
        <div className="kv"><span className="muted">Node</span><span>{nodeName || '—'}</span></div>
        <div className="kv"><span className="muted">Attempt</span><span>{run.attempt}</span></div>
        <div className="kv"><span className="muted">Exit</span><span>{run.exit_code ?? '—'}</span></div>
        <div className="kv"><span className="muted">Duration</span><span>{duration(run)}</span></div>
        {failed && (
          <div className="kv col">
            <span className={inFlight ? "muted" : "reason"}>{inFlight ? "previous attempt: " : ""}{run.failure_reason}</span>
            {run.failure_detail && <span className="muted reasondetail">{run.failure_detail}</span>}
          </div>
        )}
        {run.waiting_for && (
          /* 2026-09-06: a run aimed at a machine that is not here waits rather than
             failing -- the machine may come back. What it must not do is wait
             silently, which is what it did until today. */
          <div className="kv col">
            <span className="reason waiting">⏸ {run.waiting_for}</span>
            <span className="muted reasondetail">
              It is not blocking anything: other machines step past it and take the
              work they can. It runs the moment its machine is back.
            </span>
          </div>
        )}
        {run.escalation_count > 0 && (
          <div className="kv col">
            <span className="reason learned" title={learnedTitle(run)}>
              ↑ needs &gt; {run.learned_min_ram_mb} MB
            </span>
          </div>
        )}
        <div className="runcard-files">
          <div className="muted">Output files</div>
          {/* W6b: a private container has no writable folder on the machine, so it
              collects no files. Say why, rather than showing a bare "none" that
              looks like something went wrong. */}
          {privateJob ? (
            <p className="muted">
              none — a private run writes nothing to the machine; its results are in the logs
            </p>
          ) : (
            <Artifacts runId={run.run_id} status={run.status} attempt={run.attempt} refresh={refresh} shredded={shredded} />
          )}
          {/* Said once, so nobody looks for a checkpoint here (walk 1, row 63). */}
          <p className="muted small">
            Checkpoints are working state, kept only until the run finishes so a
            re-dispatched attempt can resume; they are never listed or downloadable.
          </p>
        </div>
      </div>
    </div>
  )
}

export default function Results({ onViewRun }) {
  const [jobs, setJobs] = useState([])
  // 2026-09-07: a job list that cannot be read says so. Before, every failure was
  // swallowed and a broken server looked exactly like "no jobs yet".
  const [jobsError, setJobsError] = useState(null)
  const [jobId, setJobId] = useState('')
  const [runs, setRuns] = useState([])
  const { nodes } = usePool()
  const names = Object.fromEntries(nodes.map((n) => [n.node_id, n.name]))
  const [refresh, setRefresh] = useState(0)
  const [shredded, setShredded] = useState({}) // job_id -> true once its key is deleted

  // Job list + node-name map (refresh the list every 5s so a fresh submit appears).
  useEffect(() => {
    let alive = true
    async function load() {
      try {
        const js = await getJobs()
        if (!alive) return
        setJobs(js)
        setJobsError(null)
      } catch (e) {
        // A 401 is sign-out, handled globally; anything else is shown until a poll
        // succeeds again.
        if (alive && e.status !== 401) setJobsError(e.message)
      }
    }
    load()
    const id = setInterval(load, 5000)
    return () => { alive = false; clearInterval(id) }
  }, [refresh])

  // Runs for the selected job — poll while any run is still in flight.
  useEffect(() => {
    setRuns([])
    if (!jobId) return
    let stop = false
    let timer
    async function tick() {
      try {
        const rs = await getJobRuns(jobId)
        if (stop) return
        setRuns(rs)
        if (rs.some((r) => !isTerminal(r))) timer = setTimeout(tick, 2500)
      } catch (e) {
        if ([401, 403, 404].includes(e.status)) return
        if (!stop) timer = setTimeout(tick, 2500)
      }
    }
    tick()
    return () => { stop = true; clearTimeout(timer) }
  }, [jobId])

  const job = jobs.find((j) => j.job_id === jobId) || null

  return (
    <div className="results">
      {jobsError && <p className="err" role="alert">cannot load jobs: {jobsError}</p>}
      <label className="results-pick">Job
        <select value={jobId} onChange={(e) => setJobId(e.target.value)}>
          <option value="">— pick a job to compare its runs —</option>
          {jobs.map((j) => (
            <option key={j.job_id} value={j.job_id}>
              {j.sealed || j.private ? '🔒 ' : ''}{j.name} · {j.status}
            </option>
          ))}
        </select>
      </label>
      {(job?.sealed || job?.private) && (
        <div className="privatebanner">
          <p>
            🔒 <b>Sealed</b> — this job’s
            data is encrypted with its own key: the input from the moment it was submitted, and
            the results and checkpoints from before they left the container. None of it has ever
            existed in readable form in storage or on any worker’s disk. The files below are
            opened for you here, on the way out.
            {job.trusted_only ? ' It runs only on machines an admin has marked trusted.' : ''}
          </p>
          {/* 2026-09-07 (walk 1, row 35): what this job carried and what it was charged
              for it — the sealed size, which is the number the storage bar counts. */}
          <p className="muted">
            {job.input_filename
              ? <>Input: <b>{job.input_filename}</b>{job.input_size_bytes != null
                  ? ` — ${bytes(job.input_size_bytes)} stored sealed and counted against your storage.`
                  : '.'}</>
              : 'Input: none — this job carries no dataset file.'}
          </p>
          <ShredKey
            key={job.job_id}
            jobId={job.job_id}
            onShredded={() => setShredded((s) => ({ ...s, [job.job_id]: true }))}
          />
        </div>
      )}
      {job && <CancelJob key={`cancel-${job.job_id}`} jobId={job.job_id} runs={runs} />}
      {job && <ReleaseStorage key={`release-${job.job_id}`} jobId={job.job_id} runs={runs} onReleased={() => { setRefresh((n) => n + 1); window.dispatchEvent(new Event('storage-released')) }} />}
      {jobId && !runs.length && <p className="muted">no runs yet</p>}
      {runs.length > 0 && (
        <div className="runcards">
          {runs.map((r, i) => (
            <RunCard
              key={r.run_id}
              run={r}
              refresh={refresh}
              index={i}
              nodeName={r.node_id ? names[r.node_id] : null}
              privateJob={!!job?.private && !job?.sealed}
              shredded={!!shredded[jobId]}
              onView={onViewRun ? (rid) => onViewRun(jobId, runs.map((x) => x.run_id), rid) : null}
            />
          ))}
        </div>
      )}
    </div>
  )
}

function ReleaseStorage({ jobId, runs, onReleased }) {
  const [busy, setBusy] = useState(false)
  const [message, setMessage] = useState(null)
  const finished = runs.length > 0 && runs.every(isTerminal)
  async function release() {
    if (!window.confirm('Permanently remove this job’s stored input, results, checkpoints and archived logs? This frees storage and cannot be undone.')) return
    setBusy(true); setMessage(null)
    try { await releaseJobStorage(jobId); setMessage('Storage released.') }
    catch (e) { setMessage(e.message) }
    finally { setBusy(false); onReleased() }
  }
  return <div className="shred">
    <button className="minor danger" disabled={!finished || busy} onClick={release}>{busy ? 'Releasing…' : 'Release storage'}</button>
    {!finished && <span className="muted"> Available when all runs have ended.</span>}
    {message && <p role="status">{message}</p>}
  </div>
}
