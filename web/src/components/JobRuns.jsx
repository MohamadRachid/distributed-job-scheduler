import { useEffect, useState } from 'react'
import { usePool } from '../PoolContext.jsx'
import { cancelJob, getJobRuns, getRunSamples } from '../api.js'
import { learnedTitle } from '../learned.js'
import { TERMINAL } from '../status.js'

// The two ways a run ends because its owner went past the caps they accepted, said
// in the words of the thing that stopped it. The control plane owns the first (it
// refused to keep more bytes); the agent owns the second (it watched a sample cross
// the run's temporary-disk ceiling). Both end the attempt for good: neither is
// re-dispatched, because the cap belongs to the user and not to the machine, so
// another machine would hit it too.
const QUOTA_STOP = {
  STORAGE_QUOTA_EXCEEDED:
    'The server refused to store this run’s output because your kept storage is full.',
  SCRATCH_QUOTA_EXCEEDED:
    'The run wrote more temporary disk on the worker than its limit allows.',
}

// A slim progress bar + latest metric for a running row (W5b ##PROGRESS contract).
function Progress({ run }) {
  // Completion is authoritative; a recovering run's prior progress belongs to its
  // previous attempt and must not look like work already done on the next one.
  if (['PENDING', 'ASSIGNED'].includes(run.status)) return null
  const succeeded = run.status === 'SUCCEEDED'
  if (run.progress == null && !succeeded) {
    if (TERMINAL.includes(run.status)) return null
    return <span className="muted">—</span>
  }
  const pct = succeeded ? 100 : Math.round(run.progress * 100)
  const loss = run.metrics_last?.loss
  return (
    <div className="prog" title={`${pct}%`}>
      <span className="progbar"><span className="progfill" style={{ width: `${pct}%` }} /></span>
      <span className="progpct">{pct}%{loss != null ? ` · loss ${loss}` : ''}</span>
    </div>
  )
}

// The "last resource picture" for a dead run — the last few samples, so the RAM
// climb to an OOM limit is visible (W5b). Fetched on demand when a row is opened.
function Samples({ runId }) {
  const [rows, setRows] = useState(null)
  useEffect(() => {
    let alive = true
    getRunSamples(runId).then((s) => alive && setRows(s)).catch(() => alive && setRows([]))
    return () => { alive = false }
  }, [runId])
  if (rows == null) return <p className="muted">loading samples…</p>
  if (!rows.length) return <p className="muted">no resource samples recorded</p>
  const last = rows.slice(-4)
  return (
    <table className="samples">
      <thead><tr><th>CPU</th><th>RAM used</th><th>RAM limit</th><th>Temporary disk</th></tr></thead>
      <tbody>
        {last.map((s, i) => (
          <tr key={i}>
            <td>{s.cpu_pct != null ? `${s.cpu_pct}%` : '—'}</td>
            <td>{s.mem_used_mb != null ? `${s.mem_used_mb} MB` : '—'}</td>
            <td>{s.mem_limit_mb != null ? `${s.mem_limit_mb} MB` : '—'}</td>
            <td>{s.scratch_used_mb != null ? `${s.scratch_used_mb} MB` : '—'}</td>
          </tr>
        ))}
      </tbody>
    </table>
  )
}

// W4/W5b demo surface: one row per run, so parallel dispatch is *visible*. W5b adds
// a live progress bar and, for a dead run, WHY it died + its last resource picture.
export default function JobRuns({ jobId }) {
  const [runs, setRuns] = useState([])
  const { nodes } = usePool()
  const names = Object.fromEntries(nodes.map((n) => [n.node_id, n.name]))
  const [open, setOpen] = useState(null)
  const [cancelMsg, setCancelMsg] = useState(null)
  const [error, setError] = useState(null)

  useEffect(() => {
    setRuns([]); setOpen(null); setCancelMsg(null); setError(null)
    let stop = false
    let timer
    async function tick() {
      try {
        const rs = await getJobRuns(jobId)
        if (stop) return
        setRuns(rs)
        if (rs.some((r) => !TERMINAL.includes(r.status))) {
          timer = setTimeout(tick, 2000)
        }
      } catch (e) {
        if (stop) return
        setError(e.message)
        if ([401, 403, 404].includes(e.status)) return
        if (!stop) timer = setTimeout(tick, 2000)
      }
    }
    tick()
    return () => {
      stop = true
      clearTimeout(timer)
    }
  }, [jobId])

  if (!runs.length) return error ? <p className="err">{error}</p> : null
  const live = runs.some((r) => !TERMINAL.includes(r.status))
  async function cancel() {
    if (!window.confirm('Stop this job? Runs that have not started end now; running ones are stopped by their worker.')) return
    setCancelMsg('busy')
    try {
      const r = await cancelJob(jobId)
      setCancelMsg(r.detail)
    } catch (e) {
      setCancelMsg(e.message)
    }
  }
  // A run stopped for going past the limits its owner accepted (2026-09-07). The
  // reason already reached the table as a bare enum in a tooltip, which is not a
  // way to tell somebody their work was thrown away. The two cases are different
  // and are named separately: one is the server refusing to keep more bytes, the
  // other is the worker stopping a run that filled its temporary disk.
  const quotaStop = runs.find((r) => QUOTA_STOP[r.failure_reason])
  return (
    <div className="tablewrap">
    {quotaStop && (
      <div className="quotastop" role="alert">
        <b>Stopped: you went past the storage limits you accepted.</b>{' '}
        {QUOTA_STOP[quotaStop.failure_reason]}{' '}
        The attempt is not tried again, and its workspace on the worker is gone. Free
        space by releasing a finished job&rsquo;s storage under Results, then submit again.
      </div>
    )}
    {/* 2026-09-07 (walk 1, row 64): the one control a running job was missing. */}
    {(live || (cancelMsg && cancelMsg !== 'busy')) && (
      <div className="shred">
        {live && (
          <button className="minor danger" onClick={cancel} disabled={cancelMsg === 'busy'}>
            ■ Cancel job
          </button>
        )}
        {cancelMsg && cancelMsg !== 'busy' && <span className="muted"> {cancelMsg}</span>}
      </div>
    )}
    <table>
      <thead>
        <tr><th>Run</th><th>Node</th><th>Status</th><th>Progress</th><th>Exit</th></tr>
      </thead>
      <tbody>
        {runs.map((r, i) => {
          const failed = r.failure_reason != null && TERMINAL.includes(r.status)
          return [
            <tr
              key={r.run_id}
              className={failed ? 'runrow clickable' : 'runrow'}
              onClick={failed ? () => setOpen(open === r.run_id ? null : r.run_id) : undefined}
            >
              <td>
                {failed ? <button className="disclosure" aria-expanded={open === r.run_id} aria-label={`Failure details for run ${i + 1}`} onClick={(e) => { e.stopPropagation(); setOpen(open === r.run_id ? null : r.run_id) }}>run {i + 1}</button> : <>run {i + 1}</>}
                {/* 2026-09-07 (walk 1, row 42): the fencing token, where the row used
                    to change machines without a word. Attempt 2 means the run was
                    re-dispatched — the first attempt's machine was lost. */}
                {r.attempt > 0 && (
                  <div className="sub">attempt {r.attempt}{r.attempt > 1 ? ' · re-dispatched' : ''}</div>
                )}
              </td>
              <td>{r.node_id ? (names[r.node_id] ?? r.node_id.slice(0, 8)) : '—'}</td>
              <td>
                {/* key on status -> the pill pops each time the state advances */}
                <span key={r.status} className={`pill ${r.status.toLowerCase()}`}>{r.status}</span>
                {!failed && r.failure_reason && <span className="muted"> previous attempt: {r.failure_reason}</span>}
                {failed && <span className="reason" title={r.failure_detail || ''}>{r.failure_reason}</span>}
                {/* 2026-09-07 (walk 1, rows 49 and 66): what a waiting run is waiting
                    for, in the table itself and not only on the results card. */}
                {r.waiting_for && <div className="sub waiting">⏸ {r.waiting_for}</div>}
                {r.cancel_requested_at && !TERMINAL.includes(r.status) && (
                  <div className="sub waiting">■ cancel requested — the worker stops it at its next check-in</div>
                )}
                {/* W5c: this run outgrew a weaker node — show the learned
                    requirement (info, not error). The hover text is read off the
                    run's own state: waiting, re-dispatched, or gave up. */}
                {r.escalation_count > 0 && (
                  <span
                    className="reason learned"
                    title={learnedTitle(r)}
                  >
                    ↑ needs &gt; {r.learned_min_ram_mb} MB
                  </span>
                )}
              </td>
              <td><Progress run={r} /></td>
              <td>{r.exit_code ?? ''}</td>
            </tr>,
            failed && open === r.run_id && (
              <tr key={`${r.run_id}-d`} className="detailrow">
                <td colSpan="5">
                  {r.failure_detail && <p className="reasondetail">{r.failure_detail}</p>}
                  <Samples runId={r.run_id} />
                </td>
              </tr>
            ),
          ]
        })}
      </tbody>
    </table>
    </div>
  )
}
