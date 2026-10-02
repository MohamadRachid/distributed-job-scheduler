import { useEffect, useRef, useState } from 'react'
import { getLogsSince, getJobRuns, wsLogsUrl } from '../api.js'
import { TERMINAL } from '../status.js'

// Live log view for one run.
//
//  * Primary transport: WebSocket /runs/{id}/logs — the server replays stored
//    chunks then pushes new ones, ending with {end:true, run_status}.
//  * Fallback (risk register §21): if the socket errors/closes early, poll
//    GET /runs/{id}/logs?since_seq=N with a rising cursor. Same data, same order.
//
// Chunks are de-duplicated by (attempt, seq) and rendered sorted by (attempt, seq),
// so a line shows exactly once, in order, no matter how the posts arrived — the W3
// reliability point (brief §4). A separate 2s poll keeps the status pill fresh.
//
// A new attempt restarts seq at zero. Keep earlier output, reset the polling cursor,
// and advance it only with chunks belonging to the current attempt.
export default function RunLogs({ jobId, runId }) {
  const [lines, setLines] = useState([]) // sorted [{attempt, seq, chunk}]
  const [conn, setConn] = useState('connecting') // connecting|live|polling|closed
  const [runStatus, setRunStatus] = useState(null)
  const [progress, setProgress] = useState(null) // W5b: live 0..1

  const [epoch, setEpoch] = useState(0) // bumped on re-dispatch -> restarts the transport

  const seen = useRef(new Set()) // "attempt-seq" keys already shown
  const lastSeq = useRef(-1) // cursor for the polling fallback
  const attemptRef = useRef(null) // highest attempt this view has seen
  const terminalRef = useRef(false)
  const boxRef = useRef(null)
  const followRef = useRef(true)
  const unavailableRef = useRef(false)

  function ingest(chunks) {
    const fresh = []
    for (const c of chunks) {
      const key = `${c.attempt}-${c.seq}`
      if (c.attempt === attemptRef.current && c.seq > lastSeq.current) lastSeq.current = c.seq
      if (seen.current.has(key)) continue
      seen.current.add(key)
      fresh.push(c)
    }
    if (fresh.length) {
      setLines((prev) =>
        [...prev, ...fresh].sort((a, b) => a.attempt - b.attempt || a.seq - b.seq),
      )
    }
  }

  // Keep the status pill current in both transports (WS only reports status at end).
  useEffect(() => {
    let alive = true
    let timer
    async function poll() {
      if (!alive) return
      try {
        const runs = await getJobRuns(jobId)
        const r = runs.find((x) => x.run_id === runId)
        if (alive && !r) { unavailableRef.current = true; setConn('unavailable'); return }
        if (r && alive) {
          setRunStatus(r.status)
          setProgress(r.status === 'RUNNING' ? r.progress : null)
          // A re-dispatch bumped the fencing token: this run is being executed
          // again, from seq 0, on another node. Both cursors are now ahead of the
          // new attempt's output, so clear the pane and re-read from the start.
          if (r.attempt != null) {
            if (attemptRef.current == null) {
              attemptRef.current = r.attempt // first sighting — nothing to reset
            } else if (r.attempt > attemptRef.current) {
              attemptRef.current = r.attempt
              // This reset only ever ADDS. Two earlier versions cleared the pane
              // first and both lost the previous attempt's output, because what
              // survived then depended on when the socket's replay happened to
              // land — and in development React runs this effect twice, so the
              // clear could arrive in the middle of a replay. Nothing is removed
              // here, so the outcome no longer depends on timing or on how many
              // times this runs: `ingest` de-duplicates on (attempt, seq), so a
              // repeat is a no-op and the pane converges on every stored chunk.
              lastSeq.current = -1 // the polling fallback re-reads from the start
              try {
                ingest(await getLogsSince(runId, -1))
              } catch {
                /* transient — the restarted socket replays from the start anyway */
              }
              // Restart the socket too: its cursor lives on the SERVER, so only a
              // new connection replays from the beginning. Whatever the read above
              // already delivered is filtered out by `seen`, so nothing doubles.
              setEpoch((e) => e + 1)
            }
          }
          if (TERMINAL.includes(r.status)) {
            terminalRef.current = true
            return // stop status polling once terminal
          }
        }
      } catch (e) {
        if ([401, 403, 404].includes(e.status)) { unavailableRef.current = true; if (alive) setConn('unavailable'); return }
      }
      if (alive) timer = setTimeout(poll, 2000)
    }
    poll()
    return () => {
      alive = false
      clearTimeout(timer)
    }
  }, [jobId, runId])

  // Log transport: WebSocket, falling back to polling.
  useEffect(() => {
    let closed = false
    let ws = null
    let pollTimer = null
    let finished = false

    function startPolling() {
      if (closed || unavailableRef.current) return
      setConn('polling')
      async function tick() {
        if (closed || unavailableRef.current) return
        try {
          ingest(await getLogsSince(runId, lastSeq.current))
        } catch (e) {
          if ([401, 403, 404].includes(e.status)) { unavailableRef.current = true; if (!closed) setConn('unavailable'); return }
        }
        if (terminalRef.current) {
          // one last read to catch the final chunk, then stop
          try {
            ingest(await getLogsSince(runId, lastSeq.current))
          } catch {
            /* ignore */
          }
          if (!closed) setConn('closed')
          return
        }
        pollTimer = setTimeout(tick, 1000)
      }
      tick()
    }

    try {
      ws = new WebSocket(wsLogsUrl(runId))
      ws.onopen = () => {
        if (!closed) setConn('live')
      }
      ws.onmessage = (ev) => {
        const msg = JSON.parse(ev.data)
        if (msg.end) {
          finished = true
          // 2026-09-07: gated like the line under it. A replaced socket's buffered
          // end message used to overwrite the pill with the old attempt's status.
          if (!closed) setRunStatus(msg.run_status)
          if (!closed) setConn('closed')
          return
        }
        if (msg.error) return
        ingest([msg])
      }
      ws.onclose = () => {
        if (closed || finished) return
        startPolling() // socket dropped before the run finished — fall back
      }
      ws.onerror = () => {
        /* onclose runs next and handles the fallback */
      }
    } catch {
      startPolling()
    }

    return () => {
      closed = true
      if (ws) {
        try {
          // A socket that has not finished connecting cannot be closed quietly: the
          // browser logs "closed before the connection is established" (walk 1,
          // row 28 — once per run, harmless and alarming). Let it open, then close.
          if (ws.readyState === WebSocket.CONNECTING) {
            const sock = ws
            sock.onopen = () => sock.close()
            sock.onmessage = null
            sock.onclose = null
          } else {
            ws.close()
          }
        } catch {
          /* ignore */
        }
      }
      if (pollTimer) clearTimeout(pollTimer)
    }
    // `epoch` is in the dependency list on purpose: bumping it tears the socket
    // down and opens a new one, which makes the SERVER replay from the beginning.
    // Re-reading over the old socket would not help — its cursor is server-side.
  }, [runId, epoch])

  // Autoscroll to the newest line.
  useEffect(() => {
    if (boxRef.current && followRef.current) boxRef.current.scrollTop = boxRef.current.scrollHeight
  }, [lines])

  const connLabel =
    conn === 'unavailable' ? 'run unavailable' : conn === 'live'
      ? 'live (websocket)'
      : conn === 'polling'
        ? 'polling (fallback)'
        : conn === 'closed'
          ? 'finished'
          : 'connecting…'

  return (
    <div>
      <div className="logmeta">
        {/* key remounts the pill when status changes -> pop animation */}
        <span key={runStatus} className={`pill ${runStatus ? runStatus.toLowerCase() : 'running'}`}>
          {runStatus || 'running'}
        </span>
        <span className="muted"> · {connLabel}</span>
        <span className="muted"> · run {runId.slice(0, 8)}</span>
        {progress != null && !TERMINAL.includes(runStatus) && (
          <span className="muted"> · {Math.round(progress * 100)}%</span>
        )}
      </div>
      <pre ref={boxRef} className="logbox" onScroll={(e) => { const box = e.currentTarget; followRef.current = box.scrollHeight - box.scrollTop - box.clientHeight < 32 }}>
        {lines.length
          ? lines.map((l) => (
              // one span per chunk so fresh output slides in as it arrives
              <span key={`${l.attempt}-${l.seq}`} className="ln">{l.chunk}</span>
            ))
          : 'waiting for output…'}
        {/* Blinking block cursor while the run is still producing output. */}
        {conn !== 'closed' && <span className="cursor" />}
      </pre>
    </div>
  )
}
