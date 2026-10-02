import { useEffect, useRef, useState } from 'react'
import { usePool } from '../PoolContext.jsx'
import { getNodeEvents, setNodeTrusted } from '../api.js'

// Live node pool — polls GET /nodes every 3s. Online/offline is derived
// server-side; we just render it.
//
// W4 dashboard: each node now carries `hw_specs` (real identity, set at
// registration) and `usage` (live sample, refreshed each heartbeat). Both are
// best-effort dicts — anything a machine couldn't detect is simply absent, so
// every render guards with fallbacks. Click a row for the full spec sheet.

function gb(mb) {
  return mb ? `${(mb / 1024).toFixed(mb >= 1024 ? 0 : 1)} GB` : '—'
}

function hbAgo(ts) {
  const s = Math.max(0, Math.round((Date.now() - new Date(ts).getTime()) / 1000))
  return s < 60 ? `${s}s ago` : `${Math.round(s / 60)}m ago`
}

// Tiny live CPU history (last ~minute), built client-side from the 3s polls —
// no backend change, no history stored server-side (that stays stretch).
function Spark({ points }) {
  if (!points || points.length < 2) return null
  const w = 64
  const h = 14
  const xy = (v, i) =>
    `${((i / (points.length - 1)) * w).toFixed(1)},${(h - (Math.min(v, 100) / 100) * h).toFixed(1)}`
  const last = points[points.length - 1]
  return (
    <svg className="spark" width={w} height={h} viewBox={`0 0 ${w} ${h}`} aria-hidden="true">
      <polyline points={points.map(xy).join(' ')} fill="none" />
      <circle cx={w} cy={h - (Math.min(last, 100) / 100) * h} r="1.7" />
    </svg>
  )
}

// Three dancing bars — shown while a node reports itself busy executing runs.
function Eq() {
  return (
    <span className="eq" title="executing runs" aria-hidden="true"><i /><i /><i /></span>
  )
}

function UsageBar({ label, pct }) {
  if (pct == null) return null
  const level = pct >= 85 ? 'hot' : pct >= 60 ? 'warm' : ''
  return (
    <div className="usage" title={`${label} ${pct}%`}>
      <span className="usage-label">{label}</span>
      <span className="bar"><span className={`fill ${level}`} style={{ width: `${Math.min(pct, 100)}%` }} /></span>
      {/* key remounts the number when the value changes -> flash animation */}
      <span className="usage-pct" key={Math.round(pct)}>{Math.round(pct)}%</span>
    </div>
  )
}

// Battery text ("78% · charging"), or null when the machine has no battery.
function batteryText(n) {
  if (n.battery_pct == null) return null
  return `${n.battery_pct}%${n.battery_charging ? ' · charging' : ' · on battery'}`
}

// W5b node postmortem — goodbyes + classified comeback causes for this node.
function Events({ nodeId }) {
  const [events, setEvents] = useState(null)
  useEffect(() => {
    let alive = true
    getNodeEvents(nodeId).then((e) => alive && setEvents(e)).catch(() => alive && setEvents([]))
    return () => { alive = false }
  }, [nodeId])
  if (events == null) return <p className="muted">loading history…</p>
  if (!events.length) return <p className="muted">no recorded events</p>
  return (
    <ul className="events">
      {events.map((e, i) => (
        <li key={i}>
          <span className={`ev ${(e.cause || e.event).toLowerCase()}`}>{e.cause || e.event}</span>
          <span className="muted"> {e.evidence?.detail || e.event}</span>
        </li>
      ))}
    </ul>
  )
}

// W6b trust tier: the admin's switch for "may this machine run private jobs?".
// It lives here, in the pool, because trust is a judgment about a MACHINE — and it
// is deliberately the only way the flag can ever be set. Nothing a node reports
// about itself can turn it on.
function TrustToggle({ n, onChanged }) {
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState(null)
  async function flip(e) {
    e.stopPropagation() // don't also expand the row
    setBusy(true)
    setError(null)
    try {
      const updated = await setNodeTrusted(n.node_id, !n.trusted)
      onChanged(updated)
    } catch (e) {
      setError(e.message)
    } finally {
      setBusy(false)
    }
  }
  return (
    <>
    <button
      className={n.trusted ? 'trustbtn on' : 'trustbtn'}
      onClick={flip}
      disabled={busy}
      title={
        n.trusted
          ? 'Trusted: this machine may run private jobs. Click to withdraw trust (future placements only).'
          : 'Not trusted: private jobs will never be placed here. Click to trust it.'
      }
    >
      {n.trusted ? '🔒 trusted' : 'trust'}
    </button>
    {error && <span className="err" role="alert">{error}</span>}
    </>
  )
}

// Full spec sheet for one node — everything the agent managed to detect + W5b
// battery and the node's event history.
function Details({ n }) {
  const hw = n.hw_specs || {}
  const u = n.usage || {}
  const rows = [
    ['Machine model', hw.machine_model],
    ['Hostname', hw.hostname],
    ['CPU', hw.cpu_name],
    ['Cores / threads', hw.cpu_cores_physical && `${hw.cpu_cores_physical} cores / ${hw.cpu_threads ?? n.cpu_cores} threads`],
    ['GPU', hw.gpu_name ?? (n.has_gpu ? 'yes (unnamed)' : 'none')],
    ['RAM', `${gb(n.ram_mb)}${hw.ram_mhz ? ` @ ${hw.ram_mhz} MHz` : ''}`],
    ['Disk', hw.disk_total_gb && `${hw.disk_total_gb} GB total${u.disk_pct != null ? ` (${u.disk_pct}% used)` : ''}`],
    ['Free temporary disk', n.disk_free_mb != null ? `${n.disk_free_mb} MB` : null],
    ['Battery', batteryText(n)],
    // W6b: stated in words as well as the badge, so the spec sheet answers
    // "may a trusted-only job land here?" without needing the legend.
    ['Trusted-only jobs', n.trusted ? 'allowed (an admin trusted this machine)' : 'not allowed (not trusted)'],
    ['OS', hw.os],
    ['Python', hw.python_version],
    ['Docker', hw.docker_version],
    ['Agent', n.agent_outdated
      ? `${n.agent_version ?? '?'} — too old for today's jobs (needs ${n.min_agent_version}); upgrade it`
      : n.agent_version],
    ['Max parallel runs', n.capacity],
  ].filter(([, v]) => v != null && v !== '')
  return (
    <div className="specsheet">
      {rows.map(([k, v]) => (
        <div key={k} className="specrow"><span className="muted">{k}</span><span>{v}</span></div>
      ))}
      {u.gpu_pct != null && <div className="specrow"><span className="muted">GPU load</span><span>{u.gpu_pct}%</span></div>}
      <div className="eventswrap">
        <div className="muted">Event history</div>
        <Events nodeId={n.node_id} />
      </div>
    </div>
  )
}

// While a node is offline, a best-effort "likely" hint from the last black-box
// picture (derived at read time — nothing is written; nodes are never reaped).
function offlineHint(n) {
  if (n.online || n.battery_pct == null) return null
  if (n.battery_pct <= 15 && n.battery_charging === false) {
    return `battery ${n.battery_pct}% & discharging → likely battery`
  }
  return null
}

export default function NodePool({ isAdmin = false }) {
  const { nodes, setNodes, error } = usePool()
  const [open, setOpen] = useState(null)
  const hist = useRef({})
  useEffect(() => {
    // 2026-09-07: forget machines that left the pool, or the map grows for ever.
    const present = new Set(nodes.map((n) => n.node_id))
    for (const id of Object.keys(hist.current)) if (!present.has(id)) delete hist.current[id]
    for (const n of nodes) {
      if (n.online && n.usage?.cpu_pct != null) {
        const samples = (hist.current[n.node_id] ??= [])
        samples.push(n.usage.cpu_pct)
        if (samples.length > 20) samples.shift()
      }
    }
  }, [nodes])

  if (error) return <p className="err">cannot reach API: {error}</p>
  if (!nodes.length) return <p className="muted">no nodes yet — start an agent</p>

  return (
    <div className="tablewrap">
    <table className="pool">
      <thead>
        <tr>
          <th>Machine</th><th>Live</th><th>CPU</th><th>GPU</th><th>RAM</th><th>Load</th><th>Trusted</th>
        </tr>
      </thead>
      <tbody>
        {nodes.map((n, i) => {
          const hw = n.hw_specs || {}
          const u = n.online ? (n.usage || {}) : {}
          return [
            <tr
              key={n.node_id}
              className="poolrow"
              style={{ animationDelay: `${i * 70}ms` }}
              onClick={() => setOpen(open === n.node_id ? null : n.node_id)}
            >
              <td>
                <button className="disclosure" aria-expanded={open === n.node_id} aria-label={`Details for ${n.name}`} onClick={(e) => { e.stopPropagation(); setOpen(open === n.node_id ? null : n.node_id) }}>{n.name}</button>
                <div className="sub">{hw.machine_model ?? hw.os ?? ''}</div>
              </td>
              <td>
                <span className={n.online ? 'pill on' : 'pill off'}>
                  {n.online && <span className="beacon" />}
                  {n.online ? 'online' : 'offline'}
                </span>
                <div className="sub">
                  {/* An offline machine's last self-report ("busy") is not its state
                      now (walk 1, row 47); say when it was last heard from instead. */}
                  {n.online ? (
                    <>
                      {n.reported_status === 'busy' && <Eq />}
                      {n.reported_status}
                      {n.last_heartbeat ? ` · hb ${hbAgo(n.last_heartbeat)}` : ''}
                      {batteryText(n) ? ` · 🔋 ${batteryText(n)}` : ''}
                    </>
                  ) : (
                    n.last_heartbeat ? `last heard ${hbAgo(n.last_heartbeat)}` : 'never heard from'
                  )}
                </div>
                {offlineHint(n) && <div className="sub likely">{offlineHint(n)}</div>}
                {/* 2026-09-07 (walk 1, row 65): an agent too old for today's jobs is
                    offered nothing, and the pool used to show it as a healthy node. */}
                {n.agent_outdated && (
                  <div className="sub likely">
                    agent {n.agent_version ?? '?'} is too old — needs {n.min_agent_version}; it is
                    offered no new job until it is upgraded
                  </div>
                )}
              </td>
              <td>
                <div>{hw.cpu_name ?? `${n.cpu_cores} threads`}</div>
                {hw.cpu_name && <div className="sub">{hw.cpu_cores_physical ?? '?'}c / {hw.cpu_threads ?? n.cpu_cores}t</div>}
              </td>
              <td>{hw.gpu_name ? <span className="gputag">{hw.gpu_name}</span> : (n.has_gpu ? 'yes' : '—')}</td>
              <td>
                <div>{gb(n.ram_mb)}</div>
                {hw.ram_mhz && <div className="sub">{hw.ram_mhz} MHz</div>}
              </td>
              <td className="loadcell">
                {n.online && u.cpu_pct != null ? (
                  <>
                    <UsageBar label="CPU" pct={u.cpu_pct} />
                    <UsageBar label="RAM" pct={u.ram_pct} />
                    <UsageBar label="GPU" pct={u.gpu_pct} />
                    <div className="sparkrow" title="CPU, last minute">
                      <Spark points={hist.current[n.node_id]} />
                    </div>
                  </>
                ) : (
                  <span className="muted">{n.online ? 'n/a' : '—'}</span>
                )}
              </td>
              <td>
                {/* The switch is an ADMIN's judgment and the route refuses anyone
                    else (403), so a non-admin sees the state and no button (walk 1,
                    row 67: it used to show a button whose click failed silently). */}
                {isAdmin ? (
                  <TrustToggle
                    n={n}
                    onChanged={(u) =>
                      setNodes((ns) =>
                        ns.map((x) => (x.node_id === u.node_id ? { ...x, trusted: u.trusted } : x)),
                      )
                    }
                  />
                ) : (
                  <span className="muted">{n.trusted ? '🔒 trusted' : 'not trusted'}</span>
                )}
              </td>
            </tr>,
            open === n.node_id && (
              <tr key={`${n.node_id}-d`} className="detailrow">
                <td colSpan="7"><Details n={n} /></td>
              </tr>
            ),
          ]
        })}
      </tbody>
    </table>
    </div>
  )
}
