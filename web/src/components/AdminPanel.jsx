import { useEffect, useState } from 'react'
import { createUser, getTiers, getUsers, setUserTier } from '../api.js'

// The admin's two jobs from the interface (2026-09-07, walk 1 row 14): create a user,
// and put a user in a storage tier. Both routes existed since 2026-09-04; nothing on
// the page reached them, so the first stranger had to find them in the API's own
// documentation. Shown to admins only — the routes answer 403 to anyone else.
//
// Kept thin on purpose (the UI is the demo surface, not the contribution): one list,
// one form, one select per row.

function tierLine(t) {
  const gb = (mb) => (mb >= 1024 ? `${Math.round(mb / 1024)} GB` : `${mb} MB`)
  return `${t.tier} — ${gb(t.retained_cap_mb)} kept on the server, ${gb(t.scratch_cap_mb)} temporary disk per run`
}

export default function AdminPanel() {
  const [users, setUsers] = useState(null)
  const [tiers, setTiers] = useState([])
  const [username, setUsername] = useState('')
  const [password, setPassword] = useState('')
  const [tier, setTier] = useState('standard')
  const [busy, setBusy] = useState(false)
  const [msg, setMsg] = useState(null)
  const [err, setErr] = useState(null)
  const [pendingTiers, setPendingTiers] = useState({})

  // Fetches and nothing else (2026-09-07). It used to also default the tier select,
  // but the interval below captures this function once, so it kept comparing against
  // the select's FIRST value for ever and could reset a choice the admin had made.
  async function load() {
    try {
      const [us, ts] = await Promise.all([getUsers(), getTiers()])
      setUsers(us)
      setTiers(ts)
    } catch (e) {
      setErr(e.message)
    }
  }

  useEffect(() => {
    load()
    const id = setInterval(load, 10000)
    return () => clearInterval(id)
  }, [])

  // The select's default, reading the CURRENT tier: only when its value is not in
  // the list (first load, or a tier that no longer exists) does it fall to the first.
  useEffect(() => {
    if (tiers.length && !tiers.some((t) => t.tier === tier)) setTier(tiers[0].tier)
  }, [tiers, tier])

  async function create(e) {
    e.preventDefault()
    setBusy(true)
    setErr(null)
    setMsg(null)
    try {
      const u = await createUser(username.trim(), password, tier)
      setMsg(`created ${u.username} in the ${u.tier} tier — they accept its two limits on the submit form before their first job`)
      setUsername('')
      setPassword('')
      await load()
    } catch (e2) {
      setErr(e2.message)
    } finally {
      setBusy(false)
    }
  }

  async function moveTier(u, next) {
    setPendingTiers((p) => ({ ...p, [u.user_id]: next }))
    setErr(null)
    setMsg(null)
    try {
      const updated = await setUserTier(u.user_id, next)
      setUsers((us) => us.map((user) => user.user_id === updated.user_id ? updated : user))
      setMsg(`${updated.username} is now in the ${updated.tier} tier — they are asked to accept its limits again`)
      await load()
    } catch (e) {
      setErr(e.message)
    } finally {
      setPendingTiers((p) => { const next = { ...p }; delete next[u.user_id]; return next })
    }
  }

  return (
    <div className="admin">
      <p className="muted">
        Accounts are made here — there is no sign-up page. A tier is two numbers: how much
        the server keeps for the user, and how much temporary disk one of their runs may use.
      </p>
      {tiers.length > 0 && (
        <ul className="tiers">
          {tiers.map((t) => <li key={t.tier} className="muted">{tierLine(t)}</li>)}
        </ul>
      )}
      <form onSubmit={create} className="form adminform">
        <div className="row">
          <label>Username<input value={username} onChange={(e) => setUsername(e.target.value)} required /></label>
          <label>Password<input type="password" value={password} onChange={(e) => setPassword(e.target.value)} required /></label>
          <label>Tier
            <select value={tier} onChange={(e) => setTier(e.target.value)}>
              {tiers.map((t) => <option key={t.tier} value={t.tier}>{t.tier}</option>)}
            </select>
          </label>
        </div>
        <button type="submit" disabled={busy || !username.trim() || !password}>
          {busy ? 'creating…' : 'Create user'}
        </button>
      </form>
      {users == null ? (
        <p className="muted">loading users…</p>
      ) : (
        <div className="tablewrap">
          <table>
            <thead>
              <tr><th>User</th><th>Role</th><th>Tier</th><th>Limits accepted</th></tr>
            </thead>
            <tbody>
              {users.map((u) => (
                <tr key={u.user_id}>
                  <td>{u.username}</td>
                  <td>{u.is_admin ? 'admin' : 'user'}</td>
                  <td>
                    <select aria-label={`Tier for ${u.username}`} disabled={pendingTiers[u.user_id] != null} value={pendingTiers[u.user_id] ?? u.tier} onChange={(e) => moveTier(u, e.target.value)}>
                      {tiers.map((t) => <option key={t.tier} value={t.tier}>{t.tier}</option>)}
                    </select>
                  </td>
                  <td>{u.limits_accepted ? '✓ yes' : 'not yet — they accept on the submit form'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
      {msg && <p className="muted">{msg}</p>}
      {err && <p className="err">{err}</p>}
    </div>
  )
}
