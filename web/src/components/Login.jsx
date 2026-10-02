import { useState } from 'react'
import { login } from '../api.js'

// W6: the login gate. Nothing in the app renders until a valid JWT is stored.
// Kept thin (the UI is the demo surface, not the contribution) — one form, one call.
export default function Login({ onLoggedIn }) {
  const [username, setUsername] = useState('admin')
  const [password, setPassword] = useState('')
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState(null)

  async function submit(e) {
    e.preventDefault()
    setBusy(true)
    setError(null)
    try {
      await login(username, password)
      onLoggedIn()
    } catch (err) {
      setError(err.message)
    } finally {
      setBusy(false)
    }
  }

  return (
    <div className="app login-page">
      <div className="stars" aria-hidden="true" />
      <div className="login-card panel">
        <h1><span className="sheen">Distributed Training Platform</span></h1>
        <p className="sub">Sign in to submit jobs and watch them run.</p>
        <form onSubmit={submit} className="form">
          <label>Username
            <input value={username} onChange={(e) => setUsername(e.target.value)} />
          </label>
          <label>Password
            <input type="password" autoFocus value={password} onChange={(e) => setPassword(e.target.value)} />
          </label>
          <button type="submit" disabled={busy}>{busy ? 'signing in…' : 'Sign in'}</button>
          {error && <p className="err">{error}</p>}
        </form>
      </div>
    </div>
  )
}
