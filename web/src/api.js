// Central API access for the UI.
//
// The control plane runs on :8000; the Vite dev server on :5173 reaches it
// cross-origin (dev CORS is open on the control plane). Override the
// host with ?api=https://HOST:8000 — same convention as the W1 static page.
//
// Encrypted transport (2026-08-22): the default follows THIS PAGE's own protocol
// rather than naming one. Serve the dashboard over https and the API calls and the
// log socket are https/wss; serve it over plain http and they are http/ws. That
// makes mixed content — an https page calling http, which browsers block outright —
// impossible to create by forgetting to change a second setting.
//
// W6: every user request now carries the login JWT. `authFetch` attaches it and, on
// any 401 (missing/expired token), clears it and calls the registered handler so the
// app drops back to the login screen. The WebSocket takes the token as ?token=
// (browsers can't set an Authorization header on a socket).

const params = new URLSearchParams(location.search)
export const API = (params.get('api') || `${location.protocol}//localhost:8000`).replace(/\/$/, '')
// http -> ws, https -> wss (so the live-log socket points at the same host).
export const WS_BASE = API.replace(/^http/, 'ws')

const TOKEN_KEY = 'fyp_token'
let onUnauthorized = null

export function setUnauthorizedHandler(fn) {
  onUnauthorized = fn
}
export function getToken() {
  return localStorage.getItem(TOKEN_KEY)
}
export function setToken(t) {
  localStorage.setItem(TOKEN_KEY, t)
}
export function clearToken() {
  localStorage.removeItem(TOKEN_KEY)
}

function authHeaders(extra = {}) {
  const t = getToken()
  return t ? { ...extra, Authorization: `Bearer ${t}` } : extra
}

// Shared 401 handling: forget the token and bounce to login.
function handleUnauthorized() {
  clearToken()
  if (onUnauthorized) onUnauthorized()
}

async function authFetch(url, options) {
  const token = getToken()
  const res = await fetch(url, options)
  // A response from a signed-out session must not clear a later session's token.
  if (res.status === 401 && token === getToken()) handleUnauthorized()
  return res
}

async function jsonOrThrow(res) {
  if (res.status === 401) {
    throw Object.assign(new Error('401 not logged in'), { status: 401 })
  }
  if (!res.ok) throw Object.assign(new Error(await errorText(res)), { status: res.status })
  return res.json()
}

// The words the control plane put in a refusal, without the status code in front of
// them (walk 1, row 52: the form showed "422 scratch_mb 6000 MB is above…"). A body
// with a `detail` sentence is shown as that sentence; a `{reason, detail}` object as
// its detail; anything else keeps the code so a strange failure is still traceable.
export async function errorText(res) {
  let body = null
  try {
    body = await res.json()
  } catch {
    /* body wasn't JSON */
  }
  const detail = body?.detail
  if (Array.isArray(detail)) return detail.map((e) => `${e.loc?.slice(-1)[0] || 'Request'}: ${e.msg || 'Invalid value'}`).join('; ')
  if (typeof detail === 'string') return detail
  if (detail && typeof detail === 'object') {
    if (typeof detail.detail === 'string') return detail.detail
    if (detail.reason) return `${detail.reason}${detail.used_mb != null ? ` (${detail.used_mb} MB used of ${detail.cap_mb} MB)` : ''}`
  }
  return `${res.status} ${res.statusText}`
}

// --- auth -------------------------------------------------------------------

export async function login(username, password) {
  const res = await fetch(`${API}/auth/login`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ username, password }),
  })
  if (res.status === 401) throw new Error('Invalid username or password')
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`)
  const data = await res.json()
  setToken(data.token)
  return data
}

export function logout() {
  clearToken()
}

// --- reads / writes (all authed) --------------------------------------------

export function getNodes() {
  return authFetch(`${API}/nodes`, { headers: authHeaders() }).then(jsonOrThrow)
}

export function createJob(body) {
  return authFetch(`${API}/jobs`, {
    method: 'POST',
    headers: authHeaders({ 'Content-Type': 'application/json' }),
    body: JSON.stringify(body),
  }).then(jsonOrThrow)
}

// W6b: submit a job whose input file is SEALED before anything is stored. The file
// rides as multipart alongside the same job spec `createJob` sends as JSON — a
// separate door because JSON cannot carry a file, not a different kind of job.
export function createPrivateJob(body, file) {
  const form = new FormData()
  form.append('spec', JSON.stringify(body))
  form.append('file', file, file.name)
  // No Content-Type header on purpose: the browser sets it with the boundary.
  return authFetch(`${API}/jobs/private`, {
    method: 'POST',
    headers: authHeaders(),
    body: form,
  }).then(jsonOrThrow)
}

// 2026-09-05: submit an ORDINARY job that carries a dataset file. The same
// multipart shape as `createPrivateJob` with one thing removed -- the sealing. A
// third door for the same reason there is a second one: JSON cannot carry a file.
// The file is stored as it arrives and the container reads it directly, so a user
// who only needs to hand their code a dataset does not pay for privacy they did not
// ask for (a private job gets no output files and cannot checkpoint).
export function createJobWithInput(body, file) {
  const form = new FormData()
  form.append('spec', JSON.stringify(body))
  form.append('file', file, file.name)
  // No Content-Type header on purpose: the browser sets it with the boundary.
  return authFetch(`${API}/jobs/with-input`, {
    method: 'POST',
    headers: authHeaders(),
    body: form,
  }).then(jsonOrThrow)
}

// W6b: mark (or unmark) a machine as trusted to run private jobs. Admin-only by
// nature — a node can never set this about itself.
export function setNodeTrusted(nodeId, trusted) {
  return authFetch(`${API}/nodes/${nodeId}/trusted`, {
    method: 'PATCH',
    headers: authHeaders({ 'Content-Type': 'application/json' }),
    body: JSON.stringify({ trusted }),
  }).then(jsonOrThrow)
}

// W6b crypto-shred: delete a private job's key. Every sealed copy of its input
// becomes permanently unreadable — irreversible, so the caller confirms first.
export function shredJobKey(jobId) {
  return authFetch(`${API}/jobs/${jobId}/key`, {
    method: 'DELETE',
    headers: authHeaders(),
  }).then(jsonOrThrow)
}

// --- storage quota policy (2026-09-04) --------------------------------------

// Where the signed-in user stands: tier, both caps, retained bytes used, and whether
// they have accepted the limits. The submit form reads this BEFORE it lets anything
// be submitted, because a cap the user cannot see is a trap — the platform would
// otherwise refuse work for a reason nothing on the screen had ever mentioned.
export function getMe() {
  return authFetch(`${API}/me`, { headers: authHeaders() }).then(jsonOrThrow)
}

// The one click that records agreement to the two numbers currently on the screen.
// Acceptance is stored as those NUMBERS, so if a tier moves it lapses on its own.
export function acceptLimits() {
  return authFetch(`${API}/me/accept-limits`, {
    method: 'POST',
    headers: authHeaders(),
  }).then(jsonOrThrow)
}

// Free everything a finished job is holding in storage — results, checkpoints,
// archived logs and any sealed input. Not crypto-shred: that removes readability and
// deliberately leaves the sealed bytes in place; this removes the bytes.
export function releaseJobStorage(jobId) {
  return authFetch(`${API}/jobs/${jobId}/storage`, {
    method: 'DELETE',
    headers: authHeaders(),
  }).then(jsonOrThrow)
}

// --- admin: users and tiers (2026-09-07, walk 1 row 14) ----------------------

export function getTiers() {
  return authFetch(`${API}/tiers`, { headers: authHeaders() }).then(jsonOrThrow)
}

export function getUsers() {
  return authFetch(`${API}/users`, { headers: authHeaders() }).then(jsonOrThrow)
}

export function createUser(username, password, tier) {
  return authFetch(`${API}/users`, {
    method: 'POST',
    headers: authHeaders({ 'Content-Type': 'application/json' }),
    body: JSON.stringify({ username, password, tier }),
  }).then(jsonOrThrow)
}

export function setUserTier(userId, tier) {
  return authFetch(`${API}/users/${userId}/tier`, {
    method: 'PATCH',
    headers: authHeaders({ 'Content-Type': 'application/json' }),
    body: JSON.stringify({ tier }),
  }).then(jsonOrThrow)
}

// Stop a job (2026-09-07, walk 1 row 64): runs that have not started end now; running
// ones are stopped by their worker at its next check-in, or ended when their lease
// expires if the worker never answers.
export function cancelJob(jobId) {
  return authFetch(`${API}/jobs/${jobId}/cancel`, {
    method: 'POST',
    headers: authHeaders(),
  }).then(jsonOrThrow)
}

export function getJobs() {
  return authFetch(`${API}/jobs`, { headers: authHeaders() }).then(jsonOrThrow)
}

// One job, including the checkpoint-use advice (2026-09-05). Separate from
// getJobRuns because the advice is a property of the JOB, computed once just after
// submission, while the runs list is polled for as long as anything is still moving.
export function getJob(jobId) {
  return authFetch(`${API}/jobs/${jobId}`, { headers: authHeaders() }).then(jsonOrThrow)
}

export function getJobRuns(jobId) {
  return authFetch(`${API}/jobs/${jobId}/runs`, { headers: authHeaders() }).then(jsonOrThrow)
}

// The WebSocket fallback (risk register §21): poll for chunks with seq > sinceSeq.
export function getLogsSince(runId, sinceSeq) {
  return authFetch(`${API}/runs/${runId}/logs?since_seq=${sinceSeq}`, {
    headers: authHeaders(),
  }).then(jsonOrThrow)
}

// W5b: a run's own resource samples (the "last picture" before a failure).
export function getRunSamples(runId, limit = 60) {
  return authFetch(`${API}/runs/${runId}/samples?limit=${limit}`, {
    headers: authHeaders(),
  }).then(jsonOrThrow)
}

// W5b: a node's postmortem history (goodbyes + classified comeback causes).
export function getNodeEvents(nodeId, limit = 20) {
  return authFetch(`${API}/nodes/${nodeId}/events?limit=${limit}`, {
    headers: authHeaders(),
  }).then(jsonOrThrow)
}

// W6: a run's collected output files.
export function getRunArtifacts(runId) {
  return authFetch(`${API}/runs/${runId}/artifacts`, { headers: authHeaders() }).then(jsonOrThrow)
}

// W6: download an artifact THROUGH the control plane (brokered — the browser never
// talks to MinIO). Fetch the bytes with the auth header, then save as a file.
export async function downloadArtifact(artifactId, filename) {
  const res = await authFetch(`${API}/artifacts/${artifactId}/download`, { headers: authHeaders() })
  if (res.status === 401) {
    throw Object.assign(new Error('401 not logged in'), { status: 401 })
  }
  // The control plane's own sentence — "this job's key was deleted…" — and not a
  // bare "410 Gone" (walk 1, row 62: the click did nothing visible at all).
  if (!res.ok) throw Object.assign(new Error(await errorText(res)), { status: res.status })
  const blob = await res.blob()
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = filename
  document.body.appendChild(a)
  a.click()
  a.remove()
  setTimeout(() => URL.revokeObjectURL(url), 1000)
}

// W6: the live-log socket URL, carrying the JWT as ?token=.
export function wsLogsUrl(runId) {
  const t = getToken() || ''
  return `${WS_BASE}/runs/${runId}/logs?token=${encodeURIComponent(t)}`
}
