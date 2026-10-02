import React from 'react'
import { afterEach, expect, test, vi } from 'vitest'
import { act, cleanup, fireEvent, render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import AdminPanel from './components/AdminPanel.jsx'
import NodePool from './components/NodePool.jsx'
import Results from './components/Results.jsx'
import RunLogs from './components/RunLogs.jsx'
import SubmitForm from './components/SubmitForm.jsx'

// 2026-09-07: the dashboard hardening audit. One test per defect, each written to
// fail on the code as it stood before the fix.

const json = (data, status = 200) => new Response(JSON.stringify(data), {status})
const me = {limits_accepted:true,tier:'standard',retained_cap_mb:100,scratch_cap_mb:100,retained_used_mb:0}
const job = {job_id:'j', name:'Training', status:'RUNNING'}
const run = {run_id:'r', status:'RUNNING', attempt:1}
afterEach(() => { cleanup(); vi.useRealTimers(); vi.unstubAllGlobals(); localStorage.clear() })

// --- 1. the submit button waits for the storage tier ---------------------------

test('submit stays disabled while the storage tier is still being read', async () => {
  vi.stubGlobal('fetch', vi.fn(async (url) => url.endsWith('/me') ? new Promise(() => {}) : json([])))
  render(<SubmitForm onSubmitted={() => {}} />)
  expect(await screen.findByText(/Reading your storage tier/)).toBeTruthy()
  expect(screen.getByRole('button', {name:/submit job/i}).disabled).toBe(true)
})

test('a failing tier read is shown and keeps submit disabled', async () => {
  vi.stubGlobal('fetch', vi.fn(async (url) => url.endsWith('/me') ? json({detail:'database unavailable'}, 500) : json([])))
  render(<SubmitForm onSubmitted={() => {}} />)
  const alert = await screen.findByRole('alert')
  expect(alert.textContent).toMatch(/database unavailable/)
  expect(alert.textContent).toMatch(/every few seconds/)
  expect(screen.getByRole('button', {name:/submit job/i}).disabled).toBe(true)
})

test('submit is enabled once the tier is read and its limits are accepted', async () => {
  vi.stubGlobal('fetch', vi.fn(async (url) => url.endsWith('/me') ? json(me) : json([])))
  render(<SubmitForm onSubmitted={() => {}} />)
  await screen.findByText(/Limits accepted/)
  expect(screen.getByRole('button', {name:/submit job/i}).disabled).toBe(false)
  expect(screen.queryByText(/Reading your storage tier/)).toBeNull()
})

// --- 2. the admin panel's background refresh must not reset the chosen tier ------

test('the ten-second refresh keeps the tier the admin chose', async () => {
  // A deployment whose tiers are not named "standard": the select's initial value is
  // absent from the list, which is what drives the stale closure in the old code.
  vi.useFakeTimers()
  const posts = []
  vi.stubGlobal('fetch', vi.fn(async (url, options) => {
    if (options?.method === 'POST') { posts.push(JSON.parse(options.body)); return json({username:'new', tier:posts[0].tier}) }
    if (url.endsWith('/tiers')) return json([{tier:'large',retained_cap_mb:1,scratch_cap_mb:1},{tier:'limited',retained_cap_mb:1,scratch_cap_mb:1}])
    return json([])
  }))
  render(<AdminPanel />)
  await act(async () => { await vi.advanceTimersByTimeAsync(1) })
  const select = screen.getByLabelText('Tier')
  expect(select.value).toBe('large')
  fireEvent.change(select, {target:{value:'limited'}})
  expect(select.value).toBe('limited')
  await act(async () => { await vi.advanceTimersByTimeAsync(10000) })
  expect(select.value).toBe('limited')
  fireEvent.change(screen.getByLabelText('Username'), {target:{value:'new'}})
  fireEvent.change(screen.getByLabelText('Password'), {target:{value:'pw'}})
  fireEvent.submit(select.closest('form'))
  await act(async () => { await vi.advanceTimersByTimeAsync(1) })
  expect(posts).toHaveLength(1)
  expect(posts[0].tier).toBe('limited')
})

// --- 3. a broken job list says so ------------------------------------------------

test('a persistent job-list error is shown and clears on the next good poll', async () => {
  vi.useFakeTimers()
  let broken = true
  vi.stubGlobal('fetch', vi.fn(async (url) => {
    if (url.endsWith('/jobs')) return broken ? json({detail:'database unavailable'}, 500) : json([job])
    return json([])
  }))
  render(<Results />)
  await act(async () => { await vi.advanceTimersByTimeAsync(1) })
  expect(screen.getByRole('alert').textContent).toBe('cannot load jobs: database unavailable')
  broken = false
  await act(async () => { await vi.advanceTimersByTimeAsync(5001) })
  expect(screen.queryByRole('alert')).toBeNull()
  expect(screen.getByRole('option', {name:/Training/})).toBeTruthy()
})

// --- 4. a late end message on a torn-down socket does not touch the status pill -----

test('an end message on the replaced socket cannot set a stale status', async () => {
  vi.useFakeTimers()
  const sockets = []
  vi.stubGlobal('WebSocket', class { static CONNECTING = 0; readyState = 1; constructor() { sockets.push(this) } close() {} })
  let attempt = 1
  vi.stubGlobal('fetch', vi.fn(async (url) => url.includes('/logs?') ? json([]) : json([{...run, attempt}])))
  render(<RunLogs jobId="j" runId="r" />)
  await act(async () => { await vi.advanceTimersByTimeAsync(1) })
  expect(sockets).toHaveLength(1)
  // A re-dispatch bumps the attempt; the status poll sees it and opens a new socket.
  attempt = 2
  await act(async () => { await vi.advanceTimersByTimeAsync(2000) })
  expect(sockets).toHaveLength(2)
  // The old socket delivers its buffered end message after it was replaced.
  await act(async () => sockets[0].onmessage({data:JSON.stringify({end:true, run_status:'LOST'})}))
  expect(screen.getByText('RUNNING')).toBeTruthy()
  expect(screen.queryByText('LOST')).toBeNull()
})

// --- 5. every list of terminal states agrees ----------------------------------------

test('a job whose runs are all LOST can release its storage', async () => {
  vi.stubGlobal('fetch', vi.fn(async (url) => {
    if (url.endsWith('/jobs')) return json([job])
    if (url.endsWith('/runs')) return json([{...run, status:'LOST'}])
    return json([])
  }))
  render(<Results />)
  await screen.findByRole('option', {name:/Training/})
  await userEvent.selectOptions(screen.getByRole('combobox'), 'j')
  await screen.findByText('no output files')
  expect(screen.getByRole('button', {name:'Release storage'}).disabled).toBe(false)
})

// --- 6. the CPU history forgets a machine that left the pool -------------------------

test('a machine that left the pool comes back with no CPU history', async () => {
  // The sparkline is the only window onto the history map: it draws from two
  // samples up, so a returning machine with one fresh sample must show none.
  vi.useFakeTimers()
  const a = (cpu) => ({node_id:'a', name:'A', online:true, usage:{cpu_pct:cpu}})
  const b = {node_id:'b', name:'B', online:true, usage:{cpu_pct:5}}
  let pool = [a(10)]
  vi.stubGlobal('fetch', vi.fn(async () => json(pool)))
  const {container} = render(<NodePool />)
  await act(async () => { await vi.advanceTimersByTimeAsync(1) })
  pool = [b]
  await act(async () => { await vi.advanceTimersByTimeAsync(3001) })
  pool = [a(20)]
  await act(async () => { await vi.advanceTimersByTimeAsync(3001) })
  // The sparkline reads the map during render, before the effect adds that poll's
  // sample, so the returning machine's history shows one poll later than it grows.
  pool = [a(30)]
  await act(async () => { await vi.advanceTimersByTimeAsync(3001) })
  expect(screen.getByText('A')).toBeTruthy()
  expect(container.querySelector('svg.spark')).toBeNull()
})
