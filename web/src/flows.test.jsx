import React from 'react'
import { afterEach, expect, test, vi } from 'vitest'
import { act, cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import Results from './components/Results.jsx'
import RunLogs from './components/RunLogs.jsx'
import JobRuns from './components/JobRuns.jsx'
import SubmitForm from './components/SubmitForm.jsx'
import CheckpointAdvice from './components/CheckpointAdvice.jsx'
import AdminPanel from './components/AdminPanel.jsx'
import { getJobRuns, getToken, setToken, setUnauthorizedHandler } from './api.js'

const json = (data, status = 200) => new Response(JSON.stringify(data), {status})
const job = {job_id:'j', name:'Training', status:'RUNNING', sealed:true}
const run = {run_id:'r', status:'RUNNING', attempt:1}
afterEach(() => { cleanup(); vi.useRealTimers(); vi.unstubAllGlobals(); localStorage.clear(); setUnauthorizedHandler(null) })

test('a late unauthorized response cannot sign out a newly signed-in account', async () => {
  let respond
  const unauthorized = vi.fn()
  setUnauthorizedHandler(unauthorized)
  setToken('old')
  vi.stubGlobal('fetch', vi.fn(() => new Promise((resolve) => { respond = resolve })))
  const request = getJobRuns('j')
  setToken('new')
  respond(json({detail:'expired'}, 401))
  await expect(request).rejects.toMatchObject({status:401})
  expect(getToken()).toBe('new')
  expect(unauthorized).not.toHaveBeenCalled()
})

test('results refresh files on completion and release even after a partial failure', async () => {
  let status = 'RUNNING', released = false
  const refreshQuota = vi.fn()
  window.addEventListener('storage-released', refreshQuota)
  vi.spyOn(window, 'confirm').mockReturnValue(true)
  vi.stubGlobal('fetch', vi.fn(async (url, options) => {
    if (options?.method === 'DELETE') { released = true; return json({detail:'One object could not be deleted. Please retry.'}, 503) }
    if (url.endsWith('/nodes')) return json([])
    if (url.endsWith('/jobs')) return json([{...job, status}])
    if (url.endsWith('/runs')) return json([{...run, status}])
    if (url.endsWith('/artifacts')) return json(status === 'RUNNING' || released ? [] : [{artifact_id:'a',filename:'model.bin',size:5}])
    throw Error(url)
  }))
  render(<Results />)
  await screen.findByRole('option', {name:/Training/})
  await userEvent.selectOptions(screen.getByRole('combobox'), 'j')
  await screen.findByText('no output files')
  expect(screen.getByRole('button', {name:'Release storage'}).disabled).toBe(true)
  status = 'SUCCEEDED'
  await screen.findByRole('button', {name:/model.bin/}, {timeout:4000})
  await userEvent.click(screen.getByRole('button', {name:'Release storage'}))
  await screen.findByText(/One object could not be deleted/)
  await screen.findByText('no output files')
  expect(refreshQuota).toHaveBeenCalledOnce()
  window.removeEventListener('storage-released', refreshQuota)
})

test('changing Results jobs clears destructive-action feedback and old rows', async () => {
  vi.spyOn(window, 'confirm').mockReturnValue(true)
  vi.stubGlobal('fetch', vi.fn(async (url, options) => {
    if (options?.method === 'DELETE') return json({detail:'Key deleted for first job'})
    if (url.endsWith('/nodes') || url.endsWith('/artifacts')) return json([])
    if (url.endsWith('/jobs')) return json([job, {...job,job_id:'j2',name:'Second'}])
    if (url.includes('/j2/runs')) return new Promise(() => {})
    return json([{...run,status:'SUCCEEDED',node_id:'oldnode'}])
  }))
  render(<Results />)
  await screen.findByRole('option', {name:/Training/})
  await userEvent.selectOptions(screen.getByRole('combobox'), 'j')
  await userEvent.click(await screen.findByRole('button', {name:/Delete key/}))
  await screen.findByText('Key deleted for first job')
  await userEvent.selectOptions(screen.getByRole('combobox'), 'j2')
  expect(screen.queryByText('Key deleted for first job')).toBeNull()
  expect(screen.queryByText('run 1')).toBeNull()
})

test('recovering run shows the previous reason without stale progress or failure control', async () => {
  vi.stubGlobal('fetch', vi.fn(async (url) => json(url.endsWith('/nodes') ? [] : [{...run,status:'PENDING',progress:0.8,failure_reason:'NODE_LOST'}])))
  render(<JobRuns jobId="j" />)
  await screen.findByText(/previous attempt: NODE_LOST/)
  expect(screen.queryByText('80%')).toBeNull()
  expect(screen.queryByRole('button', {name:/Failure details/})).toBeNull()
})

test('unavailable runs stop both status and fallback log polling', async () => {
  vi.useFakeTimers()
  vi.stubGlobal('WebSocket', class { constructor() { throw Error('offline') } })
  const fetcher = vi.fn(async () => json({detail:'not found'}, 404))
  vi.stubGlobal('fetch', fetcher)
  render(<RunLogs jobId="j" runId="r" />)
  await act(async () => { await vi.advanceTimersByTimeAsync(1) })
  const requests = fetcher.mock.calls.length
  await act(async () => { await vi.advanceTimersByTimeAsync(10000) })
  expect(fetcher).toHaveBeenCalledTimes(requests)
  expect(screen.getByText(/run unavailable/)).toBeTruthy()
})

test('fallback reads early chunks from the next attempt despite old high sequence numbers', async () => {
  vi.useFakeTimers()
  vi.stubGlobal('WebSocket', class { constructor() { throw Error('offline') } })
  let attempt = 1, chunks = [{attempt:1,seq:90,chunk:'old attempt\n'}]
  const cursors = []
  vi.stubGlobal('fetch', vi.fn(async (url) => {
    if (url.includes('/logs?')) {
      const since = Number(new URL(url).searchParams.get('since_seq'))
      cursors.push(since)
      return json(chunks.filter((c) => c.seq > since))
    }
    return json([{...run,attempt}])
  }))
  render(<RunLogs jobId="j" runId="r" />)
  await act(async () => { await vi.advanceTimersByTimeAsync(1001) })
  attempt = 2
  chunks = [...chunks,{attempt:2,seq:0,chunk:'new zero\n'}]
  await act(async () => { await vi.advanceTimersByTimeAsync(1000) })
  chunks = [...chunks,{attempt:2,seq:1,chunk:'new one\n'}]
  await act(async () => { await vi.advanceTimersByTimeAsync(1100) })
  expect(screen.getByText('new zero')).toBeTruthy()
  expect(screen.getByText('new one')).toBeTruthy()
  expect(cursors.slice(-2)).not.toContain(90)
})

test('advice remains discoverable after the former five-second cutoff', async () => {
  vi.useFakeTimers()
  let ready = false
  vi.stubGlobal('fetch', vi.fn(async () => json({checkpoint_advice:ready ? {verdict:'resumes'} : null})))
  render(<CheckpointAdvice jobId="j" />)
  await act(async () => { await vi.advanceTimersByTimeAsync(6000) })
  ready = true
  await act(async () => { await vi.advanceTimersByTimeAsync(2000) })
  expect(screen.getByText(/saves and resumes/)).toBeTruthy()
})

test('blank replicas submit one run and target selection explains placement constraints', async () => {
  const posts = []
  vi.stubGlobal('fetch', vi.fn(async (url, options) => {
    if (options?.method === 'POST') { posts.push(JSON.parse(options.body)); return json({job_id:'j',run_ids:['r']}) }
    if (url.endsWith('/nodes')) return json([{node_id:'n',name:'Worker',online:true,trusted:false}])
    return json({limits_accepted:true,tier:'standard',retained_cap_mb:100,scratch_cap_mb:100,retained_used_mb:0})
  }))
  render(<SubmitForm onSubmitted={() => {}} />)
  await screen.findByText(/Limits accepted/)
  fireEvent.change(screen.getByLabelText('Replicas'), {target:{value:''}})
  fireEvent.submit(screen.getByLabelText('Replicas').closest('form'))
  await waitFor(() => expect(posts).toHaveLength(1))
  expect(posts[0].replicas).toBe(1)
  await userEvent.click(screen.getByRole('checkbox', {name:/Worker/}))
  expect(screen.getByLabelText('Replicas').disabled).toBe(true)
  expect(screen.getByText(/One run per selected machine/)).toBeTruthy()
})

test('incoming output respects scrolling up and follows again at the bottom', async () => {
  let socket
  vi.stubGlobal('WebSocket', class { static CONNECTING = 0; readyState = 1; constructor() { socket = this } close() {} })
  vi.stubGlobal('fetch', vi.fn(async () => json([run])))
  const {container} = render(<RunLogs jobId="j" runId="r" />)
  const box = container.querySelector('pre')
  Object.defineProperties(box, {scrollHeight:{value:1000,configurable:true},clientHeight:{value:100}})
  box.scrollTop = 100
  fireEvent.scroll(box)
  await act(async () => socket.onmessage({data:JSON.stringify({attempt:1,seq:0,chunk:'first'})}))
  expect(box.scrollTop).toBe(100)
  box.scrollTop = 900
  fireEvent.scroll(box)
  await act(async () => socket.onmessage({data:JSON.stringify({attempt:1,seq:1,chunk:'second'})}))
  expect(box.scrollTop).toBe(1000)
})

test('successful tier change remains selected when the following list refresh fails', async () => {
  let changed = false
  const user = {user_id:'u', username:'Member', tier:'standard'}
  vi.stubGlobal('fetch', vi.fn(async (url, options) => {
    if (options?.method === 'PATCH') { changed = true; return json({...user,tier:'large'}) }
    if (url.endsWith('/tiers')) return json([{tier:'standard'},{tier:'large'}])
    return changed ? json({detail:'Refresh unavailable'},503) : json([user])
  }))
  render(<AdminPanel />)
  const tier = await screen.findByRole('combobox', {name:'Tier for Member'})
  await userEvent.selectOptions(tier,'large')
  await screen.findByText('Refresh unavailable')
  expect(tier.value).toBe('large')
  expect(tier.disabled).toBe(false)
})

test('terminal failure keeps the diagnostic stopping percentage', async () => {
  vi.stubGlobal('fetch', vi.fn(async (url) => json(url.endsWith('/nodes') ? [] : [{...run,status:'FAILED',progress:0.8,failure_reason:'OOM'}])))
  render(<JobRuns jobId="j" />)
  expect(await screen.findByText('80%')).toBeTruthy()
})
