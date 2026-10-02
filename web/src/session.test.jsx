import React from 'react'
import { afterEach, expect, test, vi } from 'vitest'
import { act, cleanup, render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import App from './App.jsx'

afterEach(() => { cleanup(); vi.unstubAllGlobals(); localStorage.clear() })
test('signing out and signing in as another user forgets the previous run', async () => {
  localStorage.setItem('fyp_token', 'first')
  vi.stubGlobal('WebSocket', class { static CONNECTING = 0; readyState = 1; close() {} })
  const fetcher = vi.fn(async (url, options) => {
    let data = []
    if (url.endsWith('/auth/login')) data = {token:'second'}
    else if (url.endsWith('/me')) data = {limits_accepted:true,is_admin:false,tier:'standard'}
    else if (url.endsWith('/jobs') && options?.method === 'POST') data = {job_id:'first-job',run_ids:['first-run']}
    else if (url.endsWith('/runs')) data = [{run_id:'first-run',status:'SUCCEEDED',attempt:1}]
    else if (url.endsWith('/first-job')) data = {checkpoint_advice:{verdict:'not_checked'}}
    return new Response(JSON.stringify(data))
  })
  vi.stubGlobal('fetch', fetcher)
  render(<App />)
  await screen.findByText(/Limits accepted/)
  expect(fetcher.mock.calls.filter(([url]) => url.endsWith('/nodes'))).toHaveLength(1)
  await userEvent.click(screen.getByRole('button', {name:/submit job/i}))
  await screen.findByRole('heading', {name:'Live logs'})
  await userEvent.click(screen.getByRole('button', {name:'Sign out'}))
  await userEvent.type(screen.getByLabelText('Password'), 'password')
  await userEvent.click(screen.getByRole('button', {name:/sign in/i}))
  await screen.findByRole('button', {name:'Sign out'})
  expect(screen.queryByRole('heading', {name:'Live logs'})).toBeNull()
  expect(screen.queryByText('run 1')).toBeNull()
})

test('submission finishing after sign-out cannot restore the old account job', async () => {
  localStorage.setItem('fyp_token', 'first')
  let finishSubmission
  vi.stubGlobal('fetch', vi.fn(async (url, options) => {
    if (url.endsWith('/jobs') && options?.method === 'POST') return new Promise((resolve) => { finishSubmission = resolve })
    const data = url.endsWith('/auth/login') ? {token:'second'} : url.endsWith('/me') ? {limits_accepted:true,is_admin:false,tier:'standard'} : []
    return new Response(JSON.stringify(data))
  }))
  render(<App />)
  await screen.findByText(/Limits accepted/)
  await userEvent.click(screen.getByRole('button', {name:/submit job/i}))
  await userEvent.click(screen.getByRole('button', {name:'Sign out'}))
  await act(async () => finishSubmission(new Response(JSON.stringify({job_id:'old-job',run_ids:['old-run']}))))
  await userEvent.type(screen.getByLabelText('Password'), 'password')
  await userEvent.click(screen.getByRole('button', {name:/sign in/i}))
  await screen.findByRole('button', {name:'Sign out'})
  expect(screen.queryByRole('heading', {name:'Live logs'})).toBeNull()
  expect(screen.queryByText('run 1')).toBeNull()
})
