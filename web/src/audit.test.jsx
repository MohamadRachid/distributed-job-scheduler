import React from 'react'
import { afterEach, expect, test, vi } from 'vitest'
import { cleanup, render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { errorText, getJobRuns } from './api.js'
import NodePool from './components/NodePool.jsx'

afterEach(() => { cleanup(); vi.unstubAllGlobals() })
test('validation errors name the invalid field', async () => {
  expect(await errorText(new Response(JSON.stringify({detail:[{loc:['body','replicas'],msg:'Must be at least 1'}]}), {status:422}))).toBe('replicas: Must be at least 1')
})
test('API errors retain status for poll termination', async () => {
  vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response('{"detail":"not found"}', {status:404})))
  await expect(getJobRuns('missing')).rejects.toMatchObject({status:404})
})
test('keyboard trust activation does not expand the node row', async () => {
  vi.stubGlobal('fetch', vi.fn(async (_url, options) => new Response(JSON.stringify(String(_url).includes('/events') ? [] : options?.method === 'PATCH' ? {node_id:'n',trusted:true} : [{node_id:'n',name:'Worker',online:true,capacity:1}]))))
  render(<NodePool isAdmin />)
  const trust = await screen.findByRole('button', {name:'trust'})
  trust.focus()
  await userEvent.keyboard('{Enter}')
  await waitFor(() => expect(screen.getByRole('button', {name:/trusted/})).toBeTruthy())
  expect(screen.queryByText('Machine model')).toBeNull()
  expect(screen.queryByText('Event history')).toBeNull()
})
test('failed trust change is visible and remains retryable', async () => {
  vi.stubGlobal('fetch', vi.fn(async (_url, options) => options?.method === 'PATCH'
    ? new Response('{"detail":"Trust service unavailable"}', {status:503})
    : new Response(JSON.stringify([{node_id:'n',name:'Worker',online:true}]))))
  render(<NodePool isAdmin />)
  await userEvent.click(await screen.findByRole('button', {name:'trust'}))
  expect(await screen.findByRole('alert')).toHaveProperty('textContent', 'Trust service unavailable')
  expect(screen.getByRole('button', {name:'trust'}).disabled).toBe(false)
})
