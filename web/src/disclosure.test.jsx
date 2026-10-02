import React from 'react'
import { afterEach, expect, test, vi } from 'vitest'
import { cleanup, render, screen } from '@testing-library/react'
import SubmitForm from './components/SubmitForm.jsx'

// 2026-09-08: the submit page tells the user what happens to the two kinds of text
// they type in, at the point where they type it. `source_text`, the dataset address
// and every env value are stored readable; the pasted script is additionally returned
// by no route and deleted by no user-facing route.
// Fixed text, no logic — so the test asserts the rendered strings and nothing else.

const json = (data, status = 200) => new Response(JSON.stringify(data), {status})
const me = {limits_accepted:true,tier:'standard',retained_cap_mb:100,scratch_cap_mb:100,retained_used_mb:0}
afterEach(() => { cleanup(); vi.unstubAllGlobals(); localStorage.clear() })

const renderForm = () => {
  vi.stubGlobal('fetch', vi.fn(async (url) => String(url).endsWith('/me') ? json(me) : json([])))
  render(<SubmitForm onSubmitted={() => {}} />)
}

test('the pasted training script carries its disclosure where it is pasted', async () => {
  renderForm()
  const said = await screen.findByText(/What happens to this text/)
  const text = said.textContent.replace(/\s+/g, ' ')
  expect(text).toBe(
    "What happens to this text: it is stored readable in the platform's database and kept. " +
    'It is checked on this server against twelve fixed patterns and nothing from it is sent ' +
    'anywhere else. It is not shown back to you and the platform does not delete it. ' +
    'An administrator can read it. Do not paste anything you would not want kept.'
  )
})

test('the dataset address and the env settings both say they are stored readable', async () => {
  renderForm()
  const warnings = await screen.findAllByText('Stored readable. Do not put a password or a token here.')
  expect(warnings).toHaveLength(2)
})
