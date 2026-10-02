// The run states nothing moves out of, in one place (2026-09-07). The reaper never
// rests a run at LOST today — it moves it on to PENDING or FAILED in the same
// transaction — but the enum exists, and every list on this side must agree on it.
export const TERMINAL = ['SUCCEEDED', 'FAILED', 'LOST']
export const isTerminal = (r) => TERMINAL.includes(r.status)
