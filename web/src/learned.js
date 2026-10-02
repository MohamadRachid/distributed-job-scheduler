// W5c/FR-12: the hover text behind the "↑ needs > N MB" badge.
//
// A run that died of a proven RAM shortage has THREE possible endings, not two
// and the badge looks identical in all three:
//
//   1. waiting      — a big enough machine is registered but not available right
//                     now, so the run sits PENDING carrying its learned floor;
//   2. re-dispatched — it was placed on a bigger machine and ran there;
//   3. gave up      — no registered machine is big enough, so it failed at once
//                     with INSUFFICIENT_POOL.
//
// Until 2026-08-13 the hover said "re-dispatched to a node with more than N MB"
// in ALL THREE, so while a run was visibly waiting the interface claimed a
// re-dispatch that had not happened. The text is now read off the run's own
// state instead of being inferred from the badge.
//
// What it still does NOT say: WHICH machine it is waiting for, or how long the
// wait has run. That gap is real and stays as future work.
export function learnedTitle(run) {
  const mb = run.learned_min_ram_mb

  if (run.status === 'PENDING') {
    return (
      `Ran out of memory on a weaker machine. Waiting for a machine with more ` +
      `than ${mb} MB to become available — this run has not been re-dispatched yet.`
    )
  }

  if (run.status === 'FAILED' && run.failure_reason === 'INSUFFICIENT_POOL') {
    return (
      `Ran out of memory again after being moved to a bigger machine, and no ` +
      `registered machine in the pool is big enough — so the run was given up. ` +
      `The failure reason carries the exact size it needed.`
    )
  }

  return `Died of a RAM shortage on a weaker node; re-dispatched to a node with more than ${mb} MB.`
}
