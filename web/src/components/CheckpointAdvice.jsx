import { useEffect, useState } from 'react'
import { getJob } from '../api.js'

// Checkpoint-use advice (2026-09-05, M. Ayli's suggestion of the same day).
//
// One line telling the submitter whether the script they pasted would actually
// survive its machine dying: does it save to the platform's checkpoint location, and
// does it load from there on start. A re-dispatched run that does neither is
// recovered, but its WORK is not — it trains again from the top.
//
// It is advice and nothing else. It never blocked the submission, never changed where
// the job was placed, and never touched a run. By the time this renders, the job is
// already queued.

const PHRASE = {
  not_checked: 'not checked — no script was pasted',
  no_checkpoint: 'no checkpointing found — a re-dispatched run would start again',
  saves_checkpoint: 'saves, but does not resume — a re-dispatched run would start again',
  resumes: 'saves and resumes — a re-dispatched run would carry on',
}

// What produced the verdict, named on the line itself so the reader knows how much
// evidence is behind it.
function source(advice) {
  if (advice.verdict === 'not_checked') return null
  return 'plain scan'
}

export default function CheckpointAdvice({ jobId }) {
  const [advice, setAdvice] = useState(null)

  useEffect(() => {
    let stop = false
    let timer
    // The advice is computed in the background after submit. Keep checking while mounted.
    async function tick() {
      try {
        const job = await getJob(jobId)
        if (stop) return
        if (job.checkpoint_advice) {
          setAdvice(job.checkpoint_advice)
          return
        }
      } catch (e) {
        if ([401, 403, 404].includes(e.status)) return
      }
      if (!stop) timer = setTimeout(tick, 2000)
    }
    setAdvice(null)
    tick()
    return () => {
      stop = true
      clearTimeout(timer)
    }
  }, [jobId])

  if (!advice) return null
  const how = source(advice)
  return (
    <p className="muted">
      <strong>Checkpoint use:</strong> {PHRASE[advice.verdict] ?? advice.verdict}
      {how ? ` (${how})` : ''}
      {advice.arm_a?.reason ? <><br />{advice.arm_a.reason}</> : null}
      {advice.disagreement ? <><br />{advice.disagreement}</> : null}
    </p>
  )
}
