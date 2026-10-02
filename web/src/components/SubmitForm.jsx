import { usePool } from '../PoolContext.jsx'
import { useEffect, useRef, useState } from 'react'
import { acceptLimits, createJob, createJobWithInput, getMe } from '../api.js'

// The locked honesty guard. It says exactly what sealing
// protects against and exactly what it does not, and it is never softened.
//
// 2026-09-06: it describes EVERY job now rather than an option, because sealing is
// the floor. The last sentence is the one that must never be dropped.
const PRIVACY_NOTE =
  'Every job’s data is sealed with its own key: the file you upload is sealed the ' +
  'moment you submit it, and the results and checkpoints your run produces are ' +
  'sealed inside the container before they leave it. Storage, the network and the ' +
  'worker’s disk only ever hold sealed bytes; you download your results in the ' +
  'clear from here. If any byte is changed the seal breaks and the run fails. This ' +
  'protects the data from users of the machine — not from its root administrator, ' +
  'who can read a running container’s memory. Root-proof privacy needs special ' +
  'hardware (TEE), which is future work.'

// The one sentence of that note which must be on screen without a click: what sealing
// does NOT protect against. The rest is carried whole by <More> below, unchanged.
// Making the form shorter must never make what we promise shorter.
const PRIVACY_SHORT =
  'Every job is sealed with its own key. That protects your data from users of the ' +
  'machine — not from its root administrator.'

// A hint is ONE short line. Anything longer lives behind this toggle.
//
// 2026-09-06: the form carried a paragraph above almost every field, and a paragraph
// above every field is read as decoration and skipped — which explains less than a
// single line does. So the long text is not deleted here, it is folded: the line a
// first-time user needs stays visible, and the detail is one click away for the user
// who wants it.
function More({ children, label = 'more' }) {
  return (
    <details className="more">
      <summary>{label}</summary>
      <div>{children}</div>
    </details>
  )
}

// Several steps in one container. A job runs ONE entrypoint, which is Docker's own
// one-process-per-container model — but an entrypoint is a LIST, so a shell chain
// runs as many steps as you like, in order, and stops at the first one that fails.
// Shown to the user because nothing about the form said this was possible.
const ENTRYPOINT_HINT =
  'Several steps in order: sh -c "python prep.py && python train.py && python eval.py"'

// Turn what the user typed into the argument list the API takes (entrypoint is a
// list[str], see protocol.md). Splitting on spaces alone is NOT enough, and the
// reason matters: a shell chain puts a whole command INSIDE one argument, so
//   sh -c "a && b && c"
// must arrive as exactly three arguments, the third being the entire chain. A
// space-split can never produce an argument that contains a space — it would hand
// sh the string `"a` instead, and the container would die with an unterminated-quote
// syntax error before running anything. So we group quoted sections the way a shell
// does. Unquoted input behaves exactly as it did before.
export function tokenizeEntrypoint(line) {
  const args = []
  let cur = ''
  let started = false
  let quote = null // the quote character we are inside, or null
  let escaped = false // the previous character was a backslash inside double quotes
  for (const ch of line) {
    if (quote) {
      // 2026-09-07 (walk 1, row 57): inside double quotes a backslash escapes the
      // next character, as a shell does, so a command that itself needs a double
      // quote can carry one. Single quotes stay literal, also as a shell does.
      // Narrowed the same day: only the four characters a shell treats as special
      // inside double quotes consume the backslash. Everything else keeps it, so a
      // Windows path and a regex survive being quoted -- before this, "C:\data" came
      // out as C:data.
      if (escaped) {
        if (ch !== '\n') cur += ('"\\$`'.includes(ch) ? '' : '\\') + ch
        escaped = false
      } else if (quote === '"' && ch === '\\') {
        escaped = true
      } else if (ch === quote) {
        quote = null
      } else {
        cur += ch
      }
      continue
    }
    if (ch === '"' || ch === "'") {
      quote = ch
      started = true // so that "" is one empty argument, not nothing
      continue
    }
    if (/\s/.test(ch)) {
      if (started) {
        args.push(cur)
        cur = ''
        started = false
      }
      continue
    }
    cur += ch
    started = true
  }
  // Never guess at what a half-quoted line meant: say so and let the user fix it,
  // rather than silently sending a command they did not type.
  if (quote) throw new Error(`Unmatched ${quote} in the entrypoint — close the quote, or remove it.`)
  if (started) args.push(cur)
  return args
}

// The job's environment, built in one place so it can be exercised without a
// browser. Exported for the same reason tokenizeEntrypoint is: the web app has no
// test runner, so a pure function is the only part of this file a test can reach,
// and the parts that matter are pure.
//
// DATASET_URL is the odd one out and deliberately so. An empty box means the
// variable is ABSENT, never an empty string: a script that checks "is DATASET_URL
// set" must be able to get the answer no. It is set last, so the dedicated box
// wins over a custom row of the same name; leaving the box empty leaves such a row
// untouched. The value is trimmed, because a trailing space in a URL is a typo and
// never a dataset.
export function buildEnv({ epochs, lr, batchSize, optimizer, seed, extra, fail, datasetUrl }) {
  const env = {
    EPOCHS: String(epochs),
    EPOCH_SECONDS: '1',
    LR: String(lr),
    BATCH_SIZE: String(batchSize),
    OPTIMIZER: optimizer,
    SEED: String(seed),
  }
  for (const { k, v } of extra) if (k.trim()) env[k.trim()] = v
  if (fail) env.FAIL = '1'
  if (datasetUrl.trim()) env.DATASET_URL = datasetUrl.trim()
  return env
}

// A number of megabytes in the unit a person would say it in (walk 1, row 23: the
// tier said "20 GB" in one place and "20480 MB" in another). GB above a gigabyte,
// with the exact MB beside it so the two places agree.
export function mbText(mb) {
  if (mb == null) return ''
  if (mb >= 1024) return `${(mb / 1024).toFixed(mb % 1024 === 0 ? 0 : 1)} GB (${Math.round(mb)} MB)`
  return `${Math.round(mb * 100) / 100} MB`
}

// A file size in the unit a person would say it in.
export function fileSize(n) {
  if (n == null) return ''
  if (n < 1024) return `${n} B`
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`
  return `${(n / (1024 * 1024)).toFixed(1)} MB`
}

// Restored 2026-07-15 for W5 (failure detection + re-dispatch): the "force fail"
// control is needed again for kill-testing the reaper. It was hidden 2026-07-12
// for the proposal demo (UI-only — the backend FAILED path was never touched).
const SHOW_FORCE_FAIL = true

// Submit form -> POST /jobs. Defaults run the dummy workload (workloads/dummy),
// which prints metrics line-by-line for ~epochs seconds — good live-log fodder.
export default function SubmitForm({ onSubmitted }) {
  const mounted = useRef(false)
  useEffect(() => {
    mounted.current = true
    return () => { mounted.current = false }
  }, [])
  const [name, setName] = useState('w3-demo')
  const [image, setImage] = useState('fyp-dummy:latest')
  const [entrypoint, setEntrypoint] = useState('python train.py')
  const [epochs, setEpochs] = useState(5)
  const [replicas, setReplicas] = useState(1)
  const [fail, setFail] = useState(false)
  const [needsGpu, setNeedsGpu] = useState(false)
  const [minRam, setMinRam] = useState('') // MB; empty = no requirement
  const [memLimit, setMemLimit] = useState('') // MB container memory cap; empty = none
  // Hyperparameters (W4, supervisor-requested). They travel as env vars — the
  // platform stays generic (a job is a container), the container reads them.
  const [lr, setLr] = useState('0.001')
  const [batchSize, setBatchSize] = useState('32')
  const [optimizer, setOptimizer] = useState('adam')
  const [seed, setSeed] = useState('42')
  // Which dataset the job should train on. The platform does not fetch, copy or
  // cache the data: the address travels to the container as DATASET_URL and the
  // script reads it. A dataset store is future work.
  const [datasetUrl, setDatasetUrl] = useState('')
  // Checkpoint-use advisor (2026-09-05, M. Ayli's suggestion). The platform receives
  // an IMAGE, not source code, so it cannot read the training script by itself. This
  // box is how it gets something to read. Optional, and nothing about the job changes
  // if it stays empty: the advice records "not checked", which is deliberately a
  // different answer from "checked, and it never checkpoints".
  const [sourceText, setSourceText] = useState('')
  const [extra, setExtra] = useState([]) // custom [{k, v}] env rows
  const { nodes } = usePool()
  const [targets, setTargets] = useState([]) // selected node ids
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState(null)
  // 2026-09-06: the one thing left to choose. Sealing is the floor for every job and
  // has no box, because it costs the user nothing; running only on machines an admin
  // trusts costs them the rest of the pool, so it is theirs to ask for.
  const [trustedOnly, setTrustedOnly] = useState(false)
  // The job's dataset. ONE box now: there used to be two, because a sealed input was
  // a different act from a file the job needs -- it cost the run its output files and
  // its ability to resume. It costs neither any more, so there is one file box and
  // its contents are sealed.
  const [datasetFile, setDatasetFile] = useState(null)
  // 2026-09-07 (walk 1, rows 30 and 35). A chosen file used to stay attached to every
  // later submission with nothing on screen saying so — six jobs in the first walk
  // carried a dataset nobody had attached to them, and each was charged for it. The
  // file input is a browser control that keeps its own value, so clearing our state
  // is not enough: bumping this key remounts the control empty after every submit.
  const [fileKey, setFileKey] = useState(0)
  // Storage quota policy (2026-09-04). `me` carries the tier, both caps, how much is
  // retained already, and whether the limits have been accepted. It is read BEFORE
  // anything can be submitted, because a cap the user cannot see is a trap: without
  // it the platform would refuse work for a reason nothing on the screen had ever
  // mentioned, and the first they would hear of it is a failed run.
  const [me, setMe] = useState(null)
  // 2026-09-07: why the tier is not on screen yet. Null while it is being read; the
  // control plane's sentence when the read failed and is being retried. Either way
  // the form stays closed, because a cap the user cannot see is the trap above.
  const [meError, setMeError] = useState(null)
  const [accepting, setAccepting] = useState(false)
  // How much temporary disk this run needs on a worker. Optional, and the two states
  // mean genuinely different things — left empty the run is simply held to the tier
  // ceiling; typed in, it becomes a REQUIREMENT, so the scheduler will not place the
  // run on a machine with less free disk than that.
  const [scratchMb, setScratchMb] = useState('')

  useEffect(() => {
    let alive = true, timer
    async function refresh() {
      try { const user = await getMe(); if (alive) { setMe(user); setMeError(null) } }
      catch (e) { if (alive) { setMeError(e.message); timer = setTimeout(refresh, 3000) } }
    }
    refresh()
    window.addEventListener('storage-released', refresh)
    return () => { alive = false; clearTimeout(timer); window.removeEventListener('storage-released', refresh) }
  }, [])

  async function onAccept() {
    setAccepting(true)
    setError(null)
    try {
      setMe(await acceptLimits())
    } catch (e) {
      setError(e.message)
    } finally {
      setAccepting(false)
    }
  }

  function toggleTarget(id) {
    setTargets((t) => (t.includes(id) ? t.filter((x) => x !== id) : [...t, id]))
  }

  async function submit(e) {
    e.preventDefault()
    setBusy(true)
    setError(null)
    const env = buildEnv({ epochs, lr, batchSize, optimizer, seed, extra, fail, datasetUrl })
    // W4: hardware needs travel with the job; the scheduler matches them against
    // each node's declared specs at pull-time.
    const reqs = { needs_gpu: needsGpu }
    if (minRam !== '' && Number(minRam) > 0) reqs.min_ram_mb = Number(minRam)
    // W5b: a container memory cap. The agent applies it as --memory, so an
    // over-budget run is OOM-killed by the kernel (try it with `--oom`).
    if (memLimit !== '' && Number(memLimit) > 0) reqs.mem_limit_mb = Number(memLimit)
    // Only send it when the user actually typed a number. Sending the tier ceiling
    // here instead would turn a limit into a requirement and ask the scheduler for a
    // machine with hundreds of gigabytes free, which no lab machine has.
    if (scratchMb !== '' && Number(scratchMb) > 0) reqs.scratch_mb = Number(scratchMb)
    // A half-quoted command is a typo, not a job. Stop here so the user sees why,
    // instead of a container failing later on a command they did not write.
    let entrypointArgs
    try {
      entrypointArgs = tokenizeEntrypoint(entrypoint)
    } catch (err) {
      setError(err.message)
      setBusy(false)
      return
    }
    const body = {
      name,
      image,
      entrypoint: entrypointArgs,
      env,
      resource_reqs: reqs,
      // target nodes set -> one run per node (replicas ignored, per protocol.md §4)
      target_node_ids: targets.length ? targets : null,
      replicas: targets.length ? 1 : Number(replicas || 1),
    }
    // Absent, never an empty string — the same rule the Dataset URL box follows. The
    // server tells "nothing was pasted" apart from "something was pasted" and a blank
    // string would blur the two.
    if (sourceText.trim()) body.source_text = sourceText
    // Placement, and only placement (2026-09-06). Sealing is not asked for because it
    // is not optional.
    if (trustedOnly) body.trusted_only = true
    try {
      // One door for a job with a file and one for a job without, because JSON cannot
      // carry a file. Both seal.
      const result = datasetFile
        ? await createJobWithInput(body, datasetFile)
        : await createJob(body)
      if (!mounted.current) return
      onSubmitted(result)
      // The file went with THIS job and with no other: forget it and empty the box.
      setDatasetFile(null)
      setFileKey((k) => k + 1)
      // The submitted job may have cost storage (a sealed input does immediately),
      // so refresh the bar rather than leaving a stale number on the screen.
      getMe().then(setMe).catch(() => {})
    } catch (e) {
      // 2026-09-07: the same guard the success path has. A submission refused after
      // sign-out used to set state on a form that was no longer there.
      if (mounted.current) setError(e.message)
    } finally {
      if (mounted.current) setBusy(false)
    }
  }

  // Refused here for the same reason the server refuses it (403), just said earlier
  // and more kindly. The server is still the one that enforces it — this is a
  // courtesy, not the mechanism.
  //
  // 2026-09-07: closed until `me` is known. Before, the button was live while the
  // tier was still loading (or failing quietly every 3 s), so a user on a slow link
  // could submit and get a bare 403 for limits nothing on the screen had shown.
  const blocked = busy || me == null || !me.limits_accepted
  const usedPct =
    me && me.retained_cap_mb > 0
      ? Math.min(100, (me.retained_used_mb / me.retained_cap_mb) * 100)
      : 0

  return (
    <form onSubmit={submit} className="form">
      {/* Storage tier: the two numbers, what is used of the first, and the one-time
          agreement. Shown at the TOP of the form on purpose — it is the constraint
          everything below it is subject to. */}
      {me == null && !meError && <p className="muted">Reading your storage tier…</p>}
      {me == null && meError && (
        <p role="alert" className="err">
          Cannot read your storage tier: {meError}. It retries every few seconds; nothing
          can be submitted until it is known.
        </p>
      )}
      {me && (
        <fieldset className="targets">
          <legend>Storage tier — {me.tier}</legend>
          <div className="quotabar" aria-hidden="true">
            <div
              className={usedPct >= 75 ? 'quotafill high' : 'quotafill'}
              style={{ width: `${usedPct}%` }}
            />
          </div>
          <p className="muted">
            <b>{mbText(me.retained_used_mb)}</b> of <b>{mbText(me.retained_cap_mb)}</b> used ·{' '}
            <b>{mbText(me.scratch_cap_mb)}</b> temporary disk per run
          </p>
          <More>
            The first number is everything kept on the server for you: results, checkpoints,
            archived logs and sealed inputs, for every attempt including ones that were
            replaced. The second is what one run may write on a worker while it runs.
          </More>
          {me.limits_accepted ? (
            <p className="muted">
              ✓ Limits accepted. You are asked again if either number changes.
            </p>
          ) : (
            <>
              <button type="button" onClick={onAccept} disabled={accepting}>
                {accepting ? 'Accepting…' : 'I accept these storage limits'}
              </button>
              <p className="muted">Jobs are refused until you accept.</p>
              <More>
                At the cap, uploads are refused too and the run that produced them is marked
                failed. Free space by releasing a finished job&rsquo;s storage.
              </More>
            </>
          )}
        </fieldset>
      )}
      {/* One row, not three stacked labels (2026-09-07). Stacked, these were three
          full-width text inputs and the three tallest lines in the panel; in a row
          they take one line on a wide page and still wrap to three on a narrow one,
          because `.form .row label` is `flex: 1 1 9rem`. */}
      <div className="row">
        <label>Name<input value={name} onChange={(e) => setName(e.target.value)} /></label>
        <label>Image<input value={image} onChange={(e) => setImage(e.target.value)} /></label>
        <label>Entrypoint<input value={entrypoint} placeholder={ENTRYPOINT_HINT} onChange={(e) => setEntrypoint(e.target.value)} /></label>
      </div>
      <p className="muted">{ENTRYPOINT_HINT}</p>
      <p className="muted">
        Split like a shell: spaces separate arguments; quotes keep a group together; inside
        double quotes <code>\"</code> gives a literal quote. Every step in one container can use
        the job’s key.
      </p>
      {targets.length > 0 && <p className="muted">One run per selected machine; replicas do not apply.</p>}
      {trustedOnly && targets.length > 0 && targets.some((id) => !nodes.find((n) => n.node_id === id)?.trusted) && <p role="alert" className="waiting">Untrusted selected machines will wait until an admin trusts them.</p>}
      {needsGpu && !nodes.some((n) => n.online && !n.agent_outdated && n.has_gpu && (!targets.length || targets.includes(n.node_id)) && (!trustedOnly || n.trusted)) && <p role="alert" className="waiting">No eligible selected or pooled machine currently has a GPU. This job will wait.</p>}
      <div className="row">
        <label>Epochs<input type="number" min="1" value={epochs} onChange={(e) => setEpochs(e.target.value)} /></label>
        <label>Replicas<input type="number" min="1" disabled={targets.length > 0} value={replicas} onChange={(e) => setReplicas(e.target.value)} /></label>
        {SHOW_FORCE_FAIL && (
          <label className="check"><input type="checkbox" checked={fail} onChange={(e) => setFail(e.target.checked)} /> force fail</label>
        )}
      </div>
      {/* Same shape as the row above on purpose: two growing fields then the
          checkbox, so Min RAM sits under Epochs, Memory limit under Replicas and
          "needs GPU" under "force fail". Temporary disk gets its own row rather
          than relying on a wrap, which put it in a different place at different
          widths. */}
      <div className="row">
        <label>Min RAM (MB)<input type="number" min="0" placeholder="any" value={minRam} onChange={(e) => setMinRam(e.target.value)} /></label>
        <label>Memory limit (MB)<input type="number" min="0" placeholder="none" value={memLimit} onChange={(e) => setMemLimit(e.target.value)} /></label>
        <label className="check"><input type="checkbox" checked={needsGpu} onChange={(e) => setNeedsGpu(e.target.checked)} /> needs GPU</label>
      </div>
      <div className="row">
        <label>Temporary disk (MB)
          <input
            type="number"
            min="0"
            placeholder={me ? `up to ${me.scratch_cap_mb}` : 'tier limit'}
            value={scratchMb}
            onChange={(e) => setScratchMb(e.target.value)}
          />
        </label>
      </div>
      <p className="muted">
        Space for <code>/scratch</code> and <code>/tmp</code>, wiped when the run ends.
        Empty means your tier&rsquo;s limit.
      </p>
      <More>
        Type a number and it becomes a requirement: the run only goes to a machine with
        at least that much free disk, and it stops if it writes more.
      </More>
      <fieldset className="targets">
        <legend>Dataset file (optional)</legend>
        {/* No label text and no empty-state line (2026-09-07). Between the legend, a
            "Upload a file" label and a "No file chosen. Optional." paragraph, the box
            said "optional" twice and "no file chosen" twice — the second time directly
            underneath the native control that had just said it. What the user cannot
            read off the control is what the platform DOES with the file, so that is
            the only sentence left, and it is the same sentence whether or not a file
            is chosen. */}
        <input
          key={fileKey}
          type="file"
          aria-label="Dataset file"
          onChange={(e) => setDatasetFile(e.target.files?.[0] ?? null)}
        />
        {datasetFile && (
          <p className="filechip">
            📎 <b>{datasetFile.name}</b> · {fileSize(datasetFile.size)}
            <button
              type="button"
              className="minor"
              onClick={() => { setDatasetFile(null); setFileKey((k) => k + 1) }}
            >
              ✕ remove
            </button>
          </p>
        )}
        <p className="privacynote muted">
          Goes with this job only. Sealed, then mounted read-only at{' '}
          <code>INPUT_PATH</code>, and counts against your storage.
        </p>
        <More>
          Open it with <code>fyp_data.open_input()</code> instead of <code>open()</code>
          — one line in your loader. It reads the file a piece at a time, so a large
          dataset does not need a large machine. An archive is <b>not</b> unpacked for you.
        </More>
      </fieldset>
      <fieldset className="targets private">
        <legend>🔒 Privacy</legend>
        <p className="privacynote">{PRIVACY_SHORT}</p>
        <More label="what sealing does and does not protect">{PRIVACY_NOTE}</More>
        <label className="check">
          <input
            type="checkbox"
            checked={trustedOnly}
            onChange={(e) => setTrustedOnly(e.target.checked)}
          />
          Run only on machines an admin has marked <b>trusted</b>
        </label>
        <p className="privacynote muted">The job waits until a trusted machine is free.</p>
        <More>
          This is the one privacy choice left to make, because it is the only one that
          costs you something. Sealing itself is on for every job and cannot be turned
          off. You still read your result files here, and a run still resumes after a
          machine dies.
        </More>
      </fieldset>
      <fieldset className="targets">
        <legend>Hyperparameters (passed to the container as env vars)</legend>
        <p className="muted">Stored readable. Do not put a password or a token here.</p>
        <div className="row">
          <label>Learning rate<input type="number" step="any" min="0" value={lr} onChange={(e) => setLr(e.target.value)} /></label>
          <label>Batch size<input type="number" min="1" value={batchSize} onChange={(e) => setBatchSize(e.target.value)} /></label>
        </div>
        <div className="row">
          <label>Optimizer
            <select value={optimizer} onChange={(e) => setOptimizer(e.target.value)}>
              <option value="adam">adam</option>
              <option value="sgd">sgd</option>
              <option value="rmsprop">rmsprop</option>
              <option value="adamw">adamw</option>
            </select>
          </label>
          <label>Seed<input type="number" value={seed} onChange={(e) => setSeed(e.target.value)} /></label>
        </div>
        <label>Dataset URL (optional)
          <input
            value={datasetUrl}
            placeholder="https://… or a lab file path the script can read"
            onChange={(e) => setDatasetUrl(e.target.value)}
          />
        </label>
        <p className="muted">
          Sent as <code>DATASET_URL</code>. Your script reads the address — the platform
          does not fetch or copy the data.
        </p>
        <p className="muted">Stored readable. Do not put a password or a token here.</p>
        <label>Training script (optional)
          <textarea
            rows={6}
            value={sourceText}
            placeholder={'paste your training script here — it is checked for checkpoint use'}
            onChange={(e) => setSourceText(e.target.value)}
          />
        </label>
        <p className="muted">Checked for checkpoint use. Never executed, never sent to a worker.</p>
        {/* The disclosure sits where the text is typed, not in a page nobody opens.
            `source_text` is stored readable, is returned by no route and is removed by
            no user-facing route, so the person pasting cannot read it back or delete
            it. Twelve is the arm A token count in
            control-plane/app/checkpoint_advisor.py: 3 path + 5 save + 4 load. */}
        <p className="muted">
          What happens to this text: it is stored readable in the platform's database and
          kept. It is checked on this server against twelve fixed patterns and nothing from
          it is sent anywhere else. It is not shown back to you and the platform does not
          delete it. An administrator can read it. Do not paste anything you would not want
          kept.
        </p>
        <More>
          Only this pasted text is read, not the image. It produces the advice line on the
          job page, which tells you whether a re-dispatched run would resume or start again.
        </More>
        {extra.map((row, i) => (
          <div className="row" key={i}>
            <label>Custom key<input placeholder="e.g. DATASET" value={row.k} onChange={(e) => setExtra(extra.map((r, j) => (j === i ? { ...r, k: e.target.value } : r)))} /></label>
            <label>Value<input placeholder="value" value={row.v} onChange={(e) => setExtra(extra.map((r, j) => (j === i ? { ...r, v: e.target.value } : r)))} /></label>
            <button type="button" className="minor" onClick={() => setExtra(extra.filter((_, j) => j !== i))}>✕</button>
          </div>
        ))}
        <button type="button" className="minor" onClick={() => setExtra([...extra, { k: '', v: '' }])}>+ add custom parameter</button>
      </fieldset>
      {nodes.length > 0 && (
        <fieldset className="targets">
          <legend>Target nodes (optional — none = any eligible)</legend>
          {nodes.map((n) => (
            <label key={n.node_id} className="check">
              <input
                type="checkbox"
                checked={targets.includes(n.node_id)}
                onChange={() => toggleTarget(n.node_id)}
                disabled={!n.online}
              />
              {n.name}{n.online ? '' : ' (offline)'}
              {/* A trusted-only job can land on a trusted node alone, so say which
                  ones qualify right where the user picks them. */}
              {trustedOnly && !n.trusted && <span className="muted"> — not trusted</span>}
            </label>
          ))}
        </fieldset>
      )}
      <button type="submit" disabled={blocked}>
        {busy ? 'submitting…' : '🔒 Submit job'}
      </button>
      {me && !me.limits_accepted && (
        <p className="muted">accept your storage limits above before submitting</p>
      )}
      {error && <p className="err">{error}</p>}
    </form>
  )
}
