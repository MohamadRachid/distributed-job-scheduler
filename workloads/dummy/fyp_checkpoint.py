"""fyp_checkpoint.py — the one file a workload adds to survive a machine dying.

**This is the adoption contract**, and it is the same philosophy as the ``##PROGRESS``
line (W5b) and ``fyp_open.py`` (W6b): one tiny readable file a real training script
adopts in a minute, not a framework it has to be rewritten for.

Copy this file into your image and use two calls::

    import fyp_checkpoint

    state = fyp_checkpoint.load() or {"epoch": 0}      # absent -> start at the top
    for epoch in range(state["epoch"] + 1, total + 1):
        ...train one epoch...
        fyp_checkpoint.save({"epoch": epoch, "weights": ...})

That is the whole contract. Everything else stays yours.

**Why you get anything at all.** When the machine running your job dies, the platform
notices the lease expire, marks the run lost and hands it to another machine. Without
this file the new machine starts your training from epoch 1 — the run is recovered
but the *work* is not. With it, the agent on the new machine has already fetched your
last saved state and put it where ``load()`` looks, so you carry on.

**Absent is normal, not an error.** ``load()`` returns ``None`` on a first attempt, on
a machine that saved nothing yet, and whenever the platform could not verify the file
it was holding. Your code must treat that as "start from the beginning", because that
is exactly what every run did before this feature existed.

**Saving is atomic.** ``save()`` writes a temporary file next to the real one and
renames it into place. Rename is atomic on a POSIX filesystem, so the agent — which
sweeps this file up while you are still running — can never catch a half-written
save. It reads a complete old state or a complete new one, never a torn one.

**The saved file is world-readable, on purpose, and that is now harmless.** The agent
that collects it runs as a different user on the machine outside your container, so a
checkpoint only it could read would never leave the machine — and it would fail
silently, because collecting is best-effort by design. Since 2026-09-06 the bytes in
that world-readable file are sealed, so "anyone on this worker can read this file"
means "anyone on this worker can read ciphertext".

**One file, one name, overwritten in place.** Every save writes the same path, so the
platform stores exactly one checkpoint per attempt however often you call it. That is
what keeps storage bounded with no cleanup job to run or forget.

**It is SEALED, as of 2026-09-06, and that is what lets a private job resume.** The
state is sealed inside this container with your job's own key before it is written, so
what leaves the machine is ciphertext and the agent that carries it cannot read it. A
run killed mid-training is handed to another machine, whose container holds the same
job key and opens the checkpoint there. That closes the hole this file used to carry
in its own words: a private job had no writable host mount at all, so it could not
checkpoint, so privacy cost you your work whenever a machine died. Privacy and resume
no longer trade against each other.

**Three formats are read, and the older two are read for ever.** Sealed (today), the
plain envelope (2026-09-01), bare JSON (before that). A run checkpointed by attempt 1
can be resumed by attempt 2 running a DIFFERENT image, so on any day this contract
changes the two attempts may disagree about the format — and a reader that understood
only the newest one would return None and silently restart the training, which is the
exact defect this file exists to prevent.

**What this does not do.** It does not version your state. If a checkpoint is corrupt
it is thrown away and you start over: a run that crashes on resume is worse than a run
that starts again. Sealed state that will not OPEN is treated the same way — as absent
— for the same reason.

**It carries an envelope and an optional payload, and the text packing is gone
(2026-09-01).** It used to carry JSON alone. JSON cannot hold a tensor, so a workload
with real weights base64'd them on the way in, and that cost a third of the file.
Measured on `workloads/trainer/train.py`: 206,922 parameters are **2.38 MiB** of
tensors and **3.17 MiB** once encoded — **12.06 bytes per parameter of real state
against 16.06 stored**. The twelve is what the format actually needs: four bytes of
float32 weight plus Adam's `exp_avg` and `exp_avg_sq` at four each. **The extra four
were the encoding, not the model**, and `save(state, payload=...)` removes them by
writing the bytes as bytes.

**A save with no payload is unchanged, byte for byte.** `save({"epoch": 3})` writes
exactly the JSON it always wrote, because a workload with no tensors was never paying
the surcharge and must not pay a new header instead. Only a save that carries bytes
uses the envelope format.
"""

import json
import os
import tempfile

# The sealing contract (2026-09-06). Optional on purpose: an image built before that
# date does not carry `fyp_data.py`, and a checkpoint helper that refused to import
# without it would break every such image at once. Absent -> the two calls below fall
# back to identity, which is exactly what this file did yesterday.
try:
    import fyp_data
except ImportError:  # pragma: no cover - only on an image built before 2026-09-06
    fyp_data = None

# The envelope format's first bytes. A reader tells the two formats apart by sniffing
# this rather than by a flag, a setting, or a version the caller passes in -- the file
# on disk has to be self-describing, because whoever reads it may not be whoever wrote
# it (see `load`).
MAGIC = b"FYPCKPT1\n"

# Where `load` puts the binary payload in the dict it returns, and the key `save`
# refuses to let an envelope shadow. Reserved: do not use it for your own state.
PAYLOAD_KEY = "fyp_payload"


def path() -> str | None:
    """Where the platform expects this run's checkpoint, or None when the platform
    did not give us one (an old agent, or a private run)."""
    return os.environ.get("CHECKPOINT_PATH") or None


def load():
    """The state saved by an earlier attempt, or **None meaning start at the top**.

    None on every ordinary first run. None too if the file is unreadable, or is
    neither shape we understand — an unusable checkpoint is treated exactly like no
    checkpoint, because failing the run over it would be worse than repeating the
    training.

    **This reads ALL THREE formats, and the older two are read for ever.** A run checkpointed
    by attempt 1 can be resumed by attempt 2 running a **different image** — that is
    the entire point of handing a lost run to a machine that was not the one that
    died. So on any day this contract changes, attempt 1 may have written the old
    JSON while attempt 2 is reading with the new code. A reader that understood only
    the new format would return None there, and the run would start its training from
    the beginning: **a silent restart, which is the exact defect this file exists to
    prevent, reintroduced by the change meant to improve it.** The old format is never
    written again. It is always read.

    When the file carries a binary payload it comes back under `PAYLOAD_KEY` as
    `bytes`. When it carries none — an old file, or a save that passed no payload —
    that key is absent rather than present-and-empty, so `state.get(PAYLOAD_KEY)`
    answers the only question a workload has."""
    p = path()
    if not p or not os.path.isfile(p):
        return None
    try:
        with open(p, "rb") as f:
            raw = f.read()
    except OSError:
        return None

    # 2026-09-06: unseal first, and pass through anything that is not sealed. This one
    # line is what lets attempt 2 read a checkpoint attempt 1 wrote, in any of the three
    # formats, on any machine, in any image built since. Sealed state that will not open
    # is treated as ABSENT rather than as an error, for the same reason corrupt state
    # is: starting the training again is always better than crashing on resume.
    if fyp_data is not None:
        try:
            raw = fyp_data.open_bytes(raw)
        except Exception:  # noqa: BLE001 - unopenable state is absent state
            return None

    if raw.startswith(MAGIC):
        body = raw[len(MAGIC):]
        head, sep, payload = body.partition(b"\n")
        if not sep:
            return None                      # header but no envelope: unusable
        try:
            state = json.loads(head.decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            return None
        if not isinstance(state, dict):
            return None
        if payload:
            state[PAYLOAD_KEY] = payload
        return state

    try:
        return json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return None


def save(state, payload: bytes | None = None) -> bool:
    """Write `state` (anything json can hold) as this run's checkpoint, optionally
    alongside `payload` — raw bytes, stored raw. Returns True if it was written.

    **Pass your tensors as `payload`, not inside `state`.** Serialise them however
    your framework likes and hand over the bytes; they are written verbatim. Packing
    them into a string first costs a third of the file for nothing.

    **With no payload this writes exactly the JSON it always wrote**, byte for byte,
    so a workload that saves only numbers pays nothing for a feature it does not use.

    Temp-file-then-rename, so a reader can never see a partial save. Best-effort: a
    failure here costs repeated training if the machine then dies, and nothing else,
    so it never raises into your training loop."""
    p = path()
    if not p:
        return False
    if payload:
        if not isinstance(payload, (bytes, bytearray)):
            return False
        if isinstance(state, dict) and PAYLOAD_KEY in state:
            # Refuse rather than silently drop one of them: the envelope would
            # overwrite the payload on the way back out and the resume would use
            # whichever won, which is the kind of thing that is found much later.
            return False
    directory = os.path.dirname(p) or "."
    try:
        os.makedirs(directory, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=directory, prefix=".ckpt-", suffix=".tmp")
        try:
            # mkstemp creates 0600 owned by whoever this container runs as, and the
            # AGENT is a different user on the machine outside. Without this the agent
            # cannot read the file it is meant to sweep up, so the save succeeds, the
            # run reports nothing wrong, and no checkpoint ever leaves the machine.
            os.fchmod(fd, 0o644)
            if payload:
                body = (
                    MAGIC + json.dumps(state).encode("utf-8") + b"\n" + bytes(payload)
                )
            else:
                # Byte for byte what this file has always written, before sealing.
                body = json.dumps(state).encode("utf-8")
            # Sealed here, at the last moment, so exactly one thing is written and the
            # rename below stays atomic. On a job with no key this returns `body`
            # unchanged, which is what an unsealed job and a hand-run image get.
            if fyp_data is not None:
                body = fyp_data.seal_bytes(body)
            with os.fdopen(fd, "wb") as f:
                f.write(body)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, p)   # atomic: readers see old or new, never half
        except BaseException:
            try:
                os.remove(tmp)
            except OSError:
                pass
            raise
    except OSError:
        return False
    # One line in the run's own log, so a person watching can tell WHEN a checkpoint
    # exists (walk 1, row 38: nothing on screen said it). The agent sweeps the file up
    # on its own timer and prints its own line when it has been stored.
    where = ""
    if isinstance(state, dict) and "epoch" in state:
        where = " (epoch %s)" % state["epoch"]
    print("checkpoint saved%s" % where, flush=True)
    return True
