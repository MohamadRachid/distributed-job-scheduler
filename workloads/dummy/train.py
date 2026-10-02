"""Dummy training workload — the ONLY thing that ever goes on the
critical path. Never a real ML job (§1).

Behaviour:
  - normal: sleeps a few seconds, printing fake metrics line-by-line (log streaming,
    W3) AND a machine-readable ``##PROGRESS`` line per epoch (the W5b progress
    contract → a live progress bar); writes one output file (artifact upload, W6);
  - ``--oom``: allocates memory gradually until the kernel OOM-kills it — under a
    container memory limit this is a real, provable kernel kill (W5b diagnostics);
  - ``--crash``: raises an exception and exits non-zero, printing a traceback (W5b);
  - ``FAIL=1``: exits non-zero the plain way (existing W5 failure path);
  - ``--resume``: OPT-IN. Saves one checkpoint per epoch and continues from it if an
    earlier attempt left one, so a machine dying mid-run costs one epoch instead of
    all of them (2026-08-13).
  - ``--write-mb N``: writes N MB into the output directory and exits 0 — a run whose
    RESULT is a known size, so the retained-storage cap can be shown refusing an
    upload live and in seconds (2026-09-04);
  - ``--fill-scratch``: writes 8 MB blocks into the output directory without stopping,
    so the per-run temporary-disk cap can be shown stopping a run. Deliberately steady
    rather than as fast as the disk will go: the overshoot at the kill is bounded by
    write speed x sample tick, and a measurement of that bound is worth more than a
    demonstration of how fast this laptop can write. Without the flag this file behaves exactly as it did
    before that date — the checkpoint mount is simply never touched.

Created in W1; RUN from W2; failure/progress modes added in W5b.
"""

import argparse
import json
import os
import sys
import time

# The sealing contract (2026-09-06). Every result this workload writes goes through
# it, and every dataset it reads comes back through it. On a job with no key it is
# identity in both directions, so this same image still runs an unsealed job.
import fyp_data


def _hparams() -> dict:
    # Hyperparameters arrive as env vars (the job contract keeps the platform
    # generic). Echoing them proves the values the user typed reached the container.
    return {
        "lr": os.environ.get("LR", "0.001"),
        "batch_size": os.environ.get("BATCH_SIZE", "32"),
        "optimizer": os.environ.get("OPTIMIZER", "adam"),
        "seed": os.environ.get("SEED", "42"),
    }


def _dataset_line() -> str:
    """The dataset this job was told to use, echoed like the hyperparameters above.

    The address arrives as an environment variable and this script would be the
    thing that fetches it. The platform never moves the data and never claims to,
    so an unset variable is reported as unset rather than defaulted to something."""
    url = os.environ.get("DATASET_URL", "")
    return f"DATASET_URL={url}" if url else "DATASET_URL=(not set)"


def _run_oom(out_dir: str) -> None:
    """Allocate memory gradually. Under a container memory limit (--memory) this
    triggers a REAL kernel OOM kill and Docker sets State.OOMKilled=true — our
    provable RAM-overload signal. Slow on purpose so the per-run samples show RAM
    climbing to the limit before the kill.

    ``OOM_TARGET_MB`` (optional) bounds the appetite: the job needs about that much
    RAM, then finishes normally. A node whose cap is below the target is OOM-killed;
    a node whose cap is above it succeeds — which is exactly the W5c escalation demo
    (a weak node dies, a stronger node runs it). Unset -> allocate until killed
    (the original W5b behaviour, unchanged)."""
    target = os.environ.get("OOM_TARGET_MB")
    target_mb = int(target) if target else None
    if target_mb:
        print(f"OOM mode: this job needs about {target_mb} MB of RAM", flush=True)
    else:
        print("OOM mode: allocating memory until the kernel stops us", flush=True)
    blocks = []
    mb = 0
    step_mb = 8
    while target_mb is None or mb < target_mb:
        blocks.append(bytearray(step_mb * 1024 * 1024))  # zero-filled -> pages committed
        mb += step_mb
        print(f"allocated {mb} MB", flush=True)
        time.sleep(0.4 if target_mb else 1.2)
    # Reached the target without being killed -> this machine was big enough. Finish
    # like a normal run so the escalated run can end SUCCEEDED on the stronger node.
    print(f"reached {mb} MB target without an OOM kill — this machine is big enough", flush=True)
    out_path = _write_json(
        out_dir, "metrics.json", {"oom_target_mb": target_mb, "allocated_mb": mb}
    )
    print(f"wrote {out_path}", flush=True)


def _run_private_input() -> None:
    """W6b demo: prove the sealed input really opened, WITHOUT printing it.

    By the time this runs, `fyp_open.py` has already redeemed the ticket, verified
    the seal, and written the plaintext into the RAM-backed `/private` folder. We
    print its SHA-256 and size — enough to show a real file with real content is
    there (and to compare against the original the user submitted), while the
    content itself never reaches the stored logs. Privacy would be a strange thing
    to demonstrate by publishing the data."""
    import hashlib

    path = os.environ.get("INPUT_PLAIN_PATH", "")
    if not path or not os.path.exists(path):
        print(f"private input not found at {path!r}", file=sys.stderr, flush=True)
        raise SystemExit(1)
    with open(path, "rb") as f:
        data = f.read()
    print(f"private input opened: {len(data)} bytes at {path}", flush=True)
    print(f"sha256 = {hashlib.sha256(data).hexdigest()}", flush=True)
    # Where it lives, so the demo can say "RAM, not disk" and point at the proof.
    print(f"filesystem holding it: {'tmpfs (RAM)' if path.startswith('/private') else path}", flush=True)
    print("integrity OK", flush=True)


def _run_resumable(epochs: int, epoch_seconds: float, hparams: dict, out_dir: str) -> None:
    """The normal run, made restartable — the demo of the two-call adoption contract
    (``fyp_checkpoint.load`` / ``save``).

    The only differences from ``_run_normal`` are the first line and the last line of
    the loop: ask where we got to, and write down where we are. Printing the epoch it
    starts at is what makes a resume visible: attempt 1's log ends at epoch N and
    attempt 2's begins at N+1, in the stored logs, with nothing added to the
    interface to say so."""
    import fyp_checkpoint

    state = fyp_checkpoint.load()
    start = int(state.get("epoch", 0)) + 1 if state else 1
    if start > 1:
        print(f"resuming from checkpoint at epoch {start - 1}", flush=True)
    else:
        print("no checkpoint found — starting at epoch 1", flush=True)
    print(
        "hyperparameters: " + " ".join(f"{k}={v}" for k, v in hparams.items()),
        flush=True,
    )
    print(_dataset_line(), flush=True)

    started = time.time()
    final_loss = None
    final_acc = None
    for e in range(start, epochs + 1):
        final_loss = round(1.0 / e, 4)
        final_acc = round(1.0 - final_loss, 4)
        print(f"epoch {e}/{epochs} loss={final_loss} acc={final_acc} lr={hparams['lr']}", flush=True)
        print(f'##PROGRESS {{"epoch": {e}, "total": {epochs}, "loss": {final_loss}}}', flush=True)
        # Save AFTER the epoch is complete, so what we record is work that finished.
        fyp_checkpoint.save({"epoch": e, "loss": final_loss, "accuracy": final_acc})
        time.sleep(epoch_seconds)

    if final_loss is None:  # every epoch was already done before this attempt began
        final_loss = state.get("loss") if state else None
        final_acc = state.get("accuracy") if state else None

    result = {
        "node": os.environ.get("FYP_NODE_NAME", "unknown"),
        "epochs": epochs,
        "resumed_from_epoch": start - 1,
        "final_loss": final_loss,
        "accuracy": final_acc,
        "duration_seconds": round(time.time() - started, 2),
        "hyperparameters": hparams,
    }
    out_path = _write_json(out_dir, "metrics.json", result)
    print(f"wrote {out_path}", flush=True)


def _write_mb(out_dir: str, mb: float) -> None:
    """Write one file of about `mb` megabytes and finish normally (2026-09-04).

    Its purpose is a run whose RESULT is a size we chose, so the retained-storage cap
    can be shown refusing an upload in seconds rather than after somebody trains
    something. Written in 1 MB pieces and flushed, so the file on disk grows the way a
    real output does instead of appearing all at once when the process exits — which
    is what makes it usable for watching the temporary-disk reading climb too."""
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, "out.bin")
    block = b"x" * (1024 * 1024)
    whole, remainder = divmod(int(mb * 1024 * 1024), 1024 * 1024)
    print(f"writing {mb} MB to {path}", flush=True)
    # Through the sealing writer like every other result (2026-09-06). The bytes on
    # disk are therefore SEALED bytes, and a little larger than `mb` — which is the
    # honest thing for the quota demonstration to charge, because sealed bytes are
    # what the platform actually keeps.
    with fyp_data.create_output("out.bin", directory=out_dir) as f:
        for _ in range(whole):
            f.write(block)
        if remainder:
            f.write(b"x" * remainder)
    print(f"wrote {os.path.getsize(path)} bytes", flush=True)


def _fill_scratch(out_dir: str) -> None:
    """Write 8 MB blocks into the output directory until something stops us.

    Two things can, and the difference is the whole point of having this mode. On a
    PUBLIC run the agent measures the run's directory every sample tick and stops the
    container when it crosses the cap — so the run dies from the outside, and the
    overshoot between the last sample and the kill is the number that gets measured.
    On a PRIVATE run there is no host mount at all and the working space is a RAM
    folder the kernel sizes exactly, so the write itself fails with `ENOSPC` and the
    exit classifier names it.

    Each block is its own file, and each is flushed and fsynced before the next
    begins, so what the agent measures is bytes that are really on the disk rather
    than bytes still sitting in a buffer. Printing the running total after every block
    means the log itself shows how far past the cap the run got."""
    os.makedirs(out_dir, exist_ok=True)
    block = b"x" * (8 * 1024 * 1024)
    written_mb = 0
    i = 0
    print("fill mode: writing 8 MB blocks until something stops us", flush=True)
    while True:
        with fyp_data.create_output(f"fill-{i:04d}.bin", directory=out_dir) as f:
            f.write(block)
        written_mb += 8
        i += 1
        print(f"wrote {written_mb} MB of temporary disk", flush=True)
        # A short pause between blocks. Without it the overshoot measured at the kill
        # would be a statement about this machine's disk speed and nothing else; with
        # it, the bound (write speed x sample tick) is the thing being demonstrated.
        time.sleep(0.2)


def _write_json(out_dir: str, name: str, obj: dict) -> str:
    """Write one of this run's result files, sealed when the platform gave us a key.

    Every result this workload produces goes through here (2026-09-06). On a sealed
    job the bytes are sealed inside this container before they touch the folder the
    agent collects from, which is what makes "the plaintext never lands on a disk"
    true of outputs and not only of inputs. On an unsealed job — or when this image is
    run by hand — `fyp_data` hands back a plain file and this writes exactly what it
    always wrote."""
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, name)
    with fyp_data.create_output(name, directory=out_dir) as f:
        f.write(json.dumps(obj, indent=2).encode("utf-8"))
    return path


def _read_input(expect_marker: str | None) -> None:
    """Read this run's dataset end to end through the sealed reader, and report what
    it cost (2026-09-06).

    This is the mode the sealed-by-default proof drives. It prints four things, all
    read off the machine rather than asserted: how many bytes came back, their digest
    (so it can be compared with what was submitted, without printing the data), the
    peak memory this process ever held, and — if asked — whether a marker string known
    to be in the plaintext was found.

    Peak memory is `VmHWM` from the kernel's own accounting for this process: the
    high-water mark, so it cannot miss a spike between two samples. It is the number
    that answers "does a bigger dataset need a bigger machine", and with a reader that
    opens one piece at a time the answer is no."""
    import hashlib

    def peak_mb() -> float:
        try:
            with open("/proc/self/status", "r", encoding="utf-8") as f:
                for line in f:
                    if line.startswith("VmHWM:"):
                        return round(int(line.split()[1]) / 1024.0, 1)
        except OSError:
            pass
        return -1.0

    path = os.environ.get("INPUT_PATH", "")
    if not path or not os.path.exists(path):
        print(f"no input at {path!r}", file=sys.stderr, flush=True)
        raise SystemExit(1)
    on_disk = os.path.getsize(path)
    with open(path, "rb") as raw:
        head = raw.read(8)
    print(f"INPUT_PATH={path}", flush=True)
    print(f"on-disk bytes: {on_disk}", flush=True)
    print(f"on-disk first 8 bytes: {head!r}", flush=True)
    print(f"on-disk is sealed: {head == b'FYPSEAL2'}", flush=True)
    print(f"peak memory before reading: {peak_mb()} MB", flush=True)

    digest = hashlib.sha256()
    total = 0
    found = False
    # A window carried across reads, so a marker that straddles two pieces is still
    # found. Without it the search would silently depend on where the pieces happen
    # to fall, which is the kind of test that passes until the day it matters.
    tail = b""
    needle = expect_marker.encode("utf-8") if expect_marker else b""
    with fyp_data.open_input(path) as f:
        while True:
            block = f.read(1024 * 1024)
            if not block:
                break
            total += len(block)
            digest.update(block)
            if needle and not found:
                if needle in tail + block:
                    found = True
                tail = block[-len(needle):]
    print(f"plaintext bytes read: {total}", flush=True)
    print(f"plaintext sha256: {digest.hexdigest()}", flush=True)
    print(f"peak memory after reading: {peak_mb()} MB", flush=True)
    if expect_marker:
        print(f"marker {expect_marker!r} found in plaintext: {found}", flush=True)
    print("integrity OK", flush=True)


def _run_crash() -> None:
    """Print a traceback and exit non-zero — the traceback becomes the stderr tail
    evidence behind an APP_ERROR reason."""
    print("crash mode: raising an exception", file=sys.stderr, flush=True)
    raise RuntimeError("simulated application crash (--crash)")


def _run_normal(epochs: int, epoch_seconds: float, hparams: dict, out_dir: str) -> None:
    print(
        "hyperparameters: " + " ".join(f"{k}={v}" for k, v in hparams.items()),
        flush=True,
    )
    print(_dataset_line(), flush=True)
    started = time.time()
    final_loss = None
    final_acc = None
    for e in range(1, epochs + 1):
        # Deterministic fake metrics — no randomness, so runs are reproducible.
        final_loss = round(1.0 / e, 4)
        final_acc = round(1.0 - final_loss, 4)
        print(f"epoch {e}/{epochs} loss={final_loss} acc={final_acc} lr={hparams['lr']}", flush=True)
        # W5b progress contract: one machine-readable line the agent parses into a
        # live progress bar + latest metrics. Any real script adopts it with one print.
        print(f'##PROGRESS {{"epoch": {e}, "total": {epochs}, "loss": {final_loss}}}', flush=True)
        time.sleep(epoch_seconds)

    # W6: write ONE result file to the declared output path (/output). The agent
    # collects it and uploads it as this run's artifact — the unified results view
    # then shows it, downloadable. It records WHICH machine ran the job (FYP_NODE_NAME,
    # injected by the agent), how long it took, and a deterministic fake accuracy, so
    # two runs of one job on two nodes can be compared side by side.
    result = {
        "node": os.environ.get("FYP_NODE_NAME", "unknown"),
        "epochs": epochs,
        "final_loss": final_loss,
        "accuracy": final_acc,
        "duration_seconds": round(time.time() - started, 2),
        "hyperparameters": hparams,
    }
    out_path = _write_json(out_dir, "metrics.json", result)
    print(f"wrote {out_path}", flush=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="FYP dummy training workload")
    parser.add_argument("--oom", action="store_true", help="allocate RAM until OOM-killed")
    parser.add_argument("--crash", action="store_true", help="raise an exception, exit non-zero")
    parser.add_argument(
        "--resume",
        action="store_true",
        help="save a checkpoint each epoch and continue from one if an earlier attempt left it",
    )
    parser.add_argument(
        "--read-input",
        action="store_true",
        help="read INPUT_PATH through the sealed reader and report bytes, digest and peak memory",
    )
    parser.add_argument(
        "--expect-marker",
        default=None,
        help="a string known to be in the plaintext; reports whether it was found",
    )
    parser.add_argument(
        "--private-input",
        action="store_true",
        help="W6b: hash the opened sealed input to prove it decrypted (never prints it)",
    )
    parser.add_argument(
        "--write-mb",
        type=float,
        default=None,
        help="write this many MB of output and exit 0 (storage quota demonstration)",
    )
    parser.add_argument(
        "--fill-scratch",
        action="store_true",
        help="write 8 MB blocks without stopping, to hit the temporary-disk cap",
    )
    args = parser.parse_args()

    # /scratch since 2026-09-04 — the agent sets OUTPUT_DIR, and the fallback here
    # only matters when this script is run outside the platform by hand.
    out_dir = os.environ.get("OUTPUT_DIR", "/scratch")
    if args.write_mb is not None:
        _write_mb(out_dir, args.write_mb)
        return 0
    if args.fill_scratch:
        _fill_scratch(out_dir)
        return 0  # unreachable — the run is stopped from outside, or ENOSPC raises
    if args.oom:
        _run_oom(out_dir)
        return 0  # reached only when OOM_TARGET_MB is set and fit under the cap
    if args.crash:
        _run_crash()
        return 1  # unreachable — the raise exits first
    if args.private_input:
        _run_private_input()
        return 0
    if args.read_input:
        _read_input(args.expect_marker)
        return 0

    epochs = int(os.environ.get("EPOCHS", "5"))
    epoch_seconds = float(os.environ.get("EPOCH_SECONDS", "1"))
    if args.resume:
        _run_resumable(epochs, epoch_seconds, _hparams(), out_dir)
    else:
        _run_normal(epochs, epoch_seconds, _hparams(), out_dir)

    if os.environ.get("FAIL", "").strip().lower() in ("1", "true", "yes"):
        print("forced failure (FAIL is set)", file=sys.stderr, flush=True)
        return 1

    print("done", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
