"""Agent-side log capture logic (W3) — the seq/cursor rules in Runner.

These are pure-logic tests: no Docker daemon. We monkeypatch `docker.from_env`
with a fake client so the REAL Runner methods (start / collect_logs /
confirm_logs) run against a fake container whose output grows over time.

The one subtle rule under test (found 2026-07-06, from the 2026-07-02 demo
caveat): a batch whose post was not confirmed must be re-sent BYTE-IDENTICAL
under the same seq. The control plane may have already stored it and only the
200 got lost; the DB keeps the FIRST row for a seq, so a retry that grew
fatter (same seq, more lines scooped up meanwhile) would have its extra lines
silently dropped from storage — while still showing live. That was the gap.

Run from the repo root:  pytest agent/tests -q
(Not part of the control-plane container suite — the agent is host software.)
"""

from agent import runner as runner_mod
from agent.runner import Runner

RUN_ID = "run-1"
ATTEMPT = 1


# --- fakes -------------------------------------------------------------------


class FakeContainer:
    """Stands in for docker's Container: `.logs()` returns ALL output so far
    (that is how the real API behaves — cumulative, not incremental)."""

    short_id = "fake0000"

    def __init__(self) -> None:
        self._text = ""

    def feed(self, text: str) -> None:
        self._text += text

    def logs(self, **_kw) -> bytes:
        return self._text.encode("utf-8")


class FakeClient:
    """Just enough of docker-py's client for Runner.start(): ping, image lookup
    (always "already local"), and containers.run returning our fake."""

    def __init__(self, container: FakeContainer) -> None:
        self._container = container

    def ping(self) -> None:
        pass

    @property
    def images(self):
        class _Images:
            @staticmethod
            def get(_image):
                return object()  # image is local; no pull

        return _Images

    @property
    def containers(self):
        container = self._container

        class _Containers:
            @staticmethod
            def run(*_a, **_kw):
                return container

        return _Containers


def make_runner(monkeypatch) -> tuple[Runner, FakeContainer]:
    """A real Runner tracking one run, wired to a fake container via the real
    start() path (so the tracked-state shape can never drift from production)."""
    container = FakeContainer()
    monkeypatch.setattr(
        runner_mod.docker, "from_env", lambda: FakeClient(container)
    )
    r = Runner()
    r.start(RUN_ID, ATTEMPT, "fyp-dummy:latest", ["python", "train.py"], {})
    return r, container


class FirstWriteWinsStore:
    """The DB's UNIQUE(run_id, attempt, seq) in miniature: the first chunk
    stored under a seq stays; a duplicate seq is dropped (idempotent 200)."""

    def __init__(self) -> None:
        self.rows: dict[int, str] = {}

    def post(self, seq: int, chunk: str) -> None:
        self.rows.setdefault(seq, chunk)

    def all_text(self) -> str:
        return "".join(self.rows[k] for k in sorted(self.rows))


# --- the normal path ---------------------------------------------------------


def test_stream_batches_by_tick_all_lines_in_order(monkeypatch):
    """Two ticks + final flush -> two seqs stored, every line present, in order."""
    r, container = make_runner(monkeypatch)
    store = FirstWriteWinsStore()

    container.feed("epoch 1\nepoch 2\n")
    seq, chunk = r.collect_logs(RUN_ID)
    assert (seq, chunk) == (0, "epoch 1\nepoch 2\n")
    store.post(seq, chunk)
    r.confirm_logs(RUN_ID)

    container.feed("epoch 3\ndone\n")
    seq, chunk = r.collect_logs(RUN_ID, final=True)
    assert (seq, chunk) == (1, "epoch 3\ndone\n")
    store.post(seq, chunk)
    r.confirm_logs(RUN_ID)

    assert store.all_text() == "epoch 1\nepoch 2\nepoch 3\ndone\n"
    assert r.collect_logs(RUN_ID, final=True) is None  # nothing left


def test_incomplete_final_line_held_back_until_final(monkeypatch):
    """A half-written line (no newline yet) is not sent while running — only the
    final flush emits it, so it can never be sent twice across two batches."""
    r, container = make_runner(monkeypatch)

    container.feed("epoch 1\npartial")
    seq, chunk = r.collect_logs(RUN_ID)
    assert (seq, chunk) == (0, "epoch 1\n")  # 'partial' held back
    r.confirm_logs(RUN_ID)

    assert r.collect_logs(RUN_ID) is None  # still incomplete: nothing to send
    seq, chunk = r.collect_logs(RUN_ID, final=True)
    assert (seq, chunk) == (1, "partial\n")


# --- the retry rule (the 2026-07-02 caveat) -----------------------------------


def test_unconfirmed_batch_is_frozen_on_retry(monkeypatch):
    """No confirm (the post failed or its 200 was lost) -> the next collect must
    return the SAME seq with the IDENTICAL chunk, even though the container has
    printed more since. New lines wait for the next seq."""
    r, container = make_runner(monkeypatch)

    container.feed("epoch 1\n")
    first = r.collect_logs(RUN_ID)
    assert first == (0, "epoch 1\n")

    container.feed("epoch 2\nepoch 3\n")  # output grows while seq 0 is in flight
    retry = r.collect_logs(RUN_ID)
    assert retry == first  # frozen: same seq, byte-identical chunk


def test_no_line_lost_when_response_was_lost_after_commit(monkeypatch):
    """The full failure story, end to end: the control plane stores seq 0 but its
    200 never reaches the agent. The agent retries seq 0 (frozen), the store drops
    the duplicate, and the held-back lines arrive under seq 1 — nothing is lost."""
    r, container = make_runner(monkeypatch)
    store = FirstWriteWinsStore()

    # Tick 1: seq 0 posted and STORED, but the response is lost -> no confirm.
    container.feed("epoch 1\nepoch 2\n")
    seq, chunk = r.collect_logs(RUN_ID)
    store.post(seq, chunk)

    # Container finishes meanwhile.
    container.feed("epoch 3\ndone\n")

    # Tick 2: retry of seq 0 — deduped by the store; this time the 200 arrives.
    seq, chunk = r.collect_logs(RUN_ID)
    store.post(seq, chunk)
    r.confirm_logs(RUN_ID)

    # Same tick: the rest goes out under seq 1 (this is _stream_logs' loop).
    seq, chunk = r.collect_logs(RUN_ID, final=True)
    assert seq == 1
    store.post(seq, chunk)
    r.confirm_logs(RUN_ID)

    assert store.all_text() == "epoch 1\nepoch 2\nepoch 3\ndone\n"
