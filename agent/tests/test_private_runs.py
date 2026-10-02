"""Agent-side private-run logic (W6b) — staging, lockdown, and the broken seal.

Pure-logic tests, no Docker daemon and no control plane: a fake docker client backs
the REAL Runner, so the container arguments asserted here are the arguments the real
agent would pass. The point of these tests is that the *shape of the container* is
the privacy claim — if a flag silently changed, the claim would quietly become
false, and these assertions are what stops that.

Run from the repo root:  pytest agent/tests -q
(Not part of the control-plane container suite — the agent is host software.)
"""

import os

from agent import agent as agent_mod
from agent import runner as runner_mod
from agent.classify import INTEGRITY_ERROR, classify_failure
from agent.runner import Runner

RUN_ID = "run-p"
ATTEMPT = 1
SEALED = b"\x00sealed-bytes-not-readable\x01"


# --- fakes -------------------------------------------------------------------


class FakeContainer:
    short_id = "fake0000"

    def __init__(self, output: bytes = b"") -> None:
        self._output = output

    def logs(self, **_kw) -> bytes:
        return self._output

    def remove(self, **_kw) -> None:
        pass


class FakeClient:
    """Records the kwargs `containers.run` was called with, so the test can assert
    the exact isolation flags a private container is given."""

    def __init__(self, container: FakeContainer) -> None:
        self._container = container
        self.run_kwargs: dict = {}

    def ping(self) -> None:
        pass

    @property
    def images(self):
        class _Images:
            @staticmethod
            def get(_image):
                return object()  # local; no pull

        return _Images

    @property
    def containers(self):
        outer = self

        class _Containers:
            @staticmethod
            def run(*args, **kwargs):
                outer.run_kwargs = kwargs
                return outer._container

        return _Containers


def make_client(monkeypatch, output: bytes = b"") -> FakeClient:
    client = FakeClient(FakeContainer(output))
    monkeypatch.setattr(runner_mod.docker, "from_env", lambda: client)
    return client


def stage_sealed(tmp_path) -> str:
    path = tmp_path / "sealed.bin"
    path.write_bytes(SEALED)
    return str(path)


# --- the shape of a private container ---------------------------------------


def test_private_container_is_locked_down(monkeypatch, tmp_path):
    """Every assertion here is one sentence of the privacy claim, in code."""
    client = make_client(monkeypatch)
    r = Runner()
    sealed = stage_sealed(tmp_path)
    r.start(
        RUN_ID, ATTEMPT, "fyp-dummy:latest",
        ["python", "fyp_open.py", "python", "train.py"], {},
        private=True, sealed_path=sealed, ticket="tkt-123",
        key_url="http://host.docker.internal:8000/container/key",
        input_filename="secret.csv",
    )
    kw = client.run_kwargs

    # The plaintext folder is RAM, not disk — and it is sized.
    assert "/private" in kw["tmpfs"] and "size=" in kw["tmpfs"]["/private"]
    # The ONLY host mount is the sealed file, and it is read-only.
    assert list(kw["volumes"].keys()) == [sealed]
    assert kw["volumes"][sealed]["mode"] == "ro"
    # No writable host mount at all -> no working dir -> no artifacts (stated).
    assert all(v["mode"] == "ro" for v in kw["volumes"].values())
    # Untrusted code gets the least we can give it.
    assert kw["read_only"] is True
    assert kw["user"] == "65534:65534"
    # The container is told where to redeem, and given a TICKET — never a key.
    env = kw["environment"]
    assert env["FYP_TICKET"] == "tkt-123"
    assert env["FYP_KEY_URL"].endswith("/container/key")
    assert env["INPUT_CIPHER_PATH"] == "/sealed/input.bin"
    assert env["INPUT_PLAIN_PATH"] == "/private/secret.csv"
    # Nothing in the environment can open anything.
    assert not any("key_b64" in str(v) for v in env.values())
    # Outputs, if the workload writes any, stay in RAM with the plaintext.
    assert env["OUTPUT_DIR"].startswith("/private")


def test_private_run_collects_no_artifacts(monkeypatch, tmp_path):
    """A private run has no writable host folder, so there is nothing to collect —
    the honest consequence of the lockdown, asserted rather than assumed."""
    make_client(monkeypatch)
    r = Runner()
    r.start(
        RUN_ID, ATTEMPT, "img", [], {},
        private=True, sealed_path=stage_sealed(tmp_path), ticket="t", key_url="u",
    )
    assert r._runs[RUN_ID]["output_dir"] is None
    assert r.artifact_paths(RUN_ID) == []


def test_ordinary_run_gets_none_of_the_private_staging(monkeypatch):
    """W6b must not alter normal runs: no tmpfs, no unprivileged user, no sealed
    mount, no ticket. A regression here would break every W2-W6 behaviour.

    2026-09-04: an ordinary run IS read-only at the root now, and that is a different
    change for a different reason — it is what makes temporary disk measurable, not
    part of the privacy staging. So this test no longer asserts the absence of
    `read_only`; it asserts the absence of everything that belongs to privacy, and the
    new mount shape is asserted in `test_scratch_quota.py` where it belongs. The test
    was renamed rather than quietly reinterpreted, because "unchanged by W6b" stopped
    being what it checks."""
    client = make_client(monkeypatch)
    r = Runner()
    r.start(RUN_ID, ATTEMPT, "img", ["python", "train.py"], {})
    kw = client.run_kwargs
    assert "tmpfs" not in kw and "user" not in kw
    env = kw["environment"]
    assert "FYP_TICKET" not in env and "INPUT_CIPHER_PATH" not in env
    assert not any(m["bind"] == "/sealed/input.bin" for m in kw["volumes"].values())
    out_dir = r._runs[RUN_ID]["output_dir"]
    assert kw["volumes"][out_dir] == {"bind": "/scratch", "mode": "rw"}
    assert env["OUTPUT_DIR"] == "/scratch"


# --- the staged ciphertext is wiped -----------------------------------------


def test_cleanup_wipes_the_staged_sealed_file(monkeypatch, tmp_path):
    make_client(monkeypatch)
    r = Runner()
    sealed = stage_sealed(tmp_path)
    r.start(RUN_ID, ATTEMPT, "img", [], {}, private=True, sealed_path=sealed, ticket="t", key_url="u")
    assert os.path.exists(sealed)
    r.cleanup(RUN_ID)
    assert not os.path.exists(sealed)


def test_abort_wipes_the_staged_sealed_file(monkeypatch, tmp_path):
    """A fencing 409 must leave nothing behind either — the wipe is in the shared
    finally-path, not only on the happy exit."""
    make_client(monkeypatch)
    r = Runner()
    sealed = stage_sealed(tmp_path)
    r.start(RUN_ID, ATTEMPT, "img", [], {}, private=True, sealed_path=sealed, ticket="t", key_url="u")
    r.abort(RUN_ID)
    assert not os.path.exists(sealed)


def test_wipe_survives_an_already_deleted_file(monkeypatch, tmp_path):
    make_client(monkeypatch)
    r = Runner()
    sealed = stage_sealed(tmp_path)
    r.start(RUN_ID, ATTEMPT, "img", [], {}, private=True, sealed_path=sealed, ticket="t", key_url="u")
    os.remove(sealed)
    r.cleanup(RUN_ID)  # must not raise


# --- staging orchestration ---------------------------------------------------


def test_stage_private_fetches_blob_then_ticket(monkeypatch, tmp_path):
    """The agent downloads ciphertext and a ticket — and never a key. Order matters:
    the ticket is short-lived, so it is fetched last, closest to container start."""
    monkeypatch.setenv("AGENT_OUTPUT_ROOT", str(tmp_path))
    calls = []
    monkeypatch.setattr(
        agent_mod, "fetch_sealed_input",
        lambda s, t, job_id: calls.append(("input", job_id)) or SEALED,
    )
    monkeypatch.setattr(
        agent_mod, "fetch_key_ticket",
        lambda s, t, run_id, attempt: calls.append(("ticket", run_id, attempt))
        or {"ticket": "tkt", "key_url": "http://cp/container/key"},
    )

    extra = agent_mod._stage_private(
        "http://cp", "tok",
        {"run_id": RUN_ID, "attempt": ATTEMPT, "job_id": "job-1", "input_filename": "s.csv"},
    )
    assert calls == [("input", "job-1"), ("ticket", RUN_ID, ATTEMPT)]
    assert extra["private"] is True and extra["ticket"] == "tkt"
    with open(extra["sealed_path"], "rb") as f:
        assert f.read() == SEALED    # ciphertext, staged as-is
    os.remove(extra["sealed_path"])


def test_assignment_without_private_flag_stages_nothing(monkeypatch):
    """An ordinary assignment must never touch the private path (and an old agent's
    assignments never carry the flag at all)."""
    client = make_client(monkeypatch)
    r = Runner()
    monkeypatch.setattr(
        agent_mod, "_stage_private",
        lambda *a, **k: (_ for _ in ()).throw(AssertionError("must not stage")),
    )
    monkeypatch.setattr(agent_mod, "post_run_status", lambda *a, **k: "accepted")
    agent_mod._act_on_assignments(
        "s", "t", r,
        [{"run_id": RUN_ID, "attempt": ATTEMPT, "image": "img", "entrypoint": [], "env": {}}],
    )
    assert r.has(RUN_ID)
    # Nothing from the private path: no RAM folder, no unprivileged user, no ticket.
    # (`read_only` IS set from 2026-09-04, for the temporary-disk cap rather than for
    # privacy, so its presence is no longer evidence of private staging.)
    assert "tmpfs" not in client.run_kwargs and "user" not in client.run_kwargs
    assert "FYP_TICKET" not in client.run_kwargs["environment"]


def test_staging_failure_reports_failed_not_a_hang(monkeypatch):
    """If the blob or the ticket cannot be fetched, the run must be REPORTED failed —
    never left stuck in ASSIGNED waiting for a reaper."""
    make_client(monkeypatch)
    r = Runner()
    monkeypatch.setattr(
        agent_mod, "_stage_private",
        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("control plane unreachable")),
    )
    posted = []
    monkeypatch.setattr(
        agent_mod, "post_run_status",
        lambda s, t, run_id, attempt, state, *a, **k: posted.append(state) or "accepted",
    )
    agent_mod._act_on_assignments(
        "s", "t", r,
        [{"run_id": RUN_ID, "attempt": ATTEMPT, "image": "img", "entrypoint": [], "env": {},
          "private": True, "job_id": "j"}],
    )
    assert posted == ["FAILED"]
    assert not r.has(RUN_ID)


# --- a broken seal is classified as exactly that -----------------------------


def test_integrity_marker_is_classified():
    tail = (
        "some earlier output\n"
        "##INTEGRITY_ERROR: the sealed data was changed after it was sealed "
        "(AES-GCM tag mismatch)\n"
    )
    reason, detail = classify_failure({"ExitCode": 1}, 1, None, log_tail=tail)
    assert reason == INTEGRITY_ERROR
    assert "changed" in detail


def test_integrity_beats_a_bare_exit_code():
    """Without the marker the same run would read as a plain APP_ERROR — the marker
    is what turns "exited 1" into "the data was tampered with"."""
    plain = classify_failure({"ExitCode": 1}, 1, None, log_tail="normal output\n")
    assert plain[0] != INTEGRITY_ERROR
    marked = classify_failure({"ExitCode": 1}, 1, None, log_tail="##INTEGRITY_ERROR: bad tag\n")
    assert marked[0] == INTEGRITY_ERROR


def test_no_log_tail_keeps_the_old_classification():
    """W5b behaviour is untouched when there is no tail to read (old callers, no
    output): the classifier must not change what it said before W6b."""
    assert classify_failure({"OOMKilled": True}, 137, None, 128)[0] == "OOM_KILLED"
    assert classify_failure({"ExitCode": 139}, 139, None)[0] == "APP_CRASH"
    assert classify_failure({"ExitCode": 0}, 0, None) is None


def test_log_tail_is_read_from_the_container(monkeypatch):
    make_client(monkeypatch, output=b"line one\n##INTEGRITY_ERROR: bad tag\n")
    r = Runner()
    r.start(RUN_ID, ATTEMPT, "img", [], {})
    assert "##INTEGRITY_ERROR" in r.log_tail(RUN_ID)
    assert r.log_tail("no-such-run") == ""
