"""Agent-side handling of an ordinary job's dataset file (2026-09-05).

Pure-logic tests, no Docker daemon and no control plane: a fake docker client backs
the REAL Runner, so the container arguments asserted here are the arguments the real
agent would pass.

Two properties are pinned, and each is a sentence somebody will have to defend:

  * **the file is mounted READ-ONLY**, so a run cannot alter the dataset it was
    handed. A run that could rewrite its own input would make every later attempt
    read something the user never uploaded;
  * **the file is not unpacked by the agent.** A zip arrives as a zip and
    `INPUT_PATH` names it. Unpacking an uploaded archive is untrusted-input work --
    it can expand to fill the disk, or carry paths that escape the directory it is
    opened into -- and the container already owns a measured, capped place to do it.

Run from the repo root:  pytest agent/tests -q
(Not part of the control-plane container suite -- the agent is host software.)
"""

import os

from agent import agent as agent_mod
from agent import runner as runner_mod
from agent.runner import Runner

RUN_ID = "run-i"
ATTEMPT = 1
DATASET = b"PK\x03\x04 not really a zip, but it does not need to be\n"


# --- fakes -------------------------------------------------------------------


class FakeContainer:
    short_id = "fake0000"

    def logs(self, **_kw) -> bytes:
        return b""

    def remove(self, **_kw) -> None:
        pass


class FakeClient:
    """Records the kwargs `containers.run` was called with, so the test can assert
    the exact mount and environment a container is given."""

    def __init__(self) -> None:
        self._container = FakeContainer()
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


def make_client(monkeypatch) -> FakeClient:
    client = FakeClient()
    monkeypatch.setattr(runner_mod.docker, "from_env", lambda: client)
    return client


# --- staging -----------------------------------------------------------------


def test_the_file_is_written_into_the_runs_own_directory(monkeypatch, tmp_path):
    """Inside the run directory, not beside it, so one attempt's file is measured
    with everything else that attempt writes and is cleared with it."""
    monkeypatch.setattr(
        agent_mod, "fetch_job_input", lambda server, token, job_id: DATASET
    )
    a = {
        "run_id": RUN_ID, "attempt": ATTEMPT, "job_id": "job-1",
        "input_filename": "dataset.zip",
    }
    out = agent_mod._stage_input("https://cp", "tok", a, str(tmp_path))

    assert out["input_name"] == "dataset.zip"
    assert os.path.isfile(out["input_path"])
    with open(out["input_path"], "rb") as f:
        assert f.read() == DATASET
    # Under the run directory it was given, never somewhere else on the machine.
    assert os.path.commonpath([str(tmp_path), out["input_path"]]) == str(tmp_path)


def test_a_hostile_filename_cannot_escape_the_run_directory(monkeypatch, tmp_path):
    """The control plane already reduces the name to a basename. This does it again,
    because the name decides a path on THIS machine and a filename that arrived over
    the network is not a thing to trust once."""
    monkeypatch.setattr(
        agent_mod, "fetch_job_input", lambda server, token, job_id: DATASET
    )
    a = {
        "run_id": RUN_ID, "attempt": ATTEMPT, "job_id": "job-1",
        "input_filename": "../../../etc/passwd",
    }
    out = agent_mod._stage_input("https://cp", "tok", a, str(tmp_path))

    assert out["input_name"] == "passwd"
    assert os.path.commonpath([str(tmp_path), out["input_path"]]) == str(tmp_path)


def test_a_missing_filename_still_produces_a_file(monkeypatch, tmp_path):
    monkeypatch.setattr(
        agent_mod, "fetch_job_input", lambda server, token, job_id: DATASET
    )
    a = {"run_id": RUN_ID, "attempt": ATTEMPT, "job_id": "job-1"}
    out = agent_mod._stage_input("https://cp", "tok", a, str(tmp_path))

    assert out["input_name"] == "input.bin"
    assert os.path.isfile(out["input_path"])


# --- the shape of the container ---------------------------------------------


def test_the_dataset_is_mounted_read_only_and_named(monkeypatch, tmp_path):
    """The two sentences this feature has to be able to defend, in code."""
    client = make_client(monkeypatch)
    r = Runner()
    staged = tmp_path / "dataset.zip"
    staged.write_bytes(DATASET)

    r.start(
        RUN_ID, ATTEMPT, "fyp-dummy:latest", ["python", "train.py"], {},
        run_dir=str(tmp_path), input_path=str(staged), input_name="dataset.zip",
    )
    kw = client.run_kwargs

    mount = kw["volumes"][str(staged)]
    assert mount["bind"] == "/input/dataset.zip"
    # READ-ONLY: the run cannot rewrite the dataset it was handed.
    assert mount["mode"] == "ro"
    # The container is told exactly where the file is, so nothing has to guess, and
    # the user's own name survives -- a workload expecting `dataset.zip` finds it.
    assert kw["environment"]["INPUT_PATH"] == "/input/dataset.zip"


def test_the_agent_does_not_unpack_the_archive(monkeypatch, tmp_path):
    """Deliberate, not missing. The bytes the container sees are the bytes that were
    uploaded, and opening them is the container's own business."""
    client = make_client(monkeypatch)
    r = Runner()
    staged = tmp_path / "dataset.zip"
    staged.write_bytes(DATASET)

    r.start(
        RUN_ID, ATTEMPT, "fyp-dummy:latest", ["python", "train.py"], {},
        run_dir=str(tmp_path), input_path=str(staged), input_name="dataset.zip",
    )

    # Still one file, still the same bytes: nothing was extracted beside it.
    with open(str(staged), "rb") as f:
        assert f.read() == DATASET
    assert client.run_kwargs["environment"]["INPUT_PATH"].endswith(".zip")


def test_a_run_without_a_file_is_unchanged(monkeypatch, tmp_path):
    """The regression guard. Every job that existed before today must start exactly
    as it did: no input mount, and no INPUT_PATH in the environment."""
    client = make_client(monkeypatch)
    r = Runner()

    r.start(
        RUN_ID, ATTEMPT, "fyp-dummy:latest", ["python", "train.py"], {},
        run_dir=str(tmp_path),
    )
    kw = client.run_kwargs

    assert not any("/input/" in v["bind"] for v in kw["volumes"].values())
    assert "INPUT_PATH" not in kw["environment"]
