"""The entrypoint is a LIST, so several steps run in order inside ONE container.

Why this file exists. The supervisor's
remark of 28 August was that the platform allows only "a single entrypoint". It
does — one entrypoint per container, which is Docker's own one-process model —
but an entrypoint is a list of arguments, so

    ["sh", "-c", "python step1.py && python step2.py && python step3.py"]

runs three steps in order in that one container, and stops at the first step that
fails. Nothing had to be built for this. What was missing was a test saying it is
true, and a live run showing it.

The load-bearing assertion is the FIRST one: the agent hands Docker the argument
list unchanged, including the one argument that CONTAINS SPACES. A chain only
works if nothing along the way re-splits that argument. These tests pin that, so
it cannot drift silently.

No Docker daemon: `docker.from_env` is monkeypatched with a fake client, the same
pattern as test_runner_logs.py and test_runner_artifacts.py, so the REAL
Runner.start / collect_logs / poll code runs.

Run from the repo root:  pytest agent/tests/test_entrypoint_chain.py -q
"""

from agent import runner as runner_mod
from agent.classify import classify_failure
from agent.runner import Runner

RUN_ID = "run-chain"
ATTEMPT = 1

# The chain exactly as a user types it into the browser's Entrypoint box, after
# the form has turned it into an argument list. Three arguments: the shell, its
# -c flag, and the WHOLE chain as one argument.
CHAIN = ["sh", "-c", "python step1.py && python step2.py && python step3.py"]


# --- fakes -------------------------------------------------------------------


class FakeContainer:
    """Docker's Container, reduced to what these tests need: cumulative output,
    a status, and the State dict poll() reads (ExitCode lives there)."""

    short_id = "chain000"

    def __init__(self) -> None:
        self._text = ""
        self.status = "running"
        self.attrs = {"State": {}}

    def feed(self, text: str) -> None:
        self._text += text

    def exit_with(self, code: int) -> None:
        """The container finished. This is what poll() will see next tick."""
        self.status = "exited"
        self.attrs = {"State": {"ExitCode": code, "OOMKilled": False}}

    def logs(self, **_kw) -> bytes:
        return self._text.encode("utf-8")

    def reload(self) -> None:
        pass


class RecordingClient:
    """Records the keyword arguments `containers.run` was called with, so a test
    can look at the entrypoint the agent actually handed to Docker."""

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
                return object()  # already local; no pull

        return _Images

    @property
    def containers(self):
        outer = self

        class _Containers:
            @staticmethod
            def run(*_a, **kw):
                outer.run_kwargs = kw
                return outer._container

        return _Containers


def make_runner(monkeypatch, entrypoint):
    """A real Runner that started one run with `entrypoint`, against a fake
    container. Returns the runner, the container, and the recording client."""
    container = FakeContainer()
    client = RecordingClient(container)
    monkeypatch.setattr(runner_mod.docker, "from_env", lambda: client)
    r = Runner()
    r.start(RUN_ID, ATTEMPT, "fyp-dummy:latest", entrypoint, {})
    return r, container, client


# --- the argument list survives ---------------------------------------------


def test_chain_reaches_docker_as_three_arguments_unchanged(monkeypatch):
    """The whole point. Docker is handed exactly three arguments, and the third
    is the entire chain, spaces and all — the agent does not re-split it.

    If this ever fails, a shell chain stops working: `sh` would be given only the
    first word of the chain and the run would die on a syntax error before doing
    anything."""
    _r, _c, client = make_runner(monkeypatch, CHAIN)

    assert client.run_kwargs["entrypoint"] == CHAIN
    assert len(client.run_kwargs["entrypoint"]) == 3
    # The argument that carries the chain contains spaces AND both operators.
    chain_arg = client.run_kwargs["entrypoint"][2]
    assert " " in chain_arg
    assert chain_arg.count("&&") == 2
    assert chain_arg == "python step1.py && python step2.py && python step3.py"


def test_ordinary_single_command_entrypoint_is_unchanged(monkeypatch):
    """Chain support must not have altered ordinary jobs. The dummy image's own
    entrypoint still arrives exactly as it always did."""
    plain = ["python", "train.py"]
    _r, _c, client = make_runner(monkeypatch, plain)

    assert client.run_kwargs["entrypoint"] == plain


# --- direction 1: every step runs, in order ---------------------------------


def test_chain_success_all_three_steps_in_order_exit_zero(monkeypatch):
    """The success direction: each step prints its own name, the names appear in
    the order the chain wrote them, and the run reports exit 0."""
    r, container, _client = make_runner(monkeypatch, CHAIN)

    container.feed("step-1\nstep-2\nstep-3\n")
    container.exit_with(0)

    seq, chunk = r.collect_logs(RUN_ID, final=True)
    assert seq == 0
    assert chunk == "step-1\nstep-2\nstep-3\n"
    # In order, not merely present.
    assert chunk.index("step-1") < chunk.index("step-2") < chunk.index("step-3")

    finished = r.poll()
    assert len(finished) == 1
    assert finished[0]["run_id"] == RUN_ID
    assert finished[0]["exit_code"] == 0
    # Exit 0 is a success, so there is no failure to classify.
    assert classify_failure(finished[0]["state"], 0, None) is None


# --- direction 2: the chain stops at the first failing step ------------------


def test_chain_failure_stops_after_step_one_and_reports_exit_one(monkeypatch):
    """The failure direction, which matters as much: step 2 exits 1, so `&&`
    short-circuits — step 3 never runs, its name never reaches the log, and the
    run reports exit 1 with a detail that names it."""
    r, container, _client = make_runner(monkeypatch, CHAIN)

    # step-1 printed, step-2 failed, step-3 never ran.
    container.feed("step-1\n")
    container.exit_with(1)

    seq, chunk = r.collect_logs(RUN_ID, final=True)
    assert seq == 0
    assert chunk == "step-1\n"
    assert "step-3" not in chunk  # the chain stopped; this is the whole point

    finished = r.poll()
    assert len(finished) == 1
    assert finished[0]["exit_code"] == 1

    reason, detail = classify_failure(finished[0]["state"], 1, None)
    assert reason == "APP_ERROR"
    assert "1" in detail  # the exit code is named in what the user is shown
