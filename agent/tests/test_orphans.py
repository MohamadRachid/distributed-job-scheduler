"""Orphaned containers after an agent crash or restart (2026-09-07 audit, defect C).

`Runner._runs` lives in memory. If the agent process dies mid-run, its containers
kept running unmonitored for ever: disk and CPU spent on work the control plane's
reaper had already handed to someone else. From 0.12.2 every container the agent
starts carries three Docker labels naming the node, the run and the attempt, and at
startup the agent lists the containers labelled with ITS OWN node name, removes the
ones it is not tracking, and wipes their host directories.

Own name only, because several agents share one Docker daemon on one laptop
(`scripts/stage_demo.ps1` starts three): an agent must never touch a sibling's
containers.

Pure-logic tests with a fake docker client that answers `containers.list` with a
label filter the way the daemon does. Run from the repo root:  pytest agent/tests -q
"""

import os
import time

import pytest

from agent import agent as agent_mod
from agent import runner as runner_mod
from agent.runner import Runner, run_dir_path

NODE = "node-a"
OTHER = "node-b"


# --- fakes -------------------------------------------------------------------


class FakeContainer:
    _n = 0

    def __init__(self, labels: dict | None = None) -> None:
        FakeContainer._n += 1
        self.id = f"c{FakeContainer._n:04d}" + "0" * 60
        self.short_id = self.id[:12]
        self.labels = dict(labels or {})
        self.removed = False

    def logs(self, **_kw) -> bytes:
        return b""

    def remove(self, **_kw) -> None:
        self.removed = True


class FakeClient:
    """`containers.run` records its kwargs and returns a container carrying the
    labels it was given; `containers.list` filters by `label=key=value` the way the
    daemon does, and records that it was asked."""

    def __init__(self, existing: list[FakeContainer] | None = None) -> None:
        self.existing: list[FakeContainer] = list(existing or [])
        self.run_kwargs: dict = {}
        self.list_calls: list[dict] = []

    def ping(self) -> None:
        pass

    @property
    def images(self):
        class _Images:
            @staticmethod
            def get(_image):
                return object()

        return _Images

    @property
    def containers(self):
        outer = self

        class _Containers:
            @staticmethod
            def run(*_a, **kw):
                outer.run_kwargs = kw
                c = FakeContainer(kw.get("labels"))
                outer.existing.append(c)
                return c

            @staticmethod
            def list(all=False, filters=None):  # noqa: A002 - docker-py's own signature
                outer.list_calls.append({"all": all, "filters": filters})
                wanted = (filters or {}).get("label")
                if not wanted:
                    return list(outer.existing)
                key, _, value = wanted.partition("=")
                return [c for c in outer.existing if c.labels.get(key) == value]

        return _Containers


def _orphan(node: str, run_id: str, attempt: int) -> FakeContainer:
    return FakeContainer({"fyp.node": node, "fyp.run_id": run_id, "fyp.attempt": str(attempt)})


@pytest.fixture
def root(monkeypatch, tmp_path):
    monkeypatch.setenv(runner_mod._RUN_ROOT_ENV, str(tmp_path))
    return tmp_path


def _make(monkeypatch, existing=None, node=NODE) -> tuple[Runner, FakeClient]:
    client = FakeClient(existing)
    monkeypatch.setattr(runner_mod.docker, "from_env", lambda: client)
    r = Runner()
    r.node_name = node
    return r, client


# --- the labels every container is started with -------------------------------


def test_a_started_container_carries_the_node_run_and_attempt_labels(monkeypatch, root):
    r, client = _make(monkeypatch)
    r.start("run-1", 3, "img", [], {})
    assert client.run_kwargs["labels"] == {
        "fyp.node": NODE, "fyp.run_id": "run-1", "fyp.attempt": "3",
    }
    assert client.run_kwargs["name"] == "fyp-run-run-1-3", "the name is kept"


# --- reconciliation ------------------------------------------------------------


def test_an_orphan_of_this_node_is_removed_and_its_run_dir_wiped(monkeypatch, root):
    orphan = _orphan(NODE, "run-dead", 2)
    r, client = _make(monkeypatch, [orphan])
    stale_dir = run_dir_path("run-dead", 2)
    os.makedirs(os.path.join(stale_dir, "scratch"))

    removed = r.reconcile_orphans()

    assert removed == [{"run_id": "run-dead", "attempt": 2}]
    assert orphan.removed
    assert not os.path.exists(stale_dir)
    assert client.list_calls == [{"all": True, "filters": {"label": f"fyp.node={NODE}"}}]


def test_another_nodes_container_is_left_alone(monkeypatch, root):
    theirs = _orphan(OTHER, "run-theirs", 1)
    mine = _orphan(NODE, "run-mine", 1)
    r, _client = _make(monkeypatch, [theirs, mine])
    removed = r.reconcile_orphans()
    assert removed == [{"run_id": "run-mine", "attempt": 1}]
    assert mine.removed and not theirs.removed


def test_a_tracked_container_is_left_alone(monkeypatch, root):
    r, client = _make(monkeypatch)
    r.start("run-live", 1, "img", [], {})
    live = client.existing[-1]
    orphan = _orphan(NODE, "run-dead", 1)
    client.existing.append(orphan)

    removed = r.reconcile_orphans()

    assert removed == [{"run_id": "run-dead", "attempt": 1}]
    assert not live.removed and r.has("run-live")
    assert orphan.removed


def test_without_a_node_name_nothing_is_listed(monkeypatch, root):
    r, client = _make(monkeypatch, [_orphan(NODE, "run-dead", 1)], node=None)
    assert r.reconcile_orphans() == []
    assert client.list_calls == []
    assert not client.existing[0].removed


def test_a_container_without_the_labels_is_never_touched(monkeypatch, root):
    """Started by an older agent version: the filter does not match it, and the
    fake mirrors that -- no label, not listed, not removed."""
    unlabelled = FakeContainer()
    r, _client = _make(monkeypatch, [unlabelled])
    assert r.reconcile_orphans() == []
    assert not unlabelled.removed


# --- leaked staging files ------------------------------------------------------


def test_stale_sealed_staging_files_are_swept_and_fresh_ones_kept(monkeypatch, root):
    old = root / "fyp-sealed-run-x-1-abc.bin"
    fresh = root / "fyp-sealed-run-y-1-def.bin"
    other = root / "not-ours.bin"
    for p in (old, fresh, other):
        p.write_bytes(b"x")
    two_hours_ago = time.time() - 2 * 3600
    os.utime(old, (two_hours_ago, two_hours_ago))
    os.utime(other, (two_hours_ago, two_hours_ago))

    assert agent_mod._sweep_stale_staging() == 1
    assert not old.exists()
    assert fresh.exists() and other.exists()
