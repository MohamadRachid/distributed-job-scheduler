"""The clean-machine gate's memory threshold, fired at the boundary in both directions.

WHAT THIS IS FOR
----------------
`harness.require_clean_machine()` refuses to run a measurement campaign on a
machine that is not fit to measure on. On 2026-08-30 the lead ruled MIN_FREE_MB
down from 8192 to 4096. A threshold that has never been
seen to refuse at N-1 and pass at N is a number somebody typed, not a boundary,
so this file pins the boundary itself:

    4095 MB available -> REFUSED, exit 1, and the operator is told why
    4096 MB available -> PASSES, and the reading is handed back for the manifest

The gate compares `available < min_free_mb`, so 4096 is the first passing
value. That off-by-one is the whole point of testing at the boundary rather
than at 1000 and 9000, which any threshold between them would satisfy.

WHY THE OLD VALUE NEEDED A BOUNDARY TEST AND NEVER GOT ONE
-----------------------------------------------------------
8192 was never calibrated. It was written from a refusal (3.7 GB, nine hours of
uptime) rather than from a passing run, and no passing run could have informed
it: `machine_state()` landed on 2026-08-16 and the last campaign before it, E2c,
ran on 2026-08-11 - so no manifest in this repository records the memory its own
campaign had. `test_the_old_threshold_refuses_the_machine_the_ruling_was_made_on`
below is the direction that proves the ruling changed an outcome rather than a
comment: today's real reading passes at 4096 and refuses at 8192.

HOW THE READING IS CONTROLLED
------------------------------
By patching `psutil.virtual_memory` and `psutil.boot_time`, NOT by stubbing
`machine_state()`. The real `int(vm.available / (1024 * 1024))` arithmetic runs,
so the test exercises the code that decides the number - including the
truncation - rather than a reimplementation of it. `available` is fed as exact
whole mebibytes so the boundary lands where the test says it lands.

`cpu_percent` is patched only to keep the run fast and deterministic: it is
sampled over a real 1.0 s interval in production and is recorded but NEVER
gated, so it cannot affect any assertion here.

WHERE THIS LIVES, AND WHY NOT IN A COUNTED SUITE
-------------------------------------------------
Alongside `test_stage_demo_schema_check.py`, deliberately outside both counted
suites, for the same reason that one is. The control-plane suite runs inside a
pinned 3.12 container that mounts only `control-plane/` and cannot see
`scripts/`. The agent suite is host-side, and its count is published. Adding
tests there would move a published number for a script that is neither the
control plane nor the agent.

Run it with:  python -m pytest scripts/tests/test_clean_machine_gate.py -q
"""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

REPO = Path(__file__).resolve().parents[2]
MIB = 1024 * 1024


def _load_harness():
    """Import scripts/experiments/harness.py by path.

    Registered in sys.modules before exec_module because the module defines
    dataclasses, and dataclasses resolves annotations through
    sys.modules[cls.__module__] - which is None for a module loaded by path and
    never registered.
    """
    path = REPO / "scripts" / "experiments" / "harness.py"
    spec = importlib.util.spec_from_file_location("fyp_harness_under_test", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules["fyp_harness_under_test"] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def harness():
    return _load_harness()


@pytest.fixture
def machine(monkeypatch, harness):
    """Present the gate with a machine of exactly the given size and age."""
    import psutil

    def present(*, available_mb: int, total_mb: int = 15599, uptime_h: float = 0.5):
        monkeypatch.setattr(
            psutil, "virtual_memory",
            lambda: SimpleNamespace(available=available_mb * MIB, total=total_mb * MIB),
        )
        monkeypatch.setattr(harness.time, "time", lambda: uptime_h * 3600.0)
        monkeypatch.setattr(psutil, "boot_time", lambda: 0.0)
        # Recorded, never gated - patched only for speed and determinism.
        monkeypatch.setattr(psutil, "cpu_percent", lambda interval=None: 4.0)

    return present


# --- The boundary, both directions ------------------------------------------

def test_refuses_at_4095(machine, harness, capsys):
    """One mebibyte under the threshold is refused, and the operator is told why."""
    machine(available_mb=4095)

    with pytest.raises(SystemExit) as exit_info:
        harness.require_clean_machine()

    assert exit_info.value.code == 1, "a refusal must exit non-zero, not merely warn"

    stderr = capsys.readouterr().err
    assert "STOP" in stderr
    # The label matches the psutil field actually read (.available, not .free).
    # Saying "free" sent a reader to Task Manager's Free figure, a different number.
    assert "available memory 4095 MB of 15599 MB" in stderr
    assert "4096 MB this campaign requires" in stderr
    assert "free memory" not in stderr, "the old mislabel must not come back"


def test_passes_at_4096(machine, harness, capsys):
    """The threshold value itself passes, and the reading is handed back."""
    machine(available_mb=4096)

    state = harness.require_clean_machine()

    assert state["available_mb"] == 4096
    assert state["total_mb"] == 15599
    # The key is named for the psutil field it holds. Pinned so the old name,
    # which sent readers to a different number, cannot quietly come back.
    assert "free_mb" not in state
    # Handed back so manifest() can record the conditions beside the numbers.
    assert state["uptime_h"] == 0.5
    assert state["cpu_idle_pct"] == 96.0

    out = capsys.readouterr().out
    assert "machine ready: 4096 MB available of 15599" in out
    assert "STOP" not in out


# --- The direction that proves the ruling changed an outcome -----------------

def test_the_old_threshold_refuses_the_machine_the_ruling_was_made_on(machine, harness):
    """6407 MB available - the real 2026-08-30 reading - passes now and refused before.

    Without this the two tests above would pass just as happily against a gate
    that had never moved.
    """
    machine(available_mb=6407)

    state = harness.require_clean_machine()          # new value: passes
    assert state["available_mb"] == 6407

    with pytest.raises(SystemExit):                  # old value: refuses
        harness.require_clean_machine(min_free_mb=8192)


# --- The limbs that must NOT have moved --------------------------------------

def test_uptime_limb_still_refuses_independently(machine, harness, capsys):
    """Lowering the memory threshold must not have disabled the other gate."""
    machine(available_mb=15000, uptime_h=9.0)

    with pytest.raises(SystemExit):
        harness.require_clean_machine()

    stderr = capsys.readouterr().err
    assert "uptime 9.0 h is above the 3.0 h ceiling" in stderr
    assert "available memory" not in stderr, "memory was fine; only uptime should be named"


def test_unreadable_state_still_refuses(monkeypatch, harness, capsys):
    """psutil missing is refused, not passed - conditions unknown is not conditions met."""
    import psutil

    def explode():
        raise RuntimeError("psutil unavailable")

    monkeypatch.setattr(psutil, "virtual_memory", explode)

    with pytest.raises(SystemExit):
        harness.require_clean_machine()

    assert "cannot read machine state" in capsys.readouterr().err


def test_the_constant_is_the_ruled_value(harness):
    """Pins 2026-08-30k itself, so a silent drift back is a failing test."""
    assert harness.MIN_FREE_MB == 4096
    assert harness.MAX_UPTIME_H == 3.0
