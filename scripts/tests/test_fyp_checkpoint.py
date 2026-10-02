"""Tests for the checkpoint adoption contract, `workloads/dummy/fyp_checkpoint.py`.

**Why this file lives here and not in a counted suite.** `fyp_checkpoint.py` is a
workload file, so it belongs to neither `control-plane/tests` nor `agent/tests`, and
adding it to either would move a published test count. `scripts/tests` is in
neither counted suite, as `control-plane/pytest.ini` (`testpaths = tests`) shows.

**What the contract has to survive.** Until 2026-09-01 this file had NO test
coverage at all, which Session 0 found and this file closes. The property that
matters most is the dual-format read: a run checkpointed by attempt 1 may be
resumed by attempt 2 running a different image, so the reader must understand the
old JSON file for ever even though it never writes one again.

The old-format fixtures below are written in the TRUE shapes both real adopters
save -- five keys with `torch_b64` for `workloads/trainer/train.py`, three keys and
no payload for `workloads/dummy/train.py` -- so the reader is proven against what is
actually on disk rather than against a reconstruction. The tensor bytes inside the
base64 are arbitrary: what is being pinned is the file's shape, not torch's.
"""

from __future__ import annotations

import base64
import importlib.util
import json
import os
import sys
from pathlib import Path

import pytest

_CONTRACT = Path(__file__).resolve().parents[2] / "workloads" / "dummy" / "fyp_checkpoint.py"


def _load_contract():
    """Import the contract file directly. It is not importable as a package: a
    workload copies this ONE file into its image, which is the whole point of it."""
    spec = importlib.util.spec_from_file_location("fyp_checkpoint_undertest", _CONTRACT)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture()
def ckpt(tmp_path, monkeypatch):
    """The contract, pointed at a throwaway checkpoint path."""
    mod = _load_contract()
    p = tmp_path / "state"
    monkeypatch.setenv("CHECKPOINT_PATH", str(p))
    return mod, p


# --- the true old shapes, as the two real adopters write them today ----------

TRAINER_TENSORS = b"\x80\x02torch-ish-bytes\x00\xff" * 40
OLD_TRAINER = {
    "epoch": 4,
    "accuracy": 0.9016,
    "loss": 0.2731,
    "history": [{"epoch": 1, "loss": 0.9}, {"epoch": 2, "loss": 0.5}],
    "torch_b64": base64.b64encode(TRAINER_TENSORS).decode("ascii"),
}
OLD_DUMMY = {"epoch": 7, "loss": 0.1429, "accuracy": 0.8571}


def _write_old(path: Path, state: dict) -> None:
    """Exactly what today's `save()` produces: one json.dump, nothing else."""
    with open(path, "w", encoding="utf-8") as f:
        json.dump(state, f)


# --- 1a: the old format must keep loading, for ever -------------------------

def test_old_trainer_five_key_file_still_loads(ckpt):
    mod, p = ckpt
    _write_old(p, OLD_TRAINER)
    state = mod.load()
    assert state == OLD_TRAINER
    # and the payload is still reachable the way the trainer reaches it today
    assert base64.b64decode(state["torch_b64"]) == TRAINER_TENSORS


def test_old_dummy_three_key_file_still_loads(ckpt):
    mod, p = ckpt
    _write_old(p, OLD_DUMMY)
    assert mod.load() == OLD_DUMMY


def test_old_format_carries_no_payload_key(ckpt):
    """An old file has no binary payload, so the reserved key must be absent
    rather than present-and-empty -- a workload checks `if payload is None`."""
    mod, p = ckpt
    _write_old(p, OLD_DUMMY)
    assert mod.PAYLOAD_KEY not in mod.load()


# --- 1b/1c: the new format ---------------------------------------------------

def test_new_format_round_trips_envelope_and_payload(ckpt):
    mod, p = ckpt
    env = {"epoch": 4, "accuracy": 0.9016, "loss": 0.2731, "history": [{"epoch": 1}]}
    assert mod.save(env, payload=TRAINER_TENSORS) is True
    state = mod.load()
    assert state[mod.PAYLOAD_KEY] == TRAINER_TENSORS
    assert {k: v for k, v in state.items() if k != mod.PAYLOAD_KEY} == env


def test_payload_is_stored_raw_and_not_text_packed(ckpt):
    """The whole point of the reshape: the tensors appear in the file verbatim,
    and the base64 of them does not appear at all."""
    mod, p = ckpt
    mod.save({"epoch": 1}, payload=TRAINER_TENSORS)
    raw = p.read_bytes()
    assert TRAINER_TENSORS in raw
    assert base64.b64encode(TRAINER_TENSORS) not in raw


def test_new_format_file_is_smaller_than_the_old_one_would_be(ckpt):
    mod, p = ckpt
    env = {"epoch": 4, "accuracy": 0.9016, "loss": 0.2731, "history": []}
    mod.save(env, payload=TRAINER_TENSORS)
    new_size = p.stat().st_size
    old_equivalent = len(json.dumps(
        dict(env, torch_b64=base64.b64encode(TRAINER_TENSORS).decode("ascii"))
    ).encode("utf-8"))
    assert new_size < old_equivalent


def test_empty_payload_round_trips_as_absent(ckpt):
    """`payload=b""` is not a payload. Absent and empty must not be told apart by
    a workload, because there is nothing to resume from in either case."""
    mod, p = ckpt
    mod.save({"epoch": 2}, payload=b"")
    assert mod.PAYLOAD_KEY not in mod.load()


# --- a payload-free save must cost exactly what it costs today ---------------

def test_payload_free_save_is_byte_identical_to_todays_json(ckpt):
    """The dummy adopter saves no tensors. Its file must not change at all -- not
    its bytes, not its size, not its shape."""
    mod, p = ckpt
    assert mod.save(OLD_DUMMY) is True
    expected = json.dumps(OLD_DUMMY).encode("utf-8")
    assert p.read_bytes() == expected


def test_payload_free_save_round_trips(ckpt):
    mod, p = ckpt
    mod.save(OLD_DUMMY)
    assert mod.load() == OLD_DUMMY


# --- corrupt of EITHER shape is ABSENT, never an error ----------------------

def test_corrupt_new_format_is_absent(ckpt):
    """A truncated payload cannot be told from a good one by length alone, so the
    case that matters is a mangled envelope: it must be absent, not a crash."""
    mod, p = ckpt
    mod.save({"epoch": 3}, payload=TRAINER_TENSORS)
    raw = bytearray(p.read_bytes())
    # The OPENING BRACE, not a byte inside the envelope: replacing a character in
    # the middle of {"epoch": 3} yields {"e~och": 3}, which is still valid json and
    # loads fine. That was this test's own first mistake, and the code was right.
    raw[len(mod.MAGIC)] = ord("~")
    p.write_bytes(bytes(raw))
    assert mod.load() is None


def test_new_format_header_without_an_envelope_is_absent(ckpt):
    mod, p = ckpt
    p.write_bytes(mod.MAGIC)
    assert mod.load() is None


def test_corrupt_old_format_is_absent(ckpt):
    mod, p = ckpt
    p.write_text('{"epoch": 3, "loss":', encoding="utf-8")
    assert mod.load() is None


def test_a_file_of_neither_shape_is_absent(ckpt):
    mod, p = ckpt
    p.write_bytes(b"\x00\x01\x02 not json and not ours \xff")
    assert mod.load() is None


def test_no_file_at_all_is_absent(ckpt):
    mod, _p = ckpt
    assert mod.load() is None


def test_no_checkpoint_path_is_absent_and_save_is_a_no_op(ckpt, monkeypatch):
    mod, _p = ckpt
    monkeypatch.delenv("CHECKPOINT_PATH")
    assert mod.load() is None
    assert mod.save({"epoch": 1}) is False


# --- the write path itself ---------------------------------------------------

@pytest.mark.skipif(
    sys.platform == "win32",
    reason=(
        "POSIX permission bits. Measured on this machine: os.fchmod(fd, 0o644) leaves "
        "mode 666 on Windows, so the assertion can only run where the contract "
        "actually runs, which is inside a Linux container. Proven there by "
        "docs/evidence/real_workload_2026-08-13/checkpoint_permissions.txt."
    ),
)
@pytest.mark.parametrize("payload", [None, TRAINER_TENSORS])
def test_save_leaves_the_file_world_readable(ckpt, payload):
    """0644, both formats. A checkpoint only the container can read never leaves
    the machine, and it fails silently when it does not."""
    mod, p = ckpt
    mod.save({"epoch": 1}, payload=payload) if payload else mod.save({"epoch": 1})
    assert oct(p.stat().st_mode)[-3:] == "644"


@pytest.mark.parametrize("payload", [None, TRAINER_TENSORS])
def test_save_leaves_no_temporary_file_behind(ckpt, payload):
    """Temp-then-rename: readers see a whole old state or a whole new one."""
    mod, p = ckpt
    mod.save({"epoch": 1}, payload=payload) if payload else mod.save({"epoch": 1})
    leftovers = [f for f in os.listdir(p.parent) if f.startswith(".ckpt-")]
    assert leftovers == []


def test_second_save_overwrites_in_place(ckpt):
    """One file, one name -- what bounds storage with no cleanup job."""
    mod, p = ckpt
    mod.save({"epoch": 1}, payload=b"first")
    mod.save({"epoch": 2}, payload=b"second")
    assert len([f for f in os.listdir(p.parent)]) == 1
    state = mod.load()
    assert state["epoch"] == 2 and state[mod.PAYLOAD_KEY] == b"second"


def test_a_new_format_save_replaces_an_old_format_file(ckpt):
    """The migration in one attempt: attempt 1 wrote JSON, attempt 2 saves the new
    shape over it, and what comes back is the new state and not a mixture."""
    mod, p = ckpt
    _write_old(p, OLD_TRAINER)
    mod.save({"epoch": 9}, payload=b"new-bytes")
    state = mod.load()
    assert state["epoch"] == 9
    assert "torch_b64" not in state
    assert state[mod.PAYLOAD_KEY] == b"new-bytes"
