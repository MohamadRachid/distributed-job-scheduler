"""The in-container reader and writer (`workloads/dummy/fyp_data.py`, 2026-09-06).

**Why these live here.** The file under test ships inside a workload IMAGE, not in the
control plane and not in the agent — it has to be copyable into any image with nothing
but `cryptography` available. The control-plane suite runs in a container that does not
mount `workloads/`, so the host-side agent suite is where it can actually be imported.

**The test that earns the duplication.** The sealed format is implemented twice on
purpose (see the module docstring in either file). `test_the_two_implementations_agree`
seals with one and opens with the other, in both directions, so the day one of them
drifts is the day this fails rather than the day a run cannot read its dataset.

Run from the repo root:  pytest agent/tests -q
"""

import io
import json
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "workloads", "dummy"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "control-plane"))

import fyp_checkpoint  # noqa: E402
import fyp_data  # noqa: E402
from app.sealing import new_key, open_any, seal, seal_stream  # noqa: E402

KEY = new_key()
DATA = b"".join(b"row-%05d,marker-XYZ-%05d\n" % (i, i) for i in range(4000))


@pytest.fixture(autouse=True)
def with_key(monkeypatch, tmp_path):
    """Stand in for a redeemed ticket: the key is in this process's memory, which is
    the only place the real one ever is."""
    monkeypatch.setattr(fyp_data, "_KEY", KEY)
    monkeypatch.setenv("OUTPUT_DIR", str(tmp_path / "out"))
    yield
    monkeypatch.setattr(fyp_data, "_KEY", None)


def _sealed_file(tmp_path, data=DATA, chunk=1024, name="in.sealed"):
    path = tmp_path / name
    path.write_bytes(seal_stream(data, KEY, chunk_size=chunk))
    return str(path)


# --- reading ----------------------------------------------------------------


def test_the_two_implementations_agree(tmp_path):
    """One format, two implementations, both directions. This is the test that pays
    for writing it twice."""
    written_by_the_control_plane = _sealed_file(tmp_path)
    with fyp_data.open_input(written_by_the_control_plane) as f:
        assert f.read() == DATA

    with fyp_data.create_output("out.bin", chunk_size=1024) as f:
        f.write(DATA)
    written_by_the_container = os.path.join(os.environ["OUTPUT_DIR"], "out.bin")
    assert open_any(open(written_by_the_container, "rb").read(), KEY) == DATA


def test_reading_is_flat_in_memory_whatever_the_file_size(tmp_path):
    """The claim that replaced the RAM folder: what a container needs to READ a file
    does not grow with the file.

    Measured here as the reader's own cache rather than as process memory, because a
    unit test cannot see a container's peak — that number is printed by the live proof,
    from the kernel's own high-water mark. What this pins is the mechanism behind it:
    one piece is held at a time, whether the file is four pieces long or four hundred."""
    small = _sealed_file(tmp_path, b"x" * 4096, chunk=1024, name="small.sealed")
    big = _sealed_file(tmp_path, b"x" * (4096 * 100), chunk=1024, name="big.sealed")

    def held_after_full_read(path):
        with fyp_data.open_input(path, buffer_size=1024) as f:
            raw = f.raw
            while f.read(1024):
                pass
            return len(raw._cached)

    assert held_after_full_read(small) == held_after_full_read(big) == 1024


def test_seeking_opens_only_the_piece_it_lands_in(tmp_path):
    """What the fixed piece size buys: a position is arithmetic, so a reader can jump
    into the middle of a large file without opening what came before it."""
    path = _sealed_file(tmp_path, DATA, chunk=1024)
    with fyp_data.open_input(path) as f:
        f.seek(50_000)
        assert f.read(26) == DATA[50_000:50_026]
        assert f.seek(0, io.SEEK_END) == len(DATA)


def test_iteration_and_readline_work(tmp_path):
    """The reader is wrapped in a BufferedReader, so a workload's `for line in f`
    keeps working -- which is what makes this a one-line change in a loader."""
    path = _sealed_file(tmp_path, DATA, chunk=1024)
    with fyp_data.open_input(path) as f:
        first = f.readline()
        rest = sum(1 for _ in f)
    assert first == b"row-00000,marker-XYZ-00000\n"
    assert rest == 3999


def test_a_changed_byte_fails_at_the_piece_after_handing_back_the_earlier_ones(tmp_path):
    """Tampering is caught when the piece is TOUCHED. The bytes before it were already
    handed over, and that is the honest behaviour: the workload is told the moment the
    data stops being trustworthy, not after the whole file has been re-read."""
    path = tmp_path / "tampered.sealed"
    blob = bytearray(seal_stream(DATA, KEY, chunk_size=1024))
    blob[32 + 5 * (1024 + 28) + 40] ^= 0x01     # inside the SIXTH piece (index 5)
    path.write_bytes(bytes(blob))

    got = 0
    with pytest.raises(fyp_data.IntegrityError) as exc:
        with fyp_data.open_input(str(path), buffer_size=1024) as f:
            while True:
                block = f.read(1024)
                if not block:
                    break
                got += len(block)
    assert got == 5 * 1024, "the five good pieces were delivered before the bad one"
    # One-based in the sentence, zero-based in the arithmetic: the byte was changed
    # at index 5, which is the sixth piece, and that is the number a human is told.
    assert "piece 6 of" in str(exc.value), str(exc.value)


def test_the_marker_line_is_printed_so_the_platform_can_classify_it(tmp_path, capsys):
    """`##INTEGRITY_ERROR` is the contract with agent/classify.py — a printed line,
    the same idea as `##PROGRESS`, so any workload gets the diagnosis for free without
    being asked to cooperate."""
    path = tmp_path / "bad.sealed"
    blob = bytearray(seal_stream(b"z" * 100, KEY, chunk_size=1024))
    blob[-1] ^= 0x01
    path.write_bytes(bytes(blob))
    with pytest.raises(fyp_data.IntegrityError):
        with fyp_data.open_input(str(path)) as f:
            f.read()
    assert "##INTEGRITY_ERROR" in capsys.readouterr().err


def test_a_file_that_is_not_sealed_is_opened_as_it_is(tmp_path):
    """One line in the loader has to work in both worlds: an unsealed job, an old job,
    or the image run by hand outside the platform."""
    path = tmp_path / "plain.csv"
    path.write_bytes(b"a,b\n1,2\n")
    with fyp_data.open_input(str(path)) as f:
        assert f.read() == b"a,b\n1,2\n"


def test_a_sealed_file_with_no_key_stops_rather_than_guesses(tmp_path, monkeypatch):
    path = _sealed_file(tmp_path)
    monkeypatch.setattr(fyp_data, "_KEY", None)
    monkeypatch.delenv("FYP_TICKET", raising=False)
    monkeypatch.delenv("FYP_KEY_URL", raising=False)
    with pytest.raises(fyp_data.IntegrityError):
        fyp_data.open_input(path)


# --- writing ----------------------------------------------------------------


def test_the_writer_seals_and_a_short_file_is_one_piece(tmp_path):
    with fyp_data.create_output("small.bin") as f:
        f.write(b"tiny")
    blob = open(os.path.join(os.environ["OUTPUT_DIR"], "small.bin"), "rb").read()
    assert blob.startswith(b"FYPSEAL2")
    assert len(blob) == 32 + 12 + 4 + 16      # header + nonce + payload + tag
    assert open_any(blob, KEY) == b"tiny"


def test_many_small_writes_produce_the_same_file_as_one_big_write(tmp_path):
    """A workload writes however it likes, and the pieces fall where the format says —
    not where the calls happened to land."""
    with fyp_data.create_output("a.bin", chunk_size=1024) as f:
        f.write(DATA)
    one = open(os.path.join(os.environ["OUTPUT_DIR"], "a.bin"), "rb").read()
    with fyp_data.create_output("b.bin", chunk_size=1024) as f:
        for i in range(0, len(DATA), 37):
            f.write(DATA[i:i + 37])
    many = open(os.path.join(os.environ["OUTPUT_DIR"], "b.bin"), "rb").read()

    assert len(one) == len(many)              # same framing, different call pattern
    assert open_any(one, KEY) == open_any(many, KEY) == DATA


def test_without_a_key_the_writer_writes_plainly(tmp_path, monkeypatch):
    """An unsealed job, or the image run by hand: the same call, the file it always
    wrote."""
    monkeypatch.setattr(fyp_data, "_KEY", None)
    monkeypatch.delenv("FYP_TICKET", raising=False)
    with fyp_data.create_output("plain.json") as f:
        f.write(b'{"a": 1}')
    assert open(os.path.join(os.environ["OUTPUT_DIR"], "plain.json"), "rb").read() == b'{"a": 1}'


# --- checkpoints ------------------------------------------------------------


def test_a_checkpoint_is_sealed_and_reopens(tmp_path, monkeypatch):
    monkeypatch.setenv("CHECKPOINT_PATH", str(tmp_path / "checkpoint.bin"))
    assert fyp_checkpoint.save({"epoch": 4}, payload=b"\x00\x01weights")
    on_disk = open(os.environ["CHECKPOINT_PATH"], "rb").read()
    assert on_disk.startswith(b"FYPSEAL2"), "the agent carries ciphertext"
    state = fyp_checkpoint.load()
    assert state["epoch"] == 4 and state["fyp_payload"] == b"\x00\x01weights"


@pytest.mark.parametrize("older", ["json", "envelope", "one-piece-sealed"])
def test_all_three_older_checkpoint_formats_are_still_read(tmp_path, monkeypatch, older):
    """A run checkpointed by attempt 1 is resumed by attempt 2, possibly in a
    different image — so on any day this contract changes, the two attempts may
    disagree about the format. A reader that understood only the newest one would
    return None and silently restart the training, which is the exact defect
    `fyp_checkpoint` exists to prevent."""
    monkeypatch.setenv("CHECKPOINT_PATH", str(tmp_path / "checkpoint.bin"))
    body = {
        "json": json.dumps({"epoch": 7}).encode(),
        "envelope": b"FYPCKPT1\n" + json.dumps({"epoch": 7}).encode() + b"\nPAYLOAD",
        "one-piece-sealed": seal(json.dumps({"epoch": 7}).encode(), KEY),
    }[older]
    open(os.environ["CHECKPOINT_PATH"], "wb").write(body)

    state = fyp_checkpoint.load()
    assert state is not None and state["epoch"] == 7
    if older == "envelope":
        assert state["fyp_payload"] == b"PAYLOAD"


def test_a_plain_checkpoint_on_a_sealed_job_prints_no_false_alarm(
    tmp_path, monkeypatch, capsys
):
    """A defect this suite caught while it was being written.

    `open_bytes` tells one-piece bytes from plain bytes by TRYING to open them, so a
    checkpoint that was never sealed takes the failing path on the way to being
    returned unchanged. If that attempt printed `##INTEGRITY_ERROR`, every such run
    would carry a marker the platform reads as a diagnosis — a false accusation
    manufactured by the reader itself. Whoever decides a failure is real is the one
    that prints."""
    monkeypatch.setenv("CHECKPOINT_PATH", str(tmp_path / "checkpoint.bin"))
    open(os.environ["CHECKPOINT_PATH"], "wb").write(json.dumps({"epoch": 2}).encode())
    assert fyp_checkpoint.load() == {"epoch": 2}
    assert "##INTEGRITY_ERROR" not in capsys.readouterr().err


def test_an_unopenable_checkpoint_is_absent_rather_than_an_error(tmp_path, monkeypatch):
    """Starting the training again is always better than crashing on resume."""
    monkeypatch.setenv("CHECKPOINT_PATH", str(tmp_path / "checkpoint.bin"))
    blob = bytearray(seal_stream(b'{"epoch": 3}', KEY))
    blob[-1] ^= 0x01
    open(os.environ["CHECKPOINT_PATH"], "wb").write(bytes(blob))
    assert fyp_checkpoint.load() is None


def test_without_a_key_a_checkpoint_is_byte_for_byte_what_it_always_was(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("CHECKPOINT_PATH", str(tmp_path / "checkpoint.bin"))
    monkeypatch.setattr(fyp_data, "_KEY", None)
    monkeypatch.delenv("FYP_TICKET", raising=False)
    assert fyp_checkpoint.save({"epoch": 1})
    assert open(os.environ["CHECKPOINT_PATH"], "rb").read() == b'{"epoch": 1}'
