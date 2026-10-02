"""Sealing — the one place that encrypts and decrypts private job input (W6b).

Two pure functions and a key generator. Nothing here touches the database, the
network, or a file, so both directions are unit-testable without any of that.

**What "sealed" means, exactly.** We use **AES-GCM**, an *authenticated* cipher:
one operation gives two guarantees at once.

  * secrecy   — without the key the bytes are noise (a stolen MinIO object, a
                copied file on a worker's disk, a sniffed download: all gibberish);
  * integrity — GCM appends a 16-byte *tag* computed over the ciphertext. Opening
                recomputes it. Change ANY byte — the data, the tag, the nonce —
                and the tag no longer matches, so `open_sealed` raises instead of
                returning wrong-but-plausible data. This is guaranteed by the
                math, not by a check we could forget: it holds even against an
                attacker with full control of the machine.

**Blob format (pinned — the wall):** ``nonce(12 bytes) || ciphertext+tag``.
The nonce is a fresh random number for each seal. It is NOT a secret (it travels
in the clear at the front of the blob); it exists so that sealing the same file
twice with the same key produces different bytes. 12 bytes is the size AES-GCM is
specified for.

**Honesty guard:** job data is sealed
from submit until it opens inside the container, never touches the worker's disk
in readable form, and any tampering breaks the seal and fails the run. That
protects it from *users* of the machine — not from its *root administrator*, who
can read the container's memory while it runs. Root-proof privacy needs TEE
hardware (our named future work). Never claim root-proof privacy.
"""

from __future__ import annotations

import base64
import os

# 256-bit keys: the strongest standard AES size, and a fresh one PER JOB, so one
# job's key can never open another job's data.
KEY_BYTES = 32
# AES-GCM's specified nonce length. Stored as the blob's first 12 bytes.
NONCE_BYTES = 12


class SealError(Exception):
    """The seal did not verify — the bytes were tampered with, truncated, or the
    key is wrong. Deliberately one error for all three: from the outside they are
    the same fact ("these bytes are not the sealed data this key opens"), and
    saying which would leak information."""


def new_key() -> bytes:
    """A fresh 256-bit key from the OS random source. One per job."""
    return os.urandom(KEY_BYTES)


def key_to_b64(key: bytes) -> str:
    """Text form for storing the key in a database column."""
    return base64.b64encode(key).decode("ascii")


def key_from_b64(text: str) -> bytes:
    return base64.b64decode(text)


def seal(data: bytes, key: bytes) -> bytes:
    """Encrypt + authenticate `data`. Returns ``nonce || ciphertext+tag``.

    A fresh random nonce every call, so sealing the same file twice never yields
    the same bytes (an observer learns nothing from comparing two blobs)."""
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM

    if len(key) != KEY_BYTES:
        raise SealError("key must be 32 bytes")
    nonce = os.urandom(NONCE_BYTES)
    return nonce + AESGCM(key).encrypt(nonce, data, None)


def open_sealed(blob: bytes, key: bytes) -> bytes:
    """Verify + decrypt a blob produced by `seal`. Raises `SealError` if the tag
    does not match — i.e. if a single byte changed anywhere, or the key is wrong.

    This raise IS the tamper detection. There is no "decrypt anyway" path."""
    from cryptography.exceptions import InvalidTag
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM

    if len(key) != KEY_BYTES:
        raise SealError("key must be 32 bytes")
    if len(blob) <= NONCE_BYTES:
        raise SealError("sealed blob is too short to be valid")
    nonce, body = blob[:NONCE_BYTES], blob[NONCE_BYTES:]
    try:
        return AESGCM(key).decrypt(nonce, body, None)
    except InvalidTag as exc:
        raise SealError(
            "integrity check failed — the sealed data was changed, or this is the wrong key"
        ) from exc


# ---------------------------------------------------------------------------
# The FRAMED format (2026-09-06, "sealed by default")
# ---------------------------------------------------------------------------
#
# **Why a second format at all.** The one above seals a file as ONE piece: to read
# any of it you must hold all of it, and to verify any of it you must decrypt all of
# it. That is why the old private path had to open the whole dataset into a RAM
# folder before the workload could touch it, and why a 6 GB dataset on an 8 GB
# machine left nothing for the training. The size of the file became the size of the
# memory needed to read it, which is not a design — it is a ceiling nobody chose.
#
# The framed format cuts the plaintext into fixed-size pieces and seals each piece
# **independently**, with its own nonce and its own tag. A reader that wants the byte
# at 900,000,000 opens the one frame holding it and nothing else, so the memory a
# container needs in order to read a file stops depending on how big the file is.
#
# **Layout.**
#
#     header (32 bytes, in the clear, integrity-protected by every frame's AAD)
#       magic      8   b"FYPSEAL2"
#       version    1   0x01
#       reserved   3   zero
#       chunk_size 4   big-endian: PLAINTEXT bytes per frame
#       stream_id  16  random, fresh for every seal
#     frame i        nonce(12) || ciphertext+tag(len + 16)
#
# **What stops a frame being moved.** AES-GCM lets a sealer bind data that travels
# in the clear to the tag — "additional authenticated data", AAD. Every frame is
# sealed with header || frame_index || is_last as its AAD, so a frame carries proof
# of WHICH file it belongs to, WHERE in that file it sits, and WHETHER the file ends
# there. That closes the four attacks a per-frame format would otherwise open:
#
#   * reorder two frames -> each is opened at the wrong index -> the tag fails;
#   * splice in a frame from another sealed file, even one sealed with the same key
#     -> its stream_id is not this file's -> the tag fails;
#   * truncate the file -> the frame that is now last was sealed as not-last -> the
#     tag fails. This is the one a per-frame format gets wrong if it is careless: a
#     file that simply STOPS early would otherwise open cleanly and hand a workload
#     half a dataset with nothing anywhere to say so;
#   * edit the header — claim a different frame size, say — and the header is in
#     every frame's AAD, so every tag fails.
#
# So the guarantee the one-piece format gave over a whole file is kept, and it is now
# enforced piece by piece, at the moment each piece is touched.
#
# **Frames are a fixed size on purpose.** Every frame but the last carries exactly
# chunk_size plaintext bytes, so a frame's position is arithmetic rather than a
# lookup: frame i starts at HEADER_BYTES + i * (chunk_size + FRAME_OVERHEAD). That is
# what lets the in-container reader seek — no index, no table of contents, nothing
# that could disagree with the data it describes.
#
# **An empty file still gets one frame.** Zero frames would make "an empty file" and
# "a file truncated to nothing" the same bytes, and detecting truncation is exactly
# what the AAD is there for.

# The framed format's first eight bytes. A reader sniffs these rather than being told
# which format to expect, because whoever reads a sealed file is very often not
# whoever wrote it: a checkpoint written by attempt 1 on one machine is read by
# attempt 2 on another, possibly running a different image. That lesson is already
# written into workloads/dummy/fyp_checkpoint.py, dated 2026-09-01.
SEAL_MAGIC = b"FYPSEAL2"
SEAL_VERSION = 1
HEADER_BYTES = 32
STREAM_ID_BYTES = 16
# AES-GCM's tag is 16 bytes, appended to the ciphertext by encrypt().
TAG_BYTES = 16
# What one frame costs over its plaintext: its own nonce and its own tag.
FRAME_OVERHEAD = NONCE_BYTES + TAG_BYTES
# 4 MiB of plaintext per frame. The number settles two things that pull against each
# other: a container's floor memory while reading (one frame at a time) and the
# per-frame overhead (28 bytes, 0.0007% at this size). 4 MiB is small enough that the
# reader's footprint is flat and unremarkable on any machine in the pool, and large
# enough that a gigabyte-scale dataset is a few hundred frames rather than a million.
DEFAULT_CHUNK_BYTES = 4 * 1024 * 1024


def is_sealed(blob: bytes) -> bool:
    """True if these bytes are the FRAMED format. Cheap enough to run on a prefix.

    This is the whole test the control plane applies to an artefact before storing
    it: sealed bytes are stored, anything else is refused. It is deliberately a
    statement about the BYTES, not about who sent them or what they claim."""
    return blob[: len(SEAL_MAGIC)] == SEAL_MAGIC


def _header(chunk_size: int, stream_id: bytes) -> bytes:
    return (
        SEAL_MAGIC
        + bytes([SEAL_VERSION])
        + b"\x00\x00\x00"
        + chunk_size.to_bytes(4, "big")
        + stream_id
    )


def _aad(header: bytes, index: int, last: bool) -> bytes:
    """What a frame's tag is computed over besides its own ciphertext: the whole
    header, this frame's index, and whether the file ends here."""
    return header + index.to_bytes(8, "big") + (b"\x01" if last else b"\x00")


def parse_header(blob: bytes) -> tuple[bytes, int]:
    """Return (header, chunk_size), or raise SealError.

    Nothing here is trusted yet — the header is unauthenticated until some frame
    opens against it. Which is why an impossible frame size is rejected on plain
    sanity grounds, before it is ever used to size an allocation."""
    if len(blob) < HEADER_BYTES:
        raise SealError("sealed data is too short to hold a header")
    header = blob[:HEADER_BYTES]
    if header[: len(SEAL_MAGIC)] != SEAL_MAGIC:
        raise SealError("not the framed sealed format")
    if header[8] != SEAL_VERSION:
        raise SealError(f"unknown sealed-format version {header[8]}")
    chunk_size = int.from_bytes(header[12:16], "big")
    if chunk_size <= 0:
        raise SealError("sealed header declares an impossible frame size")
    return header, chunk_size


def frame_count(total_bytes: int, chunk_size: int) -> int:
    """How many frames a sealed file of total_bytes holds. Arithmetic, not a scan.

    Every frame but the last is chunk_size + FRAME_OVERHEAD long, so the body divides
    cleanly except for whatever the last — possibly short — frame leaves."""
    body = total_bytes - HEADER_BYTES
    if body < FRAME_OVERHEAD:
        raise SealError("sealed data holds no complete frame")
    full = chunk_size + FRAME_OVERHEAD
    whole, rest = divmod(body, full)
    if rest == 0:
        return whole
    if rest < FRAME_OVERHEAD:
        raise SealError("sealed data ends inside a frame")
    return whole + 1


def plain_size(total_bytes: int, chunk_size: int) -> int:
    """The plaintext length of a sealed file, from its size alone — so a reader can
    answer size() and seek-to-end without opening a single frame."""
    frames = frame_count(total_bytes, chunk_size)
    body = total_bytes - HEADER_BYTES
    return body - frames * FRAME_OVERHEAD


def seal_stream(data: bytes, key: bytes, chunk_size: int = DEFAULT_CHUNK_BYTES) -> bytes:
    """Seal data into independently-opened frames. Returns header || frames.

    The counterpart of seal() above, and what every new seal uses. seal() is kept
    because the files it wrote are still in the store and are still opened."""
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM

    if len(key) != KEY_BYTES:
        raise SealError("key must be 32 bytes")
    if chunk_size <= 0:
        raise SealError("frame size must be positive")
    header = _header(chunk_size, os.urandom(STREAM_ID_BYTES))
    aes = AESGCM(key)
    # An empty input still gets one frame: see the note above about truncation.
    pieces = [data[i : i + chunk_size] for i in range(0, len(data), chunk_size)] or [b""]
    out = bytearray(header)
    last_index = len(pieces) - 1
    for index, piece in enumerate(pieces):
        nonce = os.urandom(NONCE_BYTES)
        out += nonce
        out += aes.encrypt(nonce, piece, _aad(header, index, index == last_index))
    return bytes(out)


def open_frame(blob: bytes, key: bytes, index: int) -> bytes:
    """Open ONE frame of a framed blob.

    The unit of work the in-container reader is built on, exposed here so that the
    control plane and the tests exercise the same code path the container does."""
    from cryptography.exceptions import InvalidTag
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM

    if len(key) != KEY_BYTES:
        raise SealError("key must be 32 bytes")
    header, chunk_size = parse_header(blob)
    frames = frame_count(len(blob), chunk_size)
    if not 0 <= index < frames:
        raise SealError(f"piece {index} is outside this sealed file")
    start = HEADER_BYTES + index * (chunk_size + FRAME_OVERHEAD)
    last = index == frames - 1
    end = len(blob) if last else start + chunk_size + FRAME_OVERHEAD
    record = blob[start:end]
    try:
        return AESGCM(key).decrypt(
            record[:NONCE_BYTES], record[NONCE_BYTES:], _aad(header, index, last)
        )
    except InvalidTag as exc:
        # One-based in the sentence, zero-based in the arithmetic — the same rule
        # the in-container reader follows, so the two implementations cannot print
        # the same failure with different numbers.
        raise SealError(
            f"integrity check failed on piece {index + 1} of {frames} — the sealed "
            "data was changed, moved, truncated, or this is the wrong key"
        ) from exc


def open_stream(blob: bytes, key: bytes) -> bytes:
    """Open every frame and return the whole plaintext.

    Used where the whole file is wanted anyway — the control plane unsealing an
    artefact for its owner's download. The in-container reader does NOT use this: it
    opens one frame at a time, which is the entire point of the format."""
    _header_bytes, chunk_size = parse_header(blob)
    frames = frame_count(len(blob), chunk_size)
    return b"".join(open_frame(blob, key, i) for i in range(frames))


def open_any(blob: bytes, key: bytes) -> bytes:
    """Open a blob of EITHER format, sniffed from its first bytes.

    The one-piece format is never written again and is read for ever: inputs sealed
    before 2026-09-06 are still in the store, and a checkpoint written by an earlier
    attempt is still opened by a later one."""
    return open_stream(blob, key) if is_sealed(blob) else open_sealed(blob, key)
