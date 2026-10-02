"""fyp_data.py — the one file a workload adds to read and write SEALED data.

**This is the adoption contract**, and it is the same philosophy as the ``##PROGRESS``
line (W5b) and ``fyp_checkpoint.py`` (2026-08-13): one small readable file a real
training script adopts in a minute, not a framework it has to be rewritten for.

Your one change is the line where you open your dataset::

    import fyp_data

    with fyp_data.open_input() as f:      # instead of open(os.environ["INPUT_PATH"])
        for line in f:
            ...

and, if your job writes result files, the line where you create them::

    with fyp_data.create_output("model.bin") as f:
        f.write(weights)

That is the whole contract. Everything else stays yours.

**Why you get anything at all.** Every job on this platform has its own key. Your
dataset is sealed before it is stored, so what sits in object storage and what sits on
the worker's disk is ciphertext — unreadable to the machine's other users, to its
backups, to anyone who copies the file, and to us. It is opened only here, inside your
running container, and only a piece at a time.

**Nothing is opened into a file.** The old version of this contract decrypted the
whole dataset into a RAM folder before your program started, which meant a 6 GB
dataset needed 6 GB of memory before a single line of training ran. This reader opens
one piece when you ask for the bytes in it, so the memory it needs does not grow with
the size of the file, and there is no folder anywhere holding your plaintext.

**Tampering is caught at the piece, not at the end.** Each piece carries its own tag.
Change one byte anywhere and the read of THAT piece raises — after the pieces before
it have been handed over, which is the honest behaviour: you are told the moment the
data stops being trustworthy, not after the whole file has been re-read. A piece
cannot be moved, swapped for a piece of another file, or dropped off the end either;
each one is sealed together with where it sits and whether the file ends there.

**What this does NOT protect against (say it plainly).** While your program runs, the
pieces it has asked for are plain in this container's memory, because a computer
cannot compute on data it cannot read. Someone with **root** on this machine can read
that memory. This design keeps the data from the machine's users, its disk, its
backups and anyone who intercepts it — not from its administrator. Root-proof
execution needs TEE hardware, which is named future work.

**The format** is pinned in ``protocol.md`` §2 and implemented twice on purpose: here,
and in ``control-plane/app/sealing.py``. Two implementations of a wire format is a real
cost, and it is paid deliberately — this file has to be copyable into any image with
nothing but ``cryptography`` available, and importing the control plane into a
workload's container would drag a web framework and a database driver in with it. Both
sides have tests, and the proof of 2026-09-06 reads a file sealed by one with the
other.
"""

import io
import json
import os
import ssl
import sys
import urllib.error
import urllib.request

# --- the format (must match control-plane/app/sealing.py) --------------------
MAGIC = b"FYPSEAL2"
VERSION = 1
HEADER_BYTES = 32
NONCE_BYTES = 12
TAG_BYTES = 16
FRAME_OVERHEAD = NONCE_BYTES + TAG_BYTES
DEFAULT_CHUNK_BYTES = 4 * 1024 * 1024


class IntegrityError(Exception):
    """A piece did not open: it was changed, moved, truncated, or this is the wrong
    key. One error for all of those, because from the outside they are one fact."""


def fail(message):
    """Report a broken seal in the one shape the platform classifies (the marker line
    is the contract — see agent/classify.py) and stop.

    Printed AND raised: printed so the platform names the failure without the workload
    having to cooperate, raised so a workload that wants to handle it can."""
    print("##INTEGRITY_ERROR: %s" % message, file=sys.stderr, flush=True)
    raise IntegrityError(message)


# --- the key ----------------------------------------------------------------
# Held in this process's memory, and (2026-09-07) in ONE place a second process in
# the SAME container can read it from: a file under /dev/shm, which is a folder that
# lives in the container's RAM and dies with it. Never put in an environment
# variable, never logged, never on the worker's disk.
#
# Why the cache exists (walk 1, row 32). The ticket is single-use against the outside
# — the control plane spends it in the same transaction that answers, so a replay is
# refused — and that is right. But a job's entrypoint may be a chain of several steps
# (`sh -c "python prep.py && python train.py"`, the form's own suggestion), and each
# step is a NEW process with an empty memory. The first step redeemed the ticket; the
# second found it spent and could neither read the dataset nor seal its results. So
# the first process to obtain the key leaves it here, mode 0600, and every later
# process in this container reads it instead of asking again. The ticket stays
# single-use; what changed is that "once per container" now means the container and
# not its first process.
_KEY = None
KEY_CACHE_ENV = "FYP_KEY_CACHE"
DEFAULT_KEY_CACHE = "/dev/shm/.fyp_key"


def _key_cache_path():
    return os.environ.get(KEY_CACHE_ENV) or DEFAULT_KEY_CACHE


def _read_cached_key():
    """The key an earlier process in this container left, or None."""
    try:
        with open(_key_cache_path(), "rb") as f:
            data = f.read()
    except OSError:
        return None
    return data or None


def _write_cached_key(key_bytes):
    """Best-effort: a container whose RAM folder is missing simply pays one ticket
    per process, which is what it did before the cache existed."""
    path = _key_cache_path()
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "wb") as f:
            f.write(key_bytes)
    except OSError:
        pass


def _tls_context(key_url, ca_pem):
    """Verify the control plane, or refuse to speak to it.

    None for a plain-http key URL (an unencrypted deployment). For https, a context
    that trusts EXACTLY the given authority and nothing else — not the image's system
    trust store — so a certificate from any other authority is refused even if that
    authority is a public one. `cadata` takes the certificate as text, so the PEM
    never becomes a file: nothing on disk to clean up afterwards."""
    if not key_url.lower().startswith("https://"):
        return None
    if not ca_pem:
        # An https URL we cannot verify. Failing here is the point: continuing would
        # hand this job's key to whoever answered.
        fail("the key endpoint is https but no FYP_CA_PEM was provided to verify it")
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.verify_mode = ssl.CERT_REQUIRED
    ctx.check_hostname = True
    ctx.load_verify_locations(cadata=ca_pem)
    return ctx


def _redeem(key_url, ticket, ca_pem=None):
    """Trade the single-use ticket for this job's key. The control plane stamps the
    ticket as used inside the same transaction that answers, so this can only ever
    succeed once — a replay gets 410 Gone, and `docker inspect` on a running
    container shows a ticket that is already dead."""
    body = json.dumps({"ticket": ticket}).encode("utf-8")
    req = urllib.request.Request(key_url, data=body, method="POST")
    req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(
            req, timeout=30, context=_tls_context(key_url, ca_pem)
        ) as resp:
            return json.loads(resp.read().decode("utf-8"))["key_b64"]
    except urllib.error.HTTPError as exc:
        # Name the real cause (walk 1, row 32). The control plane's codes are the
        # contract (protocol.md §9): each one is a different fact about this run.
        raise KeyUnavailable(_explain_http(exc.code, key_url)) from exc
    except urllib.error.URLError as exc:
        raise KeyUnavailable(
            "could not reach the control plane at %s to obtain the key (%s)"
            % (key_url, getattr(exc, "reason", exc))
        ) from exc


class KeyUnavailable(Exception):
    """The key could not be obtained, and the message says why."""


def _explain_http(code, key_url):
    if code == 410:
        return (
            "the one-time key ticket has already been used or has expired (HTTP 410 "
            "from %s); a later step in this container should find the key cached in "
            "%s, so this usually means the container was restarted or the cache was "
            "removed" % (key_url, _key_cache_path())
        )
    if code == 409:
        return (
            "this run was re-dispatched to another machine, so this container's "
            "ticket is no longer valid (HTTP 409); its work is not wanted any more"
        )
    if code == 404:
        return "the control plane does not know this key ticket (HTTP 404)"
    return "the control plane answered HTTP %d when asked for the key" % code


def key():
    """This job's key, fetched once and kept in memory.

    Returns None when the platform gave this container no ticket, which is not an
    error: an unsealed job, an old-shaped private job (that one uses fyp_open.py), or
    an image being run by hand outside the platform. Callers treat None as "there is
    nothing to unseal here" rather than as a failure."""
    global _KEY
    if _KEY is not None:
        return _KEY
    ticket = os.environ.get("FYP_TICKET")
    key_url = os.environ.get("FYP_KEY_URL")
    if not (ticket and key_url):
        return None
    # A step that runs after the first one in this container finds the key here and
    # never touches the ticket, which is already spent.
    cached = _read_cached_key()
    if cached is not None:
        _KEY = cached
        return _KEY
    import base64

    try:
        _KEY = base64.b64decode(_redeem(ticket=ticket, key_url=key_url,
                                        ca_pem=os.environ.get("FYP_CA_PEM")))
    except IntegrityError:
        raise
    except KeyUnavailable as exc:
        fail("could not obtain the key: %s" % exc)
    except Exception as exc:  # noqa: BLE001 - any failure to get the key stops the run
        fail("could not obtain the key (%s)" % exc)
    _write_cached_key(_KEY)
    return _KEY


# --- reading ----------------------------------------------------------------


def _aad(header, index, last):
    return header + index.to_bytes(8, "big") + (b"\x01" if last else b"\x00")


def is_sealed(prefix):
    """Are these the first bytes of a sealed file?"""
    return prefix[: len(MAGIC)] == MAGIC


class SealedReader(io.RawIOBase):
    """A read-only, seekable view of a sealed file that opens one piece at a time.

    Wrapped in a BufferedReader by `open_input`, which is what gives you `readline`,
    iteration and the rest for free. The memory this holds is one piece, whatever the
    file's size — which is the whole reason the format has pieces."""

    def __init__(self, fh, key_bytes):
        self._fh = fh
        self._key = key_bytes
        fh.seek(0, io.SEEK_END)
        self._total_bytes = fh.tell()
        fh.seek(0)
        header = fh.read(HEADER_BYTES)
        if len(header) < HEADER_BYTES or not is_sealed(header):
            fail("this file is not in the sealed format")
        if header[8] != VERSION:
            fail("unknown sealed-format version %d" % header[8])
        self._header = header
        self._chunk = int.from_bytes(header[12:16], "big")
        if self._chunk <= 0:
            fail("sealed header declares an impossible piece size")
        body = self._total_bytes - HEADER_BYTES
        full = self._chunk + FRAME_OVERHEAD
        whole, rest = divmod(body, full)
        if body < FRAME_OVERHEAD or (rest and rest < FRAME_OVERHEAD):
            fail("sealed file ends inside a piece")
        self._frames = whole if rest == 0 else whole + 1
        self._size = body - self._frames * FRAME_OVERHEAD
        self._pos = 0
        self._cached_index = -1
        self._cached = b""

    # -- the pieces --
    def _piece(self, index):
        """Open piece `index`, or hand back the one already open.

        The cache is exactly one piece, so a sequential read opens each piece once
        and a seek backwards into the piece you are already in costs nothing."""
        if index == self._cached_index:
            return self._cached
        from cryptography.exceptions import InvalidTag
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM

        last = index == self._frames - 1
        start = HEADER_BYTES + index * (self._chunk + FRAME_OVERHEAD)
        length = (
            self._total_bytes - start if last else self._chunk + FRAME_OVERHEAD
        )
        self._fh.seek(start)
        record = self._fh.read(length)
        try:
            piece = AESGCM(self._key).decrypt(
                record[:NONCE_BYTES],
                record[NONCE_BYTES:],
                _aad(self._header, index, last),
            )
        except InvalidTag:
            # THE tamper check, and it fires at the piece being touched rather than
            # at the end of the file. Guaranteed by the maths whenever a single byte
            # changed, or a piece was moved, or the file was cut short — not a
            # comparison this code could get wrong or skip.
            self._cached_index = -1
            self._cached = b""
            # ONE-BASED, deliberately. `index` counts from zero everywhere inside
            # this file, and printing it beside a total that counts from one would
            # say "piece 5 of 8" about the SIXTH piece of eight — a number the
            # platform prints into a log, a failure detail and a report, where
            # nobody can see which base it came from. The arithmetic stays
            # zero-based; only the sentence is human.
            fail(
                "piece %d of %d did not open — the sealed data was changed, moved or "
                "truncated after it was sealed (AES-GCM tag mismatch)"
                % (index + 1, self._frames)
            )
        self._cached_index = index
        self._cached = piece
        return piece

    # -- the file interface --
    def readable(self):
        return True

    def seekable(self):
        return True

    def size(self):
        """The plaintext length, known from the file's size alone — no piece is
        opened to answer it."""
        return self._size

    def tell(self):
        return self._pos

    def seek(self, offset, whence=io.SEEK_SET):
        if whence == io.SEEK_SET:
            target = offset
        elif whence == io.SEEK_CUR:
            target = self._pos + offset
        elif whence == io.SEEK_END:
            target = self._size + offset
        else:
            raise ValueError("invalid whence: %r" % (whence,))
        self._pos = max(0, min(target, self._size))
        return self._pos

    def readinto(self, buf):
        if self._pos >= self._size:
            return 0
        index = self._pos // self._chunk
        piece = self._piece(index)
        offset = self._pos - index * self._chunk
        n = min(len(buf), len(piece) - offset)
        buf[:n] = piece[offset : offset + n]
        self._pos += n
        return n

    def close(self):
        try:
            self._fh.close()
        finally:
            self._cached = b""
            super().close()


def open_input(path=None, buffer_size=io.DEFAULT_BUFFER_SIZE):
    """Open this run's dataset for reading. **This is the one line you change.**

    `path` defaults to `INPUT_PATH`, which the platform sets to the file it mounted
    read-only for this run.

    Sealed or not, you get the same object back. A file that is not sealed — an old
    job, or this image run by hand outside the platform — is opened directly, so one
    line in your loader works in both worlds and you never branch on it.

    Returns a buffered binary file. Wrap it in `io.TextIOWrapper` if you want text."""
    path = path or os.environ.get("INPUT_PATH")
    if not path:
        raise ValueError("no input path: pass one, or set INPUT_PATH")
    fh = open(path, "rb")
    head = fh.read(len(MAGIC))
    fh.seek(0)
    if not is_sealed(head):
        return fh  # already a buffered binary file
    k = key()
    if k is None:
        fh.close()
        fail("this input is sealed but this container was given no key ticket")
    return io.BufferedReader(SealedReader(fh, k), buffer_size)


# --- writing ----------------------------------------------------------------


class SealedWriter(io.RawIOBase):
    """A write-only file that seals what it is given, one piece at a time.

    Pieces are emitted as they fill, so the memory this holds is two pieces at most
    however much you write. The LAST piece is emitted on close, because a piece is
    sealed together with whether the file ends there and nothing can know that until
    the writing stops. Close it — or use it as a context manager, which does."""

    def __init__(self, fh, key_bytes, chunk_size=DEFAULT_CHUNK_BYTES):
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM

        self._fh = fh
        self._chunk = chunk_size
        self._buf = bytearray()
        self._index = 0
        stream_id = os.urandom(16)
        self._header = (
            MAGIC
            + bytes([VERSION])
            + b"\x00\x00\x00"
            + chunk_size.to_bytes(4, "big")
            + stream_id
        )
        self._aes = AESGCM(key_bytes)
        self._fh.write(self._header)
        self._closed_cleanly = False

    def writable(self):
        return True

    def _emit(self, piece, last):
        nonce = os.urandom(NONCE_BYTES)
        self._fh.write(nonce)
        self._fh.write(
            self._aes.encrypt(nonce, bytes(piece), _aad(self._header, self._index, last))
        )
        self._index += 1

    def write(self, data):
        self._buf += data
        # Strictly greater, never equal: a buffer holding exactly one piece might be
        # the end of the file, and a piece sealed as not-last that turns out to be
        # last would never open again.
        while len(self._buf) > self._chunk:
            self._emit(self._buf[: self._chunk], last=False)
            del self._buf[: self._chunk]
        return len(data)

    def close(self):
        if self.closed:
            return
        try:
            if not self._closed_cleanly:
                self._emit(self._buf, last=True)
                self._buf = bytearray()
                self._closed_cleanly = True
            self._fh.flush()
            os.fsync(self._fh.fileno())
        finally:
            self._fh.close()
            super().close()


def create_output(name, chunk_size=DEFAULT_CHUNK_BYTES, directory=None):
    """Create one of this run's result files, sealed as it is written.

    `name` is a plain filename; it lands in `directory`, or in `OUTPUT_DIR`, which the
    platform points at the folder it collects results from. Use it as a context manager so the file is
    closed — and therefore finished — before your program ends.

    On a job with no key (an unsealed job, or this image run by hand) it writes the
    file plainly, so the same line works in both worlds.

    **Why your results are sealed too.** They are your data as much as your input is,
    they sit in the same storage, and one deletion of your job's key has to make all
    of it unreadable. You still download them in the clear: the control plane holds
    the key and opens them for you at the download door."""
    directory = directory or os.environ.get("OUTPUT_DIR") or "."
    os.makedirs(directory, exist_ok=True)
    path = os.path.join(directory, name)
    # The key FIRST, and the file only once there is one (walk 1, row 32). Opening
    # the file before asking for the key left an empty, unsealed file behind when the
    # key could not be obtained — which the agent then uploaded, the control plane
    # refused as unsealed, and the run was labelled with a cause that was not the
    # cause. Nothing is created now until the run can seal it.
    k = key()
    fh = open(path, "wb")
    if k is None:
        return fh
    return io.BufferedWriter(SealedWriter(fh, k, chunk_size))


# --- bytes in, bytes out (what fyp_checkpoint uses) -------------------------


def seal_bytes(data, chunk_size=DEFAULT_CHUNK_BYTES):
    """Seal a whole blob in memory. Returns the sealed bytes, or the data unchanged
    when this container has no key."""
    k = key()
    if k is None:
        return data
    # Sealed into a buffer rather than through SealedWriter, which closes and fsyncs
    # a real file. Same format, same AAD, and the tests seal with one and open with
    # the other so the two cannot drift.
    return _seal_into(io.BytesIO(), k, data, chunk_size)


def _seal_into(buf, key_bytes, data, chunk_size):
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM

    stream_id = os.urandom(16)
    header = (
        MAGIC + bytes([VERSION]) + b"\x00\x00\x00"
        + chunk_size.to_bytes(4, "big") + stream_id
    )
    aes = AESGCM(key_bytes)
    pieces = [data[i : i + chunk_size] for i in range(0, len(data), chunk_size)] or [b""]
    buf.write(header)
    last = len(pieces) - 1
    for index, piece in enumerate(pieces):
        nonce = os.urandom(NONCE_BYTES)
        buf.write(nonce)
        buf.write(aes.encrypt(nonce, bytes(piece), _aad(header, index, index == last)))
    return buf.getvalue()


def open_one_piece(blob, key_bytes):
    """Open the ONE-PIECE format: nonce(12) || ciphertext+tag.

    Never written again, and read for ever. It is what sealed a private job's input
    before 2026-09-06, and those objects are still in the store. Kept here — rather
    than only in the control plane — so that both implementations of this format
    understand exactly the same set of files; an asymmetry between them is a bug
    waiting for the day the two sides meet."""
    from cryptography.exceptions import InvalidTag
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM

    # Raised, never PRINTED. `open_bytes` calls this speculatively — plain bytes and
    # one-piece bytes are told apart by trying — so printing the marker here would put
    # a false ##INTEGRITY_ERROR in the log of every run whose checkpoint was simply
    # not sealed, and the platform reads that marker as a diagnosis. Whoever decides
    # the failure is real is the one that prints.
    if len(blob) <= NONCE_BYTES:
        raise IntegrityError("sealed data is too short to be valid")
    try:
        return AESGCM(key_bytes).decrypt(blob[:NONCE_BYTES], blob[NONCE_BYTES:], None)
    except InvalidTag as exc:
        raise IntegrityError(
            "the sealed data was changed after it was sealed (AES-GCM tag mismatch)"
        ) from exc


def open_bytes(blob):
    """Open a whole sealed blob in memory, in EITHER format, sniffed from its first
    bytes. Bytes that are not sealed at all come back unchanged, which is what lets a
    reader accept a file written before any of this existed."""
    k = key()
    if is_sealed(blob[: len(MAGIC)]):
        if k is None:
            fail("these bytes are sealed but this container was given no key ticket")
        with SealedReader(io.BytesIO(blob), k) as r:
            return r.read()
    if k is None:
        return blob
    # Not the framed format. It may be the one-piece format, or it may be plain bytes
    # written before anything was sealed -- and the only way to tell is to try. A
    # failure here means "not sealed with this key", which for a checkpoint reader is
    # the same answer as "not sealed", so the bytes go back unchanged and the caller
    # decides what they are.
    try:
        return open_one_piece(blob, k)
    except IntegrityError:
        return blob
