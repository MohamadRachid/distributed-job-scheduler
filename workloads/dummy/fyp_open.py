"""fyp_open.py — the one file a workload adds to accept SEALED input (W6b).

**This is the adoption contract.** Copy this file into your image and wrap your
command with it:

    ENTRYPOINT ["python", "fyp_open.py", "python", "train.py"]

Everything else stays yours. Same philosophy as the ``##PROGRESS`` line (W5b): one
tiny, readable contract a real training script can adopt in a minute — not a
framework it has to be rewritten for.

**What it does, in order**

  1. Reads the one-shot **ticket** from ``FYP_TICKET`` and trades it, over the
     network, for the job's key (``FYP_KEY_URL``). The ticket dies on use, so from
     this moment the environment holds nothing that opens anything — which is why
     ``docker inspect`` on a running private container shows no usable secret.
     Since 2026-08-22 that exchange runs over **HTTPS**, and the control plane is
     **verified** against the authority handed in as ``FYP_CA_PEM`` — the
     certificate as text, so nothing is written to disk and the container still
     mounts exactly one thing. Without verification an encrypted connection would
     protect the key from a listener but not from whoever answered; we do not
     accept an unverified connection, and there is no switch to make us.
  2. Reads the **sealed** file from ``INPUT_CIPHER_PATH`` (mounted read-only) and
     opens it **in memory** with AES-GCM.
  3. Writes the plaintext ONLY to ``INPUT_PLAIN_PATH``, which lives on a tmpfs — a
     folder that is really RAM. It never touches the worker's disk, and it is gone
     the moment the container stops. There is no cleanup step to forget.
  4. Runs your command.

**If the seal does not verify** — one changed byte anywhere in the file, or the
wrong key — AES-GCM refuses. We do not fall back, retry, or continue with partial
data: we print ``##INTEGRITY_ERROR`` and exit non-zero, so the platform records the
run as ``INTEGRITY_ERROR``. Training on data we cannot vouch for would be worse than
failing.

**What this does NOT protect against (say it plainly).** While your program runs,
the data is plain in that container's memory, because a computer cannot compute on
data it cannot read. Someone with **root** on this machine can read that memory.
This design keeps the data from the machine's *users*, its disk, its backups, and
anyone who intercepts it — not from its administrator. Root-proof execution needs
TEE hardware, which is named future work.
"""

import json
import os
import ssl
import sys
import urllib.request


def _fail(message):
    """Report a broken seal in the one shape the platform classifies (the marker
    line is the contract — see agent/classify.py) and stop."""
    print(f"##INTEGRITY_ERROR: {message}", file=sys.stderr, flush=True)
    sys.exit(1)


def _tls_context(key_url, ca_pem):
    """Verify the control plane, or refuse to speak to it.

    Returns None for a plain-http key URL (an unencrypted deployment, which is
    what this was before 2026-08-22 and still is if TLS is not configured).

    For https it returns a context that trusts EXACTLY the given authority and
    nothing else -- not the image's system trust store, so a certificate from any
    other authority is refused even if that authority is a public one.

    `cadata` takes the certificate as text, so the PEM never becomes a file: no
    write to the tmpfs, no mount, nothing on disk to clean up afterwards."""
    if not key_url.lower().startswith("https://"):
        return None
    if not ca_pem:
        # An https URL we cannot verify. Failing here is the point: continuing
        # would hand the job's key to whoever answered.
        _fail("the key endpoint is https but no FYP_CA_PEM was provided to verify it")
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.verify_mode = ssl.CERT_REQUIRED
    ctx.check_hostname = True
    ctx.load_verify_locations(cadata=ca_pem)
    return ctx


def _redeem_ticket(key_url, ticket, ca_pem=None):
    """Trade the single-use ticket for the job's key. The control plane stamps the
    ticket as used inside the same transaction that answers, so this call can only
    ever succeed once — a replay gets 410 Gone."""
    body = json.dumps({"ticket": ticket}).encode("utf-8")
    req = urllib.request.Request(key_url, data=body, method="POST")
    req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=30, context=_tls_context(key_url, ca_pem)) as resp:
        return json.loads(resp.read().decode("utf-8"))["key_b64"]


def main(argv):
    if not argv:
        print("usage: fyp_open.py <command> [args...]", file=sys.stderr)
        return 2

    cipher_path = os.environ.get("INPUT_CIPHER_PATH")
    plain_path = os.environ.get("INPUT_PLAIN_PATH")
    ticket = os.environ.get("FYP_TICKET")
    key_url = os.environ.get("FYP_KEY_URL")
    # The authority to verify the control plane against, as PEM text. Absent on a
    # plain-http deployment; required, and checked, when the key URL is https.
    ca_pem = os.environ.get("FYP_CA_PEM")

    # Not a private run (no sealed input configured) -> just run the command. The
    # same image then works for both private and ordinary jobs.
    if not (cipher_path and plain_path and ticket and key_url):
        os.execvp(argv[0], argv)

    import base64

    from cryptography.exceptions import InvalidTag
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM

    try:
        key = base64.b64decode(_redeem_ticket(key_url, ticket, ca_pem))
    except Exception as exc:  # noqa: BLE001 - any failure to get the key stops the run
        _fail(f"could not obtain the key ({exc})")

    with open(cipher_path, "rb") as f:
        blob = f.read()
    # Pinned blob format (protocol.md §2): nonce(12) || ciphertext+tag.
    if len(blob) <= 12:
        _fail("sealed input is too short to be valid")
    nonce, body = blob[:12], blob[12:]

    try:
        plaintext = AESGCM(key).decrypt(nonce, body, None)
    except InvalidTag:
        # THE tamper check. GCM's tag is computed over the ciphertext, so this raise
        # is guaranteed by the maths whenever a single byte changed — it is not a
        # comparison we could get wrong or skip.
        _fail("the sealed data was changed after it was sealed (AES-GCM tag mismatch)")

    # The ONLY place plaintext is written: a RAM-backed folder (tmpfs).
    os.makedirs(os.path.dirname(plain_path) or "/", exist_ok=True)
    with open(plain_path, "wb") as f:
        f.write(plaintext)
    print(
        f"integrity OK — opened {len(plaintext)} bytes into {plain_path} (in RAM, tmpfs)",
        flush=True,
    )

    # Hand off to the real workload. execvp REPLACES this process, so the training
    # script keeps the container's PID 1 and its signal handling is unaffected.
    os.execvp(argv[0], argv)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
