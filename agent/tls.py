"""How the agent trusts the control plane over TLS.

THE PROBLEM THIS SOLVES
-----------------------
Until 2026-08-22 every agent-to-control-plane call travelled as plain HTTP, so
anyone on the same network could read a node token, a sealed job's ciphertext and
the AES key that opens it (report section 5.5, limitation 8). Moving those calls
to HTTPS removes the wire as a path to any of it.

WHY VERIFICATION IS ON, WITH NO SWITCH TO TURN IT OFF
-----------------------------------------------------
An encrypted connection to a machine you have not identified protects you from a
passive listener and not from anyone able to answer in the control plane's place.
Since the second is the attacker the first invites, a TLS client that skips
verification buys the appearance of security rather than security. There is
deliberately no "insecure" option here: the only way to run without verification
is to run without TLS, which is the state we are leaving and which the report
names honestly.

WHAT WE TRUST
-------------
The demo network is self-hosted with no public name, so no public authority can
vouch for it and we act as our own (scripts/make_certs.py). The agent is pointed
at that authority's certificate and trusts THAT ALONE -- not the machine's system
trust store. A narrower trust root is the right default here: it means a
certificate issued by any other authority, including a public one, is refused.
"""

from __future__ import annotations

import ssl
from pathlib import Path

# The env var that names the authority's certificate. Documented in protocol.md
# section 8 beside the other tunables.
CA_CERT_ENV = "AGENT_CA_CERT"


class CaCertError(RuntimeError):
    """The configured authority certificate is missing or unreadable.

    Raised rather than warned about: an agent told to verify against an authority
    it cannot read must not quietly fall back to trusting everything, which is the
    exact failure the no-insecure-switch rule above exists to prevent."""


def read_ca_pem(ca_cert_path: str | None) -> str | None:
    """Read the authority certificate as text, or return None when none is set.

    The text, not the path, is what travels: the same PEM is handed to a private
    job's container through the environment, where a file path on the worker's
    disk would mean nothing (see agent/runner.py)."""
    if not ca_cert_path:
        return None
    path = Path(ca_cert_path)
    try:
        pem = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise CaCertError(
            f"cannot read the CA certificate at {path} ({exc}). "
            f"Generate one with scripts/make_certs.py, or unset {CA_CERT_ENV} to "
            f"run without TLS."
        ) from exc
    if "BEGIN CERTIFICATE" not in pem:
        raise CaCertError(f"{path} does not look like a PEM certificate.")
    return pem


def context_for(ca_pem: str | None) -> ssl.SSLContext | None:
    """A TLS context trusting exactly the given authority, or None for plain HTTP.

    None is returned rather than a permissive context so that callers pass
    `context=None` to urlopen and get stdlib's ordinary behaviour -- there is one
    code path, and TLS is configured or it is absent."""
    if not ca_pem:
        return None
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    # Both are already the default for PROTOCOL_TLS_CLIENT. Set explicitly so that
    # a future edit which loosens either has to do it in plain sight.
    ctx.verify_mode = ssl.CERT_REQUIRED
    ctx.check_hostname = True
    # cadata takes the certificate as text, so nothing is written to disk.
    ctx.load_verify_locations(cadata=ca_pem)
    return ctx
