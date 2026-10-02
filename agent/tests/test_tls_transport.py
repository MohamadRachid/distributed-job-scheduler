"""Encrypted transport (2026-08-22) — the agent and the container verify, or refuse.

These are not mocked. Each test starts a REAL HTTPS server on a real socket with a
real certificate signed by a throwaway authority, and drives the REAL code paths —
`agent._urlopen`, which every outbound agent call now goes through, and
`fyp_open._redeem_ticket`, which is how a private job's container trades its
one-shot ticket for the key. A mocked TLS test proves that a mock was configured.

BOTH DIRECTIONS, ALWAYS
-----------------------
Every check here has a matching refusal: a connection that should succeed, and the
same connection against the wrong authority that must fail. A verification test
that only ever shows success cannot tell the difference between verifying and not
verifying — which is exactly the state this change exists to leave behind, and a
standing rule of this project.

Run from the repo root:  pytest agent/tests -q
"""

import http.server
import json
import os
import ssl
import sys
import threading
import urllib.error
import urllib.request

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "scripts"))
from make_certs import make_ca_and_server  # noqa: E402

from agent import agent as agent_mod  # noqa: E402
from agent.tls import CaCertError, context_for, read_ca_pem  # noqa: E402

KEY_B64 = "c2VjcmV0LWtleS1ieXRlcw=="


# --- a real TLS server, on a real socket ------------------------------------


class _Handler(http.server.BaseHTTPRequestHandler):
    """Answers the two shapes these tests need and nothing else."""

    def do_GET(self):  # noqa: N802 - stdlib's naming
        self._json({"ok": True, "path": self.path})

    def do_POST(self):  # noqa: N802
        length = int(self.headers.get("Content-Length") or 0)
        self.rfile.read(length)
        # The one endpoint a container calls: hand back a key, as the real one does.
        self._json({"key_b64": KEY_B64})

    def _json(self, payload):
        body = json.dumps(payload).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_args):  # keep pytest output readable
        pass


class _TlsServer:
    """A throwaway HTTPS server with its own authority, on a free port."""

    def __init__(self, tmp_path, name="a"):
        blobs = make_ca_and_server()
        self.ca_pem = blobs["ca.pem"].decode()
        cert = tmp_path / f"server-{name}.pem"
        key = tmp_path / f"server-{name}.key"
        cert.write_bytes(blobs["server.pem"])
        key.write_bytes(blobs["server.key"])
        self.ca_file = tmp_path / f"ca-{name}.pem"
        self.ca_file.write_bytes(blobs["ca.pem"])

        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.load_cert_chain(certfile=str(cert), keyfile=str(key))
        self._httpd = http.server.HTTPServer(("127.0.0.1", 0), _Handler)
        self._httpd.socket = ctx.wrap_socket(self._httpd.socket, server_side=True)
        self.port = self._httpd.server_address[1]
        # The certificate carries `localhost` as a name; 127.0.0.1 is in it too,
        # but connecting by name is what a real deployment does.
        self.base = f"https://localhost:{self.port}"
        self._thread = threading.Thread(target=self._httpd.serve_forever, daemon=True)
        self._thread.start()

    def close(self):
        self._httpd.shutdown()
        self._httpd.server_close()


@pytest.fixture
def tls_server(tmp_path):
    srv = _TlsServer(tmp_path, "a")
    yield srv
    srv.close()


@pytest.fixture
def other_ca(tmp_path):
    """A DIFFERENT authority — never the one that signed tls_server."""
    blobs = make_ca_and_server()
    path = tmp_path / "other-ca.pem"
    path.write_bytes(blobs["ca.pem"])
    return blobs["ca.pem"].decode(), path


@pytest.fixture(autouse=True)
def _restore_agent_tls():
    """Leave the agent module as it was found: its TLS context is global."""
    yield
    agent_mod.configure_tls(None)


# --- the agent's own connections --------------------------------------------


def test_agent_connects_over_tls_when_it_trusts_the_authority(tls_server):
    """The success half. Every agent call goes through this one door."""
    agent_mod.configure_tls(tls_server.ca_pem)
    req = urllib.request.Request(f"{tls_server.base}/health", method="GET")
    with agent_mod._urlopen(req, timeout=10) as resp:
        assert json.loads(resp.read())["ok"] is True


def test_agent_refuses_a_server_signed_by_a_different_authority(tls_server, other_ca):
    """The refusal half, and the one that gives the success half its meaning.

    Same server, same code, one thing changed: the authority the agent trusts. If
    this passed, the test above would be proving only that TLS was switched on —
    not that anyone checked who answered."""
    other_pem, _ = other_ca
    agent_mod.configure_tls(other_pem)
    req = urllib.request.Request(f"{tls_server.base}/health", method="GET")
    with pytest.raises(urllib.error.URLError) as excinfo:
        agent_mod._urlopen(req, timeout=10)
    assert isinstance(excinfo.value.reason, ssl.SSLCertVerificationError)


def test_agent_without_tls_configured_will_not_reach_an_https_server(tls_server):
    """No authority configured means no verified connection — not a quiet fallback
    to trusting whatever certificate turns up."""
    agent_mod.configure_tls(None)
    req = urllib.request.Request(f"{tls_server.base}/health", method="GET")
    with pytest.raises(urllib.error.URLError) as excinfo:
        agent_mod._urlopen(req, timeout=10)
    assert isinstance(excinfo.value.reason, ssl.SSLCertVerificationError)


def test_the_authority_is_carried_as_text_for_a_container(tls_server):
    """A private run hands the certificate to its container through the
    environment, so the agent keeps the PEM text and not just a path."""
    agent_mod.configure_tls(tls_server.ca_pem)
    assert agent_mod.ca_pem() == tls_server.ca_pem
    assert "BEGIN CERTIFICATE" in agent_mod.ca_pem()


# --- reading the configured authority ---------------------------------------


def test_a_missing_authority_file_stops_the_agent(tmp_path):
    """Refused, not warned about: an agent that cannot read its authority must not
    carry on trusting everything."""
    with pytest.raises(CaCertError):
        read_ca_pem(str(tmp_path / "nope.pem"))


def test_a_file_that_is_not_a_certificate_stops_the_agent(tmp_path):
    junk = tmp_path / "junk.pem"
    junk.write_text("this is not a certificate")
    with pytest.raises(CaCertError):
        read_ca_pem(str(junk))


def test_no_authority_configured_means_no_context():
    """Plain HTTP stays exactly as it was: one code path, not a third state where
    TLS is on but unverified."""
    assert context_for(None) is None
    assert read_ca_pem(None) is None


def test_the_context_verifies_and_checks_the_hostname(tls_server):
    ctx = context_for(tls_server.ca_pem)
    assert ctx.verify_mode is ssl.CERT_REQUIRED
    assert ctx.check_hostname is True
    # Exactly our authority — not the machine's system trust store.
    assert len(ctx.get_ca_certs()) == 1


# --- the container's key redemption (the most sensitive call we have) --------


def _fyp_open():
    """Import the adoption-contract file the way a workload image would."""
    import importlib.util

    path = os.path.join(
        os.path.dirname(__file__), "..", "..", "workloads", "dummy", "fyp_open.py"
    )
    spec = importlib.util.spec_from_file_location("fyp_open_undertest", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_container_redeems_its_ticket_over_a_verified_connection(tls_server):
    """The key — the one secret that opens a private job's data — now crosses the
    network encrypted, to a server whose identity was checked."""
    fyp_open = _fyp_open()
    key = fyp_open._redeem_ticket(
        f"{tls_server.base}/container/key", "one-shot-ticket", tls_server.ca_pem
    )
    assert key == KEY_B64


def test_container_refuses_to_redeem_against_the_wrong_authority(tls_server, other_ca):
    """The refusal that matters most. An attacker who answers in the control
    plane's place is handed nothing: the ticket is never sent."""
    other_pem, _ = other_ca
    fyp_open = _fyp_open()
    with pytest.raises(urllib.error.URLError) as excinfo:
        fyp_open._redeem_ticket(
            f"{tls_server.base}/container/key", "one-shot-ticket", other_pem
        )
    assert isinstance(excinfo.value.reason, ssl.SSLCertVerificationError)


def test_container_refuses_an_https_key_url_with_no_authority(tls_server, capsys):
    """An https endpoint we cannot verify is refused outright, and it is reported in
    the one shape the platform classifies, so the run ends as INTEGRITY_ERROR rather
    than hanging or continuing without its input."""
    fyp_open = _fyp_open()
    with pytest.raises(SystemExit) as excinfo:
        fyp_open._redeem_ticket(f"{tls_server.base}/container/key", "t", None)
    assert excinfo.value.code == 1
    assert "##INTEGRITY_ERROR" in capsys.readouterr().err


def test_a_plain_http_key_url_still_works_unchanged(tls_server):
    """A deployment that has not turned TLS on behaves exactly as it did before:
    no context, no authority, no new requirement."""
    fyp_open = _fyp_open()
    assert fyp_open._tls_context("http://cp:8000/container/key", None) is None


def test_the_container_helper_can_never_skip_verification():
    """The contract file is copied into user images, so the rule has to be legible
    in the file itself: there is no path here that turns checking off."""
    path = os.path.join(
        os.path.dirname(__file__), "..", "..", "workloads", "dummy", "fyp_open.py"
    )
    with open(path, encoding="utf-8") as fh:
        source = fh.read()
    assert "CERT_NONE" not in source
    assert "check_hostname = False" not in source
    assert "_create_unverified_context" not in source


# --- the certificates themselves, against a stricter client -----------------


def test_our_certificates_satisfy_a_strict_verifier(tls_server):
    """Our own certificates must carry the key identifiers a chain is supposed to have.

    Every other test in this file connects the way the agent does, with a context built
    from `SSLContext(PROTOCOL_TLS_CLIENT)`, which does NOT check them. So a gap here was
    invisible to all of them -- and there was one: until 25 August 2026 `make_certs.py`
    emitted no Subject Key Identifier and no Authority Key Identifier, and any client
    verifying strictly refused our authority outright with `Missing Authority Key
    Identifier`. Found when a helper script happened to use Python's own
    `create_default_context()`, which turns strict verification on from 3.13.

    Strict is set EXPLICITLY here rather than relied on: CI runs 3.12, where it is off by
    default, so a test that merely called `create_default_context()` would pass there
    while proving nothing.
    """
    ctx = ssl.create_default_context(cadata=tls_server.ca_pem)
    ctx.verify_flags |= ssl.VERIFY_X509_STRICT
    assert ctx.verify_flags & ssl.VERIFY_X509_STRICT

    with urllib.request.urlopen(tls_server.base + "/health", context=ctx, timeout=5) as r:
        assert r.status == 200


# --- the scheme-mismatch sentence (ruling of 2026-08-30) ---------------------


def test_a_scheme_mismatch_is_named_in_one_plain_sentence(tls_server, tmp_path):
    """The agent's `--server` default is the scheme the control plane serves, and
    pointing it at the wrong one says so instead of retrying for ever.

    Both directions, as this file requires of every check in it. Neither exception
    below is constructed: each is whatever a real socket really raises, taken from
    a real HTTPS server and a real plain-HTTP server started on real ports. That
    matters because the whole helper is a claim about which exception a wrong
    scheme produces, and a hand-built exception would prove only that we can build
    one.

    The refusal half is the third assertion: an ordinary unreachable port -- a
    control plane that is simply not up yet -- must NOT be named a scheme
    mismatch, because the demonstration's staging script depends on the agent
    waiting for a server that is still starting.
    """
    # The default is the scheme the control plane serves. Read off the real
    # parser, with SERVER_URL cleared so the fallback is what is under test.
    saved = os.environ.pop("SERVER_URL", None)
    try:
        default_server = agent_mod.build_parser().get_default("server")
    finally:
        if saved is not None:
            os.environ["SERVER_URL"] = saved
    assert default_server.startswith("https://"), default_server

    # Direction 1: http:// against a server that speaks HTTPS.
    plain = "http://localhost:%d" % tls_server.port
    try:
        urllib.request.urlopen(plain + "/health", timeout=5)
        raise AssertionError("plain http against an HTTPS server should not succeed")
    except Exception as exc:                                    # noqa: BLE001
        hint = agent_mod.scheme_mismatch(plain, exc)
    assert hint is not None, "an HTTPS server reached over http:// was not named"
    assert "speaking HTTPS" in hint and "https://localhost:%d" % tls_server.port in hint

    # Direction 2: https:// against a server that speaks plain HTTP.
    httpd = http.server.HTTPServer(("127.0.0.1", 0), _Handler)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    try:
        secure = "https://localhost:%d" % httpd.server_address[1]
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        try:
            urllib.request.urlopen(secure + "/health", timeout=5, context=ctx)
            raise AssertionError("https against a plain HTTP server should not succeed")
        except Exception as exc:                                # noqa: BLE001
            hint = agent_mod.scheme_mismatch(secure, exc)
        assert hint is not None, "a plain-HTTP server reached over https:// was not named"
        assert "speaking plain HTTP" in hint
    finally:
        httpd.shutdown()
        httpd.server_close()

    # The refusal: a port with nothing on it is not a scheme mismatch.
    dead = "http://localhost:9"
    try:
        urllib.request.urlopen(dead, timeout=3)
        raise AssertionError("port 9 should not answer")
    except Exception as exc:                                    # noqa: BLE001
        assert agent_mod.scheme_mismatch(dead, exc) is None, (
            "an unreachable control plane must be waited for, not renamed"
        )
