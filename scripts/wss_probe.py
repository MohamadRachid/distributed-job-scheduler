"""Read a run's live log over a SECURE web socket (wss), with stdlib only.

The browser's live-log view is the one place a token travels in a URL rather than
a header, because browsers cannot set an Authorization header on a socket. Over
plain ws that URL -- token included -- was readable by anyone on the network, which
is the second half of what report section 5.5 limitation 8 described.

This connects the way the dashboard does, over wss, verifying the control plane
against the demo authority, and reads frames until the server closes. It uses no
web-socket library on purpose: the agent is stdlib-only, and a proof tool that
needs a dependency the project does not otherwise have is a proof that is harder
to re-run than the thing it proves.

Usage:
  python scripts/wss_probe.py <run_id> [--base https://localhost:8000] [--ca certs/ca.pem]
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import secrets
import socket
import ssl
import struct
import sys
import urllib.parse
import urllib.request


def _login(base: str, ctx: ssl.SSLContext) -> str:
    body = json.dumps({
        "username": os.environ.get("ADMIN_USERNAME", "admin"),
        "password": os.environ.get("ADMIN_PASSWORD", "fyp-admin"),
    }).encode()
    req = urllib.request.Request(f"{base}/auth/login", data=body, method="POST")
    req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=20, context=ctx) as resp:
        return json.loads(resp.read())["token"]


def _read_exactly(sock: socket.socket, n: int) -> bytes:
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("socket closed mid-frame")
        buf += chunk
    return buf


def _read_frame(sock: socket.socket):
    """One server->client frame. Returns (opcode, payload); server frames are
    never masked, which is what the protocol requires of a server."""
    first, second = _read_exactly(sock, 2)
    opcode = first & 0x0F
    masked = bool(second & 0x80)
    length = second & 0x7F
    if length == 126:
        length = struct.unpack(">H", _read_exactly(sock, 2))[0]
    elif length == 127:
        length = struct.unpack(">Q", _read_exactly(sock, 8))[0]
    mask = _read_exactly(sock, 4) if masked else b""
    payload = _read_exactly(sock, length) if length else b""
    if masked:
        payload = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
    return opcode, payload


def main() -> int:
    ap = argparse.ArgumentParser(description="Read a run's live log over wss.")
    ap.add_argument("run_id")
    ap.add_argument("--base", default=os.environ.get("FYP_HTTPS_BASE", "https://localhost:8000"))
    ap.add_argument("--ca", default=os.environ.get("FYP_CA_FILE", "certs/ca.pem"))
    ap.add_argument("--max-frames", type=int, default=200)
    args = ap.parse_args()

    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.verify_mode = ssl.CERT_REQUIRED
    ctx.check_hostname = True
    ctx.load_verify_locations(cafile=args.ca)

    parsed = urllib.parse.urlparse(args.base)
    host = parsed.hostname or "localhost"
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    secure = parsed.scheme == "https"

    token = _login(args.base, ctx) if secure else _login(args.base, ctx)
    path = f"/runs/{args.run_id}/logs?token={urllib.parse.quote(token)}"

    raw = socket.create_connection((host, port), timeout=30)
    sock = ctx.wrap_socket(raw, server_hostname=host) if secure else raw
    scheme = "wss" if secure else "ws"
    print(f"connecting {scheme}://{host}:{port}/runs/{args.run_id}/logs (token in the URL)")
    if secure:
        print(f"  TLS: {sock.version()}, cipher {sock.cipher()[0]}")
        print(f"  certificate verified against {args.ca}")

    key = base64.b64encode(secrets.token_bytes(16)).decode()
    handshake = (
        f"GET {path} HTTP/1.1\r\n"
        f"Host: {host}:{port}\r\n"
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        f"Sec-WebSocket-Key: {key}\r\n"
        "Sec-WebSocket-Version: 13\r\n"
        "\r\n"
    )
    sock.sendall(handshake.encode())

    header = b""
    while b"\r\n\r\n" not in header:
        header += sock.recv(4096)
    status = header.split(b"\r\n", 1)[0].decode()
    print(f"  handshake: {status}")
    if "101" not in status:
        print("  the socket did not upgrade.")
        return 1

    chunks, frames = 0, 0
    try:
        while frames < args.max_frames:
            opcode, payload = _read_frame(sock)
            frames += 1
            if opcode == 0x8:  # close
                print("  server closed the socket (run reached a terminal state)")
                break
            if opcode not in (0x1, 0x2):
                continue
            try:
                msg = json.loads(payload.decode("utf-8", "replace"))
            except json.JSONDecodeError:
                continue
            if msg.get("end"):
                print(f"  end marker: run_status={msg.get('run_status')}")
                break
            chunks += 1
    except (ConnectionError, OSError) as exc:
        print(f"  socket ended: {exc}")
    finally:
        sock.close()

    print(f"\nRESULT: {chunks} log chunk(s) received over {scheme.upper()}")
    print("  the token travelled in the URL, and over wss that URL is inside the")
    print("  encrypted stream -- which is what limitation 8 asked for.")
    return 0 if chunks or frames else 1


if __name__ == "__main__":
    raise SystemExit(main())
