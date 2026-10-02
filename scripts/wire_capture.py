"""Record what a passive observer on the network would actually see.

WHY THIS EXISTS
---------------
Report section 5.5, limitation 8 said that the agent's token, a private job's
ciphertext and the AES key that opens it all crossed the network in the clear, so
anyone on the same local network could read them. That was a claim established by
reading the code. This turns it into something anyone can watch happen, and then
watch stop happening.

It is a transparent relay: it listens on one port, forwards every byte to the real
control plane, and writes both directions to a file as it goes. It does not
terminate TLS and holds no certificate -- it simply sits in the middle, which is
exactly the position the limitation describes. Point a client at the relay instead
of the control plane and the recording is what an observer would have captured.

  BEFORE, plain HTTP : the login request appears in the recording as readable text,
                       password included.
  AFTER,  HTTPS      : the same exchange is TLS ciphertext. The relay still sees
                       every byte and can read none of them.

Nothing here is specific to this project, and nothing here breaks TLS: a relay that
only copies bytes cannot read an encrypted stream, which is the entire point being
demonstrated. Certificate verification still succeeds through it because the client
still checks the certificate the control plane presents.

Usage:
  python scripts/wire_capture.py --listen 8443 --target localhost:8000 \\
      --out docs/evidence/.../capture.bin --look-for fyp-admin

Then send traffic to localhost:8443 and stop it with Ctrl-C, or use --requests N to
stop on its own after N connections.
"""

from __future__ import annotations

import argparse
import socket
import sys
import threading
from pathlib import Path

_lock = threading.Lock()


def _pump(src: socket.socket, dst: socket.socket, sink, label: str) -> None:
    """Copy one direction, recording every byte that passes."""
    try:
        while True:
            chunk = src.recv(65536)
            if not chunk:
                break
            with _lock:
                sink.write(chunk)
                sink.flush()
            dst.sendall(chunk)
    except OSError:
        pass
    finally:
        try:
            dst.shutdown(socket.SHUT_WR)
        except OSError:
            pass


def _handle(client: socket.socket, target: tuple[str, int], sink) -> None:
    try:
        upstream = socket.create_connection(target, timeout=30)
    except OSError as exc:
        print(f"  cannot reach {target[0]}:{target[1]} ({exc})", file=sys.stderr)
        client.close()
        return
    a = threading.Thread(target=_pump, args=(client, upstream, sink, "->"), daemon=True)
    b = threading.Thread(target=_pump, args=(upstream, client, sink, "<-"), daemon=True)
    a.start()
    b.start()
    a.join()
    b.join()
    client.close()
    upstream.close()


def main() -> int:
    ap = argparse.ArgumentParser(description="Record what a network observer sees.")
    ap.add_argument("--listen", type=int, required=True, help="port to listen on")
    ap.add_argument("--target", required=True, help="host:port to forward to")
    ap.add_argument("--out", required=True, help="file to write the recording to")
    ap.add_argument(
        "--look-for",
        action="append",
        default=[],
        help="string to search for in the recording afterwards (repeatable)",
    )
    ap.add_argument(
        "--requests", type=int, default=0, help="stop after N connections (0 = until Ctrl-C)"
    )
    args = ap.parse_args()

    host, _, port = args.target.partition(":")
    target = (host, int(port))
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)

    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", args.listen))
    srv.listen(8)
    print(f"observing 127.0.0.1:{args.listen} -> {args.target}, recording to {out}")

    seen = 0
    with open(out, "wb") as sink:
        try:
            while True:
                client, _addr = srv.accept()
                seen += 1
                t = threading.Thread(target=_handle, args=(client, target, sink), daemon=True)
                t.start()
                t.join(timeout=60)
                if args.requests and seen >= args.requests:
                    break
        except KeyboardInterrupt:
            print("\nstopped.")
        finally:
            srv.close()

    data = out.read_bytes()
    print(f"\nrecorded {len(data)} bytes over {seen} connection(s)")

    # The verdict, stated plainly either way.
    if args.look_for:
        print("\nwhat the observer could read:")
        for needle in args.look_for:
            hit = needle.encode() in data
            verdict = "READABLE IN CLEAR" if hit else "not present in the clear"
            print(f"  {needle!r}: {verdict}")

    printable = sum(1 for b in data if 32 <= b < 127 or b in (9, 10, 13))
    ratio = (printable / len(data)) if data else 0.0
    print(f"\nprintable bytes: {ratio:.1%} of the recording")
    print(
        "  a plain-HTTP exchange reads as mostly text; a TLS one reads as "
        "mostly binary, because it is ciphertext."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
