"""Generate the demo LAN's own certificate authority and a server certificate.

WHY A LOCAL AUTHORITY AND NOT A PUBLIC ONE
------------------------------------------
This platform is self-hosted by design (NFR-5, NFR-6): it runs on lab machines on
a private network with no public name, so no public authority could issue a
certificate for it even if we wanted one. We therefore act as our own authority
for the demo network: one CA certificate that machines are told to trust, and one
server certificate signed by it.

WHY THE NAMES MATTER MORE THAN ANYTHING ELSE HERE
-------------------------------------------------
A certificate is only accepted for the names written into it, and this control
plane is reached under three DIFFERENT names by three different callers:

  * the browser, and an agent on the same machine, reach it at localhost / 127.0.0.1
  * an agent on another laptop reaches it at this machine's LAN address
  * a CONTAINER reaches it at host.docker.internal, because a container's own
    localhost is the container, not the host

All three go into the certificate's Subject Alternative Names. Miss one and that
caller alone fails verification -- the kind of fault that shows up in front of a
jury rather than in a test.

Written with `cryptography` (already a dependency, for AES-GCM sealing) rather
than by shelling out to `openssl`, so this behaves identically on Windows and
Linux and needs no tool the project does not already install.
"""

from __future__ import annotations

import argparse
import datetime as _dt
import ipaddress
import socket
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.x509.oid import NameOID
from cryptography.hazmat.primitives.asymmetric import rsa

# Ten years. This authority exists for a demo network and a defence, and a
# certificate that expires mid-project is a self-inflicted outage.
_VALID_DAYS = 3650


def _now() -> _dt.datetime:
    return _dt.datetime.now(_dt.timezone.utc)


def _key() -> rsa.RSAPrivateKey:
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


def local_ipv4_addresses() -> list[str]:
    """Best effort: this machine's own LAN addresses, so an agent on another
    laptop can verify the certificate. Loopback is added separately and always."""
    found: set[str] = set()
    try:
        for info in socket.getaddrinfo(socket.gethostname(), None, socket.AF_INET):
            addr = info[4][0]
            if not addr.startswith("127."):
                found.add(addr)
    except OSError:
        pass
    return sorted(found)


def make_ca_and_server(
    hostnames: list[str] | None = None,
    ip_addresses: list[str] | None = None,
) -> dict[str, bytes]:
    """Return a fresh CA and a server certificate signed by it, as PEM bytes.

    Returns the four PEM blobs rather than writing files, so a test can build a
    throwaway authority in memory while the command line below writes the same
    bytes to disk. ONE implementation, two callers -- deliberately: two copies of
    a security primitive quietly disagreeing is the defect this project already
    caught once, when a checkpoint filename was defined in two modules
    (2026-08-13)."""
    names = list(hostnames or []) + ["localhost", "host.docker.internal"]
    ips = list(ip_addresses or []) + ["127.0.0.1"]

    # --- the authority ------------------------------------------------------
    ca_key = _key()
    ca_name = x509.Name([
        x509.NameAttribute(NameOID.ORGANIZATION_NAME, "FYP Distributed Training Platform"),
        x509.NameAttribute(NameOID.COMMON_NAME, "FYP Demo LAN CA"),
    ])
    ca_cert = (
        x509.CertificateBuilder()
        .subject_name(ca_name)
        .issuer_name(ca_name)
        .public_key(ca_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(_now() - _dt.timedelta(minutes=5))
        .not_valid_after(_now() + _dt.timedelta(days=_VALID_DAYS))
        # CA:TRUE -- this authority signs server certificates, and nothing signs
        # on its behalf.
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=True, key_cert_sign=True, crl_sign=True,
                content_commitment=False, key_encipherment=False,
                data_encipherment=False, key_agreement=False,
                encipher_only=False, decipher_only=False,
            ),
            critical=True,
        )
        # The two key identifiers. A chain without them verifies fine against an
        # ordinary client and is REFUSED by a strict one -- Python turns strict
        # verification on by default from 3.13, and ours carried neither until
        # 2026-08-25. The agent's own context is not strict, so nothing we ran
        # would ever have told us. A self-signed root points its authority
        # identifier at itself, which is what makes the chain self-describing.
        .add_extension(
            x509.SubjectKeyIdentifier.from_public_key(ca_key.public_key()),
            critical=False,
        )
        .add_extension(
            x509.AuthorityKeyIdentifier.from_issuer_public_key(ca_key.public_key()),
            critical=False,
        )
        .sign(ca_key, hashes.SHA256())
    )

    # --- the server certificate ---------------------------------------------
    srv_key = _key()
    san: list[x509.GeneralName] = [x509.DNSName(h) for h in dict.fromkeys(names)]
    for raw in dict.fromkeys(ips):
        try:
            san.append(x509.IPAddress(ipaddress.ip_address(raw)))
        except ValueError:
            # Not an address. Skip it rather than emit a broken certificate.
            continue

    srv_cert = (
        x509.CertificateBuilder()
        .subject_name(x509.Name([
            x509.NameAttribute(NameOID.COMMON_NAME, "fyp-control-plane"),
        ]))
        .issuer_name(ca_name)
        .public_key(srv_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(_now() - _dt.timedelta(minutes=5))
        .not_valid_after(_now() + _dt.timedelta(days=_VALID_DAYS))
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(x509.SubjectAlternativeName(san), critical=False)
        # Its own identifier, and a pointer to the authority that signed it. Same
        # reason as above: without these a strict verifier cannot follow the chain.
        .add_extension(
            x509.SubjectKeyIdentifier.from_public_key(srv_key.public_key()),
            critical=False,
        )
        .add_extension(
            x509.AuthorityKeyIdentifier.from_issuer_public_key(ca_key.public_key()),
            critical=False,
        )
        .sign(ca_key, hashes.SHA256())
    )

    pem = serialization.Encoding.PEM
    fmt = serialization.PrivateFormat.PKCS8
    no_pw = serialization.NoEncryption()
    return {
        "ca.pem": ca_cert.public_bytes(pem),
        "ca.key": ca_key.private_bytes(pem, fmt, no_pw),
        "server.pem": srv_cert.public_bytes(pem),
        "server.key": srv_key.private_bytes(pem, fmt, no_pw),
    }


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Generate the demo LAN certificate authority and server certificate.",
    )
    ap.add_argument("--out", default="certs", help="output directory (default: certs)")
    ap.add_argument("--host", action="append", default=[], help="extra hostname (repeatable)")
    ap.add_argument("--ip", action="append", default=[], help="extra IP address (repeatable)")
    ap.add_argument("--force", action="store_true", help="replace an existing authority")
    args = ap.parse_args()

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    # Refuse to silently replace an authority the machines already trust. A new CA
    # means every trust step has to be redone, and discovering that at T-45 is
    # exactly the surprise this project writes scripts to avoid.
    if (out / "ca.pem").exists() and not args.force:
        print(f"{out / 'ca.pem'} already exists -- leaving it alone.")
        print("Pass --force to replace it (every machine must then re-trust the new CA).")
        return 0

    detected = local_ipv4_addresses()
    blobs = make_ca_and_server(hostnames=args.host, ip_addresses=args.ip + detected)
    for name, data in blobs.items():
        path = out / name
        path.write_bytes(data)
        if name.endswith(".key"):
            # Owner-read-only where the platform honours it.
            try:
                path.chmod(0o600)
            except OSError:
                pass

    every_host = ["localhost", "host.docker.internal"] + list(args.host)
    every_ip = ["127.0.0.1"] + list(args.ip) + detected
    print(f"Wrote the authority and server certificate to {out.resolve()}")
    print("  names    : " + ", ".join(dict.fromkeys(every_host)))
    print("  addresses: " + ", ".join(dict.fromkeys(every_ip)))
    print("")
    print("Next: trust the CA on this machine -- docs/DEMO_RUNBOOK.md, pre-flight.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
