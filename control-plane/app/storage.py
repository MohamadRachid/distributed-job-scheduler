"""Object storage — the control plane's ONE MinIO client (W6).

Design rule (ISO 27001 A.8.31): the workers never hold storage credentials. So
there is exactly one place that talks to MinIO — here, inside the control plane —
and the browser never talks to MinIO either (downloads are brokered through
`GET /artifacts/{id}/download`). The agent uploads bytes to the control plane; the
control plane writes them to MinIO. Storage creds live only in the control-plane
environment (docker-compose.yml), never in an agent env.

The store is behind a tiny interface with two implementations:
  * `MinioStore`  — the real one, used by the app.
  * `MemoryStore` — a dict-backed stand-in, used by tests (no MinIO server needed).

Endpoints depend on `get_storage`, so tests override it with a MemoryStore exactly
like they override `get_session` with SQLite. The MinIO SDK is synchronous, so the
endpoints call these methods via `asyncio.to_thread` to avoid blocking the loop.
"""

from __future__ import annotations

import io
import logging
from functools import lru_cache
from typing import Protocol

from .config import get_settings

log = logging.getLogger("storage")


class ObjectStore(Protocol):
    """The narrow surface the control plane needs from object storage."""

    def ensure_bucket(self) -> None: ...
    def put_object(self, key: str, data: bytes, content_type: str | None) -> None: ...
    def get_object(self, key: str) -> bytes: ...
    def delete_object(self, key: str) -> None: ...


class MinioStore:
    """The real store. The MinIO client is lazy — constructing it does not connect;
    the first operation does — so importing this module is side-effect-free."""

    def __init__(self) -> None:
        from minio import Minio

        s = get_settings()
        self._bucket = s.minio_bucket
        self._client = Minio(
            s.minio_endpoint,
            access_key=s.minio_access_key,
            secret_key=s.minio_secret_key,
            secure=s.minio_secure,
        )

    def ensure_bucket(self) -> None:
        if not self._client.bucket_exists(self._bucket):
            self._client.make_bucket(self._bucket)

    def put_object(self, key: str, data: bytes, content_type: str | None) -> None:
        self._client.put_object(
            self._bucket,
            key,
            io.BytesIO(data),
            length=len(data),
            content_type=content_type or "application/octet-stream",
        )

    def get_object(self, key: str) -> bytes:
        resp = self._client.get_object(self._bucket, key)
        try:
            return resp.read()
        finally:
            resp.close()
            resp.release_conn()

    def delete_object(self, key: str) -> None:
        """Remove one object. Two callers, and they delete for opposite reasons.

        The log archiver calls it on a FAILED verification — an archive whose
        read-back did not match must not be left where a later pointer could reach
        it. It is never used to remove a *verified* archive: a purged log set has
        exactly one copy, and this is it.

        Checkpoint cleanup (2026-08-29) calls it on SUCCESS, which is safe for the
        opposite reason: a checkpoint is working state on the way to a result, so
        once the result is stored, or once a newer checkpoint has been read back and
        matched, the older bytes can never be needed again. The worst a wrong delete
        there could cost is repeated training — never a wrong answer — which is why
        that path may remove something verified and this one may not."""
        self._client.remove_object(self._bucket, key)


class MemoryStore:
    """A dict-backed store for tests. Same interface, no server. Overwriting the
    same key returns the last bytes — mirrors MinIO's put semantics, so the
    idempotent-upload behaviour under test matches production."""

    def __init__(self) -> None:
        self._objs: dict[str, bytes] = {}

    def ensure_bucket(self) -> None:
        pass

    def put_object(self, key: str, data: bytes, content_type: str | None) -> None:
        self._objs[key] = data

    def get_object(self, key: str) -> bytes:
        return self._objs[key]

    def delete_object(self, key: str) -> None:
        self._objs.pop(key, None)


@lru_cache
def _minio_store() -> MinioStore:
    return MinioStore()


def get_storage() -> ObjectStore:
    """FastAPI dependency. Returns the process-wide MinIO store. Tests override this
    with a MemoryStore (app.dependency_overrides[get_storage] = lambda: mem)."""
    return _minio_store()
