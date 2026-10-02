"""Node-token auth for /agent/* endpoints (protocol.md §1).

A node token is an opaque, high-entropy secret issued once at registration. It is
stored HASHED (SHA-256) — never plaintext. Because the token is high-entropy
(unlike a human password), a fast hash is appropriate; the W6 user-password path
will use a slow hash (bcrypt/argon2) instead.

`POST /agent/register` is the bootstrap exception: it *issues* the token, so it
cannot require one.
"""

import hashlib
import secrets

from fastapi import Depends, Header, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from .db import get_session
from .models import Node


def new_token() -> str:
    """A fresh opaque node token (URL-safe, ~256 bits)."""
    return secrets.token_urlsafe(32)


def hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _extract_bearer(authorization: str | None) -> str:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="missing node token")
    token = authorization[len("Bearer ") :].strip()
    if not token:
        raise HTTPException(status_code=401, detail="empty node token")
    return token


async def require_node(
    authorization: str | None = Header(default=None),
    session: AsyncSession = Depends(get_session),
) -> Node:
    """Resolve the calling node from its bearer token, or 401.

    Shares the request-cached `get_session`, so the returned Node is attached to
    the same session the endpoint uses and can be mutated + committed directly.
    """
    token = _extract_bearer(authorization)
    result = await session.execute(
        select(Node).where(Node.token_hash == hash_token(token))
    )
    node = result.scalar_one_or_none()
    if node is None:
        raise HTTPException(status_code=401, detail="invalid node token")
    return node
