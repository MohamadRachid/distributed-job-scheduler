"""User auth for the user-facing endpoints (protocol.md §1, §10) — W6.

Two kinds of callers, two kinds of keys (the frozen contract's whole auth model):
  * agents  -> an opaque NODE TOKEN (see auth.py). Never expires, checked by hash.
  * users   -> a JWT from POST /auth/login, sent as `Authorization: Bearer <jwt>`.

A node token can never act as a user and a JWT can never act as a node — they are
verified by different code paths against different columns. This module owns only
the user side.

Plain words:
  * bcrypt is a slow, salted password hash — a stolen `users` table does not give
    up the passwords, and the per-password salt defeats precomputed tables.
  * a JWT is a signed pass card: the server re-checks the signature (and expiry)
    on every request instead of keeping a server-side session table.

JWT is AUTHENTICATION, not privacy: it answers "who are you", it does not hide job
data from anyone. Data privacy is W6b.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import bcrypt
import jwt
from fastapi import Depends, Header, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from .config import get_settings
from .db import get_session
from .models import Tier, User
from .quota import DEFAULT_TIER_ID, accept_limits

_ALGO = "HS256"


# --- passwords --------------------------------------------------------------


def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


def verify_password(password: str, password_hash: str) -> bool:
    try:
        return bcrypt.checkpw(password.encode("utf-8"), password_hash.encode("utf-8"))
    except (ValueError, TypeError):
        return False


# --- tokens -----------------------------------------------------------------


def create_access_token(user: User) -> tuple[str, datetime]:
    """Mint a signed JWT for `user`. Returns (token, expires_at). `sub` is the user
    id, so a gated endpoint can attribute a job to its owner without a second call."""
    s = get_settings()
    expires_at = datetime.now(timezone.utc) + timedelta(hours=s.jwt_expiry_hours)
    payload = {"sub": user.id, "username": user.username, "exp": expires_at}
    token = jwt.encode(payload, s.jwt_secret, algorithm=_ALGO)
    return token, expires_at


def decode_token(token: str) -> dict:
    """Verify signature + expiry and return the claims, or raise. `jwt.decode`
    raises `ExpiredSignatureError` / `InvalidTokenError` on a bad or expired token."""
    return jwt.decode(token, get_settings().jwt_secret, algorithms=[_ALGO])


def _bearer(authorization: str | None) -> str:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="missing login token")
    token = authorization[len("Bearer ") :].strip()
    if not token:
        raise HTTPException(status_code=401, detail="empty login token")
    return token


async def require_user(
    authorization: str | None = Header(default=None),
    session: AsyncSession = Depends(get_session),
) -> User:
    """Gate a user endpoint: resolve the caller from a valid JWT, or 401.

    Shares the request-cached session (like require_node), so the returned User is
    attached to the endpoint's session — create_job can set jobs.user_id from it.
    A token whose user no longer exists is treated as unauthenticated."""
    token = _bearer(authorization)
    try:
        claims = decode_token(token)
    except jwt.PyJWTError:
        raise HTTPException(status_code=401, detail="invalid or expired token")
    user = await session.get(User, claims.get("sub"))
    if user is None:
        raise HTTPException(status_code=401, detail="unknown user")
    return user


async def require_admin(user: User = Depends(require_user)) -> User:
    """Gate an ADMIN route: a valid login that also carries the admin flag, or 403.

    401 and 403 are deliberately different answers to different questions. 401 says
    "I do not know who you are"; 403 says "I know exactly who you are, and this is
    not yours to do". Collapsing them would make an admin route indistinguishable
    from an expired session, and the UI would bounce a perfectly logged-in user back
    to the login screen for a permission problem.

    Three routes sit behind this, and they have one thing in common: each is a
    decision made ABOUT someone rather than BY them — creating a user, moving a user
    between storage tiers, and marking a machine trusted to open private data. The
    trust route was JWT-gated from W6b and is admin-gated from 2026-09-04, so trust
    and tier are now set by the same kind of caller, which is what they always were
    in intent."""
    if not user.is_admin:
        raise HTTPException(status_code=403, detail="admin only")
    return user


def authorize_ws_token(token: str | None) -> bool:
    """Validate a JWT passed on a WebSocket query string (browsers cannot set an
    Authorization header on a socket). True iff the token is present and valid."""
    if not token:
        return False
    try:
        decode_token(token)
        return True
    except jwt.PyJWTError:
        return False


async def user_from_ws_token(session: AsyncSession, token: str | None) -> User | None:
    """The socket's form of `require_user`: resolve the caller, or None (2026-09-07).

    `authorize_ws_token` above answers "is this a valid login", which was the whole
    question while the platform had one account. The log socket now has to answer
    "whose login is it", because a valid token belonging to somebody else is exactly
    the case read-scoping exists to refuse. Same signature check, same expiry check —
    it then loads the user the way the HTTP gate does, so a token whose user has been
    deleted is refused here too."""
    if not token:
        return None
    try:
        claims = decode_token(token)
    except jwt.PyJWTError:
        return None
    return await session.get(User, claims.get("sub"))


# --- bootstrap --------------------------------------------------------------


async def ensure_admin_user(session: AsyncSession) -> None:
    """Create the bootstrap admin on first startup if the users table is empty and
    ADMIN_USERNAME/ADMIN_PASSWORD are set. Idempotent: does nothing once a user
    exists. There is still NO self-registration — from 2026-09-04 further users are
    created by this admin through `POST /users`, which is a different thing from a
    stranger signing themselves up.

    Two facts are stamped on that first user, and the second one is a judgment worth
    stating rather than burying:

      * `is_admin = True`. Somebody has to be able to create the second user, and it
        can only be the account the deployment itself created.
      * its storage limits are recorded as ACCEPTED at creation. This account is made
        from the deployment's own environment by the operator who configured the
        tiers, so asking that same person to agree to their own configuration would
        be ceremony, and it would break every existing script and the staged
        demonstration for no gain in honesty. **Every user created afterwards through
        `POST /users` starts un-accepted and must agree for themselves** — which is
        where the supervisor's request actually bites, and what P1c proves.

    `ensure_tiers` runs before this in `main.lifespan`, so the tier being accepted
    here exists."""
    s = get_settings()
    if not s.admin_username or not s.admin_password:
        return
    existing = (await session.execute(select(User.id).limit(1))).first()
    if existing is not None:
        return
    admin = User(
        username=s.admin_username,
        password_hash=hash_password(s.admin_password),
        is_admin=True,
        tier_id=DEFAULT_TIER_ID,
    )
    tier = await session.get(Tier, DEFAULT_TIER_ID)
    if tier is not None:
        accept_limits(admin, tier)
    session.add(admin)
    await session.commit()
