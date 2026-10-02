"""Users, tiers and the acceptance box (2026-09-04) — protocol.md §10.

Four routes, two audiences:

  * `GET /me` and `POST /me/accept-limits` (any logged-in user) — where I stand, and
    the one click that says I agree to it;
  * `POST /users` and `PATCH /users/{id}/tier` (admin only) — creating a user, and
    moving one between tiers.

**Why `GET /me` exists at all.** A cap the user cannot see is a trap: the platform
would refuse work for a reason nothing on the screen had ever mentioned, and the
first they would hear of it is a failed run. So the submit form reads this before it
lets anything be submitted, and it is also what the acceptance box is drawn from —
the box shows the two numbers, because the agreement is to numbers rather than to a
word.

**Why acceptance is not simply assumed.** The supervisor's request on 2026-09-03 was
that a user be told the limits and accept them, not merely be bound by them. The one
exception is the bootstrap admin, which the deployment creates from its own
environment and which is therefore recorded as accepting at creation (see
`userauth.ensure_admin_user`, where that judgment is stated). Every user created
through `POST /users` starts un-accepted.

**There is still no self-registration.** `POST /users` is an admin creating an
account for someone; it is not a stranger signing themselves up, and the frozen
contract's "no registration endpoint" holds in the sense it was written in.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from ..db import get_session
from ..models import Tier, User
from ..quota import (
    accept_limits,
    get_tier,
    limits_accepted,
    mb,
    retained_used_bytes,
)
from ..schemas import MeOut, TierAssignment, TierOut, UserCreate, UserOut
from ..userauth import hash_password, require_admin, require_user

router = APIRouter(tags=["users"])


async def _me(session: AsyncSession, user: User) -> MeOut:
    tier = await get_tier(session, user.tier_id)
    used = await retained_used_bytes(session, user.id)
    return MeOut(
        username=user.username,
        is_admin=user.is_admin,
        tier=user.tier_id,
        retained_cap_mb=mb(tier.retained_cap_bytes if tier else 0),
        scratch_cap_mb=mb(tier.scratch_cap_bytes if tier else 0),
        retained_used_mb=mb(used),
        limits_accepted=limits_accepted(user, tier),
        limits_accepted_at=user.limits_accepted_at,
    )


async def _username_taken(session: AsyncSession, username: str) -> bool:
    """Is this (already stripped) name in use? The pre-check behind the clean 409.

    It is a module-level function rather than an inline query so a test can stand
    in for it and reproduce the race below: two requests that both saw "free" a
    moment before one of them lost at the constraint."""
    clash = (
        await session.execute(select(User.id).where(User.username == username))
    ).first()
    return clash is not None


def _user_out(user: User, tier: Tier | None) -> UserOut:
    return UserOut(
        user_id=user.id,
        username=user.username,
        is_admin=user.is_admin,
        tier=user.tier_id,
        limits_accepted=limits_accepted(user, tier),
    )


@router.get("/me", response_model=MeOut)
async def read_me(
    user: User = Depends(require_user),
    session: AsyncSession = Depends(get_session),
) -> MeOut:
    """Tier, both caps, retained bytes used, and whether the limits were accepted.

    `retained_used_mb` is computed the same way the refusal computes it — the same
    sum over the same rows, in `quota.retained_used_bytes` — rather than from a
    counter kept in step by hand. So the number on the screen and the number that
    refuses an upload cannot disagree, which is the only way a user can trust the
    bar they are looking at."""
    return await _me(session, user)


@router.post("/me/accept-limits", response_model=MeOut)
async def accept_my_limits(
    user: User = Depends(require_user),
    session: AsyncSession = Depends(get_session),
) -> MeOut:
    """Record that this user agrees to the numbers their tier carries right now.

    What is stored is the tier AND both figures as they stand at this moment, so the
    agreement is to what was on the screen. Accepting again is harmless; it simply
    re-stamps the same three values."""
    tier = await get_tier(session, user.tier_id)
    if tier is None:
        raise HTTPException(status_code=422, detail=f"unknown tier: {user.tier_id}")
    accept_limits(user, tier)
    await session.commit()
    return await _me(session, user)


@router.get("/tiers", response_model=list[TierOut])
async def list_tiers(
    user: User = Depends(require_user),
    session: AsyncSession = Depends(get_session),
) -> list[TierOut]:
    """The tiers this deployment offers. A read, not a management route: there is no
    endpoint that edits a tier's numbers, because they are configuration and belong
    where the deployment is configured."""
    rows = (await session.execute(select(Tier).order_by(Tier.id))).scalars().all()
    return [
        TierOut(
            tier=t.id,
            retained_cap_mb=mb(t.retained_cap_bytes),
            scratch_cap_mb=mb(t.scratch_cap_bytes),
            description=t.description,
        )
        for t in rows
    ]


@router.post("/users", response_model=UserOut)
async def create_user(
    req: UserCreate,
    admin: User = Depends(require_admin),
    session: AsyncSession = Depends(get_session),
) -> UserOut:
    """Create a user in a tier (admin only).

    The new user starts with acceptance NOT given. That is the point of asking: an
    admin can decide which numbers apply to someone, and cannot agree to them on
    their behalf. Until they accept, both submission doors refuse them with
    `LIMITS_NOT_ACCEPTED`.

    A duplicate username is `409` rather than a database error surfacing as a 500 —
    the row already exists, and that is a fact about the caller's request, not a
    fault. The name is stripped ONCE, up front, and the stripped value is what is
    checked and what is stored; checking the raw one let " bob" past the check and
    into the UNIQUE constraint. And the check is only the polite first answer: two
    identical requests can both pass it before either commits, so the constraint's
    refusal is caught and turned into the same 409 the check would have given."""
    tier = await get_tier(session, req.tier)
    if tier is None:
        raise HTTPException(status_code=422, detail=f"unknown tier: {req.tier}")
    username = req.username.strip()
    if not username or not req.password:
        raise HTTPException(status_code=422, detail="username and password required")
    if await _username_taken(session, username):
        raise HTTPException(status_code=409, detail="username already exists")
    user = User(
        username=username,
        password_hash=hash_password(req.password),
        is_admin=False,
        tier_id=tier.id,
    )
    session.add(user)
    try:
        await session.commit()
    except IntegrityError:
        # The race lost: another request inserted this name between the check and
        # the commit. The database kept it to one row, which is the answer we want;
        # report it as the duplicate it is rather than as a fault.
        await session.rollback()
        raise HTTPException(status_code=409, detail="username already exists")
    return _user_out(user, tier)


@router.get("/users", response_model=list[UserOut])
async def list_users(
    admin: User = Depends(require_admin),
    session: AsyncSession = Depends(get_session),
) -> list[UserOut]:
    rows = (await session.execute(select(User).order_by(User.created_at))).scalars().all()
    out: list[UserOut] = []
    for u in rows:
        out.append(_user_out(u, await get_tier(session, u.tier_id)))
    return out


@router.patch("/users/{user_id}/tier", response_model=UserOut)
async def set_user_tier(
    user_id: str,
    req: TierAssignment,
    admin: User = Depends(require_admin),
    session: AsyncSession = Depends(get_session),
) -> UserOut:
    """Move a user to another tier (admin only).

    Acceptance is not reset here, and it does not need to be: `limits_accepted`
    compares the tier and both caps the user agreed to against the ones that now
    apply, so a move withdraws the agreement by arithmetic. There is no flag to
    clear, so there is no flag to forget to clear — which is the same reason the
    retained figure is a sum rather than a counter."""
    tier = await get_tier(session, req.tier)
    if tier is None:
        raise HTTPException(status_code=422, detail=f"unknown tier: {req.tier}")
    user = await session.get(User, user_id)
    if user is None:
        raise HTTPException(status_code=404, detail="unknown user_id")
    user.tier_id = tier.id
    await session.commit()
    return _user_out(user, tier)
