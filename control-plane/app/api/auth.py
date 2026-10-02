"""User login (protocol.md §10) — W6.

`POST /auth/login {username, password}` -> `{token, expires_at}`.

There is deliberately NO registration endpoint: it is not in the frozen contract,
and one admin user (created by the startup bootstrap) is the honest, single-user
scope. "User management is future work."
"""

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..db import get_session
from ..models import User
from ..schemas import LoginRequest, LoginResponse
from ..userauth import create_access_token, verify_password

router = APIRouter(tags=["auth"])


@router.post("/auth/login", response_model=LoginResponse)
async def login(
    req: LoginRequest, session: AsyncSession = Depends(get_session)
) -> LoginResponse:
    """Verify the password (bcrypt) and return a signed JWT.

    A wrong username and a wrong password return the SAME 401 with the same body —
    we never leak which one existed (a small but real information-disclosure guard)."""
    user = (
        await session.execute(select(User).where(User.username == req.username))
    ).scalar_one_or_none()
    if user is None or not verify_password(req.password, user.password_hash):
        raise HTTPException(status_code=401, detail="invalid username or password")
    token, expires_at = create_access_token(user)
    return LoginResponse(token=token, expires_at=expires_at)
