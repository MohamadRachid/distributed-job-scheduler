"""Test fixtures — the W1 smoke test runs WITHOUT Docker (brief §9).

Strategy: point the app's settings at SQLite *before* importing it, then back the
app with an in-memory SQLite engine via a StaticPool (one shared connection, so
the schema and rows persist across requests). The real asyncpg engine is never
touched — `get_session` is dependency-overridden.

W6 adds two more overrides so the ~90 existing tests keep working after auth landed:
  * `require_user` -> a seeded test user, so a call with no Authorization header is
    transparently authenticated (the pre-W6 tests never send one);
  * `get_storage`  -> an in-memory MemoryStore, so artifact endpoints work without a
    MinIO server.
The `anon_client` fixture leaves `require_user` REAL, for the 401 + login tests.
"""

import os

# Must be set before `app` is imported so get_settings() caches a sqlite URL and
# the module-level engine in app.db is harmless (never connected to in tests).
os.environ.setdefault("DATABASE_URL", "sqlite+aiosqlite://")

import pytest_asyncio  # noqa: E402
from fastapi import Depends  # noqa: E402
from httpx import ASGITransport, AsyncClient  # noqa: E402
from sqlalchemy import select  # noqa: E402
from sqlalchemy.ext.asyncio import (  # noqa: E402
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import StaticPool  # noqa: E402

import app.models  # noqa: E402,F401  -- import first so all six tables register
from app.db import Base, get_session, get_session_factory  # noqa: E402
from app.main import app  # noqa: E402  -- binds name `app` to the FastAPI instance last
from app.models import User  # noqa: E402
from app.storage import MemoryStore, get_storage  # noqa: E402
from app.userauth import hash_password, require_user  # noqa: E402

# Credentials the auth tests log in with (the seeded user).
TEST_USERNAME = "tester"
TEST_PASSWORD = "test-pass-123"


@pytest_asyncio.fixture
async def session_factory():
    """Fresh in-memory DB per test, with the six tables created."""
    engine = create_async_engine(
        "sqlite+aiosqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        yield factory
    finally:
        await engine.dispose()


@pytest_asyncio.fixture
def mem_store():
    """One in-memory object store per test (so a test can inspect stored bytes)."""
    return MemoryStore()


async def _seed_user(session_factory) -> None:
    """Create the one test user, so a submitted job's user_id FK resolves and the
    login tests have real credentials to authenticate against.

    2026-09-04 (storage quota): the tiers are seeded first — every user row points at
    one, so a database created from the models rather than from the migration would
    otherwise have a `tier_id` naming a row that does not exist. The test user is an
    ADMIN and has ACCEPTED its limits, which is the ordinary state of the one account
    a deployment bootstraps (see `userauth.ensure_admin_user`), and is what the ~230
    tests written before this date assume when they submit a job without ever
    mentioning storage. The tests that exercise the refusals build their own users
    and accept nothing — the gate is proven by the tests that are about the gate, not
    by breaking every test that is about something else."""
    from app.models import Tier
    from app.quota import accept_limits, ensure_tiers

    async with session_factory() as s:
        await ensure_tiers(s)
        tier = await s.get(Tier, "standard")
        user = User(
            username=TEST_USERNAME,
            password_hash=hash_password(TEST_PASSWORD),
            is_admin=True,
            tier_id=tier.id,
        )
        accept_limits(user, tier)
        s.add(user)
        await s.commit()


async def seal_for_run(session_factory, run_id, data: bytes) -> bytes:
    """Seal test bytes with the run's own job key, the way its container would.

    Every job created since 2026-09-06 is sealed, and the control plane refuses to
    store an artefact that is not (api/artifacts.upload_artifact). A test that uploads
    a result is standing in for a container, so it has to do what a container does —
    and doing it with the REAL key, read out of `job_keys`, is what keeps the download
    tests meaningful: they get their plaintext back because the seal genuinely opens.

    Bytes that are already sealed come back untouched, so a helper can call this
    without knowing whether its caller did."""
    from app.models import Job, JobKey, Run
    from app.sealing import is_sealed, key_from_b64, seal_stream

    if is_sealed(data):
        return data
    async with session_factory() as s:
        run = await s.get(Run, run_id)
        if run is None:
            return data
        job = await s.get(Job, run.job_id)
        if job is None or not job.sealed:
            return data          # an unsealed job stores plain bytes, as it always did
        row = await s.get(JobKey, job.id)
        if row is None:
            return data          # shredded: that test is about the shred, not sealing
    return seal_stream(data, key_from_b64(row.key_b64))


@pytest_asyncio.fixture
async def client(session_factory, mem_store):
    """Authenticated httpx client: get_session -> SQLite, require_user -> the seeded
    test user (pre-W6 tests send no token), get_storage -> the in-memory store."""
    await _seed_user(session_factory)

    async def _override_session():
        async with session_factory() as session:
            yield session

    async def _override_user(session: AsyncSession = Depends(get_session)):
        # Reuse the request's (overridden) session — one StaticPool connection, so a
        # second session here would deadlock. FastAPI caches get_session per request,
        # so this User is attached to the same session the endpoint uses.
        return (
            await session.execute(select(User).where(User.username == TEST_USERNAME))
        ).scalar_one()

    app.dependency_overrides[get_session] = _override_session
    app.dependency_overrides[require_user] = _override_user
    app.dependency_overrides[get_storage] = lambda: mem_store
    # A background task opens its OWN session after the response has gone out, so it
    # needs the factory rather than the request's session. Without this override it
    # would build one from the module-level engine and quietly write to a database no
    # assertion in this suite ever looks at.
    app.dependency_overrides[get_session_factory] = lambda: session_factory
    transport = ASGITransport(app=app)
    try:
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            # Handed to the client so a module-level helper — which gets no fixtures —
            # can seal what it uploads with the run's real key. Set on the object
            # rather than threaded through a dozen helper signatures that are not
            # about sealing.
            c.session_factory = session_factory
            yield c
    finally:
        app.dependency_overrides.clear()


@pytest_asyncio.fixture
async def anon_client(session_factory, mem_store):
    """Like `client` but with REAL user auth (require_user is NOT overridden) — for
    the 401 tests and the /auth/login flow. The test user is seeded so login works."""
    await _seed_user(session_factory)

    async def _override_session():
        async with session_factory() as session:
            yield session

    app.dependency_overrides[get_session] = _override_session
    app.dependency_overrides[get_storage] = lambda: mem_store
    app.dependency_overrides[get_session_factory] = lambda: session_factory
    transport = ASGITransport(app=app)
    try:
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            yield c
    finally:
        app.dependency_overrides.clear()
