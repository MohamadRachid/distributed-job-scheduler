"""Async SQLAlchemy engine + session factory.

Async is the locked choice (§7: "async fits many polling agents"). The engine is
created lazily-connecting — it does not touch the DB until first use — so importing
this module is cheap and side-effect-free.

Tests do NOT use this engine: they override the `get_session` dependency with a
SQLite StaticPool engine (see control-plane/conftest.py).
"""

from collections.abc import AsyncIterator

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

from .config import get_settings


class Base(DeclarativeBase):
    """Declarative base; all models inherit this. `Base.metadata` is what Alembic
    autogenerate diffs against."""


_settings = get_settings()
engine = create_async_engine(_settings.database_url, future=True, echo=False)
SessionLocal = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


def get_session_factory() -> async_sessionmaker[AsyncSession]:
    """FastAPI dependency returning the session FACTORY rather than a session.

    A background task outlives the request that scheduled it, so it cannot borrow the
    request's session — that one is closed before the task runs. It needs to open its
    own, which means it needs the factory.

    This is a dependency and not a direct `SessionLocal` import for the same reason
    `get_session` is: the tests replace the database wholesale, and a task that
    imported the module-level factory would quietly talk to the real engine while
    every assertion looked at SQLite. Overriding it in `conftest` keeps the one rule
    that makes the suite trustworthy — no test reaches a real server.
    """
    return SessionLocal


async def get_session() -> AsyncIterator[AsyncSession]:
    """FastAPI dependency. Cached per-request, so all dependencies in one request
    (e.g. the node-token check *and* the endpoint) share one session/transaction."""
    async with SessionLocal() as session:
        yield session
