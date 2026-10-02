"""GET /nodes — the node pool with DERIVED liveness (the key W1 logic).

online/offline is NOT stored; it is computed at read time:
    online == (now - last_heartbeat) <= NODE_TIMEOUT_S
There is no reaper for nodes (a dead node cannot report its own death, so storing
`offline` would need a writer; we avoid that — see README "defend this" #1).
Runs DO get a reaper in W5, because a lost run must be actively requeued.

GET /nodes was open through W5; user-facing JWT gating switches on in W6 — both
reads now require a login.
"""

from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..config import get_settings
from ..db import get_session
from ..models import Node, NodeEvent, User
from ..scheduler import MIN_SEALED_AGENT_VERSION, _version_tuple
from ..schemas import NodeEventOut, NodeOut, TrustUpdate
from ..userauth import require_admin, require_user

router = APIRouter(tags=["nodes"])


def _node_out(n: Node, online: bool) -> NodeOut:
    return NodeOut(
        node_id=n.id,
        name=n.name,
        online=online,
        reported_status=n.status.value,
        cpu_cores=n.cpu_cores,
        has_gpu=n.has_gpu,
        ram_mb=n.ram_mb,
        capacity=n.capacity,
        last_heartbeat=n.last_heartbeat,
        agent_version=n.agent_version,
        hw_specs=n.hw_specs,
        usage=n.usage,
        battery_pct=n.battery_pct,
        battery_charging=n.battery_charging,
        trusted=n.trusted,  # W6b — admin-set; gates private jobs
        disk_free_mb=n.disk_free_mb,  # 2026-09-04 — free disk, read by placement
        # 2026-09-07 (walk 1, row 65): say on the node itself when its agent is too
        # old to be offered today's (sealed) jobs, with the version it needs.
        agent_outdated=_version_tuple(n.agent_version) < MIN_SEALED_AGENT_VERSION,
        min_agent_version=".".join(str(p) for p in MIN_SEALED_AGENT_VERSION),
    )


def _as_utc(dt: datetime) -> datetime:
    """Normalise to tz-aware UTC. Postgres returns aware datetimes; SQLite (tests)
    returns naive — assume UTC there so the subtraction below never raises."""
    return dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)


@router.get("/nodes", response_model=list[NodeOut])
async def list_nodes(
    user: User = Depends(require_user),
    session: AsyncSession = Depends(get_session),
) -> list[NodeOut]:
    timeout = timedelta(seconds=get_settings().node_timeout_s)
    now = datetime.now(timezone.utc)

    rows = (await session.execute(select(Node))).scalars().all()
    out: list[NodeOut] = []
    for n in rows:
        online = (
            n.last_heartbeat is not None
            and (now - _as_utc(n.last_heartbeat)) <= timeout
        )
        out.append(_node_out(n, online))
    return out


@router.patch("/nodes/{node_id}/trusted", response_model=NodeOut)
async def set_trusted(
    node_id: str,
    req: TrustUpdate,
    admin: User = Depends(require_admin),
    session: AsyncSession = Depends(get_session),
) -> NodeOut:
    """Mark a machine as trusted to run PRIVATE jobs — or take that back (W6b).

    ADMIN-ONLY since 2026-09-04 (a non-admin login gets 403). It was JWT-gated from
    W6b, which was right while there was exactly one user; once an admin can create
    other users, "the admin's judgment, made from outside the machine" has to be
    enforced rather than merely described. Trust and storage tier are now set by the
    same kind of caller, which is what they always were in intent.

    This is the admin's judgment, made from outside the machine, and it is the whole
    point of the tier: a node cannot send this about itself (registration and
    heartbeat carry no such field), because "trust me" from an untrusted machine is
    worth nothing. Sealing protects the data from a machine's *users*; the trust tier
    is how we choose which machines get to open it at all.

    Untrusting a node does NOT touch runs already executing there — the data is
    already open in that container. It only stops FUTURE private placements, which is
    the honest description of what a scheduler filter can do."""
    node = await session.get(Node, node_id)
    if node is None:
        raise HTTPException(status_code=404, detail="unknown node_id")
    node.trusted = req.trusted
    await session.commit()
    timeout = timedelta(seconds=get_settings().node_timeout_s)
    online = (
        node.last_heartbeat is not None
        and (datetime.now(timezone.utc) - _as_utc(node.last_heartbeat)) <= timeout
    )
    return _node_out(node, online)


@router.get("/nodes/{node_id}/events", response_model=list[NodeEventOut])
async def list_node_events(
    node_id: str,
    limit: int = 20,
    user: User = Depends(require_user),
    session: AsyncSession = Depends(get_session),
) -> list[NodeEventOut]:
    """The node's postmortem history (W5b) — goodbyes + classified comeback causes,
    newest first. The UI shows these under a node card so a silent machine's story
    is right there. (Nodes are never reaped; this is history, not liveness.)"""
    if await session.get(Node, node_id) is None:
        raise HTTPException(status_code=404, detail="unknown node_id")
    rows = (
        await session.execute(
            select(NodeEvent)
            .where(NodeEvent.node_id == node_id)
            .order_by(NodeEvent.ts.desc())
            .limit(max(1, min(limit, 200)))
        )
    ).scalars().all()
    return [
        NodeEventOut(ts=e.ts, event=e.event, cause=e.cause, evidence=e.evidence)
        for e in rows
    ]
