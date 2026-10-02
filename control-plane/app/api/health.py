"""Liveness probe (brief §4).

W7a adds two fields, and neither is decoration.

`experiment_mode` — the measurement harness READS this before every arm and
refuses to record a number unless the mode it finds is the mode it asked for. A
run labelled "fencing off" that was quietly measured with fencing on would be a
fabricated result, so the platform states its own configuration and the harness
checks it (brief §4.1).

`mode_epoch` — a value that is different every time this process starts. The mode
check above only proves the configuration was right at the moment it was read; if
something restarts the control plane *during* a repetition — a manual `docker
compose up`, a stray `stage_demo.ps1`, a crash and restart — the mode could change
underneath a measurement already in flight and the harness would never know. So it
records the epoch when it asserts the mode and re-checks it when the repetition
ends: a changed epoch means that repetition is void, loudly.

This is the reaper's under-lock re-check argument pointed at our own bench: check
the thing again at the moment you rely on it, not only when you first read it.

Why a fresh id per process rather than a counter that climbs: a counter has to be
stored somewhere, and both places available here are wrong. In the database, our
own `reset_platform()` truncation would reset it. In the container's filesystem, it
dies with the container on the very restart it is meant to report. An id that is
new on every start needs no storage and answers the only question the harness
actually asks — "is this the same control-plane process I checked a moment ago?"
"""

import uuid

from fastapi import APIRouter

from ..config import experiment_mode

router = APIRouter(tags=["health"])

# Generated once, when this module is first imported — i.e. once per control-plane
# process. Any restart produces a different value.
MODE_EPOCH = uuid.uuid4().hex[:12]


@router.get("/health")
async def health() -> dict:
    return {
        "status": "ok",
        "experiment_mode": experiment_mode(),
        "mode_epoch": MODE_EPOCH,
    }
