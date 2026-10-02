"""Server-side diagnosis — the honest classifiers behind W5b.

Pure functions, so they are tested directly in the container suite:

  classify_lost      -> why a run's lease lapsed (NODE_LOST vs RUN_LOST), from the
                        reaper, inside its existing transaction.
  reason_from_exit_code -> a minimal fallback reason when an OLD agent reports FAILED
                        without a classified reason (the exit code is a hard fact the
                        server has too; the agent's classifier is richer).
  classify_comeback  -> the node "comeback interview": given the facts a returning
                        agent hands over, name the outage cause in priority order.

Honesty rules (locked): reasons come from HARD FACTS; where a cause is only inferred
the detail says "likely" and shows the evidence; the platform never invents a reason.
"""

from datetime import datetime, timezone


def _as_utc(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    return dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)


def _hhmm(epoch: float | None) -> str:
    """Format a unix-seconds timestamp as HH:MM:SS UTC (deterministic, for details)."""
    if epoch is None:
        return "?"
    try:
        return datetime.fromtimestamp(float(epoch), tz=timezone.utc).strftime("%H:%M:%S")
    except (ValueError, OverflowError, OSError):
        return "?"


# --- run loss (from the reaper) ----------------------------------------------


def classify_lost(node, lease_expires_at, *, exhausted: bool = False):
    """Two honest stories, told apart by whether the node was still heart-beating
    when the lease died.

      NODE_LOST : the node's last heartbeat PREDATES the lease deadline -> it went
                  silent (the machine or its network went down).
      RUN_LOST  : the node kept heart-beating but stopped reporting THIS run -> an
                  agent/container problem; the machine itself is fine.
    """
    name = getattr(node, "name", None) or "the node"
    last_hb = _as_utc(getattr(node, "last_heartbeat", None)) if node is not None else None
    lease = _as_utc(lease_expires_at)

    if last_hb is not None and lease is not None and last_hb >= lease:
        reason = "RUN_LOST"
        detail = (
            f"{name} is alive but stopped reporting this run — an agent or container "
            "problem, not the machine."
        )
    else:
        reason = "NODE_LOST"
        if last_hb is not None and lease is not None:
            gap = max(0, round((lease - last_hb).total_seconds()))
            detail = (
                f"{name} stopped heart-beating {gap}s before the lease expired — the "
                "machine or its network went down."
            )
        else:
            detail = f"{name} stopped heart-beating — the machine or its network went down."

    if exhausted:
        detail = f"Run lost and no retries left. Last loss: {detail}"
    return reason, detail


def reason_from_exit_code(code):
    """Minimal hard-fact reason from just an exit code — the server fallback used
    only when an agent reports FAILED without a classified reason (older agents)."""
    if code in (0, None):
        return None, None
    if code == 137:
        return "KILLED", "The container was killed (SIGKILL, exit 137)."
    if code == 139:
        return "APP_CRASH", "The program crashed — bad memory access (SIGSEGV, exit 139)."
    if code == 143:
        return "TERMINATED", "The program was told to stop (SIGTERM, exit 143)."
    return "APP_ERROR", f"The program exited with code {code}."


# --- private inputs: was the sealed file ever actually opened? ----------------

# W6b + 2026-08-15. A classified reason like any other (NFR-8), carried by a run that
# SUCCEEDED — which is deliberate, and explained in decide_private_input_outcome.
PRIVATE_INPUT_NOT_OPENED = "PRIVATE_INPUT_NOT_OPENED"
# 2026-09-07 (walk 1, row 58): the same finding on a SEALED job, which is every job
# submitted since 2026-09-06. A different label because the fix is different — the
# old private shape needed the opener wrapper (fyp_open.py); a sealed job needs its
# dataset read through `fyp_data.open_input()`. The message names that and nothing
# about "private", because no such box exists on the form any more.
INPUT_NOT_OPENED = "INPUT_NOT_OPENED"


def decide_private_input_outcome(
    *, is_private: bool, input_opened: bool, sealed: bool = False
):
    """Pure decision behind a finished private run — testable without a database.

    A private job's sealed file is opened by a wrapper that redeems a single-use
    ticket inside the container. Which wrapper runs is chosen by the CLIENT: the
    browser hard-codes it, and the API requires nothing. So a private job submitted
    with a plain entrypoint is accepted, sealed, placed on a trusted node — and its
    container never opens the file. Before this check it finished SUCCEEDED with no
    warning at all, and the user was told their private input had been used.

    Nothing leaked. The seal held and the trust gate held. What was wrong is that the
    platform reported a success it could not support.

    **Why redemption is the signal, and not the entrypoint.** Matching the entrypoint
    against a known wrapper is guessing from a string: a workload that calls the
    opener from a shell script, a Makefile or a subprocess would be falsely accused.
    Redemption is the event itself. The agent takes a ticket for every private run
    before the container starts, and only the container can spend it — so an unspent
    ticket on a finished run is proof, not inference.

    **Flagged, not failed.** The program exited zero and we do not rewrite that.
    "The workload ran" and "the workload used its input" are two different claims,
    and collapsing them would change what an accepted result means. So the run stays
    SUCCEEDED and carries a reason, which is where every other diagnosis already
    lives and which the interface already renders on the presence of a reason.

    Returns (reason, detail), or (None, None) when there is nothing to say — which is
    every ordinary run and every private run that did open its input.
    """
    if not is_private or input_opened:
        return None, None
    if sealed:
        return (
            INPUT_NOT_OPENED,
            "The container never opened the dataset you attached: the one-time key "
            "ticket was never redeemed, which is what happens when the file is read "
            "with open() instead of the reader. The run exited cleanly, so its result "
            "stands — but it did not read your file. Open it with "
            "fyp_data.open_input() (the reader shipped in the image) instead of "
            "open(); that one line is the whole change.",
        )
    return (
        PRIVATE_INPUT_NOT_OPENED,
        "This job was submitted as private, but the container never opened its "
        "sealed input: the one-time key ticket was never redeemed. The run exited "
        "cleanly, so its result stands — but it did not read the file you attached. "
        "The entrypoint has to invoke the opener (fyp_open.py) for the input to "
        "reach the workload.",
    )


# --- node comeback interview -------------------------------------------------

# Battery reading counts as "critical" (a plausible power cause) below this.
_BATTERY_CRITICAL_PCT = 15


def classify_comeback(
    interview: dict,
    *,
    goodbye: bool = False,
    last_battery_pct: float | None = None,
    last_battery_charging: bool | None = None,
):
    """A returning agent hands over hard facts; name the outage cause in the locked
    PRIORITY order. Returns (cause, detail), or
    (None, None) when nothing notable happened (a plain fresh start writes no event).

    interview keys (all optional):
      failed_deliveries : [{ts, error}]  heartbeats that failed to send during the gap
      slept_ranges      : [[start, end]] wall-clock spans the agent's clock jumped
      reboot            : bool           the machine's boot time changed
      new_agent_session : bool           a new agent process (vs same process resumed)
      dirty_shutdown    : bool           the previous session left no clean marker
    """
    interview = interview or {}
    failed = interview.get("failed_deliveries") or []
    slept = interview.get("slept_ranges") or []
    reboot = bool(interview.get("reboot"))
    new_session = bool(interview.get("new_agent_session"))
    dirty = bool(interview.get("dirty_shutdown"))

    # 1) Buffered failed deliveries, no reboot, same process -> PROVABLE partition.
    if failed and not reboot and not new_session:
        span = f"{_hhmm(_min_ts(failed))}–{_hhmm(_max_ts(failed))}"
        return (
            "NETWORK_PARTITION",
            f"Alive but unreachable {span}; {len(failed)} heartbeat(s) failed to send. "
            "The machine never died — only the network link was down.",
        )

    # 2) A monotonic-clock jump -> PROVABLE sleep (the #1 cause on lab laptops).
    if slept:
        start, end = slept[0][0], slept[-1][-1]
        return (
            "SLEPT",
            f"Machine slept {_hhmm(start)}–{_hhmm(end)} — lid closed or sleep mode. It "
            "froze the agent, then resumed.",
        )

    # 3) New process, machine never rebooted -> the agent crashed, the machine is fine.
    if new_session and not reboot:
        return (
            "AGENT_CRASH",
            "The agent process restarted; the machine never rebooted (its boot time is "
            "unchanged).",
        )

    # 4) Rebooted with a clean marker (or a goodbye landed) -> a normal restart. EXACT.
    if reboot and (not dirty or goodbye):
        return ("CLEAN_SHUTDOWN", "The machine restarted cleanly (a normal shutdown or reboot).")

    # 5) Rebooted with no clean marker -> power/battery/hard crash. Only "LIKELY",
    #    refined by the last black-box battery reading.
    if reboot and dirty:
        if (
            last_battery_pct is not None
            and last_battery_pct <= _BATTERY_CRITICAL_PCT
            and last_battery_charging is False
        ):
            return (
                "POWER_LOSS_OR_CRASH",
                f"The machine rebooted after an unclean stop; the last reading was "
                f"battery {last_battery_pct}% and discharging — likely battery depletion.",
            )
        return (
            "POWER_LOSS_OR_CRASH",
            "The machine rebooted after an unclean stop — likely power loss or a hard crash.",
        )

    return (None, None)  # nothing notable — write no event


def _min_ts(items):
    return min((i.get("ts") for i in items if i.get("ts") is not None), default=None)


def _max_ts(items):
    return max((i.get("ts") for i in items if i.get("ts") is not None), default=None)
