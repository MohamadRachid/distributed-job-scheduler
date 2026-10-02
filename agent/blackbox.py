"""The node black box (W5b) — the agent side of the comeback interview.

A dead machine cannot say why it died: at the instant of death, a pulled cable, a
closed lid, and a power cut are identical silence. So the agent does NOT guess at
death — it records facts before death and hands them over when it comes back, and
the control plane turns those facts into an honest cause (app/diagnostics).

This module keeps a tiny local session file next to the agent state, touched every
loop, and gathers three kinds of hard fact:

  * failed heartbeat deliveries (buffered while unreachable) -> a partition PROVES
    the machine was alive, only the link was down;
  * a monotonic-clock jump between loop ticks -> the machine slept (lid/sleep);
  * on a NEW process: boot time vs the previous session's, and whether the previous
    session stopped cleanly -> agent crash vs reboot vs power loss.

Everything is best-effort (psutil probes, a small JSON file) and pure enough to
test with injected clocks. The CLASSIFICATION lives server-side (the graded suite);
here we only collect and report.
"""

import json
import time
import uuid

try:
    import psutil
except Exception:  # noqa: BLE001 - psutil is a dep, but never let its import kill the agent
    psutil = None

# A loop tick is ~heartbeat interval; a gap of many × that (and this many seconds
# absolute) between ticks means the machine froze — it slept, it didn't just lag.
_SLEEP_ABS_S = 30.0
_SLEEP_MULT = 4
_MAX_FAILED = 50  # cap the buffered failed-delivery list


def _safe_boot_time():
    if psutil is None:
        return None
    try:
        return float(psutil.boot_time())
    except Exception:  # noqa: BLE001
        return None


class BlackBox:
    """Per-agent-process outage recorder. One instance lives for the whole agent
    lifetime (created in main, survives re-registration)."""

    def __init__(
        self,
        state_file: str,
        *,
        boot_time=None,
        session_id=None,
        now_mono=None,
        now_wall=None,
    ) -> None:
        self._path = state_file + ".session"
        self._boot_time = boot_time if boot_time is not None else _safe_boot_time()
        self._session_id = session_id or uuid.uuid4().hex

        prev = self._load_prev()
        self._prev_existed = prev is not None
        if prev is not None:
            prev_boot = prev.get("boot_time")
            self._reboot = (
                prev_boot is not None
                and self._boot_time is not None
                and abs(self._boot_time - prev_boot) > 5
            )
            self._dirty = not prev.get("clean", False)
            self._prev_boot = prev_boot
        else:
            self._reboot = False
            self._dirty = False
            self._prev_boot = None

        self._startup_reported = False
        self._slept: list[list[float]] = []
        self._failed: list[dict] = []
        self._last_mono = now_mono if now_mono is not None else time.monotonic()
        self._last_wall = now_wall if now_wall is not None else time.time()
        self._save(clean=False)  # a running session is "not clean" until it says goodbye

    # --- session file ------------------------------------------------------

    def _load_prev(self):
        try:
            with open(self._path, encoding="utf-8") as f:
                return json.load(f)
        except (OSError, ValueError):
            return None

    def _save(self, *, clean: bool) -> None:
        try:
            with open(self._path, "w", encoding="utf-8") as f:
                json.dump(
                    {
                        "session_id": self._session_id,
                        "boot_time": self._boot_time,
                        "clean": clean,
                        "last_touch": self._last_wall,
                    },
                    f,
                )
        except OSError:
            pass  # best-effort; a missing black box just means fewer facts, never a crash

    # --- per-loop recording ------------------------------------------------

    def touch(self, *, interval: float = 3.0, now_mono=None, now_wall=None) -> None:
        """Call once per loop. Detects a sleep as a jump far larger than one tick."""
        m = now_mono if now_mono is not None else time.monotonic()
        w = now_wall if now_wall is not None else time.time()
        gap = max(m - self._last_mono, w - self._last_wall)
        if gap > max(_SLEEP_ABS_S, interval * _SLEEP_MULT):
            self._slept.append([round(self._last_wall, 3), round(w, 3)])
        self._last_mono, self._last_wall = m, w
        self._save(clean=False)

    def record_failed_delivery(self, error, *, now_wall=None) -> None:
        """A heartbeat that could not be sent — evidence of a live-but-unreachable gap."""
        w = now_wall if now_wall is not None else time.time()
        name = type(error).__name__ if isinstance(error, BaseException) else str(error)
        self._failed.append({"ts": round(w, 3), "error": name})
        if len(self._failed) > _MAX_FAILED:
            self._failed = self._failed[-_MAX_FAILED:]

    # --- the interview -----------------------------------------------------

    def pending_interview(self):
        """The facts to send on the next contact, or None when nothing is notable.

        Startup facts (reboot / agent-crash) are reported once, and ONLY when the
        previous session ended abnormally (a reboot, or a dirty stop). A clean agent
        restart on an un-rebooted machine is benign — the goodbye already covered it,
        so we stay silent rather than cry AGENT_CRASH."""
        report: dict = {}
        if (
            self._prev_existed
            and not self._startup_reported
            and (self._reboot or self._dirty)
        ):
            report.update(
                {
                    "reboot": self._reboot,
                    "new_agent_session": True,
                    "dirty_shutdown": self._dirty,
                    "boot_time": self._boot_time,
                    "prev_boot_time": self._prev_boot,
                }
            )
        if self._slept:
            report["slept_ranges"] = list(self._slept)
        if self._failed:
            report["failed_deliveries"] = list(self._failed)
        return report or None

    def clear_interview(self) -> None:
        """Called after the interview was delivered (a 200) — don't report it twice."""
        self._startup_reported = True
        self._slept = []
        self._failed = []

    def mark_clean_shutdown(self) -> None:
        """Record that this session stopped on purpose, so the next start sees a clean
        marker (not a dirty one) — the twin of the goodbye message."""
        self._save(clean=True)
