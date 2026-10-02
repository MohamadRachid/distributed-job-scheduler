"""Agent-side black box (W5b) — the FACTS a returning agent gathers for the comeback
interview. The classification of those facts is the server's job (tested in the
container suite, app.diagnostics); here we prove the collector produces the right
facts, with injected clocks so a "sleep" is deterministic (no real sleeping).

Run from the repo root:  pytest agent/tests -q
"""

import urllib.error

from agent.blackbox import BlackBox


def _bb(tmp_path, **kw):
    kw.setdefault("boot_time", 1000.0)
    kw.setdefault("now_mono", 0.0)
    kw.setdefault("now_wall", 0.0)
    return BlackBox(str(tmp_path / "agent_state.json"), **kw)


def test_fresh_start_reports_nothing(tmp_path):
    bb = _bb(tmp_path)
    assert bb.pending_interview() is None  # a first-ever start had no prior death


def test_sleep_gap_becomes_a_slept_range(tmp_path):
    bb = _bb(tmp_path)
    # One loop tick should take ~interval seconds; a jump of 1000s = the machine froze.
    bb.touch(interval=3.0, now_mono=1000.0, now_wall=1000.0)
    interview = bb.pending_interview()
    assert interview is not None
    assert interview["slept_ranges"] == [[0.0, 1000.0]]


def test_normal_ticks_are_not_sleeps(tmp_path):
    bb = _bb(tmp_path)
    bb.touch(interval=3.0, now_mono=3.0, now_wall=3.0)
    bb.touch(interval=3.0, now_mono=6.0, now_wall=6.0)
    assert bb.pending_interview() is None  # ordinary ticks -> nothing notable


def test_failed_delivery_is_buffered(tmp_path):
    bb = _bb(tmp_path)
    bb.record_failed_delivery(urllib.error.URLError("down"), now_wall=100.0)
    bb.record_failed_delivery(urllib.error.URLError("down"), now_wall=103.0)
    interview = bb.pending_interview()
    assert len(interview["failed_deliveries"]) == 2
    assert interview["failed_deliveries"][0]["error"] == "URLError"


def test_clear_interview_stops_re_reporting(tmp_path):
    bb = _bb(tmp_path)
    bb.record_failed_delivery(urllib.error.URLError("down"), now_wall=100.0)
    assert bb.pending_interview() is not None
    bb.clear_interview()
    assert bb.pending_interview() is None  # delivered once -> not reported again


def test_dirty_agent_restart_reports_new_session(tmp_path):
    # First process runs (writes a not-clean session), then a NEW process starts on
    # the SAME boot -> agent-crash facts (new session, not rebooted, dirty).
    _bb(tmp_path)  # first session, killed hard (never marks clean)
    bb2 = _bb(tmp_path, boot_time=1000.0)
    iv = bb2.pending_interview()
    assert iv["new_agent_session"] is True
    assert iv["reboot"] is False
    assert iv["dirty_shutdown"] is True


def test_clean_agent_restart_is_benign(tmp_path):
    # A clean stop marks the session clean and sends a goodbye — a restart on the same
    # machine is then NOT worth an event (the goodbye already told the story).
    bb1 = _bb(tmp_path)
    bb1.mark_clean_shutdown()
    bb2 = _bb(tmp_path, boot_time=1000.0)
    assert bb2.pending_interview() is None


def test_reboot_after_dirty_stop_reports_reboot(tmp_path):
    _bb(tmp_path, boot_time=1000.0)             # ran, then lost power (never clean)
    bb2 = _bb(tmp_path, boot_time=2000.0)        # new boot time = a reboot
    iv = bb2.pending_interview()
    assert iv["reboot"] is True
    assert iv["dirty_shutdown"] is True
