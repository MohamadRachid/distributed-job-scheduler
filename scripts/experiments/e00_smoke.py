"""E00 — the Phase 0 exit test (brief §4.3).

Not a comparison and not a result for the report. Its only job is to prove the
bench itself works before four people start building experiments on top of it:

  * a job goes all the way through the real loop (submit -> claim -> container ->
    SUCCEEDED) with a real agent and a real container;
  * `set_mode` genuinely restarts the control plane into a chosen arm, and the
    `/health` assertion catches it if it does not;
  * `record` / `summarize` / `chart` / `manifest` write all four output files;
  * a `dry` row never reaches the summary.

It runs TWO repetitions on the shipped defaults, then one extra repetition with
the guarantee weakened, purely to show the switch really flips and comes back.
Nothing here is evidence of anything except that the harness is trustworthy.

    .venv\\Scripts\\python.exe scripts\\experiments\\e00_smoke.py
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import harness as h  # noqa: E402

EXP = "E00"
REPS = 2


def one_rep(arm: str, rep: int, dry: bool = False) -> dict:
    """Submit one short job to one node and time it from the server's own rows."""
    job_id = h.submit_job(
        name=f"{EXP}-{arm}-{rep}",
        env={"EPOCHS": "2", "EPOCH_SECONDS": "1"},
    )
    result = h.wait_for(job_id, ("SUCCEEDED", "FAILED"), timeout_s=120)
    if not result["reached"]:
        raise h.HarnessError(f"job {job_id[:8]} did not finish in time: {result['runs']}")

    # Re-check the mode BEFORE recording: if the control plane restarted while this
    # repetition ran, the row would be labelled with an arm it cannot be proven to
    # have run under. Better to lose the repetition loudly than to keep a lie.
    h.assert_stable(where=f"{arm} rep {rep}")

    run_id = result["runs"][0]["run_id"]
    times = h.server_times(run_id)
    metrics = {
        "status": times["status"],
        "attempt": times["attempt"],
        "log_rows": times["log_rows"],
        # Server-side throughout: created_at/started_at/finished_at are all stamped
        # by the control plane when it accepts each transition.
        "queue_to_running_s": times["queue_to_running_s"],
        "running_to_finish_s": times["running_to_finish_s"],
        "total_s": times["total_s"],
    }
    return h.record(EXP, arm, rep, metrics, dry=dry)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--fresh", action="store_true",
        help="archive an existing raw.jsonl and start clean. Without it, a "
             "re-run that would append onto measured rows stops.",
    )
    args = parser.parse_args(argv)

    h.require_ready()
    h.ensure_image()
    # E00 took its own advice late: it had no argument parsing at all, so an
    # earlier `--fresh` was silently ignored and this run appended its rows onto
    # the previous run's, summarising two different code states as one table.
    # That is precisely what guard_fresh exists to stop, and the smoke test that
    # validates the harness was the one script not calling it.
    h.guard_fresh(EXP, args.fresh)

    # Start from the system we actually ship, and prove it on /health.
    h.set_mode()
    h.reset_platform()
    agents = h.start_agents(1, names=["exp-a"])
    modes_used = [h.current_mode()]

    try:
        print("\n--- a DRY repetition (must NOT reach the summary) ---")
        dry_row = one_rep("full", 0, dry=True)
        assert dry_row["dry"] is True

        print(f"\n--- {REPS} measured repetitions on the shipped defaults ---")
        for rep in range(1, REPS + 1):
            row = one_rep("full", rep)
            print(f"    rep {rep}: {row['status']} in {row['total_s']}s "
                  f"({row['log_rows']} stored log rows)")

        print("\n--- prove the switch flips: guarantee=lease_only ---")
        weak = h.set_mode(guarantee="lease_only")
        assert weak["guarantee"] == "lease_only", weak
        modes_used.append(weak)
        # The agents keep heart-beating across the restart; give them one beat to
        # reconnect before submitting, so we measure the platform and not the gap.
        time.sleep(4)
        h.wait_online(1)
        row = one_rep("lease_only", 1)
        print(f"    lease_only rep 1: {row['status']} in {row['total_s']}s")

        print("\n--- restore the shipped defaults ---")
        back = h.set_mode()
        assert back == h.EXPERIMENT_DEFAULTS, back
        print(f"    /health confirms {back}")

    finally:
        h.stop_agents(agents)
        h.set_mode()  # never leave the platform weakened, even after a failure

    h.summarize(EXP, title="E00 — harness smoke test (not a result)")
    h.chart(EXP, metric="total_s", title="E00 total run time (median, min–max)")
    h.manifest(
        EXP,
        reps=REPS,
        modes=modes_used,
        notes="Phase 0 exit test. Proves the harness, not the platform. "
              "Not for the report.",
    )

    # The exit condition the brief names: a valid summary and manifest, with the
    # dry row excluded from both.
    summary = (h.exp_dir(EXP) / "summary.md").read_text(encoding="utf-8")
    measured, total = len(h.load_raw(EXP)), len(h.load_raw(EXP, include_dry=True))
    assert "«MISSING»" not in summary, "summary has gaps — the smoke run did not measure"
    assert total - measured >= 1, "the dry row was not quarantined"
    print(f"\nPHASE 0 OK — {measured} measured rows, {total - measured} dry row(s) excluded.")
    print(f"  {h.exp_dir(EXP).relative_to(h.REPO)}: raw.jsonl, summary.md, chart.png, manifest.json")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (h.HarnessError, AssertionError) as exc:
        print(f"\nPHASE 0 FAILED: {exc}", file=sys.stderr)
        sys.exit(1)
