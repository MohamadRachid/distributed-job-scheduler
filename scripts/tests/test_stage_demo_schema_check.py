"""The two refusal branches of the demo staging script's schema check, fired for real.

WHAT THIS IS FOR
----------------
`scripts/stage_demo.ps1` refuses to wipe the demo database unless the tables it
is about to truncate match the tables that actually exist. That check has two
branches which say "I could not read my own input" rather than passing quietly:

  (a) psql returned a non-zero exit code  -> "could not read the table list"
  (b) psql returned zero rows             -> "found NO tables in schema 'public'"

The project notes recorded both as implemented, reviewed, and NEVER SEEN TO
FIRE - coverage claimed by reading rather than by execution. This file closes
that gap. Both branches are driven with real inputs and asserted on the refusal
an operator would actually see.

WHY THEY WERE UNREACHABLE THROUGH A NORMAL RUN
----------------------------------------------
Two earlier gates stand in front of them, not one. The obvious one is the control
plane's health gate; there is also a second, tighter one. Immediately above the
schema check, phase 3 runs `SELECT current_database() ...` through the SAME
`Invoke-Psql`, so any failure that breaks psql wholesale is refused THERE, by a
different branch, before the schema check is ever called. To reach these two you
need a database that answers one query and not the other - which is why nobody
had seen them fire.

HOW THIS FIRES THEM WITHOUT TOUCHING THE DEMO DATABASE
-------------------------------------------------------
Not by stubbing psql. The check's own functions are lifted verbatim out of the
live `stage_demo.ps1` with the PowerShell parser - never a copy pasted in here -
and run against real docker with exactly ONE seam moved: which container
`Invoke-Psql` is pointed at.

  (a) point it at a container id that does not exist. `docker exec` really fails,
      `Invoke-Native` really records a non-zero exit code, and the check really
      refuses. Nothing is faked and no database is touched.
  (b) point it at a throwaway `postgres:16` started by this test, with the same
      user and database name the script expects. A freshly initialised postgres
      has an empty `public` schema, so psql really returns zero rows with exit 0.

The live demo database is never read and never written by this file.

BOTH DIRECTIONS
---------------
A test that only ever sees a refusal cannot tell a working branch from a broken
one. So each branch is also proven in the negative: it is deleted from a COPY of
the script - cut at its exact parser offsets - and the same scenario re-run. The
branch's refusal disappears and a WRONG one takes its place: docker's error text
read as a list of table names, and an empty database misdiagnosed as a stale wipe
list. Those misdiagnoses are what the branches exist to prevent. A positive
control runs alongside, so the rig is not wired to refuse whatever it sees.

WHERE THIS LIVES, AND WHY NOT IN A COUNTED SUITE
------------------------------------------------
Deliberately outside both. The control-plane suite runs inside a pinned 3.12
container that mounts only `control-plane/`, and has neither PowerShell nor a
docker socket. The agent suite is host-side, but CI runs it on ubuntu where this
would skip - and `agent/tests` reading "83 passed" with NO skipped clause is the
signal the published test counts lean on. Adding a
Linux-skipping test there would change that reading, for a script that is neither
the agent nor the control plane. So it sits here, and the two counted numbers do
not move.

EVERY ARGUMENT TRAVELS BY ENVIRONMENT, NOT ON THE COMMAND LINE
---------------------------------------------------------------
The same lesson `stage_demo.ps1` records about its own Python calls. Two of the
values below would be mangled otherwise: `$WIPE_TABLES` starts with a `$` and
PowerShell would expand it to nothing, and `$r.ExitCode -ne 0` carries both a `$`
and spaces. Passing them through the environment means they arrive verbatim.

    Run it:  .venv\\Scripts\\python.exe -m pytest scripts/tests -q
    Needs:   Windows PowerShell 5.1, and a running Docker engine.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import time
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
STAGE_DEMO = REPO_ROOT / "scripts" / "stage_demo.ps1"

# The function under test, plus everything it calls. Lifted from the live script,
# never copied into this file: if one of these is renamed or removed, extraction
# fails loudly instead of testing a stale copy of the logic.
NEEDED_FUNCTIONS = [
    "Ok",
    "Warn",
    "Say",
    "Die",
    "Get-Tail",
    "Get-Lines",
    "Invoke-Native",
    "Invoke-Psql",
    "Test-WipeListMatchesSchema",
]
NEEDED_VARIABLES = ["$WIPE_TABLES", "$KEEP_TABLES"]

# The two conditions this file exists to execute, exactly as they read in the
# script. Matched against the parsed syntax tree, so a reformat that changes the
# spacing fails the test rather than silently matching nothing.
COND_PSQL_FAILED = "$r.ExitCode -ne 0"
COND_ZERO_ROWS = "$actual.Count -eq 0"

# A container id that cannot exist. Used to make `docker exec` fail for real.
NO_SUCH_CONTAINER = "fyp-schema-check-no-such-container"

THROWAWAY_NAME = "fyp-schemacheck-test-pg"
THROWAWAY_IMAGE = "postgres:16"

# --- PowerShell helpers, written to a temp dir at run time -------------------

# Pulls the real definitions out of a script with the PowerShell parser. Given a
# mutated copy it pulls that copy's definitions, which is what the deletion
# proofs rely on.
EXTRACT_PS1 = r"""
$ErrorActionPreference = 'Stop'
$Script    = $env:FYP_T_SCRIPT
$Out       = $env:FYP_T_OUT
$Functions = $env:FYP_T_FUNCTIONS -split ','
$Variables = $env:FYP_T_VARIABLES -split ','

$tokens = $null; $errors = $null
$ast = [System.Management.Automation.Language.Parser]::ParseFile($Script, [ref]$tokens, [ref]$errors)
if ($errors.Count -gt 0) { throw "PowerShell parse errors in ${Script}: $($errors.Count)" }

$parts = @()
$fns = $ast.FindAll({ param($n) $n -is [System.Management.Automation.Language.FunctionDefinitionAst] }, $true)
foreach ($name in $Functions) {
  $f = $fns | Where-Object { $_.Name -eq $name } | Select-Object -First 1
  if (-not $f) { throw "function not found in ${Script}: $name" }
  $parts += $f.Extent.Text
}
$asgs = $ast.FindAll({ param($n) $n -is [System.Management.Automation.Language.AssignmentStatementAst] }, $true)
foreach ($v in $Variables) {
  $a = $asgs | Where-Object { $_.Left.Extent.Text -eq $v } | Select-Object -First 1
  if (-not $a) { throw "variable not found in ${Script}: $v" }
  $parts += $a.Extent.Text
}
Set-Content -LiteralPath $Out -Value ($parts -join [Environment]::NewLine) -Encoding UTF8
Write-Host ("extracted " + $parts.Count + " definitions")
"""

# Writes a copy of the script with one if-statement removed, located inside a
# named function by its condition text and cut out at its exact parser offsets.
# The condition is searched for INSIDE that function only: `$r.ExitCode -ne 0`
# also appears elsewhere in the script, and a whole-file match would cut the
# wrong one.
MUTATE_PS1 = r"""
$ErrorActionPreference = 'Stop'
$Script    = $env:FYP_T_SCRIPT
$Out       = $env:FYP_T_OUT
$Function  = $env:FYP_T_FUNCTION
$Condition = $env:FYP_T_CONDITION

$tokens = $null; $errors = $null
$ast = [System.Management.Automation.Language.Parser]::ParseFile($Script, [ref]$tokens, [ref]$errors)
if ($errors.Count -gt 0) { throw "PowerShell parse errors in ${Script}: $($errors.Count)" }
$f = $ast.FindAll({ param($n) $n -is [System.Management.Automation.Language.FunctionDefinitionAst] }, $true) |
     Where-Object { $_.Name -eq $Function } | Select-Object -First 1
if (-not $f) { throw "function not found: $Function" }
$ifs = @($f.FindAll({ param($n) $n -is [System.Management.Automation.Language.IfStatementAst] }, $true) |
         Where-Object { $_.Clauses[0].Item1.Extent.Text -eq $Condition })
if ($ifs.Count -ne 1) {
  throw "expected exactly one if-statement with condition '$Condition' inside ${Function}, found $($ifs.Count)"
}
$text = [IO.File]::ReadAllText($Script)
$cut = $text.Substring(0, $ifs[0].Extent.StartOffset) + $text.Substring($ifs[0].Extent.EndOffset)
[IO.File]::WriteAllText($Out, $cut)
Write-Host ("removed " + ($ifs[0].Extent.EndOffset - $ifs[0].Extent.StartOffset) + " characters")
"""

# Runs the extracted check under the script's own runtime conditions: strict mode
# on and $ErrorActionPreference at 'Stop', because Get-Lines' own comment warns
# that strict mode is exactly what makes its array handling matter.
HARNESS_PS1 = r"""
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
. $env:FYP_T_EXTRACTED
$DbUser = 'fyp'
$DbName = 'fyp'
$PgContainer = $env:FYP_T_CONTAINER
Test-WipeListMatchesSchema
Write-Host 'CHECK-RETURNED-WITHOUT-REFUSING'
"""

# Reports the two table lists as JSON, so the positive control builds its schema
# from the script's own lists. There is exactly one parser of PowerShell here,
# and it is PowerShell's.
EMIT_LISTS_PS1 = r"""
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
. $env:FYP_T_EXTRACTED
[pscustomobject]@{ wipe = @($WIPE_TABLES); keep = @($KEEP_TABLES) } | ConvertTo-Json -Compress
"""


# --- plumbing ---------------------------------------------------------------


def _powershell():
    return shutil.which("powershell") or shutil.which("powershell.exe")


def _docker_up() -> bool:
    exe = shutil.which("docker")
    if not exe:
        return False
    try:
        done = subprocess.run(
            [exe, "info", "--format", "{{.ServerVersion}}"],
            capture_output=True,
            text=True,
            timeout=60,
        )
    except (OSError, subprocess.SubprocessError):
        return False
    return done.returncode == 0


pytestmark = [
    pytest.mark.skipif(
        _powershell() is None,
        reason="Windows PowerShell is required: the thing under test is a .ps1 script",
    ),
    pytest.mark.skipif(
        not _docker_up(),
        reason="a running Docker engine is required: both branches fire against real docker",
    ),
]


def _run_ps1(body_path: Path, **env_values: str) -> subprocess.CompletedProcess:
    """Run one helper. Everything it needs arrives in the environment."""
    env = dict(os.environ)
    env.update({f"FYP_T_{k.upper()}": v for k, v in env_values.items()})
    return subprocess.run(
        [
            _powershell(),
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(body_path),
        ],
        capture_output=True,
        text=True,
        timeout=180,
        env=env,
    )


def _docker(*args: str, check: bool = True) -> subprocess.CompletedProcess:
    done = subprocess.run(
        [shutil.which("docker"), *args], capture_output=True, text=True, timeout=180
    )
    if check and done.returncode != 0:
        raise RuntimeError(f"docker {' '.join(args)} failed: {done.stderr.strip()}")
    return done


@pytest.fixture(scope="module")
def helpers(tmp_path_factory):
    """The PowerShell helpers, on disk in a temp directory."""
    directory = tmp_path_factory.mktemp("stage_demo_schema_check")
    written = {"dir": directory}
    for name, body in (
        ("extract.ps1", EXTRACT_PS1),
        ("mutate.ps1", MUTATE_PS1),
        ("harness.ps1", HARNESS_PS1),
        ("emit_lists.ps1", EMIT_LISTS_PS1),
    ):
        path = directory / name
        path.write_text(body, encoding="utf-8")
        written[name] = path
    return written


def _extract(helpers, source: Path, out_name: str) -> Path:
    """Lift the real check and everything it calls out of `source`."""
    out = helpers["dir"] / out_name
    done = _run_ps1(
        helpers["extract.ps1"],
        script=str(source),
        out=str(out),
        functions=",".join(NEEDED_FUNCTIONS),
        variables=",".join(NEEDED_VARIABLES),
    )
    assert done.returncode == 0, f"extraction failed:\n{done.stdout}\n{done.stderr}"
    assert out.exists(), "extraction reported success but wrote nothing"
    return out


def _check(helpers, extracted: Path, container: str) -> subprocess.CompletedProcess:
    """Run the extracted schema check against one container id."""
    return _run_ps1(helpers["harness.ps1"], extracted=str(extracted), container=container)


@pytest.fixture(scope="module")
def live_check(helpers) -> Path:
    """The check exactly as it stands in the committed script."""
    assert STAGE_DEMO.exists(), f"the script under test is missing: {STAGE_DEMO}"
    return _extract(helpers, STAGE_DEMO, "live.ps1")


@pytest.fixture(scope="module")
def table_lists(helpers, live_check):
    """The script's own WIPE and KEEP lists, read back out of the extraction."""
    done = _run_ps1(helpers["emit_lists.ps1"], extracted=str(live_check))
    assert done.returncode == 0, f"could not read the table lists:\n{done.stdout}\n{done.stderr}"
    lists = json.loads(done.stdout.strip())
    assert lists["wipe"], "the script's WIPE list came back empty"
    assert lists["keep"], "the script's KEEP list came back empty"
    return lists


@pytest.fixture(scope="module")
def empty_postgres():
    """A throwaway postgres, started by this test and removed after it.

    Started with `docker run`, so it carries none of the compose project's labels
    and `docker compose ps` cannot see it. The demo database is not involved.
    """
    _docker("rm", "-f", THROWAWAY_NAME, check=False)
    _docker(
        "run",
        "-d",
        "--name",
        THROWAWAY_NAME,
        "-e",
        "POSTGRES_USER=fyp",
        "-e",
        "POSTGRES_PASSWORD=fyp",
        "-e",
        "POSTGRES_DB=fyp",
        THROWAWAY_IMAGE,
    )
    try:
        # pg_isready answers during initialisation, while the real socket is not
        # up yet - it reported ready here and psql then failed to connect. So wait
        # on the query we actually need rather than on a readiness probe.
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            probe = _docker(
                "exec", "-i", THROWAWAY_NAME,
                "psql", "-U", "fyp", "-d", "fyp", "-t", "-A", "-c", "SELECT 1;",
                check=False,
            )
            if probe.returncode == 0:
                break
            time.sleep(1)
        else:
            pytest.fail(f"the throwaway {THROWAWAY_IMAGE} never began answering queries")
        yield THROWAWAY_NAME
    finally:
        _docker("rm", "-f", THROWAWAY_NAME, check=False)


def _tables_in(container: str) -> list:
    done = _docker(
        "exec", "-i", container, "psql", "-U", "fyp", "-d", "fyp", "-t", "-A",
        "-c",
        "SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename;",
    )
    return [line.strip() for line in done.stdout.splitlines() if line.strip()]


# --- the two branches, fired ------------------------------------------------


def test_branch_a_fires_when_psql_really_fails(helpers, live_check):
    """psql exits non-zero -> the check refuses instead of guessing.

    Real `docker exec` against a container that does not exist. The exit code is
    docker's own, not one this test made up.
    """
    done = _check(helpers, live_check, NO_SUCH_CONTAINER)

    assert done.returncode == 1, (
        "the check should have refused and exited 1, but exited "
        f"{done.returncode}:\n{done.stdout}\n{done.stderr}"
    )
    assert "could not read the table list" in done.stdout, (
        f"branch (a) did not fire. Output was:\n{done.stdout}"
    )
    assert "It refuses to guess" in done.stdout
    # The operator is handed what the failing call actually said, not merely the
    # news that it failed.
    assert "psql said:" in done.stdout
    assert "Error response" in done.stdout, (
        "the refusal should carry docker's own words, not a generic message:\n"
        + done.stdout
    )
    assert "CHECK-RETURNED-WITHOUT-REFUSING" not in done.stdout

    # OBSERVED, and reported rather than worked around. `Get-Tail ... 4` keeps the
    # LAST four lines, and on this failure the useful one - docker's plain
    # "No such container: <id>" - is the FIRST, so it is the line that gets
    # omitted. What survives is a PowerShell stack fragment plus a CategoryInfo
    # that truncates the container name to "...er-deliberately". The branch fires
    # and refuses correctly; only the four lines it chooses to show are the least
    # informative four. Asserted here so the observation is a fact on the record
    # and not a memory.
    assert "earlier lines omitted" in done.stdout


def test_branch_b_fires_when_psql_returns_no_rows(helpers, live_check, empty_postgres):
    """psql exits 0 with zero rows -> the check refuses instead of passing.

    A freshly initialised postgres really does have an empty `public` schema, so
    this is the branch's real input rather than a simulation of it.
    """
    assert _tables_in(empty_postgres) == [], (
        "the throwaway postgres was expected to be empty before this test"
    )

    done = _check(helpers, live_check, empty_postgres)

    assert done.returncode == 1, (
        "the check should have refused and exited 1, but exited "
        f"{done.returncode}:\n{done.stdout}\n{done.stderr}"
    )
    assert "found NO tables in schema" in done.stdout, (
        f"branch (b) did not fire. Output was:\n{done.stdout}"
    )
    assert "Refusing to continue" in done.stdout
    assert "CHECK-RETURNED-WITHOUT-REFUSING" not in done.stdout


def test_the_check_passes_when_the_schema_is_right(
    helpers, live_check, table_lists, empty_postgres
):
    """The positive control: this rig is not wired to refuse whatever it sees.

    Builds the schema from the script's OWN lists, so it cannot drift from them.
    """
    expected = list(table_lists["wipe"]) + list(table_lists["keep"])
    for table in expected:
        _docker(
            "exec", "-i", empty_postgres,
            "psql", "-U", "fyp", "-d", "fyp", "-q", "-v", "ON_ERROR_STOP=1",
            "-c", f'CREATE TABLE IF NOT EXISTS "{table}" (id int);',
        )
    try:
        assert sorted(_tables_in(empty_postgres)) == sorted(expected)

        done = _check(helpers, live_check, empty_postgres)

        assert done.returncode == 0, (
            f"the check refused a schema that matches its own lists:\n{done.stdout}"
        )
        assert "all accounted for" in done.stdout
        assert f"{len(table_lists['wipe'])} wiped" in done.stdout
        assert f"{len(table_lists['keep'])} preserved" in done.stdout
        assert "CHECK-RETURNED-WITHOUT-REFUSING" in done.stdout
    finally:
        for table in expected:
            _docker(
                "exec", "-i", empty_postgres,
                "psql", "-U", "fyp", "-d", "fyp", "-q",
                "-c", f'DROP TABLE IF EXISTS "{table}";',
                check=False,
            )


# --- the other direction: delete the branch, watch the refusal disappear -----


def _without_branch(helpers, condition: str, out_name: str) -> Path:
    """A copy of the script with one branch cut out at its parser offsets."""
    cut = helpers["dir"] / out_name
    done = _run_ps1(
        helpers["mutate.ps1"],
        script=str(STAGE_DEMO),
        out=str(cut),
        function="Test-WipeListMatchesSchema",
        condition=condition,
    )
    assert done.returncode == 0, f"could not remove the branch:\n{done.stdout}\n{done.stderr}"
    assert cut.stat().st_size < STAGE_DEMO.stat().st_size, (
        "the copy is not smaller than the original, so nothing was cut"
    )
    return cut


def test_branch_a_removed_and_the_refusal_is_gone(helpers):
    """Without branch (a), docker's error text is read as a list of table names.

    The check does not merely stop refusing - it refuses for the WRONG reason,
    reporting an error message as schema drift. That misdiagnosis is what the
    branch exists to prevent.
    """
    cut = _without_branch(helpers, COND_PSQL_FAILED, "no_branch_a.ps1")
    extracted = _extract(helpers, cut, "extracted_no_a.ps1")

    done = _check(helpers, extracted, NO_SUCH_CONTAINER)

    assert "could not read the table list" not in done.stdout, (
        "branch (a) was removed and its refusal still appeared, so the assertion "
        "in the positive test is matching something else:\n" + done.stdout
    )
    assert "SCHEMA DRIFT" in done.stdout, (
        f"expected the misdiagnosis the branch prevents. Output was:\n{done.stdout}"
    )
    assert "No such container" in done.stdout, (
        "docker's error text should now be sitting in the list of unknown tables"
    )


def test_branch_b_removed_and_the_refusal_is_gone(helpers, empty_postgres):
    """Without branch (b), an empty database is misdiagnosed as a stale wipe list."""
    assert _tables_in(empty_postgres) == [], (
        "the throwaway postgres was expected to be empty before this test"
    )
    cut = _without_branch(helpers, COND_ZERO_ROWS, "no_branch_b.ps1")
    extracted = _extract(helpers, cut, "extracted_no_b.ps1")

    done = _check(helpers, extracted, empty_postgres)

    assert "found NO tables in schema" not in done.stdout, (
        "branch (b) was removed and its refusal still appeared, so the assertion "
        "in the positive test is matching something else:\n" + done.stdout
    )
    assert "STALE WIPE LIST" in done.stdout, (
        f"expected the misdiagnosis the branch prevents. Output was:\n{done.stdout}"
    )


# --- the extraction itself has to stay trustworthy --------------------------


def test_the_live_script_still_contains_both_branches(helpers):
    """If either branch is renamed or removed for real, say so here, not silently.

    Everything above reads the live script. This asserts the two conditions are
    still there to be found, so a future edit that removes one fails loudly
    instead of leaving four tests quietly exercising nothing.
    """
    for condition, name in (
        (COND_PSQL_FAILED, "psql-failed"),
        (COND_ZERO_ROWS, "zero-rows"),
    ):
        cut = _without_branch(helpers, condition, f"probe_{name}.ps1")
        assert cut.exists()
