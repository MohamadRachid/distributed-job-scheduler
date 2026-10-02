"""The lease default lives in TWO places that must agree. This is the check.

WHY TWO PLACES AT ALL. A compose file cannot import Python, so the number is
declared in control-plane/app/config.py and again as the interpolation fallback
in docker-compose.yml. That is not a design we chose; it is the honest floor.
What we can choose is whether the agreement is enforced by a comment in each
file (discipline) or by something that fails (code). Safety in code, not
discipline.

WHY THE CHECK IS HERE AND NOT IN THE CANONICAL SUITE. The canonical container
mounts only ./control-plane at /app, so docker-compose.yml does not exist inside
it -- verified, not assumed. A test there can see config.py and can never see the
compose fallback. The canonical suite therefore carries the half it CAN see
(test_log_tiering.py::test_settings_are_the_ones_the_app_actually_runs_with, one
added assertion inside an existing test, so the suite count does not move) and
this file carries the half it cannot.

WHY THIS FILE COSTS NOTHING DOWNSTREAM. scripts/tests/ is in neither counted
suite: control-plane/pytest.ini sets `testpaths = tests`, and only the
control-plane and agent counts are published. So no published count moves because
this file exists. That is the precedent scripts/tests/test_clean_machine_gate.py and
test_stage_demo_schema_check.py already set, for the same reason.

WHICH LAYER ACTUALLY DECIDES, AND WHY THAT MAKES THIS THE IMPORTANT HALF.
docker-compose.yml is what every real run reads -- the demonstration, the
partition test, every experiment, every live proof. Before 2026-08-29 it set
LEASE_TTL_S: "15" outright and therefore BEAT config.py's default for all of
them; changing config.py alone would have changed nothing observable while
reading as done. The compose value is the one that decides, so it is the one
that most needs a check behind it.

Run me:  python -m pytest scripts/tests/test_lease_default_agreement.py -q
"""

import ast
import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
COMPOSE = REPO / "docker-compose.yml"
CONFIG = REPO / "control-plane" / "app" / "config.py"

# `LEASE_TTL_S: "${LEASE_TTL_S:-60}"` -- capture the fallback digits only.
# Matched on the shape rather than the whole line so a reformat or a comment
# change does not break it, and a change to the NUMBER does.
FALLBACK = re.compile(
    r"""LEASE_TTL_S\s*:\s*["']?\$\{\s*LEASE_TTL_S\s*:-\s*(?P<value>\d+)\s*\}""",
)


def _compose_fallback() -> int:
    """The 60 in `${LEASE_TTL_S:-60}`, read out of docker-compose.yml.

    Read as text and not with a YAML parser on purpose: a parser resolves the
    interpolation against the environment, which would return whatever the shell
    happens to export and tell us nothing about what the file says. We want the
    file's own literal.
    """
    if not COMPOSE.exists():
        pytest.fail(
            "docker-compose.yml is missing at %s. This check cannot read its input, "
            "so it fails rather than passing quietly." % COMPOSE
        )
    hits = FALLBACK.findall(COMPOSE.read_text(encoding="utf-8"))
    if len(hits) != 1:
        pytest.fail(
            "expected exactly one `${LEASE_TTL_S:-N}` interpolation in "
            "docker-compose.yml, found %d. Either the fallback was removed (the "
            "value is then whatever the environment says, with no default of our "
            "own) or a second one appeared (two defaults, and nothing says which "
            "wins)." % len(hits)
        )
    return int(hits[0])


def _declared_default() -> int:
    """`lease_ttl_s: int = N` from config.py, read with the AST rather than imported.

    Importing would need the control-plane dependencies on the host and would run
    module code; parsing needs neither and cannot be fooled by an environment
    variable, which is exactly the confusion this check exists to prevent.
    """
    if not CONFIG.exists():
        pytest.fail("control-plane/app/config.py is missing at %s" % CONFIG)
    tree = ast.parse(CONFIG.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            if node.target.id == "lease_ttl_s" and isinstance(node.value, ast.Constant):
                return int(node.value.value)
    pytest.fail(
        "no `lease_ttl_s: int = N` declaration found in config.py. If the field was "
        "renamed, this check must be renamed with it rather than deleted."
    )


def test_the_compose_fallback_and_the_config_default_are_the_same_number():
    """The one thing a comment in each file could only ask for politely."""
    compose = _compose_fallback()
    declared = _declared_default()
    assert compose == declared, (
        "docker-compose.yml falls back to %ds while control-plane/app/config.py "
        "declares %ds. Compose is what every real run reads, so the stack would "
        "run at %ds while the code, the tests and the documentation all describe "
        "%ds." % (compose, declared, compose, declared)
    )


def test_the_agreed_number_is_the_ruled_value():
    """Pinned separately from the agreement above, because two files can agree
    perfectly on a value nobody ruled. 60s was ruled on 2026-08-29; the
    demonstration's 15s is set by scripts/stage_demo.ps1 and is
    an override, never a second default."""
    assert _declared_default() == 60


def test_the_demo_override_is_not_a_second_default():
    """The demo gets 15s by SETTING the variable, not by editing either default.

    If stage_demo.ps1 ever stopped setting it, the demonstration would silently
    run at 60 and the recovery beat would take a minute at the climax -- which is
    the hazard that put the 15 in the script as a constant rather than in shell
    state in the first place.
    """
    script = REPO / "scripts" / "stage_demo.ps1"
    if not script.exists():
        pytest.fail("scripts/stage_demo.ps1 is missing at %s" % script)
    text = script.read_text(encoding="utf-8")
    assert re.search(r"\$env:LEASE_TTL_S\s*=", text), (
        "stage_demo.ps1 no longer sets LEASE_TTL_S for the stack it brings up, so "
        "the demonstration would run at the shipped default."
    )
    assert re.search(r"DemoLeaseTtlS\s*=\s*15\b", text), (
        "the demonstration's lease value is no longer 15 in stage_demo.ps1. If that "
        "was deliberate, the runbook's timings and its READY-banner check move with it."
    )
