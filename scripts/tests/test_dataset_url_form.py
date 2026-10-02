"""The Dataset URL box on the submit form really becomes ``env.DATASET_URL``.

**Why this file lives here and not in a counted suite.** It exercises a web file,
so it belongs to neither ``control-plane/tests`` nor ``agent/tests``, and adding it
to either would move a published test count into four report sites and Table 23 --
the same reason ``test_fyp_checkpoint.py`` sits here (the precedent of section 18,
row 2026-08-29q).

**Why it drives node and not a browser.** The web app has no test runner:
``web/package.json`` declares ``dev``, ``build`` and ``preview`` and nothing else,
and CI verifies the front end with ``npm run build`` alone. Adding vitest would
mean editing ``web/package.json`` and its lock file. So this test does what the
entrypoint-chain proof of 2026-08-30 did: it takes the REAL function out of
``web/src/components/SubmitForm.jsx`` and runs it under node. Nothing is
reimplemented here -- if the source changes, this test runs the changed source.

What it does NOT cover is the wiring between the input box and ``buildEnv`` inside
the React component. That wiring is one function call, it is covered by
``npm run build``, and the live capture in
``docs/evidence/dataset_url_field_2026-09-03.txt`` shows the value arriving in a
real container's stored logs.

Run from the repo root:  pytest scripts/tests/test_dataset_url_form.py -q
"""

from __future__ import annotations

import json
import shutil
import subprocess
import tempfile
from pathlib import Path

import pytest

_SUBMIT_FORM = Path(__file__).resolve().parents[2] / "web" / "src" / "components" / "SubmitForm.jsx"

# The form's own defaults, so what is exercised is the shape the app really sends.
_BASE = {
    "epochs": 5,
    "lr": "0.001",
    "batchSize": "32",
    "optimizer": "adam",
    "seed": "42",
    "extra": [],
    "fail": False,
}


def _extract(name: str) -> str:
    """Lift one exported function out of the .jsx file as text.

    The parameter list is matched first, on its parentheses, because this function
    destructures its argument -- so the first brace in the file after the name is
    the parameter object's, not the body's, and starting there would return the
    signature alone.

    The file is an ES module that imports React, so node cannot simply import it;
    taking the one pure function out is what the 2026-08-30 entrypoint capture did,
    and it keeps the test honest -- what runs below is the shipped text."""
    src = _SUBMIT_FORM.read_text(encoding="utf-8")
    marker = f"export function {name}("
    assert src.count(marker) == 1, f"expected exactly one {marker!r} in {_SUBMIT_FORM}"
    start = src.index(marker)

    # 1. walk the parameter list to its closing parenthesis
    i = start + len(marker) - 1
    depth = 0
    while i < len(src):
        if src[i] == "(":
            depth += 1
        elif src[i] == ")":
            depth -= 1
            if depth == 0:
                break
        i += 1
    else:
        raise AssertionError(f"unbalanced parentheses while extracting {name}")

    # 2. the body starts at the next brace; walk it to its match
    depth = 0
    for j in range(src.index("{", i), len(src)):
        if src[j] == "{":
            depth += 1
        elif src[j] == "}":
            depth -= 1
            if depth == 0:
                return src[start + len("export ") : j + 1]
    raise AssertionError(f"unbalanced braces while extracting {name}")


def _build_env(**overrides) -> dict:
    """Run the real buildEnv under node and return the object it produced."""
    if shutil.which("node") is None:
        pytest.skip("node is not on PATH")
    args = {**_BASE, **overrides}
    script = (
        _extract("buildEnv")
        + "\nconsole.log(JSON.stringify(buildEnv("
        + json.dumps(args)
        + ")));\n"
    )
    with tempfile.TemporaryDirectory() as d:
        path = Path(d) / "build_env_undertest.mjs"
        path.write_text(script, encoding="utf-8")
        out = subprocess.run(
            ["node", str(path)], capture_output=True, text=True, encoding="utf-8", check=True
        )
    return json.loads(out.stdout)


def test_a_filled_dataset_url_is_posted_as_DATASET_URL():
    """The load-bearing direction: what the user typed reaches the job's
    environment under the name the container reads."""
    url = "https://example.invalid/dataset.tar"
    env = _build_env(datasetUrl=url)

    assert env["DATASET_URL"] == url
    # The rest of the environment is untouched by the new box.
    assert env["EPOCHS"] == "5"
    assert env["LR"] == "0.001"
    assert env["BATCH_SIZE"] == "32"
    assert env["OPTIMIZER"] == "adam"
    assert env["SEED"] == "42"


def test_an_empty_dataset_url_sends_no_DATASET_URL_key_at_all():
    """The other direction, and the reason the field is optional: absent, not an
    empty string. A script asking "was I given a dataset" must be able to hear no."""
    env = _build_env(datasetUrl="")

    assert "DATASET_URL" not in env
    # Whitespace alone is an empty box, not a dataset.
    assert "DATASET_URL" not in _build_env(datasetUrl="   ")
