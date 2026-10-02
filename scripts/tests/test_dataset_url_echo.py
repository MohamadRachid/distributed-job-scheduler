"""The dummy workload echoes ``DATASET_URL`` into its log, set or not.

**Why this file lives here and not in a counted suite.** ``workloads/dummy/train.py``
is a workload file, belonging to neither ``control-plane/tests`` nor
``agent/tests``; adding it to either would move a published test count into four
report sites and Table 23. Same reasoning, same place as ``test_fyp_checkpoint.py``
(the precedent of section 18, row 2026-08-29q).

**Why it runs the script rather than calling the helper.** The claim being pinned
is that the line reaches the log -- which is what the browser, the control plane and
the stored logs all end up showing. Calling ``_dataset_line()`` would prove a string
is built; running the script proves it is printed. So these tests execute the real
file in a subprocess with a temporary output folder and read its standard output.

Run from the repo root:  pytest scripts/tests/test_dataset_url_echo.py -q
"""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
from pathlib import Path

_TRAIN = Path(__file__).resolve().parents[2] / "workloads" / "dummy" / "train.py"


def _run(dataset_url: str | None) -> str:
    """One real run of the dummy workload. One epoch, no sleeping, output written
    to a temporary folder so nothing outside the test is touched."""
    env = dict(os.environ)
    env.pop("DATASET_URL", None)  # never inherit the developer's own shell
    if dataset_url is not None:
        env["DATASET_URL"] = dataset_url
    env["EPOCHS"] = "1"
    env["EPOCH_SECONDS"] = "0"
    env.pop("FAIL", None)
    with tempfile.TemporaryDirectory() as d:
        env["OUTPUT_DIR"] = d
        done = subprocess.run(
            [sys.executable, str(_TRAIN)],
            capture_output=True,
            text=True,
            encoding="utf-8",
            env=env,
            check=True,
        )
    return done.stdout


def test_the_dataset_url_the_user_typed_is_echoed_verbatim():
    """A job told where its data lives says so in its own log — which is how the
    live capture proves the value crossed browser, control plane and container."""
    url = "https://example.invalid/dataset.tar"
    out = _run(url)

    assert f"DATASET_URL={url}" in out
    # The hyperparameter echo it sits beside is unaffected.
    assert "hyperparameters: lr=0.001" in out


def test_an_absent_dataset_url_is_reported_as_not_set():
    """No dataset is a normal way to run this workload, so the line is still
    printed and says plainly that nothing was given. The platform does not invent
    a default it would then appear to have fetched."""
    out = _run(None)

    assert "DATASET_URL=(not set)" in out
    assert "DATASET_URL=http" not in out
