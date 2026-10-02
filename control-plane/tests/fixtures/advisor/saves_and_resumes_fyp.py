"""Fixture: saves AND resumes through the platform's own adoption contract.

This is the shape `workloads/dummy/fyp_checkpoint.py` documents and the shape both
of this repository's reference workloads actually use. It never writes the mount
path literally: that path arrives in the environment and the helper reads it, so a
workload never hard-codes it.

Expected arm A verdict: resumes. Under the brief's original rule it would have been
no_checkpoint, which is the false negative that widened the token set.
"""
import fyp_checkpoint


def main(model, total_epochs):
    state = fyp_checkpoint.load() or {"epoch": 0}
    for epoch in range(state["epoch"] + 1, total_epochs + 1):
        train_one_epoch(model)
        fyp_checkpoint.save({"epoch": epoch})


def train_one_epoch(*args, **kwargs):
    """Stand-in for the real training step. These fixtures are SAMPLE TEXT that the
    advisor's scanner reads, never code anything runs — but they are kept valid,
    lint-clean Python so `ruff check` covers this directory like every other."""
