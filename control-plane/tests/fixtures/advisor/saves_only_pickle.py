"""Fixture: saves via pickle to the path the platform hands over, no resume.

Expected arm A verdict: saves_checkpoint.
"""
import os
import pickle


def main(model, total_epochs):
    target = os.environ["CHECKPOINT_PATH"]
    for epoch in range(1, total_epochs + 1):
        train_one_epoch(model)
        with open(target, "wb") as fh:
            pickle.dump({"epoch": epoch, "weights": model.weights}, fh)


def train_one_epoch(*args, **kwargs):
    """Stand-in for the real training step. These fixtures are SAMPLE TEXT that the
    advisor's scanner reads, never code anything runs — but they are kept valid,
    lint-clean Python so `ruff check` covers this directory like every other."""
