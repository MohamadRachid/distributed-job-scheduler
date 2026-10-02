"""Fixture: saves diligently — to somewhere the platform will never collect from.

The interesting negative. There is a real save call, but no
reference to the checkpoint location in any of its three spellings, so the state
would not survive the machine dying. The verdict has to be no_checkpoint, and the
reason has to say which half was missing.

Expected arm A verdict: no_checkpoint.
"""
import torch


def main(model, total_epochs):
    for epoch in range(1, total_epochs + 1):
        train_one_epoch(model)
        torch.save({"epoch": epoch}, "/output/model.pt")


def train_one_epoch(*args, **kwargs):
    """Stand-in for the real training step. These fixtures are SAMPLE TEXT that the
    advisor's scanner reads, never code anything runs — but they are kept valid,
    lint-clean Python so `ruff check` covers this directory like every other."""
