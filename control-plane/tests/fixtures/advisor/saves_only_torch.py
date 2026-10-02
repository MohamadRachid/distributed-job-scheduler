"""Fixture: saves to the checkpoint location but never reads it back.

A re-dispatched run would start its training from the beginning, which is exactly
what the advice is for. No load token appears anywhere in this file.

Expected arm A verdict: saves_checkpoint.
"""
import torch


def main(model, total_epochs):
    for epoch in range(1, total_epochs + 1):
        train_one_epoch(model)
        torch.save({"epoch": epoch, "model": model.state_dict()}, "/checkpoint/state")


def train_one_epoch(*args, **kwargs):
    """Stand-in for the real training step. These fixtures are SAMPLE TEXT that the
    advisor's scanner reads, never code anything runs — but they are kept valid,
    lint-clean Python so `ruff check` covers this directory like every other."""
