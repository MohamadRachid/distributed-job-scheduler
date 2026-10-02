"""Fixture: saves AND resumes, written the explicit way — the literal mount path.

Expected arm A verdict: resumes.
"""
import os
import torch

CKPT = "/checkpoint/state"


def main(model, optimizer, total_epochs):
    start = 1
    if os.path.exists(CKPT):
        state = torch.load(CKPT)
        model.load_state_dict(state["model"])
        start = state["epoch"] + 1
    for epoch in range(start, total_epochs + 1):
        train_one_epoch(model, optimizer)
        torch.save({"epoch": epoch, "model": model.state_dict()}, CKPT)


def train_one_epoch(*args, **kwargs):
    """Stand-in for the real training step. These fixtures are SAMPLE TEXT that the
    advisor's scanner reads, never code anything runs — but they are kept valid,
    lint-clean Python so `ruff check` covers this directory like every other."""
