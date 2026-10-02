"""Fixture: an ordinary training loop that never checkpoints at all.

Expected arm A verdict: no_checkpoint.
"""


def main(model, optimizer, total_epochs):
    for epoch in range(1, total_epochs + 1):
        for batch in batches():
            loss = model.forward(batch)
            loss.backward()
            optimizer.step()
        print("epoch", epoch, "done")


def batches():
    """Stand-in for the data loader; see the note beside train_one_epoch."""
    return []
