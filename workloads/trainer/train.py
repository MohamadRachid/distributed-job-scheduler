"""Real training workload — a small convolutional network on Fashion-MNIST, in
PyTorch, on the CPU.

**This is a DEMONSTRATION workload. It is never on the critical path**
and no timing it produces is ever a scheduler measurement. `workloads/dummy/` stays the workload every experiment, every
definition of done and every regression test uses. This one exists to show the
platform running somebody's real job, which is a different claim from running our own
sleeping stand-in.

It is written the way an ordinary user's training script would be written, and it
adopts the platform's three contracts — each of which is one small thing, which is the
whole argument for them:

  * **configuration arrives as environment variables** the submit form already sends
    (`LR`, `BATCH_SIZE`, `OPTIMIZER`, `SEED`, `EPOCHS`), plus `FYP_NODE_NAME` which
    the agent injects. Nothing was added to the platform to carry them;
  * **one `##PROGRESS` line per epoch** (W5b) gives the browser a live progress bar
    and the latest loss. That is one print statement;
  * **two calls to `fyp_checkpoint`** (2026-08-13) let a re-dispatched run continue
    from the last finished epoch instead of starting over. That is `load()` at the
    top and `save()` at the bottom of the loop.

**Why the seed can come from the machine's name.** If the user supplies `SEED`, it
wins. If they do not, the seed is derived from `FYP_NODE_NAME`, so one job submitted
with three replicas trains three genuinely different models on three different
machines, comparable side by side in the results view that already exists. No
platform change was needed for that — it falls out of a variable the agent already
sends.

**The dataset is baked into the image at build time.** A run needs no network, which
is what lets the whole demonstration survive with the Wi-Fi switched off.
"""

import argparse
import base64
import gzip
import hashlib
import io
import json
import os
import sys
import time

import numpy as np
import torch
import torch.nn as nn
import torch.nn.functional as F

import fyp_checkpoint
import fyp_data

DATA_DIR = os.environ.get("DATA_DIR", "/work/data")

# Fashion-MNIST's ten classes, in label order — used only to make metrics.json
# readable by a human.
CLASSES = [
    "t-shirt/top", "trouser", "pullover", "dress", "coat",
    "sandal", "shirt", "sneaker", "bag", "ankle boot",
]


# --------------------------------------------------------------------------- data

def _read_idx(path: str) -> np.ndarray:
    """Read one IDX file — the raw format Fashion-MNIST ships in.

    Four magic bytes (the third names the element type, the fourth the number of
    dimensions), then one big-endian 32-bit length per dimension, then the data. We
    parse it here rather than depend on torchvision, which would add a package to the
    image for one function."""
    opener = gzip.open if path.endswith(".gz") else open
    with opener(path, "rb") as f:
        raw = f.read()
    ndim = raw[3]
    dims = [int.from_bytes(raw[4 + 4 * i:8 + 4 * i], "big") for i in range(ndim)]
    body = np.frombuffer(raw, dtype=np.uint8, offset=4 + 4 * ndim)
    return body.reshape(dims)


def load_data(subset: int):
    """Load the baked-in dataset and return train/test tensors.

    Pixels are scaled to 0..1 and normalised by the dataset's own mean and standard
    deviation — the ordinary thing to do, and it is what makes the network converge in
    the handful of epochs a demo has time for. `subset` caps the training set: the
    work is real, there is simply less of it, which is how the run is kept inside a
    minute or two without faking anything."""
    x_train = _read_idx(os.path.join(DATA_DIR, "train-images-idx3-ubyte.gz"))
    y_train = _read_idx(os.path.join(DATA_DIR, "train-labels-idx1-ubyte.gz"))
    x_test = _read_idx(os.path.join(DATA_DIR, "t10k-images-idx3-ubyte.gz"))
    y_test = _read_idx(os.path.join(DATA_DIR, "t10k-labels-idx1-ubyte.gz"))

    if subset and subset < len(x_train):
        x_train, y_train = x_train[:subset], y_train[:subset]

    mean, std = 0.2860, 0.3530  # Fashion-MNIST's own statistics
    def prep(x):
        # np.array copies: the buffer read out of the file is read-only, and torch
        # warns loudly when it is handed one. The logs are the demo surface, so a
        # warning nobody needs does not belong in them.
        t = torch.from_numpy(np.array(x)).float().div_(255.0)
        return t.sub_(mean).div_(std).unsqueeze(1)  # N,1,28,28

    return (
        prep(x_train), torch.from_numpy(np.array(y_train)).long(),
        prep(x_test), torch.from_numpy(np.array(y_test)).long(),
    )


# -------------------------------------------------------------------------- model

class SmallCNN(nn.Module):
    """Two convolutions, two fully-connected layers, about 206 thousand parameters.

    Deliberately small. It has to train to a believable accuracy inside a minute on a
    laptop CPU, and its weights have to fit comfortably inside a checkpoint that the
    agent uploads every thirty seconds."""

    def __init__(self) -> None:
        super().__init__()
        self.conv1 = nn.Conv2d(1, 16, 3, padding=1)
        self.conv2 = nn.Conv2d(16, 32, 3, padding=1)
        self.fc1 = nn.Linear(32 * 7 * 7, 128)
        self.fc2 = nn.Linear(128, 10)

    def forward(self, x):
        x = F.max_pool2d(F.relu(self.conv1(x)), 2)   # 28 -> 14
        x = F.max_pool2d(F.relu(self.conv2(x)), 2)   # 14 -> 7
        x = x.flatten(1)
        return self.fc2(F.relu(self.fc1(x)))


# --------------------------------------------------------------------- checkpoint

def _encode(model, optimizer) -> bytes:
    """Pack the model and optimiser into one text-safe string.

    `fyp_checkpoint` carries JSON, and JSON cannot hold a tensor. So the tensors are
    written to an in-memory buffer with torch's own serialiser and that buffer is
    base64-encoded — the contract stays exactly as it is, and a workload with real
    weights encodes them on its way in.

    **This is a real limit, and here is where it sits.** Measured on this model:
    206,922 parameters produce 2.38 MB of torch bytes, which base64 turns into a
    3.17 MB string — a 33% surcharge, and about **16 bytes per parameter** once the
    optimiser's two Adam tensors per weight are counted. At this size that is a file
    small enough to not think about. It scales linearly and the whole thing is built
    as one Python string in memory, so a 100-million-parameter model — still small by
    current standards — would mean roughly 1.6 GB of base64 held in RAM to save one
    epoch. **The boundary is the shape of the contract, not the size of this model:**
    the fix is a bytes-shaped `save`/`load` rather than a JSON-shaped one, which the
    platform side already supports because it stores and digests opaque bytes. Named
    deliberately and not built — see `fyp_checkpoint.py`."""
    buf = io.BytesIO()
    torch.save({"model": model.state_dict(), "optimizer": optimizer.state_dict()}, buf)
    return buf.getvalue()


def _decode(blob, model, optimizer) -> bool:
    """Restore the model and optimiser from that string. False if it cannot be read.

    False means *start from the beginning*, never *fail the run* — the same rule the
    contract states. A run that crashes while resuming would be worse than one that
    repeats some training."""
    try:
        # bytes from the new format; str from a checkpoint an older attempt wrote.
        raw = blob if isinstance(blob, (bytes, bytearray)) else base64.b64decode(blob)
        try:
            state = torch.load(io.BytesIO(raw), map_location="cpu", weights_only=True)
        except Exception:
            # Our own file, and the control plane checked its SHA-256 against the
            # digest it recorded when it stored the bytes, so this is not an unknown
            # blob from a stranger.
            state = torch.load(io.BytesIO(raw), map_location="cpu", weights_only=False)
        model.load_state_dict(state["model"])
        optimizer.load_state_dict(state["optimizer"])
        return True
    except Exception as exc:                     # noqa: BLE001 - see docstring
        print(f"checkpoint present but unusable ({exc}); starting from the top",
              file=sys.stderr, flush=True)
        return False


# ------------------------------------------------------------------------ helpers

def _seed_from(node_name: str) -> int:
    """A stable seed derived from the machine's name.

    Stable so a rerun on the same machine reproduces; different per machine so three
    replicas of one job are three genuinely different training runs."""
    digest = hashlib.sha256(node_name.encode("utf-8")).digest()
    return int.from_bytes(digest[:4], "big") % (2 ** 31 - 1)


def _hparams(node_name: str) -> dict:
    supplied = os.environ.get("SEED", "").strip()
    if supplied:
        seed, seed_source = int(supplied), "supplied"
    else:
        seed, seed_source = _seed_from(node_name), f"derived from node name {node_name!r}"
    return {
        "lr": float(os.environ.get("LR", "0.001")),
        "batch_size": int(os.environ.get("BATCH_SIZE", "32")),
        "optimizer": os.environ.get("OPTIMIZER", "adam").lower(),
        "seed": seed,
        "seed_source": seed_source,
    }


def _make_optimizer(name: str, params, lr: float):
    if name == "sgd":
        return torch.optim.SGD(params, lr=lr, momentum=0.9)
    if name == "rmsprop":
        return torch.optim.RMSprop(params, lr=lr)
    return torch.optim.Adam(params, lr=lr)


@torch.no_grad()
def evaluate(model, x, y, batch_size: int) -> float:
    model.eval()
    correct = 0
    for i in range(0, len(x), batch_size):
        correct += (model(x[i:i + batch_size]).argmax(1) == y[i:i + batch_size]).sum().item()
    model.train()
    return correct / len(x)


def write_curve(history: list, path: str) -> bool:
    """Loss and accuracy per epoch. Best-effort: a missing chart is not a failed run."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        epochs = [h["epoch"] for h in history]
        fig, ax1 = plt.subplots(figsize=(6, 3.5), dpi=150)
        ax1.plot(epochs, [h["loss"] for h in history], marker="o", color="#333333", label="train loss")
        ax1.set_xlabel("epoch")
        ax1.set_ylabel("loss")
        ax2 = ax1.twinx()
        ax2.plot(epochs, [h["accuracy"] for h in history], marker="s", linestyle="--",
                 color="#777777", label="test accuracy")
        ax2.set_ylabel("test accuracy")
        ax1.grid(alpha=0.3)
        fig.suptitle(f"Fashion-MNIST on {os.environ.get('FYP_NODE_NAME', 'unknown')}")
        fig.tight_layout()
        buffer = io.BytesIO()
        fig.savefig(buffer, format="png")
        with fyp_data.create_output(os.path.basename(path), directory=os.path.dirname(path)) as output:
            output.write(buffer.getbuffer())
        plt.close(fig)
        return True
    except Exception as exc:                     # noqa: BLE001
        print(f"could not draw curve.png ({exc})", file=sys.stderr, flush=True)
        return False


# --------------------------------------------------------------------------- main

def main() -> int:
    parser = argparse.ArgumentParser(description="Fashion-MNIST trainer (demonstration workload)")
    parser.add_argument("--resume", action="store_true",
                        help="save a checkpoint each epoch and continue from one if an earlier attempt left it")
    args = parser.parse_args()

    node_name = os.environ.get("FYP_NODE_NAME", "unknown")
    out_dir = os.environ.get("OUTPUT_DIR", "/output")
    epochs = int(os.environ.get("EPOCHS", "8"))
    subset = int(os.environ.get("TRAIN_SUBSET", "30000"))
    log_every = int(os.environ.get("LOG_EVERY", "20"))
    checkpoint_every = max(1, int(os.environ.get("CHECKPOINT_EVERY", "1")))
    hp = _hparams(node_name)

    # One thread keeps the run reproducible and its duration predictable. Float
    # addition is not associative, so the order a thread pool happens to reduce in can
    # move the last digits of a loss.
    torch.set_num_threads(int(os.environ.get("TORCH_THREADS", "1")))
    torch.manual_seed(hp["seed"])
    np.random.seed(hp["seed"] % (2 ** 32 - 1))

    print(f"node={node_name} epochs={epochs} train_subset={subset}", flush=True)
    print("hyperparameters: " + " ".join(f"{k}={v}" for k, v in hp.items()), flush=True)

    x_train, y_train, x_test, y_test = load_data(subset)
    print(f"loaded {len(x_train)} training images and {len(x_test)} test images "
          f"from the image (no network needed)", flush=True)

    model = SmallCNN()
    optimizer = _make_optimizer(hp["optimizer"], model.parameters(), hp["lr"])
    n_params = sum(p.numel() for p in model.parameters())
    print(f"model: small CNN, {n_params} parameters", flush=True)

    # ---- resume, if this is a re-dispatched run and the last machine saved anything
    start_epoch = 1
    history: list = []
    resumed_from = 0
    if args.resume:
        state = fyp_checkpoint.load()
        # New format first, then the key an older attempt would have written. Both,
        # because attempt 1 and attempt 2 can be running different images.
        blob = state.get(fyp_checkpoint.PAYLOAD_KEY) or state.get("torch_b64", "") \
            if state else ""
        if state and _decode(blob, model, optimizer):
            resumed_from = int(state.get("epoch", 0))
            history = list(state.get("history", []))
            start_epoch = resumed_from + 1
            print(f"resuming from checkpoint at epoch {resumed_from} "
                  f"(accuracy so far {state.get('accuracy')})", flush=True)
        else:
            # Absent is normal, not an error: it is every first attempt.
            print("no usable checkpoint — starting at epoch 1", flush=True)

    started = time.time()
    loss_fn = nn.CrossEntropyLoss()
    n = len(x_train)
    epoch_loss = None
    accuracy = history[-1]["accuracy"] if history else None

    for epoch in range(start_epoch, epochs + 1):
        # Shuffle differently each epoch, reproducibly given the seed.
        order = torch.randperm(n, generator=torch.Generator().manual_seed(hp["seed"] + epoch))
        running, batches = 0.0, 0
        for step, i in enumerate(range(0, n, hp["batch_size"]), start=1):
            idx = order[i:i + hp["batch_size"]]
            optimizer.zero_grad(set_to_none=True)
            loss = loss_fn(model(x_train[idx]), y_train[idx])
            loss.backward()
            optimizer.step()
            running += loss.item()
            batches += 1
            if step % log_every == 0:
                # Ordinary per-batch output, so the live log pane carries real numbers.
                print(f"epoch {epoch} batch {step} loss={loss.item():.4f}", flush=True)

        epoch_loss = round(running / max(batches, 1), 4)
        accuracy = round(evaluate(model, x_test, y_test, 256), 4)
        history.append({"epoch": epoch, "loss": epoch_loss, "accuracy": accuracy})
        print(f"epoch {epoch}/{epochs} loss={epoch_loss} test_accuracy={accuracy}", flush=True)
        # The W5b progress contract — one line, and the browser draws a bar.
        print(f'##PROGRESS {{"epoch": {epoch}, "total": {epochs}, '
              f'"loss": {epoch_loss}, "acc": {accuracy}}}', flush=True)

        # Save AFTER the epoch finishes, so what is recorded is work that is done.
        if args.resume and (epoch % checkpoint_every == 0 or epoch == epochs):
            ok = fyp_checkpoint.save({
                "epoch": epoch,
                "accuracy": accuracy,
                "loss": epoch_loss,
                "history": history,
            }, payload=_encode(model, optimizer))
            if ok:
                print(f"checkpoint saved at epoch {epoch}", flush=True)

    duration = round(time.time() - started, 2)

    # ---- results, for the platform to collect and the results view to show
    os.makedirs(out_dir, exist_ok=True)
    model_bytes = io.BytesIO()
    torch.save(model.state_dict(), model_bytes)
    with fyp_data.create_output("model.pt", directory=out_dir) as output:
        output.write(model_bytes.getbuffer())
    drew = write_curve(history, os.path.join(out_dir, "curve.png")) if history else False
    result = {
        "node": node_name,
        "dataset": "fashion-mnist",
        "classes": CLASSES,
        "model": {"kind": "small CNN", "parameters": n_params},
        "epochs": epochs,
        "train_subset": subset,
        "resumed_from_epoch": resumed_from,
        "final_loss": epoch_loss if epoch_loss is not None else (history[-1]["loss"] if history else None),
        "accuracy": accuracy,
        "history": history,
        "duration_seconds": duration,
        "hyperparameters": hp,
    }
    with fyp_data.create_output("metrics.json", directory=out_dir) as output:
        output.write(json.dumps(result, indent=2).encode("utf-8"))

    files = "metrics.json, model.pt" + (", curve.png" if drew else "")
    print(f"wrote {files} to {out_dir}", flush=True)
    print(f"final test accuracy {accuracy} after {duration}s "
          f"(resumed from epoch {resumed_from})", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
