# The DEMONSTRATION training image — a real PyTorch job for the platform to run.
#
# `workloads/dummy/` remains the workload on the critical path: every
# experiment, every definition of done and every regression test keeps using it, and
# no timing this image produces is ever a scheduler measurement. This image exists to
# show the platform running somebody's real job.
#
# BUILD FROM `workloads/`, not from this directory:
#
#     docker build -f workloads/trainer/Dockerfile -t fyp-trainer:latest workloads/
#
# EXPECT A SILENT TEN MINUTES. The PyTorch wheel is about 200 MB and pip draws no
# progress bar when its output is not a terminal -- which is what a build log is. So
# the build sits on the `pip install` line printing nothing at all, for minutes, on a
# first build or after that layer's cache is invalidated. That is normal and not a
# stall; do not kill it. A cached rebuild takes a few seconds instead.
#
# The wider context is deliberate. It lets the image use the SAME `fyp_checkpoint.py`
# the dummy uses instead of a second copy that could drift from it — and two copies of
# one file quietly disagreeing is the exact defect this project caught on 2026-08-13.
# An outside user adopting the contract would copy the file into their own image, as
# its own docstring says; inside this repository, sharing it is the safer half of that
# trade.
FROM python:3.12-slim

WORKDIR /work

# Create the unprivileged user FIRST, so its layer sits below the data and cannot
# duplicate it. An earlier version ran `chmod -R a+rX /work` after the dataset was
# added, which rewrote every file's metadata and cost 31 MB of pure duplication.
# Dropping that chmod was right; the sentence that justified it was not. It read
# "the files arrive world-readable already", which is true of COPY and false of ADD
# from a URL — see the dataset block below, where the mode is now set on the ADD.
RUN useradd --create-home --uid 10001 trainer

# CPU-only PyTorch, from PyTorch's own CPU wheel index. The default index would pull
# the CUDA build, which is several gigabytes of driver payload this image would never
# use — the machines in our pool train on their processors.
#
# The compiled-bytecode caches are deleted in the SAME layer, because a later layer
# can only hide files and never shrink the image. They are 141 MB, and dropping them
# is what brings this image under the brief's 1.5 GB stop condition. The cost was
# measured rather than assumed: importing torch and matplotlib takes 2.7 s instead of
# 1.6 s, once per container start, because Python recompiles what it needs and cannot
# write the result back as an unprivileged user.
RUN pip install --no-cache-dir --index-url https://download.pytorch.org/whl/cpu \
        torch==2.5.1 \
 && pip install --no-cache-dir numpy==2.1.3 matplotlib==3.9.2 \
 && find /usr/local/lib/python3.12/site-packages -name '__pycache__' -type d \
        -prune -exec rm -rf {} +

# The dataset is baked in AT BUILD TIME. This is what lets a run need no network at
# all, which is what lets the whole demonstration survive with the Wi-Fi switched off.
# The four files are Fashion-MNIST in its raw IDX form, about 30 MB together;
# `train.py` parses them with numpy, so torchvision is not installed for one function.
#
# `--chmod=644` is load-bearing, and an earlier version of this file asserted the
# opposite in writing: "the files arrive world-readable already". That is true of
# COPY and FALSE of ADD from a URL, which writes 0600 owned by root -- so `USER
# trainer` could not read the dataset it was given and every run died in load_data().
# Setting the mode ON the ADD costs nothing: it is applied as the layer is written,
# so it neither adds a layer nor duplicates the 30 MB the way a later `chmod -R`
# would. The directory is made above, deliberately, because it needs 755 and not 644.
#
# Both halves were verified by reading `ls -l` inside the built image and then having
# uid 10001 actually open the file. The first attempt set the files to 0644 and looked
# correct; the read still failed, because the directory had been left at 644 too. Each
# half was self-consistent and the system was still wrong -- which is the reason this
# note records the check rather than the intention.
ADD --chmod=644 https://github.com/zalandoresearch/fashion-mnist/raw/master/data/fashion/train-images-idx3-ubyte.gz /work/data/
ADD --chmod=644 https://github.com/zalandoresearch/fashion-mnist/raw/master/data/fashion/train-labels-idx1-ubyte.gz /work/data/
ADD --chmod=644 https://github.com/zalandoresearch/fashion-mnist/raw/master/data/fashion/t10k-images-idx3-ubyte.gz /work/data/
ADD --chmod=644 https://github.com/zalandoresearch/fashion-mnist/raw/master/data/fashion/t10k-labels-idx1-ubyte.gz /work/data/

# The DIRECTORY needs 755 and the files need 644 -- different modes, so the directory
# cannot ride on `--chmod`. `ADD --chmod=644` sets 644 on `/work/data` itself as well
# as on its contents, and 644 on a directory has no execute bit, so nothing inside can
# be reached: with the files already world-readable the dataset was STILL unreadable.
# Creating the directory in an earlier layer does not help either -- the ADD layer
# carries its own entry for `/work/data` and that entry wins. So it is corrected here,
# after the last ADD. One `chmod` on one directory is metadata only; the 31 MB this
# file warns about upstream came from `chmod -R` rewriting every file, not from this.
RUN chmod 755 /work/data

# Every job seals outputs/checkpoints; use the same dependency and helper as dummy.
RUN pip install --no-cache-dir cryptography==50.0.1
COPY trainer/train.py ./
# The resume contract, shared with the dummy rather than forked — see the note above.
COPY dummy/fyp_checkpoint.py dummy/fyp_data.py ./

# Run as an unprivileged user (created above). The platform already confines the
# container; this is the cheap half of the same idea, inside the image.
#
# The platform meets this image halfway, and it did not always. The agent creates each
# run's output directory with `tempfile.mkdtemp`, which is 0700 owned by the agent's
# own user -- so on a LINUX worker a container running as anyone else could not write
# its results, and this image runs as uid 10001. Windows hid it, because Docker
# Desktop presents that mount as world-writable regardless, and the dummy hid it too,
# because the dummy runs as root. FIXED 2026-08-13 in `agent/runner.py`
# (`_RUN_DIR_MODE`), with the both-directions proof in
# `docs/evidence/real_workload_2026-08-13/output_permissions.txt`.
USER trainer

ENV OUTPUT_DIR=/output \
    DATA_DIR=/work/data \
    EPOCHS=8 \
    TRAIN_SUBSET=30000 \
    LOG_EVERY=20 \
    TORCH_THREADS=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    MPLCONFIGDIR=/tmp/mpl

# Plain training. To make a run resumable after a machine dies mid-training, the JOB
# asks for it in its own entrypoint — ["python", "train.py", "--resume"] — exactly as
# the dummy does. The platform ships an image name and environment and nothing more,
# which is what keeps it a scheduler rather than a framework.
ENTRYPOINT ["python", "train.py"]
