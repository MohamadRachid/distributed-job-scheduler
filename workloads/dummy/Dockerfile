# The test workload image. Built in W1, RUN by the agent from W2.
# Defines the job contract: an image with an entrypoint that writes to OUTPUT_DIR.
FROM python:3.12-slim
WORKDIR /work
# `cryptography` is what the sealing contract uses. It is the ONE dependency a
# workload image is asked to add, and since 2026-09-06 it is what EVERY job needs
# rather than only a private one: `fyp_data.py` opens the dataset a piece at a time
# and seals the results on the way out.
RUN pip install --no-cache-dir cryptography==50.0.1
COPY train.py fyp_open.py fyp_checkpoint.py fyp_data.py ./
# OUTPUT_DIR is always overridden by the agent (it points at the run's own writable
# mount, /scratch since 2026-09-04). This default only matters when the image is run
# by hand outside the platform, and it now names the same path the platform uses so
# the two cannot teach a reader different things.
ENV OUTPUT_DIR=/scratch \
    EPOCHS=5 \
    EPOCH_SECONDS=1
# FAIL=1 forces a non-zero exit (used to exercise W5 failure recovery).
#
# 2026-08-13: `fyp_checkpoint.py` is the resume contract — two calls, opt-in. A job
# asks for it in its own entrypoint, e.g. ["python", "train.py", "--resume"]. Without
# that flag this image behaves exactly as it did before, so nothing that ran
# yesterday changes.
#
# 2026-09-06: `fyp_data.py` is the sealing contract — one import, one call to open
# the dataset, one call to create a result file. There is NO wrapper entrypoint any
# more: the workload imports the reader itself, so a job spec names its own command
# and nothing has to be wrapped. `fyp_open.py` stays in the image because jobs
# submitted before that date still run the old way:
#   ["python", "fyp_open.py", "python", "train.py", "--private-input"]
# That wrapping was always part of the JOB SPEC and never something the platform
# injected — the platform still only ships an image name and environment, so it stays
# generic (a job is a container).
ENTRYPOINT ["python", "train.py"]
