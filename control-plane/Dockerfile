# Pinned Python so the build is reproducible regardless of host version
# (host here is 3.14; we standardise the container on 3.12).
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# Install deps first for layer caching. We install the *dev* set so the same
# image can run `pytest` (acceptance checklist) as well as serve the API.
COPY requirements.txt requirements-dev.txt ./
RUN pip install --no-cache-dir -r requirements-dev.txt

# App source (shadowed by the dev bind-mount in docker-compose, used as-is otherwise).
COPY . .

EXPOSE 8000

# Apply migrations, then serve. `sh -c` avoids relying on a host exec bit
# (Windows host) for an entrypoint script.
#
# TLS (2026-08-22): the mode is decided by whether a server certificate is
# actually present at /certs, not by an env var that can be set while the file is
# missing. docker-compose always mounts ./certs there, so the rule is simply
# "generate the certificates and the control plane serves HTTPS".
#
# Either way it says which mode it is in, on the first line of its own log. A
# deployment that believed it was encrypted and was not is exactly the failure
# this project refuses to ship quietly, so the state is announced rather than
# left to be inferred from a URL that happens to work.
CMD ["sh", "-c", "alembic upgrade head && if [ -f /certs/server.pem ] && [ -f /certs/server.key ]; then echo 'control plane: TLS ON (serving https, cert /certs/server.pem)'; exec uvicorn app.main:app --host 0.0.0.0 --port 8000 --ssl-certfile /certs/server.pem --ssl-keyfile /certs/server.key; else echo 'control plane: TLS OFF (serving plain http; no /certs/server.pem found)'; exec uvicorn app.main:app --host 0.0.0.0 --port 8000; fi"]
