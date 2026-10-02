# Tiny wrapper over the official pgAdmin image.
#
# Why this exists: pgAdmin connects our pre-registered "FYP Postgres" server using
# a passfile. libpq REFUSES a passfile that is readable by group
# or world (it silently ignores it and then prompts for a password). A file bind-
# mounted from Windows lands as 0777, which libpq rejects, and chmod on a Windows
# bind mount does not stick. So we bake the passfile into the image with the exact
# 0600 permissions libpq wants, owned by the pgadmin runtime user. Result: the
# server connects with no password prompt, reproducibly, on any teammate's machine.
#
# 2026-09-07: the passfile is WRITTEN here from a build argument rather than copied
# from a committed file, so the database password is not a file in the repository
# and a deployment that sets POSTGRES_PASSWORD (see .env.example) gets a pgAdmin
# that still connects. docker-compose.yml passes the argument.
FROM dpage/pgadmin4:latest
ARG POSTGRES_PASSWORD=fyp
USER root
RUN printf 'postgres:5432:*:fyp:%s\n' "$POSTGRES_PASSWORD" > /pgpass \
 && chmod 600 /pgpass && chown pgadmin /pgpass
USER pgadmin
