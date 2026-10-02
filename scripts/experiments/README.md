# `scripts/experiments/` — the W7a measurement bench

One command per experiment. Results land in `docs/evidence/experiments/<exp>/`.

## Before anything

1. Open **Docker Desktop** and wait for "Engine running".
2. Bring the stack up: `docker compose up -d --build`
3. Once per machine: `.venv\Scripts\python.exe -m pip install -r scripts/experiments/requirements.txt`

## Run

```
.venv\Scripts\python.exe scripts\experiments\e00_smoke.py
```

Every experiment writes four files:

| File | What it is |
|---|---|
| `raw.jsonl` | one row per repetition — the only place a number may come from |
| `summary.md` | median / min / max per arm |
| `chart.png` | the same, drawn, with min–max error bars |
| `manifest.json` | git SHA, host, Docker/Postgres versions, modes, conditions |

## The three switches

The harness runs the weaker alternative to one of our design choices by restarting
the control plane with a switch flipped, then **reading `GET /health` back to
confirm it took**. Defaults are always the shipped system.

| Switch | Values (default first) |
|---|---|
| `guarantee` | `full` · `lease_only` · `none` |
| `claim` | `skip_locked` · `blocking` · `naive` |
| `reschedule` | `learned` · `blind` · `none` |

The switches are injected through a generated `docker-compose.experiment.yml`
(gitignored). **`docker-compose.yml` never sets one** — the committed compose file
must not be able to disable one of our own guarantees. A non-default mode prints a
three-line `!!!` banner in the control-plane log and appears on `/health`.

## Rules the bench enforces for you

- **A number needs a row.** Nothing may appear in a table or the report without a
  matching line in `raw.jsonl`. If it cannot be measured, it is written `«MISSING»`.
- **Timings come from the server.** `server_times()` reads Postgres. The one
  derived value, `assigned_at`, is computed from the server's own
  `lease_expires_at` and is labelled as derived wherever it appears.
- **Dry runs are quarantined.** Pass `dry=True` while developing; `summarize` and
  `chart` drop those rows.
- **Spread, always.** Summaries report median, min and max over repetitions — never
  a lone number.

## Measurement window

Run experiments **one at a time**, on an otherwise idle machine, on **mains power**.
Parallel runs contend for CPU, disk and Docker, and the timings come out wrong
without looking wrong. Record `machine_idle` and `mains_power` in the manifest.
