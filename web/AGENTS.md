# Web Agent Guide

## Purpose
`web/` contains the Flask dashboard for QuantOps. It serves portfolio history, performance views, analytics, and password-protected guest-safe monitoring.

## Read This With
- Also follow the root guide at `AGENTS.md`.
- If a web change touches trading data shape or wrapper calls, read `tools/AGENTS.md` or `pykis/AGENTS.md` too.

## Key Files
| File | Role |
|---|---|
| `app.py` | Flask routes, auth state, data aggregation, masking |
| `templates/index.html` | Dashboard UI |
| `gunicorn_config.py` | Gunicorn bind `0.0.0.0:15000`, 2 workers, reload enabled |
| `set_password.py` | Writes `DASHBOARD_PASSWORD_HASH` into `web/.env` |
| `docker-compose.yml` | Dashboard container and volume wiring |
| `Dockerfile` | Python 3.10-slim image with `PYTHONPATH=/app` |

## Run Commands
- Local password setup: `python web/set_password.py`
- Direct local app run: `python web/app.py`
- From repo root: `docker compose -f web/docker-compose.yml up -d --build`
- From `web/`: `docker compose up -d --build`

## Critical Security Rules
- Treat `web/.env` as sensitive because it stores `DASHBOARD_PASSWORD_HASH`.
- Guest users must never receive real portfolio values, transfers, quantities, order amounts, or account identifiers.
- In `app.py`, preserve masking in the asset, performance, order, analytics, and any new guest-visible response when `current_user.is_authenticated` is false.
- Do not push masking responsibility entirely into the frontend; sensitive values must already be scrubbed in backend responses.
- Preserve the existing response shape unless the user explicitly asks for an API change.

## Style Notes
- `web/app.py` is a compatibility-heavy legacy module; preserve behavior first.
- `sys.path.append(...)` usage in this directory is legacy glue, not a pattern to copy elsewhere.
- Keep Flask route handlers readable and avoid spreading business logic that belongs in `tools/`.
- Environment-dependent settings should keep using `.env` and existing helpers rather than new config systems.

## Deployment Notes
- The current Gunicorn config enables reload and logs to stdout/stderr, which is development-friendly rather than hardened production behavior.
- `docker-compose.yml` mounts `web/`, `tools/`, `pykis/`, `db/`, and `logs/` into the container.
- `set_password.py` instructs operators to restart the `web` service after updating credentials.

## Verification
- For backend changes, run the closest Flask path you changed and verify the JSON shape or page behavior.
- Re-check guest-mode masking after any edit that affects portfolio, performance, orders, analytics, or account-like data.
- If deployment behavior changed, verify the relevant Docker or Gunicorn startup path.
