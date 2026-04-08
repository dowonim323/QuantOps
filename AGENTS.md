# QuantOps Agent Guide

## Purpose
This repository is an automated quantitative trading system for Korea Investment & Securities (KIS). Prefer small, verified changes and preserve trading safety, guest-mode privacy, and KIS wrapper conventions.

## Instruction Files
- Root guide: `AGENTS.md`
- Business logic guide: `tools/AGENTS.md`
- API wrapper guide: `pykis/AGENTS.md`
- Dashboard guide: `web/AGENTS.md`
- Cursor rules: none found in `.cursor/rules/` or `.cursorrules`
- Copilot rules: none found in `.github/copilot-instructions.md`
- Read the nearest `AGENTS.md` before editing files in a subdirectory.

## Environment
- Recommended local environment: `conda activate quantops`
- Assume Python 3.10+ and Korea market rules / KST timing
- Web stack uses Flask/Gunicorn; data storage is mainly SQLite

## Where To Look First
| Task | Primary file or area | Key symbol or note |
|---|---|---|
| Rebalancing and order flow | `tools/trading_utils.py` | `rebalance()` |
| Market timing | `tools/market_watcher.py` | `get_market_signal()` |
| KST time helpers | `tools/time_utils.py` | market session and timezone helpers |
| Quant ranking | `tools/quant_utils.py` | factor scoring logic |
| Retry behavior | `tools/retry.py` | `retry_with_backoff()` |
| Selection persistence | `tools/selection_store.py` | save/load snapshot helpers |
| Dashboard API | `web/app.py` | guest-mode masking must hold |
| Dashboard startup | `web/docker-compose.yml` | Flask/Gunicorn container |
| KIS client entrypoint | `pykis/kis.py` | `PyKis` |
| WebSocket lifecycle | `pykis/client/websocket.py` | subscriptions and reconnect |
| Account scope | `pykis/scope/account.py` | fluent scope pattern |

## Build, Run, and Test Commands
There is no repo-wide `Makefile`, `pyproject.toml`, `pytest.ini`, Ruff config, Flake8 config, MyPy config, or pre-commit config at the root. Do not invent lint or typecheck commands that are not configured.

### Pipeline entrypoints
```bash
python -m pipelines.trading_session
python -m pipelines.stock_selection
python -m pipelines.financial_crawler
```

The scheduler container itself now runs `supervisord`, which manages:

```bash
python -m pipelines.nightly_prep_controller
python -m pipelines.trading_day_controller
```

For scheduler inspection and manual-review recovery:

```bash
python -m pipelines.scheduler_admin status
python -m pipelines.scheduler_admin clear-trading-review --run-date YYYY-MM-DD --account-id <account_id>
```

### Dashboard
```bash
python web/set_password.py
python web/app.py
docker compose up -d --build
docker compose -f web/docker-compose.yml up -d --build
```
From `web/`, the equivalent Docker command is:
```bash
docker compose up -d --build
```

### Tests

The observed test suite uses `unittest`, not `pytest`.
```bash
python -m unittest discover tests
python -m unittest tests.test_signal
python -m unittest tests.test_signal.TestMarketSignal.test_signal_buy_when_kosdaq_safe
```
Use the fully qualified `python -m unittest package.module.Class.test_method` form for single-test execution.

## Lint and Type Checking
- No dedicated lint command is configured at the repo root.
- No dedicated typecheck command is configured at the repo root.
- Type hints are common and expected, especially in `pykis/` and newer `tools/` modules.
- Do not add new toolchains unless the user asks for them.

## Code Style
### Imports
- Group imports as: standard library, third-party, local modules.
- Separate groups with one blank line.
- Use `TYPE_CHECKING` for type-only imports when helpful, as seen in `tools/trading_utils.py` and `pykis/client/websocket.py`.
- Do not copy import style from `web/app.py`; that file is a compatibility-heavy legacy module.
- Do not copy wildcard imports from `pykis/__init__.py`; that file is a re-export surface, not a style model.

### Formatting and Structure
- Use 4-space indentation.
- Prefer trailing commas in multiline literals, calls, and imports.
- Wrap long signatures and calls vertically.
- Keep helper functions small and named for intent.
- Match the existing docstring language in the file; Korean docstrings are common in trading and wrapper code.

### Naming
- `snake_case` for modules, functions, and variables.
- `PascalCase` for classes.
- `UPPER_SNAKE_CASE` for constants.
- Internal helpers may use a leading underscore.

### Types
- Add type hints to public functions and important internal helpers.
- Prefer modern typing syntax such as `T | None`, `list[str]`, and `dict[str, Any]`.
- Use `Literal` for constrained string domains when the valid set is known.
- `from __future__ import annotations` is preferred in new typed modules.
- Do not suppress type errors with `# type: ignore` unless the user explicitly asks and there is no better fix.

### Logging
- Prefer `logger = logging.getLogger(__name__)`.
- Use structured logging placeholders like `logger.info("Processing %s", symbol)`.
- Avoid introducing new `print()` calls in production paths; some legacy scripts still use them, but new code should not.
- Inside `pykis/`, prefer the existing `pykis.logging` surface where the module already uses it.

### Error Handling
- Raise specific exceptions such as `ValueError`, `KeyError`, `RuntimeError`, or domain exceptions.
- Preserve context with `raise ... from exc` when wrapping failures.
- Prefer explicit validation before expensive work.
- Avoid bare `except:` blocks.
- Avoid broad `except Exception` unless the code is at a true process, network, notification, or runtime boundary and logs or handles the failure clearly.
- Never leave an empty `except` block.

### Data and DB Work
- SQLite access is common; preserve existing table names, schemas, and path constants.
- Business logic often operates on pandas DataFrames; prefer vectorized transforms over row-by-row rewrites where feasible.
- For market-time logic, use existing time utilities instead of manual timezone math.

## Project-Specific Patterns
- Scope pattern: prefer fluent API access such as `kis.account().balance()`.
- Retry pattern: use `tools/retry.py` instead of ad-hoc retry loops.
- Value-based ordering: order logic targets KRW value, not fixed quantity, unless the surrounding code already works in shares.
- WebSocket tickets: subscription APIs return tickets whose lifecycle matters; do not break unsubscribe/reference counting logic.
- Quant computations should stay pandas/numpy-friendly and vectorized where practical.

## Security and Safety Constraints
- Never commit anything under `secrets/` or hardcode credentials.
- Treat local credential files such as `web/.env` and auth JSON snapshots as sensitive, even when they live outside `secrets/`.
- Guest users must never receive real portfolio values, order amounts, quantities, or account identifiers from `web/app.py`.
- Respect KIS rate limiting and wrapper abstractions; do not bypass them with direct raw requests when wrapper code exists.
- Market-hours behavior is Korea-specific; avoid changing session logic without tracing KST assumptions.
- `pykis/api/account/order.py` contains deprecated paths; prefer `KisOrder.from_number()` where applicable.

## Known Legacy Patterns
- `sys.path.append(...)` appears in `web/app.py` and `tests/test_signal.py`; treat this as compatibility glue, not a preferred pattern for new modules.
- Some root scripts and older modules still use broad exception handlers at runtime boundaries; improve only when already touching that code and keep behavior stable.
- The web layer contains practical compatibility code; preserve guest-mode masking and response shape unless the user asks for behavioral changes.
- Root-level automated test coverage is currently narrow; avoid large refactors without focused verification.

## Editing Guidance
- Make the smallest safe change that solves the requested problem.
- Preserve public interfaces unless the user asked for a breaking change.
- Follow nearby code before introducing a new abstraction style.
- When editing `tools/`, `pykis/`, or `web/`, also read the local subdirectory `AGENTS.md`.
- If a change affects trading flow, trace callers and downstream order/account effects before editing.
- When touching `web/app.py`, check guest-mode masking paths before and after the change.

## Verification Expectations
- Run the most targeted `unittest` command that covers your change.
- Prefer single-test or single-module runs first, then widen scope only if needed.
- For dashboard changes, verify the relevant Flask or Docker startup path if runtime behavior changed.
- If you touch trading execution, market timing, retry logic, or persistence, trace the nearest callable path and perform the closest safe runtime validation available.
- If no automated check exists, say so explicitly and describe the nearest manual verification you performed.
