# Tools Agent Guide

## Purpose
`tools/` is the business-logic layer for QuantOps. It turns market data and portfolio state into stock selection, timing signals, persistence updates, and order execution.

## Read This With
- Also follow the root guide at `AGENTS.md`.
- If your change touches `pykis/` or `web/`, read `pykis/AGENTS.md` or `web/AGENTS.md` too.

## Where To Look
| Task | File | Key symbol or note |
|---|---|---|
| Rebalance flow | `trading_utils.py` | `rebalance()` |
| Quant ranking | `quant_utils.py` | factor scoring and ranking |
| Market timing | `market_watcher.py` | `get_market_signal()` |
| KST helpers | `time_utils.py` | shared session and timezone logic |
| Retry behavior | `retry.py` | `retry_with_backoff()` |
| Selection persistence | `selection_store.py` | snapshot save/load helpers |
| Account history DB | `account_record.py` | `_init_db()` and write helpers |
| Financial DB schema | `financial_db.py` | SQLite tables and paths |
| Notifications | `notifications.py` | Discord webhook boundary |

## Core Patterns
- Value-based ordering matters here: many paths target KRW value, not fixed share count.
- Retry behavior should go through `retry.py`, not ad-hoc loops.
- Market-time logic should use shared time helpers for KST compliance.
- Quant computations should stay pandas/numpy-friendly and vectorized where practical.
- Use `logging.getLogger(__name__)` and structured log placeholders instead of new `print()` calls.

## Style Expectations
- Newer modules follow standard-library / third-party / local import grouping.
- `from __future__ import annotations` and modern type syntax are common in actively maintained files.
- `TYPE_CHECKING` imports are normal for heavier typed modules.
- Keep helpers small and intent-named; large orchestration functions should delegate to focused helpers.
- Match the existing docstring language in the file; Korean docstrings are common in this layer.

## Safety Rules
- If you change `rebalance()`, trace sell, trim, and buy sequencing before editing.
- Do not bypass wrapper abstractions with raw API calls when equivalent `pykis` functionality exists.
- Preserve DB table names, path constants, and persisted data shape unless the user requests a migration.
- Do not break WebSocket ticket or event-driven execution flows used by `trading_utils.py`.

## Legacy Notes
- Some orchestration code still uses broad `except Exception` at runtime boundaries; narrow exceptions in helpers, but preserve boundary stability.
- Some root scripts still log or print in a legacy style; do not spread that into new `tools/` code.

## Verification
- Run the most targeted `unittest` command that covers the changed behavior.
- If no direct test exists, trace the nearest callable path and perform the closest safe runtime validation available.
- For trading-flow changes, verify the downstream effect on persistence, notifications, and order/account state handling.
