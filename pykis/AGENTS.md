# PyKis Agent Guide

## Purpose
`pykis/` is the KIS wrapper layer. It handles REST requests, WebSocket streams, auth, rate limiting, typed response mapping, and fluent account/stock scopes.

## Read This With
- Also follow the root guide at `AGENTS.md`.
- If a change affects trading behavior in `tools/`, read `tools/AGENTS.md` too.

## Where To Look
| Task | Location | Notes |
|---|---|---|
| Main client entrypoint | `kis.py` | `PyKis` |
| Credential persistence | `client/auth.py` | app keys, accounts, auth file storage |
| Token lifecycle | `api/auth/token.py` | token issue, revoke, and expiry |
| WebSocket lifecycle | `client/websocket.py` | reconnect, tickets, encryption |
| Account scope | `scope/account.py` | balance, orders, history |
| Stock scope | `scope/stock.py` | quotes, charts, orderbooks |
| REST endpoint additions | `api/auth/`, `api/stock/`, or `api/account/` | mirror KIS API surface |
| Dynamic mapping | `responses/dynamic.py` | JSON-to-object conversion |
| Rate limiting | `utils/rate_limit.py` | KIS API compliance |

## Core Patterns
- Prefer fluent wrapper access such as `kis.account()` and `kis.stock()` over direct low-level calls.
- Preserve ticket/reference lifecycle semantics for WebSocket subscriptions; unsubscribe behavior depends on them.
- Keep endpoint structure aligned with existing `api/` layout and naming.
- Use shared utilities for rate limiting, thread safety, timezone handling, and response mapping.

## Style Expectations
- Typing is strong and intentional in this package; follow nearby protocols, overloads, and typed attributes.
- `TYPE_CHECKING` imports are common and preferred for type-only dependencies.
- Reuse the existing `pykis.logging` surface where the module already depends on it.
- Match nearby docstring language and wrapper conventions rather than importing styles from root scripts or `web/app.py`.

## Safety Rules
- Do not bypass rate limiting or hardcode endpoints when `__env__.py` or existing helpers already define them.
- Do not break thread-safe decorators or connection lifecycle assumptions in client code.
- Keep response object shapes and public wrapper interfaces stable unless the user asked for a breaking change.
- Treat auth JSON files and serialized credentials as sensitive, even when they are stored outside `secrets/`.
- `api/account/order.py` contains deprecated paths; prefer `KisOrder.from_number()` where applicable.

## Anti-Patterns
- Manual timezone math instead of shared timezone utilities.
- Direct client calls when a scope or wrapper method already exists.
- Hardcoded URLs or duplicated environment constants.
- Treating re-export surfaces such as `pykis/__init__.py` as style examples.

## Verification
- Run the most targeted test available for the affected path.
- If no direct test exists, validate the closest safe codepath that exercises the wrapper behavior you changed.
- For WebSocket or rate-limit changes, trace reconnect and subscription side effects before considering the edit complete.
