from __future__ import annotations

import argparse
import json

from tools.scheduler_state import (
    clear_trading_day_manual_review,
    list_unresolved_trading_day_reviews,
    load_nightly_prep_state,
    load_trading_day_state,
)
from tools.time_utils import today_kst
from tools.trading_profiles import get_enabled_accounts


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m pipelines.scheduler_admin")
    subparsers = parser.add_subparsers(dest="command", required=True)

    status_parser = subparsers.add_parser("status")
    status_parser.add_argument("--run-date", default=today_kst().isoformat())

    clear_parser = subparsers.add_parser("clear-trading-review")
    clear_parser.add_argument("--run-date", required=True)
    clear_parser.add_argument("--account-id", required=True)

    return parser


def _status(run_date: str) -> dict[str, object]:
    accounts = get_enabled_accounts()
    return {
        "run_date": run_date,
        "nightly_prep": load_nightly_prep_state(run_date),
        "trading_day": {
            account.account_id: load_trading_day_state(run_date, account_id=account.account_id)
            for account in accounts
        },
        "pending_manual_reviews": list_unresolved_trading_day_reviews(),
    }


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    if args.command == "status":
        print(json.dumps(_status(args.run_date), indent=2, sort_keys=True))
        return

    if args.command == "clear-trading-review":
        state = clear_trading_day_manual_review(args.run_date, account_id=args.account_id)
        print(json.dumps(state, indent=2, sort_keys=True))
        return

    raise ValueError(f"Unknown command: {args.command}")


if __name__ == "__main__":
    main()
