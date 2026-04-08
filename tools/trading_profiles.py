from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class StrategyProfile:
    strategy_id: str
    display_name: str
    selection_top_n: int = 20
    cash_ratio: float = 0.03


@dataclass(frozen=True)
class AccountProfile:
    account_id: str
    display_name: str
    secret_filename: str
    strategy_id: str
    enabled: bool = True


DEFAULT_STRATEGIES: dict[str, StrategyProfile] = {
    "krx_vmq": StrategyProfile(
        strategy_id="krx_vmq",
        display_name="KRX VMQ",
        selection_top_n=20,
        cash_ratio=0.03,
    ),
    "krx_us_core4": StrategyProfile(
        strategy_id="krx_us_core4",
        display_name="KRX US Core4",
        selection_top_n=4,
        cash_ratio=0.01,
    ),
}


DEFAULT_ACCOUNTS: tuple[AccountProfile, ...] = (
    AccountProfile(
        account_id="krx_vmq",
        display_name="KRX VMQ Account",
        secret_filename="krx_vmq.json",
        strategy_id="krx_vmq",
        enabled=True,
    ),
    AccountProfile(
        account_id="krx_us_core4",
        display_name="KRX US Core4 Account",
        secret_filename="krx_us_core4.json",
        strategy_id="krx_us_core4",
        enabled=True,
    ),
)


def get_enabled_accounts() -> list[AccountProfile]:
    return [account for account in DEFAULT_ACCOUNTS if account.enabled]


def get_strategy_profile(strategy_id: str) -> StrategyProfile:
    if strategy_id not in DEFAULT_STRATEGIES:
        raise KeyError(f"Unknown strategy_id: {strategy_id}")

    return DEFAULT_STRATEGIES[strategy_id]


def get_unique_strategies(
    accounts: list[AccountProfile] | None = None,
) -> list[StrategyProfile]:
    resolved_accounts = accounts or get_enabled_accounts()
    seen: set[str] = set()
    strategies: list[StrategyProfile] = []

    for account in resolved_accounts:
        if account.strategy_id in seen:
            continue
        strategies.append(get_strategy_profile(account.strategy_id))
        seen.add(account.strategy_id)

    return strategies


def get_primary_selection_account(
    accounts: list[AccountProfile] | None = None,
) -> AccountProfile:
    resolved_accounts = accounts or get_enabled_accounts()
    if not resolved_accounts:
        raise ValueError("No enabled trading accounts configured.")

    for account in resolved_accounts:
        if account.account_id == "krx_vmq":
            return account

    return resolved_accounts[0]


def resolve_secret_path(base_dir: Path, account: AccountProfile) -> Path:
    secrets_dir = base_dir / "secrets"
    configured_path = secrets_dir / account.secret_filename
    return configured_path
