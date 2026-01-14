"""Unified retry utilities with exponential backoff.

This module provides a consistent retry mechanism across the codebase,
replacing ad-hoc retry loops in trading_utils.py, quant_utils.py, and
selection_store.py.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Callable, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")


def retry_with_backoff(
    func: Callable[[], T],
    *,
    max_retries: int = 3,
    initial_delay: float = 1.0,
    max_delay: float = 30.0,
    backoff_factor: float = 2.0,
    context: str = "",
    verbose: bool = False,
    exceptions: tuple[type[Exception], ...] = (Exception,),
) -> tuple[bool, T | None]:
    """
    Execute a function with exponential backoff retry logic.

    Parameters
    ----------
    func : Callable[[], T]
        The function to execute. Should take no arguments and return a value.
    max_retries : int
        Maximum number of retry attempts (default: 3).
    initial_delay : float
        Initial delay in seconds between retries (default: 1.0).
    max_delay : float
        Maximum delay in seconds between retries (default: 30.0).
    backoff_factor : float
        Multiplier for delay after each retry (default: 2.0).
    context : str
        Description of the operation for logging purposes.
    verbose : bool
        Whether to log retry attempts (default: False).
    exceptions : tuple[type[Exception], ...]
        Exception types to catch and retry on (default: all exceptions).

    Returns
    -------
    tuple[bool, T | None]
        A tuple of (success, result). On failure, returns (False, None).

    Examples
    --------
    >>> def fetch_data():
    ...     return api.get_data()
    >>> success, data = retry_with_backoff(
    ...     fetch_data,
    ...     max_retries=5,
    ...     context="Fetching API data",
    ...     verbose=True,
    ... )
    >>> if success:
    ...     process(data)
    """
    delay = initial_delay

    for attempt in range(max_retries):
        try:
            result = func()
            return True, result
        except exceptions as e:
            is_last_attempt = attempt >= max_retries - 1

            if is_last_attempt:
                if verbose or context:
                    logger.warning(
                        "[%s] Failed after %d attempts: %s",
                        context or "retry",
                        max_retries,
                        e,
                    )
                return False, None

            if verbose:
                logger.info(
                    "[%s] Attempt %d/%d failed: %s. Retrying in %.1fs...",
                    context or "retry",
                    attempt + 1,
                    max_retries,
                    e,
                    delay,
                )

            time.sleep(delay)
            delay = min(delay * backoff_factor, max_delay)

    return False, None


def retry_simple(
    func: Callable[[], T],
    max_retries: int = 3,
    context: str = "",
    verbose: bool = False,
) -> tuple[bool, T | None]:
    """
    Simple retry wrapper with fixed 1-second delay between attempts.

    This is a drop-in replacement for the existing retry_execution pattern
    in trading_utils.py.

    Parameters
    ----------
    func : Callable[[], T]
        The function to execute.
    max_retries : int
        Maximum number of retry attempts.
    context : str
        Description of the operation for logging.
    verbose : bool
        Whether to log failures.

    Returns
    -------
    tuple[bool, T | None]
        A tuple of (success, result).
    """
    return retry_with_backoff(
        func,
        max_retries=max_retries,
        initial_delay=1.0,
        backoff_factor=1.0,  # Fixed delay, no backoff
        context=context,
        verbose=verbose,
    )


def retry_api_call(
    func: Callable[[], T],
    max_retries: int = 10,
    context: str = "",
) -> T:
    """
    Retry an API call, raising RuntimeError on final failure.

    This is a drop-in replacement for _retry_stock_call in quant_utils.py.

    Parameters
    ----------
    func : Callable[[], T]
        The API function to execute.
    max_retries : int
        Maximum number of retry attempts.
    context : str
        Error message context for the RuntimeError.

    Returns
    -------
    T
        The result of the successful function call.

    Raises
    ------
    RuntimeError
        If all retry attempts fail.
    """
    success, result = retry_with_backoff(
        func,
        max_retries=max_retries,
        initial_delay=0.5,
        max_delay=5.0,
        backoff_factor=1.5,
        context=context,
    )

    if not success:
        raise RuntimeError(context or "API call failed after max retries")

    return result  # type: ignore[return-value]


__all__ = [
    "retry_with_backoff",
    "retry_simple",
    "retry_api_call",
]
