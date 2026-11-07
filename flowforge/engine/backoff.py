"""Retry eligibility and deterministic backoff delay calculation."""

from __future__ import annotations

from flowforge.domain.enums import BackoffStrategy
from flowforge.domain.workflow import RetryPolicy


def compute_backoff_seconds(
    policy: RetryPolicy,
    attempt: int,
    *,
    max_cap_seconds: float = 3600.0,
) -> float:
    """Compute delay in seconds before the next retry attempt (1-indexed attempt)."""
    safe_attempt = max(1, attempt)
    base = max(0.0, policy.base_delay_seconds)

    if policy.strategy == BackoffStrategy.FIXED:
        raw = base
    elif policy.strategy == BackoffStrategy.LINEAR:
        raw = base * safe_attempt
    elif policy.strategy == BackoffStrategy.EXPONENTIAL:
        raw = base * (2 ** (safe_attempt - 1))
    else:
        raw = base

    return float(min(raw, max_cap_seconds))


def can_retry(attempt_made: int, max_attempts: int, *, retryable_error: bool) -> bool:
    """Return True if the failed task still has retry budget and the failure is transient."""
    if not retryable_error:
        return False
    return attempt_made < max_attempts
