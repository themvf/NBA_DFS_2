"""Plain-language failures for DeepSeek account problems.

The two scheduled DeepSeek jobs (MLB beat-writer extraction and YouTube pick
extraction) used to die on a raw ``httpx.HTTPStatusError`` traceback when the
account ran out of credit (HTTP 402, 2026-09-29). The cause is not a code bug
and no retry can fix it, so the job now fails with one line that says what is
wrong and where to fix it. Transient statuses (429, 5xx) are still retried by
the callers; this module only names the account-level ones.
"""

from __future__ import annotations

import os
import sys

TOP_UP_URL = "https://platform.deepseek.com"

_ACCOUNT_MESSAGES = {
    401: "DeepSeek rejected DEEPSEEK_API_KEY (401); check the key at " + TOP_UP_URL,
    402: "DeepSeek account has no credit (402); top it up at " + TOP_UP_URL,
    403: "DeepSeek refused the request for this account (403); check it at " + TOP_UP_URL,
}


class DeepSeekAccountError(RuntimeError):
    """The DeepSeek account (key or balance) blocks every call; do not retry."""

    def __init__(self, status: int) -> None:
        self.status = status
        super().__init__(_ACCOUNT_MESSAGES[status])


def account_error_for(status: int) -> DeepSeekAccountError | None:
    """The account-level error for an HTTP status, or None if it is not one."""
    return DeepSeekAccountError(status) if status in _ACCOUNT_MESSAGES else None


def exit_on_account_error(exc: DeepSeekAccountError, *, job: str) -> None:
    """Print one GitHub-annotated line and exit non-zero.

    Still a failure (the job did not do its work), just an honest one.
    """
    prefix = f"::error title={job}::" if os.environ.get("GITHUB_ACTIONS") else f"{job}: "
    print(f"{prefix}{exc}", file=sys.stderr)
    raise SystemExit(1)
