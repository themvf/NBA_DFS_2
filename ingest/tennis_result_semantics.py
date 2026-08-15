"""Shared result semantics for live and historical tennis settlement."""

from __future__ import annotations


_VOID_DERIVATIVE_STATUSES = frozenset({"retired", "walkover", "awarded"})


def classify_completion(comment: str | None) -> tuple[str, bool, bool]:
    """Return (completion_status, retired, walkover) from a source result note.

    The advancing player remains the moneyline winner. Derivative markets must
    void when this returns a status in ``VOID_DERIVATIVE_STATUSES``.
    """
    normalized = str(comment or "").strip().lower()
    if "walkover" in normalized:
        return "walkover", False, True
    if "retir" in normalized:
        return "retired", True, False
    if "awarded" in normalized:
        return "awarded", False, False
    return "completed", False, False


def void_derivatives(completion_status: str | None) -> bool:
    """Whether a non-completed tennis result invalidates derivative settlement."""
    return completion_status in _VOID_DERIVATIVE_STATUSES
