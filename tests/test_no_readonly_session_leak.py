"""psycopg2 set_session(readonly=True) with autocommit on sets the SESSION default, which leaks through the pooled DATABASE_URL into other jobs."""
from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CHECKED = ("db", "ingest", "model", "research", "scripts")


def test_no_read_only_session_is_set_with_autocommit():
    offenders = []
    for folder in CHECKED:
        for path in (ROOT / folder).rglob("*.py"):
            text = path.read_text(encoding="utf-8", errors="ignore")
            if re.search(r"default_transaction_read_only\s*(=|TO)\s*'?on", text, re.IGNORECASE):
                offenders.append(f"{path.relative_to(ROOT)}: sets default_transaction_read_only")
            if "readonly=True" in text and (re.search(r"autocommit\s*=\s*True", text)
                                            or re.search(r"set_session\([^)]*autocommit=True", text)):
                offenders.append(f"{path.relative_to(ROOT)}: read-only session with autocommit")
    assert not offenders, offenders
