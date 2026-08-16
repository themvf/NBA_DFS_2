"""The alert-audit disclosure floor must exist in exactly one place.

It diverged once. `web/src/app/vegas/line-alerts-panel.tsx` (MLB, soccer,
tennis) enforced a 30-alert floor before showing a rate; the NFL page has its
own inline audit table and enforced nothing, publishing "25.0%" off a 2-6
record. Same ledger, same question, two answers depending on which page you
opened.

Extracting a constant into a component would not prevent a recurrence — a
third surface would grow a third copy. These tests assert the structural
property instead: the number lives in one module, the Python engine and the
web layer agree on it, and no consumer redefines it locally.

There is no JS test runner in `web/`, so this is enforced from the Python
suite — the same technique used for the MLB totals side-selection guard.
"""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
POLICY = ROOT / "web" / "src" / "lib" / "alert-audit-policy.ts"
SHARED_PANEL = ROOT / "web" / "src" / "app" / "vegas" / "line-alerts-panel.tsx"
NFL_PANEL = ROOT / "web" / "src" / "app" / "nfl" / "nfl-vegas-client.tsx"
ENGINE = ROOT / "model" / "line_alerts.py"


def _read(p: Path) -> str:
    assert p.exists(), f"missing {p}"
    return p.read_text(encoding="utf-8")


def test_web_floor_matches_the_python_engine() -> None:
    """A UI more permissive than the engine behind it is a lie about how much
    is known. These answer the same question and must carry the same number."""
    web = re.search(r"MIN_SETTLED_FOR_CI\s*=\s*(\d+)", _read(POLICY))
    py = re.search(r"_MIN_SETTLED_FOR_CI\s*=\s*(\d+)", _read(ENGINE))
    assert web and py, "floor constant not found in both layers"
    assert int(web.group(1)) == int(py.group(1)) == 30


def test_no_panel_defines_its_own_floor() -> None:
    """The recurrence guard. A local `const MIN_SETTLED_FOR_CI = ...` is how
    the two surfaces drifted apart; importing the name is fine, redefining it
    is not."""
    for panel in (SHARED_PANEL, NFL_PANEL):
        src = _read(panel)
        assert not re.search(r"const\s+MIN_SETTLED_FOR_CI\s*=", src), (
            f"{panel.name} redefines the floor locally — import it instead"
        )


def test_both_panels_consume_the_shared_policy() -> None:
    for panel in (SHARED_PANEL, NFL_PANEL):
        assert "@/lib/alert-audit-policy" in _read(panel), (
            f"{panel.name} must import the shared audit policy"
        )


def test_nfl_table_gates_every_derived_rate() -> None:
    """Raw counts are always allowed; derived rates are not. "2-6" is an
    observation, "25.0%" is an inference the sample cannot support."""
    src = _read(NFL_PANEL)
    block = src.split("Sharp-signal accrual")[1]
    for field in ("row.winRate", "row.beatClose", "row.avgClvPp"):
        occurrences = block.count(field)
        assert occurrences, f"{field} no longer rendered — test needs updating"
        # Each must sit behind the disclosure gate.
        assert re.search(rf"d\.disclosable\s*\?[^:]*{re.escape(field)}", block), (
            f"{field} is rendered without checking disclosure()"
        )
    assert "multiplicityNote" in src, "the multiplicity warning must be present"


def test_policy_is_importable_by_client_components() -> None:
    """Both consumers are "use client"; a `server-only` import would break the
    build. This is pure display policy over already-fetched rows."""
    assert 'import "server-only"' not in _read(POLICY)
    for panel in (SHARED_PANEL, NFL_PANEL):
        assert _read(panel).lstrip().startswith('"use client"')
