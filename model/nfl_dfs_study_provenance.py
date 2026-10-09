"""Provenance stamp for research study outputs (WP10).

The three 2026-09-21 studies recorded n=1,136 team-games; a re-run the next
day found 1,150 because Sunday's games had been labelled overnight. Nothing
was wrong, but nothing in the output said which dataset the numbers came
from either. Every study report now carries:

    dataset_digest     sha256 of the exact rows it scored (same helper the
                       workload ingest uses for its immutable dataset)
    max_labelled_week  the latest (season, week) with a label in those rows
    git_sha            the commit the script ran from, or None outside git
"""

from __future__ import annotations

import subprocess
from typing import Iterable, Mapping

from model.nfl_dfs_historical import artifact_digest


def git_sha() -> str | None:
    try:
        out = subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, text=True, timeout=5)
    except (OSError, subprocess.SubprocessError):
        return None
    return out.stdout.strip() or None if out.returncode == 0 else None


def provenance(rows: Iterable[Mapping], *, season_key: str = "season", week_key: str = "week") -> dict:
    rows = list(rows)
    labelled = [(int(r[season_key]), int(r[week_key])) for r in rows
                if r.get(season_key) is not None and r.get(week_key) is not None]
    return {
        "dataset_digest": artifact_digest(rows),
        "dataset_rows": len(rows),
        "max_labelled_week": list(max(labelled)) if labelled else None,
        "git_sha": git_sha(),
    }
