"""Where the study's code and its (untracked) data live.

Code is in research/nba_top5_study. The data files (line captures, box scores, injury reports,
stat-api rows, simulation caches) are NOT in git: they sit in data/nba_top5_study of the main
checkout, or wherever NBA_TOP5_DATA points. Rebuild them with build_dataset.py and proj/dump*.py.
"""
import os
import sys
from pathlib import Path

STUDY = Path(__file__).resolve().parent
REPO = STUDY.parents[1]


def _data_root() -> Path:
    env = os.environ.get("NBA_TOP5_DATA")
    if env:
        return Path(env)
    here = REPO / "data" / "nba_top5_study"
    if here.exists():
        return here
    for parent in REPO.parents:                     # a git worktree: use the main checkout's copy
        cand = parent / "data" / "nba_top5_study"
        if parent.name != ".worktrees" and cand.exists():
            return cand
    return here


DATA_ROOT = _data_root()
for p in (str(REPO), str(STUDY)):
    if p not in sys.path:
        sys.path.insert(0, p)


def load_env() -> None:
    from dotenv import load_dotenv
    for base in (REPO, *REPO.parents):
        if (base / ".env").exists():
            load_dotenv(base / ".env")
            return
