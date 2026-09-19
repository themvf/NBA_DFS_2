"""Is every scheduled pipeline actually still writing data?

This exists because the same failure has now happened four times in this repo,
and each time it was invisible for days or weeks:

* `ff_injuries` raised on a round-tripped date and took the whole fantasy
  refresh down for **13 consecutive runs / 5 days**. The board silently froze.
* MLB health gates failed **19 of 30 runs** across ~11 days, skipping prop
  capture entirely, because a later step inherited `success()`.
* Both DeepSeek workflows failed on **every run since they shipped** for a
  missing `httpx`.
* `nfl_dfs_shadow` has failed since 2026-09-17 on a deliberate study-pin guard
  ("Baseline implementation drifted"), which correctly asked for a re-run that
  nobody performed.

**The instrument has to be the data, not the workflow status, because workflow
status lies in both directions.** The MLB case was a workflow that went GREEN
while writing nothing, since the steps it needed were skipped. The NFL case is
a workflow that goes RED while writing everything that matters, since the step
that fails is unrelated research. Anyone watching the red X learns nothing in
either direction. `max(timestamp)` on the table the pipeline is supposed to
fill cannot be argued with.

Mirrors the shape of `model/line_alerts.py::check_detector_health()`, which
solved the analogous "is this detector dead or just quiet" problem, including
its most important lesson: a pipeline that is legitimately dormant (a sport out
of season) must never be reported as broken, or the page becomes noise and
stops being read.

Usage:
    python -m model.pipeline_health --report      # print, write nothing
    python -m model.pipeline_health --record      # print and persist a snapshot
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Sequence

from psycopg2.extras import Json

from config import load_config
from db.database import DatabaseManager

CHECK_VERSION = "pipeline-health-v1"


@dataclass(frozen=True)
class Dataset:
    """One thing a scheduled job is supposed to keep current.

    `max_age_hours` is deliberately loose against the owning cron. GitHub's
    scheduler is documented in CLAUDE.md as firing 60-95 minutes late during
    busy hours and dropping overnight slots entirely, so a threshold set to the
    nominal interval would cry wolf constantly and train everyone to ignore
    this page -- which is the failure mode it exists to prevent. Roughly 2-3
    missed runs is the bar.

    `season_months` gates the check: a sport out of season is DORMANT, not
    broken. Empty means all year.
    """
    key: str
    label: str
    table: str
    timestamp_column: str
    max_age_hours: float
    owner_workflow: str
    season_months: tuple[int, ...] = ()
    note: str = ""


# Seasons as calendar months, stated rather than inferred. Wrong-by-a-few-weeks
# at the edges is fine: the cost of a late flag is a day of silence, the cost of
# a false flag is the page being ignored.
_NFL = (9, 10, 11, 12, 1, 2)
_MLB = (3, 4, 5, 6, 7, 8, 9, 10)
_NBA = (10, 11, 12, 1, 2, 3, 4, 5, 6)
_TENNIS = (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)
_FANTASY_DRAFT = (7, 8, 9)

DATASET_REGISTRY: tuple[Dataset, ...] = (
    Dataset("nfl_projections", "NFL DFS projections", "nfl_dfs_projection_runs", "as_of_at",
            36, "refresh_nfl_dfs_projections.yml", _NFL,
            "Feeds the DFS Lab and the six player topics on the specials board."),
    Dataset("nfl_schedule_odds", "NFL schedule + market lines", "nfl_season_games", "source_captured_at",
            36, "refresh_nfl_vegas.yml", _NFL,
            "Feeds survivor, pick'em and the specials board's team/game topics."),
    Dataset("nfl_win_probs", "NFL win probabilities", "nfl_game_win_probs", "computed_at",
            120, "refresh_nfl_survivor.yml", _NFL),
    Dataset("nfl_specials_board", "NFL specials board", "nfl_specials_runs", "generated_at",
            120, "refresh_nfl_specials.yml", _NFL),
    Dataset("nfl_pbp", "NFL play-by-play archetypes", "nfl_pbp_archetypes", "labelled_at",
            240, "refresh_nfl_pbp_archetypes.yml", _NFL),
    Dataset("odds_history", "Game odds history", "game_odds_history", "captured_at",
            6, "capture_odds_history.yml", (),
            "Every sport's line-movement trail; the single largest Odds API consumer."),
    Dataset("mlb_schedule", "MLB schedule + odds", "mlb_matchups", "fetched_at",
            36, "refresh_mlb_vegas.yml", _MLB),
    Dataset("mlb_props", "MLB player-prop odds", "prop_odds_history", "captured_at",
            24, "refresh_mlb_vegas.yml", _MLB),
    Dataset("mlb_beat", "MLB beat-writer articles", "mlb_beat_articles", "scraped_at",
            12, "refresh_mlb_beat_articles.yml", _MLB),
    Dataset("tennis_matches", "Tennis matches", "tennis_matches", "fetched_at",
            24, "refresh_tennis.yml", _TENNIS),
    Dataset("youtube_picks", "YouTube picks videos", "youtube_pick_videos", "scraped_at",
            12, "refresh_youtube_picks.yml"),
    Dataset("ff_board", "Fantasy football draft board", "ff_ranking_sets", "created_at",
            36, "fantasy_football_refresh.yml", _FANTASY_DRAFT),
)

FRESH, STALE, EMPTY, DORMANT = "fresh", "stale", "empty", "dormant"


@dataclass(frozen=True)
class Health:
    dataset: Dataset
    status: str
    last_row_at: datetime | None
    age_hours: float | None

    @property
    def detail(self) -> str:
        if self.status == DORMANT:
            return f"out of season; not expected to run in month {datetime.now(timezone.utc).month}"
        if self.status == EMPTY:
            return "no rows at all"
        assert self.age_hours is not None
        budget = self.dataset.max_age_hours
        if self.status == STALE:
            missed = self.age_hours / budget
            return (f"last write {_age(self.age_hours)} ago, budget {budget:g}h "
                    f"({missed:.1f}x over) - {self.dataset.owner_workflow}")
        return f"last write {_age(self.age_hours)} ago, budget {budget:g}h"


def _age(hours: float) -> str:
    if hours < 1:
        return f"{hours * 60:.0f}m"
    if hours < 48:
        return f"{hours:.1f}h"
    return f"{hours / 24:.1f}d"


def in_season(dataset: Dataset, now: datetime) -> bool:
    return not dataset.season_months or now.month in dataset.season_months


def classify(dataset: Dataset, last_row_at: datetime | None, now: datetime) -> Health:
    """Pure: the whole decision, so it is testable without a database."""
    if not in_season(dataset, now):
        return Health(dataset, DORMANT, last_row_at, None)
    if last_row_at is None:
        return Health(dataset, EMPTY, None, None)
    if last_row_at.tzinfo is None:
        last_row_at = last_row_at.replace(tzinfo=timezone.utc)
    age = (now - last_row_at).total_seconds() / 3600
    return Health(dataset, STALE if age > dataset.max_age_hours else FRESH, last_row_at, age)


def check_all(db: DatabaseManager, now: datetime | None = None) -> list[Health]:
    now = now or datetime.now(timezone.utc)
    results: list[Health] = []
    for dataset in DATASET_REGISTRY:
        try:
            row = db.execute_one(
                f"SELECT MAX({dataset.timestamp_column}) AS last_at FROM {dataset.table}"  # noqa: S608
            )
            last = row["last_at"] if row else None
        except Exception as exc:  # a missing table is a finding, not a crash
            results.append(Health(dataset, EMPTY, None, None))
            print(f"  ! {dataset.key}: {exc}", file=sys.stderr)
            continue
        results.append(classify(dataset, last, now))
    return results


def record(db: DatabaseManager, results: Sequence[Health], now: datetime | None = None) -> int:
    now = now or datetime.now(timezone.utc)
    db.execute_many(
        """INSERT INTO pipeline_health_snapshots
             (checked_at, check_version, dataset_key, label, status, last_row_at,
              age_hours, max_age_hours, owner_workflow, detail_json)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
        [(now, CHECK_VERSION, h.dataset.key, h.dataset.label, h.status, h.last_row_at,
          h.age_hours, h.dataset.max_age_hours, h.dataset.owner_workflow,
          Json({"detail": h.detail, "note": h.dataset.note, "table": h.dataset.table}))
         for h in results],
    )
    return len(results)


def report(results: Sequence[Health]) -> str:
    order = {STALE: 0, EMPTY: 1, FRESH: 2, DORMANT: 3}
    lines = [f"pipeline health  ({CHECK_VERSION})", ""]
    for h in sorted(results, key=lambda h: (order[h.status], h.dataset.key)):
        mark = {STALE: "STALE  ", EMPTY: "EMPTY  ", FRESH: "ok     ", DORMANT: "dormant"}[h.status]
        lines.append(f"  {mark} {h.dataset.label:32} {h.detail}")
    broken = [h for h in results if h.status in (STALE, EMPTY)]
    lines += ["", f"  {len(broken)} needing attention of {len(results)} datasets"]
    return "\n".join(lines)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--record", action="store_true", help="persist a snapshot as well as printing")
    parser.add_argument("--fail-on-stale", action="store_true",
                        help="exit non-zero when anything is stale (off by default: this "
                             "monitor reporting red would be one more red nobody reads)")
    args = parser.parse_args(argv)

    db = DatabaseManager(load_config().database_url)
    results = check_all(db)
    print(report(results))
    if args.record:
        print(f"  recorded {record(db, results)} rows")
    if args.fail_on_stale and any(h.status in (STALE, EMPTY) for h in results):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
