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
import json
from pathlib import Path

from psycopg2.extras import Json

from config import load_config
from db.database import DatabaseManager

# v3 (2026-09-29): the third silent-failure audit. Shared tables are watched per
# sport (`filter`), append-only capture tables can require a minimum number of
# rows in a window (`min_rows`), and every scheduled workflow that writes a
# table a page or a downstream job reads now has a row here. The set of dataset
# keys changed, so the version did.
CHECK_VERSION = "pipeline-health-v3"


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

    `schedule_step` names the step in `owner_workflow` that writes the table.
    When that step's `if:` lets it run only on manual dispatch, the pipeline
    was switched off on purpose and a stale table is reported as DORMANT with
    the gate quoted, not as STALE. The gate is read from the workflow file on
    every check, so removing the `if:` turns STALE reporting back on by itself.

    `filter` is a SQL WHERE fragment (`sport = 'mlb'`) for tables shared across
    sports. One MAX over `game_odds_history` cannot see one sport dying while
    another keeps capturing, so each sport gets its own row.

    `min_rows` with `window_hours` adds a floor for append-only capture tables
    whose newest row can move on a trivial write: at least this many rows must
    carry a timestamp inside the window, or the dataset reads STALE with the
    count in the detail. Only for datasets whose in-season volume is
    predictable; it is a floor, never an expected count.

    The timestamp column must be one the writer stamps on EVERY successful
    run. A UNIQUE-keyed upsert whose ON CONFLICT list omits the timestamp, or
    a checksum-keyed `DO NOTHING` insert, cannot tell "refreshed" from
    "untouched"; such tables are not registered, and the note on the nearest
    honest signal says so.
    """
    key: str
    label: str
    table: str
    timestamp_column: str
    max_age_hours: float
    owner_workflow: str
    season_months: tuple[int, ...] = ()
    note: str = ""
    schedule_step: str = ""
    filter: str = ""
    min_rows: int = 0
    window_hours: float = 0.0

    def __post_init__(self) -> None:
        if (self.min_rows > 0) != (self.window_hours > 0):
            raise ValueError(f"{self.key}: min_rows and window_hours go together")


# Seasons as calendar months, stated rather than inferred. Wrong-by-a-few-weeks
# at the edges is fine: the cost of a late flag is a day of silence, the cost of
# a false flag is the page being ignored.
_NFL = (9, 10, 11, 12, 1, 2)
_MLB = (3, 4, 5, 6, 7, 8, 9, 10)      # World Series can run into the first days of November; those days go unwatched
_NBA = (10, 11, 12, 1, 2, 3, 4, 5, 6)
_TENNIS = (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)
_NHL = (10, 11, 12, 1, 2, 3, 4, 5, 6)  # regular season opens early October; only regular/playoff games are stored
_NHL_GRADED = (11, 12, 1, 2, 3, 4, 5, 6)  # the first settled alert needs a verified close and a final; October would read EMPTY
_CFB = (8, 9, 10, 11, 12, 1)           # schedule and box-score refreshes write regardless of game gaps
_CFB_GAMES = (9, 10, 11)               # checkpoint captures follow kickoffs; December bowl gaps run a week
_FANTASY_DRAFT = (7, 8, 9)
_FF_ACTIVE = (7, 8, 9, 10, 11, 12, 1, 2)  # draft board in summer, roster/depth evidence for NFL DFS through February

# Budgets against the clock that actually starts the job. Vercel-dispatched
# jobs fire on the minute; GitHub-cron-only jobs start every ~4-8 h whatever
# their cron says (CLAUDE.md, measured 2026-09-26..29), so a GitHub daily is
# ~26 h apart and a GitHub 6-hourly ~6-8 h. "2-3 missed runs" is measured
# against that, not the cron text.
DATASET_REGISTRY: tuple[Dataset, ...] = (
    # ---- NFL DFS: production ------------------------------------------------
    Dataset("nfl_projections", "NFL DFS projections", "nfl_dfs_projection_runs", "as_of_at",
            36, "refresh_nfl_dfs_projections.yml", _NFL,
            "Feeds the DFS Lab and the six player topics on the specials board. Written by both the "
            "projection rebuild and the hourly availability job, so this says a snapshot exists, not "
            "which workflow made it; the matchup-forecast row below is the projection workflow's own mark."),
    Dataset("nfl_matchup_forecasts", "NFL matchup forecasts (projection run)", "nfl_fact_releases", "published_at",
            36, "refresh_nfl_dfs_projections.yml", _NFL,
            "Defensive/opponent context frozen by the projection workflow's matchup step on every run with "
            "an unstarted game in the target week; no other job writes this key.",
            filter="dataset_key = 'nfl_matchup'"),
    Dataset("nfl_pfr_evidence", "NFL PFR matchup evidence", "nfl_pfr_game_snapshots", "recorded_at",
            36, "refresh_nfl_dfs_projections.yml", _NFL,
            "Pro-Football-Reference charting behind the matchup forecasts. Its step is continue-on-error, "
            "so a failed download leaves the run green and the build on old evidence; every good run "
            "re-records every charted game."),
    Dataset("nfl_fantasypros_injuries", "NFL game-week injury report (FantasyPros)", "ff_source_snapshots", "fetched_at",
            72, "refresh_nfl_dfs_projections.yml", _NFL,
            "Optional input captured with continue-on-error by the projection and availability jobs; a new "
            "row appears only when the report changes, which in season is most days (week key rolls on Tuesday).",
            filter="source = 'fantasypros' AND dataset LIKE 'game-week-injuries-v2-%'"),
    Dataset("nfl_sleeper_capture", "NFL injury/depth capture (Sleeper)", "ff_source_snapshots", "fetched_at",
            8, "refresh_nfl_availability_context.yml", _NFL,
            "Hourly dispatch; the job's own gate captures at least every two hours in season, so fewer "
            "than six captures a day means the capture path is being skipped.",
            filter="source = 'sleeper' AND dataset LIKE 'players-live-%'", min_rows=6, window_hours=24),
    Dataset("nfl_dk_pool", "NFL DraftKings pool status polls", "nfl_dfs_dk_pool_polls", "polled_at",
            36, "refresh_nfl_dk_pool.yml", _NFL,
            "One heartbeat row per draft group per poll that read DraftKings (ok = true). Tuesday and "
            "Wednesday poll twice a day; game windows every 30 minutes. No row is written when no draft "
            "group starts within the poll window.",
            filter="ok"),
    Dataset("nfl_dk_pool_changes", "NFL DraftKings pool changes", "nfl_dfs_dk_pool_snapshots", "captured_at",
            96, "refresh_nfl_dk_pool.yml", _NFL,
            "A snapshot is stored only when DraftKings changed a status, salary or roster. In season that "
            "happens most days (new groups on Tuesday, injury flips through the week), so four quiet days "
            "means the change detection is broken, not that DraftKings went quiet."),
    # ---- NFL DFS: post-week and research ------------------------------------
    Dataset("nfl_week_results", "NFL realized DK points (post-week)", "nfl_dfs_player_week_results", "computed_at",
            192, "refresh_nfl_dfs_postweek.yml", _NFL,
            "Append-only by input digest: an unchanged week inserts nothing, a scored week inserts hundreds "
            "of rows on Tuesday/Wednesday. Also written by the projection workflow's results phase. Eight "
            "days without a row means last week's games were never scored (the PHI@CHI case)."),
    Dataset("nfl_replacement_upside_grade", "NFL replacement-upside weekly grade", "nfl_replacement_upside_grade_runs", "evaluated_at",
            192, "refresh_nfl_dfs_postweek.yml", _NFL,
            "One row per post-week run (Tuesday and Wednesday), blinded until its floors are met; shown on /dfs/nfl/results."),
    Dataset("nfl_shadow", "NFL shadow research forecasts", "nfl_dfs_shadow_predictions", "captured_at",
            240, "refresh_nfl_dfs_research.yml", _NFL,
            "Weekly pregame evidence; separate from production projection freshness."),
    Dataset("nfl_report_cards", "NFL slate report cards", "nfl_dfs_slate_report_cards", "created_at",
            36, "refresh_nfl_dfs_research.yml", _NFL,
            "Projections graded against what DraftKings paid, re-evaluated on every research and post-week "
            "run for every scorable upload; the grader for the zero-history prior study."),
    Dataset("nfl_pickem_forecasts", "NFL pick'em matchup forecasts", "nfl_pickem_matchup_forecasts", "available_at",
            48, "refresh_nfl_dfs_research.yml", _NFL,
            "Frozen per pregame game on every research run; the 5-minute close worker and the daily Vegas "
            "refresh add rows only when a quote moved."),
    # ---- NFL: schedule, lines, survivor, specials, play-by-play ----------------
    Dataset("nfl_schedule_odds", "NFL schedule + market lines", "nfl_season_games", "source_captured_at",
            36, "refresh_nfl_vegas.yml", _NFL,
            "Feeds survivor, pick'em and the specials board's team/game topics. Stamped on every load by the "
            "Vegas, survivor, post-week and projection jobs alike, so it says the schedule was reloaded, not by whom."),
    Dataset("nfl_survivor_lines", "NFL survivor market lines", "nfl_season_games", "market_captured_at",
            168, "refresh_nfl_survivor.yml", _NFL,
            "Season-wide moneylines/spreads from the Tuesday and Thursday survivor refresh. A missing "
            "ODDS_API_KEY is logged as SKIP and the run stays green, which only this row would show."),
    Dataset("nfl_survivor_pick_share", "NFL survivor pick share", "survivor_pick_popularity", "captured_at",
            168, "refresh_nfl_survivor.yml", _NFL,
            "Stamped for every published week on each survivor run; if the source 404s for every week the "
            "run stays green and writes nothing."),
    Dataset("nfl_win_probs", "NFL win probabilities", "nfl_game_win_probs", "computed_at",
            120, "refresh_nfl_survivor.yml", _NFL,
            "Every game on Tuesday/Thursday; the daily Vegas refresh restamps only that day's games."),
    Dataset("nfl_specials_board", "NFL specials board", "nfl_specials_runs", "generated_at",
            120, "refresh_nfl_specials.yml", _NFL),
    Dataset("nfl_pbp", "NFL play-by-play archetypes", "nfl_pbp_archetypes", "labelled_at",
            240, "refresh_nfl_pbp_archetypes.yml", _NFL,
            "Rows are rewritten only for games the release changed; a quiet week writes nothing, hence the week-plus budget."),
    # ---- Odds captures, one row per sport -------------------------------------
    # One MAX over the shared table cannot see one sport dying while another keeps
    # capturing (NFL checkpoints would have hidden a dead MLB capture), so each
    # sport is watched on its own months and cadence. The any-sport row keeps a
    # floor on total volume: the shared table gets hundreds of rows a day in any
    # month, so a day with fewer than 24 is the capture machinery, not a quiet day.
    Dataset("odds_history", "Game odds history (any sport)", "game_odds_history", "captured_at",
            6, "capture_odds_history.yml", (),
            "Every sport's line-movement trail; the single largest Odds API consumer.",
            min_rows=24, window_hours=24),
    Dataset("odds_mlb", "MLB odds captures", "game_odds_history", "captured_at",
            36, "capture_odds_history.yml", _MLB,
            "Dispatched every 30 minutes 14:00-03:59 UTC; nothing is bought when no game is upcoming, so an "
            "off day (All-Star break, playoff travel days) is a day without rows, which the budget allows for.",
            filter="sport = 'mlb'"),
    Dataset("odds_nfl", "NFL odds captures", "game_odds_history", "captured_at",
            36, "capture_event_closes.yml", _NFL,
            "Daily checkpoints from a week before kickoff plus the daily Vegas refresh; something is due every day in season.",
            filter="sport = 'nfl'"),
    Dataset("odds_nhl", "NHL odds captures", "game_odds_history", "captured_at",
            72, "capture_event_closes.yml", _NHL,
            "Checkpoint captures from seven days out; the budget covers the Christmas and All-Star breaks.",
            filter="sport = 'nhl'"),
    Dataset("odds_cfb", "CFB odds captures", "game_odds_history", "captured_at",
            120, "capture_event_closes.yml", _CFB_GAMES,
            "Checkpoints start 48 hours before kickoff, so Sunday-Tuesday is quiet by design (about 65 hours); "
            "December's bowl gaps run a week, so December is not watched here.",
            filter="sport = 'cfb'"),
    # ---- Verified closes and signal grading, per sport -------------------------
    Dataset("closes_mlb", "MLB verified closing lines", "event_closing_lines", "frozen_at",
            72, "capture_event_closes.yml", _MLB,
            "One frozen close per game once it starts; feeds the CLV harness and the terminal's close column. Playoff off-days write none.",
            filter="sport = 'mlb'"),
    Dataset("closes_nfl", "NFL verified closing lines", "event_closing_lines", "frozen_at",
            240, "capture_event_closes.yml", _NFL,
            "Frozen at kickoff, so rows land Thursday, Sunday and Monday.",
            filter="sport = 'nfl'"),
    Dataset("closes_nhl", "NHL verified closing lines", "event_closing_lines", "frozen_at",
            72, "capture_event_closes.yml", _NHL,
            "The close worker swallows NHL freeze exceptions, so a dead freeze shows only here.",
            filter="sport = 'nhl'"),
    Dataset("mlb_signal_grading", "MLB signal grading", "line_alerts", "settled_at",
            120, "refresh_mlb_terminal_settlement.yml", _MLB,
            "Set once per alert when a final score grades it (about 25 a day in season); the hourly "
            "settlement job writes no run row, so this is its only trace.",
            filter="sport = 'mlb'"),
    Dataset("nfl_signal_grading", "NFL signal grading", "line_alerts", "settled_at",
            240, "capture_event_closes.yml", _NFL,
            "Alerts settle after a verified close and a final score, so rows land after each game day.",
            filter="sport = 'nfl'"),
    Dataset("nhl_signal_grading", "NHL signal grading", "line_alerts", "settled_at",
            120, "refresh_nhl_terminal.yml", _NHL_GRADED,
            "Detectors enabled 2026-09-29; graded nightly once closes and finals exist.",
            filter="sport = 'nhl'"),
    # ---- MLB ------------------------------------------------------------------
    Dataset("mlb_schedule", "MLB schedule + odds", "mlb_matchups", "fetched_at",
            36, "refresh_mlb_vegas.yml", _MLB,
            "Stamped on every schedule refresh whether or not anything changed."),
    Dataset("mlb_bets_rated", "MLB bet ledger snapshots", "mlb_bet_snapshots", "captured_at",
            72, "refresh_mlb_vegas.yml", _MLB,
            "One snapshot per rated pregame bet on each of the three daily refreshes; an off day writes none, and playoff gaps run two days."),
    # Paused on the schedule since 2026-08-24 (Odds API quota, cb326f5): last
    # write 2026-08-23 13:47 UTC. Reported as a pause, not an outage.
    Dataset("mlb_props", "MLB player-prop odds", "prop_odds_history", "captured_at",
            24, "refresh_mlb_vegas.yml", _MLB, schedule_step="Capture MLB player-prop odds"),
    Dataset("mlb_team_stats_history", "MLB team stats (daily snapshot)", "mlb_team_stats_history", "available_at",
            72, "refresh_mlb_stats.yml", _MLB,
            "Append-only, one row per team per run. The current-state table mlb_team_stats is NOT watched: "
            "the FanGraphs path that updates it has failed (403) since April and the official-API fallback "
            "writes only this snapshot, so its fetched_at has been frozen since 2026-04-06 while the run is green."),
    Dataset("mlb_pitcher_stats_history", "MLB pitcher stats (daily snapshot)", "mlb_pitcher_stats_history", "available_at",
            72, "refresh_mlb_stats.yml", _MLB,
            "Append-only, one row per pitcher per run. Both fetch paths have written zero pitchers since "
            "2026-07-12 while printing 'Pitcher stats: 0 pitchers upserted' and exiting 0."),
    Dataset("mlb_batter_stats", "MLB batter stats", "mlb_batter_stats", "fetched_at",
            72, "refresh_mlb_stats.yml", _MLB,
            "Upsert stamps fetched_at on every successful write; there is no batter history table."),
    Dataset("mlb_beat", "MLB beat-writer articles", "mlb_beat_articles", "scraped_at",
            12, "refresh_mlb_beat_articles.yml", _MLB),
    Dataset("polymarket_positions", "Polymarket watchlist positions", "polymarket_watchlist_captures", "completed_at",
            72, "refresh_polymarket_watchlist.yml", _MLB,
            "Daily display snapshot of the frozen wallet cohort; the capture row is claimed before fetching, "
            "so it is the job's heartbeat."),
    Dataset("polymarket_forward", "Polymarket watchlist forward scoring", "polymarket_watchlist_forward", "scored_at",
            240, "refresh_polymarket_watchlist.yml", _MLB,
            "Weekly (Monday) scoring of markets that started after the freeze; nothing is written when no such market exists."),
    # ---- Tennis (left as it was: another owner) ---------------------------------
    Dataset("tennis_matches", "Tennis matches", "tennis_matches", "fetched_at",
            24, "refresh_tennis.yml", _TENNIS),
    # ---- NHL ------------------------------------------------------------------
    Dataset("nhl_schedule", "NHL schedule + finals", "nhl_matchups", "fetched_at",
            24, "refresh_nhl_terminal.yml", _NHL,
            "Canonical games behind the /nhl line terminal; the hourly scores pass restamps every game in its "
            "window. GitHub starts the hourly cron every 4-8 h, hence the day of slack."),
    # ---- CFB ------------------------------------------------------------------
    Dataset("cfb_schedule", "CFB schedule + scores", "cfb_matchups", "fetched_at",
            24, "refresh_cfb_terminal.yml", _CFB,
            "Whole-season refresh every 6 h and recent-week scores hourly, both restamping every row touched."),
    Dataset("cfb_player_stats", "CFB player box scores", "cfb_player_game_boxes", "fetched_at",
            72, "cfb_player_games.yml", _CFB,
            "Every completed week is re-fetched daily (the DK-points table it feeds has no timestamp of its own)."),
    Dataset("cfb_team_features", "CFB team features (weekly research)", "cfb_team_game_features", "as_of_at",
            240, "refresh_cfb_research.yml", (8, 9, 10, 11, 12),
            "Thursday point-in-time features for upcoming games; new rows every run while games remain."),
    # ---- YouTube (left as it was: another owner) --------------------------------
    Dataset("youtube_picks", "YouTube picks videos", "youtube_pick_videos", "scraped_at",
            12, "refresh_youtube_picks.yml"),
    # ---- Fantasy football -----------------------------------------------------
    Dataset("ff_board", "Fantasy football draft board", "ff_ranking_sets", "created_at",
            36, "fantasy_football_refresh.yml", _FANTASY_DRAFT,
            "A new set is created only when the board digest changes, which in draft season is most runs."),
    Dataset("ff_roster_refresh", "Fantasy football roster refresh", "ff_player_season_features", "fetched_at",
            36, "fantasy_football_refresh.yml", _FF_ACTIVE,
            "Stamped on every successful board refresh and by nothing else; ff_players is also touched hourly "
            "by the NFL availability job, so it cannot stand in. The refresh feeds the NFL DFS optimizer's "
            "roster and depth evidence through February."),
    Dataset("ff_adp_snapshot", "Fantasy football ADP snapshot", "ff_adp_snapshots", "captured_at",
            36, "refresh_ff_adp_snapshot.yml", (7, 8),
            "Twice daily in draft season; stops on purpose after the last Week 1 kickoff, so it is watched "
            "in July and August only."),
)

FRESH, STALE, EMPTY, DORMANT = "fresh", "stale", "empty", "dormant"


@dataclass(frozen=True)
class Health:
    dataset: Dataset
    status: str
    last_row_at: datetime | None
    age_hours: float | None
    detail_override: str | None = None

    @property
    def detail(self) -> str:
        if self.detail_override is not None:
            return self.detail_override
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


WORKFLOW_DIR = Path(__file__).resolve().parent.parent / ".github" / "workflows"


def manual_only_gate(workflow: str, step_prefix: str, workflow_dir: Path = WORKFLOW_DIR) -> str | None:
    """The step's `if:` when it can run only on manual dispatch, else None.

    A small line reader rather than a YAML dependency: it finds the
    `- name:` item starting with `step_prefix` and reads that item's `if:`.
    A condition on `inputs.` or `workflow_dispatch` that never mentions
    `schedule` is false on every scheduled run.
    """
    try:
        lines = (workflow_dir / workflow).read_text(encoding="utf-8").splitlines()
    except OSError:
        return None
    for index, line in enumerate(lines):
        stripped = line.strip()
        if not stripped.startswith("- name:"):
            continue
        name = stripped[len("- name:"):].strip().strip("\"'")
        if not name.startswith(step_prefix):
            continue
        indent = len(line) - len(line.lstrip())
        for follow in lines[index + 1:]:
            text = follow.strip()
            if text.startswith("- ") and len(follow) - len(follow.lstrip()) <= indent:
                break
            if text.startswith("if:"):
                condition = text[3:].strip()
                manual = "inputs." in condition or "workflow_dispatch" in condition
                return condition if manual and "schedule" not in condition else None
        return None
    return None


def in_season(dataset: Dataset, now: datetime) -> bool:
    return not dataset.season_months or now.month in dataset.season_months


def classify(dataset: Dataset, last_row_at: datetime | None, now: datetime,
             schedule_gate: str | None = None, rows_in_window: int | None = None) -> Health:
    """Pure: the whole decision, so it is testable without a database.

    `schedule_gate` is the manual-only `if:` guarding the writing step (see
    `manual_only_gate`). A fresh table wins over the gate: a manual run that
    wrote data is simply fresh.

    `rows_in_window` is how many rows carry a timestamp inside the dataset's
    `window_hours`; with `min_rows` set, fewer than that is STALE even when the
    newest row is recent. The age is withheld on that row (None) so the
    reading reads as the count, not as "0.1x its budget".
    """
    if not in_season(dataset, now):
        return Health(dataset, DORMANT, last_row_at, None)
    if last_row_at is not None and last_row_at.tzinfo is None:
        last_row_at = last_row_at.replace(tzinfo=timezone.utc)
    age = (now - last_row_at).total_seconds() / 3600 if last_row_at is not None else None
    if schedule_gate and (age is None or age > dataset.max_age_hours):
        last = f"last write {_age(age)} ago ({last_row_at:%Y-%m-%d})" if age is not None else "no rows yet"
        return Health(dataset, DORMANT, last_row_at, age,
                      f"paused on the schedule: '{dataset.schedule_step}' in {dataset.owner_workflow} "
                      f"runs only when `{schedule_gate}`; {last}")
    if last_row_at is None:
        return Health(dataset, EMPTY, None, None)
    if age > dataset.max_age_hours:
        return Health(dataset, STALE, last_row_at, age)
    if dataset.min_rows and rows_in_window is not None and rows_in_window < dataset.min_rows:
        return Health(dataset, STALE, last_row_at, None,
                      f"only {rows_in_window} rows in the last {dataset.window_hours:g}h, expected at least "
                      f"{dataset.min_rows}; newest row {_age(age)} ago - {dataset.owner_workflow}")
    return Health(dataset, FRESH, last_row_at, age)


def freshness_sql(dataset: Dataset) -> str:
    """The one read per dataset: newest timestamp, plus the windowed count when a floor is set.

    Returned ready for `cursor.execute(sql, ())`: psycopg2 still scans for
    `%` placeholders with an empty parameter tuple, so a `LIKE 'x%'` in a
    filter is doubled here rather than in every registry entry.
    """
    where = f" WHERE {dataset.filter}" if dataset.filter else ""
    count = (f", COUNT(*) FILTER (WHERE {dataset.timestamp_column} > NOW() - INTERVAL '{dataset.window_hours:g} hours')"
             f" AS in_window" if dataset.min_rows else "")
    sql = f"SELECT MAX({dataset.timestamp_column}) AS last_at{count} FROM {dataset.table}{where}"  # noqa: S608
    return sql.replace("%", "%%")


def check_all(db: DatabaseManager, now: datetime | None = None) -> list[Health]:
    now = now or datetime.now(timezone.utc)
    results: list[Health] = []
    for dataset in DATASET_REGISTRY:
        try:
            row = db.execute_one(freshness_sql(dataset))
            last = row["last_at"] if row else None
            in_window = int(row["in_window"]) if row and dataset.min_rows else None
        except Exception as exc:  # a missing table is a finding, not a crash
            results.append(Health(dataset, EMPTY, None, None))
            print(f"  ! {dataset.key}: {exc}", file=sys.stderr)
            continue
        gate = manual_only_gate(dataset.owner_workflow, dataset.schedule_step) if dataset.schedule_step else None
        results.append(classify(dataset, last, now, gate, in_window))
    results.extend(check_nfl_context_freezes(db, now))
    return results


def check_nfl_context_freezes(db, now):
    """A recently written shadow table can still be missing its context bundle."""
    dataset = Dataset("nfl_context_variant_freeze", "NFL context variant freeze", "nfl_dfs_shadow_predictions",
                      "captured_at", 168, "refresh_nfl_dfs_research.yml", _NFL,
                      "Current study pin; zero context rows by Saturday 21:35 UTC fails.")
    if not in_season(dataset, now):
        return [Health(dataset, DORMANT, None, None)]
    try:
        from research.nfl_matchup_study import ledger_inputs
        from model.nfl_dfs_context_variant_study import freeze_health
        config = json.loads(Path("artifacts/nfl_dfs_shadow_config.json").read_text())
        season = now.year-1 if now.month <= 3 else now.year
        games, rows, _ = ledger_inputs(db, season, now, config["study_run_id"])
        upcoming = [g for g in games if g["kickoff"] > now]
        if not upcoming:
            return [Health(dataset, DORMANT, None, None, "no upcoming regular-season week")]
        week = min(upcoming, key=lambda g: g["kickoff"])["week"]
        check = freeze_health([g for g in games if g["week"] == week], rows, config["study_run_id"], now)[0]
        status = EMPTY if check["status"] == "failure" else STALE if check["status"] == "warning" else FRESH if check["status"] == "healthy" else DORMANT
        return [Health(dataset, status, None, None, f"week {week}: {check['status']}; {check['eligible_player_weeks']} eligible player-weeks; deadline {check['deadline']}; missing started games {check['missing_started_game_ids']}")]
    except Exception as exc:
        return [Health(dataset, EMPTY, None, None, f"context freeze check failed: {type(exc).__name__}")]


def record(db: DatabaseManager, results: Sequence[Health], now: datetime | None = None) -> int:
    now = now or datetime.now(timezone.utc)
    db.execute_many(
        """INSERT INTO pipeline_health_snapshots
             (checked_at, check_version, dataset_key, label, status, last_row_at,
              age_hours, max_age_hours, owner_workflow, detail_json)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
        [(now, CHECK_VERSION, h.dataset.key, h.dataset.label, h.status, h.last_row_at,
          h.age_hours, h.dataset.max_age_hours, h.dataset.owner_workflow,
          Json({"detail": h.detail, "note": h.dataset.note, "table": h.dataset.table,
                "filter": h.dataset.filter, "timestamp_column": h.dataset.timestamp_column}))
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
