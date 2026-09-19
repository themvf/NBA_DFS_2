"""The weekly NFL specials board: our ranking and expected stat per topic.

Nine questions DK asks about a Sunday ("who leads the slate in receiving
yards", "which game is highest scoring"). This module answers each one every
week with a **ranked list and the expected stat behind it**, and persists it so
the topics accumulate a week-by-week record.

It deliberately does NOT produce probabilities, and that is a measured
decision rather than a shortcut. Over the 2023-2025 regular seasons,
walk-forward with means taken from prior weeks only, the player with the
highest projected stat actually led the week:

    most_receiving_yards   11.1%    actual leader's median rank 15
    most_passing_yards     15.6%    actual leader's median rank 11
    any touchdown (proxy)   4.4%    actual leader's median rank 25

Among ~840 candidates that is roughly 90x better than chance, so the ordering
carries real signal -- and our number one is still an underdog to lead. A
probability that says otherwise cannot be derived from a mean; it needs the
joint slate simulation (Layer A/B/C, not built). Until then the honest output
is an order, published deep enough that the eventual leader is usually on it,
with `p_leads` left NULL rather than filled with something that looks like a
probability.

Four ranking keys were screened on the same slates -- the mean, the prior max,
the prior 90th percentile, and P(>= a slate-winning threshold). None beat the
mean, so the mean stays. That screen is design selection on 45 slates, not an
edge study; it is recorded in `model/nfl_slate_specials.RANKING_STAT`.

A market price is optional. `ingest/nfl_specials_market.py` captures DK's board
when one can be pasted, but nothing here requires it: the board is what we
project, whether or not anybody is pricing the question.

Usage:
    python -m model.nfl_specials_board --season 2026 --week 3 --scope sunday_1pm
    python -m model.nfl_specials_board --season 2026 --week 3 --dry-run
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import uuid
from dataclasses import dataclass, field, replace
from datetime import datetime
from typing import Any, Iterable, Sequence
from zoneinfo import ZoneInfo

from psycopg2.extras import Json

from config import load_config
from db.database import DatabaseManager
# Reused, not reimplemented: the same "next scheduled regular-season week" the
# projection run itself targets, so a scheduled board can never drift onto a
# different week than the projections it reads.
from ingest.nfl_dfs_projections import infer_target_week
from model.nfl_dfs_research import implied_totals
from model.nfl_slate_specials import (
    ASCENDING_FAMILIES,
    BOARD_DEPTH,
    FAMILIES,
    FAMILY_POSITIONS,
    SLATE_SCOPES,
    in_scope,
    ranking_stat,
    selection_kind,
)

BOARD_MODEL_VERSION = "nfl-specials-board-v1"
BOARD_METHOD = "expected_stats"
EASTERN = ZoneInfo("America/New_York")

# Touchdown-scoring routes we count toward the first-TD proxy. Passing TDs are
# excluded on purpose: the passer does not score the touchdown.
_TD_COMPONENTS = ("rushing_tds", "receiving_tds")


@dataclass(frozen=True)
class Game:
    game_id: int
    home: str
    away: str
    kickoff: datetime | None
    total: float | None
    spread: float | None
    spread_available: bool
    source: str | None


@dataclass(frozen=True)
class Player:
    gsis_id: str | None
    name: str
    normalized_name: str
    team: str | None
    position: str
    stat_means: dict[str, Any]
    projection_status: str

    @property
    def selection_key(self) -> str:
        if self.gsis_id:
            return str(self.gsis_id)
        return f"{self.normalized_name}|{self.team or ''}"


@dataclass(frozen=True)
class Inputs:
    games: tuple[Game, ...]
    players: tuple[Player, ...]
    projection_run_id: str | None


@dataclass(frozen=True)
class BoardRow:
    family: str
    selection_key: str
    selection_label: str
    stat_key: str
    is_proxy: bool
    rank: int | None = None
    expected_value: float | None = None
    context: dict[str, Any] = field(default_factory=dict)
    status: str = "ok"
    block_reason: str | None = None


@dataclass(frozen=True)
class Board:
    season: int
    week: int
    scope: str
    rows: tuple[BoardRow, ...]
    games_in_scope: tuple[Game, ...]
    excluded: tuple[dict[str, Any], ...]
    blocked: tuple[dict[str, Any], ...]
    projection_run_id: str | None


# ── pure construction ────────────────────────────────────────────────────────

def _number(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number == number else None  # NaN check


def games_in_scope(games: Iterable[Game], scope: str) -> tuple[tuple[Game, ...], tuple[dict[str, Any], ...]]:
    """Split games by scope. Exclusion is total -- a 4:25 kickoff is simply not
    in the 1pm market, never a down-weighted member of it."""
    kept: list[Game] = []
    dropped: list[dict[str, Any]] = []
    for game in games:
        if game.kickoff is None:
            dropped.append({"game": f"{game.away}@{game.home}", "reason": "no_kickoff_time"})
            continue
        if in_scope(scope, game.kickoff.astimezone(EASTERN)):
            kept.append(game)
        else:
            dropped.append({"game": f"{game.away}@{game.home}", "reason": "out_of_scope"})
    return tuple(kept), tuple(dropped)


def _rank(rows: list[BoardRow], family: str) -> list[BoardRow]:
    """Order a family and assign ranks, honouring lowest-* questions."""
    ascending = family in ASCENDING_FAMILIES
    ordered = sorted(
        rows,
        key=lambda row: (row.expected_value if row.expected_value is not None else 0.0,
                         row.selection_key),
        reverse=not ascending,
    )
    depth = BOARD_DEPTH[family]
    return [replace(row, rank=index + 1) for index, row in enumerate(ordered[:depth])]


def build_game_rows(family: str, games: Sequence[Game]) -> tuple[list[BoardRow], list[dict[str, Any]]]:
    stat_key, is_proxy = ranking_stat(family)
    rows: list[BoardRow] = []
    blocked: list[dict[str, Any]] = []
    for game in games:
        key = f"{game.away}@{game.home}"
        if game.total is None:
            blocked.append({"family": family, "selection": key, "reason": "no_quoted_total"})
            rows.append(BoardRow(family, key, f"{game.away} @ {game.home}", stat_key, is_proxy,
                                 status="blocked", block_reason="no_quoted_total"))
            continue
        rows.append(BoardRow(
            family, key, f"{game.away} @ {game.home}", stat_key, is_proxy,
            expected_value=game.total,
            context={"home": game.home, "away": game.away, "spread": game.spread,
                     "spread_available": game.spread_available, "source": game.source},
        ))
    return rows, blocked


def build_team_rows(family: str, games: Sequence[Game]) -> tuple[list[BoardRow], list[dict[str, Any]]]:
    stat_key, is_proxy = ranking_stat(family)
    rows: list[BoardRow] = []
    blocked: list[dict[str, Any]] = []
    for game in games:
        if game.total is None:
            for team, opponent in ((game.home, game.away), (game.away, game.home)):
                blocked.append({"family": family, "selection": team, "reason": "no_quoted_total"})
                rows.append(BoardRow(family, team, team, stat_key, is_proxy,
                                     status="blocked", block_reason="no_quoted_total"))
            continue
        # implied_totals expects nflverse's sign convention: POSITIVE when the
        # home team is favoured. Both quoted_spread_line and market_spread_line
        # are stored that way (ingest/nfl_survivor_odds.py negates the Odds API's
        # book-style spread on write), so either can be passed straight through.
        # With no spread the split is even: an honest but uninformative estimate,
        # flagged in context rather than hidden.
        home_points, away_points = implied_totals(game.total, game.spread or 0.0)
        for team, opponent, points, home in ((game.home, game.away, home_points, True),
                                             (game.away, game.home, away_points, False)):
            rows.append(BoardRow(
                family, team, team, stat_key, is_proxy, expected_value=points,
                context={"opponent": opponent, "is_home": home, "game_total": game.total,
                         "spread": game.spread, "spread_available": game.spread_available,
                         "source": game.source},
            ))
    return rows, blocked


def player_stat(player: Player, stat_key: str) -> float | None:
    if stat_key == "expected_touchdowns":
        parts = [_number(player.stat_means.get(component)) for component in _TD_COMPONENTS]
        found = [part for part in parts if part is not None]
        return sum(found) if found else None
    return _number(player.stat_means.get(stat_key))


def build_player_rows(
    family: str, players: Sequence[Player], teams_in_scope: set[str],
) -> tuple[list[BoardRow], list[dict[str, Any]]]:
    stat_key, is_proxy = ranking_stat(family)
    allowed = FAMILY_POSITIONS.get(family)
    rows: list[BoardRow] = []
    excluded: list[dict[str, Any]] = []
    for player in players:
        if player.team not in teams_in_scope:
            continue
        if allowed and player.position not in allowed:
            continue
        # A player DK has ruled out must never head our board.
        if player.projection_status == "out":
            excluded.append({"family": family, "player": player.name, "reason": "out"})
            continue
        value = player_stat(player, stat_key)
        if value is None:
            excluded.append({"family": family, "player": player.name, "reason": f"no_{stat_key}"})
            continue
        if value <= 0:
            excluded.append({"family": family, "player": player.name, "reason": "zero_expectation"})
            continue
        rows.append(BoardRow(
            family, player.selection_key, player.name, stat_key, is_proxy,
            expected_value=value,
            context={"team": player.team, "position": player.position},
        ))
    return rows, excluded


def dedupe_or_block(rows: list[BoardRow], family: str) -> tuple[list[BoardRow], list[dict[str, Any]]]:
    """Convert a duplicated selection key into blocked rows, never a crash.

    `nfl_specials_board_rows` is UNIQUE on (run_id, family, selection_key), and a
    duplicate used to surface as an opaque UniqueViolation that killed the whole
    board build. It is reachable: `nfl_season_games` is UNIQUE on
    (season, week, home_team_id, away_team_id), which permits BOTH `CIN@NYG` and
    `NYG@CIN` in the same week, and a team in two in-scope games has no single
    expected-points number. A duplicated player key is likewise possible when two
    same-named players on one team both lack a gsis id.

    Either way the honest answer is the same: that selection cannot be ranked, so
    it is carried blocked with a reason while the rest of the family publishes.
    A data anomaly should cost one row's visibility, not the entire board.
    """
    counts: dict[str, int] = {}
    for row in rows:
        counts[row.selection_key] = counts.get(row.selection_key, 0) + 1
    duplicated = {key for key, count in counts.items() if count > 1}
    if not duplicated:
        return rows, []
    kept: list[BoardRow] = []
    blocked: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in rows:
        if row.selection_key not in duplicated:
            kept.append(row)
            continue
        if row.selection_key in seen:
            continue
        seen.add(row.selection_key)
        blocked.append({"family": family, "selection": row.selection_key,
                        "reason": "duplicate_selection_in_scope"})
        kept.append(replace(row, rank=None, expected_value=None, status="blocked",
                            block_reason="duplicate_selection_in_scope"))
    return kept, blocked


def build_board(inputs: Inputs, *, season: int, week: int, scope: str) -> Board:
    kept, dropped = games_in_scope(inputs.games, scope)
    teams = {team for game in kept for team in (game.home, game.away)}
    rows: list[BoardRow] = []
    excluded: list[dict[str, Any]] = list(dropped)
    blocked: list[dict[str, Any]] = []

    for family in FAMILIES:
        kind = selection_kind(family)
        if kind == "game":
            family_rows, family_blocked = build_game_rows(family, kept)
        elif kind == "team":
            family_rows, family_blocked = build_team_rows(family, kept)
        else:
            family_rows, family_excluded = build_player_rows(family, inputs.players, teams)
            family_blocked = []
            excluded.extend(family_excluded)
        blocked.extend(family_blocked)
        family_rows, duplicate_blocked = dedupe_or_block(family_rows, family)
        blocked.extend(duplicate_blocked)
        ok = [row for row in family_rows if row.status == "ok"]
        rows.extend(_rank(ok, family))
        rows.extend(row for row in family_rows if row.status != "ok")

    return Board(season=season, week=week, scope=scope, rows=tuple(rows),
                 games_in_scope=kept, excluded=tuple(excluded), blocked=tuple(blocked),
                 projection_run_id=inputs.projection_run_id)


# ── database ─────────────────────────────────────────────────────────────────

def load_inputs(db: DatabaseManager, season: int, week: int) -> Inputs:
    games = db.execute(
        """SELECT g.id, home.abbreviation AS home, away.abbreviation AS away, g.kickoff,
                  COALESCE(g.market_spread_line, g.quoted_spread_line) AS spread,
                  COALESCE(g.market_spread_line, g.quoted_spread_line) IS NOT NULL AS spread_available,
                  COALESCE(g.market_total_line, g.quoted_total_line) AS total,
                  g.quote_source
             FROM nfl_season_games g
             JOIN nfl_teams home ON home.team_id = g.home_team_id
             JOIN nfl_teams away ON away.team_id = g.away_team_id
            WHERE g.season = %s AND g.week = %s""",
        (season, week),
    )
    run = db.execute_one(
        """SELECT run_id FROM nfl_dfs_projection_runs
            WHERE season = %s AND week = %s AND player_count > 0
            ORDER BY as_of_at DESC, created_at DESC LIMIT 1""",
        (season, week),
    )
    players: list[Player] = []
    run_id = str(run["run_id"]) if run else None
    if run_id:
        for row in db.execute(
            """SELECT player_gsis_id, player_name, normalized_name, team, position,
                      stat_means, projection_status
                 FROM nfl_dfs_player_projections WHERE run_id = %s""",
            (run_id,),
        ):
            players.append(Player(
                gsis_id=row["player_gsis_id"], name=row["player_name"],
                normalized_name=row["normalized_name"], team=row["team"],
                position=row["position"], stat_means=dict(row["stat_means"] or {}),
                projection_status=str(row["projection_status"]),
            ))
    return Inputs(
        games=tuple(Game(
            game_id=int(row["id"]), home=str(row["home"]), away=str(row["away"]),
            kickoff=row["kickoff"], total=_number(row["total"]), spread=_number(row["spread"]),
            spread_available=bool(row["spread_available"]), source=row["quote_source"],
        ) for row in games),
        players=tuple(players),
        projection_run_id=run_id,
    )


def _git_sha() -> str | None:
    try:
        return subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True,
                              text=True, timeout=5, check=True).stdout.strip() or None
    except Exception:
        return None


def persist(db: DatabaseManager, board: Board) -> str:
    run_id = str(uuid.uuid4())
    db.execute(
        """INSERT INTO nfl_specials_runs
             (run_id, season, week, slate_scope, model_version, method,
              projection_run_id, games_json, blocked_reasons, git_sha)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
        (run_id, board.season, board.week, board.scope, BOARD_MODEL_VERSION, BOARD_METHOD,
         board.projection_run_id,
         Json([{"game": f"{g.away}@{g.home}", "kickoff": g.kickoff.isoformat() if g.kickoff else None,
                "total": g.total, "spread": g.spread, "included": True}
               for g in board.games_in_scope]),
         Json(list(board.blocked) + list(board.excluded)), _git_sha()),
    )
    db.execute_many(
        """INSERT INTO nfl_specials_board_rows
             (run_id, family, rank, selection_key, selection_label, stat_key,
              expected_value, is_proxy, context_json, status, block_reason)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
        [(run_id, row.family, row.rank, row.selection_key, row.selection_label, row.stat_key,
          row.expected_value, row.is_proxy, Json(row.context), row.status, row.block_reason)
         for row in board.rows],
    )
    return run_id


def run(*, season: int, week: int, scope: str, db: DatabaseManager,
        dry_run: bool = False) -> dict[str, Any]:
    if scope not in SLATE_SCOPES:
        raise ValueError(f"unknown scope {scope!r}; expected one of {sorted(SLATE_SCOPES)}")
    board = build_board(load_inputs(db, season, week), season=season, week=week, scope=scope)
    published = [row for row in board.rows if row.status == "ok"]
    report: dict[str, Any] = {
        "season": season, "week": week, "scope": scope,
        "games_in_scope": len(board.games_in_scope),
        "projection_run_id": board.projection_run_id,
        "rows": len(published),
        "blocked": len(board.blocked),
        "excluded": len(board.excluded),
        "per_family": {
            family: sum(1 for row in published if row.family == family) for family in FAMILIES
        },
        "leaders": {
            family: next(((row.selection_label, row.expected_value)
                          for row in published if row.family == family and row.rank == 1), None)
            for family in FAMILIES
        },
        "run_id": None,
    }
    if not dry_run:
        report["run_id"] = persist(db, board)
    return report


def _print(report: dict[str, Any]) -> None:
    print(f"specials board  {report['season']} week {report['week']}  {report['scope']}")
    print(f"  games in scope   {report['games_in_scope']}")
    print(f"  projection run   {report['projection_run_id'] or 'NONE (player families empty)'}")
    print(f"  rows published   {report['rows']}   blocked {report['blocked']}   excluded {report['excluded']}")
    for family in FAMILIES:
        count = report["per_family"][family]
        leader = report["leaders"][family]
        _, is_proxy = ranking_stat(family)
        tag = "  [proxy]" if is_proxy else ""
        if leader:
            print(f"  {family:22} {count:3} ranked   top: {leader[0]} ({leader[1]:.1f}){tag}")
        else:
            print(f"  {family:22} {count:3} ranked   -- nothing to rank{tag}")
    print(f"  run_id           {report['run_id'] or 'not written (dry run)'}")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--season", type=int, required=True)
    parser.add_argument("--week", type=int, help="omit to target the next scheduled week")
    parser.add_argument("--scope", default="both", choices=[*sorted(SLATE_SCOPES), "both"],
                        help="'both' publishes the all-Sunday and 1pm-only boards")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    db = DatabaseManager(load_config().database_url)

    scopes = sorted(SLATE_SCOPES) if args.scope == "both" else [args.scope]
    try:
        week = args.week if args.week is not None else infer_target_week(db, args.season)
    except ValueError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    if args.week is None:
        print(f"targeting week {week} (next scheduled)")

    failed = False
    for scope in scopes:
        try:
            report = run(season=args.season, week=week, scope=scope, db=db, dry_run=args.dry_run)
        except ValueError as exc:
            print(f"ERROR ({scope}): {exc}", file=sys.stderr)
            failed = True
            continue
        _print(report)
        print()
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
