"""Materialize auditable realized DraftKings points for historical NFL weeks.

The immutable nflverse component-stat payload in ``ff_player_week_stats`` is
the source of truth.  A source correction produces a new input digest and a
new result row; this command never updates or deletes prior results.

DST scoring is rebuilt from PLAY-BY-PLAY components when they are available
(`model/nfl_dst_components`), falling back to the retained team-week aggregate
when they are not; both records are kept in the evidence with any per-component
disagreement named.  Points allowed is adjusted to remove scores produced by
the opponent's defense.  The component ledger is retained for custom redraft
rules.

Usage:
    python -m ingest.nfl_dfs_results --season 2023 2024 2025
    python -m ingest.nfl_dfs_results --season 2025 --dry-run
"""

from __future__ import annotations

import argparse
import hashlib
import json
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from psycopg2.extras import Json, execute_values

from config import load_config
from db.database import DatabaseManager
from model.nfl_dfs_historical import draftkings_points
from model.nfl_team_aliases import normalize_team
from model.nfl_dst_components import (
    VERSION as DST_COMPONENT_VERSION,
    compare_components,
    derive_dst_components,
)


# v3: DST components are sourced from play-by-play when available rather than
# the team-week aggregate, which drops events. Semantics changed, so v2 rows
# keep their version and are never reinterpreted under this one.
SCORING_VERSION = "nfl-dk-realized-v3"
EXACT_POSITIONS = {"QB", "RB", "WR", "TE", "K", "DST"}
SCORING_FIELDS = {
    "QB": (
        "passing_yards", "passing_tds", "passing_interceptions",
        "rushing_yards", "rushing_tds", "receiving_yards", "receiving_tds",
        "receptions", "passing_2pt_conversions", "rushing_2pt_conversions",
        "receiving_2pt_conversions", "special_teams_tds",
        "fumble_recovery_tds", "fumbles_lost_total",
    ),
    "RB": (),
    "WR": (),
    "TE": (),
    "K": (
        "pat_made", "fg_made_0_19", "fg_made_20_29", "fg_made_30_39",
        "fg_made_40_49", "fg_made_50_59", "fg_made_60_",
    ),
}
for _skill_position in ("RB", "WR", "TE"):
    SCORING_FIELDS[_skill_position] = SCORING_FIELDS["QB"]


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def input_digest(*, position: str, source: str, source_row: Mapping[str, Any],
                 dst_context: Mapping[str, Any] | None = None) -> str:
    payload = {
        "position": position,
        "source": source,
        "source_row": source_row,
        "scoring_version": SCORING_VERSION,
    }
    if position == "DST":
        payload["dst_context"] = dst_context
    return hashlib.sha256(_canonical_json(payload).encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class ScoredResult:
    actual_dk_fpts: float | None
    status: str
    exclusion_reason: str | None
    evidence: dict[str, Any]


def _number(row: Mapping[str, Any], key: str) -> float:
    try:
        return float(row.get(key) or 0)
    except (TypeError, ValueError):
        return 0.0


def _dk_points_allowed(points: int) -> float:
    if points == 0:
        return 10.0
    if points <= 6:
        return 7.0
    if points <= 13:
        return 4.0
    if points <= 20:
        return 1.0
    if points <= 27:
        return 0.0
    if points <= 34:
        return -1.0
    return -4.0


def _score_dst(source_row: Mapping[str, Any], context: Mapping[str, Any] | None) -> ScoredResult:
    if not context or context.get("opponent_final_points") is None or not context.get("opponent_raw_team_stats"):
        return ScoredResult(
            actual_dk_fpts=None,
            status="excluded",
            exclusion_reason="DST requires opponent final score and opponent team-week components",
            evidence={"position": "DST", "eligible_for_dk_backtest": False},
        )
    raw = source_row.get("raw_team_stats") or {}
    opponent_raw = context["opponent_raw_team_stats"]
    opponent_defensive_points = (
        6 * _number(opponent_raw, "def_tds")
        + 2 * _number(opponent_raw, "def_safeties")
        + 2 * _number(opponent_raw, "def_2pt_made")
    )
    points_allowed = max(0, round(float(context["opponent_final_points"]) - opponent_defensive_points))

    # The team-week aggregate drops events (a returned strip-sack reaching it
    # as `def_tds: 0`, a third sack recorded as two). Play-by-play carries
    # them and agreed with DraftKings on 26/26 team-defenses where the
    # aggregate managed 24/26 -- see `model/nfl_dst_components`. Prefer it
    # when the caller supplies it; fall back to the aggregate when it is
    # absent, so a week without play data still scores rather than vanishing.
    aggregate = {
        "sacks": _number(raw, "def_sacks"),
        "interceptions": _number(raw, "def_interceptions"),
        "fumble_recoveries": _number(raw, "fumble_recovery_opp"),
        "safeties": _number(raw, "def_safeties"),
        "defensive_tds": _number(raw, "def_tds"),
        "blocked_kicks": sum(_number(raw, key) for key in ("def_fg_blocks", "def_pat_blocks", "def_punt_blocks")),
    }
    derived = context.get("pbp_components")
    component_source = "play_by_play" if derived else "team_aggregate"
    chosen = dict(derived) if derived else dict(aggregate)

    components = {
        **{name: float(chosen.get(name) or 0.0) for name in aggregate},
        # Not derivable from `defteam` on a kick, and not a defensive event:
        # these two always come from the aggregate and the caller.
        "special_teams_return_tds": _number(raw, "special_teams_tds"),
        "two_point_returns": _number(raw, "def_2pt_made"),
        "opponent_final_points": int(context["opponent_final_points"]),
        "opponent_defensive_points_excluded": opponent_defensive_points,
        "dk_points_allowed": points_allowed,
        "points_allowed_fpts": _dk_points_allowed(points_allowed),
    }
    points = (
        components["sacks"]
        + 2 * components["interceptions"]
        + 2 * components["fumble_recoveries"]
        + 2 * components["safeties"]
        + 6 * (components["defensive_tds"] + components["special_teams_return_tds"])
        + 2 * components["blocked_kicks"]
        + 2 * components["two_point_returns"]
        + components["points_allowed_fpts"]
    )
    return ScoredResult(
        actual_dk_fpts=round(float(points), 4),
        status="exact",
        exclusion_reason=None,
        evidence={
            "position": "DST",
            "eligible_for_dk_backtest": True,
            "scorer": "ingest.nfl_dfs_results._score_dst",
            "scoring_components": components,
            "redraft_reusable_components": True,
            "component_source": component_source,
            "component_version": DST_COMPONENT_VERSION,
            # Both independent records are kept whenever both exist, with the
            # per-component disagreements called out. A disagreement is a
            # data-quality signal to audit, not evidence either source is
            # right: only one week of DraftKings ground truth exists so far.
            "team_aggregate_components": aggregate if derived else None,
            "component_disagreements": compare_components(derived, aggregate) or None,
        },
    )


def score_source_row(
    position: str,
    source_row: Mapping[str, Any],
    *,
    dst_context: Mapping[str, Any] | None = None,
) -> ScoredResult:
    normalized = position.upper()
    if normalized == "DST":
        return _score_dst(source_row, dst_context)
    if normalized not in EXACT_POSITIONS:
        return ScoredResult(
            actual_dk_fpts=None,
            status="excluded",
            exclusion_reason=f"unsupported DraftKings scoring position: {normalized}",
            evidence={"position": normalized, "eligible_for_dk_backtest": False},
        )

    fields = SCORING_FIELDS[normalized]
    scoring_input = {field: source_row.get(field) for field in fields}
    return ScoredResult(
        actual_dk_fpts=round(draftkings_points(normalized, source_row), 4),
        status="exact",
        exclusion_reason=None,
        evidence={
            "position": normalized,
            "eligible_for_dk_backtest": True,
            "scorer": "model.nfl_dfs_historical.draftkings_points",
            "scoring_input": scoring_input,
        },
    )


def source_rows(db: DatabaseManager, seasons: Sequence[int]) -> list[dict[str, Any]]:
    return db.execute(
        """
        SELECT w.id player_week_stat_id,w.player_id,w.season,w.week,w.source,
               w.team,w.opponent,w.source_row,w.fetched_at,p.position,
               (
                 SELECT g.id
                 FROM nfl_season_games g
                 JOIN nfl_teams home ON home.team_id=g.home_team_id
                 JOIN nfl_teams away ON away.team_id=g.away_team_id
                 WHERE g.season=w.season AND g.week=w.week AND g.game_type='REG'
                   AND w.team IN (home.abbreviation,away.abbreviation)
                 ORDER BY g.id
                 LIMIT 1
               ) game_id,
               (
                 SELECT CASE WHEN home.abbreviation=w.team THEN g.away_score ELSE g.home_score END
                 FROM nfl_season_games g
                 JOIN nfl_teams home ON home.team_id=g.home_team_id
                 JOIN nfl_teams away ON away.team_id=g.away_team_id
                 WHERE g.season=w.season AND g.week=w.week AND g.game_type='REG'
                   AND w.team IN (home.abbreviation,away.abbreviation)
                 ORDER BY g.id LIMIT 1
               ) opponent_final_points,
               (
                 SELECT ow.source_row
                 FROM ff_player_week_stats ow
                 JOIN ff_players op ON op.id=ow.player_id
                 WHERE ow.season=w.season AND ow.week=w.week AND ow.season_type='REG'
                   AND ow.team=w.opponent AND op.position='DST'
                 ORDER BY ow.id LIMIT 1
               ) opponent_raw_source
        FROM ff_player_week_stats w
        JOIN ff_players p ON p.id=w.player_id
        WHERE w.season_type='REG' AND w.season=ANY(%s)
        ORDER BY w.season,w.week,w.id
        """,
        (list(seasons),),
    )


def pbp_components(db: DatabaseManager, seasons: Sequence[int]) -> dict[tuple[int, int, str], dict[str, float]]:
    """DST components from play-by-play, keyed (season, week, canonical team).

    One query for the whole run rather than one per team-game. A season with
    no play-by-play simply contributes no keys, and every DST in it falls back
    to the team-week aggregate -- absence degrades, it does not fail.
    """
    plays = db.execute(
        """
        SELECT season, week, defteam, turnover_type, had_sack, st_outcome, description
        FROM nfl_pbp_archetypes
        WHERE season = ANY(%s) AND season_type = 'REG' AND defteam IS NOT NULL
        """,
        (list(seasons),),
    )
    by_week: dict[tuple[int, int], list[dict[str, Any]]] = {}
    for play in plays:
        by_week.setdefault((int(play["season"]), int(play["week"])), []).append(play)
    out: dict[tuple[int, int, str], dict[str, float]] = {}
    for (season, week), week_plays in by_week.items():
        for team, components in derive_dst_components(week_plays).items():
            out[(season, week, team)] = components
    return out


def materialize(db: DatabaseManager, seasons: Sequence[int], *, dry_run: bool = False) -> dict[str, int]:
    rows = source_rows(db, seasons)
    derived_components = pbp_components(db, seasons)
    counts = {"source_rows": len(rows), "exact": 0, "excluded": 0, "game_linked": 0, "inserted": 0,
              "dst_from_play_by_play": 0, "dst_from_team_aggregate": 0, "dst_component_disagreements": 0}
    inserts: list[tuple[object, ...]] = []
    for row in rows:
        # Historical source position wins over the player's current canonical
        # position so later position changes cannot rewrite old scoring rules.
        position = str(row["source_row"].get("position") or row["position"] or "").upper()
        dst_context = {
            "opponent_final_points": row["opponent_final_points"],
            "opponent_raw_team_stats": (row["opponent_raw_source"] or {}).get("raw_team_stats"),
        }
        if position == "DST":
            derived = derived_components.get(
                (int(row["season"]), int(row["week"]), normalize_team(row["team"]) or "")
            )
            if derived:
                dst_context["pbp_components"] = derived
        scored = score_source_row(
            position,
            row["source_row"],
            dst_context=dst_context,
        )
        if position == "DST" and scored.status == "exact":
            counts["dst_from_play_by_play" if scored.evidence.get("component_source") == "play_by_play"
                   else "dst_from_team_aggregate"] += 1
            counts["dst_component_disagreements"] += int(bool(scored.evidence.get("component_disagreements")))
        counts[scored.status] += 1
        counts["game_linked"] += int(row["game_id"] is not None)
        digest = input_digest(position=position, source=row["source"], source_row=row["source_row"],
                              dst_context=dst_context)
        evidence = {
            **scored.evidence,
            "source_fetched_at": row["fetched_at"].isoformat(),
            "source_row_digest": digest,
            "schedule_link_method": "season+week+team",
            "dst_context": dst_context if position == "DST" else None,
        }
        inserts.append(
            (
                row["player_week_stat_id"], row["player_id"], row["game_id"],
                row["season"], row["week"], row["team"], row["opponent"], position,
                scored.actual_dk_fpts, scored.status, scored.exclusion_reason,
                SCORING_VERSION, row["source"], digest, Json(evidence),
            )
        )

    if not dry_run and inserts:
        before = db.execute_one("SELECT COUNT(*)::int n FROM nfl_dfs_player_week_results")["n"]
        # execute_values keeps this to a few dozen network round trips even
        # for a three-season backfill; DatabaseManager.execute_many executes
        # each row separately and is prohibitively slow against remote Neon.
        with db.connect() as connection:
            with connection.cursor() as cursor:
                execute_values(
                    cursor,
                    """
                    INSERT INTO nfl_dfs_player_week_results (
                        player_week_stat_id,player_id,game_id,season,week,team,opponent,position,
                        actual_dk_fpts,scoring_status,exclusion_reason,scoring_version,
                        input_source,input_digest,scoring_evidence
                    ) VALUES %s
                    ON CONFLICT (player_week_stat_id,scoring_version,input_digest) DO NOTHING
                    """,
                    inserts,
                    page_size=1000,
                )
        after = db.execute_one("SELECT COUNT(*)::int n FROM nfl_dfs_player_week_results")["n"]
        counts["inserted"] = after - before
    return counts


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--season", nargs="+", type=int, required=True)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    db = DatabaseManager(load_config().database_url)
    print(json.dumps(materialize(db, args.season, dry_run=args.dry_run), sort_keys=True))


if __name__ == "__main__":
    main()
