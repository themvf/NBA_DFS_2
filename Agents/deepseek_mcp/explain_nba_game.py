from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import load_config
from db.database import DatabaseManager
from nba_api.stats.endpoints import BoxScoreAdvancedV3, BoxScoreTraditionalV3, PlayByPlayV3

from server import explain_game_outcome_message

def american_to_raw_prob(ml: int | None) -> float | None:
    if ml is None or ml == 0:
        return None
    if ml > 0:
        return 100 / (ml + 100)
    return abs(ml) / (abs(ml) + 100)


def remove_vig_home_prob(home_ml: int | None, away_ml: int | None) -> float | None:
    home_raw = american_to_raw_prob(home_ml)
    away_raw = american_to_raw_prob(away_ml)
    if home_raw is None or away_raw is None:
        return None
    total = home_raw + away_raw
    if total <= 0:
        return None
    return home_raw / total


def compute_dk_points(row: dict[str, Any]) -> float:
    pts = float(row.get("points") or 0)
    reb = float(row.get("reboundsTotal") or 0)
    ast = float(row.get("assists") or 0)
    stl = float(row.get("steals") or 0)
    blk = float(row.get("blocks") or 0)
    tov = float(row.get("turnovers") or 0)
    return pts + reb * 1.25 + ast * 1.5 + stl * 2 + blk * 2 - tov * 0.5


def parse_minutes(value: Any) -> float:
    if value in (None, ""):
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value)
    if ":" not in text:
        try:
            return float(text)
        except ValueError:
            return 0.0
    parts = text.split(":")
    if len(parts) != 2:
        return 0.0
    try:
        minutes = int(parts[0])
        seconds = int(parts[1])
    except ValueError:
        return 0.0
    return minutes + seconds / 60


def summarize_team_minutes(players: list[dict[str, Any]], team_abbrev: str) -> dict[str, Any]:
    team_rows = [row for row in players if str(row.get("teamTricode")) == team_abbrev and not row.get("comment")]
    minute_values = sorted((parse_minutes(row.get("minutes")) for row in team_rows), reverse=True)
    top_five = minute_values[:5]
    return {
        "players35Plus": sum(1 for value in minute_values if value >= 35),
        "players40Plus": sum(1 for value in minute_values if value >= 40),
        "topFiveAvgMinutes": round(sum(top_five) / len(top_five), 2) if top_five else None,
        "maxMinutes": round(max(minute_values), 2) if minute_values else None,
    }


def round_diff(a: Any, b: Any, digits: int = 3) -> float | None:
    if a is None or b is None:
        return None
    return round(float(a) - float(b), digits)


def average(values: list[float]) -> float | None:
    if not values:
        return None
    return round(sum(values) / len(values), 3)


@dataclass
class TeamLine:
    team: str
    points: int | None
    fg_pct: float | None
    three_pct: float | None
    ft_pct: float | None
    rebounds: int | None
    offensive_rebounds: int | None
    assists: int | None
    turnovers: int | None
    plus_minus: float | None
    offensive_rating: float | None
    defensive_rating: float | None
    net_rating: float | None
    pace: float | None
    possessions: float | None
    efg_pct: float | None
    ts_pct: float | None
    pie: float | None
    bench_points: int | None


def load_matchup(
    db: DatabaseManager,
    game_date: str,
    home_abbrev: str,
    away_abbrev: str,
) -> dict[str, Any]:
    row = db.execute_one(
        """
        SELECT
            nm.id,
            nm.game_id,
            nm.game_date::TEXT AS game_date,
            home.abbreviation AS home_abbrev,
            away.abbreviation AS away_abbrev,
            nm.home_team_id,
            nm.away_team_id,
            nm.home_score,
            nm.away_score,
            nm.vegas_total,
            nm.home_spread,
            nm.home_ml,
            nm.away_ml,
            nm.home_implied,
            nm.away_implied,
            nm.vegas_prob_home,
            nm.our_game_total_pred,
            th.pace AS home_pace,
            th.off_rtg AS home_off_rtg,
            th.def_rtg AS home_def_rtg,
            ta.pace AS away_pace,
            ta.off_rtg AS away_off_rtg,
            ta.def_rtg AS away_def_rtg
        FROM nba_matchups nm
        JOIN teams home ON home.team_id = nm.home_team_id
        JOIN teams away ON away.team_id = nm.away_team_id
        LEFT JOIN nba_team_stats th
          ON th.team_id = nm.home_team_id
         AND th.season = %s
        LEFT JOIN nba_team_stats ta
          ON ta.team_id = nm.away_team_id
         AND ta.season = %s
        WHERE nm.game_date = %s
          AND home.abbreviation = %s
          AND away.abbreviation = %s
        LIMIT 1
        """,
        ("2025-26", "2025-26", game_date, home_abbrev.upper(), away_abbrev.upper()),
    )
    if not row:
        raise RuntimeError(f"No nba_matchups row found for {away_abbrev} @ {home_abbrev} on {game_date}")
    return dict(row)


def _dataset_to_rows(dataset: Any) -> list[dict[str, Any]]:
    data = dataset.get_dict()
    headers = data.get("headers", [])
    rows = data.get("data", [])
    return [dict(zip(headers, row)) for row in rows]


def parse_clock_seconds(clock_value: Any) -> float | None:
    if not clock_value:
        return None
    text = str(clock_value)
    if not text.startswith("PT") or "M" not in text or "S" not in text:
        return None
    try:
        minutes_part = text.split("PT", 1)[1].split("M", 1)[0]
        seconds_part = text.split("M", 1)[1].split("S", 1)[0]
        return int(minutes_part) * 60 + float(seconds_part)
    except Exception:
        return None


@lru_cache(maxsize=128)
def fetch_box_score(game_id: str) -> dict[str, Any]:
    from ingest.nba_stats import _call_with_retry

    traditional = _call_with_retry(
        lambda: BoxScoreTraditionalV3(game_id=game_id, timeout=90),
        f"BoxScoreTraditionalV3-{game_id}",
    )
    advanced = _call_with_retry(
        lambda: BoxScoreAdvancedV3(game_id=game_id, timeout=90),
        f"BoxScoreAdvancedV3-{game_id}",
    )

    traditional_players = _dataset_to_rows(traditional.player_stats)
    traditional_teams = _dataset_to_rows(traditional.team_stats)
    bench_stats = _dataset_to_rows(traditional.data_sets[1]) if len(traditional.data_sets) > 1 else []
    advanced_players = _dataset_to_rows(advanced.player_stats)
    advanced_teams = _dataset_to_rows(advanced.team_stats)

    advanced_player_map = {
        (str(row.get("teamTricode")), str(row.get("personId"))): row
        for row in advanced_players
    }
    advanced_team_map = {
        str(row.get("teamTricode")): row
        for row in advanced_teams
    }
    bench_map = {
        (str(row.get("teamTricode")), str(row.get("startersBench"))): row
        for row in bench_stats
    }

    players: list[dict[str, Any]] = []
    for row in traditional_players:
        team_abbrev = str(row.get("teamTricode"))
        person_id = str(row.get("personId"))
        adv = advanced_player_map.get((team_abbrev, person_id), {})
        combined = dict(row)
        combined.update(
            {
                "usagePercentage": adv.get("usagePercentage"),
                "trueShootingPercentage": adv.get("trueShootingPercentage"),
                "effectiveFieldGoalPercentage": adv.get("effectiveFieldGoalPercentage"),
                "pace": adv.get("pace"),
                "possessions": adv.get("possessions"),
                "PIE": adv.get("PIE"),
                "dkPoints": round(compute_dk_points(row), 2),
            }
        )
        players.append(combined)

    teams: dict[str, TeamLine] = {}
    for row in traditional_teams:
        team_abbrev = str(row.get("teamTricode"))
        adv = advanced_team_map.get(team_abbrev, {})
        bench = bench_map.get((team_abbrev, "Bench"), {})
        teams[team_abbrev] = TeamLine(
            team=team_abbrev,
            points=row.get("points"),
            fg_pct=row.get("fieldGoalsPercentage"),
            three_pct=row.get("threePointersPercentage"),
            ft_pct=row.get("freeThrowsPercentage"),
            rebounds=row.get("reboundsTotal"),
            offensive_rebounds=row.get("reboundsOffensive"),
            assists=row.get("assists"),
            turnovers=row.get("turnovers"),
            plus_minus=row.get("plusMinusPoints"),
            offensive_rating=adv.get("offensiveRating"),
            defensive_rating=adv.get("defensiveRating"),
            net_rating=adv.get("netRating"),
            pace=adv.get("pace"),
            possessions=adv.get("possessions"),
            efg_pct=adv.get("effectiveFieldGoalPercentage"),
            ts_pct=adv.get("trueShootingPercentage"),
            pie=adv.get("PIE"),
            bench_points=bench.get("points"),
        )

    return {
        "players": players,
        "teams": {abbr: asdict(team_line) for abbr, team_line in teams.items()},
    }


def load_prior_team_games(
    db: DatabaseManager,
    team_abbrev: str,
    before_date: str,
    limit: int = 5,
) -> list[dict[str, Any]]:
    rows = db.execute(
        """
        SELECT
            nm.game_date::TEXT AS game_date,
            nm.game_id,
            home.abbreviation AS home_abbrev,
            away.abbreviation AS away_abbrev,
            nm.home_score,
            nm.away_score
        FROM nba_matchups nm
        JOIN teams home ON home.team_id = nm.home_team_id
        JOIN teams away ON away.team_id = nm.away_team_id
        WHERE nm.game_date < %s
          AND nm.home_score IS NOT NULL
          AND nm.away_score IS NOT NULL
          AND nm.game_id IS NOT NULL
          AND (home.abbreviation = %s OR away.abbreviation = %s)
        ORDER BY nm.game_date DESC, nm.id DESC
        LIMIT %s
        """,
        (before_date, team_abbrev, team_abbrev, limit),
    )
    return [dict(row) for row in rows]


def load_prior_head_to_head_games(
    db: DatabaseManager,
    team_a: str,
    team_b: str,
    before_date: str,
    limit: int = 7,
) -> list[dict[str, Any]]:
    rows = db.execute(
        """
        SELECT
            nm.game_date::TEXT AS game_date,
            nm.game_id,
            home.abbreviation AS home_abbrev,
            away.abbreviation AS away_abbrev,
            nm.home_score,
            nm.away_score
        FROM nba_matchups nm
        JOIN teams home ON home.team_id = nm.home_team_id
        JOIN teams away ON away.team_id = nm.away_team_id
        WHERE nm.game_date < %s
          AND nm.home_score IS NOT NULL
          AND nm.away_score IS NOT NULL
          AND nm.game_id IS NOT NULL
          AND (
            (home.abbreviation = %s AND away.abbreviation = %s)
            OR (home.abbreviation = %s AND away.abbreviation = %s)
          )
        ORDER BY nm.game_date DESC, nm.id DESC
        LIMIT %s
        """,
        (before_date, team_a, team_b, team_b, team_a, limit),
    )
    return [dict(row) for row in rows]


def extract_team_snapshot(game_row: dict[str, Any], team_abbrev: str) -> dict[str, Any]:
    box = fetch_box_score(str(game_row["game_id"]))
    team_line = box["teams"][team_abbrev]
    opponent_abbrev = game_row["away_abbrev"] if game_row["home_abbrev"] == team_abbrev else game_row["home_abbrev"]
    opponent_line = box["teams"][opponent_abbrev]
    team_score = game_row["home_score"] if game_row["home_abbrev"] == team_abbrev else game_row["away_score"]
    opp_score = game_row["away_score"] if game_row["home_abbrev"] == team_abbrev else game_row["home_score"]
    return {
        "gameDate": game_row["game_date"],
        "opponent": opponent_abbrev,
        "won": team_score > opp_score,
        "margin": team_score - opp_score,
        "points": team_score,
        "oppPoints": opp_score,
        "fgPct": team_line.get("fg_pct"),
        "threePct": team_line.get("three_pct"),
        "efgPct": team_line.get("efg_pct"),
        "tsPct": team_line.get("ts_pct"),
        "offRtg": team_line.get("offensive_rating"),
        "pace": team_line.get("pace"),
        "starterPoints": (team_line.get("points") or 0) - (team_line.get("bench_points") or 0)
        if team_line.get("points") is not None and team_line.get("bench_points") is not None else None,
        "benchPoints": team_line.get("bench_points"),
        "oppFgPct": opponent_line.get("fg_pct"),
        "oppThreePct": opponent_line.get("three_pct"),
    }


def safe_extract_team_snapshot(game_row: dict[str, Any], team_abbrev: str) -> dict[str, Any] | None:
    try:
        return extract_team_snapshot(game_row, team_abbrev)
    except Exception:
        return None


def summarize_snapshots(snapshots: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "games": len(snapshots),
        "wins": sum(1 for snap in snapshots if snap["won"]),
        "avgMargin": average([float(snap["margin"]) for snap in snapshots]),
        "avgPoints": average([float(snap["points"]) for snap in snapshots]),
        "avgOppPoints": average([float(snap["oppPoints"]) for snap in snapshots]),
        "avgFgPct": average([float(snap["fgPct"]) for snap in snapshots if snap["fgPct"] is not None]),
        "avgThreePct": average([float(snap["threePct"]) for snap in snapshots if snap["threePct"] is not None]),
        "avgEfgPct": average([float(snap["efgPct"]) for snap in snapshots if snap["efgPct"] is not None]),
        "avgTsPct": average([float(snap["tsPct"]) for snap in snapshots if snap["tsPct"] is not None]),
        "avgOffRtg": average([float(snap["offRtg"]) for snap in snapshots if snap["offRtg"] is not None]),
        "avgPace": average([float(snap["pace"]) for snap in snapshots if snap["pace"] is not None]),
        "avgStarterPoints": average([float(snap["starterPoints"]) for snap in snapshots if snap["starterPoints"] is not None]),
        "avgBenchPoints": average([float(snap["benchPoints"]) for snap in snapshots if snap["benchPoints"] is not None]),
    }


def build_recent_form_context(
    db: DatabaseManager,
    current_game_date: str,
    team_abbrev: str,
    current_snapshot: dict[str, Any],
) -> dict[str, Any]:
    recent_games = load_prior_team_games(db, team_abbrev, current_game_date, limit=3)
    recent_snapshots = [
        snapshot for snapshot in
        (safe_extract_team_snapshot(row, team_abbrev) for row in recent_games)
        if snapshot is not None
    ]
    summary = summarize_snapshots(recent_snapshots)
    current_vs_recent = {
        "pointsDiff": round_diff(current_snapshot.get("points"), summary.get("avgPoints"), 1),
        "fgPctDiff": round_diff(current_snapshot.get("fgPct"), summary.get("avgFgPct")),
        "threePctDiff": round_diff(current_snapshot.get("threePct"), summary.get("avgThreePct")),
        "tsPctDiff": round_diff(current_snapshot.get("tsPct"), summary.get("avgTsPct")),
        "offRtgDiff": round_diff(current_snapshot.get("offRtg"), summary.get("avgOffRtg"), 1),
        "starterPointsDiff": round_diff(current_snapshot.get("starterPoints"), summary.get("avgStarterPoints"), 1),
        "benchPointsDiff": round_diff(current_snapshot.get("benchPoints"), summary.get("avgBenchPoints"), 1),
    }
    return {
        "recentGames": recent_snapshots,
        "summary": summary,
        "currentVsRecent": current_vs_recent,
    }


def build_series_context(
    db: DatabaseManager,
    current_game_date: str,
    away_team: str,
    home_team: str,
    current_away_snapshot: dict[str, Any],
    current_home_snapshot: dict[str, Any],
) -> dict[str, Any]:
    prior_games = load_prior_head_to_head_games(db, away_team, home_team, current_game_date, limit=3)
    away_snaps = [
        snapshot for snapshot in
        (safe_extract_team_snapshot(row, away_team) for row in prior_games)
        if snapshot is not None
    ]
    home_snaps = [
        snapshot for snapshot in
        (safe_extract_team_snapshot(row, home_team) for row in prior_games)
        if snapshot is not None
    ]
    away_summary = summarize_snapshots(away_snaps)
    home_summary = summarize_snapshots(home_snaps)
    return {
        "priorGames": len(prior_games),
        "recentHeadToHead": [
            {
                "gameDate": row["game_date"],
                "matchup": f"{row['away_abbrev']} @ {row['home_abbrev']}",
                "finalScore": f"{row['away_abbrev']} {row['away_score']} - {row['home_abbrev']} {row['home_score']}",
            }
            for row in prior_games
        ],
        "teamSummary": {
            away_team: away_summary,
            home_team: home_summary,
        },
        "currentVsSeries": {
            away_team: {
                "pointsDiff": round_diff(current_away_snapshot.get("points"), away_summary.get("avgPoints"), 1),
                "threePctDiff": round_diff(current_away_snapshot.get("threePct"), away_summary.get("avgThreePct")),
                "tsPctDiff": round_diff(current_away_snapshot.get("tsPct"), away_summary.get("avgTsPct")),
            },
            home_team: {
                "pointsDiff": round_diff(current_home_snapshot.get("points"), home_summary.get("avgPoints"), 1),
                "threePctDiff": round_diff(current_home_snapshot.get("threePct"), home_summary.get("avgThreePct")),
                "tsPctDiff": round_diff(current_home_snapshot.get("tsPct"), home_summary.get("avgTsPct")),
            },
        },
    }


def fetch_play_by_play_summary(game_id: str, home_team: str, away_team: str) -> dict[str, Any]:
    from ingest.nba_stats import _call_with_retry

    pbp = _call_with_retry(
        lambda: PlayByPlayV3(game_id=game_id, timeout=90),
        f"PlayByPlayV3-{game_id}",
    )
    rows = _dataset_to_rows(pbp.play_by_play)
    scoring_rows = [
        row for row in rows
        if row.get("scoreHome") not in ("", None) and row.get("scoreAway") not in ("", None)
    ]
    lead_changes = 0
    ties = 0
    previous_leader = None
    largest_lead_home = 0
    largest_lead_away = 0
    biggest_run = {
        home_team: {"points": 0, "startPeriod": None, "startClock": None, "endPeriod": None, "endClock": None},
        away_team: {"points": 0, "startPeriod": None, "startClock": None, "endPeriod": None, "endClock": None},
    }
    current_run_team: str | None = None
    current_run_points = 0
    current_run_start_period: int | None = None
    current_run_start_clock: str | None = None
    quarter_scores: dict[str, dict[str, int]] = {}
    last_quarter_totals: dict[int, tuple[int, int]] = {}
    clutch_snapshot: dict[str, Any] | None = None
    previous_home_score = 0
    previous_away_score = 0

    for row in scoring_rows:
        home_score = int(row["scoreHome"])
        away_score = int(row["scoreAway"])
        period = int(row.get("period") or 0)
        clock = str(row.get("clock") or "")
        home_delta = home_score - previous_home_score
        away_delta = away_score - previous_away_score
        if home_delta > 0 and away_delta == 0:
            team = home_team
            points = home_delta
        elif away_delta > 0 and home_delta == 0:
            team = away_team
            points = away_delta
        else:
            team = str(row.get("teamTricode") or "")
            points = max(home_delta, away_delta, 0)
        margin = home_score - away_score

        if margin == 0:
            ties += 1
            leader = None
        elif margin > 0:
            leader = home_team
            largest_lead_home = max(largest_lead_home, margin)
        else:
            leader = away_team
            largest_lead_away = max(largest_lead_away, abs(margin))

        if previous_leader is not None and leader is not None and leader != previous_leader:
            lead_changes += 1
        if previous_leader is None and leader is not None and ties > 0:
            # do nothing extra; tie count already tracked
            pass
        if leader is not None:
            previous_leader = leader

        last_quarter_totals[period] = (home_score, away_score)

        if team in (home_team, away_team) and points > 0:
            if current_run_team == team:
                current_run_points += points
            else:
                if current_run_team in biggest_run and current_run_points > biggest_run[current_run_team]["points"]:
                    biggest_run[current_run_team] = {
                        "points": current_run_points,
                        "startPeriod": current_run_start_period,
                        "startClock": current_run_start_clock,
                        "endPeriod": previous_period,
                        "endClock": previous_clock,
                    }
                current_run_team = team
                current_run_points = points
                current_run_start_period = period
                current_run_start_clock = clock

        previous_period = period
        previous_clock = clock

        clock_seconds = parse_clock_seconds(clock)
        if period == 4 and clock_seconds is not None and clock_seconds <= 300 and clutch_snapshot is None:
            clutch_snapshot = {
                "period": period,
                "clock": clock,
                "score": f"{away_team} {away_score} - {home_team} {home_score}",
                "margin": abs(margin),
                "leader": leader,
            }
        previous_home_score = home_score
        previous_away_score = away_score

    if current_run_team in biggest_run and current_run_points > biggest_run[current_run_team]["points"]:
        biggest_run[current_run_team] = {
            "points": current_run_points,
            "startPeriod": current_run_start_period,
            "startClock": current_run_start_clock,
            "endPeriod": previous_period if scoring_rows else None,
            "endClock": previous_clock if scoring_rows else None,
        }

    decisive_run = None
    run_candidates = []
    for team_abbrev, run in biggest_run.items():
        if run["points"] <= 0:
            continue
        run_candidates.append({"team": team_abbrev, **run})
    if run_candidates:
        decisive_run = max(run_candidates, key=lambda item: item["points"])

    prior_home_total = 0
    prior_away_total = 0
    for period in sorted(last_quarter_totals):
        home_total, away_total = last_quarter_totals[period]
        quarter_scores[f"Q{period}"] = {
            home_team: home_total - prior_home_total,
            away_team: away_total - prior_away_total,
        }
        prior_home_total = home_total
        prior_away_total = away_total

    return {
        "leadChanges": lead_changes,
        "timesTied": ties,
        "largestLead": {
            home_team: largest_lead_home,
            away_team: largest_lead_away,
        },
        "biggestRun": biggest_run,
        "decisiveRun": decisive_run,
        "quarterScores": quarter_scores,
        "clutchSnapshot": clutch_snapshot,
        "enteredClutchWindow": clutch_snapshot is not None and (clutch_snapshot.get("margin") or 99) <= 5,
    }


def build_game_context(
    db: DatabaseManager,
    game_date: str,
    home_abbrev: str,
    away_abbrev: str,
) -> dict[str, Any]:
    matchup = load_matchup(db, game_date=game_date, home_abbrev=home_abbrev, away_abbrev=away_abbrev)
    game_id = matchup.get("game_id")
    if not game_id:
        raise RuntimeError("nba_matchups row does not have a game_id; run score/schedule backfill first")

    box = fetch_box_score(str(game_id))
    home_score = matchup.get("home_score")
    away_score = matchup.get("away_score")
    actual_total = (
        (home_score or 0) + (away_score or 0)
        if home_score is not None and away_score is not None
        else None
    )
    vegas_home_prob = matchup.get("vegas_prob_home")
    if vegas_home_prob is None:
        vegas_home_prob = remove_vig_home_prob(matchup.get("home_ml"), matchup.get("away_ml"))

    home_team = matchup["home_abbrev"]
    away_team = matchup["away_abbrev"]
    home_box = box["teams"].get(home_team, {})
    away_box = box["teams"].get(away_team, {})
    pbp_summary = fetch_play_by_play_summary(str(game_id), home_team=home_team, away_team=away_team)
    players = box["players"]
    home_minute_summary = summarize_team_minutes(players, home_team)
    away_minute_summary = summarize_team_minutes(players, away_team)
    current_home_snapshot = {
        "points": home_score,
        "fgPct": home_box.get("fg_pct"),
        "threePct": home_box.get("three_pct"),
        "tsPct": home_box.get("ts_pct"),
        "offRtg": home_box.get("offensive_rating"),
        "starterPoints": (home_box.get("points") or 0) - (home_box.get("bench_points") or 0)
        if home_box.get("points") is not None and home_box.get("bench_points") is not None else None,
        "benchPoints": home_box.get("bench_points"),
    }
    current_away_snapshot = {
        "points": away_score,
        "fgPct": away_box.get("fg_pct"),
        "threePct": away_box.get("three_pct"),
        "tsPct": away_box.get("ts_pct"),
        "offRtg": away_box.get("offensive_rating"),
        "starterPoints": (away_box.get("points") or 0) - (away_box.get("bench_points") or 0)
        if away_box.get("points") is not None and away_box.get("bench_points") is not None else None,
        "benchPoints": away_box.get("bench_points"),
    }
    top_players = sorted(
        [row for row in players if not row.get("comment")],
        key=lambda row: (
            float(row.get("points") or 0),
            float(row.get("usagePercentage") or 0),
            parse_minutes(row.get("minutes")),
        ),
        reverse=True,
    )[:12]

    top_players_by_team = {
        away_team: [],
        home_team: [],
    }
    for row in top_players:
        team_abbrev = str(row.get("teamTricode"))
        if team_abbrev not in top_players_by_team:
            continue
        top_players_by_team[team_abbrev].append(
            {
                "name": f"{row.get('firstName', '')} {row.get('familyName', '')}".strip(),
                "minutes": row.get("minutes"),
                "points": row.get("points"),
                "rebounds": row.get("reboundsTotal"),
                "assists": row.get("assists"),
                "threeMade": row.get("threePointersMade"),
                "threeAttempts": row.get("threePointersAttempted"),
                "turnovers": row.get("turnovers"),
                "plusMinus": row.get("plusMinusPoints"),
                "usagePct": row.get("usagePercentage"),
                "tsPct": row.get("trueShootingPercentage"),
                "pie": row.get("PIE"),
                "dkPoints": row.get("dkPoints"),
            }
        )

    outcome = {
        "winner": home_team if (home_score or 0) > (away_score or 0) else away_team,
        "margin": (
            abs((home_score or 0) - (away_score or 0))
            if home_score is not None and away_score is not None
            else None
        ),
        "actualTotal": actual_total,
        "vsVegasTotal": (actual_total - matchup["vegas_total"]) if actual_total is not None and matchup.get("vegas_total") is not None else None,
        "homeCovered": (
            (home_score or 0) + (matchup.get("home_spread") or 0) > (away_score or 0)
            if home_score is not None and away_score is not None and matchup.get("home_spread") is not None
            else None
        ),
    }
    favorite = None
    underdog = None
    if matchup.get("home_ml") is not None and matchup.get("away_ml") is not None:
        if matchup["home_ml"] < matchup["away_ml"]:
            favorite = home_team
            underdog = away_team
        else:
            favorite = away_team
            underdog = home_team
    favorite_score = None
    underdog_score = None
    if favorite == home_team:
        favorite_score = home_score
        underdog_score = away_score
    elif favorite == away_team:
        favorite_score = away_score
        underdog_score = home_score
    favorite_won = None
    favorite_covered = None
    if favorite_score is not None and underdog_score is not None:
        favorite_won = favorite_score > underdog_score
    if (
        favorite_score is not None
        and underdog_score is not None
        and matchup.get("home_spread") is not None
        and favorite is not None
    ):
        favorite_spread = abs(float(matchup["home_spread"]))
        favorite_margin = float(favorite_score) - float(underdog_score)
        favorite_covered = favorite_margin > favorite_spread
    else:
        favorite_spread = None
    game_went_over = None
    if actual_total is not None and matchup.get("vegas_total") is not None:
        game_went_over = actual_total > matchup["vegas_total"]
    margin = outcome["margin"]
    evidence_flags = {
        "competitiveGameLikely": margin is not None and margin <= 12,
        "possibleGarbageTime": margin is not None and margin >= 20,
        "heavyMinutesHome": home_minute_summary["players40Plus"] >= 2 or (home_minute_summary["topFiveAvgMinutes"] or 0) >= 34,
        "heavyMinutesAway": away_minute_summary["players40Plus"] >= 2 or (away_minute_summary["topFiveAvgMinutes"] or 0) >= 34,
        "slowPaceRelativeToSeason": (
            home_box.get("pace") is not None
            and matchup.get("home_pace") is not None
            and away_box.get("pace") is not None
            and matchup.get("away_pace") is not None
            and float(home_box["pace"]) < float(matchup["home_pace"])
            and float(away_box["pace"]) < float(matchup["away_pace"])
        ),
        "enteredClutchWindow": bool(pbp_summary.get("enteredClutchWindow")),
    }
    starter_vs_bench = {
        away_team: {
            "benchPoints": away_box.get("bench_points"),
            "starterPoints": round((away_box.get("points") or 0) - (away_box.get("bench_points") or 0), 1)
            if away_box.get("points") is not None and away_box.get("bench_points") is not None else None,
        },
        home_team: {
            "benchPoints": home_box.get("bench_points"),
            "starterPoints": round((home_box.get("points") or 0) - (home_box.get("bench_points") or 0), 1)
            if home_box.get("points") is not None and home_box.get("bench_points") is not None else None,
        },
    }
    shooting_gap_summary = {
        "fgPctDiff": round_diff(away_box.get("fg_pct"), home_box.get("fg_pct")),
        "threePctDiff": round_diff(away_box.get("three_pct"), home_box.get("three_pct")),
        "efgPctDiff": round_diff(away_box.get("efg_pct"), home_box.get("efg_pct")),
        "tsPctDiff": round_diff(away_box.get("ts_pct"), home_box.get("ts_pct")),
        "betterShootingTeam": away_team if (away_box.get("ts_pct") or 0) > (home_box.get("ts_pct") or 0) else home_team,
    }
    edge_candidates: list[dict[str, Any]] = []
    for label, team, value in [
        ("Bench points edge", away_team, round_diff(away_box.get("bench_points"), home_box.get("bench_points"), 1)),
        ("Offensive rebounds edge", away_team, round_diff(away_box.get("offensive_rebounds"), home_box.get("offensive_rebounds"), 1)),
        ("Turnover edge", away_team, round_diff((home_box.get("turnovers") or 0), (away_box.get("turnovers") or 0), 1)
         if home_box.get("turnovers") is not None and away_box.get("turnovers") is not None else None),
        ("True shooting edge", away_team, round_diff(away_box.get("ts_pct"), home_box.get("ts_pct"))),
        ("Offensive rating edge", away_team, round_diff(away_box.get("offensive_rating"), home_box.get("offensive_rating"), 1)),
    ]:
        if value is None:
            continue
        edge_candidates.append({"label": label, "team": team, "value": value})
    largest_team_edge = max(edge_candidates, key=lambda item: abs(float(item["value"])), default=None)
    recent_form = {
        away_team: build_recent_form_context(db, game_date, away_team, current_away_snapshot),
        home_team: build_recent_form_context(db, game_date, home_team, current_home_snapshot),
    }
    series_context = build_series_context(
        db,
        current_game_date=game_date,
        away_team=away_team,
        home_team=home_team,
        current_away_snapshot=current_away_snapshot,
        current_home_snapshot=current_home_snapshot,
    )

    return {
        "game": {
            "date": game_date,
            "gameId": game_id,
            "matchup": f"{away_team} @ {home_team}",
            "finalScore": f"{away_team} {away_score} - {home_team} {home_score}",
            "outcome": {
                **outcome,
                "gameWentOver": game_went_over,
            },
        },
        "vegas": {
            "vegasTotal": matchup.get("vegas_total"),
            "homeSpread": matchup.get("home_spread"),
            "homeMl": matchup.get("home_ml"),
            "awayMl": matchup.get("away_ml"),
            "homeImpliedPoints": matchup.get("home_implied"),
            "awayImpliedPoints": matchup.get("away_implied"),
            "homeWinProb": vegas_home_prob,
            "awayWinProb": (1 - vegas_home_prob) if vegas_home_prob is not None else None,
            "ourGameTotalPred": matchup.get("our_game_total_pred"),
            "favorite": favorite,
            "underdog": underdog,
            "favoriteMl": matchup.get("home_ml") if favorite == home_team else matchup.get("away_ml"),
            "underdogMl": matchup.get("away_ml") if favorite == home_team else matchup.get("home_ml"),
            "favoriteWinProb": vegas_home_prob if favorite == home_team else (1 - vegas_home_prob) if vegas_home_prob is not None else None,
            "underdogWinProb": (1 - vegas_home_prob) if favorite == home_team and vegas_home_prob is not None else vegas_home_prob if favorite == away_team else None,
            "favoriteSpread": favorite_spread,
            "favoriteWon": favorite_won,
            "favoriteCovered": favorite_covered,
        },
        "seasonContext": {
            home_team: {
                "pace": matchup.get("home_pace"),
                "offRtg": matchup.get("home_off_rtg"),
                "defRtg": matchup.get("home_def_rtg"),
                "netRtg": (
                    (matchup.get("home_off_rtg") or 0) - (matchup.get("home_def_rtg") or 0)
                    if matchup.get("home_off_rtg") is not None and matchup.get("home_def_rtg") is not None
                    else None
                ),
            },
            away_team: {
                "pace": matchup.get("away_pace"),
                "offRtg": matchup.get("away_off_rtg"),
                "defRtg": matchup.get("away_def_rtg"),
                "netRtg": (
                    (matchup.get("away_off_rtg") or 0) - (matchup.get("away_def_rtg") or 0)
                    if matchup.get("away_off_rtg") is not None and matchup.get("away_def_rtg") is not None
                    else None
                ),
            },
        },
        "teamBoxScore": {
            away_team: away_box,
            home_team: home_box,
        },
        "minuteSummary": {
            away_team: away_minute_summary,
            home_team: home_minute_summary,
        },
        "starterVsBenchProduction": starter_vs_bench,
        "shootingGapSummary": shooting_gap_summary,
        "largestTeamEdge": largest_team_edge,
        "recentForm": recent_form,
        "seriesContext": series_context,
        "playByPlaySummary": pbp_summary,
        "evidenceFlags": evidence_flags,
        "topPlayers": top_players_by_team,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Explain an NBA game outcome with DeepSeek")
    parser.add_argument("--date", required=True, help="Game date YYYY-MM-DD")
    parser.add_argument("--home", required=True, help="Home team abbreviation, e.g. BOS")
    parser.add_argument("--away", required=True, help="Away team abbreviation, e.g. NYK")
    parser.add_argument("--audience", default="dfs analyst", help="Explanation audience")
    parser.add_argument("--dump-context", action="store_true", help="Print the structured context without calling DeepSeek")
    parser.add_argument("--save-context", help="Optional path to save context JSON")
    return parser.parse_args()


def main() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    args = parse_args()
    cfg = load_config()
    db = DatabaseManager(cfg.database_url or "")
    context = build_game_context(
        db,
        game_date=args.date,
        home_abbrev=args.home,
        away_abbrev=args.away,
    )
    context_json = json.dumps(context, indent=2)

    if args.save_context:
        Path(args.save_context).write_text(context_json, encoding="utf-8")

    if args.dump_context:
        print(context_json)
        return

    explanation = explain_game_outcome_message(
        context=context_json,
        sport="nba",
        audience=args.audience,
    )
    print(explanation)


if __name__ == "__main__":
    main()
