"""Build baseball-only MLB home run training rows.

This ingester does not use DraftKings player pools, salaries, ownership, or
market odds. It creates one row per hitter-game with the actual HR outcome,
game context, opposing starting pitcher, park, and joined season stat features.

Initial feature_source is "season_aggregate"; a later rolling version can write
the same table with pre-game rolling features.

Usage:
    python -m ingest.mlb_homerun_training --season 2024 --dry-run
    python -m ingest.mlb_homerun_training --season 2024
    python -m ingest.mlb_homerun_training --season 2025 --start 2025-08-01 --end 2025-09-28
"""

from __future__ import annotations

import argparse
import logging
import time
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Iterable

import requests

from config import load_config
from db.database import DatabaseManager
from db.queries import build_mlb_team_abbrev_cache, upsert_mlb_homerun_training_rows
from ingest.mlb_actual_homeruns import FINAL_STATES, normalize_name
from ingest.mlb_schedule import MLB_API_BASE
from ingest.mlb_teams import MLB_ID_TO_ABBREV

logger = logging.getLogger(__name__)

REGULAR_SEASON_DEFAULTS = {
    "2024": ("2024-03-20", "2024-09-29"),
    "2025": ("2025-03-18", "2025-09-28"),
}


@dataclass
class FeatureLookup:
    by_team_name: dict[tuple[int, str], dict]
    unique_by_name: dict[str, dict]

    def resolve(self, name: str | None, team_id: int | None) -> dict | None:
        name_key = normalize_name(name)
        if not name_key:
            return None
        if team_id is not None:
            row = self.by_team_name.get((team_id, name_key))
            if row:
                return row
        return self.unique_by_name.get(name_key)


def iter_dates(start: str, end: str) -> Iterable[str]:
    current = date.fromisoformat(start)
    final = date.fromisoformat(end)
    while current <= final:
        yield current.isoformat()
        current += timedelta(days=1)


def _int_or_none(value) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _parse_batting_order(value) -> int | None:
    raw = _int_or_none(value)
    if raw is None:
        return None
    order = raw // 100 if raw >= 100 else raw
    return order if 1 <= order <= 9 else None


def _team_cache_by_mlb_id(db: DatabaseManager) -> dict[int, tuple[int, str]]:
    abbrev_cache = build_mlb_team_abbrev_cache(db)
    out: dict[int, tuple[int, str]] = {}
    for mlb_id, abbrev in MLB_ID_TO_ABBREV.items():
        team_id = abbrev_cache.get(abbrev)
        if team_id:
            out[mlb_id] = (team_id, abbrev)
    return out


def _build_feature_lookup(rows: list[dict]) -> FeatureLookup:
    by_team_name: dict[tuple[int, str], dict] = {}
    grouped_by_name: dict[str, list[dict]] = {}
    for row in rows:
        name_key = normalize_name(row.get("name"))
        if not name_key:
            continue
        team_id = row.get("team_id")
        if team_id is not None:
            by_team_name[(int(team_id), name_key)] = row
        grouped_by_name.setdefault(name_key, []).append(row)

    unique_by_name = {
        name_key: values[0]
        for name_key, values in grouped_by_name.items()
        if len(values) == 1
    }
    return FeatureLookup(by_team_name=by_team_name, unique_by_name=unique_by_name)


def load_batter_lookup(db: DatabaseManager, season: str) -> FeatureLookup:
    rows = db.execute(
        """
        SELECT
            player_id,
            season,
            team_id,
            name,
            games,
            pa_pg,
            hr_pg,
            iso,
            slg,
            wrc_plus,
            wrc_plus_vs_l,
            wrc_plus_vs_r
        FROM mlb_batter_stats
        WHERE season = %s
        """,
        (season,),
    )
    return _build_feature_lookup(rows)


def load_pitcher_lookup(db: DatabaseManager, season: str) -> FeatureLookup:
    rows = db.execute(
        """
        SELECT
            player_id,
            season,
            team_id,
            name,
            hand,
            games,
            ip_pg,
            hr_per_9,
            hr_fb_pct,
            xfip,
            fip,
            k_per_9,
            bb_per_9,
            whip,
            era
        FROM mlb_pitcher_stats
        WHERE season = %s
        """,
        (season,),
    )
    return _build_feature_lookup(rows)


def load_park_factors(db: DatabaseManager, season: str) -> dict[int, dict]:
    rows = db.execute(
        """
        SELECT team_id, runs_factor, hr_factor
        FROM mlb_park_factors
        WHERE season = %s
        """,
        (season,),
    )
    return {int(row["team_id"]): row for row in rows if row.get("team_id") is not None}


def fetch_final_games(game_date: str, timeout: int) -> list[dict]:
    resp = requests.get(
        f"{MLB_API_BASE}/schedule",
        params={
            "sportId": 1,
            "date": game_date,
            "gameTypes": "R",
            "hydrate": "probablePitcher",
        },
        timeout=timeout,
    )
    resp.raise_for_status()
    dates = resp.json().get("dates", [])
    games = dates[0].get("games", []) if dates else []
    return [
        game for game in games
        if game.get("status", {}).get("detailedState") in FINAL_STATES
    ]


def fetch_boxscore(game_id: str, timeout: int) -> dict:
    resp = requests.get(f"{MLB_API_BASE}/game/{game_id}/boxscore", timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def _starter_from_boxscore(boxscore: dict, side: str, fallback_id: int | None, fallback_name: str | None) -> dict:
    team = boxscore.get("teams", {}).get(side, {})
    players = team.get("players", {})
    pitcher_ids = team.get("pitchers") or []
    if pitcher_ids:
        starter_id = _int_or_none(pitcher_ids[0])
        player = players.get(f"ID{starter_id}", {}) if starter_id is not None else {}
        name = player.get("person", {}).get("fullName")
        return {"id": starter_id, "name": name or fallback_name}
    return {"id": fallback_id, "name": fallback_name}


def _hitter_split_wrc(batter: dict | None, pitcher_hand: str | None):
    if not batter or not pitcher_hand:
        return None
    hand = pitcher_hand.upper()
    if hand == "L":
        return batter.get("wrc_plus_vs_l")
    if hand == "R":
        return batter.get("wrc_plus_vs_r")
    return None


def build_game_rows(
    season: str,
    game: dict,
    boxscore: dict,
    team_by_mlb_id: dict[int, tuple[int, str]],
    batter_lookup: FeatureLookup,
    pitcher_lookup: FeatureLookup,
    park_factors: dict[int, dict],
    feature_source: str,
) -> list[dict]:
    game_id = str(game.get("gamePk") or "")
    if not game_id:
        return []

    game_date = game.get("officialDate") or game.get("gameDate", "")[:10]
    home_info = game.get("teams", {}).get("home", {})
    away_info = game.get("teams", {}).get("away", {})
    home_mlb_id = home_info.get("team", {}).get("id")
    away_mlb_id = away_info.get("team", {}).get("id")
    home_team = team_by_mlb_id.get(home_mlb_id)
    away_team = team_by_mlb_id.get(away_mlb_id)
    if not home_team or not away_team:
        return []

    home_team_id, home_abbrev = home_team
    away_team_id, away_abbrev = away_team
    ballpark = game.get("venue", {}).get("name")
    park = park_factors.get(home_team_id, {})

    starters = {
        "home": _starter_from_boxscore(
            boxscore,
            "home",
            _int_or_none(home_info.get("probablePitcher", {}).get("id")),
            home_info.get("probablePitcher", {}).get("fullName"),
        ),
        "away": _starter_from_boxscore(
            boxscore,
            "away",
            _int_or_none(away_info.get("probablePitcher", {}).get("id")),
            away_info.get("probablePitcher", {}).get("fullName"),
        ),
    }

    rows: list[dict] = []
    for side in ("home", "away"):
        is_home = side == "home"
        hitter_team_id, hitter_abbrev = (home_team_id, home_abbrev) if is_home else (away_team_id, away_abbrev)
        opponent_team_id, opponent_abbrev = (away_team_id, away_abbrev) if is_home else (home_team_id, home_abbrev)
        opp_side = "away" if is_home else "home"
        opp_sp = starters[opp_side]
        opp_pitcher = pitcher_lookup.resolve(opp_sp.get("name"), opponent_team_id)
        opp_hand = (opp_pitcher or {}).get("hand")

        players = boxscore.get("teams", {}).get(side, {}).get("players", {})
        for player in players.values():
            batting = player.get("stats", {}).get("batting")
            if not batting:
                continue
            plate_appearances = _int_or_none(batting.get("plateAppearances")) or 0
            if plate_appearances <= 0:
                continue

            person = player.get("person", {})
            hitter_mlb_id = _int_or_none(person.get("id"))
            hitter_name = person.get("fullName")
            if hitter_mlb_id is None or not hitter_name:
                continue

            actual_hr = _int_or_none(batting.get("homeRuns")) or 0
            batter = batter_lookup.resolve(hitter_name, hitter_team_id)
            split_wrc = _hitter_split_wrc(batter, opp_hand)

            rows.append({
                "season": season,
                "game_date": game_date,
                "game_id": game_id,
                "hitter_mlb_id": hitter_mlb_id,
                "hitter_name": hitter_name,
                "hitter_team_id": hitter_team_id,
                "hitter_team_abbrev": hitter_abbrev,
                "opponent_team_id": opponent_team_id,
                "opponent_team_abbrev": opponent_abbrev,
                "is_home": is_home,
                "ballpark": ballpark,
                "batting_order": _parse_batting_order(player.get("battingOrder")),
                "plate_appearances": plate_appearances,
                "at_bats": _int_or_none(batting.get("atBats")),
                "opposing_sp_mlb_id": opp_sp.get("id"),
                "opposing_sp_name": opp_sp.get("name"),
                "opposing_sp_hand": opp_hand,
                "hitter_games": (batter or {}).get("games"),
                "hitter_pa_pg": (batter or {}).get("pa_pg"),
                "hitter_hr_pg": (batter or {}).get("hr_pg"),
                "hitter_iso": (batter or {}).get("iso"),
                "hitter_slg": (batter or {}).get("slg"),
                "hitter_wrc_plus": (batter or {}).get("wrc_plus"),
                "hitter_split_wrc_plus": split_wrc,
                "pitcher_games": (opp_pitcher or {}).get("games"),
                "pitcher_ip_pg": (opp_pitcher or {}).get("ip_pg"),
                "pitcher_hr_per_9": (opp_pitcher or {}).get("hr_per_9"),
                "pitcher_hr_fb_pct": (opp_pitcher or {}).get("hr_fb_pct"),
                "pitcher_xfip": (opp_pitcher or {}).get("xfip"),
                "pitcher_fip": (opp_pitcher or {}).get("fip"),
                "pitcher_k_per_9": (opp_pitcher or {}).get("k_per_9"),
                "pitcher_bb_per_9": (opp_pitcher or {}).get("bb_per_9"),
                "pitcher_whip": (opp_pitcher or {}).get("whip"),
                "pitcher_era": (opp_pitcher or {}).get("era"),
                "park_runs_factor": park.get("runs_factor"),
                "park_hr_factor": park.get("hr_factor"),
                "weather_temp": None,
                "wind_speed": None,
                "wind_direction": None,
                "actual_hr": actual_hr,
                "hit_hr_1plus": actual_hr >= 1,
                "feature_source": feature_source,
                "source": "mlb_statsapi_boxscore",
            })
    return rows


def ingest_training_rows(
    db: DatabaseManager,
    season: str,
    start: str,
    end: str,
    timeout: int,
    feature_source: str,
    dry_run: bool = False,
    sleep_seconds: float = 0.15,
) -> dict[str, int]:
    team_by_mlb_id = _team_cache_by_mlb_id(db)
    batter_lookup = load_batter_lookup(db, season)
    pitcher_lookup = load_pitcher_lookup(db, season)
    park_factors = load_park_factors(db, season)

    summary = {
        "dates": 0,
        "games": 0,
        "rows": 0,
        "written": 0,
        "hr_rows": 0,
        "hitter_feature_rows": 0,
        "pitcher_feature_rows": 0,
        "park_feature_rows": 0,
        "errors": 0,
    }

    for game_date in iter_dates(start, end):
        summary["dates"] += 1
        try:
            games = fetch_final_games(game_date, timeout)
        except requests.RequestException as exc:
            summary["errors"] += 1
            print(f"{game_date}: schedule fetch failed: {exc}")
            continue

        day_rows: list[dict] = []
        for game in games:
            game_id = str(game.get("gamePk") or "")
            if not game_id:
                continue
            try:
                boxscore = fetch_boxscore(game_id, timeout)
            except requests.RequestException as exc:
                summary["errors"] += 1
                print(f"{game_date}: boxscore fetch failed for {game_id}: {exc}")
                continue
            rows = build_game_rows(
                season=season,
                game=game,
                boxscore=boxscore,
                team_by_mlb_id=team_by_mlb_id,
                batter_lookup=batter_lookup,
                pitcher_lookup=pitcher_lookup,
                park_factors=park_factors,
                feature_source=feature_source,
            )
            day_rows.extend(rows)
            summary["games"] += 1

        summary["rows"] += len(day_rows)
        summary["hr_rows"] += sum(1 for row in day_rows if row["hit_hr_1plus"])
        summary["hitter_feature_rows"] += sum(1 for row in day_rows if row.get("hitter_hr_pg") is not None)
        summary["pitcher_feature_rows"] += sum(
            1 for row in day_rows
            if row.get("pitcher_hr_per_9") is not None or row.get("pitcher_xfip") is not None
        )
        summary["park_feature_rows"] += sum(1 for row in day_rows if row.get("park_hr_factor") is not None)
        if dry_run:
            print(f"{game_date}: would write {len(day_rows)} hitter-game rows from {len(games)} games")
        else:
            written = upsert_mlb_homerun_training_rows(db, day_rows)
            summary["written"] += written
            print(f"{game_date}: wrote {written} hitter-game rows from {len(games)} games")

        if sleep_seconds > 0:
            time.sleep(sleep_seconds)

    if dry_run:
        summary["written"] = summary["rows"]
    return summary


def resolve_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest MLB baseball-only HR training rows")
    parser.add_argument("--season", default="2025", help="MLB season year, e.g. 2024")
    parser.add_argument("--date", help="Single date YYYY-MM-DD")
    parser.add_argument("--start", help="Start date YYYY-MM-DD")
    parser.add_argument("--end", help="End date YYYY-MM-DD")
    parser.add_argument("--feature-source", default="season_aggregate")
    parser.add_argument("--sleep", type=float, default=0.15, help="Seconds to sleep between dates")
    parser.add_argument("--dry-run", action="store_true", help="Fetch and parse without writing")
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    args = resolve_args()
    default_start, default_end = REGULAR_SEASON_DEFAULTS.get(
        str(args.season),
        (f"{args.season}-03-01", f"{args.season}-10-01"),
    )
    if args.date:
        start = end = args.date
    else:
        start = args.start or default_start
        end = args.end or default_end
    if date.fromisoformat(start) > date.fromisoformat(end):
        raise ValueError("--start cannot be after --end")

    config = load_config()
    db = DatabaseManager(config.database_url)
    summary = ingest_training_rows(
        db,
        season=str(args.season),
        start=start,
        end=end,
        timeout=config.mlb_api.timeout_seconds,
        feature_source=args.feature_source,
        dry_run=args.dry_run,
        sleep_seconds=args.sleep,
    )
    action = "Would write" if args.dry_run else "Wrote"
    print(
        f"{action} {summary['written']} hitter-game rows "
        f"({summary['hr_rows']} HR-positive) across {summary['games']} games "
        f"and {summary['dates']} dates; "
        f"features hitter={summary['hitter_feature_rows']}, "
        f"pitcher={summary['pitcher_feature_rows']}, "
        f"park={summary['park_feature_rows']}; errors={summary['errors']}"
    )


if __name__ == "__main__":
    main()
