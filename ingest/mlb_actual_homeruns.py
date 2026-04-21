"""Import MLB hitter home-run outcomes into saved DraftKings rows.

This is intentionally narrow: it only writes the one outcome the Home Run
board needs for calibration, `dk_players.actual_hr`, then mirrors it into the
existing homerun tracking snapshots.

Usage:
    python -m ingest.mlb_actual_homeruns --date 2026-04-18
    python -m ingest.mlb_actual_homeruns --start 2026-04-01 --end 2026-04-18
"""

from __future__ import annotations

import argparse
import re
import unicodedata
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Iterable

import requests

from config import load_config
from db.database import DatabaseManager
from ingest.mlb_schedule import MLB_API_BASE
from ingest.mlb_teams import MLB_ID_TO_ABBREV


FINAL_STATES = {"Final", "Game Over", "Completed Early"}
TEAM_ALIASES = {
    "ARI": "ARI",
    "AZ": "ARI",
    "ATH": "OAK",
    "OAK": "OAK",
    "CHW": "CWS",
    "CWS": "CWS",
    "KCR": "KC",
    "KC": "KC",
    "LAD": "LAD",
    "LA": "LAD",
    "LAA": "LAA",
    "SDP": "SD",
    "SD": "SD",
    "SFG": "SF",
    "SF": "SF",
    "TBR": "TB",
    "TB": "TB",
    "WAS": "WSH",
    "WSN": "WSH",
    "WSH": "WSH",
}
SUFFIXES = {"jr", "sr", "ii", "iii", "iv", "v"}


@dataclass(frozen=True)
class HitterHrOutcome:
    game_id: str
    game_date: str
    name: str
    team_abbrev: str
    home_runs: int

    @property
    def name_key(self) -> str:
        return normalize_name(self.name)

    @property
    def team_key(self) -> str:
        return normalize_team(self.team_abbrev)


def normalize_name(value: str | None) -> str:
    text = unicodedata.normalize("NFKD", value or "").encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"[^a-zA-Z0-9]+", " ", text).strip().lower()
    parts = text.split()
    if parts and parts[-1] in SUFFIXES:
        parts = parts[:-1]
    return " ".join(parts)


def normalize_team(value: str | None) -> str:
    team = (value or "").strip().upper()
    return TEAM_ALIASES.get(team, team)


def iter_dates(start: str, end: str) -> Iterable[str]:
    current = date.fromisoformat(start)
    final = date.fromisoformat(end)
    while current <= final:
        yield current.isoformat()
        current += timedelta(days=1)


def fetch_final_games(game_date: str, timeout: int) -> list[dict]:
    resp = requests.get(
        f"{MLB_API_BASE}/schedule",
        params={"sportId": 1, "date": game_date},
        timeout=timeout,
    )
    resp.raise_for_status()
    dates = resp.json().get("dates", [])
    games = dates[0].get("games", []) if dates else []
    return [
        game for game in games
        if game.get("status", {}).get("detailedState") in FINAL_STATES
    ]


def fetch_game_homeruns(game: dict, game_date: str, timeout: int) -> list[HitterHrOutcome]:
    game_id = str(game.get("gamePk") or "")
    if not game_id:
        return []

    resp = requests.get(f"{MLB_API_BASE}/game/{game_id}/boxscore", timeout=timeout)
    resp.raise_for_status()
    boxscore = resp.json()

    outcomes: list[HitterHrOutcome] = []
    for side in ("home", "away"):
        mlb_team_id = game.get("teams", {}).get(side, {}).get("team", {}).get("id")
        team_abbrev = MLB_ID_TO_ABBREV.get(mlb_team_id)
        if not team_abbrev:
            continue

        players = boxscore.get("teams", {}).get(side, {}).get("players", {})
        for player in players.values():
            batting = player.get("stats", {}).get("batting")
            if not batting:
                continue
            name = player.get("person", {}).get("fullName")
            if not name:
                continue
            try:
                home_runs = int(batting.get("homeRuns") or 0)
            except (TypeError, ValueError):
                home_runs = 0
            outcomes.append(
                HitterHrOutcome(
                    game_id=game_id,
                    game_date=game_date,
                    name=name,
                    team_abbrev=team_abbrev,
                    home_runs=home_runs,
                )
            )
    return outcomes


def fetch_homerun_outcomes(start: str, end: str, timeout: int) -> list[HitterHrOutcome]:
    outcomes: list[HitterHrOutcome] = []
    for game_date in iter_dates(start, end):
        try:
            games = fetch_final_games(game_date, timeout)
        except requests.RequestException as exc:
            print(f"{game_date}: MLB schedule fetch failed: {exc}")
            continue

        day_outcomes = 0
        for game in games:
            try:
                game_outcomes = fetch_game_homeruns(game, game_date, timeout)
            except requests.RequestException as exc:
                game_id = game.get("gamePk", "unknown")
                print(f"{game_date}: MLB boxscore fetch failed for game {game_id}: {exc}")
                continue
            outcomes.extend(game_outcomes)
            day_outcomes += len(game_outcomes)
        print(f"{game_date}: parsed {day_outcomes} hitter HR outcomes from {len(games)} final games")
    return outcomes


def load_dk_hitter_rows(db: DatabaseManager, start: str, end: str) -> list[dict]:
    return db.execute(
        """
        SELECT
            dp.id,
            dp.slate_id,
            ds.slate_date::text AS slate_date,
            dp.dk_player_id,
            dp.name,
            dp.team_abbrev,
            dp.eligible_positions,
            mm.game_id
        FROM dk_players dp
        INNER JOIN dk_slates ds ON ds.id = dp.slate_id
        LEFT JOIN mlb_matchups mm ON mm.id = dp.matchup_id
        WHERE ds.sport = 'mlb'
          AND ds.slate_date BETWEEN %s::date AND %s::date
          AND NOT (dp.eligible_positions ILIKE '%%SP%%' OR dp.eligible_positions ILIKE '%%RP%%')
        ORDER BY ds.slate_date, dp.slate_id, dp.name
        """,
        (start, end),
    )


def build_outcome_indexes(
    outcomes: list[HitterHrOutcome],
) -> tuple[
    dict[tuple[str, str, str], int],
    dict[tuple[str, str, str], int],
    dict[tuple[str, str], int],
    set[tuple[str, str]],
    dict[tuple[str, str], int],
]:
    by_game: dict[tuple[str, str, str], int] = {}
    by_date: dict[tuple[str, str, str], int] = {}
    name_counts: dict[tuple[str, str], int] = {}
    name_values: dict[tuple[str, str], int] = {}
    final_game_teams: set[tuple[str, str]] = set()
    final_date_team_counts: dict[tuple[str, str], int] = {}
    seen_date_team_games: set[tuple[str, str, str]] = set()

    for outcome in outcomes:
        if not outcome.name_key or not outcome.team_key:
            continue
        final_game_teams.add((outcome.game_id, outcome.team_key))
        date_team_game = (outcome.game_date, outcome.team_key, outcome.game_id)
        if date_team_game not in seen_date_team_games:
            seen_date_team_games.add(date_team_game)
            date_team_key = (outcome.game_date, outcome.team_key)
            final_date_team_counts[date_team_key] = final_date_team_counts.get(date_team_key, 0) + 1
        by_game[(outcome.game_id, outcome.name_key, outcome.team_key)] = outcome.home_runs
        date_key = (outcome.game_date, outcome.name_key, outcome.team_key)
        by_date[date_key] = by_date.get(date_key, 0) + outcome.home_runs
        name_key = (outcome.game_date, outcome.name_key)
        name_counts[name_key] = name_counts.get(name_key, 0) + 1
        name_values[name_key] = name_values.get(name_key, 0) + outcome.home_runs

    unique_name = {
        key: name_values[key]
        for key, count in name_counts.items()
        if count == 1
    }
    return by_game, by_date, unique_name, final_game_teams, final_date_team_counts


def match_row_actual_hr(
    row: dict,
    by_game: dict[tuple[str, str, str], int],
    by_date: dict[tuple[str, str, str], int],
    unique_name: dict[tuple[str, str], int],
    final_game_teams: set[tuple[str, str]],
    final_date_team_counts: dict[tuple[str, str], int],
) -> int | None:
    name_key = normalize_name(row.get("name"))
    team_key = normalize_team(row.get("team_abbrev"))
    slate_date = str(row.get("slate_date") or "")
    game_id = str(row.get("game_id") or "")

    if game_id:
        exact = by_game.get((game_id, name_key, team_key))
        if exact is not None:
            return exact
        if (game_id, team_key) in final_game_teams:
            return 0

    by_date_value = by_date.get((slate_date, name_key, team_key))
    if by_date_value is not None:
        return by_date_value
    if final_date_team_counts.get((slate_date, team_key)) == 1:
        return 0

    return unique_name.get((slate_date, name_key))


def sync_snapshot_actuals(db: DatabaseManager, slate_ids: list[int]) -> None:
    if not slate_ids:
        return
    db.execute(
        """
        UPDATE mlb_homerun_player_snapshots hps
        SET actual_hr = dp.actual_hr,
            hit_hr_1plus = CASE
                WHEN dp.actual_hr IS NULL THEN hps.hit_hr_1plus
                ELSE dp.actual_hr > 0
            END,
            actual_fpts = dp.actual_fpts,
            actual_own_pct = dp.actual_own_pct
        FROM dk_players dp
        WHERE hps.slate_id = dp.slate_id
          AND hps.dk_player_id = dp.dk_player_id
          AND hps.slate_id = ANY(%s)
        """,
        (slate_ids,),
    )


def import_actual_homeruns(db: DatabaseManager, start: str, end: str, timeout: int, dry_run: bool = False) -> dict[str, int]:
    outcomes = fetch_homerun_outcomes(start, end, timeout)
    hitter_rows = load_dk_hitter_rows(db, start, end)
    by_game, by_date, unique_name, final_game_teams, final_date_team_counts = build_outcome_indexes(outcomes)

    updates: list[tuple[int, int, int]] = []
    unmatched = 0
    affected_slate_ids: set[int] = set()

    for row in hitter_rows:
        actual_hr = match_row_actual_hr(row, by_game, by_date, unique_name, final_game_teams, final_date_team_counts)
        if actual_hr is None:
            unmatched += 1
            continue
        updates.append((actual_hr, row["id"], actual_hr))
        affected_slate_ids.add(int(row["slate_id"]))

    updated = 0
    if not dry_run and updates:
        with db.connect() as conn:
            cur = conn.cursor()
            for params in updates:
                cur.execute(
                    """
                    UPDATE dk_players
                    SET actual_hr = %s
                    WHERE id = %s
                      AND actual_hr IS DISTINCT FROM %s
                    """,
                    params,
                )
                updated += cur.rowcount
        sync_snapshot_actuals(db, sorted(affected_slate_ids))

    return {
        "outcomes": len(outcomes),
        "dk_rows": len(hitter_rows),
        "matched": len(updates),
        "updated": updated if not dry_run else len(updates),
        "unmatched": unmatched,
        "slates": len(affected_slate_ids),
    }


def resolve_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Import actual MLB hitter HR outcomes")
    parser.add_argument("--date", help="Single date YYYY-MM-DD")
    parser.add_argument("--start", help="Start date YYYY-MM-DD")
    parser.add_argument("--end", help="End date YYYY-MM-DD")
    parser.add_argument("--dry-run", action="store_true", help="Parse and match without writing")
    return parser.parse_args()


def main() -> None:
    args = resolve_args()
    if args.date:
        start = end = args.date
    else:
        start = args.start or date.today().isoformat()
        end = args.end or start

    datetime.strptime(start, "%Y-%m-%d")
    datetime.strptime(end, "%Y-%m-%d")
    if date.fromisoformat(start) > date.fromisoformat(end):
        raise ValueError("--start cannot be after --end")

    config = load_config()
    db = DatabaseManager(config.database_url)
    summary = import_actual_homeruns(
        db,
        start=start,
        end=end,
        timeout=config.mlb_api.timeout_seconds,
        dry_run=args.dry_run,
    )
    action = "Would update" if args.dry_run else "Updated"
    print(
        f"{action} {summary['updated']} player rows across {summary['slates']} slates "
        f"({summary['matched']}/{summary['dk_rows']} DK hitters matched, "
        f"{summary['outcomes']} MLB outcomes parsed, {summary['unmatched']} unmatched)"
    )


if __name__ == "__main__":
    main()
