"""Audit and ingest historical CFB games and betting references from CFBD.

CFBD historical rows are never written to ``game_odds_history``.  The API
provides source-reported opens and historical reference values, but not the
timestamp trail required to call those observations live market movement.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

from config import DATA_DIR, load_config
from db.database import DatabaseManager
from db.queries import (
    insert_cfb_historical_line,
    set_cfb_canonical_historical_provider,
    upsert_cfb_matchup,
    upsert_cfb_team,
    upsert_cfb_team_alias,
    upsert_cfb_venue,
)
from ingest.game_odds_market import eastern_date, parse_iso

logger = logging.getLogger(__name__)

CFBD_BASE = "https://api.collegefootballdata.com"
SOURCE = "cfbd"
PROVIDER_PRIORITY = ("consensus", "draftkings", "fanduel", "betmgm", "caesars")


def _stable_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def payload_hash(value: Any) -> str:
    return hashlib.sha256(_stable_json(value).encode("utf-8")).hexdigest()


def _cache_path(cache_dir: Path, endpoint: str, season: int) -> Path:
    return cache_dir / f"{endpoint.replace('/', '_')}-{season}.json"


def fetch_cfbd(
    endpoint: str, *, api_key: str, season: int, cache_dir: Path,
    use_cache: bool = True, attempts: int = 4,
) -> list[dict]:
    path = _cache_path(cache_dir, endpoint, season)
    if use_cache and path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    if not api_key:
        raise ValueError("CFBD_API_KEY is required (cached audit files may be used without it)")
    response = None
    for attempt in range(attempts):
        try:
            response = requests.get(
                f"{CFBD_BASE}/{endpoint}",
                params={"year": season, "seasonType": "both"},
                headers={"Authorization": f"Bearer {api_key}"},
                timeout=60,
            )
            response.raise_for_status()
            payload = response.json() or []
            if not isinstance(payload, list):
                raise ValueError(f"CFBD /{endpoint} returned a non-list payload")
            cache_dir.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            logger.info(
                "CFBD %s %s: %s rows; quota remaining=%s used=%s last=%s",
                endpoint, season, len(payload),
                response.headers.get("x-requests-remaining", "?"),
                response.headers.get("x-requests-used", "?"),
                response.headers.get("x-requests-last", "?"),
            )
            return payload
        except (requests.RequestException, ValueError) as exc:
            retryable = not isinstance(exc, requests.HTTPError) or (
                exc.response is not None and exc.response.status_code in (429, 500, 502, 503, 504)
            )
            if attempt == attempts - 1 or not retryable:
                raise
            delay = 2 ** attempt
            logger.warning("CFBD /%s attempt %s failed; retrying in %ss: %s", endpoint, attempt + 1, delay, exc)
            time.sleep(delay)
    raise RuntimeError(f"CFBD /{endpoint} failed: {response}")


def audit_season(games: list[dict], line_games: list[dict], season: int) -> dict:
    line_by_id = {int(item["id"]): item for item in line_games if item.get("id") is not None}
    providers: Counter[str] = Counter()
    market_counts = Counter()
    priced_spreads = 0
    duplicate_ids = len(games) - len({item.get("id") for item in games})
    classifications = Counter()
    for game in games:
        classifications[(game.get("homeClassification"), game.get("awayClassification"))] += 1
    for item in line_games:
        for line in item.get("lines") or []:
            providers[str(line.get("provider") or "unknown")] += 1
            if line.get("spread") is not None:
                market_counts["spread"] += 1
            if line.get("overUnder") is not None:
                market_counts["total"] += 1
            if line.get("homeMoneyline") is not None and line.get("awayMoneyline") is not None:
                market_counts["moneyline"] += 1
            # CFBD's GameLine schema currently has no spread price fields.
            priced_spreads += int(line.get("spreadPrice") is not None)
    fbs_vs_fbs = sum(
        1 for game in games
        if str(game.get("homeClassification", "")).lower() == "fbs"
        and str(game.get("awayClassification", "")).lower() == "fbs"
    )
    completed = sum(
        1 for game in games
        if game.get("completed") and game.get("homePoints") is not None and game.get("awayPoints") is not None
    )
    neutral_known = sum(1 for game in games if game.get("neutralSite") is not None)
    mapped_lines = sum(1 for game_id in line_by_id if game_id in {int(g["id"]) for g in games if g.get("id") is not None})
    return {
        "season": season,
        "audited_at": datetime.now(timezone.utc).isoformat(),
        "games": len(games),
        "completed_games": completed,
        "result_completeness": completed / len(games) if games else 0,
        "fbs_vs_fbs_games": fbs_vs_fbs,
        "line_games": len(line_games),
        "line_games_mapped_to_schedule": mapped_lines,
        "line_game_coverage": mapped_lines / completed if completed else 0,
        "provider_rows": dict(sorted(providers.items())),
        "market_rows": dict(market_counts),
        "spread_price_rows": priced_spreads,
        "neutral_site_completeness": neutral_known / len(games) if games else 0,
        "duplicate_game_ids": duplicate_ids,
        "classification_pairs": {f"{a or 'unknown'}:{b or 'unknown'}": n for (a, b), n in classifications.items()},
        "line_timing_contract": {
            "spreadOpen": "source_reported_open",
            "overUnderOpen": "source_reported_open",
            "spread": "historical_reference",
            "overUnder": "historical_reference",
            "moneylines": "historical_reference",
            "verified_close_available": False,
        },
    }


def _upsert_game(db: DatabaseManager, game: dict) -> int:
    home_id = upsert_cfb_team(
        db, cfbd_team_id=int(game["homeId"]), name=str(game["homeTeam"]),
        conference=game.get("homeConference"), classification=game.get("homeClassification"),
    )
    away_id = upsert_cfb_team(
        db, cfbd_team_id=int(game["awayId"]), name=str(game["awayTeam"]),
        conference=game.get("awayConference"), classification=game.get("awayClassification"),
    )
    upsert_cfb_team_alias(db, provider=SOURCE, alias=str(game["homeTeam"]), team_id=home_id, reviewed=True)
    upsert_cfb_team_alias(db, provider=SOURCE, alias=str(game["awayTeam"]), team_id=away_id, reviewed=True)
    venue_id = None
    if game.get("venue"):
        venue_id = upsert_cfb_venue(
            db,
            cfbd_venue_id=int(game["venueId"]) if game.get("venueId") is not None else None,
            name=str(game["venue"]),
        )
    commence = parse_iso(game.get("startDate"))
    completed = bool(game.get("completed"))
    return upsert_cfb_matchup(
        db, cfbd_game_id=int(game["id"]), season=int(game["season"]),
        season_type=str(game.get("seasonType") or "regular"),
        week=int(game.get("week") or 0), game_date=eastern_date(commence),
        commence_time=commence, start_time_tbd=bool(game.get("startTimeTBD")),
        home_team_id=home_id, away_team_id=away_id, venue_id=venue_id,
        neutral_site=bool(game.get("neutralSite")),
        conference_game=bool(game.get("conferenceGame")), network=None,
        completed=completed,
        home_score=int(game["homePoints"]) if game.get("homePoints") is not None else None,
        away_score=int(game["awayPoints"]) if game.get("awayPoints") is not None else None,
        home_line_scores=game.get("homeLineScores"), away_line_scores=game.get("awayLineScores"),
        game_status="final" if completed else "scheduled",
    )


def _line_rows(matchup_id: int, game: dict, captured_at: datetime) -> list[dict]:
    rows: list[dict] = []
    common = {
        "game_id": matchup_id,
        "source_event_id": str(game.get("id")),
        "home_conference": game.get("homeConference"),
        "away_conference": game.get("awayConference"),
        "home_classification": game.get("homeClassification"),
        "away_classification": game.get("awayClassification"),
        "source_updated_at": None,
        "available_at": None,
        "captured_at": captured_at,
    }
    for source_line in game.get("lines") or []:
        provider = str(source_line.get("provider") or "unknown").strip().lower()
        definitions = (
            ("spread", "historical_reference", source_line.get("spread"), None),
            ("spread", "source_reported_open", source_line.get("spreadOpen"), None),
            ("total", "historical_reference", source_line.get("overUnder"), None),
            ("total", "source_reported_open", source_line.get("overUnderOpen"), None),
            ("moneyline", "historical_reference", source_line.get("homeMoneyline"), source_line.get("awayMoneyline")),
        )
        for market, designation, home_value, away_value in definitions:
            if home_value is None:
                continue
            if market in ("spread", "total"):
                away_value = -float(home_value) if market == "spread" else float(home_value)
            fingerprint = {
                "event": game.get("id"), "provider": provider, "market": market,
                "designation": designation, "home": home_value, "away": away_value,
            }
            rows.append({
                **common, "provider": provider, "market_type": market,
                "home_value": float(home_value),
                "away_value": float(away_value) if away_value is not None else None,
                "home_price": None, "away_price": None,
                "line_designation": designation,
                "raw_payload_hash": payload_hash(fingerprint),
                "is_canonical_reference": False,
            })
    return rows


def choose_canonical_provider(rows: list[dict]) -> str | None:
    providers = {str(row["provider"]).lower() for row in rows}
    for preferred in PROVIDER_PRIORITY:
        if preferred in providers:
            return preferred
    return sorted(providers)[0] if providers else None


def ingest_season(db: DatabaseManager, games: list[dict], line_games: list[dict]) -> dict:
    captured_at = datetime.now(timezone.utc).replace(microsecond=0)
    matchup_ids: dict[int, int] = {}
    for game in games:
        if game.get("id") is not None:
            matchup_ids[int(game["id"])] = _upsert_game(db, game)
    inserted = 0
    skipped = 0
    canonicalized = 0
    for line_game in line_games:
        source_id = int(line_game["id"])
        matchup_id = matchup_ids.get(source_id)
        if not matchup_id:
            skipped += 1
            continue
        rows = _line_rows(matchup_id, line_game, captured_at)
        for row in rows:
            inserted += int(bool(insert_cfb_historical_line(db, row)))
        grouped: dict[tuple[str, str], list[dict]] = defaultdict(list)
        for row in rows:
            grouped[(row["market_type"], row["line_designation"])].append(row)
        for (market, designation), candidates in grouped.items():
            provider = choose_canonical_provider(candidates)
            if provider:
                set_cfb_canonical_historical_provider(
                    db, game_id=matchup_id, market_type=market,
                    line_designation=designation, provider=provider,
                )
                canonicalized += 1
    return {
        "games_upserted": len(matchup_ids), "line_rows_processed": inserted,
        "unmapped_line_games": skipped, "canonical_groups": canonicalized,
    }


def _write_audit(artifact_dir: Path, report: dict) -> None:
    artifact_dir.mkdir(parents=True, exist_ok=True)
    season = report["season"]
    (artifact_dir / f"cfb-history-audit-{season}.json").write_text(
        json.dumps(report, indent=2, sort_keys=True), encoding="utf-8",
    )
    lines = [
        f"# CFB History Audit — {season}", "",
        f"- Games: {report['games']}",
        f"- Completed: {report['completed_games']} ({report['result_completeness']:.1%})",
        f"- FBS vs FBS: {report['fbs_vs_fbs_games']}",
        f"- Games with betting rows: {report['line_games']} ({report['line_game_coverage']:.1%} of completed)",
        f"- Duplicate game IDs: {report['duplicate_game_ids']}",
        f"- Spread price rows: {report['spread_price_rows']}", "",
        "## Providers", "",
        *[f"- {provider}: {count}" for provider, count in report["provider_rows"].items()],
        "", "The source exposes reported opens and historical references; this audit does not classify the latter as verified closes.",
    ]
    (artifact_dir / f"cfb-history-audit-{season}.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--season", type=int)
    parser.add_argument("--start-season", type=int, default=2016)
    parser.add_argument("--end-season", type=int, default=2025)
    parser.add_argument("--audit-only", action="store_true")
    parser.add_argument("--no-cache", action="store_true")
    parser.add_argument("--cache-dir", type=Path, default=DATA_DIR / "cfb" / "history-cache")
    parser.add_argument("--artifact-dir", type=Path, default=Path("artifacts/cfb/history-audits"))
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    seasons = [args.season] if args.season else list(range(args.start_season, args.end_season + 1))
    api_key = os.getenv("CFBD_API_KEY", "")
    db = None if args.audit_only else DatabaseManager(load_config().database_url or "")
    for season in seasons:
        games = fetch_cfbd("games", api_key=api_key, season=season, cache_dir=args.cache_dir, use_cache=not args.no_cache)
        lines = fetch_cfbd("lines", api_key=api_key, season=season, cache_dir=args.cache_dir, use_cache=not args.no_cache)
        report = audit_season(games, lines, season)
        _write_audit(args.artifact_dir, report)
        print(json.dumps(report, indent=2, sort_keys=True))
        if db is not None:
            print(json.dumps(ingest_season(db, games, lines), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
