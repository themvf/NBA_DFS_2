"""Fetch today's MLB schedule and Vegas odds into mlb_matchups.

Two data sources combined:
  1. MLB Stats API (statsapi.mlb.com) — game IDs, home/away teams,
     ballpark, probable starting pitchers
  2. The Odds API (optional) — Vegas totals + moneylines + team-specific
     implied run totals, matched by full team name

No authentication or rate limiting on the MLB Stats API.

Usage:
    python -m ingest.mlb_schedule                    # today's games
    python -m ingest.mlb_schedule --date 2025-04-01  # specific date
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
from datetime import date, datetime, timezone

import requests

from config import load_config
from db.database import DatabaseManager
from db.queries import (
    build_mlb_team_abbrev_cache,
    insert_game_odds_history_rows,
    insert_mlb_schedule_revision,
    insert_mlb_starter_workload_snapshot,
    insert_mlb_weather_forecast_snapshot,
    upsert_mlb_matchup,
)
from ingest.mlb_odds_policy import (
    MlbOddsPolicyError,
    consensus_american,
    require_pregame_capture,
    resolve_mlb_odds_event,
    validate_event_prices,
)
from ingest.mlb_teams import MLB_ID_TO_ABBREV
from model.dfs_projections import compute_team_implied_total
from model.soccer_bet_rating import american_to_prob, prob_to_american

logger = logging.getLogger(__name__)

MLB_API_BASE = "https://statsapi.mlb.com/api/v1"
NOMINATIM_SEARCH_URL = "https://nominatim.openstreetmap.org/search"
OPEN_METEO_FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
OPEN_METEO_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
NON_PLAYED_STATES = {"Cancelled", "Postponed"}
STARTER_CONFIRMATION_WINDOW_HOURS = 6
NWS_POINTS_URL = "https://api.weather.gov/points/{latitude},{longitude}"
_RETRACTABLE_ROOFS = {
    "american family field", "chase field", "daikin park", "globe life field",
    "loandepot park", "rogers centre", "t-mobile park",
}
_FIXED_ROOFS = {"tropicana field"}


def _roof_capability(venue_name: str | None) -> str:
    key = (venue_name or "").strip().lower()
    if key in _RETRACTABLE_ROOFS:
        return "retractable"
    if key in _FIXED_ROOFS:
        return "fixed"
    return "open_air" if key else "unknown"


def _confirmed_starters_from_live_feed(payload: dict) -> dict[str, dict] | None:
    """Return official starters only after MLB posts participating lineups.

    The schedule's probablePitcher field is not confirmation. A side becomes
    confirmed only when the live boxscore has a batting order and at least one
    participating pitcher; the first pitcher is the starter by boxscore order.
    """
    game_data = payload.get("gameData") or {}
    players = game_data.get("players") or {}
    boxscore_teams = ((payload.get("liveData") or {}).get("boxscore") or {}).get("teams") or {}
    resolved: dict[str, dict] = {}
    for side in ("home", "away"):
        team = boxscore_teams.get(side) or {}
        batting_order = team.get("battingOrder") or []
        pitchers = team.get("pitchers") or []
        if not batting_order or not pitchers:
            return None
        pitcher_id = int(pitchers[0])
        person = players.get(f"ID{pitcher_id}") or {}
        resolved[side] = {
            "id": pitcher_id,
            "name": person.get("fullName"),
            "hand": (person.get("pitchHand") or {}).get("code"),
            "status": "confirmed",
        }
    return resolved


def _fetch_confirmed_starters(game_id: str, game_start_iso: str, *, now: datetime) -> dict[str, dict] | None:
    try:
        game_start = datetime.fromisoformat(game_start_iso.replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if game_start.tzinfo is None:
        game_start = game_start.replace(tzinfo=timezone.utc)
    hours_to_start = (game_start - now).total_seconds() / 3600.0
    if hours_to_start <= 0 or hours_to_start > STARTER_CONFIRMATION_WINDOW_HOURS:
        return None
    try:
        response = requests.get(
            f"https://statsapi.mlb.com/api/v1.1/game/{game_id}/feed/live",
            timeout=20,
        )
        response.raise_for_status()
        return _confirmed_starters_from_live_feed(response.json() or {})
    except requests.RequestException as exc:
        logger.debug("MLB confirmed-starter feed failed for %s: %s", game_id, exc)
        return None


def _parse_innings(value: object) -> float | None:
    try:
        raw = str(value)
        whole_text, _, outs_text = raw.partition(".")
        whole = int(whole_text)
        outs = int(outs_text or "0")
        if outs not in (0, 1, 2):
            return None
        return whole + outs / 3.0
    except (TypeError, ValueError):
        return None


def _starter_workload_from_game_logs(
    rows: list[dict], *, event_start: datetime, season_ip_per_start: float | None,
) -> dict | None:
    starts = []
    for row in rows:
        stat = row.get("stat") or {}
        if int(stat.get("gamesStarted") or 0) < 1:
            continue
        game_date = row.get("date")
        try:
            played = datetime.fromisoformat(str(game_date)).date()
        except ValueError:
            continue
        if played >= event_start.date():
            continue
        starts.append({
            "date": played,
            "pitches": int(stat["numberOfPitches"]) if stat.get("numberOfPitches") is not None else None,
            "innings": _parse_innings(stat.get("inningsPitched")),
            "raw": row,
        })
    starts.sort(key=lambda item: item["date"], reverse=True)
    recent = starts[:3]
    if not recent and season_ip_per_start is None:
        return None
    innings = [item["innings"] for item in recent if item["innings"] is not None]
    pitches = [item["pitches"] for item in recent if item["pitches"] is not None]
    recent_ip = sum(innings) / len(innings) if innings else None
    if recent_ip is not None and season_ip_per_start is not None:
        expected = 0.6 * recent_ip + 0.4 * season_ip_per_start
    else:
        expected = recent_ip if recent_ip is not None else season_ip_per_start
    expected = min(7.0, max(3.0, expected)) if expected is not None else None
    last = recent[0] if recent else None
    return {
        "last_start_date": last["date"].isoformat() if last else None,
        "days_rest": (event_start.date() - last["date"]).days - 1 if last else None,
        "starts_sample": len(recent),
        "pitches_last_start": last["pitches"] if last else None,
        "avg_pitches_last_3": sum(pitches) / len(pitches) if pitches else None,
        "avg_innings_last_3": recent_ip,
        "season_ip_per_start": season_ip_per_start,
        "expected_innings": expected,
        "raw_starts": [item["raw"] for item in recent],
    }


def _capture_starter_workload(
    db: DatabaseManager, *, matchup_id: int, side: str, pitcher_id: int,
    pitcher_name: str | None, event_commence: str, available_at: datetime,
) -> int:
    event_start = datetime.fromisoformat(event_commence.replace("Z", "+00:00"))
    if event_start.tzinfo is None:
        event_start = event_start.replace(tzinfo=timezone.utc)
    if available_at >= event_start:
        return 0
    season_row = db.execute_one(
        """
        SELECT ip_per_start
        FROM mlb_pitcher_stats_history
        WHERE available_at <= %s AND (player_id = %s OR LOWER(name) = LOWER(%s))
        ORDER BY (player_id = %s) DESC, available_at DESC, id DESC
        LIMIT 1
        """,
        (available_at, pitcher_id, pitcher_name, pitcher_id),
    ) or {}
    season_ip_per_start = season_row.get("ip_per_start")
    try:
        response = requests.get(
            f"{MLB_API_BASE}/people/{pitcher_id}/stats",
            params={"stats": "gameLog", "group": "pitching", "season": event_start.year, "gameType": "R"},
            timeout=20,
        )
        response.raise_for_status()
        groups = (response.json() or {}).get("stats") or []
        rows = groups[0].get("splits") or [] if groups else []
    except requests.RequestException as exc:
        logger.debug("MLB starter workload failed for %s: %s", pitcher_id, exc)
        return 0
    workload = _starter_workload_from_game_logs(
        rows, event_start=event_start,
        season_ip_per_start=float(season_ip_per_start) if season_ip_per_start is not None else None,
    )
    if workload is None:
        return 0
    checksum_payload = {"pitcher_id": pitcher_id, "event": event_commence, **workload}
    raw_checksum = hashlib.sha256(
        json.dumps(checksum_payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    ).hexdigest()
    stats_through_at = (
        f"{workload['last_start_date']}T23:59:59Z"
        if workload["last_start_date"] else available_at
    )
    return insert_mlb_starter_workload_snapshot(
        db, matchup_id=matchup_id, side=side, pitcher_id=pitcher_id,
        pitcher_name=pitcher_name, event_commence=event_commence,
        last_start_date=workload["last_start_date"], days_rest=workload["days_rest"],
        starts_sample=workload["starts_sample"], pitches_last_start=workload["pitches_last_start"],
        avg_pitches_last_3=workload["avg_pitches_last_3"],
        avg_innings_last_3=workload["avg_innings_last_3"],
        season_ip_per_start=workload["season_ip_per_start"],
        expected_innings=workload["expected_innings"], stats_through_at=stats_through_at,
        available_at=available_at, raw_checksum=raw_checksum,
        raw_json={"recent_starts": workload["raw_starts"]},
    )


def _build_mlb_team_context_cache(db: DatabaseManager) -> dict[int, dict[str, str | None]]:
    rows = db.execute("SELECT team_id, city, ballpark FROM mlb_teams")
    return {
        int(row["team_id"]): {
            "city": row.get("city"),
            "ballpark": row.get("ballpark"),
        }
        for row in rows
    }


def _to_compass(degrees: float | None) -> str | None:
    if degrees is None:
        return None
    directions = ["N", "NE", "E", "SE", "S", "SW", "W", "NW"]
    idx = round((degrees % 360) / 45) % len(directions)
    return directions[idx]


def _nearest_hour_index(times: list[str], target_iso: str) -> int | None:
    if not times:
        return None
    try:
        target = datetime.fromisoformat(target_iso.replace("Z", "+00:00"))
    except ValueError:
        return None

    best_idx: int | None = None
    best_delta: float | None = None
    for idx, ts in enumerate(times):
        try:
            point = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            if point.tzinfo is None:
                point = point.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
        delta = abs((point - target).total_seconds())
        if best_delta is None or delta < best_delta:
            best_delta = delta
            best_idx = idx
    return best_idx


def _geocode_ballpark(
    query: str,
    *,
    timeout_seconds: int,
    cache: dict[str, tuple[float, float] | None],
) -> tuple[float, float] | None:
    if query in cache:
        return cache[query]
    try:
        resp = requests.get(
            NOMINATIM_SEARCH_URL,
            params={"q": query, "format": "jsonv2", "limit": 1},
            headers={"User-Agent": "NBADFS_v2/1.0"},
            timeout=timeout_seconds,
        )
        resp.raise_for_status()
        results = resp.json() or []
        if results:
            match = results[0]
            coords = (float(match["lat"]), float(match["lon"]))
            cache[query] = coords
            return coords
    except requests.RequestException as exc:
        logger.debug("Nominatim geocode failed for %s: %s", query, exc)
    cache[query] = None
    return None


def _fetch_weather_snapshot(
    *,
    latitude: float,
    longitude: float,
    game_start_iso: str,
    timeout_seconds: int,
) -> tuple[int | None, int | None, str | None]:
    try:
        game_start = datetime.fromisoformat(game_start_iso.replace("Z", "+00:00"))
    except ValueError:
        return (None, None, None)

    endpoint = OPEN_METEO_ARCHIVE_URL if game_start.date() < datetime.now(timezone.utc).date() else OPEN_METEO_FORECAST_URL
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "hourly": "temperature_2m,wind_speed_10m,wind_direction_10m",
        "temperature_unit": "fahrenheit",
        "wind_speed_unit": "mph",
        "start_date": game_start.date().isoformat(),
        "end_date": game_start.date().isoformat(),
        "timezone": "UTC",
    }
    try:
        resp = requests.get(endpoint, params=params, timeout=timeout_seconds)
        resp.raise_for_status()
        payload = resp.json() or {}
    except requests.RequestException as exc:
        logger.debug("Open-Meteo weather fetch failed for %.4f, %.4f: %s", latitude, longitude, exc)
        return (None, None, None)

    hourly = payload.get("hourly") or {}
    times = hourly.get("time") or []
    idx = _nearest_hour_index(times, game_start_iso)
    if idx is None:
        return (None, None, None)

    temps = hourly.get("temperature_2m") or []
    winds = hourly.get("wind_speed_10m") or []
    wind_dirs = hourly.get("wind_direction_10m") or []

    temp = round(float(temps[idx])) if idx < len(temps) and temps[idx] is not None else None
    wind_speed = round(float(winds[idx])) if idx < len(winds) and winds[idx] is not None else None
    wind_direction = _to_compass(float(wind_dirs[idx])) if idx < len(wind_dirs) and wind_dirs[idx] is not None else None
    return (temp, wind_speed, wind_direction)


def _wind_mph(value: object) -> float | None:
    numbers = [float(item) for item in re.findall(r"\d+(?:\.\d+)?", str(value or ""))]
    return sum(numbers) / len(numbers) if numbers else None


def _fetch_nws_forecast(
    *, latitude: float, longitude: float, game_start_iso: str, timeout_seconds: int,
) -> dict | None:
    headers = {
        "User-Agent": "DFSVegas/1.0 (MLB decision weather provenance)",
        "Accept": "application/geo+json",
    }
    try:
        point = requests.get(
            NWS_POINTS_URL.format(latitude=round(latitude, 4), longitude=round(longitude, 4)),
            headers=headers, timeout=timeout_seconds,
        )
        point.raise_for_status()
        point_payload = point.json() or {}
        point_props = point_payload.get("properties") or {}
        hourly_url = point_props.get("forecastHourly")
        if not hourly_url:
            return None
        forecast = requests.get(hourly_url, headers=headers, timeout=timeout_seconds)
        forecast.raise_for_status()
        payload = forecast.json() or {}
        props = payload.get("properties") or {}
        periods = props.get("periods") or []
        target = datetime.fromisoformat(game_start_iso.replace("Z", "+00:00"))
        candidates = []
        for period in periods:
            try:
                valid = datetime.fromisoformat(str(period.get("startTime")))
            except (TypeError, ValueError):
                continue
            candidates.append((abs((valid - target).total_seconds()), valid, period))
        if not candidates:
            return None
        _, valid_at, period = min(candidates, key=lambda item: item[0])
        humidity = (period.get("relativeHumidity") or {}).get("value")
        precip = (period.get("probabilityOfPrecipitation") or {}).get("value")
        return {
            "provider": "weather_gov_nws",
            "provider_model": "/".join(str(point_props.get(key) or "") for key in ("gridId", "gridX", "gridY")),
            "provider_issued_at": props.get("generatedAt") or props.get("updateTime"),
            "valid_at": valid_at.isoformat(),
            "temperature_f": float(period["temperature"]) if period.get("temperature") is not None else None,
            "relative_humidity_pct": float(humidity) if humidity is not None else None,
            "precipitation_probability_pct": float(precip) if precip is not None else None,
            "wind_speed_mph": _wind_mph(period.get("windSpeed")),
            "wind_direction": period.get("windDirection"),
            "source_status": "complete",
            "raw_json": {"point": point_props, "forecast_metadata": {
                "generatedAt": props.get("generatedAt"), "updateTime": props.get("updateTime")
            }, "period": period},
        }
    except (requests.RequestException, ValueError) as exc:
        logger.debug("NWS forecast failed for %.4f, %.4f: %s", latitude, longitude, exc)
        return None


def fetch_schedule(db: DatabaseManager, game_date: str | None = None) -> list[int]:
    """Fetch games for game_date (YYYY-MM-DD), upsert into mlb_matchups.

    Includes probable starting pitchers when posted by MLB. home_sp_id /
    away_sp_id store MLB Stats API player ids, and home_sp_name / away_sp_name
    store the probable starter names directly for later analytics joins.

    Returns list of mlb_matchup IDs upserted.
    """
    target_date = game_date or date.today().isoformat()
    logger.info("Fetching MLB schedule for %s ...", target_date)

    try:
        resp = requests.get(
            f"{MLB_API_BASE}/schedule",
            params={
                "sportId": 1,
                "date": target_date,
                "hydrate": "probablePitcher",
            },
            timeout=20,
        )
        resp.raise_for_status()
        data = resp.json()
    except requests.RequestException as e:
        logger.warning("MLB Stats API request failed: %s", e)
        return []

    dates = data.get("dates", [])
    if not dates:
        print(f"No games found for {target_date}")
        return []

    abbrev_cache = build_mlb_team_abbrev_cache(db)
    team_context_cache = _build_mlb_team_context_cache(db)
    geocode_cache: dict[str, tuple[float, float] | None] = {}
    weather_timeout = load_config().mlb_api.timeout_seconds
    schedule_available_at = datetime.now(timezone.utc)

    matchup_ids: list[int] = []
    skipped_non_played = 0
    skipped_unknown_team = 0
    for game in dates[0].get("games", []):
        detailed_state = game.get("status", {}).get("detailedState", "")
        if detailed_state in NON_PLAYED_STATES:
            skipped_non_played += 1
            continue

        game_id    = str(game.get("gamePk", ""))
        game_start = game.get("gameDate")
        home_info  = game.get("teams", {}).get("home", {})
        away_info  = game.get("teams", {}).get("away", {})

        home_mlb_id = home_info.get("team", {}).get("id")
        away_mlb_id = away_info.get("team", {}).get("id")

        home_abbrev = MLB_ID_TO_ABBREV.get(home_mlb_id)
        away_abbrev = MLB_ID_TO_ABBREV.get(away_mlb_id)

        home_team_id = abbrev_cache.get(home_abbrev) if home_abbrev else None
        away_team_id = abbrev_cache.get(away_abbrev) if away_abbrev else None

        if not home_team_id or not away_team_id:
            logger.warning(
                "Unknown team IDs for game %s: home_mlb_id=%s (%s) away_mlb_id=%s (%s)",
                game_id, home_mlb_id, home_abbrev, away_mlb_id, away_abbrev,
            )
            skipped_unknown_team += 1
            continue

        # Probable starters — store MLB player_id; NULL if not yet announced
        home_sp_id = home_info.get("probablePitcher", {}).get("id")
        home_sp_name = home_info.get("probablePitcher", {}).get("fullName")
        away_sp_id = away_info.get("probablePitcher", {}).get("id")
        away_sp_name = away_info.get("probablePitcher", {}).get("fullName")
        home_sp_status = "probable" if home_sp_id else "unavailable"
        away_sp_status = "probable" if away_sp_id else "unavailable"
        confirmed = _fetch_confirmed_starters(
            game_id, game_start, now=schedule_available_at,
        ) if game_id and game_start else None
        if confirmed:
            home_sp_id = confirmed["home"]["id"]
            home_sp_name = confirmed["home"]["name"] or home_sp_name
            home_sp_status = "confirmed"
            away_sp_id = confirmed["away"]["id"]
            away_sp_name = confirmed["away"]["name"] or away_sp_name
            away_sp_status = "confirmed"
        ballpark   = game.get("venue", {}).get("name")
        venue_id = game.get("venue", {}).get("id")
        team_context = team_context_cache.get(home_team_id, {})
        query_ballpark = ballpark or team_context.get("ballpark")
        query_city = team_context.get("city")
        weather_temp: int | None = None
        wind_speed: int | None = None
        wind_direction: str | None = None
        coords: tuple[float, float] | None = None
        weather_forecast: dict | None = None

        if game_start and query_ballpark:
            geocode_queries = []
            if query_city:
                geocode_queries.append(f"{query_ballpark}, {query_city}")
                geocode_queries.append(query_city)
            geocode_queries.append(query_ballpark)

            for geocode_query in geocode_queries:
                coords = _geocode_ballpark(
                    geocode_query,
                    timeout_seconds=weather_timeout,
                    cache=geocode_cache,
                )
                if coords is not None:
                    break
            if coords is not None:
                weather_forecast = _fetch_nws_forecast(
                    latitude=coords[0], longitude=coords[1], game_start_iso=game_start,
                    timeout_seconds=weather_timeout,
                )
                if weather_forecast is not None:
                    weather_temp = round(weather_forecast["temperature_f"]) if weather_forecast["temperature_f"] is not None else None
                    wind_speed = round(weather_forecast["wind_speed_mph"]) if weather_forecast["wind_speed_mph"] is not None else None
                    wind_direction = weather_forecast["wind_direction"]
                else:
                    weather_temp, wind_speed, wind_direction = _fetch_weather_snapshot(
                        latitude=coords[0], longitude=coords[1], game_start_iso=game_start,
                        timeout_seconds=weather_timeout,
                    )
                    if weather_temp is not None or wind_speed is not None:
                        weather_forecast = {
                            "provider": "open_meteo",
                            "provider_model": "best_match",
                            "provider_issued_at": None,
                            "valid_at": game_start,
                            "temperature_f": weather_temp,
                            "relative_humidity_pct": None,
                            "precipitation_probability_pct": None,
                            "wind_speed_mph": wind_speed,
                            "wind_direction": wind_direction,
                            "source_status": "provider_issue_time_unavailable",
                            "raw_json": {"fallback": "Open-Meteo compatibility cache"},
                        }

        mid = upsert_mlb_matchup(
            db,
            game_date=target_date,
            game_id=game_id,
            home_team_id=home_team_id,
            away_team_id=away_team_id,
            home_sp_id=home_sp_id,
            home_sp_name=home_sp_name,
            away_sp_id=away_sp_id,
            away_sp_name=away_sp_name,
            ballpark=ballpark,
            weather_temp=weather_temp,
            wind_speed=wind_speed,
            wind_direction=wind_direction,
            commence_time=game_start,
        )
        if mid:
            matchup_ids.append(mid)
            revision_payload = {
                "game_id": game_id,
                "game_date": target_date,
                "commence_time": game_start,
                "home_team_id": home_team_id,
                "away_team_id": away_team_id,
                "venue_id": venue_id,
                "venue_name": ballpark,
                "home_sp_id": home_sp_id,
                "home_sp_name": home_sp_name,
                "home_sp_status": home_sp_status,
                "away_sp_id": away_sp_id,
                "away_sp_name": away_sp_name,
                "away_sp_status": away_sp_status,
                "game_status": detailed_state,
            }
            revision_hash = hashlib.sha256(
                json.dumps(revision_payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
            ).hexdigest()
            insert_mlb_schedule_revision(
                db,
                matchup_id=mid,
                game_id=game_id,
                revision_hash=revision_hash,
                game_date=target_date,
                commence_time=game_start,
                home_team_id=home_team_id,
                away_team_id=away_team_id,
                venue_id=venue_id,
                venue_name=ballpark,
                home_sp_id=home_sp_id,
                home_sp_name=home_sp_name,
                home_sp_status=home_sp_status,
                away_sp_id=away_sp_id,
                away_sp_name=away_sp_name,
                away_sp_status=away_sp_status,
                game_status=detailed_state,
                source_available_at=schedule_available_at,
                raw_json=game,
            )
            if weather_forecast is not None and coords is not None and game_start:
                roof_capability = _roof_capability(ballpark)
                roof_state = (
                    "closed" if roof_capability == "fixed"
                    else "not_applicable" if roof_capability == "open_air"
                    else "unknown"
                )
                roof_source = "static_venue_capability" if roof_capability != "unknown" else "unavailable"
                forecast_payload = {
                    "matchup_id": mid, "event_commence": game_start,
                    "venue": ballpark, "coordinates": coords,
                    **weather_forecast, "roof_capability": roof_capability,
                    "roof_state": roof_state, "roof_source": roof_source,
                }
                forecast_hash = hashlib.sha256(
                    json.dumps(forecast_payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
                ).hexdigest()
                insert_mlb_weather_forecast_snapshot(
                    db, matchup_id=mid, event_commence=game_start, venue_name=ballpark,
                    latitude=coords[0], longitude=coords[1],
                    provider=weather_forecast["provider"],
                    provider_model=weather_forecast["provider_model"],
                    provider_issued_at=weather_forecast["provider_issued_at"],
                    valid_at=weather_forecast["valid_at"], available_at=schedule_available_at,
                    temperature_f=weather_forecast["temperature_f"],
                    relative_humidity_pct=weather_forecast["relative_humidity_pct"],
                    precipitation_probability_pct=weather_forecast["precipitation_probability_pct"],
                    wind_speed_mph=weather_forecast["wind_speed_mph"],
                    wind_direction=weather_forecast["wind_direction"],
                    roof_capability=roof_capability, roof_state=roof_state,
                    roof_source=roof_source, source_status=weather_forecast["source_status"],
                    raw_checksum=forecast_hash, raw_json=weather_forecast["raw_json"],
                )
            if home_sp_id and game_start:
                _capture_starter_workload(
                    db, matchup_id=mid, side="home", pitcher_id=int(home_sp_id),
                    pitcher_name=home_sp_name, event_commence=game_start,
                    available_at=schedule_available_at,
                )
            if away_sp_id and game_start:
                _capture_starter_workload(
                    db, matchup_id=mid, side="away", pitcher_id=int(away_sp_id),
                    pitcher_name=away_sp_name, event_commence=game_start,
                    available_at=schedule_available_at,
                )

    msg = f"Schedule: {len(matchup_ids)} games upserted for {target_date}"
    skipped_parts = []
    if skipped_non_played:
        skipped_parts.append(f"{skipped_non_played} non-played")
    if skipped_unknown_team:
        skipped_parts.append(f"{skipped_unknown_team} unknown team IDs")
    if skipped_parts:
        msg += f" ({', '.join(skipped_parts)} skipped)"
    print(msg)
    return matchup_ids


def _consensus_american(prices: list[int]) -> int | None:
    """Consensus American odds by averaging in IMPLIED-PROBABILITY space.

    Arithmetic averaging of American odds is invalid: mixed-sign prices around
    even money (+102, −112, …) average into the impossible (−100, +100) zone —
    the ledger held prices like −74 and −42 — and converting that fiction to
    decimal inflated every moneyline EV, star rating, and backtest payout
    (discovered 2026-07-02; soccer fixed the same bug in June). Average the
    implied probabilities and convert back.
    """
    if not prices:
        return None
    avg_prob = sum(american_to_prob(p) for p in prices) / len(prices)
    return prob_to_american(avg_prob)


def fetch_odds(db: DatabaseManager, api_key: str, game_date: str | None = None) -> int:
    """Fetch Vegas totals + moneylines from The Odds API and update mlb_matchups.

    Computes consensus averages across ALL bookmakers (not just [0]) for
    stability.  Also derives team-specific implied run totals from moneylines
    using the same compute_team_implied_total() formula as NBA.

    Matches games by home team name against mlb_teams.name.
    Returns number of matchups updated.
    """
    if not api_key:
        logger.info("ODDS_API_KEY not set — skipping MLB odds fetch")
        return 0

    target_date = game_date or date.today().isoformat()

    try:
        resp = requests.get(
            "https://api.the-odds-api.com/v4/sports/baseball_mlb/odds/",
            params={
                "apiKey": api_key,
                # us + eu: eu brings Pinnacle — the sharp reference book that
                # per-book movement analysis (Edge-Finding P1/P2) anchors on.
                # Doubles this call's Odds API credit cost (markets x regions);
                # revert to "us" if quota becomes a problem.
                "regions": "us,eu",
                "markets": "h2h,totals,spreads",
                "oddsFormat": "american",
                "dateFormat": "iso",
            },
            timeout=20,
        )
        resp.raise_for_status()
        games = resp.json()
    except requests.RequestException as e:
        logger.warning("Odds API request failed: %s", e)
        return 0

    # Build lookup: home team name → ALL matchup rows for that home team today.
    # A split doubleheader is two rows with the same (date, teams) and distinct
    # gamePks (game_id-first identity, 2026-07-07) — each Odds API event is
    # resolved to the row whose commence_time is nearest the event's.
    rows = db.execute(
        """
        SELECT nm.id, nm.commence_time, t_home.name AS home_name, t_away.name AS away_name
        FROM mlb_matchups nm
        JOIN mlb_teams t_home ON t_home.team_id = nm.home_team_id
        JOIN mlb_teams t_away ON t_away.team_id = nm.away_team_id
        WHERE nm.game_date = %s
        """,
        (target_date,),
    )
    known_event_rows = db.execute(
        """
        SELECT event_id, MIN(matchup_id)::int AS matchup_id
        FROM game_odds_history
        WHERE sport = 'mlb' AND game_date = %s AND event_id IS NOT NULL
        GROUP BY event_id
        HAVING COUNT(DISTINCT matchup_id) = 1
        """,
        (target_date,),
    )
    known_event_matchups = {
        str(row["event_id"]): int(row["matchup_id"])
        for row in known_event_rows
    }
    # Exact team/time matching is centralized in mlb_odds_policy.
    # Ensure h2h + totals + spreads (run line) are all fetched
    markets_to_fetch = "h2h,totals,spreads"
    captured_at = datetime.now(timezone.utc).replace(microsecond=0)
    capture_key = captured_at.isoformat()

    # The Athletics changed name — try both variants
    _OAK_ALIASES = {"Oakland Athletics", "Athletics", "Sacramento Athletics"}

    updated = 0
    skipped_live = 0
    now = datetime.now(timezone.utc)
    history_rows: list[dict] = []
    for g in games:
        # In-play guard: after first pitch the odds feed serves LIVE prices.
        # Writing them replaces the pre-game closing line that predictions,
        # the bet ledger reference, and CLV history all assume (the 30-min
        # odds-capture cron polls straight through the game window). Same
        # freeze-at-start rule as soccer (2026-07-01).
        commence_iso = g.get("commence_time")
        if commence_iso:
            try:
                if datetime.fromisoformat(commence_iso.replace("Z", "+00:00")) <= now:
                    skipped_live += 1
                    continue
            except ValueError:
                pass

        home_name = g.get("home_team", "")
        try:
            validate_event_prices(g)
            matchup = resolve_mlb_odds_event(
                g,
                rows,
                known_event_matchup_id=known_event_matchups.get(str(g.get("id") or "")),
            )
            require_pregame_capture(
                event_commence=commence_iso,
                matchup_commence=matchup.get("commence_time"),
                captured_at=captured_at,
            )
        except MlbOddsPolicyError as exc:
            logger.warning("Odds event %s rejected: %s", g.get("id"), exc)
            continue

        # Belt + suspenders on the in-play guard: the event-commence check
        # above trusts the FEED's commence_time, which the Odds API moves on
        # rain delays — a delayed in-progress game can reappear with a future
        # commence and sail past it, freezing in-play prices (e.g. -10000)
        # into mlb_matchups as "closing" lines (2026-07-08 incident). OUR
        # commence_time comes from the MLB statsapi schedule; if the game has
        # started by OUR clock, never overwrite its odds.
        if matchup.get("commence_time") is not None and matchup["commence_time"] <= now:
            skipped_live += 1
            continue

        # Consensus across ALL bookmakers for h2h and totals, plus the full
        # per-book detail — consensus averages away exactly the structure
        # sharp-movement detection needs (which book moved first, line vs
        # price). Stored as JSONB on the history row; zero extra API cost.
        away_name = g.get("away_team", "")
        home_prices: list[int] = []
        away_prices: list[int] = []
        total_points: list[float] = []
        home_spreads: list[float] = []
        books: dict[str, dict] = {}
        bookmakers = g.get("bookmakers") or []
        for bm in bookmakers:
            book = books.setdefault(bm.get("key", "?"), {"last_update": bm.get("last_update")})
            for market in bm.get("markets", []):
                if market["key"] == "h2h":
                    for o in market.get("outcomes", []):
                        if o["name"] == home_name or o["name"] in _OAK_ALIASES and home_name in _OAK_ALIASES:
                            home_prices.append(o["price"])
                            book["ml_home"] = o["price"]
                        elif o["name"] == away_name:
                            away_prices.append(o["price"])
                            book["ml_away"] = o["price"]
                elif market["key"] == "totals":
                    over = next(
                        (o for o in market.get("outcomes", []) if o["name"] == "Over"),
                        None,
                    )
                    under = next(
                        (o for o in market.get("outcomes", []) if o["name"] == "Under"),
                        None,
                    )
                    if over and over.get("point") is not None:
                        total_points.append(float(over["point"]))
                        book["total_line"] = float(over["point"])
                        book["over"] = over.get("price")
                        if under:
                            book["under"] = under.get("price")
                elif market["key"] == "spreads":
                    home_outcome = next(
                        (o for o in market.get("outcomes", []) if o["name"] == home_name),
                        None,
                    )
                    if home_outcome and home_outcome.get("point") is not None:
                        home_spreads.append(float(home_outcome["point"]))
                        book["spread_home"] = float(home_outcome["point"])
                        book["spread_price"] = home_outcome.get("price")

        home_ml    = consensus_american(home_prices)
        away_ml    = consensus_american(away_prices)
        vegas_total_raw = sum(total_points) / len(total_points) if total_points else None
        vegas_total = round(vegas_total_raw * 2) / 2 if vegas_total_raw is not None else None
        home_spread = round(sum(home_spreads) / len(home_spreads) * 2) / 2 if home_spreads else None
        vegas_prob_home = _ml_to_prob(home_ml, away_ml) if home_ml and away_ml else None

        # Team-specific implied run totals from moneylines
        # MLB avg is ~9 runs/game total, ~4.5 per team.
        # A -200 home favorite in a 9.5 O/U gets ~5.3 implied, not 4.75.
        home_implied = away_implied = None
        if vegas_total and home_ml and away_ml:
            home_implied = round(
                compute_team_implied_total(vegas_total, home_ml, away_ml, is_home=True), 3
            )
            away_implied = round(vegas_total - home_implied, 3)

        db.execute(
            """
            UPDATE mlb_matchups
            SET vegas_total     = %s,
                home_ml         = %s,
                away_ml         = %s,
                home_spread     = %s,
                vegas_prob_home = %s,
                home_implied    = %s,
                away_implied    = %s
            WHERE id = %s
            """,
            (vegas_total, home_ml, away_ml, home_spread, vegas_prob_home,
             home_implied, away_implied, matchup["id"]),
        )
        history_rows.append(
            {
                "sport": "mlb",
                "matchup_id": matchup["id"],
                "event_id": g.get("id"),
                "game_date": target_date,
                "home_team_name": home_name,
                "away_team_name": away_name,
                "bookmaker_count": len(bookmakers),
                "home_ml": home_ml,
                "away_ml": away_ml,
                "home_spread": home_spread,
                "vegas_total": vegas_total,
                "vegas_prob_home": vegas_prob_home,
                "home_implied": home_implied,
                "away_implied": away_implied,
                "capture_key": capture_key,
                "captured_at": captured_at,
                "books": books or None,
                "vegas_total_raw": vegas_total_raw,
            }
        )
        updated += 1

    if history_rows:
        insert_game_odds_history_rows(db, history_rows)
    msg = f"Odds: {updated} matchups updated with Vegas lines for {target_date}"
    if skipped_live:
        msg += f" ({skipped_live} in-play games skipped — closing lines frozen)"
    print(msg)
    return updated


def fetch_scores(db: DatabaseManager, game_date: str | None = None) -> int:
    """Fetch final scores for completed MLB games and write to mlb_matchups.

    Uses the MLB Stats API schedule endpoint with linescore hydration.
    Only writes when game status is 'Final'.  Safe to call for past dates.
    Returns number of matchups updated.
    """
    target_date = game_date or date.today().isoformat()
    logger.info("Fetching MLB scores for %s ...", target_date)

    try:
        resp = requests.get(
            f"{MLB_API_BASE}/schedule",
            params={
                "sportId": 1,
                "date": target_date,
                "hydrate": "linescore",
            },
            timeout=20,
        )
        resp.raise_for_status()
        data = resp.json()
    except requests.RequestException as e:
        logger.warning("MLB Stats API scores request failed: %s", e)
        return 0

    dates = data.get("dates", [])
    if not dates:
        return 0

    # 'Game Over' = game ended, stats being finalized — the score is safe to
    # settle on ('Final' can lag it by hours, leaving bets stuck pending).
    _FINAL_STATES = {"Final", "Game Over", "Completed Early"}
    # Postponed/cancelled games are made up on a LATER date under the SAME
    # gamePk, so without stamping the status the makeup game's score would
    # eventually land on the original date's row and grade bets on a game
    # played weeks later. Books void when the game doesn't play as scheduled;
    # settle() voids pending bets on these rows. (Suspended games resume and
    # finish under the same gamePk — those stay pending until final.)
    _VOID_STATES = {"Postponed", "Cancelled"}

    updated = 0
    for game in dates[0].get("games", []):
        detailed_state = game.get("status", {}).get("detailedState", "")
        game_id = str(game.get("gamePk", ""))
        if detailed_state in _VOID_STATES:
            db.execute(
                "UPDATE mlb_matchups SET game_status = %s WHERE game_id = %s",
                (detailed_state, game_id),
            )
            continue
        if detailed_state not in _FINAL_STATES:
            continue
        # Stamp the final status too — a made-up game keeps its gamePk, and
        # with game_id-first identity its row MOVES to the makeup date, so a
        # stale 'Postponed' stamp from the original date must be cleared once
        # the game actually completes (row 2061 / gamePk 823062 lesson).
        db.execute(
            "UPDATE mlb_matchups SET game_status = %s WHERE game_id = %s "
            "AND game_status IS DISTINCT FROM %s",
            (detailed_state, game_id, detailed_state),
        )
        linescore = game.get("linescore", {})
        teams_ls = linescore.get("teams", {})
        home_runs = teams_ls.get("home", {}).get("runs")
        away_runs = teams_ls.get("away", {}).get("runs")

        if home_runs is None or away_runs is None:
            continue

        hs, as_ = int(home_runs), int(away_runs)
        prev = db.execute_one(
            "SELECT id, home_score, away_score FROM mlb_matchups WHERE game_id = %s", (game_id,)
        )
        if prev is None or (prev["home_score"], prev["away_score"]) == (hs, as_):
            continue  # unknown game or already correct
        # Write the final score, correcting any stale/wrong value that got frozen
        # earlier (the old NULL-only guard could never fix a wrong score).
        db.execute(
            "UPDATE mlb_matchups SET home_score = %s, away_score = %s WHERE id = %s",
            (hs, as_, prev["id"]),
        )
        updated += 1
        # If we corrected a previously-recorded score, reopen this game's settled
        # bets so the next settle pass re-grades against the truth.
        if prev["home_score"] is not None or prev["away_score"] is not None:
            db.execute(
                "UPDATE mlb_bets SET status = 'pending', settled_at = NULL, result_detail = NULL "
                "WHERE matchup_id = %s AND status IN ('won', 'lost', 'void')",
                (prev["id"],),
            )

    logger.info("MLB Scores: %d matchups updated for %s", updated, target_date)
    return updated


def _ml_to_prob(home_ml: int, away_ml: int) -> float:
    """Convert American moneylines to vig-removed home win probability."""
    def _raw(ml: int) -> float:
        if ml > 0:
            return 100 / (ml + 100)
        return abs(ml) / (abs(ml) + 100)

    home_raw = _raw(home_ml)
    away_raw = _raw(away_ml)
    total    = home_raw + away_raw
    return round(home_raw / total, 4) if total > 0 else 0.5


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Fetch MLB schedule + odds")
    parser.add_argument("--date", help="Game date YYYY-MM-DD (default: today)")
    args = parser.parse_args()

    config = load_config()
    db = DatabaseManager(config.database_url)

    fetch_schedule(db, args.date)
    fetch_odds(db, config.odds_api.api_key, args.date)
    fetch_scores(db, args.date)
