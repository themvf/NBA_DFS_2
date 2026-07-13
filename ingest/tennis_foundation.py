"""Populate the immutable 2023+ Tennis player and historical-match foundation.

This is the source-of-truth ingestion path for SCRUM-20/SCRUM-27.  It keeps
raw provider observations, partition evidence and player-level match stats; it
does not overwrite the legacy ``tennis_player_ratings`` compatibility cache.

ATP match/stat backbone: TML-Database yearly Sackmann CSV.
ATP odds/rank enrichment: tennis-data.co.uk yearly XLSX.
WTA match/odds/rank backbone: tennis-data.co.uk yearly XLSX.  The current WTA
source has no serve-point detail, so match-stat rows explicitly carry
``source_unavailable`` rather than fabricated zeroes.

Usage:
    python -m ingest.tennis_foundation --from-year 2023
    python -m ingest.tennis_foundation --tour ATP --season 2025
    python -m ingest.tennis_foundation --dry-run --season 2025
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import logging
import math
import re
import time
import unicodedata
import uuid
from dataclasses import dataclass
from datetime import date, datetime, time as dt_time, timezone
from pathlib import Path
from typing import Any, Iterable

import requests
from psycopg2.extras import execute_values
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config import load_config
from db.database import DatabaseManager

logger = logging.getLogger(__name__)

START_DATE = date(2023, 1, 1)
PARSER_VERSION = "tennis-foundation-v2"
ATP_PROVIDER = "tml_database"
TENNIS_DATA_PROVIDER = "tennis_data"
TML_URL = "https://raw.githubusercontent.com/Tennismylife/TML-Database/master/{year}.csv"
TENNIS_DATA_URL = {
    "ATP": "http://www.tennis-data.co.uk/{year}/{year}.xlsx",
    "WTA": "http://www.tennis-data.co.uk/{year}w/{year}.xlsx",
}
CACHE_DIR = Path("data/tennis/raw")
_SUFFIXES = {"jr", "sr", "ii", "iii", "iv"}

EXACT_QUOTE_INSERT_SQL = """
INSERT INTO tennis_exact_quotes (
    event_id, event_revision_id, source, provider_event_id,
    bookmaker_key, bookmaker_name, region, market,
    selection_type, selection_player_id, selection_side, line_value,
    price_american, price_decimal, paired_selection_type,
    paired_player_id, paired_side, paired_line_value,
    paired_price_american, paired_price_decimal,
    bookmaker_updated_at, source_available_at, captured_at,
    commence_time_at_capture, is_prestart, validation_status,
    rejection_reason, capture_key, raw_checksum, parser_version, raw_payload
) VALUES %s
ON CONFLICT DO NOTHING
RETURNING id
"""
EXACT_QUOTE_VALUE_TEMPLATE = """(
    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb
)"""


def _session() -> requests.Session:
    retry = Retry(
        total=4,
        connect=4,
        read=4,
        backoff_factor=1.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
        raise_on_status=False,
    )
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0 DFSVEGAS-tennis-foundation/1.0"})
    session.mount("http://", HTTPAdapter(max_retries=retry))
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def normalize_name(name: str) -> str:
    text = unicodedata.normalize("NFKD", str(name or ""))
    ascii_text = text.encode("ascii", "ignore").decode("ascii").lower()
    tokens = [t for t in re.findall(r"[a-z0-9]+", ascii_text) if t not in _SUFFIXES]
    return "".join(tokens)


def surname_initial_key(name: str, abbreviated: bool = False) -> str:
    """Return a stable surname+first-initial bridge across full/abbrev names."""
    tokens = [t for t in re.findall(r"[A-Za-z0-9]+", unicodedata.normalize("NFKD", str(name or "")))]
    tokens = [t for t in tokens if t.lower() not in _SUFFIXES]
    if len(tokens) < 2:
        return normalize_name(name)
    if abbreviated:
        # tennis-data names are "Surname [SurnamePart] I.".
        first_initial = normalize_name(tokens[-1])[:1]
        surname = normalize_name("".join(tokens[:-1]))
    else:
        first_initial = normalize_name(tokens[0])[:1]
        surname = normalize_name("".join(tokens[1:]))
    return surname + first_initial


def player_identity_key(tour: str, name: str, provider: str) -> str:
    if provider == TENNIS_DATA_PROVIDER:
        return surname_initial_key(name, abbreviated=True)
    if tour == "WTA":
        return surname_initial_key(name, abbreviated=False)
    return normalize_name(name)


def normalize_surface(value: Any, indoor: Any = None) -> str | None:
    surface = str(value or "").strip().lower()
    is_indoor = str(indoor or "").strip().lower() in {"1", "true", "yes", "indoor"}
    if surface == "hard":
        return "indoor_hard" if is_indoor else "hard"
    if surface in {"clay", "grass"}:
        return surface
    return None


def _number(value: Any, cast=float):
    try:
        if value is None or (isinstance(value, float) and math.isnan(value)):
            return None
        return cast(float(value))
    except (TypeError, ValueError, OverflowError):
        return None


def _clean_json(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(k): _clean_json(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_clean_json(v) for v in value]
    # numpy scalars expose item(); avoid importing numpy in the pipeline.
    if hasattr(value, "item"):
        try:
            return _clean_json(value.item())
        except Exception:  # noqa: BLE001
            pass
    return value


def stable_json(value: Any) -> str:
    return json.dumps(_clean_json(value), sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def checksum(value: Any) -> str:
    payload = value if isinstance(value, bytes) else stable_json(value).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _date_from_yyyymmdd(value: Any) -> date | None:
    raw = str(value or "").strip().split(".")[0]
    try:
        return datetime.strptime(raw, "%Y%m%d").date()
    except ValueError:
        return None


def _as_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        import pandas as pd

        parsed = pd.to_datetime(value, errors="coerce")
        return None if pd.isna(parsed) else parsed.date()
    except Exception:  # noqa: BLE001
        return None


def _market_odds(row: dict) -> tuple[float | None, float | None, str | None]:
    for wcol, lcol, source in (("PSW", "PSL", "Pinnacle"), ("AvgW", "AvgL", "Average"), ("B365W", "B365L", "Bet365")):
        w, l = _number(row.get(wcol)), _number(row.get(lcol))
        if w and l and w > 1 and l > 1:
            return w, l, source
    return None, None, None


@dataclass
class Download:
    provider: str
    dataset: str
    tour: str
    season: int
    url: str
    content: bytes
    retrieved_at: datetime

    @property
    def raw_checksum(self) -> str:
        return checksum(self.content)


def download_partition(provider: str, dataset: str, tour: str, season: int, url: str, refresh: bool = False) -> Download:
    suffix = ".csv" if url.endswith(".csv") else ".xlsx"
    path = CACHE_DIR / provider / tour.lower() / f"{dataset}_{season}{suffix}"
    if path.exists() and not refresh:
        return Download(provider, dataset, tour, season, url, path.read_bytes(), datetime.fromtimestamp(path.stat().st_mtime, timezone.utc))

    response = _session().get(url, timeout=(10, 75))
    if response.status_code != 200 or not response.content:
        raise RuntimeError(f"{provider} {tour} {season} unavailable: HTTP {response.status_code}, {len(response.content)} bytes")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(response.content)
    return Download(provider, dataset, tour, season, url, response.content, datetime.now(timezone.utc))


def load_tml(download: Download) -> list[dict]:
    return list(csv.DictReader(io.StringIO(download.content.decode("utf-8-sig"))))


def load_tennis_data(download: Download) -> list[dict]:
    import pandas as pd

    return pd.read_excel(io.BytesIO(download.content)).to_dict("records")


def source_match_key(source: str, tour: str, row: dict, match_date: date, winner: str, loser: str) -> str:
    if source == ATP_PROVIDER and str(row.get("tourney_id") or "").strip():
        match_num = str(row.get("match_num") or "").strip()
        if match_num:
            return f"{row.get('tourney_id')}:{match_num}"
        players_hash = checksum({
            "winner": normalize_name(winner),
            "loser": normalize_name(loser),
        })[:16]
        return f"{row.get('tourney_id')}:players:{players_hash}"
    natural = {
        "source": source,
        "tour": tour,
        "date": match_date.isoformat(),
        "tournament": str(row.get("Tournament") or row.get("tourney_name") or "").strip(),
        "round": str(row.get("Round") or row.get("round") or "").strip(),
        "winner": player_identity_key(tour, winner, source),
        "loser": player_identity_key(tour, loser, source),
    }
    return checksum(natural)[:32]


def _upsert_player(cur, tour: str, provider: str, raw_name: str,
                   provider_player_id: str | None, captured_at: datetime,
                   raw_checksum: str, existing_player_id: int | None = None) -> int:
    key = player_identity_key(tour, raw_name, provider)
    if not key:
        raise ValueError(f"Cannot normalize player name: {raw_name!r}")
    player_id = existing_player_id
    if player_id is None and provider == TENNIS_DATA_PROVIDER:
        # A reviewed provider-specific alias outranks the lossy surname/initial
        # bridge (for example Wang Xiy. versus Wang Xin.).
        reviewed_key = normalize_name(raw_name)
        cur.execute(
            """
            SELECT player_id FROM tennis_player_aliases
            WHERE provider=%s AND tour=%s AND norm_name=%s AND verified
              AND match_method='manual_provider_disambiguation'
            ORDER BY captured_at DESC LIMIT 1
            """,
            (provider, tour, reviewed_key),
        )
        reviewed = cur.fetchone()
        if reviewed:
            player_id = reviewed["player_id"]
            key = reviewed_key
    if player_id is None:
        cur.execute(
            """
            INSERT INTO tennis_players (tour, canonical_name, norm_name)
            VALUES (%s, %s, %s)
            ON CONFLICT (tour, norm_name) DO UPDATE SET
                canonical_name = CASE
                    WHEN length(EXCLUDED.canonical_name) > length(tennis_players.canonical_name)
                    THEN EXCLUDED.canonical_name ELSE tennis_players.canonical_name END,
                updated_at = NOW()
            RETURNING id
            """,
            (tour, str(raw_name).strip(), key),
        )
        player_id = cur.fetchone()["id"]
    cur.execute(
        """
        INSERT INTO tennis_player_aliases (
            player_id, provider, tour, provider_player_id, raw_name, norm_name,
            match_method, match_confidence, source_available_at, captured_at, raw_checksum
        ) VALUES (%s, %s, %s, %s, %s, %s, 'exact_normalized', 1.0, %s, %s, %s)
        ON CONFLICT (provider, tour, norm_name, player_id) DO UPDATE SET
            provider_player_id = COALESCE(EXCLUDED.provider_player_id, tennis_player_aliases.provider_player_id),
            raw_name = EXCLUDED.raw_name,
            captured_at = GREATEST(tennis_player_aliases.captured_at, EXCLUDED.captured_at),
            raw_checksum = EXCLUDED.raw_checksum
        """,
        (player_id, provider, tour, provider_player_id, str(raw_name).strip(), key, captured_at, captured_at, raw_checksum),
    )
    return player_id


def _parse_iso(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except (TypeError, ValueError):
        return None


def american_to_decimal(price: int) -> float:
    if price == 0:
        raise ValueError("American price cannot be zero")
    return round(1.0 + (price / 100.0 if price > 0 else 100.0 / abs(price)), 6)


def _upsert_live_event(cur, *, tour: str, tournament: str, provider_event_id: str,
                       home_name: str, away_name: str, commence_time: datetime,
                       captured_at: datetime, raw_event: dict) -> tuple[int, int, int, int, int]:
    event_hash = checksum(raw_event)
    home_id = _upsert_player(cur, tour, "the_odds_api", home_name, None, captured_at, event_hash)
    away_id = _upsert_player(cur, tour, "the_odds_api", away_name, None, captured_at, event_hash)
    player_one_id, player_two_id = sorted((home_id, away_id))

    cur.execute(
        """
        SELECT event_id FROM tennis_event_aliases
        WHERE provider='the_odds_api' AND provider_event_id=%s
        """,
        (provider_event_id,),
    )
    alias = cur.fetchone()
    event_id = alias["event_id"] if alias else None
    if event_id is None:
        cur.execute(
            """
            SELECT id FROM tennis_events
            WHERE tour=%s AND canonical_tournament=%s
              AND player_one_id=%s AND player_two_id=%s
              AND scheduled_at BETWEEN %s - INTERVAL '48 hours' AND %s + INTERVAL '48 hours'
            ORDER BY ABS(EXTRACT(EPOCH FROM (scheduled_at - %s)))
            LIMIT 1
            """,
            (tour, tournament, player_one_id, player_two_id, commence_time, commence_time, commence_time),
        )
        existing = cur.fetchone()
        event_id = existing["id"] if existing else None
    if event_id is None:
        cur.execute(
            """
            INSERT INTO tennis_events (
                tour, canonical_tournament, player_one_id, player_two_id,
                scheduled_at, status
            ) VALUES (%s,%s,%s,%s,%s,'scheduled')
            RETURNING id
            """,
            (tour, tournament, player_one_id, player_two_id, commence_time),
        )
        event_id = cur.fetchone()["id"]

    cur.execute(
        """
        SELECT id FROM tennis_event_revisions
        WHERE provider='the_odds_api' AND provider_event_id=%s AND raw_checksum=%s
        """,
        (provider_event_id, event_hash),
    )
    exact_revision = cur.fetchone()
    cur.execute(
        """
        SELECT id, raw_checksum FROM tennis_event_revisions
        WHERE event_id=%s ORDER BY revision_no DESC LIMIT 1
        """,
        (event_id,),
    )
    latest = cur.fetchone()
    if exact_revision:
        revision_id = exact_revision["id"]
    elif latest and latest["raw_checksum"] == event_hash:
        revision_id = latest["id"]
    else:
        cur.execute("SELECT COALESCE(MAX(revision_no), 0) + 1 AS next_no FROM tennis_event_revisions WHERE event_id=%s", (event_id,))
        revision_no = cur.fetchone()["next_no"]
        cur.execute(
            """
            INSERT INTO tennis_event_revisions (
                event_id, revision_no, provider, provider_event_id, tournament_raw,
                commence_time, player_one_id, player_two_id, status,
                source_available_at, captured_at, raw_checksum, parser_version,
                raw_payload, supersedes_revision_id
            ) VALUES (%s,%s,'the_odds_api',%s,%s,%s,%s,%s,'scheduled',%s,%s,%s,%s,%s::jsonb,%s)
            RETURNING id
            """,
            (event_id, revision_no, provider_event_id, tournament, commence_time,
             player_one_id, player_two_id, captured_at, captured_at, event_hash,
             PARSER_VERSION, stable_json(raw_event), latest["id"] if latest else None),
        )
        revision_id = cur.fetchone()["id"]

    # Replaying an old capture must reuse its immutable revision without
    # moving the event/alias pointer backward from a newer observation.
    cur.execute(
        """
        SELECT id, commence_time, raw_checksum FROM tennis_event_revisions
        WHERE event_id=%s
        ORDER BY captured_at DESC, revision_no DESC
        LIMIT 1
        """,
        (event_id,),
    )
    current_revision = cur.fetchone()
    current_revision_id = current_revision["id"]

    cur.execute(
        """
        INSERT INTO tennis_event_aliases (
            event_id, event_revision_id, provider, provider_event_id,
            first_seen_at, last_seen_at, raw_checksum
        ) VALUES (%s,%s,'the_odds_api',%s,%s,%s,%s)
        ON CONFLICT (provider, provider_event_id) DO UPDATE SET
            event_revision_id=EXCLUDED.event_revision_id,
            last_seen_at=GREATEST(tennis_event_aliases.last_seen_at, EXCLUDED.last_seen_at),
            raw_checksum=EXCLUDED.raw_checksum
        """,
        (event_id, current_revision_id, provider_event_id, captured_at, captured_at,
         current_revision["raw_checksum"]),
    )
    cur.execute(
        """
        UPDATE tennis_events SET scheduled_at=%s, current_revision_id=%s,
            updated_at=NOW() WHERE id=%s
        """,
        (current_revision["commence_time"], current_revision_id, event_id),
    )
    return event_id, revision_id, current_revision_id, home_id, away_id


def ingest_live_event_quotes(db: DatabaseManager, *, tour: str, tournament: str,
                             raw_event: dict, captured_at: datetime,
                             quote_source: str = "the_odds_api") -> dict:
    """Normalize one Odds API event into immutable exact-book quote rows."""
    provider_event_id = str(raw_event.get("id") or "").strip()
    home_name = str(raw_event.get("home_team") or "").strip()
    away_name = str(raw_event.get("away_team") or "").strip()
    commence_time = _parse_iso(raw_event.get("commence_time"))
    if not provider_event_id or not home_name or not away_name or commence_time is None:
        raise ValueError("Odds API event is missing id, players, or commence_time")

    inserted = rejected = 0
    with db.connect() as conn:
        cur = conn.cursor()
        event_id, revision_id, current_revision_id, home_id, away_id = _upsert_live_event(
            cur, tour=tour, tournament=tournament, provider_event_id=provider_event_id,
            home_name=home_name, away_name=away_name, commence_time=commence_time,
            captured_at=captured_at, raw_event=raw_event,
        )
        quote_rows: list[dict] = []
        for bookmaker in raw_event.get("bookmakers") or []:
            book_key = str(bookmaker.get("key") or "").strip()
            book_updated = _parse_iso(bookmaker.get("last_update"))
            if not book_key or book_updated is None:
                rejected += 1
                continue
            for market in bookmaker.get("markets") or []:
                market_key = market.get("key")
                outcomes = market.get("outcomes") or []
                pairs: list[tuple[dict, dict, str]] = []
                if market_key == "h2h":
                    home = next((o for o in outcomes if o.get("name") == home_name), None)
                    away = next((o for o in outcomes if o.get("name") == away_name), None)
                    if home and away:
                        pairs = [(home, away, "moneyline"), (away, home, "moneyline")]
                elif market_key == "totals":
                    for line in sorted({o.get("point") for o in outcomes if o.get("point") is not None}):
                        over = next((o for o in outcomes if o.get("name") == "Over" and o.get("point") == line), None)
                        under = next((o for o in outcomes if o.get("name") == "Under" and o.get("point") == line), None)
                        if over and under:
                            pairs.extend(((over, under, "total"), (under, over, "total")))
                elif market_key == "spreads":
                    home = next((o for o in outcomes if o.get("name") == home_name), None)
                    away = next((o for o in outcomes if o.get("name") == away_name), None)
                    if home and away and home.get("point") is not None and away.get("point") is not None:
                        if abs(float(home["point"]) + float(away["point"])) < 1e-9:
                            pairs = [(home, away, "spread"), (away, home, "spread")]
                if not pairs:
                    rejected += 1
                    continue
                for selection, paired, normalized_market in pairs:
                    price = _number(selection.get("price"), int)
                    paired_price = _number(paired.get("price"), int)
                    if not price or not paired_price:
                        rejected += 1
                        continue
                    selection_name = selection.get("name")
                    paired_name = paired.get("name")
                    selection_player_id = home_id if selection_name == home_name else away_id if selection_name == away_name else None
                    paired_player_id = home_id if paired_name == home_name else away_id if paired_name == away_name else None
                    selection_side = "home" if selection_name == home_name else "away" if selection_name == away_name else str(selection_name).lower()
                    paired_side = "home" if paired_name == home_name else "away" if paired_name == away_name else str(paired_name).lower()
                    quote_rows.append({
                        "bookmaker_key": book_key,
                        "bookmaker_name": bookmaker.get("title"),
                        "market": normalized_market,
                        "selection_type": "player" if selection_player_id else selection_side,
                        "selection_player_id": selection_player_id,
                        "selection_side": selection_side,
                        "line_value": _number(selection.get("point")),
                        "price": price,
                        "paired_selection_type": "player" if paired_player_id else paired_side,
                        "paired_player_id": paired_player_id,
                        "paired_side": paired_side,
                        "paired_line_value": _number(paired.get("point")),
                        "paired_price": paired_price,
                        "book_updated": book_updated,
                        "raw": {"bookmaker": book_key, "market": market_key, "selection": selection, "paired": paired},
                    })

        quote_params: list[tuple] = []
        for quote in quote_rows:
            raw_hash = checksum(quote["raw"])
            prestart = quote["book_updated"] < commence_time and captured_at < commence_time
            quote_params.append(
                (event_id, revision_id, quote_source, provider_event_id, quote["bookmaker_key"],
                 quote["bookmaker_name"], None, quote["market"], quote["selection_type"],
                 quote["selection_player_id"], quote["selection_side"], quote["line_value"],
                 quote["price"], american_to_decimal(quote["price"]),
                 quote["paired_selection_type"], quote["paired_player_id"],
                 quote["paired_side"], quote["paired_line_value"], quote["paired_price"],
                 american_to_decimal(quote["paired_price"]), quote["book_updated"],
                 quote["book_updated"], captured_at, commence_time, prestart,
                 "valid" if prestart else "rejected", None if prestart else "post_start_or_in_play",
                 captured_at.replace(microsecond=0).isoformat(), raw_hash, PARSER_VERSION,
                 stable_json(quote["raw"])),
            )
        if quote_params:
            inserted = len(execute_values(
                cur,
                EXACT_QUOTE_INSERT_SQL,
                quote_params,
                template=EXACT_QUOTE_VALUE_TEMPLATE,
                page_size=1000,
                fetch=True,
            ))
        return {"event_id": event_id, "event_revision_id": current_revision_id,
                "quote_event_revision_id": revision_id,
                "home_player_id": home_id, "away_player_id": away_id,
                "quotes_inserted": inserted, "quotes_rejected": rejected}


def _start_partition(db: DatabaseManager, run_id: str, download: Download) -> int:
    row = db.execute_one(
        """
        INSERT INTO tennis_source_partitions (
            run_id, provider, dataset, tour, season, source_url, status,
            parser_version, retrieval_started_at, source_available_at, raw_checksum
        ) VALUES (%s, %s, %s, %s, %s, %s, 'running', %s, %s, %s, %s)
        ON CONFLICT (run_id, provider, dataset, tour, season) DO UPDATE SET
            status='running', source_url=EXCLUDED.source_url,
            retrieval_started_at=EXCLUDED.retrieval_started_at,
            source_available_at=EXCLUDED.source_available_at,
            raw_checksum=EXCLUDED.raw_checksum, error_message=NULL
        RETURNING id
        """,
        (run_id, download.provider, download.dataset, download.tour, download.season,
         download.url, PARSER_VERSION, download.retrieved_at, download.retrieved_at,
         download.raw_checksum),
    )
    return row["id"]


def _finish_partition(db: DatabaseManager, partition_id: int, *, status: str, row_count: int,
                      accepted: int, rejected: int, dates: list[date], missingness: dict,
                      error: str | None = None) -> None:
    db.execute(
        """
        UPDATE tennis_source_partitions SET
            status=%s, row_count=%s, accepted_count=%s, rejected_count=%s,
            min_match_date=%s, max_match_date=%s, missingness=%s::jsonb,
            retrieval_completed_at=NOW(), error_message=%s
        WHERE id=%s
        """,
        (status, row_count, accepted, rejected, min(dates) if dates else None,
         max(dates) if dates else None, stable_json(missingness), error, partition_id),
    )


def build_atp_odds_index(rows: Iterable[dict]) -> dict[tuple[str, str], list[dict]]:
    index: dict[tuple[str, str], list[dict]] = {}
    for row in rows:
        d = _as_date(row.get("Date"))
        winner, loser = str(row.get("Winner") or "").strip(), str(row.get("Loser") or "").strip()
        if not d or not winner or not loser:
            continue
        wk, lk = surname_initial_key(winner, abbreviated=True), surname_initial_key(loser, abbreviated=True)
        normalized = dict(row)
        normalized["__match_date"] = d
        index.setdefault((wk, lk), []).append(normalized)
    for candidates in index.values():
        candidates.sort(key=lambda row: row["__match_date"])
    return index


def find_atp_enrichment(index: dict[tuple[str, str], list[dict]] | None,
                        winner: str, loser: str, tournament_start: date | None,
                        tournament: str) -> dict | None:
    if not index or tournament_start is None:
        return None
    candidates = index.get((surname_initial_key(winner), surname_initial_key(loser)), [])
    if not candidates:
        return None
    tournament_key = normalize_name(tournament)
    plausible = []
    for row in candidates:
        d = row["__match_date"]
        day_delta = (d - tournament_start).days
        if -2 <= day_delta <= 21:
            source_tournament = normalize_name(str(row.get("Tournament") or ""))
            tournament_penalty = 0 if (
                tournament_key == source_tournament
                or tournament_key in source_tournament
                or source_tournament in tournament_key
            ) else 1
            plausible.append((tournament_penalty, abs(day_delta), d, row))
    if not plausible:
        return None
    plausible.sort(key=lambda item: (item[0], item[1], item[2]))
    return plausible[0][3]


def _insert_historical_match(cur, *, source: str, partition_id: int, tour: str,
                             row: dict, match_date: date, winner: str, loser: str,
                             winner_id: int, loser_id: int, captured_at: datetime,
                             enrichment: dict | None = None) -> tuple[int, bool]:
    enrichment = enrichment or {}
    row_clean = _clean_json(row)
    enrichment_clean = _clean_json(enrichment) if enrichment else None
    row_hash = checksum({
        "source_row": row_clean,
        "enrichment": enrichment_clean,
        "transformation_version": PARSER_VERSION,
    })
    key = source_match_key(source, tour, row, match_date, winner, loser)
    raw_surface = row.get("surface") if source == ATP_PROVIDER else row.get("Surface")
    raw_indoor = row.get("indoor") if source == ATP_PROVIDER else row.get("Court")
    surface = normalize_surface(raw_surface, raw_indoor)
    score = str(row.get("score") if source == ATP_PROVIDER else row.get("Score") or "").strip() or None
    score_upper = (score or "").upper()
    walkover = "W/O" in score_upper or "WALKOVER" in score_upper
    retired = "RET" in score_upper or "ABD" in score_upper
    completion = "walkover" if walkover else "retired" if retired else "completed"
    odds_w, odds_l, odds_source = _market_odds(enrichment)
    if source == TENNIS_DATA_PROVIDER:
        odds_w, odds_l, odds_source = _market_odds(row)
    winner_rank = _number(enrichment.get("WRank"), int) if enrichment else None
    loser_rank = _number(enrichment.get("LRank"), int) if enrichment else None
    winner_points = _number(enrichment.get("WPts"), int) if enrichment else None
    loser_points = _number(enrichment.get("LPts"), int) if enrichment else None
    if source == ATP_PROVIDER:
        winner_rank = winner_rank if winner_rank is not None else _number(row.get("winner_rank"), int)
        loser_rank = loser_rank if loser_rank is not None else _number(row.get("loser_rank"), int)
        winner_points = winner_points if winner_points is not None else _number(row.get("winner_rank_points"), int)
        loser_points = loser_points if loser_points is not None else _number(row.get("loser_rank_points"), int)
    else:
        winner_rank = _number(row.get("WRank"), int)
        loser_rank = _number(row.get("LRank"), int)
        winner_points = _number(row.get("WPts"), int)
        loser_points = _number(row.get("LPts"), int)

    cur.execute(
        """
        SELECT id, raw_checksum FROM tennis_historical_matches
        WHERE source=%s AND source_match_key=%s AND is_current
        ORDER BY created_at DESC, id DESC LIMIT 1
        """,
        (source, key),
    )
    current = cur.fetchone()
    if current and current["raw_checksum"] == row_hash:
        return current["id"], False
    if current:
        cur.execute(
            """
            UPDATE tennis_historical_matches
            SET is_current=FALSE, superseded_at=NOW()
            WHERE id=%s
            """,
            (current["id"],),
        )

    cur.execute(
        """
        INSERT INTO tennis_historical_matches (
            source, source_match_key, source_partition_id, tour, season, match_date,
            tournament, round, best_of, surface, indoor,
            winner_player_id, loser_player_id, score, completion_status, retired, walkover,
            winner_rank, loser_rank, winner_rank_points, loser_rank_points,
            winner_decimal_odds, loser_decimal_odds, odds_source, odds_timing,
            source_available_at, stats_through_at, captured_at,
            transformation_version, raw_checksum, raw_payload, correction_of_id,
            is_current
        ) VALUES (
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s,TRUE
        )
        ON CONFLICT (source, source_match_key, raw_checksum) DO NOTHING
        RETURNING id
        """,
        (source, key, partition_id, tour, match_date.year, match_date,
         str(row.get("tourney_name") if source == ATP_PROVIDER else row.get("Tournament") or "Unknown").strip(),
         str(row.get("round") if source == ATP_PROVIDER else row.get("Round") or "").strip() or None,
         _number(row.get("best_of") if source == ATP_PROVIDER else row.get("Best of"), int),
         surface, str(raw_indoor or "").strip().lower() in {"indoor", "true", "1", "yes"},
         winner_id, loser_id, score, completion, retired, walkover,
         winner_rank, loser_rank, winner_points, loser_points,
         odds_w, odds_l, odds_source, "representative_close" if odds_source else None,
         captured_at, datetime.combine(match_date, dt_time.max, tzinfo=timezone.utc), captured_at,
         PARSER_VERSION, row_hash, stable_json({"source_row": row_clean, "enrichment": enrichment_clean}),
         current["id"] if current else None),
    )
    inserted = cur.fetchone()
    if inserted:
        return inserted["id"], True
    cur.execute(
        "SELECT id FROM tennis_historical_matches WHERE source=%s AND source_match_key=%s AND raw_checksum=%s",
        (source, key, row_hash),
    )
    return cur.fetchone()["id"], False


def _build_stats_rows(match_id: int, winner_id: int, loser_id: int,
                      row: dict, source: str) -> list[tuple]:
    if source != ATP_PROVIDER:
        return [
            (match_id, player_id, opponent_id, won, None, None, None, None, None,
             None, None, None, None, None, None, False, "source_unavailable", None, None)
            for player_id, opponent_id, won
            in ((winner_id, loser_id, True), (loser_id, winner_id, False))
        ]

    winner_serve_won = (_number(row.get("w_1stWon"), int) or 0) + (_number(row.get("w_2ndWon"), int) or 0)
    loser_serve_won = (_number(row.get("l_1stWon"), int) or 0) + (_number(row.get("l_2ndWon"), int) or 0)
    winner_svpt, loser_svpt = _number(row.get("w_svpt"), int), _number(row.get("l_svpt"), int)
    sides = (
        (winner_id, loser_id, True, "w", winner_svpt, winner_serve_won,
         (loser_svpt - loser_serve_won) / loser_svpt if loser_svpt else None),
        (loser_id, winner_id, False, "l", loser_svpt, loser_serve_won,
         (winner_svpt - winner_serve_won) / winner_svpt if winner_svpt else None),
    )
    out = []
    for player_id, opponent_id, won, prefix, serve_points, serve_won, return_pct in sides:
        stats_available = bool(serve_points and serve_points > 0)
        out.append(
            (match_id, player_id, opponent_id, won,
             _number(row.get(f"{prefix}_ace"), int), _number(row.get(f"{prefix}_df"), int),
             serve_points, _number(row.get(f"{prefix}_1stIn"), int),
             _number(row.get(f"{prefix}_1stWon"), int), _number(row.get(f"{prefix}_2ndWon"), int),
             _number(row.get(f"{prefix}_SvGms"), int), _number(row.get(f"{prefix}_bpSaved"), int),
             _number(row.get(f"{prefix}_bpFaced"), int),
             serve_won / serve_points if serve_points else None, return_pct,
             stats_available, None if stats_available else "source_row_missing_serve_points",
             "serve-return-v1", serve_points)
        )
    return out


def _insert_stats_batch(cur, rows: list[tuple]) -> None:
    if not rows:
        return
    from psycopg2.extras import execute_values

    execute_values(
        cur,
        """
        INSERT INTO tennis_player_match_stats (
            historical_match_id, player_id, opponent_player_id, is_winner,
            aces, double_faults, serve_points, first_serves_in,
            first_serve_points_won, second_serve_points_won, service_games,
            break_points_saved, break_points_faced, serve_points_won_pct,
            return_points_won_pct, stats_available, missing_reason,
            formula_version, sample_size
        ) VALUES %s
        ON CONFLICT (historical_match_id, player_id) DO NOTHING
        """,
        rows,
        page_size=1000,
    )


def ingest_rows(db: DatabaseManager, run_id: str, download: Download, rows: list[dict],
                enrichment_index: dict[tuple[str, str, date], dict] | None = None,
                dry_run: bool = False) -> dict:
    partition_id = _start_partition(db, run_id, download)
    accepted = rejected = inserted = 0
    dates: list[date] = []
    missing = {"surface": 0, "rank": 0, "odds": 0, "start_time": 0, "performance": 0}

    if dry_run:
        parsed_dates = []
        for row in rows:
            d = _date_from_yyyymmdd(row.get("tourney_date")) if download.provider == ATP_PROVIDER else _as_date(row.get("Date"))
            if d and d >= START_DATE:
                parsed_dates.append(d)
        status = "pass" if parsed_dates else "fail"
        _finish_partition(db, partition_id, status=status, row_count=len(rows), accepted=len(parsed_dates),
                          rejected=len(rows)-len(parsed_dates), dates=parsed_dates,
                          missingness={"dry_run": True}, error=None if parsed_dates else "zero eligible rows")
        return {"partition_id": partition_id, "status": status, "rows": len(rows), "accepted": len(parsed_dates), "inserted": 0}

    with db.connect() as conn:
        cur = conn.cursor()
        player_cache: dict[tuple[str, str, str], int] = {}
        stats_rows: list[tuple] = []
        atp_bridge: dict[str, int | None] = {}
        if download.provider == TENNIS_DATA_PROVIDER and download.tour == "ATP":
            cur.execute(
                """
                SELECT DISTINCT p.id, p.canonical_name
                FROM tennis_players p
                JOIN tennis_player_aliases a ON a.player_id=p.id
                WHERE p.tour='ATP' AND a.provider=%s
                """,
                (ATP_PROVIDER,),
            )
            for player in cur.fetchall():
                bridge = surname_initial_key(player["canonical_name"], abbreviated=False)
                if bridge in atp_bridge and atp_bridge[bridge] != player["id"]:
                    atp_bridge[bridge] = None
                else:
                    atp_bridge[bridge] = player["id"]
        for row in rows:
            source = download.provider
            tour = download.tour
            tournament_start = _date_from_yyyymmdd(row.get("tourney_date")) if source == ATP_PROVIDER else None
            enrichment = None
            if source == ATP_PROVIDER:
                enrichment = find_atp_enrichment(
                    enrichment_index,
                    str(row.get("winner_name") or ""),
                    str(row.get("loser_name") or ""),
                    tournament_start,
                    str(row.get("tourney_name") or ""),
                )
            d = (
                enrichment.get("__match_date") if enrichment is not None
                else tournament_start if source == ATP_PROVIDER
                else _as_date(row.get("Date"))
            )
            winner = str(row.get("winner_name") if source == ATP_PROVIDER else row.get("Winner") or "").strip()
            loser = str(row.get("loser_name") if source == ATP_PROVIDER else row.get("Loser") or "").strip()
            if not d or d < START_DATE or not winner or not loser:
                rejected += 1
                continue
            try:
                row_hash = checksum(row)
                winner_identity = normalize_name(winner) if source == TENNIS_DATA_PROVIDER else player_identity_key(tour, winner, source)
                loser_identity = normalize_name(loser) if source == TENNIS_DATA_PROVIDER else player_identity_key(tour, loser, source)
                winner_cache_key = (tour, source, winner_identity)
                loser_cache_key = (tour, source, loser_identity)
                winner_id = player_cache.get(winner_cache_key)
                if winner_id is None:
                    winner_id = _upsert_player(cur, tour, source, winner,
                                               str(row.get("winner_id")) if row.get("winner_id") else None,
                                               download.retrieved_at, row_hash,
                                               atp_bridge.get(surname_initial_key(winner, abbreviated=True)) if atp_bridge else None)
                    player_cache[winner_cache_key] = winner_id
                loser_id = player_cache.get(loser_cache_key)
                if loser_id is None:
                    loser_id = _upsert_player(cur, tour, source, loser,
                                               str(row.get("loser_id")) if row.get("loser_id") else None,
                                               download.retrieved_at, row_hash,
                                              atp_bridge.get(surname_initial_key(loser, abbreviated=True)) if atp_bridge else None)
                    player_cache[loser_cache_key] = loser_id
                if winner_id == loser_id:
                    cur.execute(
                        """
                        INSERT INTO tennis_identity_reviews (
                            provider, tour, raw_name, norm_name, context,
                            candidates, reason, status
                        ) SELECT %s,%s,%s,%s,%s::jsonb,%s::jsonb,
                                 'opponents_resolved_to_same_player','open'
                        WHERE NOT EXISTS (
                            SELECT 1 FROM tennis_identity_reviews
                            WHERE provider=%s AND tour=%s AND status='open'
                              AND reason='opponents_resolved_to_same_player'
                              AND context->>'source_match_key'=%s
                        )
                        """,
                        (source, tour, f"{winner} | {loser}", normalize_name(winner),
                         stable_json({"source_match_key": source_match_key(source, tour, row, d, winner, loser),
                                      "winner": winner, "loser": loser}),
                         stable_json([{"player_id": winner_id}]), source, tour,
                         source_match_key(source, tour, row, d, winner, loser)),
                    )
                    raise ValueError(f"Opponents resolve to one player identity: {winner!r} / {loser!r}")
                match_id, was_inserted = _insert_historical_match(
                    cur, source=source, partition_id=partition_id, tour=tour, row=row,
                    match_date=d, winner=winner, loser=loser, winner_id=winner_id,
                    loser_id=loser_id, captured_at=download.retrieved_at,
                    enrichment=enrichment,
                )
                stats_rows.extend(_build_stats_rows(match_id, winner_id, loser_id, row, source))
                inserted += int(was_inserted)
                accepted += 1
                dates.append(d)
                raw_surface = row.get("surface") if source == ATP_PROVIDER else row.get("Surface")
                if normalize_surface(raw_surface, row.get("indoor") if source == ATP_PROVIDER else row.get("Court")) is None:
                    missing["surface"] += 1
                rank_value = row.get("winner_rank") if source == ATP_PROVIDER else row.get("WRank")
                if _number(rank_value, int) is None:
                    missing["rank"] += 1
                odds_row = enrichment if source == ATP_PROVIDER else row
                if not odds_row or _market_odds(odds_row)[0] is None:
                    missing["odds"] += 1
                missing["start_time"] += 1  # yearly sources provide date, not official start time
                if source != ATP_PROVIDER or _number(row.get("w_svpt"), int) is None:
                    missing["performance"] += 1
            except Exception as exc:  # noqa: BLE001
                rejected += 1
                logger.exception("Rejected %s %s match row: %s", tour, d, exc)
        _insert_stats_batch(cur, stats_rows)

    eligible = accepted + rejected
    missingness = {k: {"count": v, "pct": round(v / accepted * 100, 3) if accepted else None} for k, v in missing.items()}
    status = "pass" if accepted > 0 else "fail"
    error = None if status == "pass" else "zero eligible rows"
    _finish_partition(db, partition_id, status=status, row_count=len(rows), accepted=accepted,
                      rejected=rejected, dates=dates, missingness=missingness, error=error)
    return {"partition_id": partition_id, "status": status, "rows": len(rows),
            "eligible": eligible, "accepted": accepted, "rejected": rejected,
            "inserted": inserted, "missingness": missingness}


def _record_failed_partition(db: DatabaseManager, run_id: str, *, provider: str, dataset: str,
                             tour: str, season: int, url: str, started: datetime, error: Exception) -> None:
    db.execute(
        """
        INSERT INTO tennis_source_partitions (
            run_id, provider, dataset, tour, season, source_url, status,
            parser_version, retrieval_started_at, retrieval_completed_at, error_message
        ) VALUES (%s,%s,%s,%s,%s,%s,'fail',%s,%s,NOW(),%s)
        ON CONFLICT (run_id, provider, dataset, tour, season) DO UPDATE SET
            status='fail', retrieval_completed_at=NOW(), error_message=EXCLUDED.error_message
        """,
        (run_id, provider, dataset, tour, season, url, PARSER_VERSION, started, str(error)),
    )


def run(db: DatabaseManager, *, from_year: int = 2023, to_year: int | None = None,
        tours: tuple[str, ...] = ("ATP", "WTA"), season: int | None = None,
        refresh: bool = False, dry_run: bool = False) -> dict:
    to_year = to_year or datetime.now(timezone.utc).year
    years = [season] if season else list(range(max(2023, from_year), to_year + 1))
    run_id = f"{PARSER_VERSION}:{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}:{uuid.uuid4().hex[:8]}"
    report = {"run_id": run_id, "parser_version": PARSER_VERSION, "partitions": [], "status": "pass"}

    for tour in tours:
        for year in years:
            if tour == "ATP":
                enrichment_index = None
                enrichment_url = TENNIS_DATA_URL["ATP"].format(year=year)
                started = datetime.now(timezone.utc)
                try:
                    enrichment_download = download_partition(
                        TENNIS_DATA_PROVIDER, "historical_odds_rank", "ATP", year,
                        enrichment_url, refresh=refresh,
                    )
                    enrichment_rows = load_tennis_data(enrichment_download)
                    enrichment_index = build_atp_odds_index(enrichment_rows)
                    enrichment_partition_id = _start_partition(db, run_id, enrichment_download)
                    enrichment_dates = [
                        d for d in (_as_date(row.get("Date")) for row in enrichment_rows)
                        if d is not None and d >= START_DATE
                    ]
                    enrichment_status = "pass" if enrichment_dates else "fail"
                    _finish_partition(
                        db,
                        enrichment_partition_id,
                        status=enrichment_status,
                        row_count=len(enrichment_rows),
                        accepted=len(enrichment_dates),
                        rejected=len(enrichment_rows) - len(enrichment_dates),
                        dates=enrichment_dates,
                        missingness={
                            "odds": {
                                "count": sum(1 for row in enrichment_rows if _market_odds(row)[0] is None),
                                "pct": round(
                                    sum(1 for row in enrichment_rows if _market_odds(row)[0] is None)
                                    / len(enrichment_rows) * 100,
                                    3,
                                ) if enrichment_rows else None,
                            }
                        },
                        error=None if enrichment_status == "pass" else "zero eligible rows",
                    )
                    enrichment_result = {
                        "provider": TENNIS_DATA_PROVIDER, "dataset": "historical_odds_rank",
                        "tour": "ATP", "season": year, "status": enrichment_status,
                        "partition_id": enrichment_partition_id,
                        "rows": len(enrichment_rows), "matched_index": len(enrichment_index),
                    }
                    if enrichment_status != "pass":
                        report["status"] = "fail"
                except Exception as exc:  # noqa: BLE001
                    _record_failed_partition(db, run_id, provider=TENNIS_DATA_PROVIDER,
                                             dataset="historical_odds_rank", tour="ATP",
                                             season=year, url=enrichment_url, started=started, error=exc)
                    enrichment_result = {"provider": TENNIS_DATA_PROVIDER,
                                         "dataset": "historical_odds_rank", "tour": "ATP",
                                         "season": year, "status": "fail", "error": str(exc)}
                    report["status"] = "fail"
                report["partitions"].append(enrichment_result)

                provider, dataset = ATP_PROVIDER, "historical_matches_stats"
                url = TML_URL.format(year=year)
                started = datetime.now(timezone.utc)
                try:
                    download = download_partition(provider, dataset, tour, year, url, refresh=refresh)
                    rows = load_tml(download)
                    result = ingest_rows(db, run_id, download, rows, enrichment_index, dry_run=dry_run)
                except Exception as exc:  # noqa: BLE001
                    _record_failed_partition(db, run_id, provider=provider, dataset=dataset,
                                             tour=tour, season=year, url=url, started=started, error=exc)
                    result = {"provider": provider, "dataset": dataset, "tour": tour,
                              "season": year, "status": "fail", "error": str(exc)}
                result.update({"provider": provider, "dataset": dataset, "tour": tour, "season": year})
                report["partitions"].append(result)
                if result.get("status") != "pass":
                    report["status"] = "fail"
                print(json.dumps(result, sort_keys=True, default=str))

                # tennis-data is also preserved as the complete ATP chronology
                # source. TML remains the performance-stat backbone, but its
                # current-season file can lag by months (observed in 2026).
                fallback_dataset = "historical_matches_odds_rank"
                if enrichment_result.get("status") == "pass":
                    fallback_download = Download(
                        TENNIS_DATA_PROVIDER,
                        fallback_dataset,
                        "ATP",
                        year,
                        enrichment_url,
                        enrichment_download.content,
                        enrichment_download.retrieved_at,
                    )
                    try:
                        result = ingest_rows(
                            db, run_id, fallback_download, enrichment_rows,
                            dry_run=dry_run,
                        )
                    except Exception as exc:  # noqa: BLE001
                        _record_failed_partition(
                            db, run_id, provider=TENNIS_DATA_PROVIDER,
                            dataset=fallback_dataset, tour="ATP", season=year,
                            url=enrichment_url, started=started, error=exc,
                        )
                        result = {"status": "fail", "error": str(exc)}
                else:
                    result = {"status": "fail", "error": "ATP tennis-data chronology source unavailable"}
                result.update({
                    "provider": TENNIS_DATA_PROVIDER,
                    "dataset": fallback_dataset,
                    "tour": "ATP",
                    "season": year,
                })
            else:
                provider, dataset = TENNIS_DATA_PROVIDER, "historical_matches_odds_rank"
                url = TENNIS_DATA_URL["WTA"].format(year=year)
                started = datetime.now(timezone.utc)
                try:
                    download = download_partition(provider, dataset, tour, year, url, refresh=refresh)
                    rows = load_tennis_data(download)
                    result = ingest_rows(db, run_id, download, rows, dry_run=dry_run)
                except Exception as exc:  # noqa: BLE001
                    _record_failed_partition(db, run_id, provider=provider, dataset=dataset,
                                             tour=tour, season=year, url=url, started=started, error=exc)
                    result = {"provider": provider, "dataset": dataset, "tour": tour,
                              "season": year, "status": "fail", "error": str(exc)}
                result.update({"provider": provider, "dataset": dataset, "tour": tour, "season": year})
            report["partitions"].append(result)
            if result.get("status") != "pass":
                report["status"] = "fail"
            print(json.dumps(result, sort_keys=True, default=str))

    print(json.dumps(report, indent=2, default=str))
    return report


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Populate immutable 2023+ Tennis history")
    parser.add_argument("--from-year", type=int, default=2023)
    parser.add_argument("--to-year", type=int)
    parser.add_argument("--season", type=int, help="Run one source season only")
    parser.add_argument("--tour", choices=["ATP", "WTA", "all"], default="all")
    parser.add_argument("--refresh", action="store_true", help="Refetch raw provider files")
    parser.add_argument("--dry-run", action="store_true", help="Validate partitions without match inserts")
    args = parser.parse_args()

    selected_tours = ("ATP", "WTA") if args.tour == "all" else (args.tour,)
    config = load_config()
    database = DatabaseManager(config.database_url)
    result = run(database, from_year=args.from_year, to_year=args.to_year,
                 tours=selected_tours, season=args.season,
                 refresh=args.refresh, dry_run=args.dry_run)
    raise SystemExit(0 if result["status"] == "pass" else 1)
