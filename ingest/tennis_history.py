"""Build ATP player ratings (grass Elo + serve/return) from match history.

Source: **Tennismylife/TML-Database** (GitHub) — Sackmann-format yearly ATP CSVs,
still live and updated (the original JeffSackmann/tennis_atp repo was deleted in
2026; see memory tennis-data-sources).  WTA has no clean live mirror, so V1
ratings are ATP-only; WTA matches stay odds-only in the Vegas view.

For each ATP player we compute, in one chronological pass:
  * overall_elo  — all-surface Elo (robust, lots of matches)
  * grass_elo    — grass-only Elo (sparse → blended with overall in predictions)
  * serve / return points-won% on grass (for totals, later)

Elo: standard, K=32, base 1500, per-tour pool.  Predictions blend grass↔overall
by grass sample size.

Usage:
    python -m ingest.tennis_history                 # default window
    python -m ingest.tennis_history --from 2012     # custom start year
"""

from __future__ import annotations

import argparse
import csv
import io
import logging
import unicodedata
from datetime import datetime

import requests

from config import load_config
from db.database import DatabaseManager

logger = logging.getLogger(__name__)

_RAW_BASE = "https://raw.githubusercontent.com/Tennismylife/TML-Database/master"
_HEADERS = {"User-Agent": "Mozilla/5.0"}
_K = 32.0
_BASE_ELO = 1500.0
_DEFAULT_FROM = 2011


def _normalize_name(name: str) -> str:
    text = unicodedata.normalize("NFKD", name or "").encode("ascii", "ignore").decode("ascii")
    return "".join(ch for ch in text.lower() if ch.isalnum())


def _expected(elo_a: float, elo_b: float) -> float:
    return 1.0 / (1.0 + 10 ** ((elo_b - elo_a) / 400.0))


def _to_int(v) -> int | None:
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None


class _Player:
    __slots__ = ("display", "overall", "grass", "grass_matches", "matches",
                 "srv_won", "srv_pts", "ret_won", "ret_pts")

    def __init__(self, display: str):
        self.display = display
        self.overall = _BASE_ELO
        self.grass = _BASE_ELO
        self.grass_matches = 0
        self.matches = 0
        self.srv_won = 0
        self.srv_pts = 0
        self.ret_won = 0
        self.ret_pts = 0


def _fetch_year(year: int) -> list[dict]:
    url = f"{_RAW_BASE}/{year}.csv"
    try:
        r = requests.get(url, headers=_HEADERS, timeout=40)
        if not r.ok:
            logger.warning("Year %s unavailable (HTTP %s)", year, r.status_code)
            return []
        return list(csv.DictReader(io.StringIO(r.text)))
    except requests.RequestException as exc:
        logger.warning("Fetch failed for %s: %s", year, exc)
        return []


def build_ratings(db: DatabaseManager, from_year: int = _DEFAULT_FROM) -> int:
    """Compute ATP ratings over [from_year .. current] and upsert. Returns #players."""
    this_year = datetime.utcnow().year
    matches: list[dict] = []
    for y in range(from_year, this_year + 1):
        rows = _fetch_year(y)
        matches.extend(rows)
        if rows:
            logger.info("Year %s: %d matches", y, len(rows))

    if not matches:
        logger.warning("No match history fetched — aborting (ratings unchanged)")
        return 0

    # Chronological order: tourney_date (YYYYMMDD) then match_num.
    def _key(m):
        return (_to_int(m.get("tourney_date")) or 0, _to_int(m.get("match_num")) or 0)
    matches.sort(key=_key)

    players: dict[str, _Player] = {}

    def _get(name: str) -> _Player | None:
        if not name:
            return None
        k = _normalize_name(name)
        if not k:
            return None
        p = players.get(k)
        if p is None:
            p = _Player(name)
            players[k] = p
        return p

    for m in matches:
        w = _get(m.get("winner_name", ""))
        l = _get(m.get("loser_name", ""))
        if w is None or l is None:
            continue
        is_grass = (m.get("surface") == "Grass")

        # Overall Elo update.
        exp_w = _expected(w.overall, l.overall)
        w.overall += _K * (1 - exp_w)
        l.overall += _K * (0 - (1 - exp_w))
        w.matches += 1
        l.matches += 1

        if is_grass:
            exp_wg = _expected(w.grass, l.grass)
            w.grass += _K * (1 - exp_wg)
            l.grass += _K * (0 - (1 - exp_wg))
            w.grass_matches += 1
            l.grass_matches += 1
            # Serve/return points (grass only).
            w_svpt, l_svpt = _to_int(m.get("w_svpt")), _to_int(m.get("l_svpt"))
            w_won = (_to_int(m.get("w_1stWon")) or 0) + (_to_int(m.get("w_2ndWon")) or 0)
            l_won = (_to_int(m.get("l_1stWon")) or 0) + (_to_int(m.get("l_2ndWon")) or 0)
            if w_svpt and l_svpt:
                w.srv_won += w_won; w.srv_pts += w_svpt
                l.srv_won += l_won; l.srv_pts += l_svpt
                # Return points won = opponent serve points lost.
                w.ret_won += (l_svpt - l_won); w.ret_pts += l_svpt
                l.ret_won += (w_svpt - w_won); l.ret_pts += w_svpt

    # Upsert. Keep only players with >= 3 matches (drop one-off qualifiers noise).
    upserted = 0
    with db.connect() as conn:
        cur = conn.cursor()
        for k, p in players.items():
            if p.matches < 3:
                continue
            srv_pct = round(p.srv_won / p.srv_pts, 4) if p.srv_pts else None
            ret_pct = round(p.ret_won / p.ret_pts, 4) if p.ret_pts else None
            cur.execute(
                """
                INSERT INTO tennis_player_ratings (
                    tour, norm_name, display_name, overall_elo, grass_elo,
                    grass_matches, serve_pts_won_pct, return_pts_won_pct, matches, updated_at
                ) VALUES ('ATP', %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (tour, norm_name) DO UPDATE SET
                    display_name = EXCLUDED.display_name,
                    overall_elo = EXCLUDED.overall_elo,
                    grass_elo = EXCLUDED.grass_elo,
                    grass_matches = EXCLUDED.grass_matches,
                    serve_pts_won_pct = EXCLUDED.serve_pts_won_pct,
                    return_pts_won_pct = EXCLUDED.return_pts_won_pct,
                    matches = EXCLUDED.matches,
                    updated_at = NOW()
                """,
                (k, p.display, round(p.overall, 1), round(p.grass, 1),
                 p.grass_matches, srv_pct, ret_pct, p.matches),
            )
            upserted += 1

    print(f"Tennis ratings: {upserted} ATP players upserted "
          f"({len(matches)} matches, {from_year}-{this_year})")
    return upserted


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Build ATP player ratings from history")
    parser.add_argument("--from", dest="from_year", type=int, default=_DEFAULT_FROM,
                        help=f"Start year (default {_DEFAULT_FROM})")
    args = parser.parse_args()

    config = load_config()
    db = DatabaseManager(config.database_url)
    build_ratings(db, args.from_year)
