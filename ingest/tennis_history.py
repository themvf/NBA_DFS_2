"""Build player ratings (grass Elo + serve/return) from match history.

Two tours, two sources — because no single free source covers both with serve
stats (see memory tennis-data-sources):

  * **ATP** — ``Tennismylife/TML-Database`` (GitHub), Sackmann-format yearly CSVs
    with serve points (w_svpt / w_1stWon / …).  Gives overall Elo, grass Elo AND
    serve/return points-won% on grass.  Keyed by full-name concat (TML carries
    full names, matching the Odds API "First Last").
  * **WTA** — ``tennis-data.co.uk`` yearly xlsx (``/{year}w/``).  Results only —
    NO serve stats — so WTA gets overall + grass Elo but serve/return stay NULL.
    tennis-data uses "Surname I." names, so WTA rows are keyed by
    (surname, first-initial) — the same bridge the settlement matcher uses to
    reconcile "Swiatek I." ↔ the Odds API "Iga Swiatek".

Both tours run the SAME Elo engine (``_run_elo``); they differ only in how raw
history is parsed into normalized matches.  Predictions blend grass↔overall by
grass sample size.

Usage:
    python -m ingest.tennis_history                 # both tours, default window
    python -m ingest.tennis_history --tour WTA      # one tour only
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

_TML_BASE = "https://raw.githubusercontent.com/Tennismylife/TML-Database/master"
_TENNISDATA_WTA = "http://www.tennis-data.co.uk/{year}w/{year}.xlsx"
_HEADERS = {"User-Agent": "Mozilla/5.0"}
_K = 32.0
_BASE_ELO = 1500.0
_DEFAULT_FROM = 2011
_MIN_MATCHES = 3          # drop one-off qualifiers (noise)


def _normalize_name(name: str) -> str:
    text = unicodedata.normalize("NFKD", name or "").encode("ascii", "ignore").decode("ascii")
    return "".join(ch for ch in text.lower() if ch.isalnum())


def _surname_initial_key(name: str) -> str | None:
    """(surname+first-initial) key from a tennis-data "Surname [Parts] X." name.

    The trailing token is the initial ("Davidovich Fokina A." → surname
    'davidovichfokina', initial 'a' → 'davidovichfokinaa').  Matches the
    Odds-API-side key used by the settlement matcher, so WTA ratings key on the
    same string the predictions layer will derive from "First Last".
    """
    parts = [p for p in str(name or "").split() if p]
    if len(parts) < 2:
        return None
    initial = _normalize_name(parts[-1])[:1]
    surname = _normalize_name("".join(parts[:-1]))
    return (surname + initial) if surname and initial else None


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


# ── Shared Elo engine ─────────────────────────────────────────────
# A normalized match is a dict:
#   wkey/lkey     — rating key (str)         wdisp/ldisp — display name (str)
#   is_grass      — bool
#   serve         — optional (w_svpt, l_svpt, w_won, l_won) for serve/return %,
#                   or None when the source has no serve stats (WTA).

def _run_elo(matches: list[dict]) -> dict[str, _Player]:
    """Single chronological Elo pass. Returns {key: _Player}."""
    players: dict[str, _Player] = {}

    def _get(key: str, disp: str) -> _Player:
        p = players.get(key)
        if p is None:
            p = _Player(disp)
            players[key] = p
        return p

    for m in matches:
        w = _get(m["wkey"], m["wdisp"])
        l = _get(m["lkey"], m["ldisp"])

        exp_w = _expected(w.overall, l.overall)
        w.overall += _K * (1 - exp_w)
        l.overall += _K * (0 - (1 - exp_w))
        w.matches += 1
        l.matches += 1

        if m["is_grass"]:
            exp_wg = _expected(w.grass, l.grass)
            w.grass += _K * (1 - exp_wg)
            l.grass += _K * (0 - (1 - exp_wg))
            w.grass_matches += 1
            l.grass_matches += 1

            serve = m.get("serve")
            if serve:
                w_svpt, l_svpt, w_won, l_won = serve
                if w_svpt and l_svpt:
                    w.srv_won += w_won; w.srv_pts += w_svpt
                    l.srv_won += l_won; l.srv_pts += l_svpt
                    # Return points won = opponent serve points lost.
                    w.ret_won += (l_svpt - l_won); w.ret_pts += l_svpt
                    l.ret_won += (w_svpt - w_won); l.ret_pts += w_svpt

    return players


def _upsert(db: DatabaseManager, tour: str, players: dict[str, _Player]) -> int:
    """Upsert rated players for one tour (>= _MIN_MATCHES). Returns count."""
    upserted = 0
    with db.connect() as conn:
        cur = conn.cursor()
        for k, p in players.items():
            if p.matches < _MIN_MATCHES:
                continue
            srv_pct = round(p.srv_won / p.srv_pts, 4) if p.srv_pts else None
            ret_pct = round(p.ret_won / p.ret_pts, 4) if p.ret_pts else None
            cur.execute(
                """
                INSERT INTO tennis_player_ratings (
                    tour, norm_name, display_name, overall_elo, grass_elo,
                    grass_matches, serve_pts_won_pct, return_pts_won_pct, matches, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
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
                (tour, k, p.display, round(p.overall, 1), round(p.grass, 1),
                 p.grass_matches, srv_pct, ret_pct, p.matches),
            )
            upserted += 1
    return upserted


# ── ATP source: TML-Database (Sackmann CSV, with serve stats) ─────

def _fetch_tml_year(year: int) -> list[dict]:
    url = f"{_TML_BASE}/{year}.csv"
    try:
        r = requests.get(url, headers=_HEADERS, timeout=40)
        if not r.ok:
            logger.warning("ATP year %s unavailable (HTTP %s)", year, r.status_code)
            return []
        return list(csv.DictReader(io.StringIO(r.text)))
    except requests.RequestException as exc:
        logger.warning("ATP fetch failed for %s: %s", year, exc)
        return []


def _atp_matches(from_year: int, this_year: int) -> list[dict]:
    raw: list[dict] = []
    for y in range(from_year, this_year + 1):
        rows = _fetch_tml_year(y)
        raw.extend(rows)
        if rows:
            logger.info("ATP %s: %d matches", y, len(rows))

    def _key(m):
        return (_to_int(m.get("tourney_date")) or 0, _to_int(m.get("match_num")) or 0)
    raw.sort(key=_key)

    out: list[dict] = []
    for m in raw:
        wname, lname = m.get("winner_name", ""), m.get("loser_name", "")
        wkey, lkey = _normalize_name(wname), _normalize_name(lname)
        if not wkey or not lkey:
            continue
        w_svpt, l_svpt = _to_int(m.get("w_svpt")), _to_int(m.get("l_svpt"))
        w_won = (_to_int(m.get("w_1stWon")) or 0) + (_to_int(m.get("w_2ndWon")) or 0)
        l_won = (_to_int(m.get("l_1stWon")) or 0) + (_to_int(m.get("l_2ndWon")) or 0)
        out.append({
            "wkey": wkey, "wdisp": wname, "lkey": lkey, "ldisp": lname,
            "is_grass": m.get("surface") == "Grass",
            "serve": (w_svpt, l_svpt, w_won, l_won),
        })
    return out


# ── WTA source: tennis-data.co.uk xlsx (results only, no serve) ───

def _fetch_wta_year(year: int) -> list[dict]:
    import pandas as pd

    url = _TENNISDATA_WTA.format(year=year)
    try:
        r = requests.get(url, headers=_HEADERS, timeout=40)
        if not r.ok:
            logger.warning("WTA year %s unavailable (HTTP %s)", year, r.status_code)
            return []
        df = pd.read_excel(io.BytesIO(r.content))
    except Exception as exc:  # noqa: BLE001 — network/parse both non-fatal
        logger.warning("WTA fetch failed for %s: %s", year, exc)
        return []
    return df.to_dict("records")


def _wta_matches(from_year: int, this_year: int) -> list[dict]:
    import pandas as pd

    raw: list[dict] = []
    for y in range(from_year, this_year + 1):
        rows = _fetch_wta_year(y)
        raw.extend(rows)
        if rows:
            logger.info("WTA %s: %d matches", y, len(rows))

    # Chronological order by Date (NaT sorts last, then dropped by key check).
    def _date_key(m):
        d = m.get("Date")
        return d if isinstance(d, datetime) else datetime.max
    raw.sort(key=_date_key)

    out: list[dict] = []
    for m in raw:
        wname, lname = m.get("Winner"), m.get("Loser")
        if wname is None or lname is None or (isinstance(wname, float) and pd.isna(wname)):
            continue
        wkey, lkey = _surname_initial_key(wname), _surname_initial_key(lname)
        if not wkey or not lkey:
            continue
        out.append({
            "wkey": wkey, "wdisp": str(wname).strip(), "lkey": lkey, "ldisp": str(lname).strip(),
            "is_grass": str(m.get("Surface", "")).strip() == "Grass",
            "serve": None,
        })
    return out


# ── Orchestration ─────────────────────────────────────────────────

_BUILDERS = {"ATP": _atp_matches, "WTA": _wta_matches}


def build_ratings(db: DatabaseManager, from_year: int = _DEFAULT_FROM,
                  tours: tuple[str, ...] = ("ATP", "WTA")) -> int:
    """Build Elo ratings for the given tours and upsert. Returns #players."""
    this_year = datetime.utcnow().year
    total = 0
    for tour in tours:
        matches = _BUILDERS[tour](from_year, this_year)
        if not matches:
            logger.warning("No %s match history fetched — skipping (ratings unchanged)", tour)
            continue
        players = _run_elo(matches)
        n = _upsert(db, tour, players)
        print(f"Tennis ratings: {n} {tour} players upserted "
              f"({len(matches)} matches, {from_year}-{this_year})")
        total += n
    return total


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Build tennis player Elo ratings from history")
    parser.add_argument("--from", dest="from_year", type=int, default=_DEFAULT_FROM,
                        help=f"Start year (default {_DEFAULT_FROM})")
    parser.add_argument("--tour", choices=["ATP", "WTA"],
                        help="Only build this tour (default: both)")
    args = parser.parse_args()

    config = load_config()
    db = DatabaseManager(config.database_url)
    tours = (args.tour,) if args.tour else ("ATP", "WTA")
    build_ratings(db, args.from_year, tours)
