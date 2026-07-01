"""Build the labeled historical training corpus for the fitted tennis model.

Source: **tennis-data.co.uk** yearly xlsx (ATP ``/{year}/`` + WTA ``/{year}w/``) —
the only free tennis source that carries, in one row, everything a market-anchored
win-probability model needs to be *trained*:

  * label   — Winner / Loser (→ did the favorite win?)
  * market  — closing decimal odds (PSW/PSL Pinnacle → AvgW/AvgL → B365W/B365L),
              de-vigged to a 2-way fair prob
  * strength — WRank/LRank, WPts/LPts (carried for future features)
  * context — Surface (grass Elo), Best of, Round, tournament, Date, tour

**Orientation** is by the market favorite (lower decimal odds), NOT by the winner —
so it is leak-free and reproducible at prediction time (we have both odds live).
``y = 1`` iff the favorite won.

**Point-in-time Elo** is replayed chronologically over this same corpus and
snapshotted BEFORE each match, so no future result leaks into a match's features.
It uses the identical K / base / update rule as ``ingest/tennis_history`` (the
production ratings), so the Elo *scale* matches what the model sees live — even
though production ATP Elo is sourced from TML (coverage differs slightly; the
market anchor absorbs the residual — validated in P3).

V1 fitted feature set (intersection of "in this corpus" AND "available live"):
    market_fav_prob, elo_diff, grass_elo_diff
Rank/pts are carried but not fit in V1 (no live rankings ingest yet); serve is
absent here (tennis-data has no serve stats) — an ATP-only enhancement for later.

Usage:
    python -m ingest.tennis_training                 # both tours → data/ cache
    python -m ingest.tennis_training --tour WTA
    python -m ingest.tennis_training --from 2012 --no-cache
"""

from __future__ import annotations

import argparse
import io
import logging
from datetime import datetime
from pathlib import Path

import requests

from ingest.tennis_history import _BASE_ELO, _K, _expected, _surname_initial_key

logger = logging.getLogger(__name__)

_TOUR_URLS = {
    "ATP": "http://www.tennis-data.co.uk/{year}/{year}.xlsx",
    "WTA": "http://www.tennis-data.co.uk/{year}w/{year}.xlsx",
}
_HEADERS = {"User-Agent": "Mozilla/5.0"}
_DEFAULT_FROM = 2011
_CACHE_DIR = Path(__file__).resolve().parent.parent / "data"

# Odds columns in preference order (sharpest first). Each is (winner_col, loser_col).
_ODDS_PREFERENCE = [("PSW", "PSL"), ("AvgW", "AvgL"), ("B365W", "B365L")]

# The columns the corpus exposes (also the CSV header order).
CORPUS_COLUMNS = [
    "date", "tour", "tournament", "surface", "best_of", "round",
    "fav_name", "dog_name", "y",
    "market_fav_prob", "elo_diff", "grass_elo_diff",
    "fav_dec", "dog_dec",
    "fav_rank", "dog_rank", "fav_pts", "dog_pts",
    "fav_grass_matches", "dog_grass_matches", "odds_source",
]
# Features the fitted model actually consumes in V1.
FEATURE_COLS = ["market_fav_prob", "elo_diff", "grass_elo_diff"]


def _fetch(tour: str, year: int):
    """Return a list of row-dicts for one tour-year, or [] on failure."""
    import pandas as pd

    url = _TOUR_URLS[tour].format(year=year)
    try:
        r = requests.get(url, headers=_HEADERS, timeout=40)
        if not r.ok:
            logger.warning("%s %s unavailable (HTTP %s)", tour, year, r.status_code)
            return []
        df = pd.read_excel(io.BytesIO(r.content))
    except Exception as exc:  # noqa: BLE001 — network/parse both non-fatal
        logger.warning("%s %s fetch failed: %s", tour, year, exc)
        return []
    return df.to_dict("records")


def _num(v):
    try:
        f = float(v)
        return f if f == f else None  # NaN → None
    except (TypeError, ValueError):
        return None


def _pick_odds(row) -> tuple[float, float, str] | None:
    """(winner_decimal, loser_decimal, source) from the first usable book."""
    for wc, lc in _ODDS_PREFERENCE:
        w, l = _num(row.get(wc)), _num(row.get(lc))
        if w and l and w > 1.0 and l > 1.0:
            return w, l, wc[:-1]  # 'PSW' → 'PS'
    return None


def _devig_winner_prob(w_dec: float, l_dec: float) -> float:
    """2-way vig-free P(the WINNER column player wins), from decimal odds."""
    pw, pl = 1.0 / w_dec, 1.0 / l_dec
    return pw / (pw + pl)


class _Elo:
    """Minimal per-tour Elo state with pre-match snapshotting (leak-free)."""

    def __init__(self):
        self.overall: dict[str, float] = {}
        self.grass: dict[str, float] = {}
        self.grass_n: dict[str, int] = {}

    def snapshot(self, key: str) -> tuple[float, float, int]:
        return (self.overall.get(key, _BASE_ELO),
                self.grass.get(key, _BASE_ELO),
                self.grass_n.get(key, 0))

    def update(self, wkey: str, lkey: str, is_grass: bool) -> None:
        ow, ol = self.overall.get(wkey, _BASE_ELO), self.overall.get(lkey, _BASE_ELO)
        exp = _expected(ow, ol)
        self.overall[wkey] = ow + _K * (1 - exp)
        self.overall[lkey] = ol - _K * (1 - exp)
        if is_grass:
            gw, gl = self.grass.get(wkey, _BASE_ELO), self.grass.get(lkey, _BASE_ELO)
            expg = _expected(gw, gl)
            self.grass[wkey] = gw + _K * (1 - expg)
            self.grass[lkey] = gl - _K * (1 - expg)
            self.grass_n[wkey] = self.grass_n.get(wkey, 0) + 1
            self.grass_n[lkey] = self.grass_n.get(lkey, 0) + 1


def _build_tour(tour: str, from_year: int, this_year: int) -> list[dict]:
    """Chronological, leak-free labeled rows for one tour."""
    import pandas as pd

    raw: list[dict] = []
    for y in range(from_year, this_year + 1):
        rows = _fetch(tour, y)
        raw.extend(rows)
        if rows:
            logger.info("%s %s: %d matches", tour, y, len(rows))

    def _date(m):
        d = m.get("Date")
        return d if isinstance(d, datetime) else datetime.max
    raw.sort(key=_date)

    elo = _Elo()
    out: list[dict] = []
    for m in raw:
        wname, lname = m.get("Winner"), m.get("Loser")
        if wname is None or lname is None:
            continue
        if isinstance(wname, float) and pd.isna(wname):
            continue
        wkey, lkey = _surname_initial_key(wname), _surname_initial_key(lname)
        if not wkey or not lkey or wkey == lkey:
            continue
        d = m.get("Date")
        if not isinstance(d, datetime):
            continue
        is_grass = str(m.get("Surface", "")).strip() == "Grass"

        # Snapshot pre-match Elo, THEN update (no leakage).
        w_ov, w_gr, w_gn = elo.snapshot(wkey)
        l_ov, l_gr, l_gn = elo.snapshot(lkey)
        elo.update(wkey, lkey, is_grass)

        odds = _pick_odds(m)
        if odds is None:
            continue  # no market anchor → unusable for a market-anchored model
        w_dec, l_dec, src = odds
        p_winner = _devig_winner_prob(w_dec, l_dec)

        # Orient by favorite = lower decimal odds (higher implied prob).
        winner_is_fav = w_dec <= l_dec
        if winner_is_fav:
            fav_name, dog_name, y = str(wname).strip(), str(lname).strip(), 1
            market_fav_prob = p_winner
            fav_dec, dog_dec = w_dec, l_dec
            elo_diff = w_ov - l_ov
            grass_elo_diff = w_gr - l_gr
            fav_gn, dog_gn = w_gn, l_gn
            fav_rank, dog_rank = _num(m.get("WRank")), _num(m.get("LRank"))
            fav_pts, dog_pts = _num(m.get("WPts")), _num(m.get("LPts"))
        else:
            fav_name, dog_name, y = str(lname).strip(), str(wname).strip(), 0
            market_fav_prob = 1.0 - p_winner
            fav_dec, dog_dec = l_dec, w_dec
            elo_diff = l_ov - w_ov
            grass_elo_diff = l_gr - w_gr
            fav_gn, dog_gn = l_gn, w_gn
            fav_rank, dog_rank = _num(m.get("LRank")), _num(m.get("WRank"))
            fav_pts, dog_pts = _num(m.get("LPts")), _num(m.get("WPts"))

        out.append({
            "date": d.date().isoformat(),
            "tour": tour,
            "tournament": str(m.get("Tournament", "") or "").strip(),
            "surface": str(m.get("Surface", "") or "").strip(),
            "best_of": _num(m.get("Best of")),
            "round": str(m.get("Round", "") or "").strip(),
            "fav_name": fav_name,
            "dog_name": dog_name,
            "y": y,
            "market_fav_prob": round(market_fav_prob, 4),
            "elo_diff": round(elo_diff, 1),
            "grass_elo_diff": round(grass_elo_diff, 1),
            "fav_dec": round(w_dec if winner_is_fav else l_dec, 3),
            "dog_dec": round(l_dec if winner_is_fav else w_dec, 3),
            "fav_rank": fav_rank,
            "dog_rank": dog_rank,
            "fav_pts": fav_pts,
            "dog_pts": dog_pts,
            "fav_grass_matches": fav_gn,
            "dog_grass_matches": dog_gn,
            "odds_source": src,
        })
    return out


def build_corpus(from_year: int = _DEFAULT_FROM,
                 tours: tuple[str, ...] = ("ATP", "WTA")):
    """Build the labeled corpus DataFrame across tours (chronological within tour)."""
    import pandas as pd

    this_year = datetime.utcnow().year
    rows: list[dict] = []
    for tour in tours:
        rows.extend(_build_tour(tour, from_year, this_year))
    df = pd.DataFrame(rows, columns=CORPUS_COLUMNS)
    return df


def _cache_path(tour_tag: str) -> Path:
    return _CACHE_DIR / f"tennis_training_{tour_tag}.csv"


def load_corpus(from_year: int = _DEFAULT_FROM,
                tours: tuple[str, ...] = ("ATP", "WTA"),
                refresh: bool = False):
    """Return the corpus, reading the cached CSV unless refresh is requested."""
    import pandas as pd

    tag = "-".join(tours).lower()
    path = _cache_path(tag)
    if path.exists() and not refresh:
        return pd.read_csv(path)
    df = build_corpus(from_year, tours)
    _CACHE_DIR.mkdir(exist_ok=True)
    df.to_csv(path, index=False)
    return df


def _summary(df) -> None:
    print(f"\nTennis training corpus: {len(df)} labeled matches")
    for tour, g in df.groupby("tour"):
        fav_wr = g["y"].mean()
        print(f"  {tour}: {len(g):6d} matches  fav_win_rate={fav_wr:.3f}  "
              f"grass={int((g['surface'] == 'Grass').sum())}  "
              f"odds={g['odds_source'].value_counts().to_dict()}")
    print(f"  date range: {df['date'].min()} to {df['date'].max()}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Build the tennis training corpus")
    parser.add_argument("--from", dest="from_year", type=int, default=_DEFAULT_FROM,
                        help=f"Start year (default {_DEFAULT_FROM})")
    parser.add_argument("--tour", choices=["ATP", "WTA"], help="Only this tour (default: both)")
    parser.add_argument("--no-cache", action="store_true", help="Build fresh, don't write cache")
    args = parser.parse_args()

    tours = (args.tour,) if args.tour else ("ATP", "WTA")
    if args.no_cache:
        df = build_corpus(args.from_year, tours)
    else:
        df = load_corpus(args.from_year, tours, refresh=True)
    _summary(df)
