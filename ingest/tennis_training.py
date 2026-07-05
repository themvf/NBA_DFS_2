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

Phase 1 (2026-07-04, CLAUDE.md "Tennis Recency/Fatigue Model — Spec") adds four
more point-in-time, leak-free columns for the H1/H2 backtest — NOT yet in
FEATURE_COLS (that's P2/P3's job, after a walk-forward test justifies each):
  - round_num          — ordinal encoding of the Round string (1=1st Round ... 7=Final)
  - rank_diff, pts_diff — derived from the already-present fav/dog rank & pts
  - matches_played_fav/dog — matches this player has already played in THIS
    tournament instance (Tournament name + year) before this one — a fatigue
    proxy. tennis-data.co.uk has no match-duration column for either tour (TML
    does, for ATP only, but TML has no odds so it isn't this corpus's source
    and isn't cheaply joinable without its own name-matching pass) — round-
    depth is the fatigue signal this source can actually support.
  - days_since_last_fav/dog — days since the player's previous match in the
    corpus (any tournament) — a layoff/rustiness proxy. NaN for a player's
    first-ever match in the corpus (no prior data, not "just played").
  - recency_elo_diff, form_gap — an EWMA-style Elo that decays each player's
    rating toward the base (1500) between matches (half-life configurable,
    default 180 days) before applying the same update rule as the existing
    flat Elo. form_gap = recency_elo_diff - elo_diff is the H1 test variable:
    how much recent form disagrees with career-long rating.

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

# Verified against live tennis-data.co.uk data (2026-07-04): identical small
# vocabulary on both tours. "Round Robin" (ATP/WTA Finals group stage) doesn't
# fit the knockout ordinal progression — mapped to 1 (first match of the event
# for fatigue purposes; it's never followed by a 2nd/3rd round in this data).
_ROUND_ORDER = {
    "Round Robin": 1, "1st Round": 1, "2nd Round": 2, "3rd Round": 3,
    "4th Round": 4, "Quarterfinals": 5, "Semifinals": 6, "The Final": 7,
}

# Recency-Elo half-life (days) — chosen by P2's grid search
# (model/tennis_recency_calibration.py) over candidates [60, 90, 180, 365, 730]
# on the tuning period (matches before 2022-01-01) only. 730 won, but note
# honestly: EVERY candidate underperformed flat Elo's own tuning-period
# logloss (0.6323) as a standalone rating — 730d got closest (0.6347) because
# it's the least-aggressive decay tested, not because recency-weighting beat
# the full-history baseline. H1's actual claim is narrower (does the
# form-gap subset disagree with the market in a way that pays off — that's
# P3, not this). Frozen 2026-07-04; do not re-tune without a new P2 study.
_RECENCY_HALF_LIFE_DAYS = 730.0

# Frozen H1 subset definition (P2, same run): |form_gap| >= this Elo-point
# threshold = the tuning period's 75th percentile of |form_gap| at the chosen
# half-life. Reserved-period (>= 2022-01-01) matches meeting this are P3's
# untouched test set — 8,150 qualify (3,796 ATP / 4,354 WTA), well past the
# >=200 minimum. Do not recompute this threshold after seeing P3's result.
FORM_GAP_FREEZE_THRESHOLD = 47.5

# The columns the corpus exposes (also the CSV header order).
CORPUS_COLUMNS = [
    "date", "tour", "tournament", "surface", "best_of", "round", "round_num",
    "fav_name", "dog_name", "y",
    "market_fav_prob", "elo_diff", "grass_elo_diff",
    "fav_dec", "dog_dec",
    "fav_rank", "dog_rank", "fav_pts", "dog_pts", "rank_diff", "pts_diff",
    "fav_grass_matches", "dog_grass_matches",
    "matches_played_fav", "matches_played_dog",
    "days_since_last_fav", "days_since_last_dog",
    "recency_elo_diff", "form_gap",
    "odds_source",
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


class _RecencyElo:
    """Elo variant that decays each player's rating toward _BASE_ELO between
    matches (half-life in days) before applying the same update rule as the
    flat Elo — H1's test signal (CLAUDE.md "Tennis Recency/Fatigue Model").

    Also tracks last-match-date per player globally (any tournament), which
    doubles as the days-since-last-match layoff feature — both this decay and
    that feature need the same "when did this player last play" state, so
    they're computed together rather than in two separate passes.
    """

    def __init__(self, half_life_days: float = _RECENCY_HALF_LIFE_DAYS):
        self.rating: dict[str, float] = {}
        self.last_played: dict[str, object] = {}   # key -> date of last match
        self._half_life = half_life_days

    def _decayed(self, key: str, as_of) -> float:
        """This player's rating decayed toward base as of `as_of` (pre-match)."""
        r = self.rating.get(key)
        last = self.last_played.get(key)
        if r is None or last is None:
            return _BASE_ELO
        days = (as_of - last).days
        if days <= 0:
            return r
        decay = 0.5 ** (days / self._half_life)
        return _BASE_ELO + (r - _BASE_ELO) * decay

    def days_since(self, key: str, as_of) -> float | None:
        last = self.last_played.get(key)
        return (as_of - last).days if last is not None else None

    def snapshot(self, key: str, as_of) -> float:
        return self._decayed(key, as_of)

    def update(self, wkey: str, lkey: str, as_of) -> None:
        """Decay both players to `as_of`, apply the match result, store `as_of`
        as each player's new last-played date. Call once per match, in order."""
        dw, dl = self._decayed(wkey, as_of), self._decayed(lkey, as_of)
        exp = _expected(dw, dl)
        self.rating[wkey] = dw + _K * (1 - exp)
        self.rating[lkey] = dl - _K * (1 - exp)
        self.last_played[wkey] = as_of
        self.last_played[lkey] = as_of


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
    recency = _RecencyElo()
    # (tournament name, year) -> {player_key: matches already played in this instance}
    tourney_matches: dict[tuple[str, int], dict[str, int]] = {}
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

        # Recency Elo + layoff: snapshot pre-match, THEN update.
        w_rec, l_rec = recency.snapshot(wkey, d), recency.snapshot(lkey, d)
        w_days_since, l_days_since = recency.days_since(wkey, d), recency.days_since(lkey, d)
        recency.update(wkey, lkey, d)

        # Fatigue proxy: matches already played in this tournament instance,
        # THEN increment. (Tournament name, year) — majors don't span New
        # Year's, so this is a safe per-edition key without a tournament ID.
        tkey = (str(m.get("Tournament", "") or "").strip(), d.year)
        tcount = tourney_matches.setdefault(tkey, {})
        w_matches_played, l_matches_played = tcount.get(wkey, 0), tcount.get(lkey, 0)
        tcount[wkey] = w_matches_played + 1
        tcount[lkey] = l_matches_played + 1

        round_num = _ROUND_ORDER.get(str(m.get("Round", "") or "").strip())

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
            fav_matches_played, dog_matches_played = w_matches_played, l_matches_played
            fav_days_since, dog_days_since = w_days_since, l_days_since
            recency_elo_diff = w_rec - l_rec
        else:
            fav_name, dog_name, y = str(lname).strip(), str(wname).strip(), 0
            market_fav_prob = 1.0 - p_winner
            fav_dec, dog_dec = l_dec, w_dec
            elo_diff = l_ov - w_ov
            grass_elo_diff = l_gr - w_gr
            fav_gn, dog_gn = l_gn, w_gn
            fav_rank, dog_rank = _num(m.get("LRank")), _num(m.get("WRank"))
            fav_pts, dog_pts = _num(m.get("LPts")), _num(m.get("WPts"))
            fav_matches_played, dog_matches_played = l_matches_played, w_matches_played
            fav_days_since, dog_days_since = l_days_since, w_days_since
            recency_elo_diff = l_rec - w_rec

        rank_diff = (dog_rank - fav_rank) if (fav_rank is not None and dog_rank is not None) else None
        pts_diff = (fav_pts - dog_pts) if (fav_pts is not None and dog_pts is not None) else None

        out.append({
            "date": d.date().isoformat(),
            "tour": tour,
            "tournament": str(m.get("Tournament", "") or "").strip(),
            "surface": str(m.get("Surface", "") or "").strip(),
            "best_of": _num(m.get("Best of")),
            "round": str(m.get("Round", "") or "").strip(),
            "round_num": round_num,
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
            "rank_diff": rank_diff,
            "pts_diff": pts_diff,
            "fav_grass_matches": fav_gn,
            "dog_grass_matches": dog_gn,
            "matches_played_fav": fav_matches_played,
            "matches_played_dog": dog_matches_played,
            "days_since_last_fav": fav_days_since,
            "days_since_last_dog": dog_days_since,
            "recency_elo_diff": round(recency_elo_diff, 1),
            "form_gap": round(recency_elo_diff - elo_diff, 1),
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

    # Phase 1 columns: NaN rate (coverage) + a plausibility spot-check per tour.
    print("\n  Phase 1 columns (coverage + plausibility):")
    for tour, g in df.groupby("tour"):
        nan_pct = lambda col: f"{g[col].isna().mean() * 100:.1f}%"  # noqa: E731
        print(f"  {tour}: round_num NaN={nan_pct('round_num')}  "
              f"rank_diff NaN={nan_pct('rank_diff')}  pts_diff NaN={nan_pct('pts_diff')}  "
              f"days_since_last_fav NaN={nan_pct('days_since_last_fav')} "
              f"(expect >0% - a player's first corpus match has no prior date)")
        print(f"       matches_played_fav mean={g['matches_played_fav'].mean():.2f} "
              f"max={int(g['matches_played_fav'].max())}  "
              f"recency_elo_diff vs elo_diff corr={g['recency_elo_diff'].corr(g['elo_diff']):.3f} "
              f"(expect high but <1.0 - same signal, decayed)  "
              f"form_gap std={g['form_gap'].std():.1f}")


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
