"""NFL betting scenarios: moneyline, spread and total by situation, as chart images.

The NFL counterpart of the college scenario charts (model/cfb_scenario_charts.py).
Every regular-season game with a closing moneyline is assigned a favorite (the
shorter moneyline), then graded three ways: favorite and underdog on the
moneyline at the closing price, favorite and underdog against the spread at -110,
and over and under at -110. The season to date is compared with 2023-25 pooled.

Source: nflverse's schedule file, the same file ingest/nfl_season_schedule.py
reads. It carries the closing moneylines, spread and total for every game, plus
rest days, division flag, roof and kickoff time, so no database read is needed.
`spread_line` is the home team's expected margin (positive: home favored).

Descriptive, season to date: no edge is claimed, and small groups are faded on
the charts rather than hidden. Things the college version has and this does not:
line movement (nflverse keeps the close only) and halftime score (no quarter
scores in the file).

    python -m model.nfl_market_scenarios --out-dir artifacts/nfl_market_scenarios
"""

from __future__ import annotations

import argparse
import io
import math
import time
from collections import defaultdict
from pathlib import Path

from model import cfb_pattern_csv as cc
from model import cfb_pattern_measures as measures
from model import cfb_pattern_watch as pw

VERSION = "nfl-market-scenarios-v1"
SCHEDULE_URL = "https://github.com/nflverse/nflverse-data/releases/download/schedules/games.csv"
PRIOR_SEASONS = (2023, 2024, 2025)
DEFAULT_DIR = Path("artifacts") / "nfl_market_scenarios"

SPREAD_BANDS = (
    ("Spread 0 to 2.5", lambda s: s <= 2.5),
    ("Spread 3 to 3.5", lambda s: 2.5 < s <= 3.5),
    ("Spread 4 to 6.5", lambda s: 3.5 < s <= 6.5),
    ("Spread 7 to 9.5", lambda s: 6.5 < s <= 9.5),
    ("Spread 10 or more", lambda s: s > 9.5),
)
TOTAL_BANDS = (
    ("Total under 41", lambda t: t < 41.0),
    ("Total 41 to 44.5", lambda t: 41.0 <= t < 45.0),
    ("Total 45 to 47.5", lambda t: 45.0 <= t < 48.0),
    ("Total 48 or more", lambda t: t >= 48.0),
)
PRICE_BANDS = (   # the favorite's closing American price
    ("-105 to -150", lambda a: a >= -150),
    ("-151 to -200", lambda a: -200 <= a < -150),
    ("-201 to -300", lambda a: -300 <= a < -200),
    ("-301 to -500", lambda a: -500 <= a < -300),
    ("Beyond -500", lambda a: a < -500),
)
HOME_DOG_PRICES = (
    ("Home underdog +100 to +150", lambda a: a <= 150),
    ("Home underdog +151 to +250", lambda a: 150 < a <= 250),
    ("Home underdog +251 to +400", lambda a: 250 < a <= 400),
    ("Home underdog +401 or more", lambda a: a > 400),
)
_LADDER = tuple(label.removeprefix("Spread ") for label, _ in SPREAD_BANDS)
HOME_LADDER = tuple(f"Home favored by {b}" for b in reversed(_LADDER)) + tuple(f"Home underdog by {b}" for b in _LADDER)
VENUES = ("Home favorite", "Away favorite", "Neutral site")
DIVISION = ("Division game", "Non-division game")
KICKOFFS = ("Thursday", "Sunday before 4 pm", "Sunday 4 pm window", "Sunday night", "Monday", "Other day")
PHASES = ("Weeks 1 to 4", "Weeks 5 to 9", "Weeks 10 to 13", "Weeks 14 to 18")
ROOFS = ("Indoors", "Outdoors")
RESTS = ("Same rest", "Favorite had more rest", "Underdog had more rest", "Short week")
LAST_RESULTS = ("Favorite won last game", "Favorite lost last game")
MORE_REST_DAYS = 3        # a bye shows up as about a week more rest
SHORT_WEEK_DAYS = 5       # Thursday after Sunday is 4 days


def _num(v):
    if v is None:
        return None
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    return None if math.isnan(x) else x


def fetch_schedule(attempts: int = 3) -> list[dict]:
    import pandas as pd
    import requests

    last = None
    for attempt in range(attempts):
        try:
            r = requests.get(SCHEDULE_URL, timeout=60)
            r.raise_for_status()
            return pd.read_csv(io.StringIO(r.text)).to_dict("records")
        except Exception as exc:  # noqa: BLE001 - retried, then raised
            last = exc
            time.sleep(2 ** attempt)
    raise RuntimeError(f"nflverse schedule download failed: {last}")


def build_games(rows: list[dict]) -> list[dict]:
    """Completed regular-season games in the college module's game shape, plus NFL context.

    `last_margin` is each team's margin in its previous game of the same season,
    so week 1 has none.
    """
    played = []
    for r in rows:
        if r.get("game_type") != "REG":
            continue
        hs, as_ = _num(r.get("home_score")), _num(r.get("away_score"))
        if hs is None or as_ is None:
            continue
        played.append(r)
    played.sort(key=lambda r: (r["season"], str(r.get("gameday")), str(r.get("gametime"))))
    last: dict[tuple, int] = {}
    out = []
    for r in played:
        season = int(r["season"])
        hs, as_ = int(r["home_score"]), int(r["away_score"])
        spread = _num(r.get("spread_line"))
        hml, aml = _num(r.get("home_moneyline")), _num(r.get("away_moneyline"))
        out.append({
            "season": season, "week": int(r["week"]), "game_id": r.get("game_id"),
            "home": r["home_team"], "away": r["away_team"], "home_score": hs, "away_score": as_,
            # College convention: the home team's own line, negative when it gives points.
            "home_spread": None if spread is None else -spread,
            "home_ml": None if hml is None else int(hml), "away_ml": None if aml is None else int(aml),
            "vegas_total": _num(r.get("total_line")),
            "neutral_site": r.get("location") == "Neutral",
            "div_game": bool(_num(r.get("div_game"))),
            "roof": r.get("roof"), "weekday": r.get("weekday"), "gametime": r.get("gametime"),
            "home_rest_days": _num(r.get("home_rest")), "away_rest_days": _num(r.get("away_rest")),
            "home_last_margin": last.get((season, r["home_team"])),
            "away_last_margin": last.get((season, r["away_team"])),
        })
        last[(season, r["home_team"])] = hs - as_
        last[(season, r["away_team"])] = as_ - hs
    return out


def eligible(games: list[dict]) -> list[dict]:
    """One record per game with a spread and both moneylines; same record shape as the college module.

    The favorite is the shorter moneyline; level prices and tied finals are skipped.
    """
    out = []
    for g in games:
        if g.get("home_spread") is None or g.get("home_ml") is None or g.get("away_ml") is None:
            continue
        dh, da = pw.decimal_odds(g["home_ml"]), pw.decimal_odds(g["away_ml"])
        margin = g["home_score"] - g["away_score"]
        if dh == da or margin == 0:
            continue
        home_fav = dh < da
        p_home = pw.vig_free_home(g["home_ml"], g["away_ml"])
        fav_margin = margin if home_fav else -margin
        fav_line = g["home_spread"] if home_fav else -g["home_spread"]
        total = g.get("vegas_total")
        out.append({
            "g": g, "home_fav": home_fav, "won": fav_margin > 0,
            "price": dh if home_fav else da, "imp": p_home if home_fav else 1 - p_home,
            "dog": da if home_fav else dh,
            "cover": measures._sign(fav_margin + fav_line),
            "ou": None if total is None else measures._sign(g["home_score"] + g["away_score"] - total),
        })
    return out


def _band(value, bands):
    return None if value is None else next((label for label, rule in bands if rule(float(value))), None)


def _side(r: dict, key: str, fav: bool):
    home = r["home_fav"] == fav
    return r["g"].get(f"home_{key}" if home else f"away_{key}")


def _venue(r):
    if r["g"]["neutral_site"]:
        return "Neutral site"
    return "Home favorite" if r["home_fav"] else "Away favorite"


def _kickoff(r):
    day, t = r["g"].get("weekday"), str(r["g"].get("gametime") or "")
    if day == "Thursday":
        return "Thursday"
    if day == "Monday":
        return "Monday"
    if day != "Sunday" or ":" not in t:
        return "Other day"
    minutes = int(t.split(":")[0]) * 60 + int(t.split(":")[1])   # nflverse kickoff times are Eastern
    return KICKOFFS[1] if minutes < 15 * 60 + 30 else KICKOFFS[2] if minutes < 19 * 60 else KICKOFFS[3]


def _phase(r):
    w = r["g"]["week"]
    return PHASES[0] if w <= 4 else PHASES[1] if w <= 9 else PHASES[2] if w <= 13 else PHASES[3]


def _roof(r):
    roof = r["g"].get("roof")
    return None if roof not in ("dome", "closed", "outdoors", "open") else "Indoors" if roof in ("dome", "closed") else "Outdoors"


def _rest(r):
    fav, dog = _side(r, "rest_days", True), _side(r, "rest_days", False)
    if fav is None or dog is None:
        return None
    if min(fav, dog) <= SHORT_WEEK_DAYS:
        return "Short week"
    if fav - dog >= MORE_REST_DAYS:
        return "Favorite had more rest"
    if dog - fav >= MORE_REST_DAYS:
        return "Underdog had more rest"
    return "Same rest"


def _last(r):
    m = _side(r, "last_margin", True)
    return None if not m else LAST_RESULTS[0] if m > 0 else LAST_RESULTS[1]


def _home_ladder(r):
    if r["g"]["neutral_site"]:
        return None
    band = _band(abs(r["g"]["home_spread"]), SPREAD_BANDS).removeprefix("Spread ")
    return f"Home favored by {band}" if r["home_fav"] else f"Home underdog by {band}"


def _home_dog_price(r):
    if r["g"]["neutral_site"] or r["home_fav"]:
        return None
    return _band((r["dog"] - 1) * 100, HOME_DOG_PRICES)


CUTS = {
    "venue": (VENUES, _venue),
    "division": (DIVISION, lambda r: DIVISION[0] if r["g"]["div_game"] else DIVISION[1]),
    "kickoff": (KICKOFFS, _kickoff),
    "phase": (PHASES, _phase),
    "roof": (ROOFS, _roof),
    "spread": (tuple(l for l, _ in SPREAD_BANDS), lambda r: _band(abs(r["g"]["home_spread"]), SPREAD_BANDS)),
    "total": (tuple(l for l, _ in TOTAL_BANDS), lambda r: _band(r["g"].get("vegas_total"), TOTAL_BANDS)),
    "price": (tuple(l for l, _ in PRICE_BANDS), lambda r: _band(-100 / (r["price"] - 1), PRICE_BANDS)),
    "last": (LAST_RESULTS, _last),
    "rest": (RESTS, _rest),
    "home_ladder": (HOME_LADDER, _home_ladder),
    "home_dog_price": (tuple(l for l, _ in HOME_DOG_PRICES), _home_dog_price),
}
BREAKDOWN_ORDER = (
    ("overall", "All games", None, None),
    ("by_venue", "By venue", "venue", None),
    ("by_division", "By division game", "division", None),
    ("by_kickoff", "By kickoff", "kickoff", None),
    ("by_phase", "By point in the season", "phase", None),
    ("by_roof", "Indoors or outdoors", "roof", None),
    ("by_spread", "By spread", "spread", None),
    ("by_total", "By total", "total", None),
    ("by_price", "By price", "price", None),
    ("by_last", "By last result", "last", None),
    ("by_rest", "By rest", "rest", None),
    ("by_home_ladder", "Home team ladder", "home_ladder", None),
    ("by_home_dog_price", "Home underdog price", "home_dog_price", None),
    ("x_venue_spread", "Venue and spread", "venue", "spread"),
    ("x_venue_total", "Venue and total", "venue", "total"),
    ("x_spread_total", "Spread and total", "spread", "total"),
    ("x_division_spread", "Division game and spread", "division", "spread"),
    ("x_roof_total", "Indoors or outdoors, and total", "roof", "total"),
)

# Chart titles: level name -> (file slug, title, note) and level name -> (slug, title).
SINGLE_CUTS = {
    "By venue": ("venue", "Home or away favorite", "Neutral-site games (mostly international) are their own group."),
    "By division game": ("division", "Division vs. non-division games", ""),
    "By kickoff": ("kickoff", "Kickoff window", "Eastern time; London games count as Sunday before 4 pm."),
    "By point in the season": ("phase", "Point in the season", ""),
    "Indoors or outdoors": ("roof", "Indoors or outdoors", "Indoors: domes and closed roofs."),
    "By spread": ("spread", "Size of the spread", "3 to 3.5 is its own band: 3 is the most common NFL margin."),
    "By total": ("total", "Size of the total", ""),
    "By price": ("price", "The favorite's moneyline price", ""),
    "By last result": ("last_result", "The favorite's last result", "Week 1 left out."),
    "By rest": ("rest", "Rest", "More rest: 3+ extra days, usually a bye. Short week: 5 days or fewer for either team."),
    "Home team ladder": ("home_ladder", "The home team, from big favorite to big underdog",
                         "Told from the home team's side. Neutral sites left out."),
    "Home underdog price": ("home_dog_price", "Home underdogs by price", "Told from the home team's side."),
}
GRIDS = {
    "Venue and spread": ("venue_spread", "Home or away favorite, by spread"),
    "Venue and total": ("venue_total", "Home or away favorite, by total"),
    "Spread and total": ("spread_total", "Spread and total together"),
    "Division game and spread": ("division_spread", "Division games, by spread"),
    "Indoors or outdoors, and total": ("roof_total", "Indoors or outdoors, by total"),
}
HOME_LENS = {"Home team ladder", "Home underdog price"}


def breakdown(games: list[dict]) -> dict:
    """{key: rows} in BREAKDOWN_ORDER; rows carry `group` and `column` like the college breakdown."""
    rows = eligible(games)
    tags = [{k: fn(r) for k, (_, fn) in CUTS.items()} for r in rows]
    out: dict = {}
    for key, _name, a, b in BREAKDOWN_ORDER:
        if a is None:
            out[key] = {**measures._ml_row("All games", rows), "group": None, "column": None}
        elif b is None:
            out[key] = [{**measures._ml_row(la, [r for r, t in zip(rows, tags) if t[a] == la]), "group": la, "column": None}
                        for la in CUTS[a][0]]
        else:
            out[key] = [{**measures._ml_row(f"{la}, {lb[0].lower()}{lb[1:]}",
                                            [r for r, t in zip(rows, tags) if t[a] == la and t[b] == lb]),
                         "group": la, "column": lb}
                        for la in CUTS[a][0] for lb in CUTS[b][0]]
    return out


def breakdown_table(games: list[dict]) -> list[dict]:
    """One flat table with the college table's columns, simplest cut first."""
    data = breakdown(games)
    out = []
    for level, (key, name, _a, b) in enumerate(BREAKDOWN_ORDER, start=1):
        rows = data[key]
        rows = [rows] if isinstance(rows, dict) else rows
        for r, t in zip(rows, cc.market_splits_table(rows)):
            out.append({"Level": level, "Level name": name, "Grid": b is not None,
                        "Group": r.get("group"), "Column": r.get("column"), **t})
    return out


def split_seasons(games: list[dict], season: int | None = None) -> tuple[int, list[dict], list[dict]]:
    by = defaultdict(list)
    for g in games:
        by[g["season"]].append(g)
    current = season if season is not None else max(by)
    prior = [g for s in PRIOR_SEASONS for g in by.get(s, [])]
    return current, by.get(current, []), prior


def build_charts(games: list[dict], out_dir: Path, season: int | None = None,
                 late_scores: dict | None = None) -> dict[str, bool]:
    from model import cfb_scenario_charts as charts

    current, now, prior_games = split_seasons(games, season)
    rows = breakdown_table(now)
    prior_rows = breakdown_table(prior_games)
    label = f"{PRIOR_SEASONS[0]}-{str(PRIOR_SEASONS[-1])[2:]}"
    made: dict[str, bool] = {}
    for level, (slug, _, _) in SINGLE_CUTS.items():
        prior = {r["Split"]: r for r in prior_rows if r["Level name"] == level}
        name = f"nfl_scenario_{slug}.png"
        made[name] = charts.scenario_chart(rows, prior, level, out_dir / name, cuts=SINGLE_CUTS,
                                           home_lens=HOME_LENS, prior_label=label)
    for level, (slug, _) in GRIDS.items():
        name = f"nfl_grid_{slug}.png"
        made[name] = charts.grid_chart(rows, level, out_dir / name, grids=GRIDS)
    made.update(build_story_charts(now, prior_games, rows, prior_rows, out_dir, late_scores, label))
    return made


def build_story_charts(now: list[dict], prior_games: list[dict], rows: list[dict], prior_rows: list[dict],
                       out_dir: Path, late_scores: dict | None, label: str) -> dict[str, bool]:
    """The nine reader charts (model/story_charts.py) for the season to date."""
    from model import story_charts as sc

    rec = eligible(now)
    o = lambda n: out_dir / f"nfl_story_{n}.png"   # noqa: E731
    made = {
        "nfl_story_every_game_spread.png": sc.margin_chart(rec, o("every_game_spread"), "NFL"),
        "nfl_story_every_game_total.png": sc.total_chart(rec, o("every_game_total"), "NFL"),
        "nfl_story_line_miss.png": sc.line_miss_chart(rec, o("line_miss"), "NFL"),
        "nfl_story_bankroll.png": sc.bankroll_chart(rec, o("bankroll"), "NFL"),
        "nfl_story_what_changed.png": sc.slope_chart(rows, prior_rows, o("what_changed"), "NFL", label,
                                                     exclude=HOME_LENS),
        "nfl_story_calibration.png": sc.calibration_chart(rec, eligible(prior_games), o("calibration"), "NFL", label),
        "nfl_story_team_calendar.png": sc.team_calendar_chart(rec, o("team_calendar"), "NFL"),
        "nfl_story_team_quadrant.png": sc.team_quadrant_chart(rec, o("team_quadrant"), "NFL"),
        "nfl_story_outcomes.png": sc.outcomes_chart(rec, SPREAD_BANDS, o("outcomes"), "NFL"),
    }
    flips = []
    for g in now:
        late = (late_scores or {}).get(g["game_id"])
        if late:
            flips.append({**g, "late_home": late[0], "late_away": late[1]})
    made["nfl_story_late_flips.png"] = sc.late_flips_chart(flips, o("late_flips"), "NFL")
    return made


def pbp_url(season: int) -> str:
    return f"https://github.com/nflverse/nflverse-data/releases/download/pbp/play_by_play_{season}.csv.gz"


def late_scores_from_pbp(rows: list[dict]) -> dict[str, tuple[int, int]]:
    """{game_id: (home, away)} after the last regulation play that ended with 5:00 or more left."""
    best: dict[str, tuple] = {}
    for r in rows:
        q, left = _num(r.get("qtr")), _num(r.get("game_seconds_remaining"))
        h, a, pid = _num(r.get("total_home_score")), _num(r.get("total_away_score")), _num(r.get("play_id"))
        if None in (q, left, h, a, pid) or q > 4 or left < 300:
            continue
        gid = r["game_id"]
        if gid not in best or pid > best[gid][0]:
            best[gid] = (pid, int(h), int(a))
    return {gid: (h, a) for gid, (_, h, a) in best.items()}


def fetch_late_scores(season: int, attempts: int = 3) -> dict[str, tuple[int, int]]:
    import pandas as pd

    cols = ["game_id", "play_id", "qtr", "game_seconds_remaining", "total_home_score", "total_away_score"]
    last = None
    for attempt in range(attempts):
        try:
            frame = pd.read_csv(pbp_url(season), usecols=cols, compression="gzip", low_memory=False)
            return late_scores_from_pbp(frame.to_dict("records"))
        except Exception as exc:  # noqa: BLE001 - retried, then raised
            last = exc
            time.sleep(2 ** attempt)
    raise RuntimeError(f"nflverse play-by-play {season} download failed: {last}")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--out-dir", default=str(DEFAULT_DIR))
    ap.add_argument("--season", type=int, default=None, help="season to chart (default: the latest with results)")
    args = ap.parse_args(argv)
    games = build_games(fetch_schedule())
    current, now, prior = split_seasons(games, args.season)
    graded = len(eligible(now))
    if not graded:
        raise SystemExit(f"{current}: no completed games with closing lines; nothing to chart")
    print(f"{VERSION}: {current} {graded} games graded (through week {max(g['week'] for g in now)}), "
          f"{len(eligible(prior))} in {PRIOR_SEASONS[0]}-{PRIOR_SEASONS[-1]}")
    late = fetch_late_scores(current)
    print(f"play-by-play: score with 5:00 left found for {sum(g['game_id'] in late for g in now)} of {len(now)} games")
    out = Path(args.out_dir)
    for name, ok in build_charts(games, out, args.season, late).items():
        print(f"{out / name}: {'written' if ok else 'skipped, no data'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
