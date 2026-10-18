"""Instrument measurements for the CFB pattern watch (`cfb-pattern-measures-v1`).

All four are DESCRIPTIVE. None gates a verdict, none changes a trigger, and
none touches how a ledger row is graded. They exist so the study can see its
own instrument:

1. weekly_base_rates   the effect the favourite triggers rest on, measured
                       across EVERY FBS game, week by week: favourite
                       straight-up rate minus the vig-free implied rate,
                       favourite ATS, overs, and the share of games decided by
                       7 or fewer. If the excess sits near zero, T1/T5/T6
                       should be expected to drift toward their kill criteria.
2. game_counts         independent games behind the rows. One game can feed
                       up to three triggers; rows overstate the evidence.
3. freeze_vs_close     the publication record (frozen selection) against the
                       verified close: points and price moved toward or away
                       from the pick, and whether grading at the frozen line
                       would have changed the result.
4. board events        stale refusals and late-move flags, appended once per
                       (trigger, game, status), plus one line per board run so
                       a run that flagged nothing is recorded as zero rather
                       than as silence.
"""

from __future__ import annotations

import json
import statistics
from collections import defaultdict
from datetime import datetime
from pathlib import Path

from model import cfb_pattern_watch as pw

MEASURES_VERSION = "cfb-pattern-measures-v1"
BASE = Path("artifacts") / "cfb_pattern_watch"
EVENTS_PATH = BASE / "events.jsonl"
RUNS_PATH = BASE / "board_runs.jsonl"
BASE_RATES_PATH = BASE / "base_rates.json"

_BASE_SQL = """
SELECT m.id AS matchup_id, m.week, m.season_type, m.home_score, m.away_score,
       m.commence_time, m.home_team_id, m.away_team_id, m.conference_game,
       m.home_line_scores, m.away_line_scores,
       ht.conference AS hconf, at.conference AS aconf, ht.name AS hname, at.name AS aname,
       COALESCE(h.home_spread, m.home_spread) AS home_spread,
       COALESCE(h.home_ml, m.home_ml) AS home_ml,
       COALESCE(h.away_ml, m.away_ml) AS away_ml,
       COALESCE(h.vegas_total, m.vegas_total) AS vegas_total,
       m.neutral_site, ht.classification AS hclass, at.classification AS aclass
FROM cfb_matchups m
JOIN cfb_teams ht ON ht.team_id = m.home_team_id
JOIN cfb_teams at ON at.team_id = m.away_team_id
LEFT JOIN event_closing_lines e ON e.sport = 'cfb' AND e.matchup_id = m.id AND e.quality IN ('A', 'B')
LEFT JOIN game_odds_history h ON h.id = e.history_id
WHERE m.season = %s AND m.completed AND m.home_score IS NOT NULL AND m.away_score IS NOT NULL
"""


# ── 1. weekly base rates ─────────────────────────────────────────────────────
def weekly_base_rates(games: list[dict]) -> list[dict]:
    """Per CFBD week, FBS-vs-FBS games with a spread and both moneylines."""
    by_week: dict[int, list[dict]] = defaultdict(list)
    for g in games:
        if g.get("hclass") != "fbs" or g.get("aclass") != "fbs":
            continue
        if g.get("home_spread") is None or g.get("home_ml") is None or g.get("away_ml") is None:
            continue
        if (g.get("season_type") or "regular") != "regular":
            continue
        by_week[int(g["week"])].append(g)
    out = []
    for week in sorted(by_week):
        gs = by_week[week]
        su, implied, ats, over = [], [], [], []
        close = 0
        for g in gs:
            sp = g["home_spread"]
            home_fav = sp < 0 if sp != 0 else g["home_ml"] < g["away_ml"]
            margin = g["home_score"] - g["away_score"]
            fm = margin if home_fav else -margin
            p = pw.vig_free_home(g["home_ml"], g["away_ml"])
            implied.append(p if home_fav else 1 - p)
            if fm != 0:
                su.append(fm > 0)
            cover = fm - abs(sp)
            if cover != 0:
                ats.append(cover > 0)
            if g.get("vegas_total") is not None:
                tot = g["home_score"] + g["away_score"]
                if tot != g["vegas_total"]:
                    over.append(tot > g["vegas_total"])
            close += abs(margin) <= 7
        fav_su = sum(su) / len(su) if su else None
        imp = statistics.mean(implied)
        out.append({
            "week": week, "games": len(gs),
            "fav_su": fav_su, "implied": imp,
            "excess_pp": None if fav_su is None else (fav_su - imp) * 100,
            "fav_ats": sum(ats) / len(ats) if ats else None,
            "over": sum(over) / len(over) if over else None,
            "within_7": close / len(gs),
        })
    return out


# Moneyline-favorite splits: descriptive, whole-market, not study triggers.
# Each split is (label, rule over a game dict). "Total" is the closing O/U.
MARKET_SPLITS = (
    ("All moneyline favorites", lambda g: True),
    ("Total 45 or higher", lambda g: g.get("vegas_total") is not None and float(g["vegas_total"]) >= 45.0),
    ("Spread 2.5 or less", lambda g: abs(float(g["home_spread"])) <= 2.5),
    ("Spread 7 to 13.5", lambda g: 7.0 <= abs(float(g["home_spread"])) <= 13.5),
    ("Spread 14 or more", lambda g: abs(float(g["home_spread"])) >= 14.0),
)


SPREAD_PRICE = 1 + 100 / 110   # spread and total bets graded at -110


def _sign(x: float) -> int:
    return (x > 0) - (x < 0)


def _ml_eligible(games: list[dict]) -> list[dict]:
    """One record per FBS-vs-FBS regular-season game with a spread and both moneylines.

    The favorite is the side with the shorter moneyline; a game priced level on
    both sides has no favorite and is skipped, as is a tied final. Each record
    carries the moneyline result plus the favorite's spread result (`cover`:
    +1 covered, 0 push, -1 not) and the total result (`ou`: +1 over, 0 push,
    -1 under, None without a total).
    """
    eligible = []
    for g in games:
        if g.get("hclass") != "fbs" or g.get("aclass") != "fbs":
            continue
        if g.get("home_spread") is None or g.get("home_ml") is None or g.get("away_ml") is None:
            continue
        if (g.get("season_type") or "regular") != "regular":
            continue
        dh, da = pw.decimal_odds(g["home_ml"]), pw.decimal_odds(g["away_ml"])
        if dh == da:
            continue
        margin = g["home_score"] - g["away_score"]
        if margin == 0:
            continue
        home_fav = dh < da
        p_home = pw.vig_free_home(g["home_ml"], g["away_ml"])
        fav_margin = margin if home_fav else -margin
        # The moneyline favorite's own spread (negative when it gives points).
        fav_line = float(g["home_spread"]) if home_fav else -float(g["home_spread"])
        total = g.get("vegas_total")
        eligible.append({
            "g": g, "home_fav": home_fav, "won": fav_margin > 0,
            "price": dh if home_fav else da, "imp": p_home if home_fav else 1 - p_home,
            "dog": da if home_fav else dh,
            "cover": _sign(fav_margin + fav_line),
            "ou": None if total is None else _sign(g["home_score"] + g["away_score"] - float(total)),
        })
    return eligible


def _ml_row(label: str, picked: list[dict]) -> dict:
    """Favorite and underdog in the same games: moneyline at the close, spread and total at -110."""
    n = len(picked)
    wins = sum(1 for r in picked if r["won"])
    units = sum((r["price"] - 1) if r["won"] else -1.0 for r in picked)
    dog_units = sum(-1.0 if r["won"] else (r["dog"] - 1) for r in picked)
    imp = statistics.mean(r["imp"] for r in picked) if picked else None
    win_rate = wins / n if n else None
    covers = sum(1 for r in picked if r["cover"] > 0)
    no_covers = sum(1 for r in picked if r["cover"] < 0)
    with_total = [r for r in picked if r["ou"] is not None]
    overs = sum(1 for r in with_total if r["ou"] > 0)
    unders = sum(1 for r in with_total if r["ou"] < 0)
    nt = len(with_total)
    win_u = SPREAD_PRICE - 1
    return {
        "split": label, "games": n, "wins": wins, "losses": n - wins,
        "win_rate": win_rate, "implied": imp,
        "excess_pp": None if win_rate is None else (win_rate - imp) * 100,
        "units": units, "roi": units / n if n else None,
        "median_price": statistics.median(r["price"] for r in picked) if picked else None,
        "dog_units": dog_units, "dog_roi": dog_units / n if n else None,
        "dog_median_price": statistics.median(r["dog"] for r in picked) if picked else None,
        # Spread: the moneyline favorite against its own line. Pushes return the stake.
        "fav_covers": covers, "dog_covers": no_covers, "ats_pushes": n - covers - no_covers,
        "cover_rate": covers / (covers + no_covers) if covers + no_covers else None,
        "fav_ats_units": covers * win_u - no_covers, "dog_ats_units": no_covers * win_u - covers,
        "fav_ats_roi": (covers * win_u - no_covers) / n if n else None,
        "dog_ats_roi": (no_covers * win_u - covers) / n if n else None,
        # Total: games without a total are left out of these columns.
        "total_games": nt, "overs": overs, "unders": unders, "total_pushes": nt - overs - unders,
        "over_rate": overs / (overs + unders) if overs + unders else None,
        "over_units": overs * win_u - unders, "under_units": unders * win_u - overs,
        "over_roi": (overs * win_u - unders) / nt if nt else None,
        "under_roi": (unders * win_u - overs) / nt if nt else None,
    }


def market_splits(games: list[dict], splits=MARKET_SPLITS) -> list[dict]:
    """Moneyline favorites' and underdogs' record and flat-unit ROI at the closing moneyline.

    Same population as weekly_base_rates. One unit on the favorite: a win pays
    its closing decimal price minus 1, a loss costs 1; the underdog the reverse.
    """
    eligible = _ml_eligible(games)
    return [_ml_row(label, [r for r in eligible if rule(r["g"])]) for label, rule in splits]


# ── market breakdown: every game, then single cuts, then two-way grids ───────
# Each cut assigns a game at most one label. A game a cut cannot place (no total,
# no early line, first game of the season) is left out of that cut only.
SPREAD_BANDS = (
    ("Spread 0 to 2.5", lambda s: s <= 2.5),
    ("Spread 3 to 6.5", lambda s: 2.5 < s <= 6.5),
    ("Spread 7 to 13.5", lambda s: 6.5 < s <= 13.5),
    ("Spread 14 or more", lambda s: s > 13.5),
)
TOTAL_BANDS = (
    ("Total under 45", lambda t: t < 45.0),
    ("Total 45 to 54.5", lambda t: 45.0 <= t < 55.0),
    ("Total 55 or more", lambda t: t >= 55.0),
)
PRICE_BANDS = (   # the favorite's closing American price
    ("-105 to -150", lambda a: a >= -150),
    ("-151 to -250", lambda a: -250 <= a < -150),
    ("-251 to -500", lambda a: -500 <= a < -250),
    ("-501 to -1000", lambda a: -1000 <= a < -500),
    ("Beyond -1000", lambda a: a < -1000),
)
VENUES = ("Home favorite", "Away favorite", "Neutral site")
CONFERENCE = ("Conference game", "Non-conference game")
TIERS = ("Power vs Power", "Power vs Group of Five", "Group of Five vs Group of Five")
KICKOFFS = ("Saturday before 2 pm", "Saturday 2 to 5 pm", "Saturday 5 pm or later", "Not Saturday")
RESTS = ("Same rest", "Favorite had more rest", "Underdog had more rest", "Short week")
LAST_RESULTS = ("Favorite won last game", "Favorite lost last game")
MOVES = ("Moved toward favorite", "Held within 2 points", "Moved toward underdog")
HALVES = ("Favorite led at half", "Tied at half", "Favorite trailed at half")
_LADDER_BANDS = ("0 to 2.5", "3 to 6.5", "7 to 13.5", "14 or more")
# The home team's line, from big home favorite to big home underdog. Neutral sites are left out.
HOME_LADDER = tuple(f"Home favored by {b}" for b in reversed(_LADDER_BANDS)) + tuple(f"Home underdog by {b}" for b in _LADDER_BANDS)
HOME_DOG_PRICES = (   # the home underdog's closing American price
    ("Home underdog +100 to +150", lambda a: a <= 150),
    ("Home underdog +151 to +300", lambda a: 150 < a <= 300),
    ("Home underdog +301 to +600", lambda a: 300 < a <= 600),
    ("Home underdog +601 or more", lambda a: a > 600),
)
# Notre Dame is the one independent priced and scheduled like a Power team.
POWER_INDEPENDENTS = frozenset({"Notre Dame"})
MORE_REST_DAYS = 3        # a bye shows up as roughly a week more rest
SHORT_WEEK_DAYS = 5
MOVE_PP = 2.0             # favorite's vig-free probability, open to close
OPEN_MIN_HOURS = 12       # an "open" must be at least this far before kickoff


def _band(value: float | None, bands) -> str | None:
    if value is None:
        return None
    return next((label for label, rule in bands if rule(float(value))), None)


def _venue(r: dict) -> str:
    """Where the favorite played. Neutral-site games get their own group, so home and away stay literal."""
    if r["g"].get("neutral_site"):
        return "Neutral site"
    return "Home favorite" if r["home_fav"] else "Away favorite"


def _conference(r: dict) -> str | None:
    c = r["g"].get("conference_game")
    return None if c is None else ("Conference game" if c else "Non-conference game")


def _is_power(conf: str | None, name: str | None) -> bool:
    return conf in pw.P4 or name in POWER_INDEPENDENTS


def _tier(r: dict) -> str | None:
    g = r["g"]
    if g.get("hconf") is None or g.get("aconf") is None:
        return None
    powers = _is_power(g["hconf"], g.get("hname")) + _is_power(g["aconf"], g.get("aname"))
    return TIERS[2 - powers]


def _kickoff(r: dict) -> str | None:
    ts = r["g"].get("commence_time")
    if ts is None:
        return None
    et = pw._eastern(ts)   # the study's own Eastern conversion
    if et.strftime("%a") != "Sat":
        return "Not Saturday"
    minutes = et.hour * 60 + et.minute
    return KICKOFFS[0] if minutes < 14 * 60 else KICKOFFS[1] if minutes < 17 * 60 else KICKOFFS[2]


def _price_band(r: dict) -> str:
    american = -100 / (r["price"] - 1)
    return _band(american, PRICE_BANDS)


def _fav_side(r: dict, key: str):
    return r["g"].get(f"home_{key}" if r["home_fav"] else f"away_{key}")


def _dog_side(r: dict, key: str):
    return r["g"].get(f"away_{key}" if r["home_fav"] else f"home_{key}")


def _rest(r: dict) -> str | None:
    fav, dog = _fav_side(r, "rest_days"), _dog_side(r, "rest_days")
    if fav is None or dog is None:
        return None
    if min(fav, dog) <= SHORT_WEEK_DAYS:
        return "Short week"
    if fav - dog >= MORE_REST_DAYS:
        return "Favorite had more rest"
    if dog - fav >= MORE_REST_DAYS:
        return "Underdog had more rest"
    return "Same rest"


def _last_result(r: dict) -> str | None:
    last = _fav_side(r, "last_margin")
    if last is None or last == 0:
        return None
    return LAST_RESULTS[0] if last > 0 else LAST_RESULTS[1]


def _move(r: dict) -> str | None:
    g = r["g"]
    if g.get("open_home_ml") is None or g.get("open_away_ml") is None:
        return None
    p_open = pw.vig_free_home(g["open_home_ml"], g["open_away_ml"])
    p_open = p_open if r["home_fav"] else 1 - p_open
    diff = (r["imp"] - p_open) * 100
    return MOVES[0] if diff >= MOVE_PP else MOVES[2] if diff <= -MOVE_PP else MOVES[1]


def _half(r: dict) -> str | None:
    fav, dog = _fav_side(r, "line_scores"), _dog_side(r, "line_scores")
    if not fav or not dog or len(fav) < 2 or len(dog) < 2:
        return None
    d = (fav[0] + fav[1]) - (dog[0] + dog[1])
    return HALVES[0] if d > 0 else HALVES[1] if d == 0 else HALVES[2]


def _home_ladder(r: dict) -> str | None:
    if r["g"].get("neutral_site"):
        return None
    band = _band(abs(float(r["g"]["home_spread"])), SPREAD_BANDS).removeprefix("Spread ")
    return f"Home favored by {band}" if r["home_fav"] else f"Home underdog by {band}"


def _home_dog_price(r: dict) -> str | None:
    if r["g"].get("neutral_site") or r["home_fav"]:
        return None
    return _band((r["dog"] - 1) * 100, HOME_DOG_PRICES)


# (key, labels in display order, labeller)
CUTS = {
    "venue": (VENUES, _venue),
    "conference": (CONFERENCE, _conference),
    "tier": (TIERS, _tier),
    "kickoff": (KICKOFFS, _kickoff),
    "spread": (tuple(l for l, _ in SPREAD_BANDS), lambda r: _band(abs(float(r["g"]["home_spread"])), SPREAD_BANDS)),
    "total": (tuple(l for l, _ in TOTAL_BANDS), lambda r: _band(r["g"].get("vegas_total"), TOTAL_BANDS)),
    "price": (tuple(l for l, _ in PRICE_BANDS), _price_band),
    "move": (MOVES, _move),
    "last": (LAST_RESULTS, _last_result),
    "rest": (RESTS, _rest),
    "half": (HALVES, _half),
    "home_ladder": (HOME_LADDER, _home_ladder),
    "home_dog_price": (tuple(l for l, _ in HOME_DOG_PRICES), _home_dog_price),
}
# Display order, simplest first. Keys starting with "by_" are one cut; "x_" two.
BREAKDOWN_ORDER = (
    ("overall", "All games", None, None),
    ("by_venue", "By venue", "venue", None),
    ("by_conference", "By conference game", "conference", None),
    ("by_tier", "By matchup tier", "tier", None),
    ("by_kickoff", "By kickoff", "kickoff", None),
    ("by_spread", "By spread", "spread", None),
    ("by_total", "By total", "total", None),
    ("by_price", "By price", "price", None),
    ("by_move", "By line movement", "move", None),
    ("by_last", "By last result", "last", None),
    ("by_rest", "By rest", "rest", None),
    ("by_home_ladder", "Home team ladder", "home_ladder", None),
    ("by_home_dog_price", "Home underdog price", "home_dog_price", None),
    ("by_half", "By halftime score", "half", None),
    ("x_venue_spread", "Venue and spread", "venue", "spread"),
    ("x_venue_total", "Venue and total", "venue", "total"),
    ("x_spread_total", "Spread and total", "spread", "total"),
    ("x_tier_spread", "Tier and spread", "tier", "spread"),
    ("x_kickoff_tier", "Kickoff and tier", "kickoff", "tier"),
)


def market_breakdown(games: list[dict]) -> dict:
    """Favorites and underdogs (moneyline, spread, total), from simplest cut to the most detailed.

    Returns {key: rows} in BREAKDOWN_ORDER; every row carries `group` (its first
    label) and `column` (the second label, for two-way grids).
    """
    eligible = _ml_eligible(games)
    tags = [{k: fn(r) for k, (_, fn) in CUTS.items()} for r in eligible]
    out: dict = {}
    for key, _name, a, b in BREAKDOWN_ORDER:
        if a is None:
            out[key] = {**_ml_row("All games", eligible), "group": None, "column": None}
        elif b is None:
            out[key] = [{**_ml_row(la, [r for r, t in zip(eligible, tags) if t[a] == la]), "group": la, "column": None}
                        for la in CUTS[a][0]]
        else:
            out[key] = [{**_ml_row(f"{la}, {lb[0].lower()}{lb[1:]}",
                                   [r for r, t in zip(eligible, tags) if t[a] == la and t[b] == lb]),
                         "group": la, "column": lb}
                        for la in CUTS[a][0] for lb in CUTS[b][0]]
    return out


def attach_context(games: list[dict], open_lines: dict[int, tuple] | None = None) -> list[dict]:
    """Add each side's rest days and previous margin (from the season's schedule) and the early line.

    Rest and last result come from each team's previous completed game in
    `games`, by kickoff time, against any opponent. A team's first game has none.
    """
    by_team: dict[int, list[dict]] = defaultdict(list)
    for g in games:
        if g.get("commence_time") is None:
            continue
        by_team[g.get("home_team_id")].append(g)
        by_team[g.get("away_team_id")].append(g)
    prev: dict[tuple[int, int], dict] = {}
    for team, gs in by_team.items():
        gs.sort(key=lambda x: x["commence_time"])
        for before, g in zip(gs, gs[1:]):
            prev[(id(g), team)] = before
    for g in games:
        for side in ("home", "away"):
            team = g.get(f"{side}_team_id")
            p = prev.get((id(g), team))
            if p is None:
                g[f"{side}_rest_days"] = g[f"{side}_last_margin"] = None
                continue
            g[f"{side}_rest_days"] = (pw._eastern(g["commence_time"]).date() - pw._eastern(p["commence_time"]).date()).days
            m = p["home_score"] - p["away_score"]
            g[f"{side}_last_margin"] = m if p.get("home_team_id") == team else -m
        o = (open_lines or {}).get(g.get("matchup_id"))
        g["open_home_ml"], g["open_away_ml"] = o if o else (None, None)
    return games


_OPEN_SQL = """
SELECT DISTINCT ON (h.matchup_id) h.matchup_id, h.home_ml, h.away_ml
FROM game_odds_history h
JOIN cfb_matchups m ON m.id = h.matchup_id
WHERE h.sport = 'cfb' AND m.season = %s AND h.home_ml IS NOT NULL AND h.away_ml IS NOT NULL
  AND h.captured_at <= m.commence_time - make_interval(hours => %s)
ORDER BY h.matchup_id, h.captured_at
"""


def load_base_rate_games(db) -> list[dict]:
    games = [dict(r) for r in db.execute(_BASE_SQL, (pw.SEASON,))]
    opens = {r["matchup_id"]: (r["home_ml"], r["away_ml"]) for r in db.execute(_OPEN_SQL, (pw.SEASON, OPEN_MIN_HOURS))}
    return attach_context(games, opens)


def write_base_rates(rates: list[dict], path: Path = BASE_RATES_PATH, now: datetime | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"measures_version": MEASURES_VERSION,
                                "generated_at": now.isoformat() if now else None,
                                "weeks": rates}, indent=2), encoding="utf-8")


def read_base_rates(path: Path = BASE_RATES_PATH) -> list[dict]:
    try:
        return json.loads(path.read_text(encoding="utf-8")).get("weeks", [])
    except (OSError, ValueError):
        return []


def format_base_rates(rates: list[dict]) -> list[str]:
    lines = ["  week games  fav SU  implied  excess  fav ATS  over  within 7"]
    for r in rates:
        f = lambda v: "   -  " if v is None else f"{v:6.1%}"
        ex = "   -  " if r["excess_pp"] is None else f"{r['excess_pp']:+6.1f}"
        lines.append(f"  {r['week']:>4} {r['games']:>5}  {f(r['fav_su'])}  {f(r['implied'])}  {ex}  {f(r['fav_ats'])}  {f(r['over'])}  {f(r['within_7'])}")
    return lines


# ── 2. independent games ─────────────────────────────────────────────────────
def game_counts(rows: list[dict]) -> dict:
    by_game: dict[int, set] = defaultdict(set)
    for r in rows:
        by_game[int(r["matchup_id"])].add(r["trigger"])
    per_trigger = defaultdict(set)
    for r in rows:
        per_trigger[r["trigger"]].add(int(r["matchup_id"]))
    multi = sum(1 for t in by_game.values() if len(t) > 1)
    return {
        "rows": len(rows),
        "games": len(by_game),
        "games_with_multiple_triggers": multi,
        "max_triggers_per_game": max((len(t) for t in by_game.values()), default=0),
        "per_trigger_games": {t: len(g) for t, g in per_trigger.items()},
    }


# ── 3. frozen selection vs close ─────────────────────────────────────────────
def _points_clv(trigger: str, freeze: dict, close: dict) -> float | None:
    """Points gained (+) or given up (-) by the frozen line relative to the close, from the pick's side."""
    if trigger in ("T2_fcs_fav_spread", "T6_mid_fav_spread"):
        if freeze.get("home_spread") is None or close.get("home_spread") is None:
            return None
        return abs(close["home_spread"]) - abs(freeze["home_spread"])  # laid fewer points than the close: +
    if trigger == "T3_small_fav_dog_spread":
        if freeze.get("home_spread") is None or close.get("home_spread") is None:
            return None
        return abs(freeze["home_spread"]) - abs(close["home_spread"])  # took more points than the close: +
    if trigger == "T4_big_fav_over":
        if freeze.get("vegas_total") is None or close.get("vegas_total") is None:
            return None
        return close["vegas_total"] - freeze["vegas_total"]  # bought the over lower than the close: +
    return None  # moneyline: price only


def freeze_vs_close(rows: list[dict], selections: list[dict]) -> dict:
    graded = {(r["trigger"], int(r["matchup_id"])): r for r in rows}
    items = []
    for s in selections:
        r = graded.get((s["trigger"], int(s["matchup_id"])))
        if r is None:
            continue
        frozen_game = dict(r, home_spread=s["home_spread"], vegas_total=s["vegas_total"],
                           home_ml=s["home_ml"], away_ml=s["away_ml"])
        fg = pw.grade(s["trigger"], frozen_game)
        br = s.get("best_retail") or {}
        br_pnl = None
        if fg["result"] == "won" and br.get("price") is not None:
            br_pnl = round(pw.decimal_odds(br["price"]) - 1, 4)
        elif fg["result"] == "lost":
            br_pnl = -1.0
        elif fg["result"] == "push":
            br_pnl = 0.0
        lead_min = None
        try:
            lead_min = round((datetime.fromisoformat(s["commence_time"]) - datetime.fromisoformat(s["frozen_at"])).total_seconds() / 60)
        except (KeyError, TypeError, ValueError):
            pass
        items.append({
            "trigger": s["trigger"], "matchup_id": int(s["matchup_id"]), "away": s["away"], "home": s["home"],
            "selection": s["selection"], "lead_minutes": lead_min,
            "freeze_spread": s["home_spread"], "close_spread": r["home_spread"],
            "freeze_total": s["vegas_total"], "close_total": r["vegas_total"],
            "points_clv": _points_clv(s["trigger"], s, r),
            "price_clv": round(s["consensus_decimal"] / r["price"] - 1, 4) if s.get("consensus_decimal") and r.get("price") else None,
            "close_result": r["result"], "close_pnl": r["pnl_units"],
            "freeze_result": fg["result"], "freeze_pnl": fg["pnl_units"],
            "best_retail_book": br.get("book"), "best_retail_pnl": br_pnl,
        })
    pts = [i["points_clv"] for i in items if i["points_clv"] is not None]
    prc = [i["price_clv"] for i in items if i["price_clv"] is not None]
    leads = [i["lead_minutes"] for i in items if i["lead_minutes"] is not None]
    summary = {
        "n": len(items),
        "result_changed": sum(i["freeze_result"] != i["close_result"] for i in items),
        "mean_points_clv": statistics.mean(pts) if pts else None,
        "points_better": sum(p > 0 for p in pts), "points_worse": sum(p < 0 for p in pts),
        "mean_price_clv": statistics.mean(prc) if prc else None,
        "median_lead_minutes": statistics.median(leads) if leads else None,
        "close_units": sum(i["close_pnl"] for i in items),
        "freeze_units": sum(i["freeze_pnl"] for i in items),
        "best_retail_units": sum(i["best_retail_pnl"] for i in items if i["best_retail_pnl"] is not None),
    }
    return {"summary": summary, "items": items}


# ── 4. board events ──────────────────────────────────────────────────────────
EVENT_STATUSES = ("stale", "watch", "dropped")


def read_event_keys(path: Path = EVENTS_PATH) -> set[tuple[str, int, str]]:
    keys = set()
    if path.exists():
        for line in path.open(encoding="utf-8"):
            if line.strip():
                e = json.loads(line)
                keys.add((e["trigger"], int(e["matchup_id"]), e["status"]))
    return keys


def new_events(cards: list[dict], existing: set[tuple[str, int, str]], now: datetime) -> list[dict]:
    out = []
    for c in cards:
        if c["status"] not in EVENT_STATUSES:
            continue
        key = (c["trigger"], int(c["matchup_id"]), c["status"])
        if key in existing:
            continue
        existing.add(key)
        out.append({
            "measures_version": MEASURES_VERSION, "at": now.isoformat(), "status": c["status"],
            "trigger": c["trigger"], "matchup_id": int(c["matchup_id"]), "away": c["away"], "home": c["home"],
            "commence_time": c["commence_time"].isoformat() if hasattr(c["commence_time"], "isoformat") else c["commence_time"],
            "minutes_since_capture": c.get("minutes_since_capture"),
            "late_reasons": c.get("late_reasons") or [],
            "had_frozen_selection": c.get("frozen") is not None,
        })
    return out


def run_record(cards: list[dict], new_selections: int, new_event_count: int, now: datetime) -> dict:
    counts: dict[str, int] = {}
    for c in cards:
        counts[c["status"]] = counts.get(c["status"], 0) + 1
    for s in ("current", "final", "watch", "dropped", "stale", "started"):
        counts.setdefault(s, 0)
    return {"measures_version": MEASURES_VERSION, "run_at": now.isoformat(), "cards": len(cards),
            "status_counts": counts, "new_selections": new_selections, "new_events": new_event_count}


def append_jsonl(path: Path, rows: list[dict]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        for r in rows:
            fh.write(json.dumps(r, default=str, sort_keys=True) + "\n")


def read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return [json.loads(l) for l in path.open(encoding="utf-8") if l.strip()]


def event_summary(events: list[dict], runs: list[dict]) -> dict:
    by_status: dict[str, int] = {}
    for e in events:
        by_status[e["status"]] = by_status.get(e["status"], 0) + 1
    return {"runs": len(runs), "last_run": runs[-1]["run_at"] if runs else None,
            "events": len(events), "by_status": by_status}
