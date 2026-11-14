"""Showdown field-ownership model (`nfl-ownership-field-showdown-v2`).

Owner's decision 2026-11-13: replace the per-player fitted Showdown ownership
(`nfl-ownership-fit-showdown-v1`) with a model of how the field builds whole
lineups. Every legal lineup among the slate's top players gets a value (what
the crowd expects its six seats to score, a pull toward spending the salary
cap, a quarterback-stack bonus); the field picks lineups with probability
proportional to exp(value / tau); captain and flex ownership are what falls
out. Captain and flex therefore always sum to 100% and 500%, and a player's
two shares can never pass 100% together.

The crowd's points are our own pre-lock projection, with three stand-ins where
ours and the crowd's differ most (measured against a consensus source on the
training slates): a kicker is read off his team's Vegas points, a defense off
the opponent's, and once the books' lines are in, a player without a full set
of them is scaled down. Our projections themselves are not changed.

v2 (owner's decision 2026-11-13) adds the low-end term: a player the crowd
expects to score under `low_end.threshold` points loses `low_end.penalty` of
seat value for each point under it. v1 valued a seat in a straight line, so a
one-point player was only a little worse than a four-point one and his low
price, which frees salary for stars, earned him 3-10% of lineups; the field
plays such players about 1%. Which players are kept and which lineups survive
the trim still go by plain crowd points.

Pure: plain dicts in, percentages out. `research/nfl_showdown_field_model.py`
fits the numbers and writes them to web/src/data, where
web/src/lib/nfl-dfs/showdown-field-model.ts applies them; the shared fixture in
tests/fixtures pins the two languages together.
"""
from __future__ import annotations

import itertools

import numpy as np

VERSION = "nfl-ownership-field-showdown-v2"
POSITIONS = ("QB", "RB", "WR", "TE", "K", "DST")
#: Players the lineups are built from: the slate's top N by crowd points.
TOP_PLAYERS = 24
#: Lineups kept after the plausibility trim.
KEEP_LINEUPS = 80_000
SALARY_CAP = 50_000
#: A lineup leaving more than this unspent is not one the field builds.
MAX_UNSPENT = 6_000
CAPTAIN_MULTIPLIER = 1.5
#: The trim ranks lineups by points with this charge per $1,000 unspent.
TRIM_UNSPENT_PER_1K = 0.5
#: A captain at one of these positions with his own quarterback in the flex is a stack.
STACK_CAPTAINS = ("WR", "TE")
#: Team points assumed when a kicker or defense row carries no Vegas total.
DEFAULT_IMPLIED_TOTAL = 21.0
#: Players with a full set of sportsbook lines a slate needs before a missing set counts against anyone.
#: 12 since 2026-11-13 (was 6): two days before a game ten players had a set and five starters did not yet,
#: and they were scaled down as backups. The eight training slates had 12 to 17.
MIN_FULL_ANCHORS = 12
MIN_CROWD_POINTS = 0.1

_COMBOS: dict[int, np.ndarray] = {}


def _combos(n: int) -> np.ndarray:
    """Every 5-subset of range(n), in lexicographic order."""
    if n not in _COMBOS:
        _COMBOS[n] = np.array(list(itertools.combinations(range(n), 5)), dtype=np.int64).reshape(-1, 5)
    return _COMBOS[n]


def eligible(player: dict) -> bool:
    return (not player.get("is_out") and player.get("position") in POSITIONS and (player.get("pts") or 0) > 0
            and (player.get("salary") or 0) > 0 and (player.get("captain_salary") or 0) > 0)


def has_lines(players: list[dict]) -> bool:
    """Whether the books' player lines are in on this slate. Until they are, nobody
    has a full set and the lack of one says nothing about a player."""
    return sum(1 for p in players if eligible(p) and p.get("anchor") == "full") >= MIN_FULL_ANCHORS


def crowd_points(player: dict, model: dict, lined: bool = True) -> float:
    """What the crowd expects the player to score (see the module note)."""
    position = player["position"]
    if position == "K":
        total = player.get("team_implied")
        value = model["kicker"]["intercept"] + model["kicker"]["slope"] * (DEFAULT_IMPLIED_TOTAL if total is None else float(total))
    elif position == "DST":
        total = player.get("opponent_implied")
        value = model["defense"]["intercept"] + model["defense"]["slope"] * (DEFAULT_IMPLIED_TOTAL if total is None else float(total))
    elif not lined or player.get("anchor") == "full":
        value = float(player["pts"])
    else:
        value = float(player["pts"]) * model["fringe"]["td_only" if player.get("anchor") == "td_only" else "none"]
    return max(value, MIN_CROWD_POINTS)


def build(players: list[dict], model: dict) -> dict | None:
    """The top players and every plausible lineup among them, or None when the
    slate cannot form one (fewer than six eligible players, or a single team).

    `players` is the whole pool; the returned `index` maps each kept player back
    to his row in it."""
    lined = has_lines(players)
    rows = [(i, crowd_points(p, model, lined)) for i, p in enumerate(players) if eligible(p)]
    rows.sort(key=lambda r: -r[1])          # stable: ties keep pool order
    rows = rows[:TOP_PLAYERS]
    n = len(rows)
    if n < 6:
        return None
    index = np.array([i for i, _ in rows], dtype=np.int64)
    pts = np.array([v for _, v in rows], dtype=np.float64)
    top = [players[i] for i in index]
    salary = np.array([float(p["salary"]) for p in top])
    captain_salary = np.array([float(p["captain_salary"]) for p in top])
    position = np.array([POSITIONS.index(p["position"]) for p in top], dtype=np.int64)
    team_a = np.array([p.get("team") == top[0].get("team") for p in top], dtype=np.int64)
    # Each team's quarterback: its highest crowd-points QB among the kept players.
    quarterback: dict = {}
    for k, p in enumerate(top):
        if p["position"] == "QB" and p.get("team") not in quarterback:
            quarterback[p.get("team")] = k
    base = _combos(n - 1)
    caps, flexes, lefts, stacks = [], [], [], []
    for c in range(n):
        others = np.array([k for k in range(n) if k != c], dtype=np.int64)
        combos = others[base]
        spent = salary[combos].sum(axis=1) + captain_salary[c]
        side = team_a[combos].sum(axis=1) + team_a[c]
        ok = (spent <= SALARY_CAP) & (SALARY_CAP - spent <= MAX_UNSPENT) & (side >= 1) & (side <= 5)
        combos = combos[ok]
        q = quarterback.get(top[c].get("team"))
        stacked = (combos == q).any(axis=1) if (q is not None and top[c]["position"] in STACK_CAPTAINS) else np.zeros(len(combos), bool)
        caps.append(np.full(len(combos), c, dtype=np.int64)); flexes.append(combos)
        lefts.append((SALARY_CAP - spent[ok]) / 1000.0); stacks.append(stacked.astype(np.float64))
    cap, flex, left, stack = np.concatenate(caps), np.concatenate(flexes), np.concatenate(lefts), np.concatenate(stacks)
    if not len(cap):
        return None
    if len(cap) > KEEP_LINEUPS:
        # Keep the most plausible: highest crowd points less a charge for unspent
        # salary; ties go to the earlier lineup, so both languages keep the same set.
        trim = pts[flex[:, 0]] + pts[flex[:, 1]] + pts[flex[:, 2]] + pts[flex[:, 3]] + pts[flex[:, 4]] \
            + CAPTAIN_MULTIPLIER * pts[cap] - TRIM_UNSPENT_PER_1K * left
        keep = np.sort(np.lexsort((np.arange(len(trim)), -trim))[:KEEP_LINEUPS])
        cap, flex, left, stack = cap[keep], flex[keep], left[keep], stack[keep]
    return {"index": index, "pts": pts, "position": position,
            "cap": cap, "flex": flex, "left": left, "stack": stack, "n": n}


def seat_points(points, model: dict):
    """Crowd points as a seat is valued: every point under `low_end.threshold`
    costs an extra `low_end.penalty`. Points at or above it are unchanged."""
    points = np.asarray(points, dtype=np.float64)
    return points - model["low_end"]["penalty"] * np.maximum(0.0, model["low_end"]["threshold"] - points)


def seat_values(built: dict, model: dict) -> tuple[np.ndarray, np.ndarray]:
    """(flex seat value, captain seat value) per kept player.

    There is no separate value for a player's salary: a captain costs exactly 1.5x
    his flex price, so a per-dollar seat value adds up to a per-dollar value of the
    whole lineup's spend, which `unused_salary` already is."""
    base = seat_points(built["pts"], model)
    flex_offset = np.array([model["flex_offsets"][p] for p in POSITIONS])
    captain_offset = np.array([model["captain_offsets"][p] for p in POSITIONS])
    return base + flex_offset[built["position"]], CAPTAIN_MULTIPLIER * base + captain_offset[built["position"]]


def shares(built: dict, model: dict) -> tuple[np.ndarray, np.ndarray]:
    """(captain %, flex %) per kept player."""
    flex_value, captain_value = seat_values(built, model)
    flex = built["flex"]
    utility = flex_value[flex[:, 0]] + flex_value[flex[:, 1]] + flex_value[flex[:, 2]] + flex_value[flex[:, 3]] + flex_value[flex[:, 4]] \
        + captain_value[built["cap"]] + model["unused_salary"] * built["left"] + model["qb_stack"] * built["stack"]
    weight = np.exp((utility - utility.max()) / model["tau"])
    weight /= weight.sum()
    captain = np.bincount(built["cap"], weights=weight, minlength=built["n"]) * 100.0
    flex_share = np.bincount(flex.reshape(-1), weights=np.repeat(weight, 5), minlength=built["n"]) * 100.0
    return captain, flex_share


def predict(model: dict, players: list[dict]) -> list[tuple[float, float]] | None:
    """(captain %, flex %) for every row of `players`, 0 for anyone outside the
    kept players; None when the slate cannot form a lineup."""
    built = build(players, model)
    if built is None:
        return None
    captain, flex = shares(built, model)
    out = [(0.0, 0.0)] * len(players)
    for k, i in enumerate(built["index"]):
        out[int(i)] = (float(captain[k]), float(flex[k]))
    return out
