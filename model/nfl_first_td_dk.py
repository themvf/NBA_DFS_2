"""DraftKings' first-TD prices as a forecast, saved and graded beside ours (``nfl-first-td-dk-v1``).

docs/nfl-first-td-dk-anchor.md. A tracked test, not a bet finder: the book's margin is taken out of
its own prices, so no player can price above DraftKings by this number. The question it answers is
whether a number read off the book forecasts the first touchdown better than our model does.

Reading (frozen; a change is a new version):
  - DraftKings' latest ``player_1st_td`` capture before the forecast is saved. "No Touchdown" is
    dropped (our forecasts are conditional on a touchdown being scored).
  - Each price's implied chance is divided by the sum over the remaining list (players and both
    D/ST entries), so the list sums to 1. D/ST entries are the return-or-defense share.
  - UNLISTED_SHARE of the listed players' share is held back for a player DraftKings does not list.
  - A listed player is matched to our candidates by (first initial, surname), then to the season's
    roster; one we cannot identify goes to "everyone else" on offense.
Proportional scaling leaves long shots a little high (books put more margin on them); that is part
of what the test measures.
"""
from __future__ import annotations

import math
import random
from collections import defaultdict

from model import cfb_first_td as cf
from model import nfl_first_td_market as mk

VERSION_DK = "nfl-first-td-dk-v1"
UNLISTED_SHARE = 0.01
MIN_PLAYERS = 12  # a thinner list is a partial capture, not a market
NO_TD = "no touchdown"
TEAM_ALIASES = {"LAR": "LA", "WSH": "WAS", "JAC": "JAX", "AZ": "ARI"}
REVIEW_GAMES = 60
BOOTSTRAP = {"resamples": 2000, "seed": 20261116}


def _team(code: str | None) -> str | None:
    return TEAM_ALIASES.get(code, code) if code else None


def _match_candidates(cands: dict[str, dict], listed: list[str]) -> dict[str, str]:
    """DraftKings name -> our candidate id, by the market grader's rule: (first initial, surname),
    or the surname alone when it is unique on both sides."""
    by_key: dict[tuple[str, str], str] = {}
    surnames: dict[str, list[str]] = defaultdict(list)
    for name in listed:
        key = cf.name_key(name)
        if key:
            by_key[key] = name
            surnames[key[1]].append(name)
    ours: dict[str, int] = defaultdict(int)
    for c in cands.values():
        key = cf.name_key(c["name"])
        if key:
            ours[key[1]] += 1
    took: dict[str, str] = {}
    for pid, c in cands.items():
        key = cf.name_key(c["name"])
        if not key:
            continue
        name = by_key.get(key)
        if name is None and len(surnames.get(key[1], [])) == 1 and ours[key[1]] == 1:
            name = surnames[key[1]][0]
        if name is not None and name not in took:
            took[name] = pid
    return took


def distribution(prices: dict[str, float], cands: dict[str, dict], roster: list[dict], home: str, away: str) -> dict | None:
    """``prices``: DraftKings name -> American first-TD price for one game. ``cands``: our candidates
    (pid -> {name, team}). ``roster``: [{pid, name, team}] for the season. Returns {"dist": pid /
    other:home / other:away / non_offense -> chance (sums to 1), "players": pid -> {name, team,
    american}, "unmatched": [names], "p_home_first"} or None when the list is too thin to read."""
    implied = {name: 1 / mk.american_to_decimal(p) for name, p in prices.items()
               if p and name.strip().lower() != NO_TD}
    dst = {name for name in implied if name.upper().endswith("D/ST")}
    listed = [name for name in implied if name not in dst]
    total = sum(implied.values())
    if len(listed) < MIN_PLAYERS or total <= 0:
        return None
    non_offense = sum(implied[name] for name in dst) / total

    took = _match_candidates(cands, listed)
    sides = {_team(home): "home", _team(away): "away"}
    by_roster: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for r in roster:
        key = cf.name_key(r["name"])
        if key:
            by_roster[key].append(r)
    players: dict[str, dict] = {}
    chance: dict[str, float] = {}
    unmatched = []
    for name in listed:
        pid, team = took.get(name), None
        if pid:
            team = cands[pid]["team"]
        else:
            key = cf.name_key(name)
            options = by_roster.get(key, []) if key else []
            if len(options) > 1:
                options = [r for r in options if _team(r["team"]) in sides]
            if len(options) == 1 and options[0]["pid"] not in players and options[0]["pid"] not in took.values():
                pid, team = options[0]["pid"], options[0]["team"]
        if not pid:
            unmatched.append(name)
            continue
        players[pid] = {"name": cands[pid]["name"] if pid in cands else name, "team": team, "american": prices[name]}
        chance[pid] = implied[name] / total

    scale = 1 - UNLISTED_SHARE
    dist = {pid: v * scale for pid, v in chance.items()}
    side_mass = {"home": 0.0, "away": 0.0}
    for pid, v in dist.items():
        side = sides.get(_team(players[pid]["team"]))
        if side:
            side_mass[side] += v
    rest = 1 - non_offense - sum(dist.values())  # the held-back share plus players we could not identify
    known = side_mass["home"] + side_mass["away"]
    for side in ("home", "away"):
        dist[f"other:{side}"] = rest * (side_mass[side] / known if known > 0 else 0.5)
    dist["non_offense"] = non_offense
    return {"dist": dist, "players": players, "unmatched": sorted(unmatched),
            "p_home_first": (side_mass["home"] / known) if known > 0 else 0.5}


def row(g: dict, read: dict, captured_at, cands: dict[str, dict], top: int) -> dict:
    """A nfl_first_td_predictions row for the DraftKings reading of one game."""
    dist, players = read["dist"], read["players"]
    ranked = sorted(players, key=lambda pid: -dist[pid])[:top]
    return {
        "game_id": g["game_id"], "season": g["season"], "week": g["week"], "away": g["away"], "home": g["home"],
        "commence_time": g["commence_time"], "game": f"{g['away']} @ {g['home']}",
        "implied": [g["away_implied"], g["home_implied"]], "p_home_first": read["p_home_first"],
        "players": [{"name": players[pid]["name"], "team": players[pid]["team"], "prob": dist[pid],
                     "fair": cf.fair_american(dist[pid]), "td": cands.get(pid, {}).get("td", 0),
                     "touches": round(cands.get(pid, {}).get("vol", 0), 1), "chips": [],
                     "dk": players[pid]["american"], "dk_prob": dist[pid], "listed": None} for pid in ranked],
        "other_offense": dist["other:home"] + dist["other:away"], "non_offense": dist["non_offense"],
        "dist": dist, "availability": None,
        "context": {"team_chips": {"home": [], "away": []}, "source": "draftkings_first_td_prices",
                    "dk_captured_at": captured_at.isoformat() if captured_at else None,
                    "unmatched": read["unmatched"]},
    }


def load_roster(db, season: int) -> list[dict]:
    return [{"pid": r["gsis_id"], "name": r["canonical_name"], "team": r["team_abbrev"]} for r in db.execute(
        "SELECT gsis_id, canonical_name, team_abbrev FROM ff_players WHERE season = %s AND gsis_id IS NOT NULL", (season,))]


def _log_loss(prob: float) -> float:
    return -math.log(max(float(prob), 1e-9))


def compare(pairs: list[dict]) -> dict:
    """``pairs``: one per game graded for both versions, {ours, dk, ours_top, dk_top} (chance given to
    what happened; whether the top pick scored). Log loss, lower is better; ``diff`` is ours minus
    DraftKings', so a positive value means the DraftKings reading was closer."""
    n = len(pairs)
    out: dict = {"version": VERSION_DK, "games": n, "review_games": REVIEW_GAMES}
    if not n:
        return out
    diffs = [_log_loss(p["ours"]) - _log_loss(p["dk"]) for p in pairs]
    out.update(ours_log_loss=sum(_log_loss(p["ours"]) for p in pairs) / n,
               dk_log_loss=sum(_log_loss(p["dk"]) for p in pairs) / n, diff=sum(diffs) / n,
               ours_top=sum(bool(p["ours_top"]) for p in pairs), dk_top=sum(bool(p["dk_top"]) for p in pairs),
               interval=None, status="descriptive until the review" if n < REVIEW_GAMES else "review")
    if n >= REVIEW_GAMES:
        rng = random.Random(BOOTSTRAP["seed"])
        means = sorted(sum(rng.choice(diffs) for _ in range(n)) / n for _ in range(BOOTSTRAP["resamples"]))
        out["interval"] = [means[int(0.025 * len(means))], means[int(0.975 * len(means)) - 1]]
    return out


def load_pairs(db, ours_version: str) -> list[dict]:
    try:
        rows = db.execute("""
            SELECT a.prob AS ours, b.prob AS dk, a.top_pick_hit AS ours_top, b.top_pick_hit AS dk_top
            FROM nfl_first_td_grades a JOIN nfl_first_td_grades b ON b.game_id = a.game_id AND b.version = %s
            WHERE a.version = %s ORDER BY a.kickoff, a.game_id""", (VERSION_DK, ours_version))
    except Exception:  # noqa: BLE001 -- table not created yet
        return []
    return [dict(r) for r in rows]
