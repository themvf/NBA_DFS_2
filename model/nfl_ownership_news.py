"""News-chalk trigger for `nfl-ownership-news-v1` (docs/nfl-ownership-news-chalk-study.md).

Pure: pandas only, no database. The research drivers load the inputs.

A *vacancy* is a player ruled out in the frozen forecast input who carried a
material role for his team recently; his *beneficiaries* are the active
teammates in the same room. Inputs:

- ``players``: the frozen input (one row per player) with columns
  ``ff_player_id, team, position, salary, is_out`` (and ``normalized_name``).
- ``box``: completed regular-season nflverse rows of ``ff_player_week_stats``
  with columns ``player_id, season, week, team, position, targets, carries,
  attempts`` (missing stat keys already 0; ``position`` from ``ff_players``).

Every constant below is frozen in docs/nfl-ownership-news-chalk-study.json;
tests/test_nfl_ownership_news.py pins them against that file.
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass, field

import pandas as pd

VERSION = "nfl-ownership-news-v1"

# Franchise key, as web/src/lib/nfl-dfs/availability.ts (one key across DraftKings,
# Sleeper and nflverse codes).
TEAM_ALIASES = {"LA": "LAR", "WAS": "WSH", "AZ": "ARI", "JAC": "JAX"}

APPEARANCES = 8
HALF_LIFE = 4.0
MIN_APPEARANCES = 2
RECENT_TEAM_GAMES = 3
BARS = {"RB": ("carries", 5.0), "WR": ("targets", 7.0), "TE": ("targets", 4.0)}
QB_MIN_ATTEMPTS = 15.0
ROOMS = {"RB": ("RB",), "WR": ("WR", "TE"), "TE": ("TE",), "QB": ("QB",)}
SEASONS_BACK = 1  # the slate season and the one before (2025, 2026 for a 2026 slate)


def team_key(code) -> str:
    s = str(code or "").strip().upper()
    return TEAM_ALIASES.get(s, s)


@dataclass
class Vacancy:
    ff_player_id: int
    team: str
    position: str
    baseline: float | None
    stat: str
    first_game: bool


@dataclass
class Trigger:
    vacancies: list[Vacancy] = field(default_factory=list)
    beneficiaries: set = field(default_factory=set)   # index labels of `players`
    box_checksum: str = ""
    beneficiary_checksum: str = ""

    def details(self, players: pd.DataFrame) -> dict:
        names = sorted(str(players.loc[i, "normalized_name"]) for i in self.beneficiaries) if "normalized_name" in players else []
        return {"vacancies": [v.__dict__ for v in self.vacancies], "beneficiaries": names,
                "box_checksum": self.box_checksum, "beneficiary_checksum": self.beneficiary_checksum}


def prior_box(box: pd.DataFrame, season: int, week: int) -> pd.DataFrame:
    """Rows strictly before the slate week, the slate season and SEASONS_BACK before it."""
    b = box[(box["season"] >= season - SEASONS_BACK) & ((box["season"] < season) | (box["week"] < week))].copy()
    b["team_key"] = b["team"].map(team_key)
    return b


def team_games(prior: pd.DataFrame) -> dict[str, list[tuple[int, int]]]:
    """Each team's completed games (season, week), most recent first."""
    out: dict[str, list[tuple[int, int]]] = {}
    for team, g in prior.groupby("team_key"):
        out[team] = sorted({(int(s), int(w)) for s, w in zip(g["season"], g["week"])}, reverse=True)
    return out


def baseline(prior: pd.DataFrame, ff_player_id: int, team: str, stat: str) -> float | None:
    """Half-life-4 weighted mean of `stat` over the player's last 8 appearances for `team`."""
    g = prior[(prior["player_id"] == ff_player_id) & (prior["team_key"] == team)]
    if len(g) < MIN_APPEARANCES:
        return None
    g = g.sort_values(["season", "week"], ascending=False).head(APPEARANCES)
    w = [0.5 ** (k / HALF_LIFE) for k in range(len(g))]
    vals = [float(v or 0.0) for v in g[stat]]
    return sum(a * b for a, b in zip(w, vals)) / sum(w)


def passing_leader(prior: pd.DataFrame, team: str, game: tuple[int, int]) -> int | None:
    g = prior[(prior["team_key"] == team) & (prior["season"] == game[0]) & (prior["week"] == game[1])
              & (prior["position"] == "QB") & (prior["attempts"].astype(float) >= QB_MIN_ATTEMPTS)]
    if g.empty:
        return None
    g = g.assign(_a=g["attempts"].astype(float)).sort_values(["_a", "player_id"], ascending=[False, True])
    return int(g["player_id"].iloc[0])


def _appeared(prior: pd.DataFrame, pid: int, team: str, games: list[tuple[int, int]]) -> bool:
    g = prior[(prior["player_id"] == pid) & (prior["team_key"] == team)]
    seen = {(int(s), int(w)) for s, w in zip(g["season"], g["week"])}
    return any(x in seen for x in games)


def find_trigger(players: pd.DataFrame, box: pd.DataFrame, season: int, week: int) -> Trigger:
    prior = prior_box(box, season, week)
    games = team_games(prior)
    trig = Trigger()
    vac_rooms: dict[str, set[str]] = {}
    for _, p in players[players["is_out"].astype(bool)].iterrows():
        pid = p.get("ff_player_id")
        if pid is None or (isinstance(pid, float) and math.isnan(pid)):
            continue
        pid, team, pos = int(pid), team_key(p["team"]), str(p["position"] or "").upper()
        tg = games.get(team, [])
        if pos not in ROOMS or not tg:
            continue
        if not _appeared(prior, pid, team, tg[:RECENT_TEAM_GAMES]):
            continue
        first_game = _appeared(prior, pid, team, tg[:1])
        if pos == "QB":
            if passing_leader(prior, team, tg[0]) != pid:
                continue
            vac = Vacancy(pid, team, pos, None, "attempts", first_game)
        else:
            stat, bar = BARS[pos]
            b = baseline(prior, pid, team, stat)
            if b is None or b < bar:
                continue
            vac = Vacancy(pid, team, pos, round(b, 4), stat, first_game)
        trig.vacancies.append(vac)
        vac_rooms.setdefault(team, set()).update(ROOMS[pos])
    for i, p in players.iterrows():
        if bool(p["is_out"]) or not (p.get("salary") or 0) > 0:
            continue
        room = vac_rooms.get(team_key(p["team"]))
        if room and str(p["position"] or "").upper() in room:
            trig.beneficiaries.add(i)
    cols = ["player_id", "season", "week", "team", "position", "targets", "carries", "attempts"]
    extract = prior[cols].sort_values(["player_id", "season", "week"]).astype(str).to_csv(index=False)
    trig.box_checksum = hashlib.sha256(extract.encode()).hexdigest()
    names = sorted(str(players.loc[i, "normalized_name"]) for i in trig.beneficiaries) if "normalized_name" in players else sorted(map(str, trig.beneficiaries))
    trig.beneficiary_checksum = hashlib.sha256(json.dumps(names).encode()).hexdigest()
    return trig


def candidate_frame(players: pd.DataFrame, beneficiaries: set) -> pd.DataFrame:
    """C1/C2 input: a beneficiary with a projection drafts on the projection alone."""
    df = players.copy()
    mask = df.index.isin(list(beneficiaries)) & df["proj"].notna()
    df.loc[mask, "dk_avg"] = None
    return df


def weights(players: pd.DataFrame, beneficiaries: set, m: float) -> list[float]:
    """Per-row allocation-weight multiplier (C2): m for beneficiaries, 1 otherwise."""
    return [m if i in beneficiaries else 1.0 for i in players.index]
