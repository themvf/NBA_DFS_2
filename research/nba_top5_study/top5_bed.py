"""A large backtest bed from LAST season (2025-26 regular season), for structure questions.

Last season has no stored sportsbook lines, so each player's "line" is a pseudo-line: an
exponentially weighted average of his own earlier points this season (alpha 0.1, about a
last-20 average, which the research map measured at MAE 5.42 against 5.02 for the real line).
His "curve" (P(score above x)) is the empirical distribution of actual / pseudo-line among
EARLIER player-games with a similar pseudo-line (bands 10-15, 15-20, 20-25, 25+), so the bed
is walk-forward by construction and roughly calibrated, but with no market information.

Two simplifications, both stated: the pool holds only players who actually played that night
(the real lock has an injury report that catches 17 of 20 sitters, so this is close to, but
slightly better than, what is knowable), and there are no spreads or injury fields.

Nights have the same shape as data/nights.json, so harness.evaluate(selector, nights=bed_nights(),
history_fn=BedHistory) scores any selector here too. Use the bed for STRUCTURE (how to build five
entries from a set of distributions); use the 24 real line nights to confirm.
"""
from __future__ import annotations
import sys as _s, pathlib as _p; _s.path.insert(0, str(_p.Path(__file__).resolve().parents[0])); import _paths

import bisect
from collections import defaultdict
from functools import lru_cache

import numpy as np

import top5_harness as H

ALPHA = 0.1
MIN_GAMES = 5
DEEP = 60
BANDS = (10.0, 15.0, 20.0, 25.0, 1e9)
MAX_RATIOS = 6000


def _band(line: float) -> int:
    return bisect.bisect_right(BANDS, line) - 1   # -1 below 10


@lru_cache(maxsize=1)
def _build() -> tuple:
    logs = [r for r in H.last_season_logs() if r["season_type"] == "Regular Season" and r["minutes"] and float(r["minutes"]) > 0]
    by_date = defaultdict(list)
    for r in logs:
        by_date[str(r["game_date"])[:10]].append(r)
    state: dict[int, dict] = {}                       # player_id -> {ewma, n}
    ratios: dict[int, list[float]] = defaultdict(list)
    nights = []
    for d in sorted(by_date):
        rows = by_date[d]
        games = defaultdict(list)
        for r in rows:
            games[r["game_id"]].append(r)
        players = []
        for r in rows:
            s = state.get(r["player_id"])
            if s and s["n"] >= MIN_GAMES and s["ewma"] >= 5.0:
                players.append((s["ewma"], r))
        players.sort(key=lambda t: -t[0])
        if len(games) >= 2 and len(players) >= 20 and sum(len(v) for v in ratios.values()) > 1500:
            out = []
            for rank, (line, r) in enumerate(players[:DEEP], start=1):
                b = max(0, _band(line))
                pool = np.array(ratios[b][-MAX_RATIOS:] or ratios[max(ratios)][-MAX_RATIOS:])
                xs = np.arange(0, int(np.ceil(line * 3.2)) + 1, 1.0)
                surv = [(float(x), float((pool * line > x).mean())) for x in xs]
                curve = [(x, p) for x, p in surv if 0.0005 < p < 0.9995]
                g = games[r["game_id"]]
                home = next((x["team_abbreviation"] for x in g if x["is_home"]), None)
                away = next((x["team_abbreviation"] for x in g if not x["is_home"]), None)
                key = f"p{r['player_id']}"
                out.append({"key": key, "name": r["name"], "eventId": str(r["game_id"]), "home": home or "", "away": away or "",
                            "team": r["team_abbreviation"], "isHome": bool(r["is_home"]), "lineRank": rank, "line": round(float(line), 2),
                            "pOver": None, "books": 6, "ladderBooks": 6, "curve": curve, "floor": None, "ceiling": None,
                            "p30": None, "p40": None, "commence": d, "capturedAt": None, "leadHours": 1.0,
                            "injuryStatus": None, "teamsSubmitted": True, "sitProbBooks": 0.0, "sitProbLead": 0.0,
                            "sitProbInjury": 0.0, "homeSpread": None, "gameTotal": None,
                            "actual": float(r["points"]), "minutes": float(r["minutes"]), "played": True, "starter": None})
            slate = [{"key": f"p{r['player_id']}", "name": r["name"], "team": r["team_abbreviation"], "points": float(r["points"]),
                      "minutes": float(r["minutes"])} for r in rows]
            nights.append({"date": d, "firstTip": d, "lock": d, "complete": True, "gamesCount": len(games), "quoted": len(players),
                           "duplicatesDropped": [], "injuryReport": None,
                           "games": [{"eventId": str(gid), "home": "", "away": "", "commence": d, "homeSpread": None, "total": None}
                                     for gid in games],
                           "players": out, "slate": slate, "v1": None, "bed": "2025-26 pseudo-lines"})
        # update state AFTER the night (walk-forward)
        for r in rows:
            s = state.setdefault(r["player_id"], {"ewma": None, "n": 0})
            pts = float(r["points"])
            if s["ewma"] is not None and s["n"] >= MIN_GAMES and s["ewma"] >= 10.0:
                ratios[_band(s["ewma"])].append(pts / s["ewma"])
            s["ewma"] = pts if s["ewma"] is None else ALPHA * pts + (1 - ALPHA) * s["ewma"]
            s["n"] += 1
    return tuple(nights)


def bed_nights(every: int = 1) -> list[dict]:
    """Last season's nights (about 150), oldest first. ``every`` thins them for quick iteration."""
    import copy
    return [copy.deepcopy(n) for i, n in enumerate(_build()) if i % every == 0]


class BedHistory:
    """History for a bed night: earlier bed nights with results (walk-forward fitting), nothing from this season."""

    def __init__(self, night: dict):
        self.date = night["date"]
        self.box = []
        self.calibration = []
        self.last_season = [r for r in H.last_season_logs() if str(r["game_date"])[:10] < self.date]
        self.last_season_teams = [r for r in H.last_season_team_logs() if str(r["game_date"])[:10] < self.date]
        self._date = self.date

    @property
    def prior_nights(self):
        return [n for n in _build() if n["date"] < self._date]


if __name__ == "__main__":
    ns = bed_nights()
    print(len(ns), ns[0]["date"], ns[-1]["date"], "players", np.mean([len(n["players"]) for n in ns]))
    rep = H.evaluate(H.line_sum_selector, label="bed line_sum", nights=bed_nights(every=1), history_fn=BedHistory, fields=500)
    s = rep["summary"]
    for m in H.METRICS:
        print(m, round(s[m]["mean"], 3))
    print({k: s[k] for k in ("top1", "top10", "top1pct")}, s["prizes_total"])
