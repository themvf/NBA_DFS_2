"""College football first touchdown scorer: our own model, before any sportsbook price is read.

UNVALIDATED. Version ``cfb-first-td-v1``. Nothing here reads a first-TD market;
comparing with sportsbook prices is a later, separate step.

The first touchdown of a game is split into three questions:

  1. Is it an offensive touchdown (a run or a catch) or something else (an
     interception, fumble, kick or punt return, blocked kick)? The "something
     else" share PHI is the historical frequency, ~7%, and is not given to any
     player.
  2. Which team scores it? Each team's scoring rate is its implied total from
     the closing spread and total: P(home) = H^K / (H^K + A^K). K = 1 is a
     plain race between two scoring rates; K is fitted on 2022-2024 first
     touchdowns and checked on 2025-2026.
  3. Which player on that team? Each candidate gets a score
         (TDs + KAPPA * RHO * touches), this season plus W_PRIOR times last season,
     where touches are carries + catches and RHO is the league's TDs per touch.
     KAPPA pulls a player's few touchdowns toward what his volume would earn.
     Candidates are the players who appeared for the team in either of its last
     two games; a share THETA of the team's offensive first TDs is left to
     everyone else (freshmen, players back from injury, the unlisted).

     P(player) = P(team) * (1 - PHI) * (1 - THETA) * score / team score total

Labels come from the first touchdown play in ``cfb_plays`` (2022+). The scorer
is read from the play text and matched to the box score of the same game
(``cfb_player_game_stats``, 2024+): only players on the scoring team who scored
that kind of touchdown in that game are eligible, and when exactly one player
did, he is the scorer whatever the text says.

Everything is computed from games before the target game, so the backtest is
walk-forward. Who actually plays is not known before kickoff and is not used.

    python -m model.cfb_first_td backtest
    python -m model.cfb_first_td slate --date 2026-11-07
    python -m model.cfb_first_td publish            # saves the next 40 hours of games for the web page

Publishing appends a run to cfb_first_td_runs and one row per game to
cfb_first_td_predictions. Games that have kicked off are never written, so the
latest row written before a game's kickoff is its frozen pregame forecast.
"""
from __future__ import annotations

import argparse
import json
import math
import re
import unicodedata
from collections import defaultdict
from dataclasses import dataclass, replace
from pathlib import Path

from config import load_config
from db.database import DatabaseManager

VERSION = "cfb-first-td-v1"
SUFFIXES = {"jr", "sr", "ii", "iii", "iv", "v"}
OFFENSIVE_TYPES = {"Rushing Touchdown": "rush", "Rush": "rush", "Passing Touchdown": "rec",
                   "Pass Reception": "rec", "Pass Completion": "rec"}
FIT_SEASONS_TEAM = (2022, 2023, 2024)
FIT_SEASON_PLAYER = 2025
TEST_SEASON = 2026
PUBLISH_HOURS = 40
PUBLISH_TOP = 10
DDL = [
    """CREATE TABLE IF NOT EXISTS cfb_first_td_runs (
        id BIGSERIAL PRIMARY KEY, version TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        params JSONB NOT NULL, games INTEGER NOT NULL)""",
    """CREATE TABLE IF NOT EXISTS cfb_first_td_predictions (
        run_id BIGINT NOT NULL REFERENCES cfb_first_td_runs(id), cfbd_game_id BIGINT NOT NULL,
        season INTEGER NOT NULL, week INTEGER, kickoff TIMESTAMPTZ NOT NULL,
        away TEXT NOT NULL, home TEXT NOT NULL, away_implied DOUBLE PRECISION NOT NULL,
        home_implied DOUBLE PRECISION NOT NULL, p_home_first DOUBLE PRECISION NOT NULL,
        other_offense DOUBLE PRECISION NOT NULL, non_offense DOUBLE PRECISION NOT NULL,
        players JSONB NOT NULL, history_through JSONB NOT NULL,
        PRIMARY KEY (run_id, cfbd_game_id))""",
    "CREATE INDEX IF NOT EXISTS idx_cfb_first_td_predictions_game ON cfb_first_td_predictions (cfbd_game_id, run_id DESC)",
    "CREATE INDEX IF NOT EXISTS idx_cfb_first_td_predictions_kickoff ON cfb_first_td_predictions (kickoff)",
]


@dataclass(frozen=True)
class Params:
    k: float = 1.0          # team power on implied totals
    phi: float = 0.07       # share of first TDs that are not a run or a catch
    theta: float = 0.10     # share of a team's offensive first TDs left to non-candidates
    kappa: float = 1.0      # weight of volume-expected TDs against actual TDs
    w_prior: float = 0.3    # weight of last season against this season
    rho: float = 0.03       # league TDs per touch, set from history


# Fitted by ``backtest`` (artifacts/cfb_first_td/backtest_v1.json): K and PHI on 2022-24, the rest on 2025.
FITTED = Params(k=1.2, phi=0.0419271791099669, theta=0.05, kappa=4.0, w_prior=0.0)


# ── names ────────────────────────────────────────────────────────────────────
def _tokens(name: str) -> list[str]:
    text = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode().lower()
    return [t for t in re.split(r"[^a-z0-9]+", text) if t and t not in SUFFIXES]


def name_key(name: str) -> tuple[str, str] | None:
    """(first initial, surname): 'J.Denman', 'W. Parker' and 'Jaylen Denman Jr.' all reduce to this."""
    t = _tokens(name)
    if len(t) < 2:
        return None
    return t[0][0], t[-1]


_NAME = r"([A-Z][A-Za-z.'\-]*(?:\s?[A-Z][A-Za-z.'\-]*)*)"
_PATTERNS = {
    "rush": [re.compile(r"#\d+\s+" + _NAME + r"\s+rush\b"),
             re.compile(r"^" + _NAME + r"\s+\d+\s+Yd\s+Run\b"),
             re.compile(r"^" + _NAME + r"\s+run for\b")],
    "rec": [re.compile(r"\bto\s+#\d+\s+" + _NAME + r"\s+(?:caught|for)\b"),
            re.compile(r"^" + _NAME + r"\s+\d+\s+Yd\s+pass from\b"),
            re.compile(r"pass complete to\s+" + _NAME + r"\s+for\b")],
}


def parse_scorer(text: str, kind: str) -> str | None:
    for pattern in _PATTERNS.get(kind, []):
        m = pattern.search(text or "")
        if m:
            return m.group(1).strip()
    return None


def resolve_scorer(text: str, kind: str, candidates: list[dict]) -> tuple[str | None, str]:
    """The box-score player who scored, from those on the scoring team with a TD of this kind.

    Returns (player id or None, method)."""
    field = "rush_td" if kind == "rush" else "rec_td"
    eligible = [c for c in candidates if (c.get(field) or 0) > 0]
    if len(eligible) == 1:
        return eligible[0]["pid"], "only_scorer"
    parsed = parse_scorer(text, kind)
    key = name_key(parsed) if parsed else None
    if key and eligible:
        by_surname = [c for c in eligible if name_key(c["name"]) and name_key(c["name"])[1] == key[1]]
        if len(by_surname) > 1:
            by_surname = [c for c in by_surname if name_key(c["name"])[0] == key[0]]
        if len(by_surname) == 1:
            return by_surname[0]["pid"], "name"
    return None, "unresolved" if eligible else "no_box_scorer"


# ── probabilities ────────────────────────────────────────────────────────────
def team_probability(home_implied: float, away_implied: float, k: float) -> float:
    h, a = max(home_implied, 0.5) ** k, max(away_implied, 0.5) ** k
    return h / (h + a)


def player_scores(cands: dict[str, dict], p: Params) -> dict[str, float]:
    return {pid: (c["td"] + p.w_prior * c["td_prior"]) + p.kappa * p.rho * (c["vol"] + p.w_prior * c["vol_prior"])
            for pid, c in cands.items()}


def game_distribution(p_home: float, home: dict[str, dict], away: dict[str, dict], p: Params) -> dict[str, float]:
    """Every outcome of the first TD with its probability; the values sum to 1.

    Keys: player ids, 'other:home', 'other:away' (an offensive TD by a non-candidate) and 'non_offense'."""
    out = {"non_offense": p.phi}
    for side, p_team, cands in (("home", p_home, home), ("away", 1 - p_home, away)):
        scores = player_scores(cands, p)
        total = sum(scores.values())
        offense = p_team * (1 - p.phi)
        if total <= 0:
            out[f"other:{side}"] = offense
            continue
        out[f"other:{side}"] = offense * p.theta
        for pid, s in scores.items():
            out[pid] = out.get(pid, 0.0) + offense * (1 - p.theta) * s / total
    return out


def fair_american(prob: float) -> int:
    if prob <= 0:
        return 0
    if prob >= 0.5:
        return -round(100 * prob / (1 - prob))
    return round(100 * (1 - prob) / prob)


# ── history ──────────────────────────────────────────────────────────────────
def _order(season: int, season_type: str, week: int) -> tuple[int, int]:
    return season, (100 if season_type == "postseason" else 0) + (week or 0)


class History:
    """Box-score rows indexed so a game can be scored using only earlier games."""

    def __init__(self, rows: list[dict]):
        self.games_by_team: dict[str, list[tuple[tuple[int, int], int]]] = defaultdict(list)
        self.rows_by_game_team: dict[tuple[int, str], list[dict]] = defaultdict(list)
        self.rows_by_player: dict[str, list[dict]] = defaultdict(list)
        self.names: dict[str, str] = {}
        seen = set()
        for r in rows:
            r["order"] = _order(r["season"], r["season_type"], r["week"])
            if (r["team"], r["cfbd_game_id"]) not in seen:
                seen.add((r["team"], r["cfbd_game_id"]))
                self.games_by_team[r["team"]].append((r["order"], r["cfbd_game_id"]))
            self.rows_by_game_team[(r["cfbd_game_id"], r["team"])].append(r)
            self.rows_by_player[r["pid"]].append(r)
            self.names[r["pid"]] = r["name"]
        for games in self.games_by_team.values():
            games.sort()

    def rho(self, before: tuple[int, int]) -> float:
        td = vol = 0.0
        for rows in self.rows_by_player.values():
            for r in rows:
                if r["order"] < before:
                    td += r["rush_td"] + r["rec_td"]
                    vol += r["rush_att"] + r["receptions"]
        return td / vol if vol else 0.03

    def candidates(self, team: str, before: tuple[int, int]) -> dict[str, dict]:
        recent = [gid for order, gid in self.games_by_team.get(team, []) if order < before][-2:]
        pids = {r["pid"] for gid in recent for r in self.rows_by_game_team[(gid, team)]}
        season = before[0]
        out = {}
        for pid in pids:
            c = {"td": 0.0, "vol": 0.0, "td_prior": 0.0, "vol_prior": 0.0, "name": self.names[pid], "team": team}
            for r in self.rows_by_player[pid]:
                if r["order"] >= before:
                    continue
                td, vol = r["rush_td"] + r["rec_td"], r["rush_att"] + r["receptions"]
                if r["season"] == season:
                    c["td"] += td
                    c["vol"] += vol
                elif r["season"] == season - 1:
                    c["td_prior"] += td
                    c["vol_prior"] += vol
            if c["td"] + c["vol"] + c["td_prior"] + c["vol_prior"] > 0:
                out[pid] = c
        return out


# ── data ─────────────────────────────────────────────────────────────────────
def load_box(db: DatabaseManager) -> list[dict]:
    rows = db.execute("""
        SELECT cfbd_game_id, cfbd_player_id AS pid, player_name AS name, team, season, week, season_type,
               COALESCE(rush_att, 0) AS rush_att, COALESCE(rush_td, 0) AS rush_td,
               COALESCE(receptions, 0) AS receptions, COALESCE(rec_td, 0) AS rec_td
        FROM cfb_player_game_stats""")
    return [dict(r) for r in rows]


def load_games(db: DatabaseManager) -> list[dict]:
    """Completed games with play-by-play, closing implied totals and the first touchdown play."""
    rows = db.execute("""
        WITH lines AS (
            SELECT game_id,
                   max(home_value) FILTER (WHERE market_type = 'spread') AS spread,
                   max(home_value) FILTER (WHERE market_type = 'total') AS total
            FROM cfb_historical_game_lines
            WHERE line_designation = 'historical_reference' AND is_canonical_reference
            GROUP BY game_id),
        td AS (
            SELECT DISTINCT ON (game_id) game_id, play_type, play_text, offense_name, defense_name, period
            FROM cfb_plays
            WHERE play_type ILIKE '%%touchdown%%'
               OR (scoring AND play_text ILIKE '%%touchdown%%' AND play_type IN ('Rush', 'Pass Reception', 'Pass Completion'))
            ORDER BY game_id, period, clock_seconds_remaining DESC, drive_number, play_number)
        SELECT m.id, m.cfbd_game_id, m.season, m.season_type, m.week, m.commence_time,
               h.name AS home, a.name AS away,
               COALESCE(m.home_implied, (l.total - l.spread) / 2) AS home_implied,
               COALESCE(m.away_implied, (l.total + l.spread) / 2) AS away_implied,
               td.play_type, td.play_text, td.offense_name, td.defense_name
        FROM cfb_matchups m
        JOIN cfb_teams h ON h.team_id = m.home_team_id
        JOIN cfb_teams a ON a.team_id = m.away_team_id
        LEFT JOIN lines l ON l.game_id = m.id
        LEFT JOIN td ON td.game_id = m.id
        WHERE m.completed AND EXISTS (SELECT 1 FROM cfb_plays p WHERE p.game_id = m.id)""")
    return [dict(r) for r in rows]


def label(game: dict, history: History) -> dict:
    """Who scored the first TD: scoring side, kind, and the box-score player when he can be named."""
    pt = game.get("play_type")
    if not pt:
        return {"outcome": "no_td"}
    kind = OFFENSIVE_TYPES.get(pt)
    team = game["offense_name"] if kind else game["defense_name"]
    side = "home" if team == game["home"] else "away" if team == game["away"] else None
    if not kind:
        return {"outcome": "non_offense", "side": side}
    box = history.rows_by_game_team.get((game["cfbd_game_id"], team), [])
    if not box:
        return {"outcome": "offense", "side": side, "kind": kind, "pid": None, "method": "no_box"}
    pid, method = resolve_scorer(game["play_text"], kind, box)
    return {"outcome": "offense", "side": side, "kind": kind, "pid": pid, "method": method}


# ── evaluation ───────────────────────────────────────────────────────────────
def outcome_probability(dist: dict[str, float], lab: dict, cands: dict[str, dict]) -> float | None:
    if lab["outcome"] == "non_offense":
        return dist["non_offense"]
    if lab["outcome"] != "offense" or lab.get("side") is None or lab.get("pid") is None:
        return None
    if lab["pid"] in cands and lab["pid"] in dist:
        return dist[lab["pid"]]
    return dist[f"other:{lab['side']}"]


def fit_team_power(games: list[dict], seasons) -> tuple[float, list[tuple[float, float]]]:
    """K by maximum likelihood on which side scored the first TD (all kinds), over a grid."""
    data = [(g["home_implied"], g["away_implied"], g["_label"]["side"] == "home") for g in games
            if g["season"] in seasons and g["home_implied"] and g["away_implied"]
            and g["_label"].get("side") in ("home", "away")]
    grid = []
    for k in [x / 10 for x in range(0, 41)]:
        ll = 0.0
        for h, a, home in data:
            p = team_probability(h, a, k)
            ll -= math.log(p if home else 1 - p)
        grid.append((k, ll / len(data)))
    return min(grid, key=lambda t: t[1])[0], grid


def team_report(games: list[dict], seasons, k: float) -> dict:
    data = [(g["home_implied"], g["away_implied"], g["_label"]["side"] == "home") for g in games
            if g["season"] in seasons and g["home_implied"] and g["away_implied"]
            and g["_label"].get("side") in ("home", "away")]
    out = {"n": len(data)}
    for name, kk in (("coin", 0.0), ("k1", 1.0), ("fitted", k)):
        out[name] = sum(-math.log(team_probability(h, a, kk) if home else 1 - team_probability(h, a, kk))
                        for h, a, home in data) / len(data)
    fav = [(team_probability(h, a, k) >= 0.5) == home for h, a, home in data]
    out["favorite_scored_first"] = sum(fav) / len(fav)
    out["mean_favorite_probability"] = sum(max(team_probability(h, a, k), 1 - team_probability(h, a, k))
                                           for h, a, _ in data) / len(data)
    return out


def scored_games(games: list[dict], history: History, season: int) -> list[dict]:
    """Games of ``season`` with lines, a usable label and pregame candidates for both teams."""
    out = []
    for g in games:
        if g["season"] != season or not (g["home_implied"] and g["away_implied"]):
            continue
        lab = g["_label"]
        if lab["outcome"] == "no_td" or (lab["outcome"] == "offense" and lab.get("pid") is None) \
                or lab.get("side") is None:
            continue
        before = _order(g["season"], g["season_type"], g["week"])
        home, away = history.candidates(g["home"], before), history.candidates(g["away"], before)
        if not home or not away:
            continue
        out.append({**g, "_home": home, "_away": away, "_before": before})
    return out


def game_losses(rows: list[dict], p: Params, rho_by_season: dict[int, float]) -> list[float]:
    out = []
    for g in rows:
        pp = replace(p, rho=rho_by_season[g["season"]])
        dist = game_distribution(team_probability(g["home_implied"], g["away_implied"], pp.k),
                                 g["_home"], g["_away"], pp)
        cands = g["_home"] if g["_label"].get("side") == "home" else g["_away"]
        out.append(-math.log(max(outcome_probability(dist, g["_label"], cands), 1e-9)))
    return out


def log_loss(rows: list[dict], p: Params, rho_by_season: dict[int, float]) -> tuple[float, int]:
    losses = game_losses(rows, p, rho_by_season)
    return sum(losses) / max(len(losses), 1), len(losses)


def paired_week_bootstrap(rows: list[dict], a: list[float], b: list[float], draws: int = 2000,
                          seed: int = 20261107) -> dict:
    """Mean of a - b per game with a 95% interval, resampling whole weeks (games in a week share news)."""
    import random

    by_week = defaultdict(list)
    for g, x, y in zip(rows, a, b):
        by_week[(g["season"], g["season_type"], g["week"])].append(x - y)
    weeks = list(by_week.values())
    rng = random.Random(seed)
    stats = []
    for _ in range(draws):
        pick = [weeks[rng.randrange(len(weeks))] for _ in weeks]
        diffs = [d for w in pick for d in w]
        stats.append(sum(diffs) / len(diffs))
    stats.sort()
    diffs = [d for w in weeks for d in w]
    return {"mean": sum(diffs) / len(diffs), "lo": stats[int(0.025 * draws)], "hi": stats[int(0.975 * draws)],
            "weeks": len(weeks)}


def uniform_log_loss(rows: list[dict], p: Params) -> float:
    """Baseline: coin-flip team, every candidate on a team equally likely, same PHI and THETA."""
    total = 0.0
    for g in rows:
        lab = g["_label"]
        if lab["outcome"] == "non_offense":
            prob = p.phi
        else:
            cands = g["_home"] if lab["side"] == "home" else g["_away"]
            base = 0.5 * (1 - p.phi)
            prob = base * (1 - p.theta) / len(cands) if lab["pid"] in cands else base * p.theta
        total -= math.log(prob)
    return total / len(rows)


def top_pick_rate(rows: list[dict], p: Params, rho_by_season: dict[int, float]) -> dict:
    hits, expected, n = 0, 0.0, 0
    bins = defaultdict(lambda: [0, 0.0, 0])
    for g in rows:
        pp = replace(p, rho=rho_by_season[g["season"]])
        dist = game_distribution(team_probability(g["home_implied"], g["away_implied"], pp.k),
                                 g["_home"], g["_away"], pp)
        players = {k: v for k, v in dist.items() if not k.startswith("other") and k != "non_offense"}
        if not players:
            continue
        best = max(players, key=players.get)
        hits += best == g["_label"].get("pid")
        expected += players[best]
        n += 1
        for pid, prob in players.items():
            b = min(int(prob * 50), 9)  # 2-point bins up to 18%+
            bins[b][0] += 1
            bins[b][1] += prob
            bins[b][2] += pid == g["_label"].get("pid")
    calib = [{"bin": f"{b * 2}-{b * 2 + 2}%" if b < 9 else "18%+", "n": c[0],
              "stated": c[1] / c[0], "happened": c[2] / c[0]} for b, c in sorted(bins.items())]
    return {"games": n, "top_pick_hit": hits / n, "top_pick_stated": expected / n, "calibration": calib}


def backtest(db: DatabaseManager) -> dict:
    box = load_box(db)
    history = History(box)
    games = load_games(db)
    for g in games:
        g["_label"] = label(g, history)
    counts = defaultdict(int)
    for g in games:
        lab = g["_label"]
        counts[(g["season"], lab["outcome"], lab.get("method", ""))] += 1
    phi_fit = [g for g in games if g["season"] in FIT_SEASONS_TEAM and g["_label"]["outcome"] != "no_td"]
    phi = sum(g["_label"]["outcome"] == "non_offense" for g in phi_fit) / len(phi_fit)
    k, grid = fit_team_power(games, FIT_SEASONS_TEAM)
    seasons = sorted({g["season"] for g in games})
    rho_by_season = {s: history.rho((s, 0)) for s in seasons}
    base = Params(k=k, phi=phi)
    fit_rows = scored_games(games, history, FIT_SEASON_PLAYER)
    test_rows = scored_games(games, history, TEST_SEASON)
    best = None
    for theta in (0.01, 0.02, 0.03, 0.05, 0.08, 0.12, 0.20):
        for kappa in (0.0, 0.5, 1.0, 2.0, 3.0, 4.0, 6.0, 8.0, 16.0):
            for w in (0.0, 0.02, 0.05, 0.1, 0.2, 0.5):
                p = replace(base, theta=theta, kappa=kappa, w_prior=w)
                ll, _ = log_loss(fit_rows, p, rho_by_season)
                if best is None or ll < best[0]:
                    best = (ll, p)
    fitted = best[1]
    report = {
        "version": VERSION,
        "label_counts": {f"{s} {o} {m}".strip(): n for (s, o, m), n in sorted(counts.items())},
        "phi": phi,
        "team": {"k": k, "fit": team_report(games, FIT_SEASONS_TEAM, k),
                 "test": team_report(games, (FIT_SEASON_PLAYER, TEST_SEASON), k)},
        "params": fitted.__dict__ | {"rho": rho_by_season},
    }
    for name, rows in (("fit_2025", fit_rows), ("test_2026", test_rows)):
        ll, n = log_loss(rows, fitted, rho_by_season)
        td_only, _ = log_loss(rows, replace(fitted, kappa=0.0), rho_by_season)
        vol_only, _ = log_loss(rows, replace(fitted, kappa=1000.0), rho_by_season)
        vs_volume = paired_week_bootstrap(rows, game_losses(rows, fitted, rho_by_season),
                                          game_losses(rows, replace(fitted, kappa=1000.0), rho_by_season))
        report[name] = {"games": n, "log_loss": ll, "td_only_shares": td_only, "volume_only_shares": vol_only,
                        "model_minus_volume_only": vs_volume,
                        "coin_and_uniform": uniform_log_loss(rows, fitted),
                        **top_pick_rate(rows, fitted, rho_by_season)}
    return report


# ── slate ────────────────────────────────────────────────────────────────────
_UPCOMING = """
    SELECT m.cfbd_game_id, m.season, m.season_type, m.week, m.commence_time, h.name AS home, a.name AS away,
           m.home_implied, m.away_implied
    FROM cfb_matchups m JOIN cfb_teams h ON h.team_id = m.home_team_id
    JOIN cfb_teams a ON a.team_id = m.away_team_id
    WHERE m.home_implied IS NOT NULL AND m.away_implied IS NOT NULL AND m.cfbd_game_id IS NOT NULL AND {where}
    ORDER BY m.commence_time, h.name"""


def slate(db: DatabaseManager, date: str, params: Params, top: int = 6) -> list[dict]:
    games = db.execute(_UPCOMING.format(where="(m.commence_time AT TIME ZONE 'America/New_York')::date = %s"), (date,))
    return predict(db, games, params, top)


def predict(db: DatabaseManager, games: list[dict], params: Params, top: int = 6) -> list[dict]:
    history = History(load_box(db))
    out = []
    for g in games:
        before = _order(g["season"], g["season_type"], g["week"])
        p = replace(params, rho=history.rho((g["season"], 0)))
        home, away = history.candidates(g["home"], before), history.candidates(g["away"], before)
        p_home = team_probability(float(g["home_implied"]), float(g["away_implied"]), p.k)
        dist = game_distribution(p_home, home, away, p)
        cands = {**home, **away}
        players = sorted(((pid, prob) for pid, prob in dist.items() if pid in cands), key=lambda t: -t[1])
        last = {t: max((o for o, _ in history.games_by_team.get(t, []) if o < before), default=None)
                for t in (g["home"], g["away"])}
        out.append({
            "cfbd_game_id": g["cfbd_game_id"], "season": g["season"], "week": g["week"],
            "away": g["away"], "home": g["home"], "commence_time": g["commence_time"],
            "game": f"{g['away']} @ {g['home']}", "kickoff": g["commence_time"].isoformat(),
            "implied": [float(g["away_implied"]), float(g["home_implied"])],
            "p_home_first": p_home, "history_through": {t: list(o) if o else None for t, o in last.items()},
            "players": [{"name": cands[pid]["name"], "team": cands[pid]["team"], "prob": prob,
                         "fair": fair_american(prob), "td": cands[pid]["td"], "touches": cands[pid]["vol"]}
                        for pid, prob in players[:top]],
            "other_offense": dist["other:home"] + dist["other:away"], "non_offense": dist["non_offense"],
        })
    return out


def publish(db: DatabaseManager, params: Params = FITTED, hours: int = PUBLISH_HOURS) -> tuple[int, int]:
    """Save forecasts for games kicking off in the next ``hours``. Started games are never written."""
    games = db.execute(_UPCOMING.format(
        where="m.commence_time > NOW() AND m.commence_time <= NOW() + %s * INTERVAL '1 hour'"), (hours,))
    rows = predict(db, games, params, PUBLISH_TOP) if games else []
    with db.connect() as conn:
        cur = conn.cursor()
        for statement in DDL:
            cur.execute(statement)
        cur.execute("INSERT INTO cfb_first_td_runs (version, params, games) VALUES (%s, %s, %s) RETURNING id",
                    (VERSION, json.dumps(params.__dict__), len(rows)))
        run_id = cur.fetchone()["id"]
        for r in rows:
            cur.execute("""
                INSERT INTO cfb_first_td_predictions (run_id, cfbd_game_id, season, week, kickoff, away, home,
                    away_implied, home_implied, p_home_first, other_offense, non_offense, players, history_through)
                SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s WHERE %s > NOW()""",
                (run_id, r["cfbd_game_id"], r["season"], r["week"], r["commence_time"], r["away"], r["home"],
                 r["implied"][0], r["implied"][1], r["p_home_first"], r["other_offense"], r["non_offense"],
                 json.dumps(r["players"]), json.dumps(r["history_through"]), r["commence_time"]))
    return run_id, len(rows)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    b = sub.add_parser("backtest")
    b.add_argument("--json", type=Path)
    s = sub.add_parser("slate")
    s.add_argument("--date", required=True)
    s.add_argument("--params", type=Path, help="a backtest JSON; its fitted parameters are used")
    s.add_argument("--top", type=int, default=6)
    s.add_argument("--json", type=Path)
    pub = sub.add_parser("publish")
    pub.add_argument("--hours", type=int, default=PUBLISH_HOURS)
    args = ap.parse_args(argv)
    db = DatabaseManager(load_config().database_url, initialize_schema=False)
    if args.cmd == "backtest":
        report = backtest(db)
        text = json.dumps(report, indent=2, default=str)
        if args.json:
            args.json.write_text(text, encoding="utf-8")
        print(text)
        return 0
    if args.cmd == "publish":
        run_id, n = publish(db, FITTED, args.hours)
        print(f"run {run_id}: {n} games saved ({VERSION})")
        return 0
    params = FITTED
    if args.params:
        fitted = json.loads(args.params.read_text(encoding="utf-8"))["params"]
        params = Params(**{k: v for k, v in fitted.items() if k != "rho"})
    games = slate(db, args.date, params, args.top)
    if args.json:
        args.json.write_text(json.dumps(games, indent=2, default=str), encoding="utf-8")
    for g in games:
        away, home = g["game"].split(" @ ")
        print(f"\n{g['game']}  implied {g['implied'][0]:.1f}-{g['implied'][1]:.1f}  "
              f"P({home} scores first) {g['p_home_first']:.0%}")
        for p in g["players"]:
            print(f"  {p['prob']:5.1%}  {p['fair']:>+6d}  {p['name']} ({p['team']}) "
                  f"{p['td']:.0f} TD / {p['touches']:.0f} touches")
        print(f"  other offense {g['other_offense']:.1%}, defense/special teams {g['non_offense']:.1%}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
