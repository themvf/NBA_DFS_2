"""`nfl-archetype-thesis-v1`: do the DFS archetype theses hold on 2023-25 game outcomes?

Registration: docs/nfl-archetype-thesis-2023-25.md (theses, metrics and slices
frozen before any game was scored). Descriptive only: no lineup is built,
nothing live changes.

    python -m research.nfl_archetype_thesis [--seasons 2023,2024,2025] [--out PATH] [--draws 1000]

Inputs: `ff_player_week_stats` (nflverse rows; DST and K rows carry the stored
DraftKings-equivalent weekly points), `ff_players` for positions, and
`nfl_season_games` for closing spread (home margin, positive = home favored),
closing total and final scores.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
import psycopg2

VERSION = "nfl-archetype-thesis-v1"
TEAM_ALIASES = {"LA": "LAR", "WAS": "WSH", "JAC": "JAX", "AZ": "ARI", "OAK": "LV", "SD": "LAC", "STL": "LAR"}
SPREAD_BUCKETS = (("0-3", 0.0, 3.0), ("3.5-6.5", 3.5, 6.5), ("7+", 7.0, 99.0))


def _num(row: dict, key: str) -> float:
    v = row.get(key)
    try:
        return float(v) if v is not None and v == v else 0.0
    except (TypeError, ValueError):
        return 0.0


def dk_skill_points(row: dict) -> float:
    """DraftKings Classic scoring for a QB/RB/WR/TE nflverse row."""
    py, ry, rcy = _num(row, "passing_yards"), _num(row, "rushing_yards"), _num(row, "receiving_yards")
    fumbles = sum(_num(row, k) for k in ("sack_fumbles_lost", "rushing_fumbles_lost", "receiving_fumbles_lost"))
    picks = _num(row, "passing_interceptions") if "passing_interceptions" in row else _num(row, "interceptions")
    pts = (py * 0.04 + 4 * _num(row, "passing_tds") - picks
           + ry * 0.1 + 6 * _num(row, "rushing_tds") + rcy * 0.1 + 6 * _num(row, "receiving_tds") + _num(row, "receptions")
           + 2 * (_num(row, "passing_2pt_conversions") + _num(row, "rushing_2pt_conversions") + _num(row, "receiving_2pt_conversions"))
           + 6 * _num(row, "special_teams_tds") - fumbles)
    pts += 3.0 * ((py >= 300) + (ry >= 100) + (rcy >= 100))
    return float(pts)


def connect():
    """Like research.nfl_ownership_inventory.connect: the repo's .env and web/.env.local, else DATABASE_URL."""
    from dotenv import load_dotenv
    root = Path(__file__).resolve().parents[1]
    for env in (root / "web" / ".env.local", root / ".env"):
        if env.exists():
            load_dotenv(env, override=False)
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise SystemExit("DATABASE_URL is not set and no .env / web/.env.local was found")
    return psycopg2.connect(url)


def load(conn, seasons: tuple[int, ...]) -> tuple[pd.DataFrame, pd.DataFrame]:
    cur = conn.cursor()
    cur.execute("""SELECT g.season, g.week, h.abbreviation, a.abbreviation, g.quoted_spread_line, g.quoted_total_line, g.home_score, g.away_score
                   FROM nfl_season_games g JOIN nfl_teams h ON h.team_id = g.home_team_id JOIN nfl_teams a ON a.team_id = g.away_team_id
                   WHERE g.game_type = 'REG' AND g.season = ANY(%s) AND g.quoted_spread_line IS NOT NULL AND g.quoted_total_line IS NOT NULL
                     AND g.home_score IS NOT NULL""", (list(seasons),))
    games = pd.DataFrame(cur.fetchall(), columns=["season", "week", "home", "away", "spread", "total", "home_score", "away_score"])
    games["home"] = games["home"].map(lambda t: TEAM_ALIASES.get(t, t)); games["away"] = games["away"].map(lambda t: TEAM_ALIASES.get(t, t))
    games["game"] = games["season"].astype(str) + "_" + games["week"].astype(str).str.zfill(2) + "_" + games["away"] + "_" + games["home"]
    games["favorite"] = np.where(games["spread"] > 0, games["home"], np.where(games["spread"] < 0, games["away"], None))
    games["abs_spread"] = games["spread"].abs()
    games["actual_total"] = games["home_score"] + games["away_score"]
    cur.execute("""SELECT s.season, s.week, s.team, s.opponent, p.position, p.canonical_name, s.fantasy_points_ppr, s.source_row
                   FROM ff_player_week_stats s JOIN ff_players p ON p.id = s.player_id
                   WHERE s.season = ANY(%s) AND s.season_type = 'REG' AND p.position IN ('QB','RB','WR','TE','K','DST')""", (list(seasons),))
    rows = []
    for season, week, team, opp, pos, name, ppr, src in cur.fetchall():
        team = TEAM_ALIASES.get(team, team); opp = TEAM_ALIASES.get(opp, opp)
        src = src or {}
        pts = float(ppr or 0.0) if pos in ("K", "DST") else dk_skill_points(src)
        rows.append({"season": season, "week": week, "team": team, "opp": opp, "pos": pos, "name": name, "pts": pts,
                     "attempts": _num(src, "attempts"), "carries": _num(src, "carries"), "targets": _num(src, "targets")})
    players = pd.DataFrame(rows)
    key = {(r.season, r.week, r.home): r.game for r in games.itertuples()} | {(r.season, r.week, r.away): r.game for r in games.itertuples()}
    players["game"] = [key.get((s, w, t)) for s, w, t in zip(players["season"], players["week"], players["team"])]
    return games, players


@dataclass
class Rate:
    label: str
    n: int
    value: float
    lo: float
    hi: float
    baseline: float | None = None

    def row(self) -> str:
        b = f" | {self.baseline:.3f}" if self.baseline is not None else " | "
        return f"| {self.label} | {self.n} | {self.value:.3f} | [{self.lo:.3f}, {self.hi:.3f}]{b} |"


def boot(values: np.ndarray, draws: int, seed: int = 7) -> tuple[float, float]:
    if len(values) == 0:
        return float("nan"), float("nan")
    rng = np.random.default_rng(seed)
    idx = rng.integers(0, len(values), size=(draws, len(values)))
    means = values[idx].mean(axis=1)
    return float(np.percentile(means, 2.5)), float(np.percentile(means, 97.5))


def rate(label: str, flags: list, draws: int, baseline: float | None = None) -> Rate:
    v = np.asarray([float(f) for f in flags if f is not None and f == f], dtype=float)
    lo, hi = boot(v, draws)
    return Rate(label, len(v), float(v.mean()) if len(v) else float("nan"), lo, hi, baseline)


def corr_boot(x: np.ndarray, y: np.ndarray, draws: int, seed: int = 7) -> tuple[float, float, float]:
    rng = np.random.default_rng(seed)
    r = float(np.corrcoef(x, y)[0, 1]) if len(x) > 2 else float("nan")
    idx = rng.integers(0, len(x), size=(draws, len(x)))
    rs = np.array([np.corrcoef(x[i], y[i])[0, 1] for i in idx])
    return r, float(np.nanpercentile(rs, 2.5)), float(np.nanpercentile(rs, 97.5))


def spread_bucket(s: float) -> str | None:
    for label, lo, hi in SPREAD_BUCKETS:
        if lo <= s <= hi:
            return label
    return None


def analyse(games: pd.DataFrame, players: pd.DataFrame, draws: int) -> list[str]:
    out: list[str] = []
    gp = {g: df for g, df in players[players["game"].notna()].groupby("game")}
    tot_edges = games["total"].quantile([1 / 3, 2 / 3]).to_numpy()
    tercile = lambda t: "low" if t <= tot_edges[0] else ("mid" if t <= tot_edges[1] else "high")
    per_game: list[dict] = []
    for g in games.itertuples():
        df = gp.get(g.game)
        if df is None or df["team"].nunique() < 2:
            continue
        df = df.sort_values("pts", ascending=False)
        top6 = df.head(6)
        top = df.iloc[0]
        fav, dog = g.favorite, (g.away if g.favorite == g.home else g.home) if g.favorite else None
        qbs = {t: d[d["pos"] == "QB"].sort_values("attempts", ascending=False).head(1) for t, d in df.groupby("team")}
        qb_pts = {t: (q["pts"].iloc[0] if len(q) else np.nan) for t, q in qbs.items()}
        qb_name = {t: (q["name"].iloc[0] if len(q) else None) for t, q in qbs.items()}
        rbs = {t: d[d["pos"] == "RB"].sort_values("carries", ascending=False).head(1) for t, d in df.groupby("team")}
        dsts = {t: d[d["pos"] == "DST"].head(1) for t, d in df.groupby("team")}
        catchers = {t: d[d["pos"].isin(["WR", "TE"])].sort_values("targets", ascending=False) for t, d in df.groupby("team")}
        teams = list(qb_pts)
        rec = {"game": g.game, "season": g.season, "spread": g.abs_spread, "bucket": spread_bucket(g.abs_spread), "tercile": tercile(g.total),
               "actual_total": g.actual_total, "top_pos": top["pos"], "top_team": top["team"],
               "both_qb_20": float(all(qb_pts[t] >= 20 for t in teams)) if len(teams) == 2 else np.nan,
               "qb_20": [float(qb_pts[t] >= 20) for t in teams],
               "fav_share": (top6["team"] == fav).mean() if fav else np.nan,
               "fav_ge4": float((top6["team"] == fav).sum() >= 4) if fav else np.nan,
               "fav_ge5": float((top6["team"] == fav).sum() >= 5) if fav else np.nan,
               "any_ge4": float(top6["team"].value_counts().max() >= 4), "any_ge5": float(top6["team"].value_counts().max() >= 5),
               "dog_qb_top3": float(qb_name.get(dog) in set(df.head(3)["name"])) if dog else np.nan,
               "dog_top": float(top["team"] == dog) if dog else np.nan,
               "kdst_in_top6": float(top6["pos"].isin(["K", "DST"]).any()), "kdst_count": float(top6["pos"].isin(["K", "DST"]).sum())}
        # T5 captain stack
        if top["pos"] in ("WR", "TE"):
            own_qb = qb_name.get(top["team"])
            rec["pc_top_own_qb_top3"] = float(own_qb in set(df.head(3)["name"]))
            rec["pc_top_own_qb_top6"] = float(own_qb in set(top6["name"]))
        if top["pos"] == "QB":
            rec["qb_top_own_catcher_top6"] = float(((top6["team"] == top["team"]) & top6["pos"].isin(["WR", "TE"])).sum() >= 1)
        # T4 / T10 RB + DST pairs, T9 QB + catcher pairs
        for t in teams:
            rb, dst = rbs[t], dsts[t]
            if len(rb) and len(dst):
                per_game.append({"kind": "rb_dst", "game": g.game, "season": g.season, "bucket": rec["bucket"], "is_fav": t == fav,
                                 "x": float(rb["pts"].iloc[0]), "y": float(dst["pts"].iloc[0])})
            for rank, label in ((0, "WR1"), (1, "WR2")):
                wrs = catchers[t][catchers[t]["pos"] == "WR"]
                if len(wrs) > rank and not np.isnan(qb_pts[t]):
                    per_game.append({"kind": f"qb_{label}", "game": g.game, "season": g.season, "x": qb_pts[t], "y": float(wrs["pts"].iloc[rank])})
            tes = catchers[t][catchers[t]["pos"] == "TE"]
            if len(tes) and not np.isnan(qb_pts[t]):
                per_game.append({"kind": "qb_TE1", "game": g.game, "season": g.season, "x": qb_pts[t], "y": float(tes["pts"].iloc[0])})
        per_game.append({"kind": "game", **rec})
    G = pd.DataFrame([r for r in per_game if r["kind"] == "game"])
    P = pd.DataFrame([r for r in per_game if r["kind"] != "game"])
    out.append(f"Games scored: {len(G)} of {len(games)} (seasons {sorted(int(x) for x in G['season'].unique())}); pairs: {P['kind'].value_counts().to_dict()}\n")

    def table(title: str, rates: list[Rate]) -> None:
        out.append(f"\n### {title}\n\n| slice | n | rate | 95% CI | baseline |\n|---|---:|---:|---|---:|")
        out.extend(r.row() for r in rates)

    # T1
    rates = []
    for name, sub in [("all", G)] + [(f"total {t}", G[G["tercile"] == t]) for t in ("low", "mid", "high")] + [(str(s), G[G["season"] == s]) for s in sorted(G["season"].unique())]:
        singles = [q for qs in sub["qb_20"] for q in qs]
        base = (np.mean(singles) ** 2) if singles else None
        rates.append(rate(name, sub["both_qb_20"].tolist(), draws, base))
    table("T1 Shootout: both starting QBs >= 20 DK (baseline = single-QB rate squared)", rates)
    # T2
    rates = []
    for name, sub in [("all", G)] + [(f"spread {b}", G[G["bucket"] == b]) for b, _, _ in SPREAD_BUCKETS] + [(str(s), G[G["season"] == s]) for s in sorted(G["season"].unique())]:
        rates.append(rate(f"{name}: favorite share of top 6", sub["fav_share"].tolist(), draws, 0.5))
        rates.append(rate(f"{name}: favorite >= 4 of 6", sub["fav_ge4"].tolist(), draws))
        rates.append(rate(f"{name}: favorite >= 5 of 6", sub["fav_ge5"].tolist(), draws))
    table("T2 Favorite onslaught", rates)
    # T3
    rates = []
    for name, sub in [("all", G)] + [(f"spread {b}", G[G["bucket"] == b]) for b, _, _ in SPREAD_BUCKETS] + [(str(s), G[G["season"] == s]) for s in sorted(G["season"].unique())]:
        rates.append(rate(f"{name}: underdog QB in top 3", sub["dog_qb_top3"].tolist(), draws))
        rates.append(rate(f"{name}: top scorer is an underdog", sub["dog_top"].tolist(), draws, 0.5))
    table("T3 Underdog comeback", rates)
    # T4 / T10 RB + DST
    rates = []
    lines = []
    q75 = {k: P[P["kind"] == "rb_dst"][k].quantile(0.75) for k in ("x", "y")}
    for name, sub in [("all teams", P[P["kind"] == "rb_dst"]), ("favorites", P[(P["kind"] == "rb_dst") & P["is_fav"]])] + \
            [(f"favorites, spread {b}", P[(P["kind"] == "rb_dst") & P["is_fav"] & (P["bucket"] == b)]) for b, _, _ in SPREAD_BUCKETS] + \
            [(f"favorites {s}", P[(P["kind"] == "rb_dst") & P["is_fav"] & (P["season"] == s)]) for s in sorted(G["season"].unique())]:
        if len(sub) < 10:
            continue
        r, lo, hi = corr_boot(sub["x"].to_numpy(), sub["y"].to_numpy(), draws)
        lines.append(f"| {name} | {len(sub)} | {r:+.3f} | [{lo:+.3f}, {hi:+.3f}] |")
        both = ((sub["x"] >= q75["x"]) & (sub["y"] >= q75["y"])).astype(float).tolist()
        base = float((sub["x"] >= q75["x"]).mean() * (sub["y"] >= q75["y"]).mean())
        rates.append(rate(f"{name}: both above 75th pct", both, draws, base))
    out.append("\n### T4 Blowout / T10 RB + own DST: within-game correlation of lead RB and DST points\n\n| slice | n | r | 95% CI |\n|---|---:|---:|---|")
    out.extend(lines)
    table("T4 / T10 co-boom (baseline = product of the single rates)", rates)
    # T5
    pc = G.dropna(subset=["pc_top_own_qb_top3"]) if "pc_top_own_qb_top3" in G else G.iloc[0:0]
    qt = G.dropna(subset=["qb_top_own_catcher_top6"]) if "qb_top_own_catcher_top6" in G else G.iloc[0:0]
    rates = [rate("WR/TE top scorer: own QB in top 3", pc["pc_top_own_qb_top3"].tolist(), draws),
             rate("WR/TE top scorer: own QB in top 6", pc["pc_top_own_qb_top6"].tolist(), draws),
             rate("QB top scorer: an own WR/TE in top 6", qt["qb_top_own_catcher_top6"].tolist(), draws)]
    for s in sorted(G["season"].unique()):
        rates.append(rate(f"{s}: WR/TE top scorer, own QB in top 3", pc[pc["season"] == s]["pc_top_own_qb_top3"].tolist(), draws))
    table("T5 Captain stack", rates)
    # T6
    out.append("\n### T6 Position of the game's top scorer\n\n| slice | n | QB | RB | WR | TE | K | DST |\n|---|---:|---:|---:|---:|---:|---:|---:|")
    for name, sub in [("all", G)] + [(f"total {t}", G[G["tercile"] == t]) for t in ("low", "mid", "high")] + [(str(s), G[G["season"] == s]) for s in sorted(G["season"].unique())]:
        vc = sub["top_pos"].value_counts(normalize=True)
        out.append(f"| {name} | {len(sub)} | " + " | ".join(f"{vc.get(p, 0):.3f}" for p in ("QB", "RB", "WR", "TE", "K", "DST")) + " |")
    # T7
    rates = []
    at = G["actual_total"]
    for name, sub in [("all", G)] + [(f"closing total {t}", G[G["tercile"] == t]) for t in ("low", "mid", "high")] + \
            [("actual total < 38", G[at < 38]), ("actual 38-48", G[(at >= 38) & (at <= 48)]), ("actual > 48", G[at > 48])]:
        rates.append(rate(f"{name}: a K or DST in top 6", sub["kdst_in_top6"].tolist(), draws))
        rates.append(rate(f"{name}: K+DST count in top 6", sub["kdst_count"].tolist(), draws))
    table("T7 Low-scoring K/DST", rates)
    # T8
    rates = []
    for name, sub in [("all", G)] + [(f"spread {b}", G[G["bucket"] == b]) for b, _, _ in SPREAD_BUCKETS] + [(f"total {t}", G[G["tercile"] == t]) for t in ("low", "mid", "high")]:
        rates.append(rate(f"{name}: some team has >= 4 of top 6", sub["any_ge4"].tolist(), draws))
        rates.append(rate(f"{name}: some team has >= 5 of top 6", sub["any_ge5"].tolist(), draws))
    table("T8 Team stack 4+ / 5+", rates)
    # T9
    out.append("\n### T9 Classic double stack: QB points against his own catchers (by target rank)\n\n| pair | n | r | 95% CI | P(QB>=25 & catcher>=20) | baseline |\n|---|---:|---:|---|---:|---:|")
    for kind in ("qb_WR1", "qb_WR2", "qb_TE1"):
        sub = P[P["kind"] == kind]
        r, lo, hi = corr_boot(sub["x"].to_numpy(), sub["y"].to_numpy(), draws)
        both = ((sub["x"] >= 25) & (sub["y"] >= 20)).mean(); base = (sub["x"] >= 25).mean() * (sub["y"] >= 20).mean()
        out.append(f"| {kind} | {len(sub)} | {r:+.3f} | [{lo:+.3f}, {hi:+.3f}] | {both:.3f} | {base:.3f} |")
        for s in sorted(G["season"].unique()):
            ss = sub[sub["season"] == s]
            r, lo, hi = corr_boot(ss["x"].to_numpy(), ss["y"].to_numpy(), draws)
            out.append(f"| {kind} {s} | {len(ss)} | {r:+.3f} | [{lo:+.3f}, {hi:+.3f}] | | |")
    return out


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--seasons", default="2023,2024,2025")
    ap.add_argument("--out", default="artifacts/nfl-archetype-thesis/thesis-2023-25.md")
    ap.add_argument("--draws", type=int, default=1000)
    a = ap.parse_args(argv)
    seasons = tuple(int(s) for s in a.seasons.split(","))
    conn = connect()
    games, players = load(conn, seasons)
    unmatched = int(players["game"].isna().sum())
    body = analyse(games, players, a.draws)
    head = [f"# {VERSION}: archetype theses on {seasons[0]}-{seasons[-1]} outcomes", "",
            f"Registration: docs/nfl-archetype-thesis-2023-25.md. Games with a closing spread, total and final: {len(games)}. "
            f"Player-weeks: {len(players)} ({unmatched} not matched to a game, dropped). Bootstrap draws: {a.draws}. Descriptive; no lineup built.", ""]
    text = "\n".join(head + body) + "\n"
    out = Path(a.out); out.parent.mkdir(parents=True, exist_ok=True); out.write_text(text, encoding="utf-8")
    print(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
