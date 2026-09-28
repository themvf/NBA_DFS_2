"""First game of a running-back absence: blind test for the back who takes over.

Study `nfl-rb-first-game-blind-v1`, registered in
docs/nfl-rb-first-game-blind-study.md before any 2013-2018 outcome was
computed.

v2 (`model/nfl_rb_absence_reallocation.py`) was NOT_PROMOTED on 2020-2021.
Slicing its results afterwards suggested the fixed pie does help in one case:
the first game a back misses, for the back who takes over (the remaining back
with the most carries). That slice was cut after seeing the outcomes, on
seasons both earlier studies had graded, so it is a hypothesis, not a result.
This module tests it on seasons no absence study has graded: 2014-2018
(2013 as warm-up history).

Before 2019, nflverse weekly rosters do not mark gameday inactives, so
activity comes from a proxy. A rostered player counts as active if he has a
stat row or a snap-count row for the game; otherwise a player listed ACT
counts as inactive. The proxy is checked against the true status on 2019-2025
before the blind seasons are unmasked.

Usage:
    python -m model.nfl_rb_first_game_blind discovery   # 2020-2025, already-seen data
    python -m model.nfl_rb_first_game_blind validate    # proxy vs true status, 2019-2025
    python -m model.nfl_rb_first_game_blind blind       # 2014-2018 structure only, no outcomes
    python -m model.nfl_rb_first_game_blind unmask      # once, after the registration is pushed
"""
from __future__ import annotations

import argparse
import hashlib
import io
import json
import re
import unicodedata
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

from model import nfl_absence_reallocation as v1
from model import nfl_rb_absence_reallocation as m

STUDY_ID = "nfl-rb-first-game-blind-v1"

SNAP_URL = v1.NFLVERSE + "/snap_counts/snap_counts_{season}.csv"

# One key per franchise. nflverse files disagree: schedules keep the historical
# code (STL, SD, OAK), stats files use today's (LA, LAC, LV) for every season,
# and 2013-2015 weekly rosters use GSIS codes (ARZ, BLT, CLV, HST, SL). One key
# also lets a team's history window cross its relocation.
FRANCHISE = {"STL": "LA", "SL": "LA", "SD": "LAC", "OAK": "LV", "JAC": "JAX",
             "ARZ": "ARI", "BLT": "BAL", "CLV": "CLE", "HST": "HOU"}

DISCOVERY_WARMUP = (2019,)
DISCOVERY_SEASONS = (2020, 2021, 2022, 2023, 2024, 2025)
# nflverse snap counts are empty for 2012, so 2013 is the first proxied season.
BLIND_WARMUP = (2013,)
BLIND_SEASONS = (2014, 2015, 2016, 2017, 2018)

# Frozen before registration (see the study doc): the discovery run on
# 2020-2025 picked phi 0.5/0.5, the same setting v2 froze. The MAE margin is
# set from the expected precision at ~290 blind events (about +/-0.44).
FROZEN: tuple[float, float] | None = (0.5, 0.5)
MAE_MARGIN: float | None = 0.50

GATE_MIN_EVENTS = 150
PROXY_MIN_RECALL = 0.90
PROXY_MIN_PRECISION = 0.90


# ---------------------------------------------------------------------------
# Data and the activity proxy
# ---------------------------------------------------------------------------

def franchise(code) -> str:
    code = str(code)
    return FRANCHISE.get(code, code)


_SUFFIXES = {"jr", "sr", "ii", "iii", "iv", "v"}


def norm_name(name) -> str:
    if not isinstance(name, str):
        return ""
    s = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode().lower()
    s = re.sub(r"[^a-z ]", "", s)
    return "".join(p for p in s.split() if p not in _SUFFIXES)


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def load(seasons, cache: Path = v1.DEFAULT_CACHE, snaps: bool = True):
    """Team games keyed by franchise with the TRUE roster status.

    Returns (teams, played, snapless, digests, quality). `played` maps a team
    game to the gsis ids with a stat row or a snap-count row; `snapless` holds
    team games that have stats but no snap rows at all (the proxy cannot be
    applied there).
    """
    import pandas as pd

    digests, quality = {}, defaultdict(int)
    games_bytes = v1.fetch(v1.GAMES_URL, cache)
    digests["games.csv"] = _sha(games_bytes)
    schedule = pd.read_csv(io.BytesIO(games_bytes), low_memory=False)
    schedule = schedule[(schedule["game_type"] == "REG") & schedule["season"].isin(seasons)]

    by_key: dict[tuple, v1.TeamGame] = {}
    for row in schedule.itertuples(index=False):
        home, away = franchise(row.home_team), franchise(row.away_team)
        for team, opp in ((home, away), (away, home)):
            by_key[(int(row.season), int(row.week), team)] = v1.TeamGame(int(row.season), int(row.week), team, opp)

    pfr: dict[tuple, str] = {}
    played: dict[tuple, set] = defaultdict(set)
    snapless: set = set()
    for season in seasons:
        roster_bytes = v1.fetch(v1.SOURCES["roster"].format(season=season), cache)
        stats_bytes = v1.fetch(v1.SOURCES["stats"].format(season=season), cache)
        digests[f"roster_weekly_{season}.csv"] = _sha(roster_bytes)
        digests[f"stats_player_week_{season}.csv"] = _sha(stats_bytes)
        roster = pd.read_csv(io.BytesIO(roster_bytes), low_memory=False,
                             usecols=["season", "week", "team", "gsis_id", "status", "position",
                                      "full_name", "game_type", "pfr_id"])
        roster = roster[roster["game_type"] == "REG"]
        quality["roster_rows_missing_gsis"] += int(roster["gsis_id"].isna().sum())
        roster = roster[roster["gsis_id"].notna()]
        for row in roster.itertuples(index=False):
            key = (int(row.season), int(row.week), franchise(row.team))
            game = by_key.get(key)
            if game is None:
                quality["roster_rows_without_game"] += 1
                continue
            game.status[row.gsis_id] = str(row.status)
            game.position[row.gsis_id] = str(row.position)
            game.name[row.gsis_id] = str(row.full_name)
            if isinstance(row.pfr_id, str) and row.pfr_id:
                pfr[(key, row.gsis_id)] = row.pfr_id
        stats = pd.read_csv(io.BytesIO(stats_bytes), low_memory=False)
        stats = stats[stats["season_type"] == "REG"]
        for row in stats.itertuples(index=False):
            key = (int(row.season), int(row.week), franchise(row.team))
            game = by_key.get(key)
            if game is None:
                quality["stat_rows_without_game"] += 1
                continue
            if not isinstance(row.player_id, str):
                continue
            game.stats[row.player_id] = {f: float(getattr(row, f)) if pd.notna(getattr(row, f)) else 0.0
                                         for f in v1.STAT_FIELDS}
            game.has_stats = True
            game.position.setdefault(row.player_id, str(row.position))
            game.name.setdefault(row.player_id, str(row.player_display_name))
            played[key].add(row.player_id)

        if not snaps:
            continue
        snap_bytes = v1.fetch(SNAP_URL.format(season=season), cache)
        digests[f"snap_counts_{season}.csv"] = _sha(snap_bytes)
        snap = pd.read_csv(io.BytesIO(snap_bytes), low_memory=False,
                           usecols=["season", "week", "game_type", "team", "pfr_player_id", "player"])
        snap = snap[snap["game_type"] == "REG"]
        snap_pfr, snap_names = defaultdict(set), defaultdict(set)
        for row in snap.itertuples(index=False):
            key = (int(row.season), int(row.week), franchise(row.team))
            if key not in by_key:
                quality["snap_rows_without_game"] += 1
                continue
            snap_pfr[key].add(str(row.pfr_player_id))
            snap_names[key].add(norm_name(row.player))
        for row in snap.itertuples(index=False):
            key = (int(row.season), int(row.week), franchise(row.team))
            game = by_key.get(key)
            if game is None:
                continue
            quality["snap_rows"] += 1
            name = norm_name(row.player)
            if not any(pfr.get((key, g)) == str(row.pfr_player_id) or norm_name(game.name.get(g)) == name
                       for g in game.status):
                quality["snap_rows_matching_no_roster_player"] += 1
        for key, game in by_key.items():
            if key[0] != season:
                continue
            if game.has_stats and not snap_pfr.get(key):
                snapless.add(key)
                continue
            for gsis in game.status:
                if gsis in played[key]:
                    continue
                p = pfr.get((key, gsis))
                if p is not None and p in snap_pfr[key]:
                    played[key].add(gsis)
                    quality["played_by_snap_pfr_id"] += 1
                elif norm_name(game.name.get(gsis)) in snap_names[key]:
                    played[key].add(gsis)
                    quality["played_by_snap_name"] += 1

    quality["games_without_snap_rows"] = len(snapless)
    teams: dict[str, list[v1.TeamGame]] = defaultdict(list)
    for game in by_key.values():
        teams[game.team].append(game)
    for games in teams.values():
        games.sort(key=lambda g: (g.season, g.week))
    return dict(teams), dict(played), snapless, digests, dict(quality)


def proxy_status(true_status: str, played: bool) -> str:
    """Played the game -> ACT. Listed ACT but never on the field -> INA. Otherwise unchanged."""
    if played:
        return "ACT"
    return "INA" if true_status == "ACT" else true_status


def with_proxy(teams, played, snapless):
    """Copies of the team games with the proxy status in place of the roster status."""
    out = {}
    for team, games in teams.items():
        copies = []
        for g in games:
            key = (g.season, g.week, g.team)
            who = played.get(key, set())
            status = {pid: proxy_status(s, pid in who) for pid, s in g.status.items()}
            copies.append(v1.TeamGame(g.season, g.week, g.team, g.opponent, status, g.position, g.name,
                                      g.stats, g.has_stats and key not in snapless))
        out[team] = copies
    return out


# ---------------------------------------------------------------------------
# Events: v2's absence games, annotated with first game and lead back
# ---------------------------------------------------------------------------

def annotated_events(teams, seasons) -> list[dict]:
    out = []
    for team, games in sorted(teams.items()):
        index = {(g.season, g.week): n for n, g in enumerate(games)}
        for e in m.build_events({team: games}, seasons):
            n = index[(e["season"], e["week"])]
            prev = games[n - 1] if n else None
            material = [d for d in e["carries"]["donors"] if d["base"] >= m.CARRIES.material]
            lead = max(range(len(e["rows"])), key=lambda i: e["rows"][i]["carries_base"])
            e["first_game"] = any(prev is not None and prev.status.get(d["id"]) == "ACT" for d in material)
            e["lead"] = lead
            e["lead_id"] = e["rows"][lead]["id"]
            e["lead_base"] = e["rows"][lead]["carries_base"]
            e["donor_base"] = max(d["base"] for d in material)
            out.append(e)
    return out


def primary(events) -> list[dict]:
    """The registered population: first game of the absence."""
    return [e for e in events if e["first_game"]]


def lead_errors(event: dict, config) -> dict:
    """The whole backfield is allocated; only the lead back is scored."""
    errs = m.errors(event, config)
    i = event["lead"]
    return {k: v[i:i + 1] for k, v in errs.items()}


def other_errors(event: dict, config) -> dict:
    errs = m.errors(event, config)
    i = event["lead"]
    return {k: np.delete(v, i) for k, v in errs.items()}


def paired(events, a, b, key: str, scorer=lead_errors) -> dict:
    """a - b on squared and absolute error, event-clustered bootstrap."""
    sq, ab, counts, bias_a, bias_b, se_a, se_b, ae_a, ae_b = [], [], [], [], [], [], [], [], []
    for ev in events:
        ea, eb = scorer(ev, a)[key], scorer(ev, b)[key]
        sq.append(float((ea ** 2 - eb ** 2).sum()))
        ab.append(float((np.abs(ea) - np.abs(eb)).sum()))
        counts.append(len(ea))
        bias_a.append(float(ea.sum()))
        bias_b.append(float(eb.sum()))
        se_a.append(float((ea ** 2).sum()))
        se_b.append(float((eb ** 2).sum()))
        ae_a.append(float(np.abs(ea).sum()))
        ae_b.append(float(np.abs(eb).sum()))
    if not events or sum(counts) == 0:
        return {"events": len(events), "rows": 0}
    sq, ab, counts = np.array(sq), np.array(ab), np.array(counts, dtype=float)
    rng = np.random.default_rng(v1.BOOTSTRAP_SEED)
    idx = rng.integers(0, len(events), size=(v1.BOOTSTRAP_DRAWS, len(events)))
    denom = counts[idx].sum(axis=1)
    ok = denom > 0
    sq_ci = np.percentile(sq[idx].sum(axis=1)[ok] / denom[ok], [2.5, 97.5])
    ab_ci = np.percentile(ab[idx].sum(axis=1)[ok] / denom[ok], [2.5, 97.5])
    n = counts.sum()
    return {"events": len(events), "rows": int(n),
            "mse_a": sum(se_a) / n, "mse_b": sum(se_b) / n, "mae_a": sum(ae_a) / n, "mae_b": sum(ae_b) / n,
            "bias_a": sum(bias_a) / n, "bias_b": sum(bias_b) / n,
            "mse_delta": float(sq.sum() / n), "mse_ci": [float(sq_ci[0]), float(sq_ci[1])],
            "mae_delta": float(ab.sum() / n), "mae_ci": [float(ab_ci[0]), float(ab_ci[1])]}


def lead_mse(events, config) -> float:
    e = np.concatenate([lead_errors(ev, config)["points"] for ev in events])
    return float((e ** 2).mean())


def lead_mae(events, config) -> float:
    e = np.concatenate([lead_errors(ev, config)["points"] for ev in events])
    return float(np.abs(e).mean())


def select(events) -> dict:
    """Lowest lead-back points squared error; ties to the smaller phi_targets, then phi_carries."""
    scores = {m.key(c): round(lead_mse(events, c), 3) for c in m.CONFIGS}
    best = sorted(m.CONFIGS, key=lambda c: (scores[m.key(c)], c[1], c[0]))[0]
    return {"phi_carries": best[0], "phi_targets": best[1], "points_mse": scores,
            "points_mae": {m.key(c): round(lead_mae(events, c), 4) for c in m.CONFIGS},
            "base_points_mse": round(lead_mse(events, m.BASE), 3),
            "base_points_mae": round(lead_mae(events, m.BASE), 4), "events": len(events)}


def verdict(points: dict, carries: dict, mae_margin: float) -> dict:
    g1 = points["mse_ci"][1] < 0
    g2 = points["mae_ci"][1] < mae_margin
    g3 = carries["mse_delta"] < 0
    g4 = points["events"] >= GATE_MIN_EVENTS
    status = "INSUFFICIENT" if not g4 else ("PROMOTE" if g1 and g2 and g3 else "NOT_PROMOTED")
    return {"G1_points_squared_error": g1, "G2_points_mae_noninferior": g2, "G3_carries": g3,
            "G4_sample": g4, "verdict": status}


def narrow(events) -> list[dict]:
    """Descriptive slice: a backup under 6 carries replacing a 12+ carry starter."""
    return [e for e in events if e["lead_base"] < 6 and e["donor_base"] >= 12]


def describe(events, config) -> dict:
    """Everything reported alongside the gate; none of it gates."""
    first = primary(events)
    under_way = [e for e in events if not e["first_game"]]
    return {
        "first_game_lead_back": paired(first, config, m.BASE, "points"),
        "first_game_other_backs": paired(first, config, m.BASE, "points", scorer=other_errors),
        "under_way_lead_back": paired(under_way, config, m.BASE, "points"),
        "narrow_backup_replaces_starter": paired(narrow(first), config, m.BASE, "points"),
        "by_season": {str(s): paired([e for e in first if e["season"] == s], config, m.BASE, "points")
                      for s in sorted({e["season"] for e in first})},
    }


def structure(events) -> dict:
    """Counts that depend on who played, never on how many points anyone scored."""
    first = primary(events)
    return {"absence_games": len(events), "first_game_events": len(first),
            "under_way_events": len(events) - len(first),
            "narrow_slice_events": len(narrow(first)),
            "first_game_by_season": {str(s): sum(e["season"] == s for e in first)
                                     for s in sorted({e["season"] for e in events})}}


# ---------------------------------------------------------------------------
# Proxy validation on seasons with a true status (2019-2025)
# ---------------------------------------------------------------------------

def event_keys(events) -> dict:
    return {(e["team"], e["season"], e["week"]): e["lead_id"] for e in primary(events)}


def validate(cache: Path, config) -> dict:
    teams, played, snapless, digests, quality = load(DISCOVERY_WARMUP + DISCOVERY_SEASONS, cache, snaps=True)
    true_events = annotated_events(teams, DISCOVERY_SEASONS)
    proxy_teams = with_proxy(teams, played, snapless)
    proxy_events = annotated_events(proxy_teams, DISCOVERY_SEASONS)

    # Player-game agreement for skill players the proxy has to classify.
    confusion = defaultdict(int)
    for team, games in teams.items():
        for g in games:
            if g.season not in DISCOVERY_SEASONS or not g.has_stats:
                continue
            who = played.get((g.season, g.week, g.team), set())
            for pid, s in g.status.items():
                if g.position.get(pid) not in ("RB", "FB", "WR", "TE") or s not in ("ACT", "INA"):
                    continue
                confusion[f"true_{s}_proxy_{proxy_status(s, pid in who)}"] += 1

    t, p = event_keys(true_events), event_keys(proxy_events)
    same = sum(1 for k, lead in t.items() if p.get(k) == lead)
    recall = same / len(t) if t else 0.0
    precision = same / len(p) if p else 0.0
    proxy_points = paired(primary(proxy_events), config, m.BASE, "points")
    true_points = paired(primary(true_events), config, m.BASE, "points")
    checks = {"V1_recall": recall >= PROXY_MIN_RECALL, "V2_precision": precision >= PROXY_MIN_PRECISION,
              "V3_proxy_replicates": proxy_points["mse_ci"][1] < 0 and true_points["mse_delta"] < 0}
    return {"sources": digests, "data_quality": quality, "player_game_confusion": dict(confusion),
            "true_first_game_events": len(t), "proxy_first_game_events": len(p),
            "same_event_same_lead": same, "recall": recall, "precision": precision,
            "true_status_points": true_points, "proxy_status_points": proxy_points,
            "checks": checks, "passed": all(checks.values())}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _require_frozen():
    if FROZEN is None or MAE_MARGIN is None:
        raise SystemExit("FROZEN and MAE_MARGIN must be set from the registered discovery run first")
    return FROZEN


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("command", choices=["discovery", "validate", "blind", "unmask"])
    parser.add_argument("--cache", type=Path, default=v1.DEFAULT_CACHE)
    parser.add_argument("--out", type=Path, default=None)
    args = parser.parse_args()
    stamp = datetime.now(timezone.utc).isoformat()

    if args.command == "discovery":
        teams, _, _, digests, quality = load(DISCOVERY_WARMUP + DISCOVERY_SEASONS, args.cache, snaps=False)
        events = annotated_events(teams, DISCOVERY_SEASONS)
        selection = select(primary(events))
        config = (selection["phi_carries"], selection["phi_targets"])
        result = {"study": STUDY_ID, "mode": "discovery", "generated_at": stamp, "sources": digests,
                  "data_quality": quality, "structure": structure(events), "selection": selection,
                  "carries": paired(primary(events), config, m.BASE, "carries"),
                  "described": describe(events, config)}
    elif args.command == "validate":
        result = {"study": STUDY_ID, "mode": "validate", "generated_at": stamp,
                  **validate(args.cache, _require_frozen())}
    elif args.command == "blind":
        teams, played, snapless, digests, quality = load(BLIND_WARMUP + BLIND_SEASONS, args.cache, snaps=True)
        events = annotated_events(with_proxy(teams, played, snapless), BLIND_SEASONS)
        result = {"study": STUDY_ID, "mode": "blind", "generated_at": stamp, "sources": digests,
                  "data_quality": quality, "structure": structure(events)}
    else:
        config = _require_frozen()
        check = validate(args.cache, config)
        if not check["passed"]:
            raise SystemExit(f"proxy validation failed, study VOID: {check['checks']}")
        teams, played, snapless, digests, quality = load(BLIND_WARMUP + BLIND_SEASONS, args.cache, snaps=True)
        events = annotated_events(with_proxy(teams, played, snapless), BLIND_SEASONS)
        first = primary(events)
        points = paired(first, config, m.BASE, "points")
        carries = paired(first, config, m.BASE, "carries")
        result = {"study": STUDY_ID, "mode": "unmask", "registered": "docs/nfl-rb-first-game-blind-study.md",
                  "generated_at": stamp, "sources": digests, "data_quality": quality,
                  "frozen": {"phi_carries": config[0], "phi_targets": config[1], "mae_margin": MAE_MARGIN},
                  "proxy_validation": check["checks"], "structure": structure(events),
                  "points": points, "carries": carries,
                  "targets": paired(first, config, m.BASE, "targets") if config[1] > 0 else None,
                  "gate": verdict(points, carries, MAE_MARGIN),
                  "described": describe(events, config)}

    out = args.out or Path(f"artifacts/nfl_rb_first_game_blind_v1_{args.command}.json")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(result, indent=1, default=float) + "\n")
    print(json.dumps({k: v for k, v in result.items() if k not in ("sources", "described")}, indent=1, default=float))


if __name__ == "__main__":
    main()
