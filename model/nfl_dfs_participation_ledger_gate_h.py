"""Gate H of ``nfl-dfs-participation-ledger-v1``: the 2024 and 2025 replays.

Registered in ``docs/nfl-dfs-rb-floor-participation-study.md`` (2026-10-20) and
amended there on 2026-10-21 before this file existed. The question: does
building a player's history as a LEDGER of team games (a dressed or
practice-squad week with no stat line enters as a 0-point line) give the
historical model a better P10 than today's stat-lines-only history, graded on
2024 and 2025 with non-players scored as 0, the way DraftKings scores them?

Both arms call ``project_player`` from ``model/nfl_dfs_historical.py`` at the
hash pinned below; the model code is not changed. The only difference is the
history each arm is handed:

* baseline  -- every pre-cutoff stat line (production's ``_history``);
* candidate -- the same lines plus a zero line for every pre-cutoff team game
  in which the player was ``ACT`` or ``DEV`` on the nflverse weekly roster and
  recorded nothing. ``INA``/``RES``/other statuses add nothing (an inactive
  cannot be told from an injury; the availability layer owns those).

Usage::

    python -m model.nfl_dfs_participation_ledger_gate_h --season 2024
    python -m model.nfl_dfs_participation_ledger_gate_h --season 2025
    python -m model.nfl_dfs_participation_ledger_gate_h --verdict

Each season run writes ``artifacts/nfl-dfs-participation-ledger-gate-h/<season>.json``;
``--verdict`` reads both and applies the registered rule once. Nothing here
touches the database or production projections.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import sys
import time
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import numpy as np

from model.nfl_absence_reallocation import DEFAULT_CACHE, GAMES_URL, SOURCES, fetch
from model.nfl_dfs_historical import (
    MODEL_CONFIG,
    OFFENSE_FIELDS,
    HistoricalWeek,
    ProjectionContext,
    before_cutoff,
    project_player,
)
from model.nfl_qb_starter import thin_starting_history
from model.nfl_team_aliases import normalize_team

STUDY_ID = "nfl-dfs-participation-ledger-v1"
GATE = "H"
# SHA-256 of model/nfl_dfs_historical.py recorded in the registration (2026-10-20),
# taken from a CRLF checkout; the LF form of the same file is e68c45b3...b5b55.
MODEL_FILE_SHA256 = "63d0353099727a292ef2a1255baf312a6cadf74bc3da38bc507709e41e3afc30"
MODEL_FILE = Path(__file__).resolve().parent / "nfl_dfs_historical.py"

SEED = 20260902                 # production default; Amendment 1
BOOTSTRAP_SEED = 20261020       # registration
BOOTSTRAP_DRAWS = 10_000
TARGET_WEEKS = tuple(range(4, 19))
POSITIONS = ("RB", "TE", "WR")  # RB is the gated position
SKILL = ("QB", "RB", "WR", "TE")
HISTORY_SEASONS_BACK = 2        # Amendment 1: target season and the two before it
BASELINE_MIN_PROJECTION = 5.0
LEDGER_STATUSES = frozenset({"ACT", "DEV"})
POPULATION_STATUSES = frozenset({"ACT", "INA", "DEV"})
MAE_GUARD_UPPER = 0.10          # 95% interval upper bound, DK points
P90_GUARD_MAX_POINT = 0.01
GATE_SEASONS = (2024, 2025)
ARTIFACT_DIR = Path(__file__).resolve().parents[1] / "artifacts" / "nfl-dfs-participation-ledger-gate-h"
BANDS = ((5.0, 10.0, "5-10"), (10.0, 15.0, "10-15"), (15.0, float("inf"), "15+"))


def model_file_sha256() -> str:
    return hashlib.sha256(MODEL_FILE.read_bytes()).hexdigest()


def model_file_matches_pin(content: bytes | None = None) -> bool:
    """The registered pin was taken in a CRLF (Windows) checkout; CI checks out LF.

    Same rule as ``ingest.nfl_dfs_shadow.matches_source_digest``: the file
    matches if its bytes, or the same text with LF or CRLF line endings, hash to
    the pin. Any change to the text itself still fails.
    """
    raw = MODEL_FILE.read_bytes() if content is None else content
    lf = raw.replace(b"\r\n", b"\n")
    return MODEL_FILE_SHA256 in {hashlib.sha256(value).hexdigest()
                                 for value in (raw, lf, lf.replace(b"\n", b"\r\n"))}


def require_pinned_model() -> None:
    actual = model_file_sha256()
    if not model_file_matches_pin():
        raise SystemExit(
            f"model/nfl_dfs_historical.py is {actual}, not the registered {MODEL_FILE_SHA256}; "
            "Gate H must run against the frozen model."
        )


# ---------------------------------------------------------------------------
# Sources
# ---------------------------------------------------------------------------

@dataclass
class TeamGameRef:
    game_id: str
    season: int
    week: int
    team: str
    opponent: str
    home: bool
    implied_total: float | None


@dataclass
class Sources:
    seasons: tuple[int, ...]
    games: dict[tuple[int, int, str], TeamGameRef]           # (season, week, team) -> game
    roster: dict[tuple[int, int], dict[str, tuple[str, str, str]]]  # (season, week) -> gsis -> (status, team, position)
    roster_names: dict[str, str]
    stat_rows: list[HistoricalWeek]                            # QB/RB/WR/TE, REG, all loaded seasons
    canonical_position: dict[str, str]                          # gsis -> position used for projection
    player_ids: dict[str, int]                                  # gsis -> stable int
    digests: dict[str, str]
    counts: dict[str, int]


def _implied(total: Any, spread: Any, home: bool) -> float | None:
    """nflverse convention: positive spread_line = home favoured."""
    try:
        t = float(total)
        s = float(spread)
    except (TypeError, ValueError):
        return None
    if not np.isfinite(t) or not np.isfinite(s):
        return None
    return (t + s) / 2.0 if home else (t - s) / 2.0


def load_sources(target_season: int, cache: Path = DEFAULT_CACHE, history_back: int = HISTORY_SEASONS_BACK) -> Sources:
    import pandas as pd

    seasons = tuple(range(target_season - history_back, target_season + 1))
    digests: dict[str, str] = {}
    counts: dict[str, int] = Counter()

    games_bytes = fetch(GAMES_URL, cache)
    digests["games.csv"] = hashlib.sha256(games_bytes).hexdigest()
    schedule = pd.read_csv(io.BytesIO(games_bytes), low_memory=False)
    schedule = schedule[(schedule["game_type"] == "REG") & schedule["season"].isin(seasons)]
    games: dict[tuple[int, int, str], TeamGameRef] = {}
    for row in schedule.itertuples(index=False):
        season, week = int(row.season), int(row.week)
        home, away = normalize_team(row.home_team), normalize_team(row.away_team)
        total = getattr(row, "total_line", None)
        spread = getattr(row, "spread_line", None)
        games[(season, week, home)] = TeamGameRef(str(row.game_id), season, week, home, away, True, _implied(total, spread, True))
        games[(season, week, away)] = TeamGameRef(str(row.game_id), season, week, away, home, False, _implied(total, spread, False))
        if _implied(total, spread, True) is None:
            counts["games_without_lines"] += 1

    roster: dict[tuple[int, int], dict[str, tuple[str, str, str]]] = defaultdict(dict)
    roster_names: dict[str, str] = {}
    raw_stats: list[dict[str, Any]] = []
    stats_position_votes: dict[str, Counter] = defaultdict(Counter)
    roster_position_votes: dict[str, Counter] = defaultdict(Counter)

    for season in seasons:
        roster_bytes = fetch(SOURCES["roster"].format(season=season), cache)
        digests[f"roster_weekly_{season}.csv"] = hashlib.sha256(roster_bytes).hexdigest()
        rframe = pd.read_csv(
            io.BytesIO(roster_bytes), low_memory=False,
            usecols=["season", "week", "team", "gsis_id", "status", "position", "full_name", "game_type"],
        )
        rframe = rframe[(rframe["game_type"] == "REG") & rframe["gsis_id"].notna()]
        for row in rframe.itertuples(index=False):
            gsis = str(row.gsis_id)
            position = str(row.position)
            roster[(int(row.season), int(row.week))][gsis] = (str(row.status), normalize_team(row.team) or "", position)
            roster_names.setdefault(gsis, str(row.full_name))
            roster_position_votes[gsis][position] += 1
            counts["roster_rows"] += 1

        stats_bytes = fetch(SOURCES["stats"].format(season=season), cache)
        digests[f"stats_player_week_{season}.csv"] = hashlib.sha256(stats_bytes).hexdigest()
        sframe = pd.read_csv(io.BytesIO(stats_bytes), low_memory=False)
        sframe = sframe[(sframe["season_type"] == "REG") & sframe["position"].isin(SKILL) & sframe["player_id"].notna()]
        for row in sframe.itertuples(index=False):
            gsis = str(row.player_id)
            stats = {}
            for key in OFFENSE_FIELDS:
                value = getattr(row, key, None)
                stats[key] = float(value) if value is not None and pd.notna(value) else 0.0
            raw_stats.append({
                "gsis": gsis, "name": str(row.player_display_name), "position": str(row.position),
                "season": int(row.season), "week": int(row.week),
                "team": normalize_team(row.team), "opponent": normalize_team(getattr(row, "opponent_team", None)),
                "stats": stats,
            })
            stats_position_votes[gsis][str(row.position)] += 1
            counts["stat_rows"] += 1

    canonical_position: dict[str, str] = {}
    for gsis, votes in stats_position_votes.items():
        canonical_position[gsis] = votes.most_common(1)[0][0]
    for gsis, votes in roster_position_votes.items():
        canonical_position.setdefault(gsis, votes.most_common(1)[0][0])

    all_ids = sorted(set(canonical_position))
    player_ids = {gsis: index + 1 for index, gsis in enumerate(all_ids)}

    stat_rows = [
        HistoricalWeek(
            player_id=player_ids[r["gsis"]], player_gsis_id=r["gsis"], player_name=r["name"],
            position=canonical_position[r["gsis"]], season=r["season"], week=r["week"],
            team=r["team"], opponent=r["opponent"], stats=r["stats"],
        )
        for r in raw_stats
    ]
    return Sources(seasons, games, dict(roster), roster_names, stat_rows, canonical_position, player_ids, digests, dict(counts))


# ---------------------------------------------------------------------------
# The ledger
# ---------------------------------------------------------------------------

ZERO_LINE: Mapping[str, float] = {key: 0.0 for key in OFFENSE_FIELDS}


def build_ledger(
    stat_rows: Sequence[HistoricalWeek],
    roster: Mapping[tuple[int, int], Mapping[str, tuple[str, str, str]]],
    games: Mapping[tuple[int, int, str], TeamGameRef],
    canonical_position: Mapping[str, str],
    player_ids: Mapping[str, int],
    names: Mapping[str, str],
    *,
    positions: Iterable[str] = SKILL,
    ledger_statuses: frozenset[str] = LEDGER_STATUSES,
) -> tuple[list[HistoricalWeek], dict[str, int]]:
    """Stat lines plus a zero line for every ACT/DEV team game with no stat line.

    A stat line with no roster week is kept untouched; a roster week with no
    team game (bye) adds nothing; INA/RES/other statuses add nothing.
    """
    wanted = set(positions)
    have = {(row.player_gsis_id, row.season, row.week) for row in stat_rows}
    out = list(stat_rows)
    counts: Counter = Counter()
    for (season, week), players in roster.items():
        for gsis, (status, team, _roster_position) in players.items():
            if status not in ledger_statuses:
                continue
            position = canonical_position.get(gsis)
            if position not in wanted:
                continue
            game = games.get((season, week, team))
            if game is None:
                counts["ledger_bye_or_no_game"] += 1
                continue
            if (gsis, season, week) in have:
                continue
            out.append(HistoricalWeek(
                player_id=player_ids[gsis], player_gsis_id=gsis, player_name=names.get(gsis, gsis),
                position=position, season=season, week=week, team=team, opponent=game.opponent,
                stats=ZERO_LINE,
            ))
            counts[f"zero_rows_{status}"] += 1
            counts["zero_rows"] += 1
    out.sort(key=lambda row: (row.season, row.week, row.player_id))
    return out, dict(counts)


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------

def pinball(tau: float, quantile: float, actual: float) -> float:
    """(tau - 1[actual < q]) * (actual - q); the registered loss."""
    indicator = 1.0 if actual < quantile else 0.0
    return (tau - indicator) * (actual - quantile)


def game_bootstrap(values: Sequence[float], groups: Sequence[str], *, draws: int = BOOTSTRAP_DRAWS, seed: int = BOOTSTRAP_SEED) -> dict[str, float | int]:
    """Mean with a 95% interval from resampling whole games (both teams together)."""
    values = np.asarray(values, dtype=float)
    if values.size == 0:
        return {"mean": float("nan"), "lo": float("nan"), "hi": float("nan"), "n": 0, "games": 0}
    keys = sorted(set(groups))
    index = {key: i for i, key in enumerate(keys)}
    sums = np.zeros(len(keys))
    counts = np.zeros(len(keys))
    for value, group in zip(values, groups):
        sums[index[group]] += value
        counts[index[group]] += 1
    rng = np.random.default_rng(seed)
    picks = rng.integers(0, len(keys), size=(draws, len(keys)))
    means = sums[picks].sum(axis=1) / counts[picks].sum(axis=1)
    return {
        "mean": float(values.mean()),
        "lo": float(np.quantile(means, 0.025)),
        "hi": float(np.quantile(means, 0.975)),
        "n": int(values.size),
        "games": len(keys),
    }


def summarize(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Every registered metric for one set of rows (one position, one season)."""
    if not rows:
        return {"n": 0}
    games = [r["game_id"] for r in rows]
    actual = np.array([r["actual"] for r in rows])
    b10 = np.array([r["base_p10"] for r in rows]); c10 = np.array([r["cand_p10"] for r in rows])
    b90 = np.array([r["base_p90"] for r in rows]); c90 = np.array([r["cand_p90"] for r in rows])
    bm = np.array([r["base_mean"] for r in rows]); cm = np.array([r["cand_mean"] for r in rows])
    pin10 = [pinball(0.10, c, a) - pinball(0.10, b, a) for a, b, c in zip(actual, b10, c10)]
    pin90 = [pinball(0.90, c, a) - pinball(0.90, b, a) for a, b, c in zip(actual, b90, c90)]
    mae = list(np.abs(cm - actual) - np.abs(bm - actual))
    played = [i for i, r in enumerate(rows) if r["played"]]
    out = {
        "n": len(rows),
        "games": len(set(games)),
        "p10_pinball_delta": game_bootstrap(pin10, games),
        "p90_pinball_delta_point": float(np.mean(pin90)),
        "mae_delta": game_bootstrap(mae, games),
        "mae_delta_played": game_bootstrap([mae[i] for i in played], [games[i] for i in played]),
        "below_p10_rate": {"baseline": float(np.mean(actual < b10)), "candidate": float(np.mean(actual < c10))},
        "mean_projection": {"baseline": float(bm.mean()), "candidate": float(cm.mean())},
        "mean_p10": {"baseline": float(b10.mean()), "candidate": float(c10.mean())},
        "moved_more_than_1pt": int(np.sum(np.abs(cm - bm) > 1.0)),
    }
    splits: dict[str, Any] = {}
    def split(name: str, mask: Sequence[bool]) -> None:
        idx = [i for i, m in enumerate(mask) if m]
        if not idx:
            splits[name] = {"n": 0}
            return
        splits[name] = {
            "n": len(idx),
            "p10_pinball_delta": game_bootstrap([pin10[i] for i in idx], [games[i] for i in idx], draws=2000),
            "mae_delta_point": float(np.mean([mae[i] for i in idx])),
            "below_p10_rate": {"baseline": float(np.mean(actual[idx] < b10[idx])), "candidate": float(np.mean(actual[idx] < c10[idx]))},
        }
    split("played", [r["played"] for r in rows])
    split("did_not_play", [not r["played"] for r in rows])
    for status in ("ACT", "INA", "DEV"):
        split(f"status_{status}", [r["status"] == status for r in rows])
    for lo, hi, label in BANDS:
        split(f"band_{label}", [lo <= r["base_mean"] < hi for r in rows])
    out["splits"] = splits
    return out


def guards(summary: Mapping[str, Any]) -> dict[str, Any]:
    """The registered guards for one position-season, each with its reading."""
    if summary.get("n", 0) == 0:
        return {"evaluable": False}
    rate = summary["below_p10_rate"]
    rate_ok = abs(rate["candidate"] - 0.10) < abs(rate["baseline"] - 0.10)
    mae_ok = summary["mae_delta"]["hi"] < MAE_GUARD_UPPER
    mae_played_ok = summary["mae_delta_played"]["hi"] < MAE_GUARD_UPPER  # Amendment 1
    p90_ok = summary["p90_pinball_delta_point"] <= P90_GUARD_MAX_POINT
    primary_below_zero = summary["p10_pinball_delta"]["hi"] < 0.0
    return {
        "evaluable": True,
        "primary_ci_wholly_below_zero": primary_below_zero,
        "below_p10_rate_closer_to_10pct": rate_ok,
        "mae_upper_below_0_10": mae_ok,
        "mae_played_upper_below_0_10": mae_played_ok,
        "p90_point_at_most_0_01": p90_ok,
        "all_guards_hold": rate_ok and mae_ok and mae_played_ok and p90_ok,
    }


def gate_h_verdict(per_season: Mapping[int, Mapping[str, Any]]) -> dict[str, Any]:
    """KILL / H_CLEARED / INCONCLUSIVE from the RB guards of both seasons.

    Gate H alone cannot PASS (Amendment 1): H_CLEARED means Gate F still runs.
    """
    missing = [s for s in GATE_SEASONS if s not in per_season]
    if missing:
        return {"verdict": "INCOMPLETE", "reason": f"seasons not yet run: {missing}"}
    readings = {s: guards(per_season[s]) for s in GATE_SEASONS}
    if any(not r.get("evaluable") for r in readings.values()):
        return {"verdict": "INCONCLUSIVE", "reason": "a season has no RB rows", "readings": readings}
    if any(not r["primary_ci_wholly_below_zero"] for r in readings.values()):
        return {"verdict": "KILL", "reason": "RB P10 pinball interval reaches zero in at least one season", "readings": readings}
    if all(r["all_guards_hold"] for r in readings.values()):
        return {"verdict": "H_CLEARED", "reason": "RB interval wholly below zero and every guard holds in both seasons; Gate F (2026 weeks 7-14) still required", "readings": readings}
    return {"verdict": "INCONCLUSIVE", "reason": "RB interval below zero but a guard failed", "readings": readings}


# ---------------------------------------------------------------------------
# The replay
# ---------------------------------------------------------------------------

def _own_rows(rows: Sequence[HistoricalWeek], gsis: str, season: int, week: int) -> list[HistoricalWeek]:
    own = sorted((r for r in rows if r.player_gsis_id == gsis and before_cutoff(r, season, week)), key=lambda r: r.chronological_key)
    return own[-int(MODEL_CONFIG["max_player_games"]):]


def run_season(
    season: int,
    *,
    cache: Path = DEFAULT_CACHE,
    weeks: Sequence[int] = TARGET_WEEKS,
    positions: Sequence[str] = POSITIONS,
    log=print,
) -> dict[str, Any]:
    started = time.time()
    sources = load_sources(season, cache)
    ledger, ledger_counts = build_ledger(
        sources.stat_rows, sources.roster, sources.games, sources.canonical_position, sources.player_ids, sources.roster_names,
    )
    log(f"[{season}] sources: {sources.counts}; ledger: {ledger_counts}")

    base_by_pos: dict[str, list[HistoricalWeek]] = defaultdict(list)
    cand_by_pos: dict[str, list[HistoricalWeek]] = defaultdict(list)
    for row in sources.stat_rows:
        base_by_pos[row.position].append(row)
    for row in ledger:
        cand_by_pos[row.position].append(row)

    rows: list[dict[str, Any]] = []
    qb_overlay: list[dict[str, Any]] = []
    excluded: Counter = Counter()
    stat_index = {(r.player_gsis_id, r.season, r.week): r for r in sources.stat_rows}

    for week in weeks:
        week_started = time.time()
        roster = sources.roster.get((season, week), {})
        # QB overlay disclosure (Amendment 1): how many ACT quarterbacks change thin-history verdict.
        base_q = [r for r in base_by_pos["QB"] if before_cutoff(r, season, week)]
        cand_q = [r for r in cand_by_pos["QB"] if before_cutoff(r, season, week)]
        changed = 0; qbs = 0
        for gsis, (status, team, _pos) in roster.items():
            if status != "ACT" or sources.canonical_position.get(gsis) != "QB" or (season, week, team) not in sources.games:
                continue
            qbs += 1
            if thin_starting_history(_own_rows(base_q, gsis, season, week)) != thin_starting_history(_own_rows(cand_q, gsis, season, week)):
                changed += 1
        qb_overlay.append({"week": week, "act_qbs": qbs, "thin_history_verdict_changed": changed})

        for position in positions:
            prior_base = [r for r in base_by_pos[position] if before_cutoff(r, season, week)]
            prior_cand = [r for r in cand_by_pos[position] if before_cutoff(r, season, week)]
            for gsis, (status, team, _roster_position) in roster.items():
                if status not in POPULATION_STATUSES or sources.canonical_position.get(gsis) != position:
                    continue
                game = sources.games.get((season, week, team))
                if game is None:
                    excluded["population_bye_or_no_game"] += 1
                    continue
                context = ProjectionContext(team_implied_total=game.implied_total)
                name = sources.roster_names.get(gsis, gsis)
                base = project_player(
                    player_id=sources.player_ids[gsis], player_gsis_id=gsis, player_name=name, position=position,
                    historical_rows=prior_base, cutoff_season=season, cutoff_week=week, context=context, seed=SEED,
                )
                if base.model_proj_fpts is None:
                    excluded["baseline_unavailable"] += 1
                    continue
                if base.model_proj_fpts < BASELINE_MIN_PROJECTION:
                    excluded["baseline_under_5"] += 1
                    continue
                cand = project_player(
                    player_id=sources.player_ids[gsis], player_gsis_id=gsis, player_name=name, position=position,
                    historical_rows=prior_cand, cutoff_season=season, cutoff_week=week, context=context, seed=SEED,
                )
                if cand.model_proj_fpts is None:
                    excluded["candidate_unavailable"] += 1
                    continue
                stat = stat_index.get((gsis, season, week))
                rows.append({
                    "season": season, "week": week, "game_id": game.game_id, "team": team, "opponent": game.opponent,
                    "gsis": gsis, "name": name, "position": position, "status": status,
                    "played": stat is not None, "actual": round(stat.dk_points, 4) if stat is not None else 0.0,
                    "base_mean": base.model_proj_fpts, "base_p10": base.floor_fpts, "base_p90": base.ceiling_fpts,
                    "base_status": base.projection_status, "base_history": base.history_games,
                    "cand_mean": cand.model_proj_fpts, "cand_p10": cand.floor_fpts, "cand_p90": cand.ceiling_fpts,
                    "cand_status": cand.projection_status, "cand_history": cand.history_games,
                    "implied_total": game.implied_total,
                })
        log(f"[{season}] week {week}: {sum(1 for r in rows if r['week'] == week)} rows, {time.time() - week_started:.0f}s")

    # Players with a target-week stat line but no roster row (outside the population by definition).
    no_roster = 0
    for (gsis, s, w), row in stat_index.items():
        if s == season and w in weeks and row.position in positions and gsis not in sources.roster.get((s, w), {}):
            no_roster += 1

    by_position = {pos: summarize([r for r in rows if r["position"] == pos]) for pos in positions}
    artifact = {
        "study_id": STUDY_ID, "gate": GATE, "season": season, "weeks": list(weeks), "positions": list(positions),
        "generated_at": datetime.now(timezone.utc).isoformat(), "runtime_seconds": round(time.time() - started, 1),
        "model_file_sha256": model_file_sha256(), "seed": SEED, "bootstrap": {"seed": BOOTSTRAP_SEED, "draws": BOOTSTRAP_DRAWS},
        "history_seasons": list(sources.seasons), "source_digests": sources.digests,
        "source_counts": sources.counts, "ledger_counts": ledger_counts,
        "excluded": dict(excluded), "stat_line_without_roster_row": no_roster,
        "qb_overlay_disclosure": qb_overlay,
        "metrics": by_position,
        "rb_guards": guards(by_position.get("RB", {})),
        "rows": rows,
    }
    return artifact


def write_artifact(artifact: Mapping[str, Any], directory: Path = ARTIFACT_DIR) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{artifact['season']}.json"
    path.write_text(json.dumps(artifact, indent=1, default=str), encoding="utf-8")
    return path


def _fmt_ci(reading: Mapping[str, Any]) -> str:
    return f"{reading['mean']:+.4f} [{reading['lo']:+.4f}, {reading['hi']:+.4f}] n={reading['n']} games={reading['games']}"


def report(artifact: Mapping[str, Any]) -> str:
    lines = [f"Gate H {artifact['season']}  ({STUDY_ID}, model {artifact['model_file_sha256'][:12]})"]
    lines.append(f"  excluded: {artifact['excluded']}  stat line w/o roster row: {artifact['stat_line_without_roster_row']}")
    for pos, m in artifact["metrics"].items():
        if not m.get("n"):
            lines.append(f"  {pos}: no rows"); continue
        lines.append(f"  {pos}: n={m['n']} games={m['games']}")
        lines.append(f"    P10 pinball delta (cand - base): {_fmt_ci(m['p10_pinball_delta'])}")
        lines.append(f"    below-P10 rate: base {m['below_p10_rate']['baseline']:.3f} -> cand {m['below_p10_rate']['candidate']:.3f}")
        lines.append(f"    MAE delta all: {_fmt_ci(m['mae_delta'])}")
        lines.append(f"    MAE delta played: {_fmt_ci(m['mae_delta_played'])}")
        lines.append(f"    P90 pinball delta point: {m['p90_pinball_delta_point']:+.4f}   moved >1pt: {m['moved_more_than_1pt']}")
        lines.append(f"    mean proj base {m['mean_projection']['baseline']:.2f} cand {m['mean_projection']['candidate']:.2f}; mean P10 base {m['mean_p10']['baseline']:.2f} cand {m['mean_p10']['candidate']:.2f}")
        for name, s in m["splits"].items():
            if s.get("n"):
                lines.append(f"      {name}: n={s['n']} pinball {_fmt_ci(s['p10_pinball_delta'])} rate {s['below_p10_rate']['baseline']:.3f}->{s['below_p10_rate']['candidate']:.3f}")
    qb = artifact["qb_overlay_disclosure"]
    lines.append(f"  QB overlay: thin-history verdict changed for {sum(q['thin_history_verdict_changed'] for q in qb)} of {sum(q['act_qbs'] for q in qb)} ACT QB-weeks")
    lines.append(f"  RB guards: {artifact['rb_guards']}")
    return "\n".join(lines)


def verdict_from_artifacts(directory: Path = ARTIFACT_DIR) -> dict[str, Any]:
    per_season: dict[int, Mapping[str, Any]] = {}
    for season in GATE_SEASONS:
        path = directory / f"{season}.json"
        if path.exists():
            artifact = json.loads(path.read_text(encoding="utf-8"))
            per_season[season] = artifact["metrics"].get("RB", {})
    verdict = gate_h_verdict(per_season)
    verdict["decided_at"] = datetime.now(timezone.utc).isoformat()
    verdict["study_id"] = STUDY_ID
    (directory / "verdict.json").write_text(json.dumps(verdict, indent=1, default=str), encoding="utf-8")
    return verdict


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--season", type=int, choices=GATE_SEASONS)
    parser.add_argument("--weeks", default=None, help="e.g. 4-18 (default) or 4-5 for a smoke run; a smoke run is not a gate result")
    parser.add_argument("--positions", default=",".join(POSITIONS))
    parser.add_argument("--cache", type=Path, default=DEFAULT_CACHE)
    parser.add_argument("--artifact-dir", type=Path, default=ARTIFACT_DIR)
    parser.add_argument("--verdict", action="store_true", help="apply the registered rule to the stored season artifacts")
    args = parser.parse_args(argv)

    require_pinned_model()
    if args.verdict:
        verdict = verdict_from_artifacts(args.artifact_dir)
        print(json.dumps(verdict, indent=1, default=str))
        return 0
    if args.season is None:
        parser.error("--season or --verdict is required")
    weeks: Sequence[int] = TARGET_WEEKS
    if args.weeks:
        lo, hi = (int(x) for x in args.weeks.split("-"))
        weeks = tuple(range(lo, hi + 1))
    positions = tuple(p.strip().upper() for p in args.positions.split(",") if p.strip())
    artifact = run_season(args.season, cache=args.cache, weeks=weeks, positions=positions, log=lambda m: print(m, flush=True))
    if tuple(weeks) != TARGET_WEEKS or positions != POSITIONS:
        artifact["smoke_run"] = True
        path = args.artifact_dir / f"smoke-{args.season}.json"
        args.artifact_dir.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(artifact, indent=1, default=str), encoding="utf-8")
    else:
        path = write_artifact(artifact, args.artifact_dir)
    print(report(artifact))
    print(f"artifact: {path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
