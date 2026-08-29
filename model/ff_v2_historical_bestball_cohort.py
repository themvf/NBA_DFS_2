"""Deterministic historical Best Ball outcome cohort for V2 validation.

This module builds synthetic, focal-seat ADP baselines.  They are retrospective
validation fixtures, not observed human drafts.  Market ADP determines the
draft-time ordering; held-out weekly stats are read only to score outcomes.
Nothing here is connected to the live projection or ranking paths.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import pandas as pd
import requests


MODEL_VERSION = "ff-v2-historical-bestball-cohort-v1"
FFC_URL = "https://fantasyfootballcalculator.com/api/v1/adp/ppr?teams=12&year={season}"
SEASONS = tuple(range(2020, 2026))
WEEKS = tuple(range(1, 18))
POSITIONS = ("QB", "RB", "WR", "TE")
ROSTER_POLICY = {"QB": 2, "RB": 6, "WR": 9, "TE": 3}
LINEUP_POLICY = {"QB": 1, "RB": 2, "WR": 3, "TE": 1, "FLEX": 1}
RESAMPLING_UNIT = "synthetic focal-seat roster-season"


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def normalize_name(value: object) -> str:
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = "".join(char for char in text if not unicodedata.combining(char)).lower()
    text = re.sub(r"\b(jr|sr|ii|iii|iv)\b", "", text)
    return re.sub(r"[^a-z0-9]+", "", text)


def number(row: Mapping[str, Any], key: str) -> float:
    value = row.get(key, 0)
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return 0.0
    return float(value)


def draftkings_points(row: Mapping[str, Any]) -> float:
    """Exact current DraftKings NFL offensive-player scoring."""
    passing_yards = number(row, "passing_yards")
    rushing_yards = number(row, "rushing_yards")
    receiving_yards = number(row, "receiving_yards")
    points = (
        passing_yards / 25.0
        + 4.0 * number(row, "passing_tds")
        - number(row, "passing_interceptions")
        + rushing_yards / 10.0
        + 6.0 * number(row, "rushing_tds")
        + receiving_yards / 10.0
        + 6.0 * number(row, "receiving_tds")
        + number(row, "receptions")
        + 2.0 * (
            number(row, "passing_2pt_conversions")
            + number(row, "rushing_2pt_conversions")
            + number(row, "receiving_2pt_conversions")
        )
        + 6.0 * (number(row, "special_teams_tds") + number(row, "fumble_recovery_tds"))
        - number(row, "fumbles_lost_total")
    )
    points += 3.0 if passing_yards >= 300 else 0.0
    points += 3.0 if rushing_yards >= 100 else 0.0
    points += 3.0 if receiving_yards >= 100 else 0.0
    return round(points, 6)


@dataclass(frozen=True)
class WeeklyPlayer:
    key: str
    name: str
    position: str
    points: float


def maximum_legal_lineup(players: Sequence[WeeklyPlayer]) -> tuple[float, tuple[str, ...]]:
    """Return the maximum legal 1/2/3/1/1-flex lineup for one week."""
    by_position = {
        position: sorted(
            (player for player in players if player.position == position),
            key=lambda player: (-player.points, player.key),
        )
        for position in POSITIONS
    }
    selected: list[WeeklyPlayer] = []
    for position, count in (("QB", 1), ("RB", 2), ("WR", 3), ("TE", 1)):
        selected.extend(by_position[position][:count])
    selected_keys = {player.key for player in selected}
    flex = sorted(
        (
            player
            for player in players
            if player.position in {"RB", "WR", "TE"} and player.key not in selected_keys
        ),
        key=lambda player: (-player.points, player.key),
    )
    if flex:
        selected.append(flex[0])
    return round(sum(player.points for player in selected), 6), tuple(player.key for player in selected)


def snake_overall_pick(seat: int, round_number: int, teams: int = 12) -> int:
    return (round_number - 1) * teams + (seat if round_number % 2 else teams + 1 - seat)


def _adp_players(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for source_index, raw in enumerate(payload.get("players", []), start=1):
        position = str(raw.get("position") or "").upper()
        name = str(raw.get("name") or "").strip()
        if position not in POSITIONS or not name:
            continue
        adp = float(raw.get("adp") or source_index)
        rows.append({
            "key": f"{normalize_name(name)}:{position}",
            "name": name,
            "position": position,
            "team": raw.get("team"),
            "adp": adp,
            "times_drafted": int(raw.get("times_drafted") or 0),
            "source_index": source_index,
        })
    return sorted(rows, key=lambda row: (row["adp"], row["source_index"], row["key"]))


def build_focal_seat_roster(payload: Mapping[str, Any], seat: int) -> list[dict[str, Any]]:
    """Sample a 20-player focal roster along one 12-team snake pick path.

    FFC's historical endpoint is a redraft board and is shorter than a complete
    240-player Best Ball room in every evaluated season.  Opponent rosters are
    therefore not invented.  At each focal pick we take the closest remaining
    ADP row, preferring rows at or after the nominal pick, while enforcing the
    declared final position policy.  The artifact labels this limitation.
    """
    if not 1 <= seat <= 12:
        raise ValueError("seat must be between 1 and 12")
    pool = _adp_players(payload)
    selected: list[dict[str, Any]] = []
    counts = {position: 0 for position in POSITIONS}
    for round_number in range(1, 21):
        target = snake_overall_pick(seat, round_number)
        candidates = [
            row for row in pool
            if row["key"] not in {pick["key"] for pick in selected}
            and counts[row["position"]] < ROSTER_POLICY[row["position"]]
        ]
        remaining_picks = 21 - round_number
        must_fill = {
            position for position in POSITIONS
            if ROSTER_POLICY[position] - counts[position] == remaining_picks
        }
        if must_fill:
            candidates = [row for row in candidates if row["position"] in must_fill]
        if not candidates:
            raise ValueError(f"ADP board cannot fill seat {seat} under roster policy")
        after = [row for row in candidates if row["adp"] >= target]
        chosen = min(after or candidates, key=lambda row: (abs(row["adp"] - target), row["adp"], row["key"]))
        pick = dict(chosen, round=round_number, nominal_overall_pick=target)
        selected.append(pick)
        counts[chosen["position"]] += 1
    if counts != ROSTER_POLICY:
        raise AssertionError(f"roster policy mismatch: {counts}")
    return selected


def _weekly_outcomes(frame: pd.DataFrame) -> tuple[dict[str, dict[int, float]], dict[str, dict[str, Any]]]:
    eligible = frame[
        frame["position"].isin(POSITIONS)
        & (frame["season_type"] == "REG")
        & frame["week"].between(1, 17)
    ].copy()
    points: dict[str, dict[int, float]] = {}
    identities: dict[str, dict[str, Any]] = {}
    for row in eligible.to_dict("records"):
        position = str(row["position"])
        name = str(row.get("player_display_name") or row.get("player_name") or "")
        key = f"{normalize_name(name)}:{position}"
        if not normalize_name(name):
            continue
        points.setdefault(key, {})[int(row["week"])] = draftkings_points(row)
        identities[key] = {"player_id": row.get("player_id"), "name": name, "position": position}
    return points, identities


def score_roster(roster: Sequence[Mapping[str, Any]], weekly: Mapping[str, Mapping[int, float]]) -> dict[str, Any]:
    counted_weeks = {str(row["key"]): 0 for row in roster}
    weekly_rows: list[dict[str, Any]] = []
    for week in WEEKS:
        candidates = [
            WeeklyPlayer(str(row["key"]), str(row["name"]), str(row["position"]), float(weekly.get(str(row["key"]), {}).get(week, 0.0)))
            for row in roster
        ]
        total, selected = maximum_legal_lineup(candidates)
        for key in selected:
            counted_weeks[key] += 1
        weekly_rows.append({"week": week, "counted_points": total, "selected": list(selected)})
    return {
        "counted_points": round(sum(row["counted_points"] for row in weekly_rows), 6),
        "counted_weeks": counted_weeks,
        "weekly": weekly_rows,
    }


def roster_match_report(roster: Sequence[Mapping[str, Any]], weekly: Mapping[str, Mapping[int, float]]) -> dict[str, Any]:
    """Keep unresolved names explicit instead of treating them as matched zeroes."""
    matched = [str(row["name"]) for row in roster if str(row["key"]) in weekly]
    missing = [str(row["name"]) for row in roster if str(row["key"]) not in weekly]
    return {"matched_roster_players": len(matched), "missing_roster_players": missing}


def _pick_regret(
    roster: Sequence[Mapping[str, Any]],
    pick: Mapping[str, Any],
    pool: Sequence[Mapping[str, Any]],
    weekly: Mapping[str, Mapping[int, float]],
    baseline_points: float,
) -> dict[str, Any]:
    roster_keys = {str(row["key"]) for row in roster}
    # A later-or-equal ADP player at the same position is the conservative set
    # that was plausibly available. Same-position replacement preserves both
    # the 20-player draft policy and weekly lineup legality.
    alternatives = [
        row for row in pool
        if row["position"] == pick["position"]
        and row["adp"] >= pick["adp"]
        and row["key"] not in roster_keys
        and row["key"] in weekly
    ]
    best_name = None
    best_points = baseline_points
    for alternative in alternatives:
        replaced = [alternative if row["key"] == pick["key"] else row for row in roster]
        candidate_points = float(score_roster(replaced, weekly)["counted_points"])
        if candidate_points > best_points or (candidate_points == best_points and str(alternative["key"]) < str(best_name or "~")):
            best_points = candidate_points
            best_name = str(alternative["name"])
    return {
        "round": pick["round"],
        "nominal_overall_pick": pick["nominal_overall_pick"],
        "picked_player": pick["name"],
        "position": pick["position"],
        "best_available_same_position": best_name,
        "baseline_regret": round(max(0.0, best_points - baseline_points), 6),
        "eligible_alternatives": len(alternatives),
    }


def _latest(cache_root: Path, source: str, season: int) -> tuple[Path, str, dict[str, Any]]:
    metadata = json.loads((cache_root / source / str(season) / "latest.json").read_text(encoding="utf-8"))
    path = Path(metadata["cache_path"])
    if not path.is_absolute():
        # Cache metadata is repository-relative.
        path = Path.cwd() / path
    content = path.read_bytes()
    digest = sha256_bytes(content)
    if digest != metadata["response_hash"]:
        raise ValueError(f"hash mismatch for {path}")
    return path, digest, metadata


def fetch_ffc(season: int, cache_root: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    url = FFC_URL.format(season=season)
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    content = response.content
    digest = sha256_bytes(content)
    directory = cache_root / "ffc-adp-ppr" / str(season)
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{digest}.json"
    if not path.exists():
        path.write_bytes(content)
    metadata = {
        "url": url,
        "cache_path": str(path),
        "response_hash": digest,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "row_count": len(response.json().get("players", [])),
    }
    (directory / "latest.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    return response.json(), metadata


def load_cached_ffc(season: int, cache_root: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    path, digest, metadata = _latest(cache_root, "ffc-adp-ppr", season)
    metadata = dict(metadata, response_hash=digest)
    return json.loads(path.read_text(encoding="utf-8")), metadata


def build_artifact(cache_root: Path, refresh_adp: bool = False) -> dict[str, Any]:
    season_rows: list[dict[str, Any]] = []
    sources: list[dict[str, Any]] = []
    for season in SEASONS:
        adp_payload, adp_meta = fetch_ffc(season, cache_root) if refresh_adp else load_cached_ffc(season, cache_root)
        stats_path, stats_digest, stats_meta = _latest(cache_root, "weekly-stats", season)
        frame = pd.read_parquet(stats_path)
        weekly, identities = _weekly_outcomes(frame)
        pool = _adp_players(adp_payload)
        sources.extend([
            {"season": season, "kind": "ffc-adp-ppr", **adp_meta},
            {"season": season, "kind": "nflverse-weekly-stats", **stats_meta, "response_hash": stats_digest},
        ])
        rosters: list[dict[str, Any]] = []
        matched_unique = {row["key"] for row in pool if row["key"] in weekly}
        for seat in range(1, 13):
            roster = build_focal_seat_roster(adp_payload, seat)
            scored = score_roster(roster, weekly)
            match_report = roster_match_report(roster, weekly)
            regrets = [_pick_regret(roster, pick, pool, weekly, scored["counted_points"]) for pick in roster]
            rosters.append({
                "cohort_id": f"{season}-seat-{seat:02d}",
                "season": season,
                "seat": seat,
                "draft_kind": "synthetic_focal_seat_adp_baseline",
                "is_observed_human_draft": False,
                "roster": [dict(row, outcome_match=row["key"] in weekly) for row in roster],
                **scored,
                **match_report,
                "per_pick_regret": regrets,
                "total_baseline_regret": round(sum(row["baseline_regret"] for row in regrets), 6),
            })
        season_rows.append({
            "season": season,
            "adp_rows": len(adp_payload.get("players", [])),
            "eligible_adp_rows": len(pool),
            "matched_eligible_adp_rows": len(matched_unique),
            "match_coverage": round(len(matched_unique) / len(pool), 6) if pool else 0.0,
            "cohort_count": len(rosters),
            "rosters": rosters,
        })
    body = {
        "model_version": MODEL_VERSION,
        "seasons": list(SEASONS),
        "weeks": list(WEEKS),
        "scoring": "DraftKings NFL full-PPR with 300/100/100-yard bonuses",
        "lineup_policy": LINEUP_POLICY,
        "roster_policy": ROSTER_POLICY,
        "resampling_unit": RESAMPLING_UNIT,
        "cohort_semantics": "Synthetic focal-seat ADP baselines; never actual human drafts. Opponent rosters are not fabricated.",
        "regret_definition": "Ex-post increase in maximum Weeks 1-17 counted points from replacing one pick with the best outcome-matched, same-position, later-or-equal-ADP player absent from that focal roster; all other picks fixed. This preserves roster policy and is an outcome diagnostic, not a preseason prediction.",
        "known_limitations": [
            "FFC historical redraft ADP boards are shorter than a complete 240-player Best Ball room, so only focal rosters are constructed.",
            "Exact normalized name and position matching is conservative; unmatched drafted players remain explicitly missing and contribute no observed outcome.",
            "The cohort is a deterministic market baseline for validation and contains no champion or challenger draft decisions.",
        ],
        "sources": sources,
        "season_results": season_rows,
    }
    digest_body = json.loads(canonical_json(body))
    digest_body["sources"] = [
        {key: value for key, value in source.items() if key not in {"fetched_at", "cache_path"}}
        for source in digest_body["sources"]
    ]
    digest = sha256_bytes(canonical_json(digest_body).encode("utf-8"))
    return {**body, "artifact_digest": digest}


def verify_artifact(path: Path, cache_root: Path) -> dict[str, Any]:
    expected = json.loads(path.read_text(encoding="utf-8"))
    actual = build_artifact(cache_root, refresh_adp=False)
    if actual["artifact_digest"] != expected.get("artifact_digest"):
        raise ValueError(f"artifact digest mismatch: {actual['artifact_digest']} != {expected.get('artifact_digest')}")
    return {"verified": True, "artifact_digest": actual["artifact_digest"], "cohorts": sum(row["cohort_count"] for row in actual["season_results"])}


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cache-root", type=Path, default=Path("data/ff_v2_sources"))
    parser.add_argument("--output", type=Path, default=Path("artifacts/ff_v2_historical_bestball_cohort_2020_2025.json"))
    parser.add_argument("--refresh-adp", action="store_true")
    parser.add_argument("--verify", type=Path)
    args = parser.parse_args(list(argv) if argv is not None else None)
    if args.verify:
        print(json.dumps(verify_artifact(args.verify, args.cache_root), indent=2))
        return 0
    artifact = build_artifact(args.cache_root, refresh_adp=args.refresh_adp)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(artifact, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({
        "output": str(args.output),
        "artifact_digest": artifact["artifact_digest"],
        "cohorts": sum(row["cohort_count"] for row in artifact["season_results"]),
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
