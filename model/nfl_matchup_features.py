"""Point-in-time matchup features shared by DFS and pick'em.

Rates with unknown denominators remain game observations, never invented
event exposures. All functions are pure once the source manifest is frozen.
"""
from __future__ import annotations

import math
from datetime import datetime, timezone
from statistics import mean
from typing import Any

from model.nfl_context_engine import ContextDefinition, ContextMeasurement, ContextState, stable_digest
from model.nfl_pfr_supplement import team_code

VERSION = "nfl-matchup-features-v1"
SCHEMA = "nflverse_pfr_fields_percentage_points"
LOOKBACK = 4
DEFINITIONS = {
    "pressure": ContextDefinition("matchup_pressure", "v1", "percentage_points", "Prior QB pressure faced and opponent pressure created", {
        "lookback_games": LOOKBACK, "aggregation": "unweighted game rates", "exact_exposure": False}),
    "contact": ContextDefinition("matchup_rb_contact", "v1", "yards_per_carry", "Same-source charted RB contact efficiency", {
        "lookback_games": LOOKBACK, "scope": "charted RB carries", "denominator": "PFR carries from identical rows"}),
}
HTML_NAMES = {"pass_pressured_pct": "times_pressured_pct", "pass_pressured": "times_pressured",
              "pass_sacked": "times_sacked", "pass_blitzed": "times_blitzed",
              "rush_att": "carries", "rush_yds_before_contact": "rushing_yards_before_contact",
              "rush_yds_after_contact": "rushing_yards_after_contact"}


def stamp(value: Any) -> datetime:
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("timezone-aware evidence timestamps required")
    return parsed.astimezone(timezone.utc)


def number(value):
    if value is None or isinstance(value, bool):
        return None
    try:
        n = float(value)
    except (ValueError, TypeError):
        return None
    return n if math.isfinite(n) else None


def normalized_rows(snapshot: dict) -> list[dict]:
    payload = snapshot.get("payload", snapshot)
    schema = payload.get("stats_schema")
    html = str(payload.get("parser_version", "")).startswith("pfr-boxscore-")
    if schema != SCHEMA and not html:
        return []
    result = []
    for row in payload.get("rows", []):
        stats = {HTML_NAMES.get(k, k) if html else k: number(v) for k, v in row.get("stats", {}).items()}
        result.append({**row, "team": team_code(row.get("team", "")), "stats": stats})
    return result


def snapshot_manifest(snapshot: dict) -> dict:
    p = snapshot.get("payload", snapshot)
    return {
        "snapshot_id": str(snapshot.get("snapshot_id", "")), "game_id": snapshot.get("game_id", p.get("game_id")),
        "captured_at": str(snapshot.get("captured_at", p.get("captured_at"))),
        "recorded_at": str(snapshot.get("recorded_at", p.get("recorded_at", p.get("captured_at")))),
        "source_provider": p.get("source_provider", "pfr_html"), "source_url": p.get("source_url"),
        "source_files": p.get("source_files", {}), "source_sha256": p.get("source_sha256"),
        "parser_version": p.get("parser_version"), "stats_schema": p.get("stats_schema"),
        "identity_manifest": p.get("identity_manifest"),
    }


def latest_eligible(snapshots: list[dict], prior_games: list[dict], cutoff: datetime) -> list[dict]:
    allowed = {g["game_id"] for g in prior_games if stamp(g["kickoff"]) < cutoff and g.get("completed", True)}
    selected = {}
    for s in snapshots:
        m = snapshot_manifest(s)
        if m["game_id"] not in allowed:
            continue
        identity_at = (m.get("identity_manifest") or {}).get("captured_at", m["captured_at"])
        participant_at = (s.get("participant_manifest") or {}).get("available_at", m["captured_at"])
        available = max(stamp(m["captured_at"]), stamp(m["recorded_at"]), stamp(identity_at), stamp(participant_at))
        if available > cutoff:
            continue
        key = (available, int(m["snapshot_id"] or 0))
        if m["game_id"] not in selected or key > selected[m["game_id"]][0]:
            selected[m["game_id"]] = (key, s)
    return [value[1] for _, value in sorted(selected.items())]


def participant_manifests(weekly_rows: list[dict], games: list[dict], cutoff: datetime) -> dict:
    """Freeze expected participants from retained weekly stats, not mutable names.

    This fallback is explicitly weekly box-score evidence, not play-by-play.
    Each retained row includes its source identity, availability and raw digest.
    No currently joined player roster or alias table participates in the join.
    """
    cutoff = stamp(cutoff)
    eligible = {g["game_id"]: g for g in games if g.get("completed") and stamp(g["kickoff"]) < cutoff}
    grouped = {}
    for row in weekly_rows:
        raw = row.get("source_row") or {}
        gid = raw.get("game_id")
        if gid not in eligible or not row.get("fetched_at") or stamp(row["fetched_at"]) > cutoff:
            continue
        team = team_code(raw.get("team") or row.get("team") or "")
        if team not in {team_code(eligible[gid]["home"]), team_code(eligible[gid]["away"])}:
            continue
        attempts, carries, sacks = (number(raw.get(k)) for k in ("attempts", "carries", "sacks_suffered"))
        position = raw.get("position")
        passer = (attempts or 0) > 0 or (sacks or 0) > 0 or (position == "QB" and (carries or 0) > 0)
        rusher = (carries or 0) > 0
        if not passer and not rusher:
            continue
        grouped.setdefault(gid, []).append({"source_row_id": str(row["id"]), "source": row.get("source"),
            "available_at": stamp(row["fetched_at"]).isoformat(), "raw_digest": stable_digest(raw),
            "team": team, "gsis_id": raw.get("player_id"), "position": position,
            "attempts": attempts, "sacks_suffered": sacks, "carries": carries, "passer": passer, "rusher": rusher})
    result = {}
    for gid, rows in grouped.items():
        rows.sort(key=lambda r: (r["team"], r["gsis_id"] or "", r["source_row_id"]))
        manifest = {"version": "nfl-matchup-participants-v1", "game_id": gid,
            "source_kind": "nflverse_weekly_player_stats", "pbp_participant_ids_available": False,
            "available_at": max(r["available_at"] for r in rows), "rows": rows}
        manifest["manifest_hash"] = stable_digest(manifest)
        result[gid] = manifest
    return result


def _coverage(rows, expected, manifest, *, family):
    reasons = []
    if not manifest:
        reasons.append("participant_source_unavailable")
    if not expected:
        reasons.append("expected_participants_unavailable")
    if any(not r.get("gsis_id") or not r.get("position") for r in expected):
        reasons.append("expected_identity_unresolved")
    relevant = [r for r in rows if r.get("section") == ("passing_advanced" if family == "pressure" else "rushing_advanced")]
    if family == "contact":
        relevant = [r for r in relevant if r.get("position") == "RB" or r.get("identity_status") != "resolved"]
    if any(not r.get("gsis_id") or r.get("identity_status") != "resolved" for r in relevant):
        reasons.append("pfr_identity_unresolved")
    actual_ids = [r.get("gsis_id") for r in relevant]
    expected_ids = [r.get("gsis_id") for r in expected]
    if len(set(actual_ids)) != len(actual_ids) or len(set(expected_ids)) != len(expected_ids):
        reasons.append("duplicate_participant_identity")
    if set(actual_ids) != set(expected_ids):
        reasons.append("participant_set_mismatch")
    if family == "pressure":
        if len(relevant) != 1:
            reasons.append("multiple_or_missing_qb_denominator")
        if any(r["stats"].get("times_pressured_pct") is None or not 0 <= r["stats"]["times_pressured_pct"] <= 100 for r in relevant):
            reasons.append("pressure_rate_missing_or_invalid")
    else:
        expected_carries = {r.get("gsis_id"): r.get("carries") for r in expected}
        for r in relevant:
            stats = r["stats"]
            if any(stats.get(k) is None for k in ("carries", "rushing_yards_before_contact", "rushing_yards_after_contact")):
                reasons.append("rb_contact_fields_missing")
            if stats.get("carries") != expected_carries.get(r.get("gsis_id")):
                reasons.append("rb_carry_count_mismatch")
    return {"complete": not reasons, "reasons": sorted(set(reasons)), "expected_ids": sorted(x for x in expected_ids if x),
            "observed_ids": sorted(x for x in actual_ids if x), "participant_manifest_hash": (manifest or {}).get("manifest_hash")}


def _summarize(snapshots: list[dict], team: str, *, defense: bool) -> dict:
    pressures, pressure_games, sacks, contacts, ids, unresolved = [], [], [], [], [], 0
    coverage = []
    for s in snapshots:
        m = snapshot_manifest(s)
        rows = [r for r in normalized_rows(s) if (r["team"] != team if defense else r["team"] == team)]
        manifest = s.get("participant_manifest")
        expected = [r for r in (manifest or {}).get("rows", []) if (r["team"] != team if defense else r["team"] == team)]
        pressure_coverage = _coverage(rows, [r for r in expected if r["passer"]], manifest, family="pressure")
        # Unknown-position rushers cannot be silently omitted from RB scope.
        contact_coverage = _coverage(rows, [r for r in expected if r["rusher"] and r.get("position") in ("RB", None, "")], manifest, family="contact")
        if not m.get("identity_manifest"):
            for c in (pressure_coverage, contact_coverage):
                c["complete"] = False; c["reasons"].append("pfr_identity_manifest_missing")
        coverage.append({"game_id": m["game_id"], "pressure": pressure_coverage, "contact": contact_coverage})
        qbs = [r for r in rows if r["section"] == "passing_advanced"]
        # A game with multiple QB rows has no exposure denominator for pooling.
        # Preserve those rows but withhold its aggregate rate.
        if pressure_coverage["complete"] and len(qbs) == 1 and qbs[0]["stats"].get("times_pressured_pct") is not None:
            rate = qbs[0]["stats"]["times_pressured_pct"]
            if 0 <= rate <= 100:
                pressures.append(rate); pressure_games.append(m["game_id"])
        sacks.extend(r["stats"]["times_sacked"] for r in qbs if r["stats"].get("times_sacked") is not None)
        for r in rows:
            if r["section"] != "rushing_advanced":
                continue
            if r.get("identity_status") != "resolved" or not m.get("identity_manifest"):
                unresolved += 1
                continue
            if r.get("position") != "RB":
                continue
            stats = r["stats"]
            c, before, after = (stats.get(k) for k in ("carries", "rushing_yards_before_contact", "rushing_yards_after_contact"))
            if contact_coverage["complete"] and c is not None and c > 0 and before is not None and after is not None:
                contacts.append((c, before, after))
        ids.append(m["snapshot_id"])
    carries = sum(c[0] for c in contacts)
    before, after = sum(c[1] for c in contacts), sum(c[2] for c in contacts)
    return {"games": len(snapshots), "snapshot_ids": ids, "pressure_game_ids": pressure_games,
            "pressure_games": len(pressures), "pressure_pct": mean(pressures) if pressures else None,
            "pressure_aggregation": "unweighted single-QB game average; multi-QB games withheld",
            "pressure_exact_denominator": None, "charted_sacks": sum(sacks) if sacks else None,
            "rb_carries": carries or None, "rb_before_contact_yards": before if carries else None,
            "rb_after_contact_yards": after if carries else None,
            "rb_before_contact_per_carry": before / carries if carries else None,
            "rb_after_contact_per_carry": after / carries if carries else None,
            "rb_contact_yards_per_carry": (before + after) / carries if carries else None,
            "unresolved_rushing_rows": unresolved, "participant_coverage": coverage,
            "pressure_coverage_complete": bool(coverage) and all(c["pressure"]["complete"] for c in coverage),
            "contact_coverage_complete": bool(coverage) and all(c["contact"]["complete"] for c in coverage)}


def build_matchup(*, game: dict, prior_games: list[dict], snapshots: list[dict], as_of: datetime) -> dict:
    as_of = stamp(as_of)
    if as_of >= stamp(game["kickoff"]):
        raise ValueError("matchup freeze must precede target kickoff")
    teams, used = {}, {}
    for team in (team_code(game["home"]), team_code(game["away"])):
        prior = sorted([g for g in prior_games if team in {team_code(g["home"]), team_code(g["away"])}
                        and stamp(g["kickoff"]) < as_of and g.get("completed", True)],
                       key=lambda g: stamp(g["kickoff"]), reverse=True)[:LOOKBACK]
        selected = latest_eligible(snapshots, prior, as_of)
        for s in selected:
            m = snapshot_manifest(s); used[m["snapshot_id"]] = m
        teams[team] = {"offense": _summarize(selected, team, defense=False),
                       "defense": _summarize(selected, team, defense=True),
                       "prior_game_ids": [g["game_id"] for g in prior],
                       "missing_game_ids": sorted(set(g["game_id"] for g in prior) - {s["game_id"] for s in selected})}
        if teams[team]["missing_game_ids"]:
            for side in ("offense", "defense"):
                teams[team][side]["pressure_coverage_complete"] = False
                teams[team][side]["contact_coverage_complete"] = False
    result = {"version": VERSION, "game_id": game["game_id"], "kickoff": stamp(game["kickoff"]).isoformat(),
              "as_of_at": as_of.isoformat(), "home": team_code(game["home"]), "away": team_code(game["away"]),
              "teams": teams, "sources": sorted(used.values(), key=lambda s: s["snapshot_id"]),
              "participant_sources": sorted({s["participant_manifest"]["manifest_hash"]: s["participant_manifest"]
                  for s in snapshots if s.get("participant_manifest") and str(s.get("snapshot_id")) in used}.values(), key=lambda s: s["game_id"]),
              "authority": "descriptive_and_shadow_only", "production_effect": "none_until_consumer_qualification"}
    result["manifest_hash"] = stable_digest(result)
    return result


def contexts(matchup: dict, available_at: datetime) -> list[ContextMeasurement]:
    output = []
    for team, summary in matchup["teams"].items():
        for family, key in (("pressure", "pressure_pct"), ("contact", "rb_contact_yards_per_carry")):
            own = summary["offense"]
            output.append(ContextMeasurement(subject_type="team", subject_id=team, target_id=matchup["game_id"],
                definition_id=DEFINITIONS[family].definition_id, as_of_at=stamp(matchup["as_of_at"]),
                available_at=available_at, window={"games": summary["prior_game_ids"], "lookback": LOOKBACK},
                numerator=None if family == "pressure" else ((own["rb_before_contact_yards"] or 0) + (own["rb_after_contact_yards"] or 0)) if own["rb_carries"] else None,
                denominator=None if family == "pressure" else own["rb_carries"], value=own[key], state=ContextState.OBSERVED,
                coverage={"missing_games": summary["missing_game_ids"], "observed_games": own["games"]},
                source_snapshot_ids=tuple("pfr:" + s for s in own["snapshot_ids"]),
                fact_release_id=matchup["manifest_hash"], payload={"matchup": matchup, "family": family}))
    return output
