"""Coverage only: no fitting, scoring, or claims about unobserved source rows."""
from collections import Counter
from datetime import datetime
from hashlib import sha256
import json
from math import isfinite

VERSION = "nfl-dfs-feature-audit-v1"
POSITIONS = ("QB", "RB", "WR", "TE", "DST")


def field(key, label, group, positions, *, unit="count", supported=True):
    return dict(key=key, label=label, group=group, positions=list(positions),
                unit=unit, supported=supported, aliases=[key])


FIELDS = [
    field("attempts", "Pass attempts", "Workload", ["QB"]),
    field("completions", "Completions", "Efficiency", ["QB"]),
    field("carries", "Carries", "Workload", POSITIONS[:4]),
    field("targets", "Targets", "Workload", ["RB", "WR", "TE"]),
    field("receptions", "Receptions", "Efficiency", ["RB", "WR", "TE"]),
    *[field(k, k.replace("_", " ").capitalize(), "Scoring", ps, unit="yards" if k.endswith("yards") else "count")
      for k, ps in [("passing_yards", ["QB"]), ("passing_tds", ["QB"]),
                    ("passing_interceptions", ["QB"]),
                    *[(k, POSITIONS[:4]) for k in ("rushing_yards", "rushing_tds", "receiving_yards", "receiving_tds",
                      "fumbles_lost_total", "passing_2pt_conversions", "rushing_2pt_conversions",
                      "receiving_2pt_conversions", "special_teams_tds", "fumble_recovery_tds")]]],
    *[field(k, k.replace("_", " ").capitalize(), "Team / DST", ["DST"])
      for k in ("attempts", "carries", "targets", "def_sacks", "def_interceptions", "fumble_recovery_opp",
                "def_safeties", "def_tds", "special_teams_tds", "def_fg_blocks", "def_pat_blocks", "def_punt_blocks", "def_2pt_made")],
    *[field(k, label, "Deferred", POSITIONS[:4], supported=False,
            unit="category" if k in ("injury_status", "depth_chart_position") else "count") for k, label in (
        ("routes", "Routes"), ("offense_snaps", "Offensive snaps"),
        ("red_zone_targets", "Red-zone targets"), ("injury_status", "Injury availability"),
        ("depth_chart_position", "Depth-chart role"))],
]
for f in FIELDS:
    f["id"] = f["group"] + ":" + f["key"]


def digest(value):
    return sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), default=str, allow_nan=False).encode()).hexdigest()


def timestamp(value):
    try:
        result = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return result if result.tzinfo else None
    except (TypeError, ValueError):
        return None


def numeric(value, unit):
    if isinstance(value, bool) or not isinstance(value, (float, int)) or not isfinite(value):
        return False
    return unit == "yards" or (value >= 0 and float(value).is_integer())


def normalize(raw, dataset):
    """Retain audited values and whole-row hash; no opaque full payload in UI."""
    payload = raw.get("source_row") or {}
    stats = payload.get("raw_team_stats", payload) if isinstance(payload, dict) else {}
    stats = stats if isinstance(stats, dict) else {}
    if dataset == "frozen_history":
        envelope = raw["payload"]
        stats = envelope.get("stats") or {}
        stats = stats if isinstance(stats, dict) else {}
        # Frozen DST history stores exact score evidence, not the team raw feed.
        position = envelope.get("position")
        metadata = {k: envelope.get(k) for k in ("season", "week", "team", "opponent")}
        identity = envelope.get("player_gsis_id") or ("DST:" + str(envelope.get("team")) if position == "DST" else None)
        captured = raw.get("captured_at")
        source = "pinned_research_history"
    else:
        position = stats.get("position") or raw.get("position")
        metadata = {k: raw.get(k) for k in ("season", "week", "team", "opponent")}
        identity = stats.get("player_id") or ("DST:" + str(metadata["team"]) if position == "DST" else raw.get("gsis_id"))
        captured = raw.get("fetched_at")
        source = raw.get("source")
    keys = {f["key"] for f in FIELDS}
    return {**metadata, "record_id": str(raw.get("row_key", raw.get("id"))), "identity": identity,
            "position": position, "position_basis": "payload" if stats.get("position") or dataset == "frozen_history" else "canonical_fallback",
            "source": source, "captured_at": str(captured) if captured else None,
            "values": {k: stats[k] for k in sorted(keys) if k in stats}, "source_hash": digest(raw)}


def build_audit(datasets, now, study_id):
    observed = timestamp(now)
    if not observed:
        raise ValueError("Audit timestamp must be timezone-aware")
    reports = []
    for name, source_rows in sorted(datasets.items()):
        rows = sorted(source_rows, key=lambda r: r["record_id"])
        eligible, excluded, seen = [], Counter(), set()
        for row in rows:
            if row["position"] not in POSITIONS:
                excluded["out_of_scope_position"] += 1
                continue
            if (not row.get("identity") or not row.get("team") or not row.get("opponent")
                or type(row.get("season")) is not int or type(row.get("week")) is not int
                or not 1 <= row["week"] <= 18):
                excluded["invalid_identity_or_week"] += 1
                continue
            capture = timestamp(row.get("captured_at"))
            if capture and capture > observed:
                excluded["future_capture"] += 1
                continue
            key = (row["identity"], row["season"], row["week"], row["source"])
            if key in seen:
                excluded["duplicate_player_week_source"] += 1
                continue
            seen.add(key)
            eligible.append(row)
        seasons = sorted({r["season"] for r in eligible})
        cells = []
        for f in FIELDS:
            for position in f["positions"]:
                for season in seasons:
                    cohort = [r for r in eligible if r["position"] == position and r["season"] == season]
                    present = [r for r in cohort if r["values"].get(f["key"]) is not None]
                    valid = [r for r in present if f["supported"] and numeric(r["values"][f["key"]], f["unit"])]
                    captures = [timestamp(r["captured_at"]) for r in valid if timestamp(r["captured_at"])]
                    cells.append({"field_id": f["id"], "position": position, "season": season, "n": len(cohort),
                                  "present": len(present), "valid": len(valid), "missing": len(cohort)-len(present),
                                  "invalid": len(present)-len(valid) if f["supported"] else 0,
                                  "zero": sum(r["values"][f["key"]] == 0 for r in valid),
                                  "captured": len(captures), "latest_capture": max(captures).isoformat() if captures else None,
                                  "status": "unsupported" if not f["supported"] else "no_rows" if not cohort else
                                      "missing" if not present else "invalid" if not valid else "retrospective_only"})
        # Complete rows are only a structural cohort, not model eligibility.
        core = {"QB": ["attempts", "carries", "completions", "passing_yards", "passing_tds"],
                "RB": ["carries", "targets", "receptions", "rushing_yards", "receiving_yards"],
                "WR": ["targets", "receptions", "receiving_yards"], "TE": ["targets", "receptions", "receiving_yards"],
                "DST": ["attempts", "carries", "def_sacks", "def_interceptions"]}
        cohort_counts = []
        for p in POSITIONS:
            relevant = [r for r in eligible if r["position"] == p]
            complete = sum(all(numeric(r["values"].get(k), "yards" if k.endswith("yards") else "count") for k in core[p]) for r in relevant)
            cohort_counts.append({"position": p, "rows": len(relevant), "complete": complete, "required": core[p]})
        captures = [timestamp(r["captured_at"]) for r in eligible if timestamp(r["captured_at"])]
        reports.append({"dataset": name, "scanned": len(rows), "eligible": len(eligible), "excluded": dict(excluded),
                        "normalization_warning": "Frozen research values include legacy missing-to-zero defaults. Numeric presence and zeros cannot establish original-source completeness; audit raw source rows before expanding this cohort." if name == "frozen_history" else None,
                        "canonical_position_fallback": sum(r["position_basis"] == "canonical_fallback" for r in eligible),
                        "seasons": seasons, "sources": dict(Counter(r["source"] for r in rows)), "cells": cells,
                        "cohorts": cohort_counts, "input_digest": digest(rows),
                        "latest_capture": max(captures).isoformat() if captures else None})
    return {"version": VERSION, "evaluated_at": observed.isoformat(), "study_id": study_id, "fields": FIELDS,
            "datasets": reports, "production_changed": False,
            "limits": ["Stored rows only; upstream unmatched players and no-stat/DNP weeks are not recoverable from this audit.",
                       "Datasets overlap and must not be summed. Frozen history and mutable working rows are distinct populations.",
                       "Capture time proves storage at that time, not historical publication or pregame availability. All valid observations remain retrospective-only.",
                       "Working rows retain only the latest source revision. This audit freezes inspected values, not missing earlier revisions.",
                       "Core-complete rows establish field presence, not full scoring, canonical game mapping or a timestamp-qualified training cohort.",
                       "Deferred fields have no approved aliases/units contract. Missing named keys do not prove absence in all providers.",
                       "Team budgets use raw team stats under DST working rows, not sums of a partial player roster."]}
