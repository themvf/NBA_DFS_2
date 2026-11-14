"""Showdown ownership after-slate grade (`nfl-showdown-ownership-grade-v1`).

After a Showdown is played, how far each ownership estimate that was stored
before kickoff was from the field's real captain and flex ownership. Owner's
decision 2026-11-13, so that a change to the ownership model can be read off
live slates and not only off one-off research runs.

Pure: no database, no I/O. The driver is ingest/nfl_showdown_ownership_grade.py.

What is graded is what was STORED in the last saved build or automatic freeze
at least 20 minutes before kickoff (`ownership` on every snapshot player, see
`snapshotPlayer` in web/src/app/dfs/nfl/actions.ts):

- ``shown``: the number the build read, with its source;
- ``ours``: our own model's number when an outside source (stat-api) was shown
  in its place;
- ``shadow``: the comparison model recorded beside it.

Nothing is recomputed. A later code change cannot move an old grade, and a
model that produced no number before kickoff gets no grade for that slate
(the lineup model on a slate whose lines were not in, for one).

Miss = the mean absolute gap between estimate and real ownership, in
percentage points a player, per slot. Every model of one slate is graded on
the same players: active in the snapshot, a usable real-ownership label in
that slot, and a number from every model. Players we ruled out that the field
drafted are counted beside the grade, never inside it.

Small samples: one slate is about 25 players of one game. The numbers describe
that game; several slates are needed before two models can be told apart.
"""

from __future__ import annotations

import math
from collections import Counter
from typing import Mapping, Sequence

from model.nfl_dfs_field_audit import normalize_name

VERSION = "nfl-showdown-ownership-grade-v1"
SLOTS = ("CPT", "FLEX")
#: Real ownership at or above which a player counts as chalk in the second read.
CHALK_PCT = 10.0
#: Largest misses kept per model and slot.
WORST_KEPT = 3

FAMILY_LINEUP_MODEL = "lineup_model"
FAMILY_FITTED_MODEL = "fitted_model"
FAMILY_STAT_API = "stat_api"
FAMILY_RULE_OF_THUMB = "rule_of_thumb"
FAMILY_LINESTAR = "linestar"
FAMILY_OTHER = "other"


def base_source(source) -> str | None:
    """The model behind a stored `ownSource`: the bands clamp appends `+<bands version>`."""
    if not isinstance(source, str) or not source.strip():
        return None
    return source.split("+", 1)[0].strip()


def family(version) -> str:
    """Which kind of estimate a stored version or source string names."""
    v = (base_source(version) or "").lower()
    if v.startswith("nfl-ownership-field-showdown"):
        return FAMILY_LINEUP_MODEL
    if v.startswith("nfl-ownership-fit-"):
        return FAMILY_FITTED_MODEL
    if v == "stat-api" or v.startswith("nfl-ownership-outside"):
        return FAMILY_STAT_API
    if v.startswith("nfl-ownership-prior"):
        return FAMILY_RULE_OF_THUMB
    if v == "linestar":
        return FAMILY_LINESTAR
    return FAMILY_OTHER


#: What a slot's shares sum to over the whole field, in percent.
SLOT_TOTALS = {"CPT": 100.0, "FLEX": 500.0}
#: Share of each slot's total a contest's stored split must reach to be read as the whole field's.
WHOLE_FIELD_SHARE = 0.995


def slot_sums(ownership: Mapping[str, Mapping]) -> dict[str, float]:
    """Stored captain and flex shares summed over a contest's ownership rows
    ({normalized name: {"drafted_by_slot": {"CPT": pct, "FLEX": pct}}})."""
    return {slot: sum(_num((row.get("drafted_by_slot") or {}).get(slot)) or 0.0 for row in ownership.values())
            for slot in SLOTS}


def whole_field_split(ownership: Mapping[str, Mapping]) -> bool:
    """Whether the stored split accounts for the whole field in both slots.

    A contest loaded from a full standings file, or a DraftKings export, stores
    every player's captain and flex share, so the shares sum to 100 and 500.
    When both sums reach 99.5% of that, at most half a captain point and 2.5
    flex points are unaccounted for across every player together, and each
    stored value is taken as real ownership. This is this grade's own rule: the
    scorecard's label rules (model/nfl_ownership_provenance.py) predate full
    standings files and distrust a captain share no fetched user's lineup shows.
    """
    sums = slot_sums(ownership)
    return all(sums[slot] >= WHOLE_FIELD_SHARE * SLOT_TOTALS[slot] for slot in SLOTS)


def whole_field_labels(ownership: Mapping[str, Mapping], pool: frozenset[str] | None) -> dict[str, dict[str, float]]:
    """Real ownership by player and slot from a whole-field split: a slot a stored
    player lacks is 0, and so is every slate player (`pool`) the contest does not list."""
    labels: dict[str, dict[str, float]] = {}
    for name, row in ownership.items():
        if not name:
            continue
        by_slot = row.get("drafted_by_slot") or {}
        labels[name] = {slot: _num(by_slot.get(slot)) or 0.0 for slot in SLOTS}
    for name in (pool or frozenset()) - set(labels):
        if name:
            labels[name] = {slot: 0.0 for slot in SLOTS}
    return labels


def _num(value) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    return f if math.isfinite(f) else None


def _pair(captain, flex) -> tuple[float | None, float | None]:
    return _num(captain), _num(flex)


def _dominant(values: Sequence[str | None]) -> str | None:
    counts = Counter(v for v in values if v)
    return counts.most_common(1)[0][0] if counts else None


def snapshot_models(players: Sequence[Mapping]) -> tuple[list[dict], list[dict]]:
    """(players, models) from one frozen snapshot.

    Each player: normalized name, display name, whether we had ruled him out.
    Each model: role, version, family and `values` {normalized name: (captain, flex)}.
    A repeated normalized name keeps its first row, as the ownership scorecard does.
    """
    rows: list[dict] = []
    seen: set[str] = set()
    for p in players:
        key = normalize_name(p.get("name"))
        if not key or key in seen:
            continue
        seen.add(key)
        o = p.get("ownership") if isinstance(p.get("ownership"), Mapping) else {}
        ours = o.get("model") if isinstance(o.get("model"), Mapping) else None
        has_shadow = o.get("shadowVersion") is not None
        rows.append({
            "key": key, "name": str(p.get("name") or ""), "is_out": bool(p.get("isOut")),
            "shown": _pair(o.get("captain"), o.get("flex")), "source": base_source(o.get("source")),
            "ours": _pair(ours.get("captain"), ours.get("flex")) if ours else None,
            "live_version": base_source(o.get("liveVersion")),
            "shadow": _pair(o.get("shadowCaptain"), o.get("shadowFlex")) if has_shadow else None,
            "shadow_version": base_source(o.get("shadowVersion")),
        })
    active = [r for r in rows if not r["is_out"]]

    def slotted(pairs) -> bool:
        return any(pair is not None and pair[1] is not None for pair in pairs)

    models: list[dict] = []
    if slotted(r["shown"] for r in active):
        sources = Counter(r["source"] or "unknown" for r in active if r["shown"][1] is not None)
        version = _dominant([r["source"] for r in active if r["shown"][1] is not None]) or "unknown"
        models.append({"role": "shown", "version": version, "family": family(version), "sources": dict(sources),
                       "values": {r["key"]: r["shown"] for r in rows}})
    if slotted(r["ours"] for r in active):
        version = _dominant([r["live_version"] for r in active if r["ours"]]) or "our model (version not recorded)"
        models.append({"role": "ours", "version": version, "family": family(version),
                       "values": {r["key"]: r["ours"] or (None, None) for r in rows}})
    if slotted(r["shadow"] for r in active):
        version = _dominant([r["shadow_version"] for r in active if r["shadow"]]) or "unknown"
        models.append({"role": "shadow", "version": version, "family": family(version),
                       "values": {r["key"]: r["shadow"] or (None, None) for r in rows}})
    return rows, models


def _summary(pairs: Sequence[tuple[str, float, float]]) -> dict:
    """Metrics for (name, estimate, real) triples of one model and slot."""
    n = len(pairs)
    if not n:
        return {"n": 0, "miss": None, "bias": None, "chalk_n": 0, "chalk_miss": None, "worst": []}
    gaps = [estimate - real for _, estimate, real in pairs]
    chalk = [abs(estimate - real) for _, estimate, real in pairs if real >= CHALK_PCT]
    worst = sorted(pairs, key=lambda t: (-abs(t[1] - t[2]), t[0]))[:WORST_KEPT]
    return {
        "n": n,
        "miss": round(sum(abs(g) for g in gaps) / n, 3),
        "bias": round(sum(gaps) / n, 3),
        "chalk_n": len(chalk),
        "chalk_miss": round(sum(chalk) / len(chalk), 3) if chalk else None,
        "worst": [{"name": name, "estimate": round(estimate, 2), "real": round(real, 2)} for name, estimate, real in worst],
    }


def grade_slate(players: Sequence[Mapping], labels: Mapping[str, Mapping[str, float | None]]) -> dict:
    """Grade every stored model of one snapshot against real ownership.

    `labels` is {normalized name: {"CPT": pct, "FLEX": pct}} holding only usable
    labels (observed, or a zero reconciled against a complete contest); a slot a
    player has no usable label in is absent or None.

    Returns {"models": [...], "detail": {...}}. `models` is empty when the
    snapshot stored no captain and flex split.
    """
    rows, models = snapshot_models(players)
    active = [r for r in rows if not r["is_out"]]
    graded = []
    for m in models:
        graded.append({k: v for k, v in m.items() if k != "values"} | {"captain": None, "flex": None})
    for slot_index, slot in enumerate(SLOTS):
        common = [r for r in active
                  if _num((labels.get(r["key"]) or {}).get(slot)) is not None
                  and all(m["values"].get(r["key"], (None, None))[slot_index] is not None for m in models)]
        for m, out in zip(models, graded):
            pairs = [(r["name"], float(m["values"][r["key"]][slot_index]), float(labels[r["key"]][slot])) for r in common]
            out["captain" if slot == "CPT" else "flex"] = _summary(pairs)
    in_snapshot = {r["key"] for r in rows}
    ruled_out = sorted(
        ({"name": r["name"], "real_flex": round(float(labels[r["key"]]["FLEX"]), 2)} for r in rows
         if r["is_out"] and _num((labels.get(r["key"]) or {}).get("FLEX")) not in (None, 0.0)),
        key=lambda x: -x["real_flex"])
    missing = sorted(name for name, by_slot in labels.items()
                     if name not in in_snapshot and (_num((by_slot or {}).get("FLEX")) or 0) > 0)
    shown = next((m for m in models if m["role"] == "shown"), None)
    totals = None
    if shown:
        totals = {"captain": round(sum(v[0] or 0 for k, v in shown["values"].items() if k in {r["key"] for r in active}), 1),
                  "flex": round(sum(v[1] or 0 for k, v in shown["values"].items() if k in {r["key"] for r in active}), 1)}
    return {"models": graded, "detail": {
        "active_players": len(active),
        "shown_totals": totals,
        "ruled_out_but_drafted": ruled_out[:5],
        "drafted_not_in_snapshot": len(missing),
    }}
