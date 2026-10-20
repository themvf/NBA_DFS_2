"""Label provenance for imported NFL DFS field ownership (`nfl-ownership-provenance-v1`).

Pure classifier for docs/nfl-ownership-scorecard-v1.md section 3. Every label a
contest supplies is one of:

- ``observed``: the share was read directly from the contest record.
- ``reconciled_zero``: the player is salary-listed, the record is complete, and
  the player is in no entry (or in no entry at that slot).
- ``synthesized_zero``: a zero the importer filled in without evidence (the
  stat-api captain share of a player no fetched user captained:
  ``captain.get(key, 0.0)`` in ``model/statapi_contest.py``; the FLEX share of a
  player stat-api only ever saw as captain).
- ``unresolved``: no usable value, and the record is not complete enough to call
  it zero.

Only the first two may enter a metric or a training target; the other two are
counted and reported, never collapsed into them.

Labels are emitted per (contest, normalized name, slot). ``overall`` is the
player's share in any slot (the stored ``drafted_pct``); Showdown adds ``CPT`` and
``FLEX``. A Classic contest has ``overall`` only (DraftKings keys a Classic
player by his position, which is the same number).

Import paths and what each can prove:

- ``dk_export`` (DraftKings contest-standings csv): the ownership table lists
  every player-slot that anyone drafted, so a missing player, or a missing slot of
  a present player, is a reconciled zero. A printed ``0.00%`` is observed (it is a
  rounded share, not a missing one).
- ``stat_api``: ownership is assembled from the fetched users' seats. A present
  player's overall share is observed when some fetched user rostered him at FLEX
  (the FLEX seat carries the overall share); a captain share is observed only
  when some fetched user captained him. Missing players are reconciled zeros only
  when the recorded mass is at least ``mass_floor`` AND the full standings were
  fetched (``score_curve`` present); otherwise unresolved.
- ``dfsdb``: the athletes list is capped at 50 rows chosen by fantasy points and
  carries the FLEX share only (``drafted_pct`` is NOT the overall share). FLEX is
  observed; overall and CPT are unresolved; missing players are unresolved.

The mass floor is a parameter, recorded with every row: 0.995 is PROVISIONAL
until the scorecard is registered (task 3).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable, Mapping

from model.nfl_dfs_field_audit import normalize_name

VERSION = "nfl-ownership-provenance-v1"
CLASSES = ("observed", "reconciled_zero", "synthesized_zero", "unresolved")
GATE_CLASSES = ("observed", "reconciled_zero")
DEFAULT_MASS_FLOOR = 0.995   # PROVISIONAL: the owner has not frozen it (scorecard task 3)
ROSTER_SLOTS = {"classic": 9, "showdown": 6}


def import_path(contest_id: str, file_name: str | None) -> str:
    """Which importer wrote the contest, from what it stamped on the contest row."""
    name = file_name or ""
    if name.startswith("stat-api:"):
        return "stat_api"
    if contest_id.startswith("dfsdb-") or "dfsdb.com" in name:
        return "dfsdb"
    if name.startswith("contest-standings-") or contest_id.isdigit():
        return "dk_export"
    return "unknown"


@dataclass(frozen=True)
class ContestRecord:
    contest_id: str
    fmt: str                         # 'classic' | 'showdown'
    path: str                        # import_path(...)
    full_standings: bool             # score_curve present
    ownership: Mapping[str, Mapping]  # normalized_name -> {"drafted_pct": x, "drafted_by_slot": {...}}
    salary_pool: frozenset[str] | None = None   # normalized names of the linked slate, None when unlinked
    # stat-api only: who the fetched users captained / rostered at a non-CPT seat.
    captained: frozenset[str] | None = None
    flex_seen: frozenset[str] | None = None
    slate_upload_id: str | None = None


@dataclass(frozen=True)
class Label:
    contest_id: str
    normalized_name: str
    slot: str
    label_class: str
    label_pct: float | None          # usable value (0.0 for a reconciled zero); None otherwise
    stored_pct: float | None         # what nfl_dfs_field_ownership holds for this slot, verbatim
    basis: str
    in_ownership_table: bool
    in_salary_pool: bool | None


@dataclass
class ContestProvenance:
    contest_id: str
    path: str
    mass: float | None
    complete: bool
    labels: list[Label] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)

    def counts(self) -> dict[str, dict[str, int]]:
        out: dict[str, dict[str, int]] = {}
        for lab in self.labels:
            out.setdefault(lab.slot, {c: 0 for c in CLASSES})[lab.label_class] += 1
        return out


def recorded_mass(fmt: str, ownership: Mapping[str, Mapping]) -> float | None:
    slots = ROSTER_SLOTS.get(fmt)
    if not slots:
        return None
    return sum(float(r.get("drafted_pct") or 0.0) for r in ownership.values()) / (slots * 100)


def _slot_value(row: Mapping, slot: str) -> float | None:
    v = (row.get("drafted_by_slot") or {}).get(slot)
    return None if v is None else float(v)


def classify_contest(rec: ContestRecord, *, mass_floor: float = DEFAULT_MASS_FLOOR) -> ContestProvenance:
    fmt, path = rec.fmt, rec.path
    mass = recorded_mass(fmt, rec.ownership)
    if path == "dk_export":
        complete = True
    elif path == "stat_api":
        complete = mass is not None and mass >= mass_floor and rec.full_standings
    else:
        complete = False
    prov = ContestProvenance(rec.contest_id, path, mass, complete)
    pool = rec.salary_pool
    slots = ("overall", "CPT", "FLEX") if fmt == "showdown" else ("overall",)

    def add(name, slot, cls, pct, stored, basis, present):
        prov.labels.append(Label(rec.contest_id, name, slot, cls, pct if cls in GATE_CLASSES else None,
                                 stored, basis, present, None if pool is None else name in pool))

    if path == "stat_api" and fmt == "showdown" and rec.captained is None:
        prov.notes.append("no fetched-user lineups: captain shares cannot be checked, every CPT 0 is synthesized")

    for name, row in sorted(rec.ownership.items()):
        overall = float(row.get("drafted_pct") or 0.0)
        if not name:
            # A stored row with no player name (stat-api seat without a name): no identity to label.
            for slot in slots:
                add(name, slot, "unresolved", None, overall if slot == "overall" else _slot_value(row, slot),
                    "no_player_identity", True)
            continue
        if path == "dk_export":
            add(name, "overall", "observed", overall, overall, "dk_export_row", True)
            if fmt == "showdown":
                for slot in ("CPT", "FLEX"):
                    v = _slot_value(row, slot)
                    if v is None:
                        add(name, slot, "reconciled_zero", 0.0, None, "dk_export_slot_absent", True)
                    else:
                        add(name, slot, "observed", v, v, "dk_export_row", True)
        elif path == "stat_api":
            if fmt != "showdown":
                add(name, "overall", "observed", overall, overall, "statapi_seat", True)
                continue
            cpt = _slot_value(row, "CPT")
            flex = _slot_value(row, "FLEX")
            captained = rec.captained is not None and name in rec.captained
            seen_flex = rec.flex_seen is None or name in rec.flex_seen
            if not seen_flex and captained:
                # Seen only as captain: the importer stored overall = captain share and FLEX = 0.
                add(name, "overall", "unresolved", None, overall, "statapi_captain_only_overall_is_lower_bound", True)
                add(name, "CPT", "observed", cpt, cpt, "statapi_cpt_seat", True)
                add(name, "FLEX", "synthesized_zero", None, flex, "statapi_captain_only_flex_filled", True)
                continue
            add(name, "overall", "observed", overall, overall, "statapi_flex_seat", True)
            if captained:
                add(name, "CPT", "observed", cpt, cpt, "statapi_cpt_seat", True)
                add(name, "FLEX", "observed", flex, flex, "statapi_overall_minus_cpt", True)
            elif cpt is None or cpt == 0.0:
                add(name, "CPT", "synthesized_zero", None, cpt, "statapi_no_fetched_captain", True)
                # FLEX = overall - (unseen captain share): bounded by the contest's unseen captain mass.
                add(name, "FLEX", "observed", flex, flex, "statapi_overall_minus_unseen_captain", True)
            else:
                # A positive captain share for a player no fetched user captained: inconsistent record.
                add(name, "CPT", "unresolved", None, cpt, "statapi_cpt_without_captain_seat", True)
                add(name, "FLEX", "unresolved", None, flex, "statapi_cpt_without_captain_seat", True)
        elif path == "dfsdb":
            add(name, "overall", "unresolved", None, overall, "dfsdb_flex_only", True)
            if fmt == "showdown":
                add(name, "CPT", "unresolved", None, _slot_value(row, "CPT"), "dfsdb_no_captain_share", True)
                f = _slot_value(row, "FLEX")
                f = overall if f is None else f
                add(name, "FLEX", "observed", f, f, "dfsdb_athlete_row", True)
        else:
            for slot in slots:
                add(name, slot, "unresolved", None, overall if slot == "overall" else _slot_value(row, slot),
                    "unknown_import_path", True)

    if pool is None:
        prov.notes.append("no linked salary pool: salary-listed players without a label cannot be enumerated")
        return prov
    missing = sorted(pool - set(rec.ownership))
    for name in missing:
        for slot in slots:
            if complete:
                add(name, slot, "reconciled_zero", 0.0, None,
                    "dk_export_absent" if path == "dk_export" else "statapi_absent_complete", False)
            else:
                basis = {"stat_api": "statapi_absent_below_mass_floor_or_partial_standings",
                         "dfsdb": "dfsdb_absent_capped_list"}.get(path, "absent_incomplete_record")
                add(name, slot, "unresolved", None, None, basis, False)
    return prov


def user_seats(lineups: Iterable[Mapping]) -> tuple[set[str], set[str]]:
    """(captained, rostered at a non-CPT seat) normalized names from stored stat-api lineups.

    A stored lineup's roster is ``[slot, name, team, position, salary, points, field_pct]``.
    """
    captained, flex = set(), set()
    for lineup in lineups:
        for seat in lineup.get("roster") or []:
            if not seat or len(seat) < 2:
                continue
            name = normalize_name(seat[1])
            if name:      # a seat with no player name carries no evidence about anyone
                (captained if str(seat[0]).upper() == "CPT" else flex).add(name)
    return captained, flex
