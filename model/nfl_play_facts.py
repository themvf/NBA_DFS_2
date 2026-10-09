"""Canonical, revisionable NFL play facts derived from preserved PBP evidence.

The raw description remains authoritative for merged and semi-merged events.
Parsed nflverse columns are retained as measurements, but they never erase a
wiped action, a layered penalty, or a second event found in the description.
"""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Iterable

import pandas as pd

from model.nfl_context_engine import stable_digest
from model.nfl_wiped_plays import wiped


FACT_SCHEMA_VERSION = "nfl-play-facts-v1"

_ACTION = re.compile(
    r"\b(pass(?:es|ed)?|incomplete|complete to|scrambl(?:es|ed)|sacked|kneels?|spikes?|left end|left guard|left tackle|"
    r"right end|right guard|right tackle|up the middle|punts?|kicks?|field goal|"
    r"fumbles?|intercepted)\b",
    re.I,
)
_NO_SNAP = re.compile(
    r"\b(false start|delay of game|encroachment|neutral zone infraction|"
    r"timeout|end of (?:quarter|game)|two-minute warning)\b",
    re.I,
)
_PENALTY_START = re.compile(r"\bPENALTY\b", re.I)
_PENALTY_TEAM = re.compile(r"\bPENALTY\s+([A-Z]{2,3})\b")
_PENALTY_YARDS = re.compile(r"(?:for|enforced at).*?(-?\d+)\s+yards?", re.I)


@dataclass(frozen=True)
class PenaltyFact:
    occurrence: int
    adjudication: str
    team: str | None
    penalty_type: str | None
    enforced_yards: float | None
    raw_segment: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "occurrence": self.occurrence,
            "adjudication": self.adjudication,
            "team": self.team,
            "penaltyType": self.penalty_type,
            "enforcedYards": self.enforced_yards,
            "rawSegment": self.raw_segment,
        }


@dataclass(frozen=True)
class PlayFact:
    fact_revision_id: str
    game_id: str
    play_id: int
    source_observation_id: str
    fact_release_id: str
    snap_execution: str
    action_validity: str
    description: str
    regime: str
    payload: dict[str, Any]
    penalties: tuple[PenaltyFact, ...]


def penalty_facts(description: str, *, parsed_type: object = None) -> tuple[PenaltyFact, ...]:
    """Return every textual penalty occurrence; never collapse to one column."""
    starts = list(_PENALTY_START.finditer(description or ""))
    result: list[PenaltyFact] = []
    for index, match in enumerate(starts):
        end = starts[index + 1].start() if index + 1 < len(starts) else len(description)
        segment = description[match.start():end].strip(" .")
        low = segment.lower()
        adjudication = (
            "declined"
            if "declined" in low
            else "offsetting"
            if "offset" in low
            else "accepted"
        )
        team_match = _PENALTY_TEAM.search(segment)
        yards_match = _PENALTY_YARDS.search(segment)
        # nflverse exposes one parsed type. It is usable only for a one-penalty
        # description; assigning it to every occurrence would fabricate facts.
        penalty_type = (
            str(parsed_type)
            if len(starts) == 1 and pd.notna(parsed_type) and str(parsed_type).strip()
            else None
        )
        result.append(
            PenaltyFact(
                occurrence=index + 1,
                adjudication=adjudication,
                team=team_match.group(1) if team_match else None,
                penalty_type=penalty_type,
                enforced_yards=float(yards_match.group(1)) if yards_match else None,
                raw_segment=segment,
            )
        )
    return tuple(result)


def second_event_tags(description: str) -> tuple[str, ...]:
    low = (description or "").lower()
    tags: list[str] = []
    checks = (
        ("injured", "PLAYER_INJURY"),
        ("touchdown nullified", "NULLIFIED_TD"),
        ("reported in as eligible", "ELIGIBLE_REPORT"),
        ("direct snap", "DIRECT_SNAP"),
        ("assisted by replay", "REPLAY_ASSISTED"),
        ("lateral", "LATERAL"),
        ("muffed", "MUFF"),
    )
    for needle, tag in checks:
        if needle in low:
            tags.append(tag)
    if "fumbles" in low and ("recovered by" in low or "recovery" in low):
        tags.append("FUMBLE_RECOVERY")
    return tuple(tags)


def _value(row: pd.Series, name: str, default: Any = None) -> Any:
    value = row.get(name, default)
    if pd.isna(value):
        return default
    return value.item() if hasattr(value, "item") else value


def build_play_facts(
    pbp: pd.DataFrame,
    *,
    source_observation_id: str,
    fact_release_id: str,
) -> list[PlayFact]:
    """Create one revision candidate per source play without dropping rows."""
    required = {"game_id", "play_id", "desc", "play_type"}
    missing = required - set(pbp.columns)
    if missing:
        raise ValueError(f"missing canonical fact columns: {sorted(missing)}")
    wiped_frame = wiped(pbp)
    records: list[PlayFact] = []
    for index, row in pbp.iterrows():
        description = str(_value(row, "desc", ""))
        play_type = str(_value(row, "play_type", ""))
        no_play = play_type == "no_play" or "no play" in description.lower()
        has_action = bool(_ACTION.search(description)) or (
            play_type in {"pass", "run"}
            and not bool(_NO_SNAP.search(description))
        )
        administrative = play_type in {
            "",
            "None",
            "timeout",
            "quarter_end",
            "game_end",
        } or bool(_NO_SNAP.search(description))
        if has_action:
            snap_execution = "executed"
        elif no_play or administrative:
            snap_execution = "no_snap"
        else:
            snap_execution = "unknown"
        if no_play and has_action:
            action_validity = "voided"
        elif administrative or (no_play and not has_action):
            action_validity = "administrative"
        elif snap_execution == "executed":
            action_validity = "counted"
        else:
            action_validity = "unknown"

        penalties = penalty_facts(description, parsed_type=row.get("penalty_type"))
        tags = second_event_tags(description)
        if no_play and has_action:
            regime = "semi_merged"
        elif has_action and (penalties or tags):
            regime = "merged"
        else:
            regime = "resolved"
        wiped_row = wiped_frame.loc[index]
        wiped_action = None
        if pd.notna(wiped_row["wiped_event"]):
            wiped_action = {
                "event": wiped_row["wiped_event"],
                "yards": (
                    None if pd.isna(wiped_row["wiped_yards"]) else float(wiped_row["wiped_yards"])
                ),
                "touchdown": bool(wiped_row["wiped_touchdown"]),
                "turnover": bool(wiped_row["wiped_turnover"]),
                "sack": bool(wiped_row["wiped_sack"]),
                "defender": (
                    None if pd.isna(wiped_row["wiped_defender"]) else wiped_row["wiped_defender"]
                ),
            }
        payload = {
            "schemaVersion": FACT_SCHEMA_VERSION,
            "gameId": str(row["game_id"]),
            "playId": int(row["play_id"]),
            "posteam": _value(row, "posteam"),
            "drive": _value(row, "drive"),
            "quarter": _value(row, "qtr"),
            "playType": play_type or None,
            "qbKneel": int(_value(row, "qb_kneel", 0)),
            "qbSpike": int(_value(row, "qb_spike", 0)),
            "twoPointAttempt": int(_value(row, "two_point_attempt", 0)),
            "scoreDifferential": _value(row, "score_differential"),
            "gameSecondsRemaining": _value(row, "game_seconds_remaining"),
            "description": description,
            "snapExecution": snap_execution,
            "actionValidity": action_validity,
            "regime": regime,
            "wipedAction": wiped_action,
            "penalties": [penalty.as_dict() for penalty in penalties],
            "secondEventTags": list(tags),
            "sentinel": (
                bool(_value(row, "qb_kneel", 0))
                or bool(_value(row, "qb_spike", 0))
                or snap_execution != "executed"
                or action_validity != "counted"
            ),
        }
        revision_id = stable_digest(
            {
                "sourceObservationId": source_observation_id,
                "factReleaseId": fact_release_id,
                "payload": payload,
            }
        )
        records.append(
            PlayFact(
                fact_revision_id=revision_id,
                game_id=str(row["game_id"]),
                play_id=int(row["play_id"]),
                source_observation_id=source_observation_id,
                fact_release_id=fact_release_id,
                snap_execution=snap_execution,
                action_validity=action_validity,
                description=description,
                regime=regime,
                payload=payload,
                penalties=penalties,
            )
        )
    return records


def facts_frame(facts: Iterable[PlayFact]) -> pd.DataFrame:
    """Materialize the exact context-builder columns from canonical facts."""
    rows = []
    for fact in facts:
        payload = fact.payload
        rows.append(
            {
                "game_id": payload["gameId"],
                "play_id": payload["playId"],
                "posteam": payload["posteam"],
                "drive": payload["drive"],
                "qtr": payload["quarter"],
                "play_type": payload["playType"],
                "qb_kneel": payload["qbKneel"],
                "qb_spike": payload["qbSpike"],
                "two_point_attempt": payload["twoPointAttempt"],
                "score_differential": payload["scoreDifferential"],
                "game_seconds_remaining": payload["gameSecondsRemaining"],
                "snap_execution": fact.snap_execution,
                "action_validity": fact.action_validity,
            }
        )
    return pd.DataFrame(rows)
