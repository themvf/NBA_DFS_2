"""Permanent identity claims and conservative retrospective zero-result checks."""
from __future__ import annotations

import math
import re
from collections import defaultdict


def external_id(value):
    if value is None or isinstance(value, float) and not math.isfinite(value):
        return None
    text = str(int(value)) if isinstance(value, float) and value.is_integer() else str(value).strip()
    return text if text.lower() not in {'', '0', 'none', 'nan', 'null'} else None


def valid_gsis(value):
    value = external_id(value)
    return value if value and re.fullmatch(r'\d{2}-\d{7}', value) else None


def registry(claims):
    """Conflicting provider IDs are quarantined, regardless of source priority."""
    grouped = defaultdict(set)
    for row in claims:
        grouped[(row['namespace'], row['external_id'])].add(row['gsis_id'])
    return {key: {'status': 'resolved' if len(ids) == 1 else 'conflict',
                  'gsis_id': next(iter(ids)) if len(ids) == 1 else None,
                  'candidates': sorted(ids)} for key, ids in grouped.items()}


def zero_result(gsis, coverage, has_stat_row=False, roster_status=None):
    """A missing row alone, or a roster status alone, never establishes a zero.

    Requires a finished game, complete recorded participation coverage and no
    event attribution for that identity. Recorded absence is separate from an
    official inactive designation; both are retrospective, not pre-lock evidence.
    """
    base = {'actual': None, 'status': 'unresolved', 'roster_status': roster_status}
    if has_stat_row:
        return {**base, 'reason': 'A stat row exists; reconcile its scoring instead.'}
    if not valid_gsis(gsis):
        return {**base, 'reason': 'No unambiguous permanent identity.'}
    if not coverage or not coverage.get('complete'):
        return {**base, 'reason': 'Incomplete game or recorded participation coverage.'}
    if gsis in coverage['event_player_ids']:
        return {**base, 'status': 'stat_reconciliation_required',
                'reason': 'Play-by-play attributes an event to this player despite the absent stat row.'}
    played = gsis in coverage['participant_ids']
    if played and roster_status in {'INA', 'RES'}:
        return {**base, 'status': 'source_conflict',
                'reason': 'Recorded participation conflicts with the retrospective inactive/reserve roster status.'}
    return {'actual': 0.0, 'status': 'recorded_participant_no_events' if played else 'recorded_nonparticipant',
            'reason': 'Complete recorded play coverage, no stat row and no attributed event.',
            'roster_status': roster_status, 'official_inactive': roster_status == 'INA'}
