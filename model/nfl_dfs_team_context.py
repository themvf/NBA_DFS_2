"""Current roster and coaching eligibility for research opportunity scenarios."""
from datetime import datetime
from math import isfinite
from model.nfl_dfs_target_share import normalize_team


def coaching_status(record, now, season):
    if not record or record.get('season') != season:
        return 'unresolved'
    try:
        captured=datetime.fromisoformat(record['checked_at'])
    except (KeyError,ValueError,TypeError):
        return 'unresolved'
    if captured.tzinfo is None or captured>now or (now-captured).total_seconds()>30*86400:
        return 'unresolved'
    if record.get('head_coach_changed') or record.get('coordinator_changed') or record.get('play_caller_changed'):
        return 'changed'
    if record.get('head_coach_same') and record.get('coordinator_same') and record.get('play_caller_same'):
        return 'continuity_verified'
    return 'partial_continuity' if record.get('head_coach_same') else 'unresolved'


def roster_role(member, historical_teams, season, now):
    source=member.get('sleeper') or {}
    captured=member.get('fetched_at')
    fresh=bool(captured and captured.tzinfo and captured<=now and (now-captured).total_seconds()<=72*3600)
    matched=normalize_team(source.get('team'))==normalize_team(member.get('team')) and source.get('position')==member.get('position')
    unavailable={'OUT','IR','PUP','NFI','SUSPENDED','INACTIVE'}
    injury=str(source.get('injury_status') or source.get('status') or 'UNKNOWN').upper()
    status=str(source.get('status') or 'UNKNOWN').upper()
    known=fresh and matched
    out=known and (injury in unavailable or status in unavailable)
    rookie=str((source.get('metadata') or {}).get('rookie_year'))==str(season)
    old=sorted(set(normalize_team(t) for t in historical_teams))
    new_team=bool(old and normalize_team(member['team']) not in old)
    depth=source.get('depth_chart_order')
    depth=depth if isinstance(depth,int) and not isinstance(depth,bool) and depth>0 and known else None
    return {'evidence_current':known,'status':injury if known else 'UNRESOLVED','out':out,
            'role':f"Listed {member['position']}{depth}" if depth else 'Role unresolved',
            'rookie':rookie if known else False,'new_team':new_team,'historical_teams':old,
            'opportunity_basis':'Current-team role required' if new_team or rookie or not old else 'Returning-team history; role still requires verification',
            'share_transferred':False,'captured_at':captured.isoformat() if captured else None}


def opportunity(plays, designed_run_rate, scramble_rate, sack_rate, target_rate, carry_share, target_share):
    values=[plays,designed_run_rate,scramble_rate,sack_rate,target_rate,carry_share,target_share]
    if not all(isfinite(x) for x in values) or plays<0 or any(not 0<=x<=1 for x in values[1:]) or scramble_rate+sack_rate>1:
        raise ValueError('Invalid opportunity inputs')
    runs=plays*designed_run_rate
    dropbacks=plays-runs
    attempts=dropbacks*(1-scramble_rate-sack_rate)
    targets=attempts*target_rate
    return {'designed_runs':runs,'dropbacks':dropbacks,'qb_scrambles':dropbacks*scramble_rate,'sacks':dropbacks*sack_rate,
            'pass_attempts':attempts,'team_targets':targets,'player_carries':runs*carry_share,'player_targets':targets*target_share,
            'other_carries':runs*(1-carry_share),'other_targets':targets*(1-target_share)}
