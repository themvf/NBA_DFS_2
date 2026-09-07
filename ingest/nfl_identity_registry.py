"""Build source-backed provider ID claims and reconcile the two archive pilots.

Reads verified local nflverse caches; no speculative provider ID conversions.
python -m ingest.nfl_identity_registry --source-root <cache checkout> --persist
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
from collections import Counter, defaultdict
from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2.extras import Json, execute_values

from config import load_config
from db.nfl_identity_registry import DDL, SLATE_MIGRATION
from ingest.nfl_archive_case_study import query, SLATES
from model.nfl_archive_case_study import checked_points, normalized_name, scoring_position, team_key
from model.nfl_dfs_historical import artifact_digest
from model.nfl_identity_registry import external_id, registry, valid_gsis, zero_result

VERSION = 'nfl-identity-registry-v1'
ROSTER_IDS = {'espn_id':'espn', 'yahoo_id':'yahoo', 'sleeper_id':'sleeper',
              'pfr_id':'pfr', 'sportradar_id':'sportradar', 'pff_id':'pff',
              'rotowire_id':'rotowire', 'fantasy_data_id':'fantasydata'}
APP_IDS = {'id':'app.ff_players', 'espn_id':'espn', 'yahoo_id':'yahoo',
           'sleeper_player_id':'sleeper', 'mfl_id':'mfl', 'fantasypros_player_id':'fantasypros',
           'draftkings_id':'sleeper.draftkings_unverified_namespace'}


def read_sources(root, season):
    manifest = json.loads((root/'artifacts/ff_v2_historical_context_2020_2025.json').read_text())
    frames, provenance = {}, {}
    for kind in ('weekly-rosters', 'play-by-play', 'participation'):
        key = f'{kind}:{season}'
        source = manifest['sources'][key]
        path = root/Path(source['cachePath'].replace('\\','/'))
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        if digest != source['responseHash']:
            raise ValueError(f'Changed source bytes: {key}')
        frames[kind] = pd.read_parquet(path)
        provenance[kind] = {k:source[k] for k in ('responseHash','fetchedAt','sourcePublishedAt','url')}
    return frames, provenance


def player_ids(value):
    return set(re.findall(r'\b\d{2}-\d{7}\b', str(value or '')))


def game_coverage(plays, participation, games):
    """Check every nondeleted football play, plus any scoring no-play rows.

    Participant counts must agree with the provider's declared side counts.
    We do not call these official snaps or infer pregame availability from them.
    """
    personnel = participation.rename(columns={'nflverse_game_id':'game_id'})
    if plays.duplicated(['game_id','play_id']).any() or personnel.duplicated(['game_id','play_id']).any():
        raise ValueError('Duplicate game/play identity')
    lookup = {(r['game_id'],r['play_id']):r for r in personnel.to_dict('records')}
    event_columns = [c for c in plays if 'player_id' in c or c in ('passer_id','rusher_id','receiver_id')]
    result = {}
    for game in games:
        rows = plays[plays.game_id == game['game_id']]
        end = rows[rows['desc'].fillna('').str.contains(r'\bEND GAME\b')]
        final_ok = len(end) == 1 and int(end.iloc[0]['total_home_score']) == game['home_score'] and int(end.iloc[0]['total_away_score']) == game['away_score']
        participants, events, voided_events, missing = set(), set(), set(), []
        count = 0
        for row in rows.to_dict('records'):
            if row.get('play_deleted') == 1:
                continue
            scoring = any(row.get(c) == 1 for c in ('touchdown','pass_touchdown','rush_touchdown','return_touchdown',
                'fumble_lost','interception','complete_pass','defensive_two_point_conv','defensive_extra_point_conv'))
            scoring = scoring or row.get('field_goal_result') == 'made' or row.get('extra_point_result') == 'good' or row.get('two_point_conv_result') == 'success'
            scoring = scoring or any(pd.notna(row.get(c)) and row[c] != 0 for c in ('passing_yards','rushing_yards','receiving_yards'))
            kind = row.get('play_type')
            # Explicitly voided plays with no counted scoring components do
            # not produce fantasy points. Retain their IDs separately as proof.
            for col in event_columns:
                (voided_events if kind == 'no_play' and not scoring else events).update(player_ids(row.get(col)))
            if (pd.isna(kind) or kind == 'no_play') and not scoring:
                continue
            count += 1
            evidence = lookup.get((row['game_id'],row['play_id']))
            ids = player_ids(evidence.get('players_on_play')) if evidence else set()
            offense = player_ids(evidence.get('offense_players')) if evidence else set()
            defense = player_ids(evidence.get('defense_players')) if evidence else set()
            valid = evidence and ids and not offense.intersection(defense) and ids == offense.union(defense) and len(offense) == evidence['n_offense'] and len(defense) == evidence['n_defense'] and len(offense) >= 10 and len(defense) >= 10
            if not valid:
                missing.append(row['play_id'])
            participants.update(ids)
        result[game['game_id']] = {'complete': bool(final_ok and count and not missing),
            'finished_game_and_score_match': bool(final_ok), 'required_plays':count,
            'uncovered_play_ids':missing, 'participant_ids':sorted(participants), 'event_player_ids':sorted(events),
            'voided_event_player_ids':sorted(voided_events)}
    return result


def build(conn, frames, provenance, season=2025):
    claims = {}
    aliases = defaultdict(dict)
    roster_statuses = defaultdict(set)
    def claim(namespace, value, gsis, name, team, position, claim_season, method, source, evidence):
        value, gsis = external_id(value), valid_gsis(gsis)
        if not value or not gsis:
            return
        row = dict(namespace=namespace,external_id=value,gsis_id=gsis,player_name=name,
            team=team_key(team),position=scoring_position(position),season=int(claim_season),
            method=method,source_digest=source,evidence=evidence)
        # Keep one deterministic supporting observation per mapping/name/source
        # family. Different GSIS claims remain separate and force quarantine.
        key = (namespace,value,gsis,name,method)
        if key not in claims:
            row['claim_digest'] = artifact_digest(row)
            claims[key] = row
        aliases[normalized_name(name)][gsis] = row

    roster_rows = frames['weekly-rosters'].sort_values(['week','team','full_name']).to_dict('records')
    for row in roster_rows:
        gsis = valid_gsis(row['gsis_id'])
        if not gsis:
            continue
        names = {row['full_name']}
        if external_id(row.get('football_name')) and external_id(row.get('last_name')):
            names.add(row['football_name']+' '+row['last_name'])
        roster_statuses[(int(row['week']),team_key(row['team']),gsis)].add(row['status'])
        for name in sorted(names):
            claim('nflverse.gsis',gsis,gsis,name,row['team'],row['position'],season,
                  'nflverse_roster_ids',provenance['weekly-rosters']['responseHash'],
                  {'week':int(row['week']),'source':provenance['weekly-rosters']})
        for field, namespace in ROSTER_IDS.items():
            claim(namespace,row.get(field),gsis,row['full_name'],row['team'],row['position'],season,
                  'nflverse_roster_ids',provenance['weekly-rosters']['responseHash'],{'week':int(row['week']),'source':provenance['weekly-rosters']})
    current = query(conn, "SELECT id,season,canonical_name,position,team_abbrev,gsis_id,espn_id,yahoo_id,sleeper_player_id,mfl_id,fantasypros_player_id,draftkings_id,fetched_at,metadata->'sleeper'->>'first_name' sleeper_first,metadata->'sleeper'->>'last_name' sleeper_last FROM ff_players ORDER BY season,id")
    for row in current:
        for field,namespace in {**APP_IDS,'gsis_id':'nflverse.gsis'}.items():
            claim(namespace,row.get(field),row['gsis_id'],row['canonical_name'],row['team_abbrev'],row['position'],row['season'],
                'stored_canonical_player_ids',artifact_digest(row),{'source_row':json.loads(json.dumps(row,default=str)),
                'note':'Canonical database ID assertion; not independent verification against each provider.'})
        if external_id(row['sleeper_first']) and external_id(row['sleeper_last']):
            claim('nflverse.gsis',row['gsis_id'],row['gsis_id'],row['sleeper_first']+' '+row['sleeper_last'],row['team_abbrev'],row['position'],row['season'],
                'stored_sleeper_name_alias',artifact_digest(row),{'source_row':json.loads(json.dumps(row,default=str))})
    bridged = []
    for row in current:
        if valid_gsis(row['gsis_id']) or row['position']=='DST':
            continue
        same = [p for p in current if p['season']==row['season'] and valid_gsis(p['gsis_id'])
                and normalized_name(p['canonical_name'])==normalized_name(row['canonical_name'])
                and team_key(p['team_abbrev'])==team_key(row['team_abbrev'])
                and scoring_position(p['position'])==scoring_position(row['position'])]
        ids = {p['gsis_id'] for p in same}
        if len(ids)!=1:
            continue
        gsis = next(iter(ids))
        evidence = {'source_row':json.loads(json.dumps(row,default=str)),
                    'matched_canonical_rows':json.loads(json.dumps(same,default=str)),
                    'note':'Identity bridge for duplicate local rows; no row/FK merge or roster mutation.'}
        for field,namespace in APP_IDS.items():
            claim(namespace,row.get(field),gsis,row['canonical_name'],row['team_abbrev'],row['position'],row['season'],
                'unique_current_roster_identity',artifact_digest(evidence),evidence)
        bridged.append({'local_id':row['id'],'name':row['canonical_name'],'gsis_id':gsis})
    entries = query(conn, '''SELECT DISTINCT draft_group_id,player_id,player_dk_id,player_name,position,
        team_nflverse,week,game_id,source_sha256 FROM nfl_dk_archive_salaries
        WHERE season=%s ORDER BY draft_group_id,player_id''',(season,))
    stats = query(conn, 'SELECT game_id,gsis_id,player_name,position,team,stats,source_sha256 FROM nfl_dk_archive_player_stats ORDER BY game_id,gsis_id')
    game_stats = {(r['game_id'],r['gsis_id']):r for r in stats}
    by_name_game = defaultdict(dict)
    for row in stats:
        by_name_game[(row['game_id'],team_key(row['team']),normalized_name(row['player_name']))][row['gsis_id']] = row
    unresolved = []
    for entry in entries:
        if entry['position']=='DST':
            continue
        named = by_name_game[(entry['game_id'],team_key(entry['team_nflverse']),normalized_name(entry['player_name']))]
        candidates = named if named else aliases.get(normalized_name(entry['player_name']),{})
        if len(candidates)!=1:
            unresolved.append({'player_id':entry['player_id'],'name':entry['player_name'],
                               'status':'ambiguous' if candidates else 'missing_identity'})
            continue
        gsis = next(iter(candidates))
        source = next(iter(candidates.values()))
        # A provider-attested unique alias establishes physical identity only;
        # it never asserts this player was active/on this team at slate lock.
        evidence = {'draft_group_id':entry['draft_group_id'],'game_id':entry['game_id'],
                    'salary_source_sha256':entry['source_sha256'],
                    'identity_source_digest':source.get('claim_digest',source.get('source_sha256')),
                    'identity_only_not_roster_or_availability':True}
        for field,namespace in [('player_id','draftkings.player_id'),('player_dk_id','draftkings.player_dk_id')]:
            claim(namespace,entry[field],gsis,entry['player_name'],entry['team_nflverse'],entry['position'],season,
                'unique_game_stat_identity' if named else 'unique_provider_attested_name_alias',
                entry['source_sha256'],evidence)
    records = sorted(claims.values(),key=lambda c:c['claim_digest'])
    # Previous conflicting claims must also participate in resolution. Never
    # resolve an ID locally while the persistent registry quarantines it.
    exists = query(conn,"SELECT to_regclass('nfl_player_identity_claims') AS name")[0]['name']
    previous = query(conn,'SELECT namespace,external_id,gsis_id FROM nfl_player_identity_claims') if exists else []
    resolved = registry(records+previous)
    games = query(conn, '''SELECT DISTINCT g.* FROM nfl_dk_archive_games g JOIN nfl_dk_archive_slate_games s USING(game_id)
        WHERE draft_group_id=ANY(%s) ORDER BY game_id''',(list(SLATES),))
    coverage = game_coverage(frames['play-by-play'],frames['participation'],games)
    results = []
    for entry in entries:
        if entry['draft_group_id'] not in SLATES or entry['position']=='DST':
            continue
        ids = [resolved.get((namespace,external_id(entry[field]))) for field,namespace in
               [('player_id','draftkings.player_id'),('player_dk_id','draftkings.player_dk_id')]]
        candidates = {r['gsis_id'] for r in ids if r and r['status']=='resolved'}
        gsis = next(iter(candidates)) if len(candidates)==1 and all(not r or r['status']=='resolved' for r in ids) else None
        stat = game_stats.get((entry['game_id'],gsis))
        statuses = roster_statuses.get((entry['week'],team_key(entry['team_nflverse']),gsis),set())
        status = next(iter(statuses)) if len(statuses)==1 else None
        evidence = {'identity':ids,'roster_statuses':sorted(str(s) for s in statuses),
                    'game_coverage':coverage[entry['game_id']], 'sources':provenance,
                    'retrospective_only':True}
        if stat and team_key(stat['team'])==team_key(entry['team_nflverse']):
            try:
                points = checked_points(entry['position'],stat['stats'])
                decision = {'actual':points,'status':'reconstructed_stat_line'}
                evidence.update(source_sha256=stat['source_sha256'],stats=stat['stats'],source_position=stat['position'])
            except ValueError as exc:
                decision = {'actual':None,'status':'stat_reconciliation_required','reason':str(exc)}
        else:
            decision = zero_result(gsis,coverage[entry['game_id']],has_stat_row=stat is not None,roster_status=status)
        results.append({'draft_group_id':entry['draft_group_id'],'player_id':entry['player_id'],
            'player_dk_id':entry['player_dk_id'],
            'player_name':entry['player_name'],'game_id':entry['game_id'],'gsis_id':gsis,
            **decision,'evidence':evidence})
    report = {'version':VERSION,'sources':provenance,'claim_count':len(records),
        'namespaces':dict(Counter(r['namespace'] for r in records)),
        'resolved_identifiers':sum(r['status']=='resolved' for r in resolved.values()),
        'conflicts':[{'namespace':k[0],'external_id':k[1],**v} for k,v in sorted(resolved.items()) if v['status']=='conflict'],
        'unmatched_archive_players':sorted({r['name'] for r in unresolved}),
        'current_canonical_coverage':{'rows':len(current),'with_valid_gsis':sum(bool(valid_gsis(r['gsis_id'])) for r in current)},
        'duplicate_local_identity_bridges':bridged,
        'unresolved_current_players':[{'local_id':r['id'],'name':r['canonical_name'],'position':r['position'],'team':r['team_abbrev']}
            for r in current if r['position']!='DST' and not valid_gsis(r['gsis_id']) and r['id'] not in {b['local_id'] for b in bridged}],
        'pilot_counts':{str(s):dict(Counter(r['status'] for r in results if r['draft_group_id']==s)) for s in SLATES},
        'participation_coverage':coverage,
        'claim_set_digest':artifact_digest(records),'result_set_digest':artifact_digest(results),
        'limitations':['Recorded participation is retrospective, not pregame evidence or official snap counts.',
            'Resolved means available ID assertions agree, not that every external provider independently verified the mapping.',
            'DK salary-entry IDs, permanent player_id and player_dk_id namespaces are not interchangeable.',
            'Identity alone never establishes current team, starting role or availability.']}
    return records, results, report


def persist(conn, claims, results, report):
    with conn.cursor() as cur:
        cur.execute(DDL)
        cur.execute(SLATE_MIGRATION)
        # Serialize registry writers and recheck conflicts before writing results.
        cur.execute("SELECT pg_advisory_xact_lock(hashtext('nfl_player_identity_registry'))")
        fields = ('claim_digest','namespace','external_id','gsis_id','player_name','season','team','position','method','source_digest','evidence')
        execute_values(cur,'INSERT INTO nfl_player_identity_claims ('+','.join(fields)+') VALUES %s ON CONFLICT DO NOTHING',
                       [tuple(Json(r[k]) if k=='evidence' else r[k] for k in fields) for r in claims],page_size=1000)
        cur.execute("SELECT namespace,external_id,gsis_id,status FROM nfl_player_identity_crosswalk WHERE namespace LIKE 'draftkings.%'")
        crosswalk = {(r[0],r[1]):(r[2],r[3]) for r in cur.fetchall()}
        for r in results:
            for field,namespace in [('player_id','draftkings.player_id'),('player_dk_id','draftkings.player_dk_id')]:
                value = crosswalk.get((namespace,external_id(r[field])))
                if r['gsis_id'] and value and value != (r['gsis_id'],'resolved'):
                    raise ValueError('Concurrent identity conflict; rerun against fresh registry')
        digest = artifact_digest(report)
        cur.execute('INSERT INTO nfl_player_identity_runs(run_digest,report) VALUES(%s,%s) ON CONFLICT DO NOTHING',(digest,Json(report)))
        execute_values(cur,'''INSERT INTO nfl_dk_archive_result_reconciliation
            (result_digest,run_digest,draft_group_id,player_id,game_id,gsis_id,actual_dk_fpts,status,evidence)
            VALUES %s ON CONFLICT DO NOTHING''',[(artifact_digest({'run':digest,'result':r}),digest,r['draft_group_id'],r['player_id'],r['game_id'],r['gsis_id'],r['actual'],r['status'],Json(r)) for r in results])
    return digest


def main():
    parser=argparse.ArgumentParser(__doc__)
    parser.add_argument('--source-root',type=Path,required=True)
    parser.add_argument('--output',type=Path,default=Path('artifacts/nfl-identity-registry'))
    parser.add_argument('--persist',action='store_true')
    args=parser.parse_args()
    frames,provenance=read_sources(args.source_root,2025)
    with psycopg2.connect(load_config().database_url) as conn:
        conn.set_session(readonly=True,isolation_level='REPEATABLE READ')
        claims,results,report=build(conn,frames,provenance)
    report=json.loads(json.dumps(report,default=str))
    if args.persist:
        with psycopg2.connect(load_config().database_url) as conn:
            persist(conn,claims,results,report)
    args.output.mkdir(parents=True,exist_ok=True)
    (args.output/'report.json').write_text(json.dumps(report,indent=2),encoding='utf-8')
    (args.output/'results.json').write_text(json.dumps(results,indent=2,default=str),encoding='utf-8')
    print(json.dumps({k:v for k,v in report.items() if k not in ('sources','participation_coverage','unmatched_archive_players')},indent=2))


if __name__=='__main__':
    main()
