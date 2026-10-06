"""Local capture, forecast, sensitivity and walk-forward longest-TD research.

python -m research.nfl_longest_touchdown capture --output INPUT.json.gz
python -m research.nfl_longest_touchdown forecast --input INPUT.json.gz --request REQUEST.json --output RESULT.json
python -m research.nfl_longest_touchdown backtest --input INPUT.json.gz --season 2025 --start-week 5 --end-week 8 --output REPORT.json

Read-only DB capture. Files are written with exclusive creation; saved inputs
and predictions are never overwritten. No production/optimizer integration.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from dataclasses import replace
from datetime import datetime, timezone, timedelta
import gzip
from hashlib import sha256
import json
from pathlib import Path

import numpy as np

from model.nfl_longest_touchdown import IMPLEMENTATION_SHA256, Settings, VERSION, evaluate, prepare, simulate, timestamp

RESEARCH_SHA256 = sha256(Path(__file__).read_bytes()).hexdigest()


def read(path: Path):
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt", encoding="utf-8") as f:
        return json.load(f)


def write(path: Path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "xt", encoding="utf-8") as f:
        json.dump(payload, f, default=str, allow_nan=False, separators=(",", ":"))


def capture(db, minimum_season: int, maximum_season: int) -> dict:
    # DISTINCT role/GSIS before aggregation prevents defensive participant
    # multiplicity. Resolve positions by season/GSIS only when unambiguous.
    rows = db.execute("""WITH identities AS (
        SELECT season,gsis_id,MIN(position) position FROM ff_players
        WHERE gsis_id IS NOT NULL GROUP BY season,gsis_id HAVING COUNT(DISTINCT position)=1
      ), historical_positions AS (
        SELECT season,week,player_gsis_id,MIN(position) position FROM ff_v2_roster_weeks
        WHERE position IS NOT NULL GROUP BY season,week,player_gsis_id HAVING COUNT(DISTINCT position)=1
      ), actors AS (
        SELECT DISTINCT game_id,play_id,season,week,role,player_id,player_name
        FROM nfl_pbp_play_participants WHERE role IN ('receiver','rusher')
      ), attribution AS (
        SELECT a.game_id,a.play_id,jsonb_agg(DISTINCT jsonb_build_object(
          'role',a.role,'player_id',a.player_id,'name',a.player_name,'position',COALESCE(h.position,i.position))) actors
        FROM actors a LEFT JOIN identities i ON i.season=a.season AND i.gsis_id=a.player_id
        LEFT JOIN historical_positions h ON h.season=a.season AND h.week=a.week AND h.player_gsis_id=a.player_id
        GROUP BY a.game_id,a.play_id
      ), timed AS (
        SELECT p.*,LEAD(game_seconds_remaining) OVER(PARTITION BY game_id ORDER BY play_id) next_clock
        FROM nfl_pbp_archetypes p WHERE season BETWEEN %s AND %s AND season_type='REG'
      ) SELECT p.game_id,p.play_id,p.season,p.week,p.season_type,p.posteam,p.defteam,p.home_team,p.away_team,
        p.drive,p.down,p.ydstogo,p.yardline_100,p.play_type,p.yards_gained,p.air_yards,p.yards_after_catch,
        p.game_seconds_remaining,p.score_differential,p.had_sack,p.turnover_type,p.description,
        p.drive_plays,p.play_labeller_version,p.drive_labeller_version,p.labelled_at,
        CASE WHEN p.game_seconds_remaining-p.next_clock BETWEEN 1 AND 60
          THEN p.game_seconds_remaining-p.next_clock ELSE NULL END elapsed_seconds,
        g.kickoff,g.game_type,g.season canonical_season,g.week canonical_week,
        h.abbreviation canonical_home,a.abbreviation canonical_away,COALESCE(q.actors,'[]'::jsonb) actors
      FROM timed p JOIN nfl_season_games g ON g.nflverse_game_id=p.game_id AND g.season=p.season AND g.week=p.week
      JOIN nfl_teams h ON h.team_id=g.home_team_id JOIN nfl_teams a ON a.team_id=g.away_team_id
      LEFT JOIN attribution q ON q.game_id=p.game_id AND q.play_id=p.play_id
      WHERE g.game_type='REG' ORDER BY g.kickoff,p.game_id,p.play_id""", (minimum_season, maximum_season))
    plays = json.loads(json.dumps([dict(r) for r in rows], default=str))
    if len({(r['game_id'],r['play_id']) for r in plays}) != len(plays):
        raise ValueError("Schedule/participant join duplicated a source play")
    games = db.execute("""SELECT g.nflverse_game_id game_id,g.season,g.week,g.kickoff,
                         a.abbreviation away,h.abbreviation home FROM nfl_season_games g
                         JOIN nfl_teams a ON a.team_id=g.away_team_id JOIN nfl_teams h ON h.team_id=g.home_team_id
                         WHERE g.season BETWEEN %s AND %s AND g.game_type='REG' AND g.nflverse_game_id IS NOT NULL""",
                       (minimum_season, maximum_season))
    return {"version": VERSION, "captured_at": datetime.now(timezone.utc).isoformat(),
        "source": "nfl_pbp_archetypes + deduplicated nfl_pbp_play_participants + canonical nfl_season_games",
        "participant_as_of_available": False, "plays": plays,
        "games":json.loads(json.dumps([dict(g) for g in games],default=str)),
        "coverage": {"games": len({r['game_id'] for r in plays}), "plays": len(plays),
                     "seasons": dict(Counter(r['season'] for r in plays))}}


def inferred_roster(history: list[dict], teams: tuple[str,str], season: int) -> list[dict]:
    """Historical usage roster only. Never call this verified game-day availability."""
    latest_games = {}
    for team in teams:
        games = sorted({(timestamp(r['kickoff']),r['game_id']) for r in history if r['team']==team})[-3:]
        latest_games[team] = {g for _,g in games}
    candidates = {}
    for r in history:
        if r['team'] not in teams or not r['actor'] or r['game_id'] not in latest_games[r['team']]:
            continue
        key = r['actor']
        names = [a['name'] for a in r['actors'] if a.get('player_id') == key]
        record = {'identity':key,'name':names[0] if names else key,'team':r['team'],
                  'position':r['position'],'status':'active'}
        # Player moving teams: use most recent observed team, not both rosters.
        if key not in candidates or timestamp(r['kickoff']) > candidates[key][0]:
            candidates[key] = timestamp(r['kickoff']),record
    return [record for _,record in candidates.values()]


def simple_baseline(history: list[dict], request: dict, settings: Settings) -> dict:
    """Independent player TD counts/distances; no field/opponent/script model.

    Uses previous three team games for workload (including zero-participation
    games), league-position TD-rate smoothing and empirical scoring distances.
    This is a challenger baseline, not a bookmaker or a validated forecast.
    """
    roster = [p for p in request['players'] if p['status']=='active']
    rng = np.random.default_rng(settings.seed)
    longest = np.zeros((settings.draws,len(roster)),int)
    for j,p in enumerate(roster):
        games = sorted({(timestamp(r['kickoff']),r['game_id']) for r in history if r['team']==p['team']})[-3:]
        ids = {g for _,g in games}
        for action in ('run','pass'):
            own = [r for r in history if r['actor']==p['identity'] and r['action']==action]
            peers = [r for r in history if r['position']==p['position'] and r['action']==action and r['actor']]
            if not peers or not games:
                continue
            peer_rate = sum(r['td'] for r in peers)/len(peers)
            rate = (sum(r['td'] for r in own)+settings.peer_prior_opportunities*peer_rate)/(len(own)+settings.peer_prior_opportunities)
            workload = sum(r['game_id'] in ids and r['team']==p['team'] for r in own)/len(games)
            count = rng.poisson(workload*rate,settings.draws)
            distances = [int(round(r['field'])) for r in own if r['td']]
            prior = [int(round(r['field'])) for r in peers if r['td']]
            distances = distances+prior
            if not distances:
                continue
            for k in range(int(count.max(initial=0))):
                mask = count > k
                longest[mask,j] = np.maximum(longest[mask,j],rng.choice(distances,int(mask.sum())))
    best = longest.max(axis=1)
    winners = (longest==best[:,None]) & (best[:,None]>0)
    ties = winners.sum(axis=1)
    share = np.divide(winners,ties[:,None],out=np.zeros_like(longest,float),where=ties[:,None]>0).mean(axis=0)
    return {'game':request['game'], 'players':sorted([
        {'identity':p['identity'],'name':p['name'],'longest_td_win_share':float(share[j])} for j,p in enumerate(roster)
        ] + [{'identity':'OTHER:'+t,'name':'OTHER:'+t,'longest_td_win_share':0.} for t in (request['game']['away'],request['game']['home'])],
        key=lambda p:-p['longest_td_win_share']), 'no_scrimmage_td_probability':float(np.mean(best==0))}


def calibration(grades: list[dict]) -> list[dict]:
    bins = defaultdict(list)
    for grade in grades:
        for row in grade['calibration_rows']:
            bins[min(9,int(row['probability']*10))].append(row)
    return [{'range':[i/10,(i+1)/10], 'player_game_outcomes':len(rows),
             'mean_probability':float(np.mean([r['probability'] for r in rows])),
             'observed_share':float(np.mean([r['observed_share'] for r in rows]))} for i,rows in sorted(bins.items())]


def walk_forward(snapshot: dict, season: int, start_week: int, end_week: int, settings: Settings,
                 *, limit: int | None = None) -> dict:
    games = {}
    for r in snapshot['plays']:
        if int(r['season'])==season and start_week <= int(r['week']) <= end_week:
            games[r['game_id']] = {'game_id':r['game_id'],'season':season,'week':int(r['week']),
                                  'kickoff':r['kickoff'],'away':r['canonical_away'],'home':r['canonical_home']}
    ordered = sorted(games.values(),key=lambda g:(timestamp(g['kickoff']),g['game_id']))
    if limit is not None:
        ordered = ordered[:limit]
    predictions, grades, baselines, skipped = [], [], [], []
    # Historical labels/participants are later corrections: explicit exploratory
    # walk-forward fitting, never a claim of immutable pregame source availability.
    all_rows,_ = prepare(snapshot,(max(timestamp(g['kickoff']) for g in ordered)+timedelta(days=2)).isoformat(),retrospective=True) if ordered else ([],{})
    for g in ordered:
        cutoff = (timestamp(g['kickoff'])-timedelta(minutes=1)).isoformat()
        history = [r for r in all_rows if timestamp(r['kickoff']) < timestamp(cutoff)]
        teams = (g['away'],g['home'])
        try:
            if any(len({r['game_id'] for r in history if r['team']==t}) < 3 for t in teams):
                raise ValueError('Need three prior team games')
            roster = inferred_roster(history,teams,season)
            request = {'decision_at':cutoff,'game':g,'players':roster,'retrospective':True,
                       'roster_evidence':'last_three_prior_games_usage_only_not_historical_inactives'}
            # Subset before fit reduces repeated parsing and makes target exclusion explicit.
            training_snapshot = {**snapshot,'plays':[r for r in snapshot['plays'] if timestamp(r['kickoff']) < timestamp(cutoff)]}
            forecast = simulate(training_snapshot,request,settings)
            grade = evaluate(forecast,all_rows)
            baseline = simple_baseline(history,request,settings)
            predictions.append(forecast); grades.append(grade); baselines.append(evaluate(baseline,all_rows))
            print(json.dumps({'graded':g['game_id'],'brier':grade['brier_score']}),flush=True)
        except ValueError as exc:
            skipped.append({'game_id':g['game_id'],'reason':str(exc)})
    def summary(items):
        return {'games':len(items),'mean_brier':float(np.mean([g['brier_score'] for g in items])) if items else None,
                'mean_log_loss':float(np.mean([g['log_loss'] for g in items])) if items else None,
                'top_choice_hit_rate':float(np.mean([g['top_choice_hit'] for g in items])) if items else None,
                'calibration':calibration(items)}
    return {'version':VERSION,'authority':'retrospective_exploratory_not_validated',
        'implementation_sha256':IMPLEMENTATION_SHA256,'research_sha256':RESEARCH_SHA256,
        'source_sha256':sha256(json.dumps(snapshot,sort_keys=True).encode()).hexdigest(),
        'season':season,'weeks':[start_week,end_week],'settings':settings.__dict__,
        'model':summary(grades),'independent_td_baseline':summary(baselines),
        'grades':grades,'baseline_grades':baselines,'predictions':predictions,'skipped':skipped,
        'limitations':['Uses corrected PBP and reconstructed usage rosters, not archived pregame inactives.',
                       'Calibration bins contain correlated player-game observations; counts are not independent sample sizes.',
                       'No promotion gate passed. Evaluate additional untouched seasons and frozen forward predictions before using edges.']}


def frozen_baseline(snapshot: dict, request: dict, settings: Settings) -> dict:
    """Freeze the challenger baseline from the same pre-decision evidence as the forecast.

    Must be run before kickoff; grading against a baseline built after the
    result would let later corrections leak into the comparison.
    """
    history,_ = prepare(snapshot,request['decision_at'],retrospective=bool(request.get('retrospective')))
    history = [r for r in history if r['game_id']!=request['game']['game_id']]
    result = simple_baseline(history,request,settings)
    result.update(version=VERSION,authority='challenger_baseline_not_validated',decision_at=request['decision_at'],
                  frozen_at=datetime.now(timezone.utc).isoformat(),settings=settings.__dict__,
                  source_sha256=sha256(json.dumps(snapshot,sort_keys=True).encode()).hexdigest(),
                  request_sha256=sha256(json.dumps(request,sort_keys=True).encode()).hexdigest())
    return result


SCORING_TEXT_EXCLUSIONS = ('TWO-POINT','TWO POINT')


def grade(snapshot: dict, forecast: dict, baseline: dict | None = None) -> dict:
    """Grade a frozen forward forecast against post-game PBP, failing closed.

    Every run/pass play in the target game whose description mentions a
    touchdown must survive `prepare` as a verified scoring row. If any was
    quarantined (reversal text, lateral, fumble, unverified yardage), the real
    longest TD may be the excluded play, so the grade is `needs_review`, never
    a silent score. Missing target-game PBP is `outcome_unknown`.
    """
    game_id = forecast['game']['game_id']
    target = [r for r in snapshot['plays'] if r['game_id']==game_id]
    base = {'version':VERSION,'game':forecast['game'],'graded_at':datetime.now(timezone.utc).isoformat(),
            'forecast_decision_at':forecast['decision_at'],'forecast_request_sha256':forecast.get('request_sha256'),
            'forecast_implementation_sha256':forecast.get('implementation_sha256'),
            'source_sha256':sha256(json.dumps(snapshot,sort_keys=True).encode()).hexdigest(),
            'authority':'single_game_forward_grade_not_calibration'}
    if not target:
        return {**base,'status':'outcome_unknown','reason':'No PBP for the target game; not a no-TD result'}
    latest_kickoff = max(timestamp(r['kickoff']) for r in snapshot['plays'])
    rows,audit = prepare(snapshot,(latest_kickoff+timedelta(days=2)).isoformat(),retrospective=True)
    verified = {int(r['play_id']) for r in rows if r['game_id']==game_id and r['td']}
    quarantined = [{'play_id':int(r['play_id']),'description':r.get('description')} for r in target
                   if r.get('play_type') in ('run','pass') and 'TOUCHDOWN' in (r.get('description') or '').upper()
                   and not any(s in (r.get('description') or '').upper() for s in SCORING_TEXT_EXCLUSIONS)
                   and int(r['play_id']) not in verified]
    result = {**base,'target_plays':len(target),'verified_scrimmage_tds':len(verified),'quarantined_td_plays':quarantined}
    if quarantined:
        return {**result,'status':'needs_review',
                'reason':'A touchdown-mentioning scrimmage play was excluded by verification; resolve it before scoring'}
    model = evaluate(forecast,rows)
    named = {p['identity']:p['name'] for p in forecast['players']}
    model['winner_names'] = [named.get(i,i) for i in model['winners']]
    model['winner_was_unmodeled'] = any(i.startswith('OTHER:') for i in model['winners'])
    result.update(status='graded',model=model)
    if baseline is not None:
        if baseline.get('decision_at')!=forecast['decision_at']:
            raise ValueError('Baseline was not frozen at the forecast decision time')
        result['independent_td_baseline'] = evaluate(baseline,rows)
        result['model_minus_baseline'] = {k:model[k]-result['independent_td_baseline'][k] for k in ('brier_score','log_loss')}
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command',required=True)
    c = sub.add_parser('capture'); c.add_argument('--minimum-season',type=int,default=2023); c.add_argument('--maximum-season',type=int,default=2026)
    g = sub.add_parser('grade'); g.add_argument('--input',type=Path,required=True)
    g.add_argument('--forecast',type=Path,required=True); g.add_argument('--baseline',type=Path)
    b = sub.add_parser('baseline'); b.add_argument('--input',type=Path,required=True); b.add_argument('--request',type=Path,required=True)
    b.add_argument('--draws',type=int,default=2000); b.add_argument('--seed',type=int,default=20261006)
    for name in ('forecast','backtest'):
        p = sub.add_parser(name); p.add_argument('--input',type=Path,required=True)
        p.add_argument('--draws',type=int,default=2000); p.add_argument('--seed',type=int,default=20261006)
        p.add_argument('--newcomer-reserve',action='store_true')
        if name=='forecast':
            p.add_argument('--request',type=Path,required=True); p.add_argument('--sensitivity',action='store_true')
        else:
            p.add_argument('--season',type=int,required=True); p.add_argument('--start-week',type=int,default=5)
            p.add_argument('--end-week',type=int,default=8); p.add_argument('--limit',type=int)
    for p in sub.choices.values():
        p.add_argument('--output',type=Path,required=True)
    args = parser.parse_args()
    if args.output.exists():
        raise FileExistsError('Choose a new output path; frozen artifacts cannot be overwritten')
    if args.command=='capture':
        from config import load_config
        from db.database import DatabaseManager
        db = DatabaseManager(load_config().database_url,initialize_schema=False)
        with db.reuse_connection():
            result = capture(db,args.minimum_season,args.maximum_season)
    else:
        snapshot = read(args.input)
        settings = (Settings(draws=args.draws,seed=args.seed,newcomer_reserve=getattr(args,'newcomer_reserve',False))
                    if hasattr(args,'draws') else None)
        if args.command=='grade':
            result = grade(snapshot,read(args.forecast),read(args.baseline) if args.baseline else None)
        elif args.command=='baseline':
            request = read(args.request)
            if datetime.now(timezone.utc) >= timestamp(request['game']['kickoff']):
                raise ValueError('Baseline must be frozen before kickoff')
            result = frozen_baseline(snapshot,request,settings)
        elif args.command=='backtest':
            result = walk_forward(snapshot,args.season,args.start_week,args.end_week,settings,limit=args.limit)
        else:
            request = read(args.request)
            result = simulate(snapshot,request,settings)
            if args.sensitivity:
                alternatives = [replace(settings,peer_prior_opportunities=40.,defense_prior_opportunities=75.),
                                replace(settings,peer_prior_opportunities=160.,defense_prior_opportunities=300.)]
                result['sensitivity'] = [simulate(snapshot,request,s) for s in alternatives]
                result['sensitivity_interpretation'] = 'Chosen assumption variations, not confidence intervals'
    result['research_sha256'] = RESEARCH_SHA256
    write(args.output,result)
    print(json.dumps({'output':str(args.output),'status':result.get('status'),'authority':result.get('authority'),
                      'coverage':result.get('coverage'),'model':result.get('model')},default=str))


if __name__=='__main__':
    main()
