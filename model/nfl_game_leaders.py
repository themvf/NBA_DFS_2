"""Joint single-game yardage/reception leaders. Experimental, market free."""
from __future__ import annotations

from collections import defaultdict
from dataclasses import asdict, dataclass
from hashlib import sha256
import json
from pathlib import Path

import numpy as np

from model.nfl_longest_touchdown import canonical, timestamp

VERSION = 'nfl-game-leaders-v1'
METRICS = ('rushing_yards', 'receptions', 'receiving_yards')
IMPLEMENTATION_SHA256 = sha256(Path(__file__).read_bytes()).hexdigest()


@dataclass(frozen=True)
class Settings:
    draws: int = 5000
    seed: int = 20261008
    recent_games: int = 6
    half_life_games: float = 3.
    prior_season_weight: float = .35
    team_prior_games: float = 3.
    efficiency_prior_events: float = 30.
    opponent_prior_events: float = 150.
    surprise_slots: int = 3

    def __post_init__(self):
        if min(self.draws, self.recent_games, self.surprise_slots) < 1:
            raise ValueError('Positive simulation, window and surprise slot counts required')
        if any(not np.isfinite(v) or v <= 0 for v in (self.half_life_games,
                self.prior_season_weight, self.team_prior_games,
                self.efficiency_prior_events, self.opponent_prior_events)):
            raise ValueError('Finite positive smoothing settings required')
        if self.prior_season_weight > 1:
            raise ValueError('Prior-season weight must be <= 1')


def prepare(snapshot: dict, decision_at: str, retrospective: bool = False):
    """Reconcile PBP counts/yards with separately captured box totals, full game.

    Missing/ambiguous actors and complex stat corrections quarantine the complete
    training game rather than becoming zero-yard touches. All periods included.
    """
    cutoff = timestamp(decision_at)
    games = {g['game_id']: g for g in snapshot['games']}
    if len(games) != len(snapshot['games']):
        raise ValueError('Duplicate canonical game')
    boxes = defaultdict(list)
    seen = set()
    for row in snapshot.get('boxes', []):
        key = (row['game_id'], row['identity'])
        if key in seen:
            raise ValueError('Duplicate box identity')
        seen.add(key)
        if row['game_id'] not in games:
            raise ValueError('Unmatched box game')
        g = games[row['game_id']]
        if row['team'] not in (g['away'], g['home']):
            raise ValueError('Box team does not match canonical schedule')
        if not retrospective and timestamp(row['fetched_at']) > cutoff:
            continue
        for field in ('carries', 'targets', *METRICS):
            if row.get(field) is None or not np.isfinite(float(row[field])):
                raise ValueError('Missing or nonfinite box stat')
        if min(row['carries'], row['targets'], row['receptions']) < 0 or row['receptions'] > row['targets']:
            raise ValueError('Invalid box opportunity counts')
        boxes[row['game_id']].append(row)
    events = defaultdict(list)
    issues = defaultdict(set)
    seen = set()
    for r in snapshot['plays']:
        key = (r['game_id'], r['play_id'])
        if key in seen:
            raise ValueError('Duplicate PBP play')
        seen.add(key)
        g = games.get(r['game_id'])
        if g is None:
            raise ValueError('Unmatched PBP game')
        if (canonical(r['home_team']) != g['home'] or canonical(r['away_team']) != g['away']
                or int(r['season']) != int(g['season']) or int(r['week']) != int(g['week'])):
            raise ValueError('PBP canonical schedule mismatch')
        if timestamp(g['kickoff']) >= cutoff:
            continue
        if not retrospective and timestamp(r['labelled_at']) > cutoff:
            issues[r['game_id']].add('later_labels')
            continue
        text = (r.get('description') or '').upper()
        if r['play_type'] not in ('run', 'qb_kneel', 'pass') or any(t in text for t in (
                'NO PLAY', 'NULLIFIED', 'TWO-POINT', 'TWO POINT', 'SPIKE')):
            continue
        if r.get('quarter') is None:
            issues[r['game_id']].add('missing_period')
        action = 'carries' if r['play_type'] in ('run','qb_kneel') else 'targets'
        role = 'rusher' if action == 'carries' else 'receiver'
        actors = {a['player_id']: a for a in r.get('actors', []) if a['role'] == role and a.get('player_id')}
        if not actors and action == 'targets' and (r.get('had_sack') or r.get('yards_after_catch') is None):
            continue  # sacks/throwaways are not assigned receiver targets
        if len(actors) != 1:
            issues[r['game_id']].add('unresolved_actor')
            continue
        actor = next(iter(actors.values()))
        caught = action == 'targets' and r.get('yards_after_catch') is not None
        yards = r.get('yards_gained') if action == 'carries' else (
            (r['air_yards'] + r['yards_after_catch'] if r.get('air_yards') is not None else r.get('yards_gained')) if caught else 0)
        if yards is None or not np.isfinite(float(yards)):
            issues[r['game_id']].add('missing_yards')
            continue
        events[r['game_id']].append(dict(game_id=r['game_id'], team=canonical(r['posteam']),
            identity=actor['player_id'], position=actor.get('position') or 'UNKNOWN',
            action=action, caught=caught, yards=float(yards)))
    accepted, rejected = [], []
    for gid, g in sorted(games.items(), key=lambda item: (timestamp(item[1]['kickoff']), item[0])):
        if timestamp(g['kickoff']) >= cutoff:
            continue
        reasons = set(issues[gid])
        if not g.get('completed') or g.get('home_score') is None or g.get('away_score') is None:
            reasons.add('game_not_final')
        if not retrospective and timestamp(g['source_captured_at']) > cutoff:
            reasons.add('later_schedule_capture')
        if not snapshot.get('game_coverage', {}).get(gid, {}).get('regulation_end_observed'):
            reasons.add('missing_terminal_evidence')
        if not boxes[gid]:
            reasons.add('no_box_totals')
        actual = defaultdict(lambda: dict(carries=0, targets=0, rushing_yards=0., receptions=0, receiving_yards=0.))
        for e in events[gid]:
            stats = actual[e['team'], e['identity']]
            stats[e['action']] += 1
            if e['action'] == 'carries':
                stats['rushing_yards'] += e['yards']
            elif e['caught']:
                stats['receptions'] += 1
                stats['receiving_yards'] += e['yards']
        expected = {(r['team'], r['identity']): r for r in boxes[gid]}
        for key in set(actual) | set(expected):
            for field in ('carries', 'targets', *METRICS):
                if abs(actual[key][field] - expected.get(key, {}).get(field, 0)) > .01:
                    reasons.add('pbp_box_mismatch')
        if reasons:
            rejected.append({'game_id': gid, 'reasons': sorted(reasons)})
        else:
            accepted.append({'game': g, 'boxes': boxes[gid], 'events': events[gid]})
    if not accepted:
        raise ValueError('No complete reconciled prior games')
    return accepted, rejected


def summaries(history):
    result = []
    for item in history:
        g = item['game']
        for team, defense in ((g['away'], g['home']), (g['home'], g['away'])):
            box = [r for r in item['boxes'] if r['team'] == team]
            result.append({**g, 'team': team, 'defense': defense,
                'games':1,
                **{k: sum(r[k] for r in box) for k in ('carries', 'targets', *METRICS)}})
    return result


def opponent_effect(rows, defense, season, numerator, denominator, prior):
    current = [r for r in rows if r['season'] == season]
    differences, evidence = [], []
    for r in current:
        if r['defense'] != defense or r[denominator] == 0:
            continue
        others = [q for q in current if q['team'] == r['team'] and q['game_id'] != r['game_id']]
        n = sum(q[denominator] for q in others)
        if n == 0:
            continue
        baseline = sum(q[numerator] for q in others) / n
        difference = r[numerator] / r[denominator] - baseline
        differences.append((difference, r[denominator]))
        evidence.append({'game_id': r['game_id'], 'offense': r['team'], 'other_games': [q['game_id'] for q in others]})
    exposure = sum(n for _, n in differences)
    return {'adjustment': sum(d*n for d, n in differences) / (exposure + prior),
            'exposure': exposure, 'comparisons': evidence}


def allocate(rng, totals, probabilities, concentration):
    p = np.asarray(probabilities, float)
    if not np.isfinite(p).all() or min(p) < 0 or abs(p.sum()-1) > 1e-8:
        raise ValueError('Role shares must be nonnegative and sum to one')
    q = np.zeros((len(totals), len(p)))
    positive = p > 0
    q[:, positive] = rng.dirichlet(p[positive] * concentration, len(totals))
    result = np.zeros_like(q, int)
    remaining, mass = totals.copy(), np.ones(len(totals))
    for j in range(len(p)-1):
        rate = np.clip(np.divide(q[:, j], mass, out=np.zeros(len(totals)), where=mass > 1e-12), 0, 1)
        result[:, j] = rng.binomial(remaining, rate)
        remaining -= result[:, j]
        mass -= q[:, j]
    result[:, -1] = remaining
    if not np.array_equal(result.sum(axis=1), totals):
        raise AssertionError('Team opportunities not conserved')
    return result


def concentration(history, action):
    # Explicit initial uncertainty prior. Fitting longitudinal role dispersion is
    # separate work; do not mislabel this cross-sectional concentration as fitted.
    return 40. if action == 'targets' else 55.


def availability_check(request):
    """Validate provenance required for an availability-verified output.

    A depth chart is not a workload forecast or official game-day confirmation.
    Missing evidence leaves the output unresolved rather than inventing roles.
    """
    if not request.get('availability_verified'):
        return False
    game, cutoff = request['game'], timestamp(request['decision_at'])
    for team in (game['away'],game['home']):
        evidence = request.get('availability_evidence',{}).get(team,{})
        for provider in ('sleeper','fantasypros_depth','fantasypros_injuries'):
            source = evidence.get(provider,{})
            if source.get('season') != game['season'] or source.get('team') != team or not source.get('source_ref'):
                raise ValueError('Verified availability requires matching dual-provider evidence')
            captured = timestamp(source.get('captured_at'))
            if captured > cutoff or (cutoff-captured).total_seconds()>48*3600:
                raise ValueError('Availability evidence is later or stale')
            if provider=='fantasypros_injuries' and source.get('week')!=game['week']:
                raise ValueError('Injury evidence must match the decision week')
        if evidence.get('unresolved_conflicts'):
            raise ValueError('Provider conflicts prevent verified availability')
    if any(p['status']=='unresolved' for p in request['players']):
        raise ValueError('Unresolved player status prevents verified availability')
    return True


def forecast(history, request, cfg=Settings()):
    game = request['game']
    cutoff = timestamp(request['decision_at'])
    if cutoff >= timestamp(game['kickoff']):
        raise ValueError('Decision time must precede kickoff')
    verified = availability_check(request)
    if any(timestamp(h['game']['kickoff']) >= cutoff for h in history):
        raise ValueError('Future/target games in training')
    if len({h['game']['game_id'] for h in history})!=len(history):
        raise ValueError('Duplicate training game')
    history=sorted(history,key=lambda h:(timestamp(h['game']['kickoff']),h['game']['game_id']))
    teams = (game['away'], game['home'])
    players = request['players']
    if len({p['identity'] for p in players}) != len(players):
        raise ValueError('Duplicate request player identity')
    if any(p['team'] not in teams or p['status'] not in ('active', 'out', 'unresolved') for p in players):
        raise ValueError('Invalid request team/status')
    active = [p for p in players if p['status'] != 'out']
    request_identities={p['identity'] for p in players}
    if any(not any(p['team'] == t for p in active) for t in teams):
        raise ValueError('Both teams require candidates')
    rows = summaries(history)
    rng = np.random.default_rng(cfg.seed)
    # Whole-game bootstrap: the same source game and orientation for both teams.
    block_index = rng.integers(len(history), size=cfg.draws)
    orientation = rng.integers(2, size=cfg.draws)
    blocks = [[next(r for r in rows if r['game_id'] == h['game']['game_id'] and r['team'] == team)
               for team in (h['game']['away'], h['game']['home'])] for h in history]
    stats, candidates, diagnostics = {}, {}, {}
    for side, team in enumerate(teams):
        own = [h for h in history if team in (h['game']['away'], h['game']['home'])][-cfg.recent_games:]
        if len(own) < 3:
            raise ValueError('Need three reconciled prior team games')
        current = [h for h in own if h['game']['season'] == game['season']]
        recent = current or own
        weights = np.array([.5**((len(recent)-1-j)/cfg.half_life_games) for j in range(len(recent))])
        named = [p for p in active if p['team'] == team]
        historical_unknown = {}
        for h in recent:
            for b in h['boxes']:
                if b['team'] == team and b['identity'] not in request_identities:
                    historical_unknown[b['identity']] = {'identity': b['identity'], 'name': b['name'], 'position': b['position'], 'team': team, 'residual': True}
        latent = list(historical_unknown.values()) + [{'identity': f'NEW:{team}:{j}', 'name': 'Unresolved newcomer',
            'position': 'UNKNOWN', 'team': team, 'residual': True} for j in range(cfg.surprise_slots)]
        roster = named + latent
        for p in roster:
            candidates[p['identity']] = {**p, 'residual': p.get('residual', False)}
            stats[p['identity']] = np.zeros((cfg.draws, 3))
        selected_blocks = [blocks[b][(side+flip)%2] for b, flip in zip(block_index, orientation)]
        diagnostics[team] = {'prior_game_ids': [h['game']['game_id'] for h in recent], 'actions': {}}
        for action in ('carries', 'targets'):
            team_counts = np.array([sum(b[action] for b in h['boxes'] if b['team'] == team) for h in recent])
            league_mean = np.mean([r[action] for r in rows])
            mean = (np.dot(team_counts, weights) + cfg.team_prior_games*league_mean)/(weights.sum()+cfg.team_prior_games)
            volume_effect = opponent_effect(rows,teams[1-side],game['season'],action,'games',cfg.team_prior_games)
            mean *= float(np.clip(1+volume_effect['adjustment']/league_mean,.8,1.2))
            totals = np.rint(np.array([b[action] for b in selected_blocks])*mean/league_mean).astype(int)
            opportunity = []
            for p in roster:
                opportunity.append(sum(w*sum(b[action] for b in h['boxes'] if b['team']==team and b['identity']==p['identity']) for w,h in zip(weights,recent)))
            # Empirical opportunity fraction to actors absent from last three games.
            newcomer, all_touches = 0, 0
            prior_seen = defaultdict(list)
            for h in history:
                for t in (h['game']['away'],h['game']['home']):
                    b = [r for r in h['boxes'] if r['team']==t]
                    prior = prior_seen[t,h['game']['season']]
                    if len(prior) >= 3:
                        seen = set().union(*prior[-3:])
                        newcomer += sum(r[action] for r in b if r['identity'] not in seen)
                        all_touches += sum(r[action] for r in b)
                    prior.append({r['identity'] for r in b if r['carries']+r['targets']>0})
            reserve = newcomer/all_touches if all_touches else .03
            original_opportunities = float(np.dot(team_counts,weights))
            probabilities = np.array(opportunity)/original_opportunities*(1-reserve) if original_opportunities else np.zeros(len(roster))
            # Removed-player workload remains unresolved; no silent award to the
            # other named backs. Explicit scenario shares can move this mass.
            unallocated = 1-float(probabilities.sum())
            if unallocated < -1e-8:
                raise ValueError('Observed role mass exceeds team totals')
            probabilities[-cfg.surprise_slots:] = max(0,unallocated)/cfg.surprise_slots
            override = request.get('role_scenarios', {}).get(team, {}).get(action)
            if override is not None:
                if not request.get('scenario_evidence'):
                    raise ValueError('Role override requires explicit scenario evidence')
                if set(override) != {p['identity'] for p in roster}:
                    raise ValueError('Role override must specify the full field including residual identities')
                probabilities = np.array([override[p['identity']] for p in roster], float)
            counts = allocate(rng, totals, probabilities, concentration(history, action))
            opponent = teams[1-side]
            yard_metric = 'rushing_yards' if action=='carries' else 'receiving_yards'
            denominator = action if action=='carries' else 'receptions'
            effect = opponent_effect(rows, opponent, game['season'], yard_metric, denominator, cfg.opponent_prior_events)
            catch_effect = opponent_effect(rows, opponent, game['season'], 'receptions','targets',cfg.opponent_prior_events)
            diagnostics[team]['actions'][action] = {'mean_budget':float(mean),'newcomer_reserve':reserve,
                'opponent_volume':volume_effect,
                'concentration_assumption':concentration(history, action),'opponent_yards':effect,
                'opponent_catch':catch_effect,'max_budget_mismatch':int(abs(counts.sum(axis=1)-totals).max()),
                'roles':{p['identity']:float(probabilities[j]) for j,p in enumerate(roster)}}
            league_catch = sum(r['receptions'] for r in rows)/sum(r['targets'] for r in rows)
            shared_catch = np.array([b['receptions']/b['targets'] if b['targets'] else league_catch for b in selected_blocks])-league_catch
            all_events = [e for h in history for e in h['events'] if e['action']==action]
            for j,p in enumerate(roster):
                own_events = [e for e in all_events if e['identity']==p['identity']]
                peer_events = [e for e in all_events if p['position']=='UNKNOWN' or e['position']==p['position']]
                if not peer_events:
                    peer_events = all_events
                if not peer_events:
                    raise ValueError('No empirical efficiency distribution')
                own_weights = np.array([cfg.prior_season_weight**max(0,game['season']-games['game']['season'])
                    for games in history for e in games['events'] if e['action']==action and e['identity']==p['identity']])
                peer_catch = np.mean([e['caught'] for e in peer_events])
                rate = (sum(w*e['caught'] for w,e in zip(own_weights,own_events))+cfg.efficiency_prior_events*peer_catch)/(own_weights.sum()+cfg.efficiency_prior_events)
                receptions = rng.binomial(counts[:,j],np.clip(rate+shared_catch+catch_effect['adjustment'],0,1)) if action=='targets' else counts[:,j]
                valid_own = [(e['yards'],w) for e,w in zip(own_events,own_weights) if action=='carries' or e['caught']]
                peer_yards = [e['yards'] for e in peer_events if action=='carries' or e['caught']]
                if not peer_yards:
                    raise ValueError('No empirical yard outcomes')
                values = np.array([y for y,w in valid_own]+peer_yards)
                event_weights = np.array([w for y,w in valid_own]+[cfg.efficiency_prior_events/len(peer_yards)]*len(peer_yards))
                event_weights /= event_weights.sum()
                yards = np.zeros(cfg.draws)
                # Losses and long gains retained. Opponent shift is applied per
                # realized touch, not to a fantasy projection or leader share.
                shift = float(np.clip(effect['adjustment'],-2.,2.))
                for k in range(int(receptions.max(initial=0))):
                    mask = receptions>k
                    yards[mask] += rng.choice(values,int(mask.sum()),p=event_weights)+shift
                stats[p['identity']][:,0 if action=='carries' else 2] = np.rint(yards)
                if action=='targets':
                    stats[p['identity']][:,1] = receptions
    output = {}
    identities = list(stats)
    for col, metric in enumerate(METRICS):
        matrix = np.array([stats[i][:,col] for i in identities]).T
        wins = matrix==matrix.max(axis=1)[:,None]
        tie_count = wins.sum(axis=1)
        credits = wins/tie_count[:,None]
        groups = defaultdict(list)
        for j,i in enumerate(identities):
            p = candidates[i]
            groups['OTHER:'+p['team'] if p['residual'] else i].append(j)
        rows_out = []
        for key, indices in groups.items():
            residual = key.startswith('OTHER:')
            p = candidates[identities[indices[0]]]
            rows_out.append({'identity':key,'name':key if residual else p['name'],'team':p['team'],
                'availability':p.get('status','unresolved'),
                'residual':residual,'win_share':float(credits[:,indices].sum(axis=1).mean()),
                'first_or_tied':float(wins[:,indices].any(axis=1).mean()),
                'sole_first':float((wins[:,indices].any(axis=1)&(tie_count==1)).mean()),
                'mean':None if residual else float(matrix[:,indices[0]].mean()),
                'p10':None if residual else float(np.quantile(matrix[:,indices[0]],.1)),
                'p90':None if residual else float(np.quantile(matrix[:,indices[0]],.9)),
                'baseline_mean':None if residual else float(np.mean([sum(b[metric] for b in h['boxes'] if b['identity']==key and b['team']==p['team'])
                    for h in history if p['team'] in (h['game']['away'],h['game']['home'])][-cfg.recent_games:]))})
        if abs(sum(r['win_share'] for r in rows_out)-1)>1e-8:
            raise AssertionError('Leader share does not sum to one')
        output[metric]={'players':sorted(rows_out,key=lambda r:-r['win_share']),
            'tie_probability':float(np.mean(tie_count>1)), 'all_zero_probability':float(np.mean(np.all(matrix==0,axis=1)))}
    return {'version':VERSION,'authority':'exploratory_not_calibrated','game':game,'decision_at':request['decision_at'],
        'settings':asdict(cfg),'implementation_sha256':IMPLEMENTATION_SHA256,'training_game_ids':[h['game']['game_id'] for h in history],
        'request_sha256':sha256(json.dumps(request,sort_keys=True).encode()).hexdigest(),
        'market_inputs_used':False,'scope':'full_game_including_overtime','metrics':output,'diagnostics':diagnostics,
        'availability_verified':verified,'availability_evidence':request.get('availability_evidence',{}),
        'limits':['Reconstructed historical usage is not game-day availability.',
            'Role concentration and smoothing are assumptions, not fitted calibration.',
            'Absent/out workloads remain in unresolved slots unless an explicit role scenario is supplied.',
            'Paired game volumes approximate game scripts; no score-by-score simulation.',
            'Newcomers use three separate latent slots; vary this assumption before acting.',
            'No fitted weather, offensive-line injury, quarterback-change or coaching-change coefficients.']}


def grade(prediction, snapshot):
    gid = prediction['game']['game_id']
    games = [g for g in snapshot['games'] if g['game_id']==gid]
    if len(games)!=1 or not games[0].get('completed'):
        return {'status':'outcome_unknown','reason':'Canonical game not final'}
    if not snapshot.get('game_coverage',{}).get(gid,{}).get('regulation_end_observed'):
        return {'status':'outcome_unknown','reason':'Missing terminal PBP evidence'}
    if any(games[0][k]!=prediction['game'][k] for k in ('season','week','home','away','kickoff')):
        raise ValueError('Grading game does not match frozen forecast')
    boxes = [b for b in snapshot['boxes'] if b['game_id']==gid]
    if not boxes or any(not any(b['team']==team for b in boxes) for team in (games[0]['home'],games[0]['away'])):
        return {'status':'outcome_unknown','reason':'Missing full-field box results'}
    if len({b['identity'] for b in boxes}) != len(boxes):
        raise ValueError('Duplicate grading identity')
    result = {}
    for metric in METRICS:
        actual = {b['identity']:float(b[metric]) for b in boxes}
        if not all(np.isfinite(v) for v in actual.values()):
            raise ValueError('Invalid grading stat')
        best = max(actual.values())
        winners = {i for i,v in actual.items() if v==best}
        rows = prediction['metrics'][metric]['players']
        known = {r['identity'] for r in rows if not r['residual']}
        target = defaultdict(float)
        for b in boxes:
            if b['identity'] in winners:
                target[b['identity'] if b['identity'] in known else 'OTHER:'+b['team']] += 1/len(winners)
        probabilities = {r['identity']:r['win_share'] for r in rows}
        if any(not np.isfinite(p) or not 0<=p<=1 for p in probabilities.values()) or abs(sum(probabilities.values())-1)>1e-8:
            raise ValueError('Invalid forecast probability mass')
        if not set(target).issubset(probabilities):
            raise ValueError('Missing unknown winner bucket')
        top = rows[0]['identity']
        baseline = max((r for r in rows if not r['residual']),key=lambda r:r['baseline_mean'])['identity']
        result[metric]={'winner_ids':sorted(winners),'winning_stat':best,'top_choice_credit':target.get(top,0),
            'mean_baseline_credit':target.get(baseline,0),'brier':sum((p-target.get(i,0))**2 for i,p in probabilities.items()),
            'log_loss':-sum(t*np.log(max(probabilities[i],1e-12)) for i,t in target.items()),
            'calibration_rows':[{'probability':p,'observed_credit':target.get(i,0),'residual':i.startswith('OTHER:')} for i,p in probabilities.items()],
            'absolute_mean_errors':{r['identity']:abs(r['mean']-actual.get(r['identity'],0)) for r in rows if not r['residual']}}
    return {'status':'graded','game_id':gid,'metrics':result}
