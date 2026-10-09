from __future__ import annotations

from collections import defaultdict
from copy import deepcopy
from hashlib import sha256
from pathlib import Path

import numpy as np

from model.nfl_gain_distribution import fit_gain_profiles, sample_draws
from model.nfl_game_leaders import Settings, forecast as baseline_forecast
from model.nfl_joint_contracts import clone_bank, digest, validate_bank
from model.nfl_joint_decisions import exact_sets, summarize
from model.nfl_longest_touchdown import timestamp
from model.nfl_midgame_exits import participation
from model.nfl_opportunity_process import allocate_segments, conditioned_volumes
from model.nfl_role_dispersion import estimate_dispersion

VERSION = 'nfl-joint-outcomes-v1'


def implementation_digest():
    paths = sorted(Path(__file__).parent.glob('nfl_joint_*.py')) + [Path(__file__).with_name(name) for name in (
        'nfl_market_reweighting.py', 'nfl_midgame_exits.py', 'nfl_opportunity_process.py', 'nfl_gain_distribution.py',
        'nfl_role_dispersion.py', 'nfl_game_leaders.py', 'nfl_longest_touchdown.py')]
    paths += [Path(__file__).with_name(name) for name in ('nfl_full_exit_allocation.py', 'nfl_shared_dfs_efficiency.py', 'nfl_shared_matchup_scenarios.py')]
    return sha256(b''.join(p.name.encode() + p.read_bytes() for p in paths)).hexdigest()


def enrich_history(history, snapshot):
    plays = defaultdict(list)
    seen = set()
    for play in snapshot['plays']:
        key = (play['game_id'], play['play_id'])
        if key in seen:
            raise ValueError('Duplicate source play')
        seen.add(key)
        action = 'targets' if play['play_type'] == 'pass' else 'carries' if play['play_type'] == 'run' else None
        if action is None or play.get('had_sack'):
            continue
        credit = play.get('stat_credit') or {}
        if credit.get('no_play'):
            continue
        identity = credit.get('receiver_player_id' if action == 'targets' else 'rusher_player_id')
        if identity is None:
            actors = [a for a in play.get('actors', []) if a['role'] == ('receiver' if action == 'targets' else 'rusher')]
            if len(actors) != 1:
                continue
            identity = actors[0]['player_id']
        caught = action == 'targets' and (bool(credit['complete_pass']) if credit.get('complete_pass') is not None else play.get('yards_after_catch') is not None)
        yards = credit.get('receiving_yards' if action == 'targets' else 'rushing_yards')
        if yards is None:
            yards = (play.get('air_yards', 0) + play['yards_after_catch']) if caught and play.get('air_yards') is not None else play.get('yards_gained') if caught or action == 'carries' else 0
        plays[(play['game_id'], identity, action, caught, yards)].append(play)
    result = deepcopy(history)
    for h in result:
        grouped = defaultdict(list)
        for event in h['events']:
            grouped[(h['game']['game_id'], event['identity'], event['action'], event['caught'], event['yards'])].append(event)
        for key, events in grouped.items():
            candidates = plays[key]
            if len(candidates) == len(events):
                for event, play in zip(events, candidates):
                    for field in ('air_yards', 'yards_after_catch', 'yardline_100'):
                        event[field] = play.get(field)
                    event['source_play_id'] = play['play_id']
            else:
                for event in events:
                    event['depth_join_unresolved'] = True
    return result


def fit(history, cutoff, exit_fit=None):
    if not history or any(timestamp(h['game']['kickoff']) >= timestamp(cutoff) for h in history):
        raise ValueError('Training games must precede fit cutoff')
    if len({h['game']['game_id'] for h in history}) != len(history):
        raise ValueError('Duplicate training games')
    if exit_fit and timestamp(exit_fit['training_cutoff']) > timestamp(cutoff):
        raise ValueError('Exit training cutoff exceeds fit boundary')
    excluded = set(exit_fit['injury_game_ids']) if exit_fit else set()
    role_history = [h for h in history if h['game']['game_id'] not in excluded]
    if not role_history:
        raise ValueError('No non-exit role history remains')
    role_counts = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
    teams = {b['team'] for h in role_history for b in h['boxes']}
    for team in teams:
        recent = sorted([h for h in role_history if team in (h['game']['away'], h['game']['home'])], key=lambda h: timestamp(h['game']['kickoff']))[-6:]
        for j, h in enumerate(recent):
            weight = .5 ** ((len(recent) - 1 - j) / 3)
            for box in h['boxes']:
                if box['team'] == team:
                    for action in ('targets', 'carries'):
                        role_counts[team][action][box['identity']] += weight * box[action]
    result = {'version': VERSION, 'training_cutoff': cutoff, 'training_game_ids': [h['game']['game_id'] for h in history],
              'role_excluded_game_ids': sorted(excluded), 'source_sha256': digest(history),
              'roles': {team: {a: dict(ids) for a, ids in actions.items()} for team, actions in role_counts.items()},
              'dispersion': {a: estimate_dispersion(role_history, a) for a in ('targets', 'carries')},
              'gains': fit_gain_profiles(history), 'exit_fit': exit_fit,
              'implementation_sha256': implementation_digest(), 'authority': 'exploratory_not_calibrated'}
    result['fit_sha256'] = digest(result)
    return result


def validate_fit(fitted, decision_at, target_game_ids=(), eligible_game_ids=None):
    fingerprint = {k: v for k, v in fitted.items() if k not in ('fit_sha256', 'source_rejections', 'generated_at', 'retrospective', 'capture_sha256')}
    if digest(fingerprint) != fitted.get('fit_sha256'):
        raise ValueError('Fit artifact digest mismatch')
    if timestamp(fitted['training_cutoff']) > timestamp(decision_at):
        raise ValueError('Fit exceeds decision boundary')
    training = set(fitted['training_game_ids'])
    if training & set(target_game_ids):
        raise ValueError('Target game leaked into fit')
    if eligible_game_ids is not None and training - set(eligible_game_ids):
        raise ValueError('Fit history outside eligible forecast history')
    if fitted.get('implementation_sha256') != implementation_digest():
        raise ValueError('Fit implementation differs from current engine')


def generate(history, request, settings=Settings(), fitted=None, market=None, volume_fit=None):
    baseline = baseline_forecast(history, request, settings, include_draws=True)
    baseline_bank = baseline['shared_draws']
    if market and not volume_fit:
        raise ValueError('Game market conditioning requires historical fitted volume coefficients')
    if volume_fit and not market:
        raise ValueError('Volume fit requires an explicit game market')
    if fitted is None and market:
        raise ValueError('Game market branch requires a fitted candidate')
    bank = clone_bank(baseline_bank, 'independent')
    if fitted is not None:
        validate_fit(fitted, request['decision_at'], [request['game']['game_id']], [h['game']['game_id'] for h in history])
        exit_fit = fitted.get('exit_fit')
        if exit_fit and not fitted['role_excluded_game_ids']:
            raise ValueError('Explicit exits require conditional non-exit role fitting')
        rng = np.random.default_rng(settings.seed)
        volumes = conditioned_volumes(bank, volume_fit, market) if market else {}
        history_by_id = {h['game']['game_id']: h for h in history}
        total_targets = sum(b['targets'] for h in history for b in h['boxes'])
        league_catch = sum(b['receptions'] for h in history for b in h['boxes']) / max(total_targets, 1)
        diagnostics = {}
        for side, team in enumerate((bank['game']['away'], bank['game']['home'])):
            players = [p for p in bank['players'] if p['team'] == team]
            presence, exits = participation(rng, players, settings.draws, exit_fit, request['decision_at']) if exit_fit else (
                np.ones((settings.draws, 1, len(players)), dtype=bool), {})
            team_diagnostic = {'exits': exits, 'actions': {}, 'pooled_exit_roles': [p['identity'] for p in players if exit_fit and p.get('position', 'UNKNOWN') not in exit_fit['roles']]}
            catch_context = np.zeros(settings.draws)
            for draw, (sample, orientation) in enumerate(zip(bank['sampled_game_ids'], bank['sampled_orientation'])):
                historical = history_by_id[sample]
                sampled_team = (historical['game']['away'], historical['game']['home'])[(side + orientation) % 2]
                boxes = [b for b in historical['boxes'] if b['team'] == sampled_team]
                targets = sum(b['targets'] for b in boxes)
                catch_context[draw] = sum(b['receptions'] for b in boxes) / targets - league_catch if targets else 0
            for action in ('targets', 'carries'):
                totals = volumes.get((team, action), np.sum([p['draws'][action] for p in players], axis=0))
                if exit_fit:
                    counts = fitted['roles'].get(team, {}).get(action, {})
                    raw = np.array([counts.get(p['identity'], 0.) for p in players])
                    unknown = max(0., sum(counts.values()) - raw.sum())
                    residual = [j for j, p in enumerate(players) if p.get('residual')]
                    if not residual or raw.sum() + unknown <= 0:
                        raise ValueError('Complete non-exit role evidence required')
                    raw[residual] += unknown / len(residual)
                    shares = raw / raw.sum()
                else:
                    shares = np.array([baseline['diagnostics'][team]['actions'][action]['roles'][p['identity']] for p in players])
                concentration = fitted['dispersion'][action]['concentration']
                segment_counts = allocate_segments(rng, players, totals, shares, concentration, presence, exit_fit, action)
                assigned = segment_counts.sum(axis=1)
                adjustment = baseline['diagnostics'][team]['actions'][action]
                for j, player in enumerate(players):
                    caught, yards = sample_draws(rng, fitted['gains'], player, action, assigned[:, j],
                        catch_context + adjustment['opponent_catch']['adjustment'] if action == 'targets' else 0.,
                        float(np.clip(adjustment['opponent_yards']['adjustment'], -2., 2.)))
                    field = 'recYds' if action == 'targets' else 'rushYds'
                    lateral_events = [e['yards'] for h in history for e in h['events'] if e['identity'] == player['identity'] and e['action'] == ('lateral_receiving' if action == 'targets' else 'lateral_rushing')]
                    lateral_only = np.zeros(settings.draws, dtype=int)
                    if lateral_events:
                        exposure = sum(sum(b[action] for b in h['boxes'] if b['team'] == team) for h in history if team in (h['game']['home'], h['game']['away']))
                        rate = min(1., len(lateral_events) / max(exposure, 1))
                        instances = rng.binomial(totals, rate * presence[:, :, j].mean(axis=1))
                        owners = np.repeat(np.arange(settings.draws), instances)
                        lateral_only = np.rint(np.bincount(owners, weights=rng.choice(lateral_events, size=len(owners)), minlength=settings.draws)).astype(int)
                    player['draws'][action] = assigned[:, j].tolist()
                    player['draws'][field] = (yards + lateral_only).tolist()
                    if action == 'targets':
                        player['draws']['receptions'] = caught.tolist()
                team_diagnostic['actions'][action] = {'max_budget_error': int(np.abs(assigned.sum(axis=1) - totals).max()),
                    'concentration': concentration, 'segments': presence.shape[1],
                    'opportunity_segments_sha256': digest(segment_counts.tolist())}
            diagnostics[team] = team_diagnostic
        bank['candidate_diagnostics'] = diagnostics
        bank['fit_sha256'] = fitted['fit_sha256']
        if market:
            bank['branch'] = 'game-market-conditioned'
            bank['game_market'] = market
            bank['volume_fit_sha256'] = digest(volume_fit)
    bank['weights'] = validate_bank(bank).tolist()
    bank['implementation_sha256'] = implementation_digest()
    bank['authority'] = 'exploratory_not_calibrated'
    bank['source_manifest'] = {'training_game_ids': baseline['training_game_ids'], 'request_sha256': baseline['request_sha256'],
                              'history_coverage': baseline['history_coverage'], 'availability_verified': baseline['availability_verified']}
    return {'version': VERSION, 'authority': bank['authority'], 'game': bank['game'], 'decision_at': bank['decision_at'],
            'branch': bank['branch'], 'shared_draws': bank, 'metrics': summarize(bank),
            'exact_top_three': {m: exact_sets(bank, m) for m in ('rushing_yards', 'receiving_yards', 'receptions', 'total_yards')},
            'limits': baseline['limits'] + ([] if fitted is None else fitted['gains']['limits'])}
