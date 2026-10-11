from __future__ import annotations

from copy import deepcopy

from model.nfl_joint_contracts import digest, require_evidence, validate_bank
from model.nfl_joint_decisions import exact_sets, summarize
from model.nfl_joint_outcomes import validate_fit
from model.nfl_longest_touchdown import timestamp
from model.nfl_market_reweighting import reweight
from model.nfl_shared_matchup_scenarios import build_coherent_banks


def complete_report(inputs, games, role_fit=None):
    require_evidence({'source_ref': digest(inputs['source_manifest']), 'captured_at': inputs['source_manifest'].get('captured_at')}, inputs['decision_at'])
    decision_year = timestamp(inputs['decision_at']).year
    if any(int(row['season']) > decision_year for row in inputs['history'] + inputs['team_rows']):
        raise ValueError('Complete history contains future-season records')
    evidence = None
    if role_fit:
        validate_fit(role_fit, inputs['decision_at'], games)
        evidence = {'source_ref': role_fit['fit_sha256'], 'training_decision_at': role_fit['training_cutoff'], 'actions': role_fit['dispersion']}
    complete = build_coherent_banks(**inputs, role_dispersion_evidence=evidence, joint_exit_fit=role_fit if role_fit and role_fit.get('exit_fit') else None,
                                    joint_gain_profiles=role_fit.get('gains') if role_fit else None)
    output = {'version': 'nfl-joint-complete-dfs-v1', 'complete_dfs': complete, 'games': {}, 'authority': 'exploratory',
              'limits': ['Complete event engine has distinct fitted marginals from the partial role/gain model.',
                         'Unallocated event pools are not fictional players; leader scope is the named modeled field.',
                         'Full DFS catch counts follow the coherent QB/receiver ledger; completed gain profiles are shared with the partial engine.',
                         'Unknown backup passing roles retain unallocated attempts; no invented backup stat line.']}
    for game_id, game in games.items():
        if game['game_id'] != game_id or timestamp(inputs['decision_at']) >= timestamp(game['kickoff']):
            raise ValueError('Invalid canonical complete-game boundary')
        forecasts = [f for f in inputs['forecasts'] if str(f['game_id']) == game_id]
        if {f['team'] for f in forecasts} != {game['home'], game['away']}:
            raise ValueError('Canonical game does not match complete forecasts')
        roster = {p['identity']: {**p, 'team': f['team']} for f in forecasts for p in f['players'] if p['position'] not in ('K', 'DST')}
        for stream_index, stream in enumerate(('selection', 'evaluation')):
            full_bank = complete[stream]
            players = [{k: p.get(k, p['identity'] if k == 'name' else False) for k in ('identity', 'name', 'team', 'residual')}
                       for p in roster.values()]
            for p in players:
                p['draws'] = {k: [] for k in ('rushYds', 'recYds', 'receptions', 'targets', 'carries')}
            for ledger in complete['diagnostics'][stream_index]['event_ledgers']:
                matches = [g for g in ledger if g['game_id'] == game_id]
                if len(matches) != 1:
                    raise ValueError('Missing or duplicated complete event game')
                teams = {t['team']: t for t in matches[0]['teams']}
                for p in players:
                    event = teams[p['team']]
                    stats = event['players'].get(p['identity'])
                    counts = event['participant_opportunities'].get(p['identity'])
                    if stats is None or counts is None:
                        raise ValueError('Incomplete individual event coverage')
                    for field, value in {'rushYds': stats.get('rushing_yards', 0), 'recYds': stats.get('receiving_yards', 0),
                                         'receptions': stats.get('receptions', 0), 'targets': counts.get('targets', 0),
                                         'carries': counts.get('carries', 0)}.items():
                        p['draws'][field].append(value)
            production = {'schema_version': 1, 'scope': 'partial_offense_not_full_dfs', 'game': game,
                'decision_at': inputs['decision_at'], 'implementation_sha256': digest(complete['manifest']['implementation_hashes']),
                'scenario_ids': [s['id'] for s in full_bank['scenarios']], 'weights': [s['weight'] for s in full_bank['scenarios']],
                'players': players, 'modeled_fields': ['rushYds', 'recYds', 'receptions', 'targets', 'carries'],
                'missing_fields': ['passing_and_scoring_fields_available_only_in_complete_companion_bank'],
                'field_scope': 'modeled_named_individuals_incomplete_game_field', 'branch': 'complete-event-production-view',
                'source_manifest': inputs['source_manifest'], 'companion_full_bank_sha256': digest(full_bank)}
            validate_bank(production)
            output['games'].setdefault(game_id, {})[stream] = {'shared_draws': production, 'metrics': summarize(production),
                'exact_top_three': {m: exact_sets(production, m) for m in ('rushing_yards', 'receiving_yards', 'receptions', 'total_yards')}}
    return output


def reweight_complete(full_bank, production, constraints, settings):
    if [s['id'] for s in full_bank['scenarios']] != production['scenario_ids']:
        raise ValueError('Complete and production scenario identities differ')
    adjusted = reweight(production, constraints, settings)
    result = deepcopy(full_bank)
    result['sampling'] = 'weighted'
    for scenario, weight in zip(result['scenarios'], adjusted['weights']):
        if weight <= 0:
            raise ValueError('Complete scorer requires positive scenario support')
        scenario['weight'] = weight
    return {'full_bank': result, 'production': adjusted}
