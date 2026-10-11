from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path

from model.nfl_game_leaders import Settings, prepare
from model.nfl_joint_contracts import digest
from model.nfl_joint_decisions import exact_set_probability, exact_sets, summarize
from model.nfl_joint_evaluation import grade_bank, paired_bootstrap, validate_registration
from model.nfl_joint_outcomes import enrich_history, fit, generate
from model.nfl_joint_full_dfs import complete_report
from model.nfl_longest_touchdown import timestamp
from model.nfl_market_reweighting import ReweightSettings, quote_constraints, reweight
from model.nfl_midgame_exits import fit_exits
from model.nfl_opportunity_process import fit_market_volume
from model.nfl_ownership_model import fit_ownership, predict_ownership
from research.nfl_longest_touchdown import read, write
from research.nfl_game_leaders import expected_history
from research.nfl_joint_store import write_bundle


def bank_from(payload):
    return payload.get('shared_draws', payload)


def audit_sources(snapshot):
    games = snapshot['games']
    if len({g['game_id'] for g in games}) != len(games):
        raise ValueError('Duplicate canonical games')
    fields = ('air_yards', 'yards_after_catch', 'yardline_100')
    by_season = {}
    play_groups = {}
    for play in snapshot['plays']:
        play_groups.setdefault(play['game_id'], []).append(play)
    for game in games:
        season = str(game['season'])
        record = by_season.setdefault(season, {'games': 0, 'plays': 0, 'field_nonnull': {f: 0 for f in fields}})
        plays = play_groups.get(game['game_id'], [])
        record['games'] += 1
        record['plays'] += len(plays)
        for f in fields:
            record['field_nonnull'][f] += sum(p.get(f) is not None for p in plays)
    return {'source_sha256': digest(snapshot), 'seasons': by_season,
            'requested_training_seasons_missing': [s for s in range(2020, 2026) if str(s) not in by_season],
            'participant_as_of_available': snapshot.get('participant_as_of_available', False),
            'current_universe_stats_used': False, 'authority': 'coverage_audit_not_reconciliation'}


def evaluate(snapshot, requests, study, population, candidate='role-gains'):
    study_sha = validate_registration(study)
    if candidate not in study['variants'] or 'baseline' not in study['variants']:
        raise ValueError('Variants absent from registration')
    if population not in ('development', 'selection', 'locked'):
        raise ValueError('Unsupported study population')
    if population == 'locked' and study['status'] != 'locked':
        raise ValueError('Locked execution requires frozen locked registration')
    latest = max(timestamp(g['kickoff']) for g in snapshot['games']) + timedelta(days=7)
    history, rejected = prepare(snapshot, latest.isoformat(), retrospective=True)
    labels = {h['game']['game_id']: h for h in history}
    pairs, skipped = [], []
    selected = study[f'{population}_games']
    seed = study['seeds'][0]
    settings = Settings(draws=study['draws'], seed=seed)
    for gid in selected:
        try:
            if gid not in requests or gid not in labels:
                raise ValueError('Missing frozen request or reconciled labels')
            request = requests[gid]
            if study['source_policy'] == 'strict_prospective' and timestamp(study['registered_at']) > timestamp(request['game']['kickoff']) and population == 'locked':
                raise ValueError('Registration after locked target kickoff')
            eligible = [h for h in history if timestamp(h['game']['kickoff']) + timedelta(hours=8) < timestamp(request['decision_at'])]
            if candidate != 'role-gains':
                raise ValueError('Unsupported registered implementation variant')
            eligible = enrich_history(eligible, snapshot)
            baseline = generate(eligible, request, settings)
            fitted = fit(eligible, request['decision_at'])
            challenger = generate(eligible, request, settings, fitted)
            pairs.append({'game_id': gid, 'baseline': grade_bank(baseline['shared_draws'], labels[gid]['boxes']),
                          'candidate': grade_bank(challenger['shared_draws'], labels[gid]['boxes']),
                          'baseline_forecast_sha256': digest(baseline), 'candidate_forecast_sha256': digest(challenger)})
        except ValueError as error:
            skipped.append({'game_id': gid, 'reason': str(error)})
    comparisons = {}
    if pairs:
        for metric in study['metrics']:
            comparable = [r for r in pairs if all(r[variant]['metrics'][metric]['exact_set_log_loss'] is not None for variant in ('baseline', 'candidate'))]
            comparisons[metric] = paired_bootstrap(
                {r['game_id']: r['candidate']['metrics'][metric]['exact_set_log_loss'] for r in comparable},
                {r['game_id']: r['baseline']['metrics'][metric]['exact_set_log_loss'] for r in comparable}, seed=seed) if comparable else {'games': 0, 'status': 'no_resolved_joint_labels'}
    return {'study_sha256': study_sha, 'population': population, 'status': 'opened' if population == 'locked' else 'development_diagnostic',
            'selected_games': len(selected), 'graded_games': len(pairs), 'comparisons': comparisons,
            'pairs': pairs, 'skipped': skipped, 'source_rejections': rejected,
            'authority': 'retrospective_reconstructed_inputs_not_forward_validation',
            'limits': ['Later stat corrections and reconstructed roster inputs.',
                       'Conservative eight-hour completed-game feature boundary.',
                       'Only the registered role-gains comparison is executed; no automatic model promotion.']}


def main():
    parser = argparse.ArgumentParser(description='Immutable joint NFL research, grading and market conditioning')
    sub = parser.add_subparsers(dest='command', required=True)
    for name in ('audit-sources', 'audit-thursday', 'register', 'fit', 'fit-exits', 'fit-volume', 'fit-ownership', 'ownership', 'constraints', 'publish', 'bundle', 'complete-dfs', 'forecast', 'reweight', 'grade', 'evaluate'):
        command = sub.add_parser(name)
        command.add_argument('--input', type=Path, required=True)
        command.add_argument('--output', type=Path, required=True)
        if name in ('fit', 'fit-exits', 'fit-volume', 'fit-ownership', 'ownership', 'constraints'):
            command.add_argument('--decision-at', required=True)
        if name == 'ownership':
            command.add_argument('--fit', type=Path, required=True)
        if name == 'constraints':
            command.add_argument('--metric-map', type=Path, required=True)
        if name == 'bundle':
            command.add_argument('--study-id', required=True)
            command.add_argument('--run-id', required=True)
        if name == 'complete-dfs':
            command.add_argument('--games', type=Path, required=True)
            command.add_argument('--fit', type=Path)
        if name == 'fit':
            command.add_argument('--exit-fit', type=Path)
            command.add_argument('--retrospective', action='store_true')
        if name == 'forecast':
            command.add_argument('--request', type=Path, required=True)
            command.add_argument('--fit', type=Path)
            command.add_argument('--market', type=Path)
            command.add_argument('--volume-fit', type=Path)
            command.add_argument('--draws', type=int, default=5000)
            command.add_argument('--seed', type=int, default=20261009)
            command.add_argument('--retrospective', action='store_true')
        if name == 'reweight':
            command.add_argument('--constraints', type=Path, required=True)
            command.add_argument('--method', choices=('hard', 'soft'), default='soft')
        if name == 'grade':
            command.add_argument('--actuals', type=Path, required=True)
        if name == 'evaluate':
            command.add_argument('--requests', type=Path, required=True)
            command.add_argument('--study', type=Path, required=True)
            command.add_argument('--population', choices=('development', 'selection', 'locked'), default='development')
        if name == 'audit-thursday':
            command.add_argument('--trio', required=True)
            command.add_argument('--metric', default='total_yards')
            command.add_argument('--original', type=Path)
    args = parser.parse_args()
    if args.output.exists():
        raise FileExistsError(args.output)
    source = read(args.input)
    if args.command == 'register':
        result = {'registration': source, 'study_sha256': validate_registration(source), 'registered_at': source['registered_at']}
    elif args.command == 'audit-sources':
        result = audit_sources(source)
    elif args.command == 'fit-exits':
        result = fit_exits(source, args.decision_at)
    elif args.command == 'fit-volume':
        result = fit_market_volume(source, args.decision_at)
    elif args.command == 'fit-ownership':
        result = fit_ownership(source, args.decision_at)
    elif args.command == 'ownership':
        result = predict_ownership(read(args.fit), source, args.decision_at)
    elif args.command == 'constraints':
        result = quote_constraints(source.get('quotes', source) if isinstance(source, dict) else source, read(args.metric_map), args.decision_at)
    elif args.command == 'bundle':
        result = write_bundle(args.output.parent, args.study_id, args.run_id, source)
    elif args.command == 'complete-dfs':
        result = complete_report(source, read(args.games), read(args.fit) if args.fit else None)
    elif args.command == 'publish':
        bank = bank_from(source)
        names = {p['identity']: p['name'] for p in bank['players']}
        sets = {m: exact_sets(bank, m) for m in ('rushing_yards', 'receiving_yards', 'receptions', 'total_yards')}
        for value in sets.values():
            value.pop('full_distribution')
            for row in value['sets']:
                row['names'] = [names[i] for i in row['identities']]
        result = {'schema_version': 1, 'status': 'generated', 'authority': 'exploratory_not_calibrated',
                  'game': bank['game'], 'decision_at': bank['decision_at'], 'branch': bank.get('branch', 'independent'),
                  'draws': len(bank['scenario_ids']), 'implementation_sha256': bank['implementation_sha256'],
                  'source_manifest': bank.get('source_manifest', {}), 'metrics': summarize(bank),
                  'exact_top_three': sets, 'market_reweighting': bank.get('market_reweighting'),
                  'exit_model_enabled': bool(bank.get('candidate_diagnostics') and any(t['exits'] for t in bank['candidate_diagnostics'].values())),
                  'source_sha256': digest(source), 'retrospective': source.get('retrospective', False),
                  'limits': source.get('limits', [])}
    elif args.command in ('fit', 'forecast'):
        request = read(args.request) if args.command == 'forecast' else None
        decision = request['decision_at'] if request else args.decision_at
        if request:
            canonical = [g for g in source['games'] if g['game_id'] == request['game']['game_id']]
            if len(canonical) != 1 or any(canonical[0][k] != request['game'][k] for k in ('season', 'week', 'home', 'away')) or timestamp(canonical[0]['kickoff']) != timestamp(request['game']['kickoff']):
                raise ValueError('Request does not match canonical schedule')
            request = {**request, 'expected_prior_game_ids': expected_history(source, request['game'], decision)}
        history, rejected = prepare(source, decision, retrospective=args.retrospective)
        history = enrich_history(history, source)
        if args.command == 'fit':
            result = fit(history, decision, read(args.exit_fit) if args.exit_fit else None)
        else:
            result = generate(history, request, Settings(draws=args.draws, seed=args.seed),
                              read(args.fit) if args.fit else None, read(args.market) if args.market else None,
                              read(args.volume_fit) if args.volume_fit else None)
        result['source_rejections'] = rejected
        result['retrospective'] = args.retrospective
        result['capture_sha256'] = digest(source)
    elif args.command == 'reweight':
        constraints = read(args.constraints)
        bank = reweight(bank_from(source), constraints.get('constraints', constraints) if isinstance(constraints, dict) else constraints, ReweightSettings(method=args.method))
        result = {'shared_draws': bank, 'metrics': summarize(bank),
                  'exact_top_three': {m: exact_sets(bank, m) for m in ('rushing_yards', 'receiving_yards', 'receptions', 'total_yards')}}
    elif args.command == 'grade':
        result = grade_bank(bank_from(source), read(args.actuals))
    elif args.command == 'evaluate':
        study_payload = read(args.study)
        study = study_payload.get('registration', study_payload)
        if args.population == 'locked':
            study_sha = validate_registration(study)
            write(args.study.parent / f'{study_sha}.opened.json', {'study_sha256': study_sha,
                'opened_at': datetime.now(timezone.utc).isoformat(), 'output': str(args.output)})
        result = evaluate(source, read(args.requests), study, args.population)
    else:
        bank = bank_from(source)
        result = {'forecast_sha256': digest(source), 'metric': args.metric, 'trio': args.trio.split(','),
                  'probability': exact_set_probability(bank, args.metric, args.trio.split(',')),
                  'original_calculation': read(args.original) if args.original else None,
                  'status': 'comparison_only_original_missing' if not args.original else 'comparison_requires_original_reproduction',
                  'authority': 'one_game_sanity_check_not_validation'}
    result['generated_at'] = datetime.now(timezone.utc).isoformat()
    write(args.output, result)


if __name__ == '__main__':
    main()
