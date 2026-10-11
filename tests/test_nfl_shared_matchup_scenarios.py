from tests.test_nfl_matchup_scenarios import inputs
import pytest
from model.nfl_shared_matchup_scenarios import build_coherent_banks


def test_shared_role_candidate_preserves_event_ledger_and_has_separate_identity():
    kwargs = inputs()
    evidence = {'source_ref':'frozen-prior-only-fit', 'training_decision_at':'2026-09-26T00:00:00Z',
                'actions':{a:{'method':'pooled_team_season_moments','concentration':5} for a in ('carries','targets')}}
    base = build_coherent_banks(**kwargs)
    candidate = build_coherent_banks(**kwargs, role_dispersion_evidence=evidence)
    assert candidate['version'].endswith('-shared-roles')
    assert candidate['selection']['snapshotId'] != base['selection']['snapshotId']
    assert candidate['manifest']['role_dispersion_evidence'] == evidence
    for diagnostic in candidate['diagnostics']:
        assert diagnostic['event_ledgers']
        for games in diagnostic['event_ledgers']:
            first, second = games[0]['teams']
            for own, opposite in ((first, second), (second, first)):
                assert own['passing_yards'] == sum(p.get('receiving_yards', 0) for p in own['players'].values()) + own['unallocated']['receiving_yards']
                assert all(p.get('receptions', 0) >= 0 for p in own['players'].values())
    evidence['training_decision_at'] = '2099-01-01T00:00:00Z'
    with pytest.raises(ValueError, match='boundary'):
        build_coherent_banks(**kwargs, role_dispersion_evidence=evidence)


def test_full_dfs_replacement_requires_complete_field_and_frozen_evidence():
    kwargs = inputs()
    roles = {'AAA': {'targets': {'AAAWR': 1, 'OTHER': 0}}}
    evidence = {'source_ref':'synthetic-replacement-fixture', 'description':'Explicit target-share assumption',
                'captured_at':'2026-09-26T00:00:00Z'}
    result = build_coherent_banks(**kwargs, replacement_roles=roles, replacement_evidence=evidence)
    assert result['manifest']['replacement_roles'] == roles
    assert kwargs['forecasts'][0]['players'][1]['components']['targets']['share'] == .75
    roles['AAA']['targets'].pop('OTHER')
    with pytest.raises(ValueError, match='complete field'):
        build_coherent_banks(**kwargs, replacement_roles=roles, replacement_evidence=evidence)


def test_shared_bank_does_not_backdate_source_capture():
    kwargs = inputs()
    kwargs['source_manifest']['captured_at'] = '2026-10-01T00:00:00Z'
    result = build_coherent_banks(**kwargs)
    assert result['evaluation']['inputsCapturedAt'] == '2026-10-01T00:00:00Z'
    assert result['evaluation']['decisionAt'] == kwargs['decision_at']
