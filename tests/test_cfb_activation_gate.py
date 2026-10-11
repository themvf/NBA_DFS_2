from research.cfb_activation_gate import assess_activation


def test_activation_stays_denied_until_every_independent_gate_passes():
    result = assess_activation(confirmation_results={"confirmation_1": "pass"},
                               provider_modes={"odds": "unknown"}, current_permission="decision-denied",
                               raw_evidence_isolated=False, reviewed_acceptance_record=False)
    assert result["eligible_for_activation_proposal"] is False
    assert result["activation_performed"] is False
    assert "confirmation_2_not_passed" in result["reason_codes"]


def test_all_gates_only_allow_a_proposal_not_activation():
    result = assess_activation(confirmation_results={"confirmation_1": "pass", "confirmation_2": "pass"},
                               provider_modes={"odds": "normalized_only"}, current_permission="shadow-predictive",
                               raw_evidence_isolated=True, reviewed_acceptance_record=True)
    assert result["eligible_for_activation_proposal"] is True
    assert result["activation_performed"] is False
    assert result["required_action"] == "separate reviewed consumer-specific proposal"
