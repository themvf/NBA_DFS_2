from model.tennis_book_rules import (
    DK_RULES_URL,
    RULE_VERSION,
    settle_tennis_selection,
    tennis_rule_snapshot,
)


def _settle(**overrides):
    values = {
        "book": "draftkings",
        "market": "moneyline",
        "selection_side": "home",
        "winner_side": "home",
        "completion_status": "completed",
    }
    values.update(overrides)
    return settle_tennis_selection(**values)


def test_completed_moneyline_settles_normally() -> None:
    assert _settle() == "won"
    assert _settle(selection_side="away") == "lost"


def test_draftkings_retirement_uses_advancing_player_rule() -> None:
    assert _settle(completion_status="retired") == "won"
    assert _settle(completion_status="retired", selection_side="away") == "void"


def test_walkover_voids_and_unverified_book_requires_review() -> None:
    assert _settle(completion_status="walkover", winner_side=None) == "void"
    assert _settle(book="fanduel", completion_status="retired") == "manual_review"


def test_draftkings_retired_total_only_grades_when_crossed() -> None:
    args = {
        "market": "total",
        "selection_side": "Games O21.5",
        "completion_status": "retired",
        "home_games": 12,
        "away_games": 10,
        "line": 21.5,
        "total_bet": "Over",
    }
    assert _settle(**args) == "won"
    assert _settle(**{**args, "total_bet": "Under"}) == "lost"
    assert _settle(**{**args, "home_games": 9, "away_games": 8}) == "void"


def test_rule_snapshot_is_versioned_and_sourced() -> None:
    snapshot = tennis_rule_snapshot("draftkings", "moneyline")
    assert snapshot["tennis_rule_version"] == RULE_VERSION
    assert snapshot["tennis_rule_verified"] is True
    assert snapshot["tennis_rule_source"] == DK_RULES_URL
    assert tennis_rule_snapshot("fanduel", "moneyline")["tennis_rule_verified"] is False
