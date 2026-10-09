import pytest

from ingest.nfl_prop_odds import normalize_prop_payload, require_credit_budget


def test_normalizes_paired_and_yes_only_props_without_erasing_raw_shape() -> None:
    payload = {
        "bookmakers": [
            {
                "key": "draftkings",
                "title": "DraftKings",
                "last_update": "2026-09-20T12:00:00Z",
                "markets": [
                    {
                        "key": "player_reception_yds",
                        "outcomes": [
                            {"name": "Over", "description": "Malik Nabers", "point": 74.5, "price": -115},
                            {"name": "Under", "description": "Malik Nabers", "point": 74.5, "price": -105},
                        ],
                    },
                    {
                        "key": "player_anytime_td",
                        "outcomes": [{"name": "Kyren Williams", "price": 120}],
                    },
                ],
            }
        ]
    }
    rows = normalize_prop_payload(payload)
    keyed = {(row["market"], row["player"]): row for row in rows}
    receiving = keyed[("player_reception_yds", "Malik Nabers")]["books"]["draftkings"]
    touchdown = keyed[("player_anytime_td", "Kyren Williams")]["books"]["draftkings"]
    assert receiving["line"] == 74.5
    assert receiving["over"] == -115 and receiving["under"] == -105
    assert len(receiving["outcomes"]) == 2
    assert touchdown["yes"] == 120
    assert touchdown["outcomes"][0]["name"] == "Kyren Williams"


def test_drops_anonymous_sided_outcome_instead_of_guessing_player() -> None:
    payload = {
        "bookmakers": [
            {
                "key": "draftkings",
                "markets": [
                    {"key": "player_pass_yds", "outcomes": [{"name": "Over", "point": 250.5, "price": -110}]}
                ],
            }
        ]
    }
    assert normalize_prop_payload(payload) == []


def test_paid_capture_requires_budget_and_fails_before_overage() -> None:
    with pytest.raises(ValueError, match="positive"):
        require_credit_budget(estimated_credits=35, max_credits=0)
    with pytest.raises(ValueError, match="exceeds"):
        require_credit_budget(estimated_credits=35, max_credits=34)
    require_credit_budget(estimated_credits=35, max_credits=35)
