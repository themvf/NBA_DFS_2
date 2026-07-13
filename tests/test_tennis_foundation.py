from datetime import date

from ingest.tennis_foundation import (
    ATP_PROVIDER,
    EXACT_QUOTE_INSERT_SQL,
    EXACT_QUOTE_VALUE_TEMPLATE,
    TENNIS_DATA_PROVIDER,
    _market_odds,
    american_to_decimal,
    checksum,
    find_atp_enrichment,
    normalize_name,
    normalize_surface,
    player_identity_key,
    source_match_key,
    surname_initial_key,
)
from ingest.backfill_tennis_exact_quotes import _market_payloads


def test_wta_full_and_abbreviated_names_share_identity_bridge():
    live = player_identity_key("WTA", "Iga Swiatek", "the_odds_api")
    historical = player_identity_key("WTA", "Swiatek I.", TENNIS_DATA_PROVIDER)
    assert live == historical == "swiateki"


def test_atp_name_normalization_handles_accents_and_suffixes():
    assert normalize_name("Martín Damm Jr.") == "martindamm"
    assert player_identity_key("ATP", "Martín Damm Jr.", ATP_PROVIDER) == "martindamm"


def test_surname_initial_bridge_handles_multi_part_surname():
    assert surname_initial_key("Alejandro Davidovich Fokina") == "davidovichfokinaa"
    assert surname_initial_key("Davidovich Fokina A.", abbreviated=True) == "davidovichfokinaa"


def test_surface_normalization_does_not_default_unknown_to_hard():
    assert normalize_surface("Hard") == "hard"
    assert normalize_surface("Hard", "Indoor") == "indoor_hard"
    assert normalize_surface("Clay") == "clay"
    assert normalize_surface("Grass") == "grass"
    assert normalize_surface("") is None
    assert normalize_surface("Carpet") is None


def test_market_odds_uses_documented_source_priority():
    row = {"PSW": 1.8, "PSL": 2.1, "AvgW": 1.7, "AvgL": 2.2}
    assert _market_odds(row) == (1.8, 2.1, "Pinnacle")
    assert _market_odds({"AvgW": 1.7, "AvgL": 2.2}) == (1.7, 2.2, "Average")
    assert _market_odds({}) == (None, None, None)


def test_tml_source_match_key_is_provider_stable():
    row = {"tourney_id": "2025-9900", "match_num": "17"}
    key = source_match_key(
        ATP_PROVIDER, "ATP", row, date(2025, 1, 3),
        "Player One", "Player Two",
    )
    assert key == "2025-9900:17"


def test_atp_enrichment_uses_actual_match_date_within_tournament_window():
    index = {
        ("zvereva", "royerv"): [
            {"__match_date": date(2025, 7, 2), "Tournament": "Wimbledon", "PSW": 1.1, "PSL": 8.0}
        ]
    }
    row = find_atp_enrichment(index, "Alexander Zverev", "Valentin Royer", date(2025, 6, 30), "Wimbledon")
    assert row is not None
    assert row["__match_date"] == date(2025, 7, 2)


def test_row_checksum_is_order_independent_and_nan_safe():
    assert checksum({"b": 2, "a": float("nan")}) == checksum({"a": None, "b": 2})


def test_american_to_decimal():
    assert american_to_decimal(150) == 2.5
    assert american_to_decimal(-200) == 1.5


def test_legacy_quote_backfill_requires_complete_two_sided_markets():
    markets, incomplete_spreads = _market_payloads(
        {
            "ml_home": -120,
            "ml_away": 105,
            "total_line": 22.5,
            "over": -110,
            "under": -110,
            "spread_home": -2.5,
            "spread_price": -105,
        },
        "Player One",
        "Player Two",
    )
    assert [market["key"] for market in markets] == ["h2h", "totals"]
    assert incomplete_spreads == 1


def test_legacy_quote_backfill_preserves_complete_spread_pair():
    markets, incomplete_spreads = _market_payloads(
        {
            "spread_home": -2.5,
            "spread_home_price": -105,
            "spread_away": 2.5,
            "spread_away_price": -115,
        },
        "Player One",
        "Player Two",
    )
    assert markets == [{
        "key": "spreads",
        "outcomes": [
            {"name": "Player One", "point": -2.5, "price": -105},
            {"name": "Player Two", "point": 2.5, "price": -115},
        ],
    }]
    assert incomplete_spreads == 0


def test_exact_quote_insert_has_one_parameter_per_column():
    assert EXACT_QUOTE_INSERT_SQL.count("%s") == 1
    assert EXACT_QUOTE_VALUE_TEMPLATE.count("%s") == 31
