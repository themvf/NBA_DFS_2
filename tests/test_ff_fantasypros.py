from ingest.ff_fantasypros import (
    build_model_projection,
    create_indicators,
    normalize_name,
    position_rank,
    projection_stats,
)


class CaptureDatabase:
    def __init__(self) -> None:
        self.params: list[tuple] = []

    def execute(self, _sql: str, params: tuple) -> None:
        self.params.append(params)


def test_normalize_name_handles_suffix_and_accents() -> None:
    assert normalize_name("Marvin Harrison Jr.") == "marvinharrison"
    assert normalize_name("José Núñez III") == "josenunez"


def test_projection_stats_accepts_object_and_list_shapes() -> None:
    assert projection_stats({"points": "100.5", "bad": "x"}) == {"points": 100.5}
    assert projection_stats([{"points": 100}, {"rec_rec": "45.5"}]) == {
        "points": 100.0,
        "rec_rec": 45.5,
    }


def test_position_rank_extracts_numeric_suffix() -> None:
    assert position_rank("WR12") == 12
    assert position_rank(None) is None


def test_model_projection_blends_available_history() -> None:
    result = build_model_projection(
        250,
        {"games": 10, "fantasy_points_std": 130, "fantasy_points_ppr": 180},
        scoring="PPR",
        rookie=False,
        injured=False,
    )
    # History is translated from 18 PPG to 15 expected active games = 270.
    assert result.points == 258.0
    assert result.expected_games == 15.0
    assert result.explanation["fantasypros_weight"] == 0.6


def test_rookie_uses_market_prior_with_wider_uncertainty() -> None:
    result = build_model_projection(210, None, scoring="HALF", rookie=True, injured=False)
    assert result.points == 210
    assert result.expected_games == 15.2
    assert result.confidence == 0.56


def test_injury_reduces_expected_games_and_confidence() -> None:
    result = build_model_projection(200, None, scoring="STD", rookie=False, injured=True)
    assert result.expected_games == 13.5
    assert result.confidence == 0.48


def test_indicators_detect_new_team_from_independent_team_key() -> None:
    db = CaptureDatabase()
    create_indicators(
        db,
        ranking_set_id=15,
        season=2026,
        rows=[{
            "player_id": 277,
            "position": "WR",
            "team": "NE",
            "our_rank": 21,
            "overall_rank": None,
            "rookie": False,
            "injury_status": None,
            "adp": 16.9,
            "confidence": 0.8,
        }],
        history={277: {"prior_team": "PHI"}},
    )
    new_team = next(params for params in db.params if params[2] == "NEW_TEAM")
    assert new_team[4] == "NEW TEAM: PHI → NE"
    assert new_team[8].adapted == {"from": "PHI", "to": "NE"}
