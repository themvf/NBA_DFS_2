from ingest.ff_fantasypros import (
    audit_fantasypros_endpoints,
    build_model_projection,
    count_fantasypros_payload_matches,
    create_indicators,
    fantasypros_endpoint_contracts,
    link_fantasypros_players,
    normalize_name,
    persist_fantasypros_projections,
    position_rank,
    projection_stats,
)


class CaptureDatabase:
    def __init__(self) -> None:
        self.params: list[tuple] = []

    def execute(self, _sql: str, params: tuple) -> None:
        self.params.append(params)


class AuditClient:
    def __init__(self, *, ranking_rows: int = 150, fail_injuries: bool = False) -> None:
        self.ranking_rows = ranking_rows
        self.fail_injuries = fail_injuries

    def get(self, path: str, params: dict | None = None) -> dict:
        import requests

        params = params or {}
        if path == "nfl/injuries":
            if self.fail_injuries:
                response = requests.Response()
                response.status_code = 403
                raise requests.HTTPError(response=response)
            return {"injuries": [], "last_updated_ts": 1_700_000_000}
        if path == "nfl/players":
            return {"players": [{"player_id": index} for index in range(250)]}
        if path.endswith("/projections"):
            return {"players": [{"fpid": index} for index in range(220)]}
        assert path.endswith("/consensus-rankings")
        return {
            "players": [{"player_id": index} for index in range(self.ranking_rows)],
            "scoring": params.get("scoring"),
            "type": params.get("type"),
        }


class IdentityDatabase:
    def __init__(self, players: list[dict]) -> None:
        self.players = players
        self.updates: list[tuple] = []

    def execute(self, sql: str, params: tuple) -> list[dict]:
        if sql.lstrip().startswith("SELECT"):
            return self.players
        self.updates.append(params)
        return []


def test_normalize_name_handles_suffix_and_accents() -> None:
    assert normalize_name("Marvin Harrison Jr.") == "marvinharrison"
    assert normalize_name("José Núñez III") == "josenunez"


def test_projection_stats_accepts_object_and_list_shapes() -> None:
    assert projection_stats({"points": "100.5", "bad": "x"}) == {"points": 100.5}
    assert projection_stats([{"points": 100}, {"rec_rec": "45.5"}]) == {
        "points": 100.0,
        "rec_rec": 45.5,
    }


def test_endpoint_contracts_cover_separate_ecr_and_adp_datasets() -> None:
    contracts = fantasypros_endpoint_contracts(2026)
    assert len(contracts) == 9
    assert {contract.dataset for contract in contracts} == {
        "players",
        "projections",
        "injuries",
        "draft-rankings-std",
        "draft-rankings-half",
        "draft-rankings-ppr",
        "adp-std",
        "adp-half",
        "adp-ppr",
    }


def test_endpoint_audit_passes_full_entitlement_without_raw_player_data() -> None:
    report = audit_fantasypros_endpoints(AuditClient(), 2026)  # type: ignore[arg-type]
    assert report["all_required_contracts_pass"] is True
    assert report["passed_required_contracts"] == 8
    assert all(result["response_hash"] for result in report["contracts"])
    assert "players" not in report["contracts"][0]


def test_endpoint_audit_rejects_sample_sized_ranking_payloads() -> None:
    report = audit_fantasypros_endpoints(AuditClient(ranking_rows=10), 2026)  # type: ignore[arg-type]
    assert report["all_required_contracts_pass"] is False
    partial = [result for result in report["contracts"] if result["status"] == "partial"]
    assert len(partial) == 6
    assert all(result["row_count"] == 10 for result in partial)


def test_optional_injury_entitlement_does_not_fail_required_contracts() -> None:
    report = audit_fantasypros_endpoints(AuditClient(fail_injuries=True), 2026)  # type: ignore[arg-type]
    injury = next(result for result in report["contracts"] if result["dataset"] == "injuries")
    assert injury["status"] == "unavailable"
    assert injury["http_status"] == 403
    assert report["all_required_contracts_pass"] is True


def test_fantasypros_identity_links_existing_independent_player_without_inserting() -> None:
    db = IdentityDatabase([{
        "id": 42,
        "normalized_name": "lamarjackson",
        "position": "QB",
        "team_abbrev": "BAL",
        "fantasypros_player_id": None,
    }])
    result = link_fantasypros_players(db, 2026, {"players": [{
        "player_id": 1001,
        "player_name": "Lamar Jackson",
        "position_id": "QB",
        "team_id": "BAL",
    }]})  # type: ignore[arg-type]
    assert result == {
        "source_rows": 1,
        "matched": 1,
        "linked": 1,
        "unmatched": 0,
        "ambiguous": 0,
        "unsupported": 0,
    }
    assert db.updates == [(1001, 42)]


def test_fantasypros_identity_refuses_ambiguous_name_position_match() -> None:
    db = IdentityDatabase([
        {"id": 1, "normalized_name": "johnsmith", "position": "WR", "team_abbrev": None, "fantasypros_player_id": None},
        {"id": 2, "normalized_name": "johnsmith", "position": "WR", "team_abbrev": None, "fantasypros_player_id": None},
    ])
    result = link_fantasypros_players(db, 2026, {"players": [{
        "player_id": 2002,
        "player_name": "John Smith",
        "position_id": "WR",
    }]})  # type: ignore[arg-type]
    assert result["matched"] == 0
    assert result["ambiguous"] == 1
    assert db.updates == []


def test_injury_payload_can_match_canonical_identity_when_vendor_id_is_absent_from_directory() -> None:
    db = IdentityDatabase([{
        "id": 42,
        "normalized_name": "lamarjackson",
        "position": "QB",
        "team_abbrev": "BAL",
        "fantasypros_player_id": None,
    }])
    matched = count_fantasypros_payload_matches(db, 2026, [{
        "player_id": 987654,
        "name": "Lamar Jackson",
        "position_id": "QB",
        "team_id": "BAL",
    }])  # type: ignore[arg-type]
    assert matched == 1


def test_fantasypros_projections_persist_separately_for_each_scoring_format() -> None:
    db = IdentityDatabase([{
        "id": 42,
        "normalized_name": "lamarjackson",
        "position": "QB",
        "team_abbrev": "BAL",
        "fantasypros_player_id": 1001,
    }])
    result = persist_fantasypros_projections(db, 2026, 115, {"players": [{
        "fpid": 1001,
        "player_name": "Lamar Jackson",
        "position_id": "QB",
        "team_id": "BAL",
        "stats": {"points": 310.0, "points_half": 315.5, "points_ppr": 321.0},
    }]})  # type: ignore[arg-type]
    assert result == {
        "matched_players": 1,
        "unmatched_players": 0,
        "values_written": 3,
        "std_scores": 1,
        "half_scores": 1,
        "ppr_scores": 1,
    }
    assert [params[3] for params in db.updates] == ["STD", "HALF", "PPR"]
    assert [params[4] for params in db.updates] == [310.0, 315.5, 321.0]


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


def test_indicators_add_scoring_aware_top_three_position_points() -> None:
    db = CaptureDatabase()
    rows = [
        {"player_id": player_id, "position": "QB", "team": team, "our_rank": player_id,
         "overall_rank": None, "rookie": False, "injury_status": None, "adp": None, "confidence": 0.8}
        for player_id, team in ((1, "BUF"), (2, "NE"), (3, "LAR"), (4, "DAL"))
    ]
    history = {
        1: {"games": 17, "fantasy_points_std": 360.0, "fantasy_points_ppr": 360.0},
        2: {"games": 17, "fantasy_points_std": 355.0, "fantasy_points_ppr": 355.0},
        3: {"games": 17, "fantasy_points_std": 350.0, "fantasy_points_ppr": 350.0},
        4: {"games": 17, "fantasy_points_std": 345.0, "fantasy_points_ppr": 345.0},
    }
    create_indicators(db, 15, 2026, rows, history, "PPR")
    leaders = [params for params in db.params if params[2] == "TOP_3_POSITION_POINTS"]
    assert [params[1] for params in leaders] == [1, 2, 3]
    assert leaders[2][4] == "2025 QB FPTS #3"
    assert leaders[2][5] == 350.0
