import math
import pytest
from model.nfl_matchup_scenarios import REQUIRED_BLOCK_FIELDS, build_coherent_banks, integer_allocate
from model.nfl_dfs_efficiency import CONFIG, simulate_team


def inputs():
    rows, forecasts, history, players, identities, baseline = [], [], [], [], {}, {}
    for game in range(100):
        for team, other in (("AAA", "BBB"), ("BBB", "AAA")):
            stats = {key: 0 for key in REQUIRED_BLOCK_FIELDS}
            stats.update(attempts=30 + game % 5, carries=20 + game % 3, targets=28, sacks_suffered=game % 4,
                         passing_tds=2, rushing_tds=1, pat_made=3, fg_made_40_49=1, fumbles_lost_total=game % 2)
            rows.append({"game_id": str(game), "team": team, "opponent": other, "season": 2025, "week": 1, "stats": stats})
    for side, team in enumerate(("AAA", "BBB")):
        roster = []
        for offset, position in enumerate(("QB", "WR", "DST")):
            identity = "DST:" + team if position == "DST" else team + position
            player_id = 100 + side * 10 + offset
            identities[identity] = player_id
            players.append({"dkPlayerId": player_id, "position": position})
            baseline[str(player_id)] = 20
            if position == "DST":
                continue
            components = {"attempts": {"share": 1}, "carries": {"share": .1}} if position == "QB" else {"targets": {"share": .75}}
            roster.append({"identity": identity, "position": position, "name": identity, "components": components})
            for week in range(1, 6):
                history.append({"identity": identity, "position": position, "team": team, "season": 2025, "week": week,
                    "stats": {"attempts": 30, "completions": 20, "passing_yards": 245, "passing_tds": 2, "passing_interceptions": 1,
                              "carries": 5, "rushing_yards": 25, "rushing_tds": 0, "targets": 8, "receptions": 6, "receiving_yards": 70,
                              "receiving_tds": 1, "fumbles_lost_total": 0, "special_teams_tds": 0, "fumble_recovery_tds": 0,
                              "passing_2pt_conversions": 0, "receiving_2pt_conversions": 0, "rushing_2pt_conversions": 0}})
        forecasts.append({"game_id": "target", "team": team, "season": 2026, "week": 3,
                          "players": roster, "budgets": {"attempts": {"mean": 33}, "carries": {"mean": 22}, "targets": {"mean": 28}}})
    return dict(slate={"format": "classic", "players": players}, forecasts=forecasts, history=history, team_rows=rows,
                identities=identities, baseline_means=baseline, source_manifest={"fixture": True}, decision_at="2026-09-27T14:00:00Z", draws=100)


def test_integer_allocation_is_exact_and_keeps_unknown():
    assert integer_allocate(11, [1, 1, 1]) == [4, 4, 3]
    assert integer_allocate(11, [0, 0]) == [0, 11]
    with pytest.raises(ValueError):
        integer_allocate(10, [math.nan])


def test_coherent_bank_replay_event_identities_and_changed_marginals():
    result = build_coherent_banks(**inputs())
    replay = build_coherent_banks(**inputs())
    assert result == replay
    assert result["productionChanged"] is False
    assert result["coverage"]["modeledPlayers"] == 6
    assert result["selection"]["seed"] != result["evaluation"]["seed"]
    assert not {s["id"] for s in result["selection"]["scenarios"]} & {s["id"] for s in result["evaluation"]["scenarios"]}
    for draw, games in zip(result["selection"]["scenarios"], result["diagnostics"][0]["event_ledgers"]):
        first, second = games[0]["teams"]
        for own, opp, dst_id in ((first, second, "102"), (second, first, "112")):
            dst = draw["stats"][dst_id]
            assert dst["sacks"] == opp["sacks_suffered"]
            assert dst["dstInterceptions"] == opp["interceptions_thrown"]
            assert dst["fumbleRecoveries"] == opp["fumbles_lost"]
            assert dst["pointsAllowed"] == opp["final_points"] - 6 * opp["defensive_tds"] - 2 * opp["safeties"] - 2 * opp["two_point_returns"]
            assert own["passing_yards"] == sum(p.get("receiving_yards", 0) for p in own["players"].values()) + own["unallocated"]["receiving_yards"]
        for stats in draw["stats"].values():
            assert all(type(value) is int for value in stats.values())
    assert any(row["delta"] != 0 for row in result["diagnostics"][0]["player_marginals"])


def test_missing_empirical_events_do_not_become_zero():
    kwargs = inputs()
    for row in kwargs["team_rows"]:
        del row["stats"]["sacks_suffered"]
    with pytest.raises(ValueError, match="coverage too small"):
        build_coherent_banks(**kwargs)


def test_retaining_draws_does_not_change_existing_team_simulation():
    kwargs = inputs()
    history = kwargs["history"]
    by_player = {key: [r for r in history if r["identity"] == key] for key in ("AAAQB", "AAAWR")}
    by_position = {key: [r for r in history if r["position"] == key] for key in ("QB", "WR")}
    config = {**CONFIG, "draws": 100}
    original, original_meta = simulate_team(kwargs["forecasts"][0], by_player, by_position, config)
    retained, retained_meta = simulate_team(kwargs["forecasts"][0], by_player, by_position, config, retain_draws=True)
    assert original == retained
    retained_meta.pop("retained_draws")
    assert original_meta == retained_meta


def test_conditional_starter_requires_unique_fresh_captured_evidence():
    from model.nfl_matchup_scenarios import condition_starting_qbs
    from copy import deepcopy
    forecasts = inputs()["forecasts"]
    before = deepcopy(forecasts)
    audit = condition_starting_qbs(forecasts, [{"team": "AAA", "identity": "AAAQB", "depth_order": 1, "fetched_at": "2026-09-26T10:00:00+00:00"}], "2026-09-27T14:00:00+00:00")
    assert forecasts == before
    assert audit[0]["state"] == "historical_allocation_unresolved_current_starter"
    audit = condition_starting_qbs(forecasts, [{"team": "AAA", "identity": "AAAQB", "depth_order": 1, "fetched_at": "2026-09-27T10:00:00+00:00"}], "2026-09-27T14:00:00+00:00")
    assert audit[0]["state"] == "conditional_normal_starter"
    assert forecasts[0]["players"][0]["components"]["attempts"]["share"] == 1


def test_showdown_kicker_events_share_scoreboard_and_no_captain_multiplier():
    kwargs = inputs()
    kwargs["slate"]["format"] = "showdown"
    kwargs["slate"]["players"].append({"dkPlayerId": 150, "position": "K", "teamAbbrev": "AAA"})
    kwargs["identities"]["AAAK"] = 150
    kwargs["kicker_roles"] = {"AAA": "AAAK"}
    result = build_coherent_banks(**kwargs)
    for draw, ledger in zip(result["selection"]["scenarios"], result["diagnostics"][0]["event_ledgers"]):
        event = ledger[0]["teams"][0]
        stats = draw["stats"]["150"]
        assert stats["extraPointsMade"] == event["pat_made"]
        assert stats["fgMade40to49"] == event["fg_medium"] == 1
        assert event["kicker_allocation"]["status"] == "conditional_allocated"
        assert ledger[0]["teams"][1]["kicker_allocation"]["status"] == "unallocated_role"
    assert result["coverage"]["modeledPlayers"] == 7


def test_scenario_slate_uses_dk_team_identity_for_rams():
    from research.nfl_coherent_scenario_export import team_code
    assert team_code("LA") == team_code("LAR") == "LAR"
