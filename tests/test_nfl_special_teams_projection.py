"""The candidate must use exact scoring and only pre-cutoff context."""

import pytest
from datetime import datetime, timezone

from ingest.nfl_dfs_results import _dk_points_allowed
from model.nfl_dfs_historical import HistoricalWeek, ProjectionContext, draftkings_points, project_player
from model.nfl_special_teams_projection import (
    SpecialTeamsContext,
    _dst_allowed_points,
    conditioned_score,
    opponent_event_factors,
    project_special_teams,
)


def dst_week(number: int, *, opponent: str = "MIA", sacks: int = 2,
             turnovers: int = 1, allowed: int = 21, season: int = 2025) -> HistoricalWeek:
    pa = _dk_points_allowed(allowed)
    points = sacks + 2 * turnovers + pa
    return HistoricalWeek(
        player_id=10, player_gsis_id=None, player_name="Buffalo DST", position="DST",
        season=season, week=number, team="BUF", opponent=opponent,
        stats={"fantasy_points": points, "scoring_components": {
            "sacks": sacks, "interceptions": turnovers, "fumble_recoveries": 0,
            "dk_points_allowed": allowed, "points_allowed_fpts": pa,
        }},
    )


def kicker_week(number: int, *, pat: int = 2, short_fg: int = 1,
                long_fg: int = 0) -> HistoricalWeek:
    stats = {"pat_made": pat, "fg_made_0_19": 0, "fg_made_20_29": 0,
             "fg_made_30_39": short_fg, "fg_made_40_49": 0,
             "fg_made_50_59": long_fg, "fg_made_60_": 0}
    return HistoricalWeek(
        player_id=20, player_gsis_id="kicker-20", player_name="Kicker", position="K",
        season=2025, week=number, team="BUF", opponent="MIA", stats=stats,
    )


@pytest.mark.parametrize("allowed", [0, 1, 6, 7, 13, 14, 20, 21, 27, 28, 34, 35, 50])
def test_dst_points_allowed_tiers_match_exact_dk_scorer(allowed: int) -> None:
    assert _dst_allowed_points(allowed) == _dk_points_allowed(allowed)


def test_conditioning_preserves_exact_scoring_at_league_total() -> None:
    defense = dst_week(1)
    kicker = kicker_week(1, pat=2, short_fg=1, long_fg=1)
    assert conditioned_score(defense, SpecialTeamsContext(opponent_implied_total=22.5)) == defense.dk_points
    assert conditioned_score(kicker, SpecialTeamsContext(team_implied_total=22.5)) == draftkings_points("K", kicker.stats)


@pytest.mark.parametrize("position", ["DST", "K"])
def test_neutral_context_uses_the_historical_distribution_scale(position: str) -> None:
    rows = [dst_week(n) if position == "DST" else kicker_week(n) for n in range(1, 5)]
    args = dict(position=position, player_id=rows[0].player_id,
                player_gsis_id=rows[0].player_gsis_id, player_name=rows[0].player_name,
                historical_rows=rows, cutoff_season=2026, cutoff_week=1, seed=7)
    baseline = project_player(**args, context=ProjectionContext(team_implied_total=22.5))
    candidate = project_special_teams(**args, context=SpecialTeamsContext(
        team_implied_total=22.5, opponent_implied_total=22.5, opponent_team="NYJ"))
    assert (candidate.mean, candidate.p10, candidate.p50, candidate.p90, candidate.boom) == (
        baseline.model_proj_fpts, baseline.floor_fpts, baseline.median_fpts,
        baseline.ceiling_fpts, baseline.boom_rate,
    )


def test_dst_projection_responds_to_opponent_total_on_same_history() -> None:
    rows = [dst_week(n, allowed=21 + n % 3) for n in range(1, 7)]
    args = dict(position="DST", player_id=10, player_gsis_id=None,
                player_name="Buffalo DST", historical_rows=rows,
                cutoff_season=2026, cutoff_week=1, seed=7)
    easy = project_special_teams(**args, context=SpecialTeamsContext(
        opponent_implied_total=16, opponent_team="MIA"))
    hard = project_special_teams(**args, context=SpecialTeamsContext(
        opponent_implied_total=30, opponent_team="MIA"))
    assert easy.status == hard.status == "candidate"
    assert easy.mean > hard.mean
    assert easy.p10 <= easy.p50 <= easy.p90
    assert hard.p10 <= hard.p50 <= hard.p90
    assert easy.as_dict() == project_special_teams(**args, context=SpecialTeamsContext(
        opponent_implied_total=16, opponent_team="MIA")).as_dict()


def test_kicker_projection_responds_to_team_total() -> None:
    rows = [kicker_week(n, pat=1 + n % 3, short_fg=n % 2) for n in range(1, 7)]
    args = dict(position="K", player_id=20, player_gsis_id="kicker-20",
                player_name="Kicker", historical_rows=rows,
                cutoff_season=2026, cutoff_week=1, seed=7)
    low = project_special_teams(**args, context=SpecialTeamsContext(team_implied_total=17))
    high = project_special_teams(**args, context=SpecialTeamsContext(team_implied_total=29))
    assert low.status == high.status == "candidate"
    assert high.mean > low.mean
    assert high.p90 >= high.p50 >= high.p10


def test_opponent_events_are_shrunk_and_future_games_excluded() -> None:
    prior = [dst_week(n, opponent="SOFT", sacks=5, turnovers=3) for n in range(1, 5)]
    prior += [dst_week(n + 4, opponent="HARD", sacks=0, turnovers=0) for n in range(1, 5)]
    sack, takeaway, games = opponent_event_factors(prior, "SOFT")
    assert games == 4
    assert 1 < sack <= 1.25
    assert 1 < takeaway <= 1.25
    target = project_special_teams(
        position="DST", player_id=10, player_gsis_id=None, player_name="Buffalo DST",
        historical_rows=[*prior, dst_week(1, season=2026, opponent="SOFT", sacks=100)],
        cutoff_season=2026, cutoff_week=1,
        context=SpecialTeamsContext(opponent_implied_total=22.5, opponent_team="SOFT"),
    )
    assert target.feature_snapshot["opponent_history_games"] == 4
    assert target.feature_snapshot["opponent_sack_factor"] == round(sack, 6)


def test_missing_context_or_exact_components_never_becomes_zero() -> None:
    row = dst_week(1)
    args = dict(position="DST", player_id=10, player_gsis_id=None,
                player_name="Buffalo DST", historical_rows=[row],
                cutoff_season=2026, cutoff_week=1)
    assert project_special_teams(**args, context=SpecialTeamsContext()).status == "unavailable"
    missing = HistoricalWeek(**{**row.__dict__, "stats": {"fantasy_points": 9}})
    result = project_special_teams(**{**args, "historical_rows": [missing]},
                                   context=SpecialTeamsContext(opponent_implied_total=22, opponent_team="MIA"))
    assert result.status == "unavailable"
    assert result.mean is None
    assert "components" in result.feature_snapshot["reason"]


def test_weekly_build_freezes_both_candidates_without_changing_live_baseline(monkeypatch) -> None:
    import ingest.nfl_dfs_projections as ingest
    from model.nfl_dfs_historical import project_player

    history = [dst_week(n) for n in range(1, 5)] + [kicker_week(n) for n in range(1, 5)]
    kickoff = datetime(2026, 9, 20, 17, tzinfo=timezone.utc)
    environment = {
        "BUF": {"opponent": "MIA", "team_implied_total": 27.0,
                "game_id": "game", "event_id": None, "commence_time": kickoff},
        "MIA": {"opponent": "BUF", "team_implied_total": 18.0,
                "game_id": "game", "event_id": None, "commence_time": kickoff},
    }
    players = [
        {"id": 10, "gsis_id": None, "canonical_name": "Buffalo DST", "normalized_name": "buffalo dst",
         "position": "DST", "team_abbrev": "BUF", "depth_order": None, "roster_fetched_at": None},
        {"id": 20, "gsis_id": "kicker-20", "canonical_name": "Kicker", "normalized_name": "kicker",
         "position": "K", "team_abbrev": "BUF", "depth_order": None, "roster_fetched_at": None},
    ]
    monkeypatch.setattr(ingest, "_history", lambda *_: history)
    monkeypatch.setattr(ingest, "_slate_environment", lambda *_: environment)
    monkeypatch.setattr(ingest, "_players", lambda *_: players)

    class EmptyDB:
        def execute(self, *_args, **_kwargs):
            return []

    built, manifest = ingest.build_week(
        EmptyDB(), season=2026, week=1,
        as_of_at=datetime(2026, 9, 15, tzinfo=timezone.utc), seed=7,
        config={"matchup_shadow_enabled": False},
    )
    assert manifest["special_teams_candidate_coverage"]["DST"]["available"] == 1
    assert manifest["special_teams_candidate_coverage"]["K"]["available"] == 1
    for row in built:
        candidate = row["feature_snapshot"]["special_teams_candidate"]
        assert candidate["status"] == "candidate"
        assert candidate["feature_snapshot"]["authority"] == "candidate_only"
        baseline = project_player(
            position=row["position"], player_id=row["player_id"],
            player_gsis_id=row["player_gsis_id"], player_name=row["player_name"],
            historical_rows=history, cutoff_season=2026, cutoff_week=1, seed=7,
        )
        assert row["model_proj_fpts"] == baseline.model_proj_fpts
