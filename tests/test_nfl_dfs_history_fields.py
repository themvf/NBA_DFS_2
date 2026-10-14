"""Skill-position projection paths read only OFFENSE_FIELDS from a stat line.

build_week fetches QB/RB/WR/TE history restricted to OFFENSE_FIELDS: the full
nflverse line is ~115 fields and fetching it cost ~47 MB of transfer per run
(2026-10-11, measured on week 5). A path that read any other key would get it
as missing and score it as zero, so every reader of that history is checked
here against the restriction.
"""
from collections.abc import Mapping

from model.nfl_dfs_availability import last_game_passing_leaders
from model.nfl_dfs_historical import (
    MODEL_CONFIG,
    OFFENSE_FIELDS,
    OPPONENT_MODES,
    SKILL_POSITIONS,
    HistoricalWeek,
    ProjectionContext,
    opponent_factors,
    project_player,
)
from model.nfl_matchup_projection import sample_baseline_draws

UNUSED_SOURCE_FIELDS = {"targets", "air_yards", "target_share", "player_display_name", "season_type"}
CONFIG_OVERRIDES = {"draws": 50}


class RecordingStats(Mapping):
    def __init__(self, data, seen):
        self._data = data
        self._seen = seen

    def __getitem__(self, key):
        self._seen.add(key)
        return self._data[key]

    def get(self, key, default=None):
        self._seen.add(key)
        return self._data.get(key, default)

    def __iter__(self):
        raise AssertionError("a skill-position path read the whole stat line")

    def __len__(self):
        return len(self._data)


def history(seen):
    rows = []
    for position_index, position in enumerate(SKILL_POSITIONS):
        for player in range(3):
            player_id = 10 * position_index + player + 1
            for week in range(1, 6):
                line = {field: float(week + player) for field in OFFENSE_FIELDS}
                line.update({field: 1.0 for field in UNUSED_SOURCE_FIELDS})
                rows.append(HistoricalWeek(
                    player_id=player_id, player_gsis_id=f"00-{player_id:07d}", player_name=f"P{player_id}",
                    position=position, season=2026, week=week, team=f"T{player}", opponent=f"O{player}",
                    stats=RecordingStats(line, seen),
                ))
    return rows


def test_projection_reads_only_offense_fields():
    seen: set[str] = set()
    rows = history(seen)
    for position_index, position in enumerate(SKILL_POSITIONS):
        player_id = 10 * position_index + 1
        project_player(player_id=player_id, player_gsis_id=f"00-{player_id:07d}", player_name=f"P{player_id}",
                       position=position, historical_rows=rows, cutoff_season=2026, cutoff_week=6,
                       context=ProjectionContext(team_implied_total=24.0, opponent_factor=1.05),
                       config={**MODEL_CONFIG, **CONFIG_OVERRIDES})
    assert seen and seen <= set(OFFENSE_FIELDS), seen - set(OFFENSE_FIELDS)


def test_opponent_factors_and_passing_leaders_read_only_offense_fields():
    seen: set[str] = set()
    rows = history(seen)
    for mode in OPPONENT_MODES:
        opponent_factors(rows, 2026, mode)
    last_game_passing_leaders(rows)
    assert seen and seen <= set(OFFENSE_FIELDS), seen - set(OFFENSE_FIELDS)


def test_matchup_draws_read_only_offense_fields():
    seen: set[str] = set()
    rows = history(seen)
    for position_index, position in enumerate(SKILL_POSITIONS):
        player_id = 10 * position_index + 1
        player = {"player_id": player_id, "player_gsis_id": f"00-{player_id:07d}", "player_name": f"P{player_id}",
                  "position": position, "feature_snapshot": {"cutoff_season": 2026, "cutoff_week": 6}}
        assert sample_baseline_draws(player, rows, CONFIG_OVERRIDES, seed=1)
    assert seen and seen <= set(OFFENSE_FIELDS), seen - set(OFFENSE_FIELDS)
