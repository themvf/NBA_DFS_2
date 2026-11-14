from datetime import date

import pytest

from ingest import statapi_nba_projections as ingest
from research import nba_statapi_projection_compare as compare


class FakeClient:
    def __init__(self, players=None):
        self.players = players or {}

    def get(self, path, params=None):
        pid = int(path.rsplit("/", 1)[1])
        if pid not in self.players:
            raise ingest.StatApiError("not found")
        return self.players[pid]


def test_preview_rows_are_refused_not_stored():
    payload = {"projected_player_stats": [{"_metadata": {"required_tier": "starter"}}], "has_more": True}
    with pytest.raises(ingest.StatApiError, match="preview"):
        ingest._rows(payload, "projected_player_stats")


def test_more_than_one_page_is_refused():
    with pytest.raises(ingest.StatApiError, match="more rows"):
        ingest._rows({"games": [{"id": 1}], "has_more": True}, "games")


def test_only_final_games_are_read():
    assert ingest.is_final({"status": "Final"})
    assert ingest.is_final({"status": "F/OT"})
    assert not ingest.is_final({"status": "InProgress"})
    assert not ingest.is_final({"status": "Scheduled"})


def test_build_rows_uses_eastern_date_and_player_key():
    game = {"id": 9, "day": 20261112, "game_time": "2026-11-12T03:00:00+00:00", "status": "Final",
            "home_team": "POR", "away_team": "OKC"}
    projection = {"player_id": 36, "projection_id": 3, "team_id": 18, "pts": 34.16, "status": "available",
                  "play_probability": 1.0, "stat_quantiles": {"pts": {"p10": 20.0, "p50": 34.0, "p90": 48.0}},
                  "inputs_as_of": "2026-11-12T02:59:03+00:00"}
    players = {36: {"full_name": "Shai Gilgeous-Alexander"}}
    row = ingest.build_rows(game, [projection], players, {18: "OKC"}, FakeClient())[0]
    assert row[3] == date(2026, 11, 11)  # 10 PM Eastern on Nov 11
    assert row[8] == "OKC"
    assert row[10] == "shaigilgeousalexander"
    assert (row[19], row[20], row[22]) == (34.16, 20.0, 48.0)


def test_unknown_player_keeps_the_row_without_a_name():
    game = {"id": 9, "day": 20261112, "game_time": "2026-11-12T03:00:00+00:00", "status": "Final"}
    row = ingest.build_rows(game, [{"player_id": 77, "projection_id": 3, "pts": 1.0}], {}, {}, FakeClient())[0]
    assert row[9] is None and row[10] is None


def _row(d, a, s, m):
    return {"game_date": d, "actual": a, "statapi": s, "market_line": m}


def test_paired_difference_and_common_population():
    rows = [_row(date(2026, 11, 1), 20, 18, 21), _row(date(2026, 11, 2), 10, 14, 11)]
    p = compare.paired(rows, "market_line", "statapi")
    assert p["mae_diff"] == pytest.approx(((1 - 2) + (1 - 4)) / 2)
    assert p["a_closer_share"] == 1.0
    rows.append({"game_date": date(2026, 11, 3), "actual": 5, "statapi": 5, "market_line": None})
    table = compare.mean_table(rows, ["statapi", "market_line"])
    assert {t["n"] for t in table} == {2}
