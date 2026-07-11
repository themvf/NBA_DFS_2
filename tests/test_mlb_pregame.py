from __future__ import annotations

import inspect

from model import mlb_game_bets, mlb_game_total_model, mlb_moneyline_model
from model.mlb_pregame import eligible_pregame_matchup_ids


class FakeDb:
    def __init__(self) -> None:
        self.sql = ""
        self.params: tuple[str, ...] = ()

    def execute(self, sql: str, params: tuple[str, ...]):
        self.sql = sql
        self.params = params
        return [{"id": 11}, {"id": 12}]


def test_live_mlb_eligibility_requires_known_future_commence() -> None:
    db = FakeDb()

    result = eligible_pregame_matchup_ids(db, "2026-07-11")  # type: ignore[arg-type]

    assert result == {11, 12}
    assert db.params == ("2026-07-11",)
    assert "commence_time IS NOT NULL" in db.sql
    assert "commence_time > NOW()" in db.sql


def test_every_live_game_line_writer_uses_fail_closed_commence_guard() -> None:
    assert "eligible_pregame_matchup_ids" in inspect.getsource(
        mlb_game_total_model.predict_and_write,
    )
    assert "eligible_pregame_matchup_ids" in inspect.getsource(
        mlb_moneyline_model.predict_and_write,
    )
    rating_source = inspect.getsource(mlb_game_bets.rate_slate)
    assert "commence_time IS NOT NULL" in rating_source
    assert "commence_time > NOW()" in rating_source
