from __future__ import annotations

from ingest import refresh_mlb_vegas as refresh


def test_final_settlement_runs_after_score_backfill(monkeypatch) -> None:
    events: list[str] = []
    db = object()

    monkeypatch.setattr(refresh, "fetch_schedule", lambda *_: events.append("schedule") or [])
    monkeypatch.setattr(refresh, "fetch_odds", lambda *_: events.append("odds") or 0)
    monkeypatch.setattr(refresh, "fetch_scores", lambda *_: events.append("scores_today") or 0)
    monkeypatch.setattr(
        refresh,
        "_write_total_predictions",
        lambda *_: events.append("predictions") or 0,
    )
    monkeypatch.setattr(
        refresh,
        "_rate_and_settle_bets",
        lambda *_: events.append("initial_settlement") or 0,
    )
    monkeypatch.setattr(
        refresh,
        "backfill_mlb_schedule",
        lambda *_args, **_kwargs: events.append("score_backfill"),
    )
    monkeypatch.setattr(
        refresh,
        "backfill_mlb_odds",
        lambda *_args, **_kwargs: events.append("odds_backfill"),
    )
    monkeypatch.setattr(
        refresh,
        "_settle_bets",
        lambda *_: events.append("final_settlement") or 38,
    )

    exit_code = refresh.run_refresh(
        db,  # type: ignore[arg-type]
        odds_api_key="test-key",
        target_date="2026-07-11",
        days_back=2,
    )

    assert exit_code == 0
    assert events.count("final_settlement") == 1
    assert events.index("score_backfill") < events.index("final_settlement")
    assert events.index("odds_backfill") < events.index("final_settlement")
