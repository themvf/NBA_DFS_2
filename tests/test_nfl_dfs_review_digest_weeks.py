"""Which week the digest reports when no --week is given."""
import ingest.nfl_dfs_review_digest as mod


def _payload(week, scored):
    return {"season": 2026, "week": week, "evaluated_at": "t", "scheduled_games": 16,
            "completed_games": 16 if scored else 0, "missing_policy": "p",
            "rows": [{"variant": "production", "position": "QB", "name": f"W{week}", "team": "GB",
                      "forecast": {"mean": 10.0}, "actual": 18.0 if scored else None,
                      "error": 8.0 if scored else None, "interval_hit": None, "overdue": False}]}


class _FakeDB:
    """weeks maps week -> whether that week has any scored row."""
    def __init__(self, weeks):
        self.weeks = weeks

    def execute(self, sql, params=None):
        if "DISTINCT week" in sql:
            return [{"week": w} for w in sorted(self.weeks, reverse=True)]
        _, week = params
        return [{"payload": _payload(week, self.weeks[week])}] if week in self.weeks else []


def pick(weeks, explicit=None):
    return mod.latest_report(_FakeDB(weeks), 2026, explicit)[1]


def test_skips_the_unplayed_newer_week():
    # The live case: week 2's card exists from projection freeze, week 1 has results.
    assert pick({1: True, 2: False}) == 1


def test_newest_scored_week_wins_when_several_qualify():
    assert pick({1: True, 2: True}) == 2


def test_falls_back_to_newest_when_nothing_is_scored_anywhere():
    assert pick({1: False, 2: False}) == 2


def test_explicit_week_is_respected():
    assert pick({1: True, 2: False}, explicit=2) == 2


def test_walks_back_past_a_week_with_no_stored_card():
    assert pick({1: True, 3: False}) == 1


def test_returns_nothing_rather_than_guessing_when_no_card_exists():
    assert pick({}) is None
