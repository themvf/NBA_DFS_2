import sys, types

import ingest.nfl_dfs_review_digest as mod

def payload(week, scored):
    rows = [{"variant":"production","position":"QB","name":f"W{week}","team":"GB",
             "forecast":{"mean":10.0},"actual":18.0 if scored else None,
             "error":8.0 if scored else None,"interval_hit":None,"overdue":False}]
    return {"season":2026,"week":week,"evaluated_at":"t","scheduled_games":16,
            "completed_games":16 if scored else 0,"missing_policy":"p","rows":rows}

class FakeDB:
    def __init__(self, weeks): self.weeks = weeks   # {week: scored?}
    def execute(self, sql, params=None):
        if "DISTINCT week" in sql:
            return [{"week": w} for w in sorted(self.weeks, reverse=True)]
        season, week = params
        return [{"payload": payload(week, self.weeks[week])}] if week in self.weeks else []

def pick(weeks, explicit=None):
    _, w = mod.latest_report(FakeDB(weeks), 2026, explicit)
    return w

# The live situation: week 2 exists and is unplayed, week 1 has results.
assert pick({1: True, 2: False}) == 1, "must skip the unplayed newer week"
# Both scored -> newest wins.
assert pick({1: True, 2: True}) == 2, "newest scored week wins"
# Nothing scored anywhere -> fall back to newest rather than returning nothing.
assert pick({1: False, 2: False}) == 2, "falls back to newest when nothing is scored"
# Explicit override always wins.
assert pick({1: True, 2: False}, explicit=2) == 2, "explicit --week is respected"
# Gap: week 3 unplayed, week 2 missing entirely, week 1 scored.
assert pick({1: True, 3: False}) == 1, "walks back past a missing week"
# No report cards at all.
assert pick({}) is None
print("week-selection checks passed")
