"""NBA season strings come from the date; mirrors web/src/lib/nba-season.ts."""
from datetime import date

from config import NbaApiConfig, nba_season_for


def test_season_turns_over_in_august():
    assert nba_season_for("2026-10-24") == "2026-27"
    assert nba_season_for("2026-04-10") == "2025-26"
    assert nba_season_for("2026-07-31") == "2025-26"
    assert nba_season_for("2026-08-01") == "2026-27"
    assert nba_season_for(date(2027, 1, 5)) == "2026-27"
    assert nba_season_for("2099-12-01") == "2099-00"


def test_config_default_is_todays_season():
    assert NbaApiConfig().season == nba_season_for(date.today())
