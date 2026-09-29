"""FantasyPros injuries bind to ACTIVE canonical players, never a deactivated duplicate."""
from ingest.nfl_dfs_availability import load_identity_candidates
from model.nfl_dfs_injury_identity import audit

# The real pair (2026): a FantasyPros-only row that ff_dedupe_identities
# deactivated, still carrying the FantasyPros id, and its active nflverse twin.
DEACTIVATED = {"id": 34, "active": False, "canonical_name": "Puka Nacua", "normalized_name": "pukanacua",
               "team_abbrev": "LAR", "position": "WR", "fantasypros_player_id": 23180, "yahoo_id": None}
ACTIVE = {"id": 560, "active": True, "canonical_name": "Puka Nacua", "normalized_name": "pukanacua",
          "team_abbrev": "LAR", "position": "WR", "fantasypros_player_id": None, "yahoo_id": "40168"}
ROW = {"player_id": 23180, "name": "Puka Nacua", "team_id": "LAR", "position_id": "WR"}


class PlayersDb:
    def __init__(self):
        self.sql = None

    def execute(self, sql, params=None):
        self.sql = " ".join(sql.split())
        rows = [DEACTIVATED, ACTIVE]
        if "AND active" in self.sql:
            rows = [row for row in rows if row["active"]]
        return [{k: v for k, v in row.items() if k != "active"} for row in rows]


def test_candidates_exclude_deactivated_rows():
    db = PlayersDb()
    ids = [row["id"] for row in load_identity_candidates(db, 2026)]
    assert ids == [560]
    assert "WHERE season=%s AND active" in db.sql


def test_the_injury_binds_to_the_active_twin_not_the_deactivated_fantasypros_id():
    decision = audit([ROW], load_identity_candidates(PlayersDb(), 2026))["decisions"][0]
    assert decision["category"] == "matched"
    assert decision["player_id"] == 560


def test_with_yahoo_id_it_binds_by_the_stronger_key():
    decision = audit([{**ROW, "yahoo_id": "40168"}], load_identity_candidates(PlayersDb(), 2026))["decisions"][0]
    assert (decision["player_id"], decision["method"]) == (560, "yahoo_id")


def test_without_the_filter_the_deactivated_id_wins():
    """Documents the defect the filter removes."""
    everyone = [{k: v for k, v in row.items() if k != "active"} for row in (DEACTIVATED, ACTIVE)]
    decision = audit([ROW], everyone)["decisions"][0]
    assert decision["player_id"] == 34
