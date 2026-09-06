import unittest
import pandas as pd
from ingest.nfl_dfs_player_context import recorded_participation


class ParticipationTests(unittest.TestCase):
    def test_only_scrimmage_and_no_missing_as_absence(self):
        plays = pd.DataFrame([
            {"game_id": "g", "play_id": 1, "play_type": "pass", "posteam": "A"},
            {"game_id": "g", "play_id": 2, "play_type": "run", "posteam": "A"},
            {"game_id": "g", "play_id": 3, "play_type": "kickoff", "posteam": "A"},
        ])
        personnel = pd.DataFrame([
            {"nflverse_game_id": "g", "play_id": 1, "offense_players": "qb;wr;wr"},
            {"nflverse_game_id": "g", "play_id": 3, "offense_players": "k"},
        ])
        result = recorded_participation(plays, personnel)[("g", "A")]
        self.assertEqual(result["plays"], 2)
        self.assertEqual(result["covered"], 1)
        self.assertEqual(dict(result["counts"]), {"qb": 1, "wr": 1})
        with self.assertRaisesRegex(ValueError, "Duplicate"):
            recorded_participation(plays, pd.concat([personnel, personnel]))


if __name__ == "__main__":
    unittest.main()
