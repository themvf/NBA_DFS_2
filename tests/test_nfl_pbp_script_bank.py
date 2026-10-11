import unittest

from model.nfl_pbp_script_bank import Drive, drive_catalog, sample_scripts, transport_bank


class PbpScriptBankTests(unittest.TestCase):
    def test_drive_is_counted_once_and_score_state_is_from_entry(self):
        rows = [
            {"game_id": "g", "posteam": "AAA", "drive": 1, "play_id": 1, "season": 2025, "week": 1,
             "drive_archetype": "FIELD_GOAL", "score_differential": -10, "game_seconds_remaining": 3000,
             "play_type": "pass", "qb_dropback": True, "had_sack": False},
            {"game_id": "g", "posteam": "AAA", "drive": 1, "play_id": 2, "season": 2025, "week": 1,
             "drive_archetype": "FIELD_GOAL", "score_differential": -10, "game_seconds_remaining": 2940,
             "play_type": "run", "qb_dropback": False, "had_sack": False},
        ]
        catalog = drive_catalog(rows)
        self.assertEqual(len(catalog), 1)
        self.assertEqual((catalog[0].state, catalog[0].terminal, catalog[0].plays, catalog[0].dropbacks),
                         ("trailing", "FIELD_GOAL", 2, 1))

    def test_sampled_game_updates_score_state_and_counts_events(self):
        catalog = [Drive(f"g{i}", team, 2025, i, "close", "FIELD_GOAL", 480, 5, 3, 0)
                   for i in range(8) for team in ("AAA", "BBB")]
        scripts, sources = sample_scripts(catalog, ("AAA", "BBB"), 3, 7)
        self.assertEqual(sources, {"team_state": 24})
        for script in scripts:
            self.assertEqual(sum(script["score"].values()), 3 * len(script["drives"]))
            self.assertEqual(sum(t["fg"] for t in script["totals"].values()), len(script["drives"]))

    def test_score_state_changes_which_drive_pool_is_sampled(self):
        catalog = []
        for i in range(8):
            for condition in ("close", "leading", "trailing"):
                catalog.append(Drive(f"g{i}", "AAA", 2025, i, condition, "TOUCHDOWN", 120, 5, 3, 0))
                catalog.append(Drive(f"g{i}", "BBB", 2025, i, condition, "STALLED", 120, 5, 3, 0))
        scripts, _ = sample_scripts(catalog, ("AAA", "BBB"), 2, 12)
        self.assertTrue(any(d["state"] == "leading" for s in scripts for d in s["drives"] if d["team"] == "AAA"))
        self.assertTrue(any(d["state"] == "trailing" for s in scripts for d in s["drives"] if d["team"] == "BBB"))

    def test_transport_keeps_whole_scenario_and_distinct_stream_identity(self):
        teams = ("AAA", "BBB")
        events = [{"team": t, "final_points": 7, "fg_short": 0, "fg_medium": 0, "fg_long": 0,
                   "interceptions_thrown": 0, "fumbles_lost": 0, "sacks_suffered": 0,
                   "opportunities": {"attempts": 20, "carries": 20}} for t in teams]
        bank = {"source": "model", "snapshotId": "base", "seed": 1,
                "scenarios": [{"id": "old", "weight": 1, "stats": {"1": {"passYds": 200}, "2": {"sacks": 3}}}]}
        script = {"id": 0, "score": {t: 7 for t in teams},
                  "totals": {t: {"fg": 0, "turnovers": 0, "sacks": 0, "plays": 40, "dropbacks": 20} for t in teams}}
        result, diagnostics = transport_bank(bank, [[{"teams": events}]], [script, {**script, "id": 1}], teams, 5)
        self.assertTrue(diagnostics["accepted"])
        self.assertEqual(result["scenarios"][0]["stats"], bank["scenarios"][0]["stats"])
        self.assertNotEqual(result["scenarios"][0]["id"], result["scenarios"][1]["id"])
        self.assertTrue(result["snapshotId"].startswith("nfl-pbp-script-transport-shadow-v1:"))

    def test_transport_rejects_unrepresented_script(self):
        teams = ("AAA", "BBB")
        events = [{"team": t, "final_points": 0, "fg_short": 0, "fg_medium": 0, "fg_long": 0,
                   "interceptions_thrown": 0, "fumbles_lost": 0, "sacks_suffered": 0,
                   "opportunities": {"attempts": 1, "carries": 1}} for t in teams]
        bank = {"source": "model", "snapshotId": "base", "scenarios": [{"id": "old", "weight": 1, "stats": {"1": {}}}]}
        script = {"id": 0, "score": {t: 70 for t in teams},
                  "totals": {t: {"fg": 6, "turnovers": 4, "sacks": 5, "plays": 40, "dropbacks": 35} for t in teams}}
        with self.assertRaisesRegex(ValueError, "coverage failed"):
            transport_bank(bank, [[{"teams": events}]], [script], teams, 5)


if __name__ == "__main__":
    unittest.main()
