"""Which saved salary uploads receive defensive captures (reliability item B3).

Before this, the capture job froze only the newest Classic upload of the week,
so every Thursday/Monday Showdown was left with nothing for the page to read
(PHI@CHI, 2026-09-28: 0 of 56 players adjusted).
"""
import json
from datetime import datetime, timedelta, timezone

import pytest

import research.nfl_allowed_rushing_volume_capture as volume
import research.nfl_matchup_implementation as impl
from research.nfl_saved_upload_selection import plan_captures, pregame_teams, research_primary, week_uploads

NOW = datetime(2026, 9, 28, 21, 43, tzinfo=timezone.utc)
V5 = "nfl-dfs-historical-v5"


def upload(upload_id, *, fmt="classic", created_hours_ago=1.0, teams=("PHI", "CHI"), signature=None, digest=None,
           stored=None, player_count=10, run_version=V5, run_hours_ago=None):
    created = NOW - timedelta(hours=created_hours_ago)
    run_at = NOW - timedelta(hours=run_hours_ago if run_hours_ago is not None else created_hours_ago + 0.1)
    return {"upload_id": upload_id, "format": fmt, "file_name": f"{upload_id}.csv",
            "slate_signature": signature or f"sig-{upload_id}", "file_digest": digest or f"digest-{upload_id}",
            "player_count": player_count, "stored_players": player_count if stored is None else stored,
            "projection_run_id": f"run-{upload_id}", "created_at": created,
            "run_model_version": run_version, "run_as_of_at": run_at, "run_created_at": run_at, "teams": list(teams)}


def reasons(skipped):
    return {s["upload_id"]: s["reason"] for s in skipped}


def test_every_open_classic_and_showdown_is_selected_each_against_its_own_run():
    sunday = upload("sunday", teams=("KC", "MIA", "BAL", "DAL"), created_hours_ago=45)
    monday = upload("monday-showdown", fmt="showdown", teams=("PHI", "CHI"), created_hours_ago=0.7)
    selected, skipped = plan_captures([sunday, monday], as_of=NOW,
                                      pregame_teams={"PHI", "CHI", "KC", "MIA", "BAL", "DAL"})
    assert [u["upload_id"] for u in selected] == ["monday-showdown", "sunday"]
    assert {u["upload_id"]: u["projection_run_id"] for u in selected} == {
        "monday-showdown": "run-monday-showdown", "sunday": "run-sunday"}
    assert skipped == []


def test_started_slate_is_skipped_with_a_reason_not_captured_empty():
    # The old job kept freezing the newest Classic after its games ended --
    # six empty runs on 2026-09-28 -- while the upcoming Showdown got nothing.
    sunday = upload("sunday", teams=("KC", "MIA"), created_hours_ago=45)
    monday = upload("monday-showdown", fmt="showdown", teams=("PHI", "CHI"), created_hours_ago=0.7)
    selected, skipped = plan_captures([sunday, monday], as_of=NOW, pregame_teams={"PHI", "CHI"})
    assert [u["upload_id"] for u in selected] == ["monday-showdown"]
    assert reasons(skipped) == {"sunday": "no_game_before_kickoff"}


def test_reupload_of_the_same_file_supersedes_the_older_upload():
    newest = upload("newest", digest="same-file", signature="sig-a", created_hours_ago=1)
    older = upload("older", digest="same-file", signature="sig-b", created_hours_ago=2)
    oldest = upload("oldest", digest="other-file", signature="sig-b", created_hours_ago=3)
    selected, skipped = plan_captures([oldest, older, newest], as_of=NOW, pregame_teams={"PHI", "CHI"})
    assert [u["upload_id"] for u in selected] == ["newest"]
    # A chain of duplicates points at the upload the page actually opens.
    assert {s["upload_id"]: s["superseded_by"] for s in skipped} == {"older": "newest", "oldest": "newest"}


def test_incomplete_upload_cannot_shadow_the_complete_one_beside_it():
    half = upload("half-written", fmt="showdown", digest="same-file", stored=3, player_count=56, created_hours_ago=0.2)
    whole = upload("complete", fmt="showdown", digest="same-file", player_count=56, created_hours_ago=0.5)
    selected, skipped = plan_captures([half, whole], as_of=NOW, pregame_teams={"PHI", "CHI"})
    assert [u["upload_id"] for u in selected] == ["complete"]
    assert reasons(skipped) == {"half-written": "incomplete_upload"}


def test_baseline_version_and_point_in_time_guards_skip_with_reasons():
    legacy = upload("legacy-run", run_version="nfl-dfs-historical-v3")
    late_run = upload("late-run", run_hours_ago=-0.1)          # bound run finished after the cutoff
    late_upload = upload("late-upload", created_hours_ago=-0.1)  # uploaded after the cutoff
    selected, skipped = plan_captures([legacy, late_run, late_upload], as_of=NOW, pregame_teams={"PHI", "CHI"})
    assert selected == []
    assert reasons(skipped) == {"legacy-run": "baseline_not_historical_v5", "late-run": "baseline_after_cutoff",
                                "late-upload": "uploaded_after_cutoff"}


def test_draftkings_team_codes_match_the_schedule():
    # DraftKings writes LAR/WSH; nfl_teams and nflverse differ. team_code
    # normalizes both sides the same way load_matchups does.
    rams = upload("rams", fmt="showdown", teams=("LAR", "WSH"))
    selected, _ = plan_captures([rams], as_of=NOW, pregame_teams={"LA", "WAS"})
    assert [u["upload_id"] for u in selected] == ["rams"]


def test_research_primary_keeps_its_original_meaning():
    classic_old = upload("classic-old", created_hours_ago=5)
    classic_new = upload("classic-new", created_hours_ago=3, run_version="nfl-dfs-historical-v3")
    showdown = upload("showdown", fmt="showdown", created_hours_ago=1)
    assert research_primary([classic_old, showdown, classic_new])["upload_id"] == "classic-new"
    assert research_primary([showdown]) is None


class RecordingDb:
    def __init__(self, rows):
        self.rows, self.calls = rows, []

    def execute(self, query, params):
        self.calls.append((query, params))
        return self.rows


def test_week_uploads_is_bounded_by_the_decision_cutoff():
    db = RecordingDb([])
    week_uploads(db, season=2026, week=3, as_of=NOW)
    query, params = db.calls[0]
    assert "u.created_at<=%s" in query and "r.season=%s AND r.week=%s" in query
    assert params == (2026, 3, NOW)


def test_pregame_teams_uses_kickoff_after_the_cutoff():
    db = RecordingDb([{"home": "CHI", "away": "PHI"}, {"home": "LAR", "away": "WSH"}])
    assert pregame_teams(db, season=2026, week=3, as_of=NOW) == {"CHI", "PHI", "LA", "WAS"}
    assert "g.kickoff>%s" in db.calls[0][0] and db.calls[0][1] == (2026, 3, NOW)


class SlateDb:
    """Serves one saved Showdown upload the way nfl_dfs_slate_players stores it."""

    def __init__(self, salary, *, upload_created=NOW - timedelta(hours=1)):
        self.salary, self.upload_created = salary, upload_created

    def execute_one(self, query, params):
        if "nfl_dfs_slate_uploads" in query:
            return {"upload_id": "sd", "format": "showdown", "file_name": "DKSalaries.csv",
                    "projection_run_id": "run-sd", "created_at": self.upload_created}
        return {"run_id": "run-sd", "model_version": V5, "model_config": {}, "seed": 1,
                "as_of_at": NOW - timedelta(hours=2), "created_at": NOW - timedelta(hours=2)}

    def execute(self, query, params):
        if "nfl_dfs_slate_players" in query:
            return [dict(s) for s in self.salary]
        if "nfl_dfs_player_projections" in query:
            return [{"player_id": s["ff_player_id"], "source_evidence": {"game_id": "2026_03_PHI_CHI"}}
                    for s in self.salary if s["ff_player_id"]]
        raise AssertionError(query)


def showdown_row(row_id, ff_id, flex_id, captain_id, *, team="PHI", roster=("FLEX",)):
    return {"id": row_id, "ff_player_id": ff_id, "dk_player_id": flex_id, "captain_dk_player_id": captain_id,
            "roster_positions": list(roster), "name": f"P{ff_id}", "position": "RB", "team": team,
            "salary": 5000, "is_out": False}


@pytest.fixture
def stub_model(monkeypatch):
    monkeypatch.setattr(impl, "sample_baseline_draws", lambda *a, **k: [])
    monkeypatch.setattr(impl, "shadow_projection", lambda *a, **k: {"status": "under_evaluation"})
    return {"g": {"game_id": "2026_03_PHI_CHI", "kickoff": "2026-09-29T00:15:00+00:00", "home": "CHI", "away": "PHI",
                  "sources": [], "manifest_hash": "m"}}


def test_showdown_capture_is_keyed_on_the_flex_id_the_page_reads(stub_model):
    # The page resolves a capture by (ffPlayerId, dkPlayerId) of the saved row;
    # a Showdown row carries the FLEX id there and the Captain id separately.
    salary = [showdown_row(1, 25, 44282403, 44282459), showdown_row(2, 673, 44282405, 44282461, team="CHI"),
              showdown_row(3, None, 44282499, 44282599)]
    artifact, _ = impl.compare_slate(SlateDb(salary), season=2026, week=3, upload_id="sd", fitted={}, as_of=NOW,
                                     history=[], matchups=stub_model)
    assert artifact["format"] == "showdown"
    assert [(p["player_id"], p["dk_player_id"]) for p in artifact["players"]] == [(25, 44282403), (673, 44282405)]
    assert artifact["skipped"] == {"unmatched_projection": 1}


def test_a_captain_only_duplicate_never_displaces_the_flex_row(stub_model):
    salary = [showdown_row(1, 25, 44282459, 44282459, roster=("CPT",)), showdown_row(2, 25, 44282403, 44282459)]
    artifact, _ = impl.compare_slate(SlateDb(salary), season=2026, week=3, upload_id="sd", fitted={}, as_of=NOW,
                                     history=[], matchups=stub_model)
    assert [(p["player_id"], p["dk_player_id"]) for p in artifact["players"]] == [(25, 44282403)]
    assert artifact["skipped"] == {"duplicate_player_identity": 1}


def test_an_upload_newer_than_the_replay_cutoff_is_refused(stub_model):
    db = SlateDb([showdown_row(1, 25, 44282403, 44282459)], upload_created=NOW + timedelta(minutes=5))
    with pytest.raises(ValueError, match="not available at the decision cutoff"):
        impl.compare_slate(db, season=2026, week=3, upload_id="sd", fitted={}, as_of=NOW, history=[], matchups=stub_model)


def test_capture_week_freezes_every_selected_upload_for_both_profiles(monkeypatch, tmp_path):
    uploads = [upload("monday-showdown", fmt="showdown", created_hours_ago=0.7),
               upload("sunday", teams=("KC", "MIA"), created_hours_ago=45),
               upload("thursday-showdown", fmt="showdown", teams=("NYJ", "CHI"), created_hours_ago=0.5,
                      run_version="nfl-dfs-historical-v3")]
    persisted, compared = [], []
    monkeypatch.setattr(impl, "load_matchups", lambda *a: {"g": {}})
    monkeypatch.setattr(impl, "persist_matchups", lambda db, m: 7)
    monkeypatch.setattr(impl, "week_uploads", lambda db, **k: uploads)
    monkeypatch.setattr(impl, "pregame_teams", lambda db, **k: {"PHI", "CHI"})
    monkeypatch.setattr(impl, "_history", lambda *a: ["history"])
    monkeypatch.setattr(volume, "load_prior_inputs", lambda db: ([], {}))

    def fake_compare(db, *, upload_id, as_of, history, matchups, **kw):
        compared.append(upload_id)
        assert history == ["history"] and as_of == NOW
        players = [] if upload_id == "sunday" else [{"shadow": {"status": "under_evaluation"}, "name": "x", "team": "PHI",
                                                     "position": "RB", "salary": 1, "player_id": 1}]
        return {"upload_id": upload_id, "players": players, "file_name": "f", "salary_rows": 1, "as_of_at": NOW.isoformat(),
                "baseline_version": V5, "baseline_run_id": "r"}, {}

    monkeypatch.setattr(impl, "compare_slate", fake_compare)
    monkeypatch.setattr(impl, "render", lambda artifact: "md")
    monkeypatch.setattr(impl, "persist_forecasts", lambda db, a: persisted.append(("pfr", a["upload_id"])) or {"players": 1})
    def fake_volume(db, upload_id, as_of, *, persist, history, prior_inputs, **kw):
        assert persist and history == ["history"] and prior_inputs == ([], {})
        persisted.append(("volume", upload_id))
        return {"status": "captured", "players": 1, "adjusted": 1, "persisted": {}}

    monkeypatch.setattr(volume, "capture_outcome", fake_volume)
    report = impl.capture_week(object(), season=2026, week=3, as_of=NOW, fitted={}, persist=True, output_dir=tmp_path)
    assert report["status"] == "captured" and report["context_rows"] == 7 and report["failed"] == []
    assert [u["upload_id"] for u in report["uploads"]] == ["monday-showdown"]
    assert reasons(report["skipped"]) == {"sunday": "no_game_before_kickoff", "thursday-showdown": "baseline_not_historical_v5"}
    assert ("pfr", "monday-showdown") in persisted and ("volume", "monday-showdown") in persisted
    assert not any(uid == "sunday" for _, uid in persisted), "a started slate is never persisted"
    # The scenario research input keeps its meaning: newest Classic, written even with zero players.
    assert json.loads((tmp_path / "slate-comparison.json").read_text())["upload_id"] == "sunday"
    assert report["research_primary"] == {"upload_id": "sunday", "players": 0}
    assert (tmp_path / "uploads" / "monday-showdown" / "slate-comparison.json").exists()
    assert compared == ["monday-showdown", "sunday"]


def test_one_failing_upload_is_reported_without_starving_the_others(monkeypatch, tmp_path):
    uploads = [upload("broken", fmt="showdown", created_hours_ago=0.5), upload("fine", created_hours_ago=2)]
    monkeypatch.setattr(impl, "load_matchups", lambda *a: {})
    monkeypatch.setattr(impl, "week_uploads", lambda db, **k: uploads)
    monkeypatch.setattr(impl, "pregame_teams", lambda db, **k: {"PHI", "CHI"})
    monkeypatch.setattr(impl, "_history", lambda *a: [])
    monkeypatch.setattr(impl, "render", lambda artifact: "md")

    def fake_compare(db, *, upload_id, **kw):
        if upload_id == "broken":
            raise ValueError("No eligible v5 baseline")
        return {"upload_id": upload_id, "players": []}, {}

    monkeypatch.setattr(impl, "compare_slate", fake_compare)
    report = impl.capture_week(object(), season=2026, week=3, as_of=NOW, fitted={}, profiles={impl.PFR},
                               output_dir=tmp_path)
    assert report["failed"] == [{"upload_id": "broken", "profile": impl.PFR, "error": "ValueError: No eligible v5 baseline"}]
    assert [u["upload_id"] for u in report["uploads"]] == ["broken", "fine"]
    assert report["uploads"][1][impl.PFR]["players"] == 0


def test_no_saved_upload_this_week_is_awaiting_not_an_error(monkeypatch):
    monkeypatch.setattr(impl, "load_matchups", lambda *a: {})
    monkeypatch.setattr(impl, "week_uploads", lambda db, **k: [])
    monkeypatch.setattr(impl, "pregame_teams", lambda db, **k: set())
    report = impl.capture_week(object(), season=2026, week=4, as_of=NOW, fitted={})
    assert report["status"] == "awaiting_current_salary_slate" and report["failed"] == []


def test_a_pinned_baseline_requires_one_named_upload():
    with pytest.raises(ValueError, match="pass upload_id"):
        impl.capture_week(object(), season=2026, week=3, as_of=NOW, fitted={}, baseline_run_id="run-x")


def test_volume_profile_skips_a_started_slate_instead_of_failing(monkeypatch):
    def started(*a, **k):
        raise volume.SlateNotPregame("Every salary game must be strictly pregame")

    monkeypatch.setattr(volume, "capture", started)
    assert volume.capture_outcome(object(), "sunday", NOW) == {"status": "skipped", "reason": "slate_partially_started"}

    def broken(*a, **k):
        raise KeyError("model_config")

    monkeypatch.setattr(volume, "capture", broken)
    assert volume.capture_outcome(object(), "sunday", NOW)["status"] == "failed"
