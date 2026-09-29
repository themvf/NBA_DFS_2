"""Choose which saved DraftKings salary uploads receive defensive captures.

The NFL DFS page reads a defensive capture only for the exact upload and
projection run it has open (`readDefensiveCaptures`). Capturing one upload per
week therefore leaves every other slate -- every Thursday and Monday Showdown --
with nothing to read. This module selects every upload the page can still open
whose slate has a game ahead, once each, and captures each against its OWN
bound projection run.
"""
from __future__ import annotations

from model.nfl_matchup_features import stamp
from model.nfl_pfr_supplement import team_code

HISTORICAL_V5 = "nfl-dfs-historical-v5"


def week_uploads(db, *, season, week, as_of):
    """Every saved upload bound to one of this week's projection runs, as known at `as_of`."""
    return [dict(r) for r in db.execute("""SELECT u.upload_id,u.format,u.file_name,u.slate_signature,u.file_digest,
        u.player_count,u.projection_run_id,u.created_at,
        r.model_version run_model_version,r.as_of_at run_as_of_at,r.created_at run_created_at,
        (SELECT COUNT(*) FROM nfl_dfs_slate_players s WHERE s.upload_id=u.upload_id) stored_players,
        (SELECT COALESCE(array_agg(DISTINCT s.team),'{}') FROM nfl_dfs_slate_players s
          WHERE s.upload_id=u.upload_id) teams
        FROM nfl_dfs_slate_uploads u JOIN nfl_dfs_projection_runs r ON r.run_id=u.projection_run_id
        WHERE r.season=%s AND r.week=%s AND u.created_at<=%s
        ORDER BY u.created_at DESC,u.upload_id DESC""", (season, week, as_of))]


def pregame_teams(db, *, season, week, as_of):
    """Teams whose game this week kicks off after `as_of` (the same rule as `load_matchups`)."""
    rows = db.execute("""SELECT h.abbreviation home,a.abbreviation away FROM nfl_season_games g
        JOIN nfl_teams h ON h.team_id=g.home_team_id JOIN nfl_teams a ON a.team_id=g.away_team_id
        WHERE g.season=%s AND g.week=%s AND g.game_type='REG' AND g.kickoff>%s""", (season, week, as_of))
    return {team_code(team) for r in rows for team in (r["home"], r["away"])}


def _newest_first(uploads):
    return sorted(uploads, key=lambda u: (stamp(u["created_at"]), str(u["upload_id"])), reverse=True)


def plan_captures(uploads, *, as_of, pregame_teams):
    """Every saved upload the page can still open, once each, with a game ahead.

    Mirrors the page's saved-slate list (`listSavedNflSlates`): an incomplete
    upload is dropped BEFORE de-duplication, so a half-written row cannot
    shadow the complete one beside it. The newest upload then wins for each
    slate signature or file digest -- re-uploading a file against newer
    projections supersedes the older upload, and the newer one is what the
    page opens. Classic and Showdown are selected alike.

    Returns (selected, skipped); every skip carries a reason.
    """
    cutoff = stamp(as_of)
    selected, skipped, owner = [], [], {}
    for upload in _newest_first(uploads):
        upload_id = str(upload["upload_id"])

        def skip(reason, **extra):
            skipped.append({"upload_id": upload_id, "format": upload.get("format"),
                            "file_name": upload.get("file_name"), "reason": reason, **extra})

        if stamp(upload["created_at"]) > cutoff:
            skip("uploaded_after_cutoff")
            continue
        stored = int(upload.get("stored_players") or 0)
        if stored <= 0 or stored < int(upload.get("player_count") or 0):
            skip("incomplete_upload", stored_players=stored, player_count=upload.get("player_count"))
            continue
        keys = [key for key in (("signature", upload.get("slate_signature")), ("digest", upload.get("file_digest"))) if key[1]]
        newer = next((owner[key] for key in keys if key in owner), None)
        for key in keys:
            owner.setdefault(key, newer or upload_id)
        if newer:
            skip("superseded_by_newer_upload", superseded_by=newer)
        elif upload.get("run_model_version") != HISTORICAL_V5:
            skip("baseline_not_historical_v5", baseline_version=upload.get("run_model_version"))
        elif max(stamp(upload["run_as_of_at"]), stamp(upload["run_created_at"])) > cutoff:
            skip("baseline_after_cutoff")
        elif not any(team_code(team) in pregame_teams for team in upload.get("teams") or ()):
            skip("no_game_before_kickoff")
        else:
            selected.append(upload)
    return selected, skipped


def research_primary(uploads):
    """The upload the scenario research has always read: the newest Classic of the week."""
    return next((u for u in _newest_first(uploads) if u.get("format") == "classic"), None)
