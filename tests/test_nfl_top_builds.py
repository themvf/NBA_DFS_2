"""Top-builds report: stack shape, portfolio dispersion and the score-curve rank, all pure."""

import pytest

from research.nfl_top_builds import analyze_portfolio, estimate_rank, stack_shape


def seat(slot, name, team, pos, fpts=10.0, field=10.0, salary=5000, opp=None):
    return {"slot": slot, "name": name, "team": team, "position": pos, "salary": salary, "fpts": fpts,
            "field_pct": field, "opponent": opp}


WINNER = [seat("QB", "Dak Prescott", "DAL", "QB", 21.1, 5.19, opp="HOU"), seat("RB", "Javonte Williams", "DAL", "RB", 31.3, 1.51),
          seat("RB", "Kyren Williams", "LAR", "RB", 36.7, 9.15), seat("WR", "CeeDee Lamb", "DAL", "WR", 44.3, 10.26),
          seat("WR", "Nico Collins", "HOU", "WR", 33.8, 12.17), seat("WR", "Zay Flowers", "BAL", "WR", 28.8, 7.31),
          seat("TE", "Zach Ertz", "WAS", "TE", 3.3, 1.08), seat("FLEX", "T.J. Hockenson", "MIN", "TE", 27.9, 15.93),
          seat("DST", "Jaguars", "JAX", "DST", 7.0, 1.73)]


def test_stack_shape_counts_qb_teammates_and_the_bring_back():
    sh = stack_shape(WINNER)
    assert sh["qb"] == "Dak Prescott" and sh["teammates"] == 2 and sh["bring_back"] is True
    assert sh["label"] == "QB/RB/WR+BB" and sh["bring_back_team"] == "HOU"
    no_qb = stack_shape([s for s in WINNER if s["slot"] != "QB"])
    assert no_qb["qb"] is None and no_qb["label"] == "no QB"
    dst_only = stack_shape([seat("QB", "Q", "DAL", "QB", opp="HOU"), seat("DST", "Cowboys", "DAL", "DST"), seat("DST", "Texans", "HOU", "DST")])
    assert dst_only["teammates"] == 0 and dst_only["bring_back"] is False


def test_analyze_portfolio_dispersion_exposure_and_edge():
    other = [seat("QB", "Josh Allen", "BUF", "QB", 19.5, 7.5, opp="NO"), seat("RB", "Kyren Williams", "LAR", "RB", 36.7, 9.15),
             seat("RB", "Chase Brown", "CIN", "RB", 19.1, 16.04), seat("WR", "Khalil Shakir", "BUF", "WR", 12.0, 4.0),
             seat("WR", "Nico Collins", "HOU", "WR", 33.8, 12.17), seat("WR", "Zay Flowers", "BAL", "WR", 28.8, 7.31),
             seat("TE", "Dalton Kincaid", "BUF", "TE", 9.0, 3.0), seat("FLEX", "T.J. Hockenson", "MIN", "TE", 27.9, 15.93),
             seat("DST", "Jaguars", "JAX", "DST", 7.0, 1.73)]
    a = analyze_portfolio([{"roster": WINNER, "salary": 49800}, {"roster": other, "salary": 50000}], field_avg_per_slot=13.17)
    assert a["stacks"]["qb_plus_2"] == 2 and a["stacks"]["qb_plus_3_or_more"] == 0   # Dak+Javonte+Lamb; Allen+Shakir+Kincaid
    assert a["stacks"]["with_bring_back"] == 1 and a["stacks"]["distinct_qbs"] == 2
    d = a["dispersion"]
    # Shared: Kyren, Collins, Flowers, Hockenson, Jaguars = 5; 9 - 5 = 4 apart.
    assert d["players_used"] == 13 and d["shared_avg"] == 5.0 and d["closest_twin_avg"] == 4.0
    assert d["most_used_pct"] == 100.0
    kyren = next(p for p in a["players"] if p["name"] == "Kyren Williams")
    assert kyren["exposure_pct"] == 100.0 and kyren["leverage"] == pytest.approx(90.85, abs=0.06)   # rounded to 0.1
    assert kyren["edge"] == pytest.approx(0.9085 * (36.7 - 13.17), abs=0.01)
    assert a["ownership"]["min"] == pytest.approx(64.33, abs=0.06) and a["salary"]["avg"] == 49900


def test_estimate_rank_reads_the_curve():
    curve = [[1, 234.2], [2, 234.0], [3, 233.28], [100, 200.0], [900, 150.0], [161764, 0.0]]
    assert estimate_rank(240.0, curve) == 1
    assert estimate_rank(234.1, curve) == 2
    assert estimate_rank(200.0, curve) == 100
    assert estimate_rank(175.0, curve) == 501                 # midway between the 100th and 900th: ~500 above it
    assert estimate_rank(-1.0, curve) == 161765               # below every entry
    assert estimate_rank(100.0, []) is None


class _FakeDb:
    """Answers load_ours's queries from fixed rows, keyed on the table each one reads."""

    def __init__(self, week_results, seats, lineup):
        self.week_results, self.seats, self.lineup = week_results, seats, lineup

    def execute(self, sql, params=None):
        from datetime import datetime, timezone
        if "FROM nfl_dfs_optimizer_runs" in sql:
            return [{"run_id": "run-1", "created_at": datetime(2026, 10, 11, tzinfo=timezone.utc), "generated_lineups": 1, "mode": "gpp"}]
        if "FROM nfl_dfs_slate_players" in sql:
            return [{"dk_player_id": i, "name": n, "position": p, "team": t, "opponent": None, "salary": 5000}
                    for i, (n, p, t) in enumerate(self.lineup)]
        if "FROM nfl_dfs_field_user_builds" in sql:
            return [{"lineups": [{"roster": [[p, n, t, p, 5000, f, 1.0] for n, p, t, f in self.seats]}]}]
        if "FROM nfl_dfs_field_ownership" in sql:
            return []
        if "FROM nfl_dfs_player_week_results" in sql:
            return [{"name": n, "position": p, "team": t, "pts": f} for n, p, t, f in self.week_results]
        if "FROM nfl_dfs_lineups" in sql:
            return [{"lineup_number": 1, "total_salary": 50000,
                     "slots": [{"slot": p, "name": n, "dkPlayerId": i, "team": t} for i, (n, p, t) in enumerate(self.lineup)]}]
        raise AssertionError(sql)


def test_late_game_without_a_results_row_uses_the_contest_points_not_zero():
    # 2026-10-11: the results table had no Detroit rows yet, so Gibbs's 40.7 read as 0.
    from research.nfl_top_builds import load_ours
    db = _FakeDb(week_results=[("Chase Brown", "RB", "CIN", 8.0)],
                 seats=[("Jahmyr Gibbs", "RB", "DET", 40.7)],
                 lineup=[("Jahmyr Gibbs", "RB", "DET"), ("Chase Brown", "RB", "CIN")])
    _, lineups = load_ours(db, {"slate_upload_id": "u", "contest_id": "c", "season": 2026, "week": 5}, "run-1")
    assert lineups[0]["points"] == 48.7
    assert lineups[0]["missing"] == []


def test_a_player_with_no_points_anywhere_leaves_the_lineup_unscored():
    from research.nfl_top_builds import load_ours
    db = _FakeDb(week_results=[("Chase Brown", "RB", "CIN", 8.0)], seats=[],
                 lineup=[("Jerry Jeudy", "WR", "CLE"), ("Chase Brown", "RB", "CIN")])
    _, lineups = load_ours(db, {"slate_upload_id": "u", "contest_id": "c", "season": 2026, "week": 5}, "run-1")
    assert lineups[0]["points"] is None
    assert lineups[0]["missing"] == ["Jerry Jeudy"]
