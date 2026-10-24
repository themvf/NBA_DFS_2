"""nfl-ownership-news-v1 trigger (model/nfl_ownership_news.py)."""

import json
from pathlib import Path

import pandas as pd
import pytest

from model import nfl_ownership_news as news

SPEC = json.loads((Path(__file__).resolve().parents[1] / "docs" / "nfl-ownership-news-chalk-study.json").read_text())


def box_rows(rows):
    """rows: (player_id, season, week, team, position, targets, carries, attempts)."""
    return pd.DataFrame(rows, columns=["player_id", "season", "week", "team", "position", "targets", "carries", "attempts"])


def players(rows):
    """rows: (ff_player_id, name, team, position, salary, is_out)."""
    df = pd.DataFrame(rows, columns=["ff_player_id", "normalized_name", "team", "position", "salary", "is_out"])
    df["proj"], df["dk_avg"] = 10.0, 6.0
    return df


def wr_history(pid, team, weeks, targets, season=2026):
    return [(pid, season, w, team, "WR", targets, 0, 0) for w in weeks]


def test_constants_match_registration():
    assert news.APPEARANCES == SPEC["baseline"]["appearances"]
    assert news.HALF_LIFE == SPEC["baseline"]["half_life_appearances"]
    assert news.MIN_APPEARANCES == SPEC["baseline"]["min_appearances"]
    bars = SPEC["vacancy"]["bars"]
    for pos in ("RB", "WR", "TE"):
        assert news.BARS[pos] == (bars[pos]["stat"], float(bars[pos]["min_per_game"]))
    assert news.QB_MIN_ATTEMPTS == bars["QB"]["min_attempts"]
    assert {k: list(v) for k, v in news.ROOMS.items()} == SPEC["beneficiaries"]["rooms"]
    assert news.TEAM_ALIASES == SPEC["inputs"]["team_key"]["aliases"]
    assert SPEC["inputs"]["box_scores"]["seasons"] == [2025, 2026]


def test_star_wr_out_flags_wr_and_te_room_only_on_his_team():
    box = box_rows(wr_history(1, "TB", [1, 2, 3, 4], 9) + wr_history(9, "CAR", [1, 2, 3, 4], 9)
                   + [(2, 2026, 4, "TB", "WR", 3, 0, 0)])
    pl = players([(1, "star", "TB", "WR", 5700, True), (2, "cheap", "TB", "WR", 3000, False),
                  (3, "te", "TB", "TE", 3100, False), (4, "rb", "TB", "RB", 6100, False),
                  (5, "other", "CAR", "WR", 4000, False)])
    t = news.find_trigger(pl, box, 2026, 5)
    assert [v.ff_player_id for v in t.vacancies] == [1]
    assert t.vacancies[0].first_game is True
    assert {pl.loc[i, "normalized_name"] for i in t.beneficiaries} == {"cheap", "te"}


def test_below_bar_and_one_appearance_are_not_vacancies():
    box = box_rows(wr_history(1, "TB", [1, 2, 3, 4], 5) + wr_history(2, "TB", [4], 12))
    pl = players([(1, "low", "TB", "WR", 5000, True), (2, "once", "TB", "WR", 5000, True),
                  (3, "mate", "TB", "WR", 3000, False)])
    t = news.find_trigger(pl, box, 2026, 5)
    assert t.vacancies == [] and t.beneficiaries == set()


def test_long_term_absence_is_not_news():
    # Played weeks 1-3 of 2025 only; the team has played since.
    box = box_rows([(1, 2025, w, "ARI", "RB", 2, 18, 0) for w in (1, 2, 3)]
                   + [(2, 2025, w, "ARI", "RB", 2, 10, 0) for w in range(4, 18)]
                   + [(2, 2026, w, "ARI", "RB", 2, 10, 0) for w in range(1, 7)])
    pl = players([(1, "conner", "ARI", "RB", 5000, True), (2, "benson", "ARI", "RB", 5500, False)])
    assert news.find_trigger(pl, box, 2026, 7).vacancies == []


def test_absence_under_way_is_a_vacancy_but_not_first_game():
    box = box_rows(wr_history(1, "TB", [1, 2, 3], 9) + [(2, 2026, w, "TB", "WR", 4, 0, 0) for w in (1, 2, 3, 4)])
    pl = players([(1, "star", "TB", "WR", 5700, True), (2, "mate", "TB", "WR", 3000, False)])
    t = news.find_trigger(pl, box, 2026, 5)
    assert len(t.vacancies) == 1 and t.vacancies[0].first_game is False


def test_washington_codes_match():
    box = box_rows(wr_history(1, "WSH", [1, 2, 3], 9))
    pl = players([(1, "star", "WAS", "WR", 5700, True), (2, "mate", "WAS", "WR", 3000, False)])
    t = news.find_trigger(pl, box, 2026, 4)
    assert len(t.vacancies) == 1 and len(t.beneficiaries) == 1


def test_traded_player_counts_only_his_current_team_games():
    box = box_rows(wr_history(1, "NYG", [1, 2, 3], 10) + [(1, 2026, 4, "MIN", "WR", 2, 0, 0)]
                   + [(7, 2026, w, "MIN", "WR", 5, 0, 0) for w in (1, 2, 3, 4)])
    pl = players([(1, "traded", "MIN", "WR", 5000, True), (2, "mate", "MIN", "WR", 3000, False)])
    assert news.find_trigger(pl, box, 2026, 5).vacancies == []   # one MIN appearance


def test_baseline_weights_recent_games_more():
    box = box_rows(wr_history(1, "TB", [1], 20) + wr_history(1, "TB", [2], 0)).assign(team_key="TB")
    b = news.baseline(box, 1, "TB", "targets")
    assert b == pytest.approx(20 * 0.5 ** 0.25 / (1 + 0.5 ** 0.25))


def test_qb_leader_rule_needs_15_attempts_and_breaks_ties_by_id():
    box = box_rows([(5, 2026, 4, "NYJ", "QB", 0, 0, 30), (4, 2026, 4, "NYJ", "QB", 0, 0, 30),
                    (6, 2026, 4, "NYJ", "QB", 0, 0, 10)]).assign(team_key="NYJ")
    assert news.passing_leader(box, "NYJ", (2026, 4)) == 4
    pl = players([(4, "starter", "NYJ", "QB", 5500, True), (5, "backup", "NYJ", "QB", 4800, False),
                  (8, "wr", "NYJ", "WR", 5000, False)])
    t = news.find_trigger(pl, box.drop(columns="team_key"), 2026, 5)
    assert [v.position for v in t.vacancies] == ["QB"]
    assert {pl.loc[i, "normalized_name"] for i in t.beneficiaries} == {"backup"}


def test_two_vacancies_one_beneficiary_and_out_or_unpriced_teammates_excluded():
    box = box_rows(wr_history(1, "TB", [1, 2, 3], 9) + [(2, 2026, w, "TB", "TE", 6, 0, 0) for w in (1, 2, 3)])
    pl = players([(1, "wr1", "TB", "WR", 5700, True), (2, "te1", "TB", "TE", 4000, True),
                  (3, "te2", "TB", "TE", 2500, False), (4, "wr9", "TB", "WR", 0, False), (5, "wr3", "TB", "WR", 3000, True)])
    t = news.find_trigger(pl, box, 2026, 4)
    assert len(t.vacancies) == 2
    assert {pl.loc[i, "normalized_name"] for i in t.beneficiaries} == {"te2"}


def test_future_weeks_are_never_read():
    box = box_rows(wr_history(1, "TB", [5, 6, 7], 12))
    pl = players([(1, "star", "TB", "WR", 5700, True), (2, "mate", "TB", "WR", 3000, False)])
    assert news.find_trigger(pl, box, 2026, 5).vacancies == []


def test_candidate_frame_drops_dk_average_only_for_beneficiaries_with_a_projection():
    pl = players([(1, "a", "TB", "WR", 3000, False), (2, "b", "TB", "WR", 3000, False)])
    pl.loc[1, "proj"] = None
    df = news.candidate_frame(pl, {0, 1})
    assert pd.isna(df.loc[0, "dk_avg"]) and df.loc[1, "dk_avg"] == 6.0
    assert news.weights(pl, {1}, 3.0) == [1.0, 3.0]


def _classic_pool():
    rows = []
    for k in range(12):
        for pos in ("QB", "RB", "WR", "TE", "DST"):
            rows.append({"position": pos, "salary": 3000 + 400 * k, "proj": 4.0 + k, "dk_avg": 3.0 + k,
                         "dk_status": "", "is_out": False, "captain_salary": (3000 + 400 * k) * 1.5, "team": "T%d" % (k % 2)})
    return pd.DataFrame(rows)


def test_v6_hook_leaves_v5_unchanged_and_moves_share_within_the_position():
    from model import nfl_ownership_scorecard as sc

    df = _classic_pool()
    v5 = sc.prior_classic(df, "v5")
    assert (sc.prior_classic(df, "v5", weight_multiplier=[1.0] * len(df)) == v5).all()
    target = df.index[(df["position"] == "WR")][0]
    mult = [3.0 if i == target else 1.0 for i in df.index]
    v6 = sc.prior_classic(df, "v5", weight_multiplier=mult)
    assert v6[target] > v5[target]
    for pos in ("QB", "RB", "WR", "TE", "DST"):
        m = (df["position"] == pos).to_numpy()
        assert v6[m].sum() == pytest.approx(v5[m].sum(), abs=0.05)
    assert (v6[(df["position"] != "WR").to_numpy()] == v5[(df["position"] != "WR").to_numpy()]).all()


def test_v6_hook_showdown_matches_v5_at_ones_and_rejects_other_variants():
    from model import nfl_ownership_scorecard as sc

    df = _classic_pool().head(20).reset_index(drop=True)
    f5, c5, _ = sc.prior_showdown_version(df, "v5")
    f6, c6, _ = sc.prior_showdown_version(df, "v5", weight_multiplier=[1.0] * len(df))
    assert (f5 == f6).all() and (c5 == c6).all()
    f7, c7, _ = sc.prior_showdown_version(df, "v5", weight_multiplier=[4.0] + [1.0] * (len(df) - 1))
    assert f7[0] >= f5[0] and c7[0] >= c5[0] and f7.sum() == pytest.approx(f5.sum(), abs=0.1)
    with pytest.raises(ValueError):
        sc.prior_showdown_version(df, "v4", weight_multiplier=[1.0] * len(df))
