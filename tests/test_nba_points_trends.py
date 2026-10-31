from copy import deepcopy
from datetime import date, timedelta

import numpy as np

from research.nba_points_trends import build_features, HISTORY, FAMILIES


def fixture():
    players, teams = [], []
    for day in range(15):
        when = str(date(2025,11,1)+timedelta(days=day))
        for team,opp in [(1,2),(2,1)]:
            total=0
            for slot in range(5):
                points=20+day+slot
                total+=points
                players.append(dict(player_id=team*10+slot,name=f"P{team}-{slot}",game_id=str(day),
                    game_date=when,team_id=team,opponent_team_id=opp,is_home=team==1,minutes=48.,
                    points=float(points),fga=20.,fgm=10.,fg3a=5.,fta=4.))
            teams.append(dict(game_id=str(day),game_date=when,team_id=team,opponent_team_id=opp,
                pts=float(total),opp_pts=float(total),fga=100.,fta=20.,oreb=10.,tov=12.,fg3a=25.,fg3m=10.,opp_fga=None))
    return {"players":players,"teams":teams}


def test_scoring_and_context_features_exclude_target_and_future_results():
    raw=fixture(); before,_=build_features(raw)
    changed=deepcopy(raw)
    for p in changed["players"]:
        if int(p["game_id"])>=12:
            p["points"]+=50;p["fga"]+=10;p["minutes"]+=0
    for t in changed["teams"]:
        if int(t["game_id"])>=12:
            t["pts"]+=250;t["opp_pts"]+=250;t["fga"]+=50
    after,_=build_features(changed)
    cols=HISTORY+sum(FAMILIES.values(),[])
    a=before.loc[before.game_id.eq("12"),cols].to_numpy(dtype=float)
    b=after.loc[after.game_id.eq("12"),cols].to_numpy(dtype=float)
    assert np.allclose(a,b,equal_nan=True)
    # The next game's feature history is allowed to incorporate that outcome.
    assert not np.allclose(before.loc[before.game_id.eq("13"),"p10"],after.loc[after.game_id.eq("13"),"p10"])


def test_team_schedule_rest_is_separate_from_player_last_appearance():
    raw=fixture()
    # An appearance assigned to a replacement identity leaves the team's game
    # and scores intact while the original player's own rest becomes longer.
    row=next(p for p in raw["players"] if p["player_id"]==10 and p["game_id"]=="11")
    row["player_id"]=999;row["name"]="Replacement"
    frame,_=build_features(raw)
    row=frame.loc[frame.player_id.eq(10)&frame.game_id.eq("12")].iloc[0]
    assert row.team_b2b==1 and row.team_rest==0 and row.player_rest==1
    assert row.three_in_four==4 and row.four_in_six==6


def test_opponent_features_come_from_opposing_team_not_player_own_team():
    raw=fixture()
    # Before game 12, team 2 allowed unusually many threes in game 11.
    team=next(t for t in raw["teams"] if t["team_id"]==1 and t["game_id"]=="11")
    team["fg3a"]=80;team["fg3m"]=30
    frame,_=build_features(raw)
    a=frame.loc[frame.game_id.eq("12")&frame.team_id.eq(1)].iloc[0]
    b=frame.loc[frame.game_id.eq("12")&frame.team_id.eq(2)].iloc[0]
    assert a.opp_three_rate10>b.opp_three_rate10
