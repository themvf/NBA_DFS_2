"""Read-only audit of frozen pre-first-tip game totals and actual scoring leaders."""
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import urllib.request

import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
from config import load_config


def main():
    out = Path('artifacts/nba-vegas-leaders-20261024')
    out.mkdir(exist_ok=True)
    raw = json.loads(Path('artifacts/nba-points-trends-20261024/inputs.json').read_text())
    with psycopg2.connect(load_config().database_url, cursor_factory=RealDictCursor) as c:
        c.set_session(readonly=True)
        with c.cursor() as q:
            q.execute("select h.*,m.game_id from game_odds_history h left join nba_matchups m on m.id=h.matchup_id where h.sport='nba' order by h.captured_at,h.id")
            odds = q.fetchall()
    (out/'odds.json').write_text(json.dumps(odds, default=str))
    odds = pd.DataFrame(odds)
    odds['game_date'] = odds.game_date.astype(str)
    odds['captured_at'] = pd.to_datetime(odds.captured_at, utc=True)
    p,t = pd.DataFrame(raw['players']),pd.DataFrame(raw['teams'])
    # Only dates with a capture on/before the actual local game day can be pregame.
    dates=sorted(set(odds.loc[(odds.captured_at.dt.date.astype(str)<=odds.game_date) & (odds.game_date<='2026-04-12'),'game_date']))
    def fetch(d):
        path=out/f'espn-{d}.json'
        if path.exists(): return d,json.loads(path.read_text())
        url='https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard?dates='+d.replace('-','')
        with urllib.request.urlopen(url,timeout=30) as response: data=json.load(response)
        path.write_text(json.dumps(data))
        return d,data
    schedules=dict(ThreadPoolExecutor(max_workers=4).map(fetch,dates))
    aliases={'GS':'GSW','NO':'NOP','NY':'NYK','SA':'SAS','UTAH':'UTA','WSH':'WAS','PHO':'PHX'}
    def abbr(x): return aliases.get(x,x)
    rows=[]; coverage=[]; full=[]
    for d,s in schedules.items():
        events=s['events']
        if not events: continue
        cutoff=min(pd.Timestamp(e['competitions'][0]['date']) for e in events)
        selected=[]
        for e in events:
            comp=e['competitions'][0]
            home=next(x for x in comp['competitors'] if x['homeAway']=='home')
            away=next(x for x in comp['competitors'] if x['homeAway']=='away')
            team=t[(t.game_date==d)&t.is_home&(t.team_abbreviation==abbr(home['team']['abbreviation']))&(t.opponent_abbreviation==abbr(away['team']['abbreviation']))]
            assert len(team)==1,(d,home,away)
            game=team.iloc[0]
            h=odds[(odds.game_id==game.game_id)&(odds.captured_at<cutoff)&odds.vegas_total.notna()]
            if h.empty: continue
            line=h.iloc[-1]
            names={x['team_id']:x['name'] for x in raw['team_names']}
            canonical=lambda n: 'LA Clippers' if n=='Los Angeles Clippers' else n
            assert canonical(line.home_team_name)==names[game.team_id]
            assert canonical(line.away_team_name)==names[game.opponent_team_id]
            # Both independently sourced score totals must match before grading.
            assert game.pts==float(home['score']) and game.opp_pts==float(away['score'])
            selected.append({'date':d,'game_id':game.game_id,'matchup':abbr(away['team']['abbreviation'])+' @ '+abbr(home['team']['abbreviation']), 'total':float(line.vegas_total),'spread':float(line.home_spread) if pd.notna(line.home_spread) else None,'captured_at':str(line.captured_at),'cutoff':str(cutoff),'odds_id':int(line.id),'actual_game_total':float(game.pts+game.opp_pts)})
        coverage.append({'date':d,'scheduled_games':len(events),'covered_before_first_tip':len(selected),'cutoff':str(cutoff)})
        full.extend(selected)
        if len(selected)!=len(events) or len(events)<2: continue
        byid={x['game_id']:x for x in selected}
        players=p[(p.game_date==d)&p.minutes.gt(0)&p.points.notna()].copy()
        assert set(players.game_id)==set(byid)
        top_total=max(x['total'] for x in selected)
        high=[x for x in selected if x['total']==top_total]
        high_ids={x['game_id'] for x in high}
        leader=players[players.points==players.points.max()]
        # Fractional credit preserves a total of one leader and three top-three slots.
        third=players.points.nlargest(3).iloc[-1]
        above=players[players.points>third]; tied=players[players.points==third]
        top3_share=(above.game_id.isin(high_ids).sum()+(3-len(above))*tied.game_id.isin(high_ids).mean())/3
        rows.append({'date':d,'games':len(events),'highest_total_games':high,'leaders':[{'name':r['name'],'points':r['points'],'matchup':byid[r['game_id']]['matchup'],'total':byid[r['game_id']]['total']} for r in leader.to_dict('records')], 'leader_credit':float(leader.game_id.isin(high_ids).mean()),'top_three_share':float(top3_share),'random_game_baseline':len(high_ids)/len(events)})
    # Point residuals compare scoring with the player's own prior 20 appearances.
    p=p[p.minutes.gt(0)&p.points.notna()].sort_values(['game_date','game_id','player_id'])
    p['p20']=p.groupby('player_id').points.transform(lambda x:x.shift().rolling(20,min_periods=10).mean())
    joined=p.merge(pd.DataFrame(full),on='game_id',validate='many_to_one')
    joined['residual']=joined.points-joined.p20
    joined.to_csv(out/'player-game-evidence.csv',index=False)
    def corr(frame):
        return {'n':len(frame),'games':int(frame.game_id.nunique()),'total_vs_points':float(frame.total.corr(frame.points)),'total_vs_residual':float(frame.total.corr(frame.residual))}
    eligible=joined[joined.p20.notna() & joined.minutes.gt(0)]
    result={'coverage':coverage,'complete_slates':rows,'all_covered_games':full,'correlations_all_players':corr(eligible),'correlations_prior20_at_least20':corr(eligible[eligible.p20>=20]),'summary':{'slates':len(rows),'leader_credits':sum(x['leader_credit'] for x in rows),'mean_top_three_share':sum(x['top_three_share'] for x in rows)/len(rows) if rows else None,'mean_random_game_baseline':sum(x['random_game_baseline'] for x in rows)/len(rows) if rows else None}}
    games=joined.groupby('game_id').agg(total=('total','first'),max_individual_points=('points','max'),actual_game_total=('actual_game_total','first'))
    result['game_level_correlations']={'n':len(games),'total_vs_game_high_player_points':float(games.total.corr(games.max_individual_points)), 'total_vs_actual_game_total':float(games.total.corr(games.actual_game_total))}
    (out/'results.json').write_text(json.dumps(result,indent=2,default=str))
    print(json.dumps(result,indent=2,default=str))


if __name__=='__main__': main()
