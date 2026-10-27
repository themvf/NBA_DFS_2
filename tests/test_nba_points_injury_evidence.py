from copy import deepcopy
from datetime import datetime,timezone
import hashlib
import pytest
from research import nba_points_injury_evidence as m

def at(hour,minute=0,day=26):return datetime(2026,10,day,hour,minute,tzinfo=timezone.utc).isoformat()
def cell(text,x,y):return {'text':text,'x':x,'y':y}
def source(status='Questionable',published=None,observed=None,name='Anthony Edwards'):
    row={'game_date':'2026-10-26','matchup':'MIN@CLE','team':'Minnesota Timberwolves','player':name,'player_key':m.player_key(name),'status':status,'reason':'Knee'}
    return {'published_at':published or at(20,45),'observed_at':observed or at(21,5),'sha256':status+str(observed),'source_url':'official',
            'rows':[row],'team_submissions':[{'game_date':'2026-10-26','matchup':'MIN@CLE','team':'Minnesota Timberwolves','submission':'submitted'}]}
def candidate():return {'player_key':'anthonyedwards','home_team':'Cleveland Cavaliers','away_team':'Minnesota Timberwolves'}

def test_pdf_physical_rows_wrapped_reason_and_cross_page_team_context():
    page1=[cell('Current',585.7,480),cell('10/26/2026',24,450),cell('07:00 (ET)',120,450),cell('MIN@CLE',200,450),cell('Minnesota Timberwolves',265,450),
           cell('Edwards,',426,450),cell('Anthony',470,450),cell('Questionable',586,450),cell('Injury/Illness - Right Knee',667,457),cell('Management',667,443)]
    page2=[cell('DiVincenzo,',426,500),cell('Donte',490,500),cell('Out',586,500),cell('Achilles',667,500)]
    rows,teams=m.parse_cells([list(reversed(page1)),page2])
    assert rows[0]['player']=='Anthony Edwards' and rows[0]['reason']=='Injury/Illness - Right Knee Management'
    assert rows[1]['team']=='Minnesota Timberwolves' and rows[1]['page']==2 and len(teams)==1

def test_pending_team_submission_never_becomes_available():
    p=[cell('10/26/2026',24,450),cell('MIN@CLE',200,450),cell('Minnesota Timberwolves',265,450),cell('Edwards, Anthony',426,450),cell('Out',586,450),
       cell('Cleveland Cavaliers',265,400),cell('NOT',426,400),cell('YET',450,400),cell('SUBMITTED',475,400)]
    rows,teams=m.parse_cells([p]);assert len(rows)==1 and teams[-1]['submission']=='not_yet_submitted'
    report=source();report['rows']=[];report['team_submissions']=teams
    state=m.state_at([report],candidate(),'2026-10-26',at(22))
    assert state['status']=='unknown' and state['reason']=='game_has_unsubmitted_team_report'
    pending=[cell('10/26/2026',24,400),cell('MIN@CLE',200,400),cell('Cleveland Cavaliers',265,400),cell('NOT',426,400),cell('YET',450,400),cell('SUBMITTED',475,400)]
    rows,teams=m.parse_cells([pending]);assert rows==[] and teams[0]['submission']=='not_yet_submitted'

@pytest.mark.parametrize('problem',['wrong_team','missing_status','duplicate'])
def test_layout_or_identity_errors_fail_closed(problem):
    p=[cell('10/26/2026',24,450),cell('MIN@CLE',200,450),cell('Miami Heat' if problem=='wrong_team' else 'Minnesota Timberwolves',265,450),
       cell('Edwards, Anthony',426,450),cell('Unknown' if problem=='missing_status' else 'Out',586,450)]
    if problem=='duplicate':p+=[cell('Edwards, Anthony',426,400),cell('Out',586,400)]
    with pytest.raises(ValueError):m.parse_cells([p])

def test_observation_time_blocks_retrospective_available_status():
    early=source();late=source('Available',at(22,45),at(12,day=27))
    state=m.state_at([early,late],candidate(),'2026-10-26',at(23))
    assert state['status']=='Questionable'
    assert m.state_at([late],candidate(),'2026-10-26',at(23))['status']=='unknown'
    late['observed_at']=at(22,46)
    assert m.state_at([early,late],candidate(),'2026-10-26',at(23))['status']=='Available'

def test_exact_cutoff_and_future_publication_excluded():
    r=source(observed=at(22));assert m.state_at([r],candidate(),'2026-10-26',at(22))['status']=='unknown'
    r=source(published=at(23),observed=at(22));assert m.state_at([r],candidate(),'2026-10-26',at(23))['status']=='unknown'

def test_latest_report_missing_player_does_not_carry_forward_or_imply_active():
    old=source();new=source('Out',at(21,15),at(21,16),name='Donte DiVincenzo')
    state=m.state_at([old,new],candidate(),'2026-10-26',at(22))
    assert state['status']=='unknown' and state['same_game_reported_out'][0]['player']=='Donte DiVincenzo'

def test_same_name_other_matchup_cannot_join():
    r=source();r['rows'][0].update(matchup='MIA@DAL',team='Miami Heat')
    assert m.state_at([r],candidate(),'2026-10-26',at(23))['status']=='unknown'

def test_index_selects_latest_published_official_pdf_only():
    a='https://ak-static.cms.nba.com/referee/injury/Injury-Report_2026-10-26_04_45PM.pdf'
    b=a.replace('04_45PM','06_45PM')
    html=f'<a href="{b}">future</a><a href="{a}">now</a><a href="https://evil.test/report.pdf">bad</a>'
    assert m.latest_url(html,datetime(2026,10,26).date(),at(22))==a
    with pytest.raises(ValueError):m.url_time('http://ak-static.cms.nba.com/referee/injury/Injury-Report_2026-10-26_04_45PM.pdf')
    with pytest.raises(ValueError,match='ambiguous'):m.url_time('https://ak-static.cms.nba.com/referee/injury/Injury-Report_2026-11-01_01_15AM.pdf')

def test_complete_builder_keeps_status_quote_times_and_retrospective_timeline():
    c={**candidate(),'player':'Anthony Edwards','event_id':'market','captured_at':at(15),'predictions':{'market_line':28.5,'model':28.3},'opportunity_forecasts':{'minutes':31.4,'fga':18.2}}
    s={'sha256':'original','game_date':'2026-10-26','created_at':at(15,33),'lock_at':at(23),'candidates':[c]}
    bm={'snapshot_sha256':'original','rows':[{'event_id':'market','player_key':c['player_key'],'scheduled_start':at(23),'outcome_status':'graded','actual_points':47,
         'frozen_line':{'line':28.5,'captured_at':at(15)},'contest_lock_proxy':{'line':27.5,'captured_at':at(22,7)},'game_close_proxy':{'line':27.5,'captured_at':at(22,7)}}]}
    d={'snapshot_sha256':'original','rows':[{'event_id':'market','player_key':c['player_key'],'actual':{'minutes':41,'fga':34}}]};before=deepcopy(s)
    reports=[source(),source('Available',at(22,45),at(12,day=27))]
    q={'event_id':'market','sport':'nba','market':'player_points','game_date':'2026-10-26','player':c['player'],'home_team_name':c['home_team'],'away_team_name':c['away_team'],'captured_at':at(22,7),'commence_time':at(23,10),'books':{'dk':{'line':27.5,'over':-110,'under':-110}}}
    out=m.build(s,{'snapshot_sha256':'original'},bm,d,reports,[q]);r=out['rows'][0]
    assert r['states']['forecast_created']['status']=='unknown' and r['states']['game_close_proxy']['status']=='Questionable'
    assert r['injury_timeline'][-1]['observed_before_game'] is False and r['injury_timeline'][-1]['published_before_game'] is True
    assert r['line_movement']==-1 and r['quote_timeline'][0]['injury_state']['status']=='Questionable' and s==before
    assert r['actual_fga']==34 and 'Retrospective' not in r['states']['forecast_created']['status']
    with pytest.raises(ValueError):m.build(s,{'snapshot_sha256':'other'},bm,d,reports,[])

def test_archive_forbids_backdating_and_requires_matching_receipt(tmp_path,monkeypatch):
    with pytest.raises(ValueError,match='receipt'):m.archive(b'pdf','https://example.org',at(21),tmp_path)
    receipt={'source_sha256':'wrong','source_url':'url','captured_at':at(21)}
    with pytest.raises(ValueError,match='receipt'):m.archive(b'pdf','url',at(21),tmp_path,receipt=receipt)
    url='https://ak-static.cms.nba.com/referee/injury/Injury-Report_2026-10-26_04_45PM.pdf';data=b'fakepdf'
    receipt={'source_sha256':hashlib.sha256(data).hexdigest(),'source_url':url,'captured_at':at(21)}
    monkeypatch.setattr(m,'parse_pdf',lambda data,url:{'published_at':at(20,45),'rows':[{'game_date':'2026-10-26'}],'team_submissions':[]})
    folder,report=m.archive(data,url,at(21),tmp_path,receipt=receipt)
    assert report['observation_provenance']=='imported_capture_receipt' and m.load_archive(tmp_path)[0]['observed_at']==at(21)
    (folder/'report.pdf').write_bytes(b'tampered')
    with pytest.raises(ValueError,match='changed'):m.load_archive(tmp_path)

def test_index_block_uses_bounded_official_pdf_fallback():
    import requests
    class Response:
        def __init__(self,status,content=b'',text=''):self.status_code=status;self.content=content;self.text=text
        def raise_for_status(self):
            if self.status_code>=400:raise requests.HTTPError(str(self.status_code))
    class Session:
        def __init__(self,success=True):self.urls=[];self.success=success
        def get(self,url,timeout):
            self.urls.append(url)
            if 'official.nba.com/' in url:return Response(403)
            if self.success and '_04_45PM.pdf' in url:return Response(200,b'%PDFsample')
            return Response(404)
    session=Session();url,data,index,discovery=m.fetch_latest(datetime(2026,10,26).date(),at(21,7),session)
    assert url.endswith('_04_45PM.pdf') and data.startswith(b'%PDF') and index is None and discovery['index_status']==403
    assert len(session.urls)==3 and all('nba.com/' in u for u in session.urls)
    session=Session(False)
    with pytest.raises(ValueError,match='bounded'):m.fetch_latest(datetime(2026,10,26).date(),at(21,7),session)
    assert len(session.urls)==5

def test_availability_export_preserves_source_and_rejects_collisions_and_wrong_day():
    r=source('Out');evidence,blocked=m.availability(r,'2026-10-26')
    assert evidence['anthonyedwards']['status']=='Out' and evidence['anthonyedwards']['captured_at']==r['observed_at']
    assert evidence['anthonyedwards']['source_url']==r['source_url'] and not blocked
    assert m.availability(r,'2026-10-27')==({},[])
    r['rows'].append({**r['rows'][0],'team':'Cleveland Cavaliers'})
    assert m.availability(r,'2026-10-26')==({},['anthonyedwards'])

def test_export_flows_to_existing_freeze_without_numerical_projection_changes():
    from datetime import date
    from research.nba_points_forward import build_snapshot,aware
    from research.nba_points_ghost_v2 import freeze
    raw=[{'event_id':'e','game_date':date(2026,10,26),'commence_time':aware(at(23)), 'captured_at':aware(at(20)),
          'home_team_name':'Boston Celtics','away_team_name':'Miami Heat','player':name,'market':'player_points',
          'books':{'dk':{'line':line,'over':-110,'under':-110}}} for name,line in [('One',40),('Two',30),('Three',20),('Four',10)]]
    market=build_snapshot(raw,date(2026,10,26),aware(at(23)),aware(at(21,30)))
    report=source('Out',name='One');report['rows'][0].update(team='Boston Celtics',matchup='MIA@BOS')
    evidence,_=m.availability(report,'2026-10-26')
    plain=freeze(market,{}, {},{},aware(at(22)))
    checked=freeze(market,{}, {},{},aware(at(22)),availability=evidence)
    assert [c['predictions'] for c in plain['candidates']]==[c['predictions'] for c in checked['candidates']]
    one=next(c for c in checked['candidates'] if c['player']=='One')
    assert one['availability']['status']=='Out' and one['ceiling_watch']['status']=='reported_out'
    assert checked['leader_selections']['strategies']['market_line']['eligible_count']==3
    assert all(p['player']!='One' for p in checked['leader_selections']['strategies']['market_line']['selected'])
