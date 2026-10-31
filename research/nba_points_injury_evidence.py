"""Archive free official NBA injury reports and join evidence by actual observation time.

No sportsbook requests, projection adjustments, or assumed participation probabilities.
"""
from __future__ import annotations
import argparse
from collections import Counter,defaultdict
from datetime import date,datetime,timezone,timedelta
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urlparse
from zoneinfo import ZoneInfo
import hashlib,io,json,re
import requests

from model.nba_points_ladder import player_key,team_key
from research.nba_points_forward import aware,digest,write_new
from research.nba_points_market_benchmarks import choose

VERSION='nba-injury-evidence-v1'
STATUSES={'Available','Probable','Questionable','Doubtful','Out'}
TEAM_NAMES={'ATL':'Atlanta Hawks','BOS':'Boston Celtics','BKN':'Brooklyn Nets','CHA':'Charlotte Hornets','CHI':'Chicago Bulls',
 'CLE':'Cleveland Cavaliers','DAL':'Dallas Mavericks','DEN':'Denver Nuggets','DET':'Detroit Pistons','GSW':'Golden State Warriors',
 'HOU':'Houston Rockets','IND':'Indiana Pacers','LAC':'Los Angeles Clippers','LAL':'Los Angeles Lakers','MEM':'Memphis Grizzlies',
 'MIA':'Miami Heat','MIL':'Milwaukee Bucks','MIN':'Minnesota Timberwolves','NOP':'New Orleans Pelicans','NYK':'New York Knicks',
 'OKC':'Oklahoma City Thunder','ORL':'Orlando Magic','PHI':'Philadelphia 76ers','PHX':'Phoenix Suns','POR':'Portland Trail Blazers',
 'SAC':'Sacramento Kings','SAS':'San Antonio Spurs','TOR':'Toronto Raptors','UTA':'Utah Jazz','WAS':'Washington Wizards'}

def clean(text):return ' '.join(text.split())

def url_time(url):
    p=urlparse(url)
    if p.scheme!='https' or p.hostname!='ak-static.cms.nba.com':raise ValueError('only official HTTPS NBA injury PDF sources are allowed')
    m=re.fullmatch(r'/referee/injury/Injury-Report_(\d{4}-\d{2}-\d{2})_(\d{2})_(\d{2})(AM|PM)\.pdf',p.path)
    if not m or p.query or p.fragment:raise ValueError('unrecognized official injury report URL')
    day,hour,minute,period=m.groups()
    local=datetime.strptime(day+' '+hour+':'+minute+period,'%Y-%m-%d %I:%M%p').replace(tzinfo=ZoneInfo('America/New_York'))
    if local.utcoffset()!=local.replace(fold=1).utcoffset():raise ValueError('ambiguous or nonexistent Eastern publication time')
    return local.astimezone(timezone.utc)

def parse_cells(pages):
    """Rows are assigned by status anchors and physical PDF coordinates, not text order."""
    fields={'date':0.,'time':119.5,'matchup':199.9,'team':264.2,'player':424.9,'status':585.7,'reason':666.0}
    current={'game_date':None,'matchup':None,'team':None,'game_time_text':None};rows=[];submissions=[]
    for page_no,chunks in enumerate(pages,1):
        headers=[c['y'] for c in chunks if c['text']=='Current' and abs(c['x']-fields['status'])<5]
        data=[c for c in chunks if 70<c['y']<515 and (not headers or c['y']<min(headers)-1)]
        # A different table layout must not quietly assign a status to another player.
        anchors=sorted({round(c['y'],1) for c in data if fields['status']<=c['x']<fields['reason'] and c['text'] in STATUSES},reverse=True)
        pending_y={round(c['y'],1) for c in data if fields['player']<=c['x'] and c['text']=='NOT'}
        anchors=sorted(set(anchors)|pending_y,reverse=True)
        if not anchors and any(c['x']>=fields['player'] for c in data):raise ValueError('unrecognized injury table layout')
        for i,y in enumerate(anchors):
            upper=(anchors[i-1]+y)/2 if i else float('inf');lower=(y+anchors[i+1])/2 if i+1<len(anchors) else -float('inf')
            band=[c for c in data if lower<c['y']<=upper]
            names=list(fields);cells={}
            for j,key in enumerate(names):
                tokens=[c for c in band if fields[key]<=c['x']<(fields[names[j+1]] if j+1<len(names) else float('inf'))]
                cells[key]=clean(' '.join(c['text'] for c in sorted(tokens,key=lambda c:(-round(c['y'],1),c['x']))))
            if cells['date']:
                current['game_date']=datetime.strptime(cells['date'],'%m/%d/%Y').date().isoformat()
            if cells['time']:current['game_time_text']=cells['time']
            if cells['matchup']:
                if not re.fullmatch(r'[A-Z]{3}@[A-Z]{3}',cells['matchup']):raise ValueError('unrecognized report matchup')
                current['matchup']=cells['matchup'];current['team']=None
            if cells['team']:current['team']=cells['team']
            if not all(current[k] for k in ['game_date','matchup','team']):raise ValueError('injury row lacks date/game/team context')
            abbrs=current['matchup'].split('@')
            if any(a not in TEAM_NAMES for a in abbrs) or team_key(current['team']) not in {team_key(TEAM_NAMES[a]) for a in abbrs}:raise ValueError('report team does not belong to matchup')
            submitted='not_yet_submitted' if 'NOT YET SUBMITTED' in ' '.join(cells.values()) else 'submitted'
            submissions.append({**current,'submission':submitted})
            if submitted=='not_yet_submitted':continue
            if cells['status'] not in STATUSES or ',' not in cells['player']:raise ValueError('invalid player/status row')
            parts=[clean(v) for v in cells['player'].split(',')]
            player=clean(parts[-1]+' '+' '.join(parts[:-1]))
            rows.append({**current,'player':player,'player_key':player_key(player),'published_name':cells['player'],
                         'status':cells['status'],'reason':cells['reason'],'page':page_no})
    keys=[(r['game_date'],r['matchup'],team_key(r['team']),r['player_key']) for r in rows]
    if len(keys)!=len(set(keys)):raise ValueError('duplicate report player identity')
    if not rows and not submissions:raise ValueError('no recognized injury rows or team submissions')
    unique={ (r['game_date'],r['matchup'],team_key(r['team'])):r for r in submissions}
    return rows,list(unique.values())

def parse_pdf(data,source_url):
    from pypdf import PdfReader
    reader=PdfReader(io.BytesIO(data));pages=[]
    for page in reader.pages:
        chunks=[]
        def visitor(text,cm,tm,font,size):
            text=clean(text)
            if text:chunks.append({'text':text,'x':tm[4]*cm[0]+tm[5]*cm[2]+cm[4],'y':tm[4]*cm[1]+tm[5]*cm[3]+cm[5]})
        page.extract_text(visitor_text=visitor);pages.append(chunks)
    heading=clean(reader.pages[0].extract_text())
    m=re.search(r'Injury Report:\s*(\d{2}/\d{2}/\d{2})\s+(\d{2}:\d{2})\s*(AM|PM)',heading)
    if not m:raise ValueError('missing official publication header')
    issued=datetime.strptime(' '.join(m.groups()),'%m/%d/%y %I:%M %p').replace(tzinfo=ZoneInfo('America/New_York')).astimezone(timezone.utc)
    if issued!=url_time(source_url):raise ValueError('PDF publication header and URL disagree')
    rows,submissions=parse_cells(pages)
    return {'published_at':issued.isoformat(),'rows':rows,'team_submissions':submissions,'page_count':len(pages)}

class Links(HTMLParser):
    def __init__(self):super().__init__();self.urls=[]
    def handle_starttag(self,tag,attrs):
        if tag=='a':self.urls.extend(v for k,v in attrs if k=='href' and v)

def latest_url(html,target,now):
    links=Links();links.feed(html);choices=[]
    for url in links.urls:
        try:published=url_time(url)
        except ValueError:continue
        if published.astimezone(ZoneInfo('America/New_York')).date()==target and published<=aware(now):choices.append((published,url))
    if not choices:raise ValueError('no published official report for requested date; use an explicit archived report URL')
    return max(choices)[1]

def fetch_latest(target,now,session):
    """Use the public index, or at most four already-published quarter-hour URLs."""
    now=aware(now);year=target.year if target.month>=7 else target.year-1
    index_url=f'https://official.nba.com/nba-injury-report-{year}-{str(year+1)[-2:]}-season/'
    discovery={'index_url':index_url,'pdf_attempts':[]};index=None
    try:
        response=session.get(index_url,timeout=20);discovery['index_status']=response.status_code;response.raise_for_status();index=response.text
        url=latest_url(index,target,now);candidates=[url]
    except (ValueError,requests.RequestException) as e:
        discovery['index_error']=str(e);zone=ZoneInfo('America/New_York')
        end=datetime.combine(target+timedelta(days=1),datetime.min.time(),tzinfo=zone).astimezone(timezone.utc)-timedelta(minutes=15)
        at=min(now,end);at=at.replace(minute=(at.minute//15)*15,second=0,microsecond=0);candidates=[]
        for i in range(4):
            local=(at-timedelta(minutes=15*i)).astimezone(zone)
            if local.date()!=target:continue
            candidate='https://ak-static.cms.nba.com/referee/injury/Injury-Report_'+local.strftime('%Y-%m-%d_%I_%M%p')+'.pdf'
            try:url_time(candidate)
            except ValueError:continue
            candidates.append(candidate)
    for url in candidates:
        response=session.get(url,timeout=25);discovery['pdf_attempts'].append({'url':url,'status':response.status_code})
        if response.status_code in (403,404):continue
        response.raise_for_status()
        if response.content.startswith(b'%PDF'):return url,response.content,index,discovery
        raise ValueError('official report response is not a PDF')
    raise ValueError('no available report in bounded archive lookup; retry later or specify --report-url')

def archive(data,url,observed,root,*,receipt=None,index_html=None,discovery=None):
    observed=aware(observed);now=datetime.now(timezone.utc)
    if observed>now:raise ValueError('observation time is in the future')
    if receipt is None and abs((now-observed).total_seconds())>300:raise ValueError('old observation times require a matching archived capture receipt')
    if receipt and (receipt.get('source_sha256')!=hashlib.sha256(data).hexdigest() or receipt.get('source_url')!=url or aware(receipt['captured_at'])!=observed):raise ValueError('capture receipt does not match source/time')
    folder=root/(now.strftime('%Y%m%dT%H%M%S%fZ')+'-'+hashlib.sha256(data).hexdigest()[:12]);folder.mkdir(parents=True,exist_ok=False)
    (folder/'report.pdf').write_bytes(data)
    if index_html is not None:(folder/'index.html').write_text(index_html,encoding='utf-8')
    if receipt:write_new(folder/'capture-receipt.json',receipt)
    try:
        parsed=parse_pdf(data,url)
        if aware(parsed['published_at'])>observed:raise ValueError('publication is later than observation')
        record={'version':VERSION,'source_url':url,'source_sha256':hashlib.sha256(data).hexdigest(),
                'observed_at':observed.isoformat(),'ingested_at':now.isoformat(),
                'observation_provenance':'imported_capture_receipt' if receipt else 'fresh_capture','discovery':discovery,**parsed}
        record['sha256']=digest(record);write_new(folder/'observation.json',record)
    except Exception as e:
        write_new(folder/'parse-error.json',{'error':str(e),'source_url':url,'observed_at':observed.isoformat()});raise
    return folder,record

def load_archive(root):
    records=[]
    for p in sorted(root.rglob('observation.json')) if root.exists() else []:
        r=json.loads(p.read_text(encoding='utf-8'))
        if r.get('version')!=VERSION or digest({k:v for k,v in r.items() if k!='sha256'})!=r.get('sha256'):raise ValueError('injury evidence digest/version mismatch')
        if hashlib.sha256(p.with_name('report.pdf').read_bytes()).hexdigest()!=r['source_sha256']:raise ValueError('archived source PDF changed')
        if aware(r['published_at'])!=url_time(r['source_url']) or aware(r['published_at'])>aware(r['observed_at']):raise ValueError('invalid archived publication timeline')
        records.append(r)
    return records

def relevant(report,c,day):
    pair={team_key(c['home_team']),team_key(c['away_team'])}
    return [r for r in report['rows'] if r['game_date']==day and {team_key(TEAM_NAMES[a]) for a in r['matchup'].split('@')}==pair]

def availability(report,target):
    rows=[r for r in report['rows'] if r['game_date']==str(target)]
    counts=Counter(r['player_key'] for r in rows)
    result={r['player_key']:{'status':r['status'],'player':r['player'],'team':r['team'],'game_date':r['game_date'],
            'captured_at':report['observed_at'],'published_at':report['published_at'],'source_url':report['source_url'],
            'report_sha256':report['sha256'],'reason':r['reason']} for r in rows if counts[r['player_key']]==1}
    return result,sorted(key for key,n in counts.items() if n>1)

def state_at(reports,c,day,cutoff):
    cutoff=aware(cutoff)
    eligible=[r for r in reports if aware(r['observed_at'])<cutoff and aware(r['published_at'])<=aware(r['observed_at']) and any(s['game_date']==day for s in r['team_submissions'])]
    if not eligible:return {'status':'unknown','reason':'no_report_observed_before_cutoff'}
    report=max(eligible,key=lambda r:(aware(r['published_at']),aware(r['observed_at'])))
    game_rows=relevant(report,c,day);hits=[r for r in game_rows if r['player_key']==c['player_key']]
    pair={team_key(c['home_team']),team_key(c['away_team'])}
    submission=[r for r in report['team_submissions'] if r['game_date']==day and {team_key(TEAM_NAMES[a]) for a in r['matchup'].split('@')}==pair]
    reason='game_absent_from_latest_observed_report' if not submission else 'game_has_unsubmitted_team_report' if any(r['submission']=='not_yet_submitted' for r in submission) else 'not_listed_in_latest_observed_report'
    result={'status':'unknown','reason':reason,'report_sha256':report['sha256'],'game_team_submissions':submission,
            'published_at':report['published_at'],'observed_at':report['observed_at'],'source_url':report['source_url'],
            'same_game_reported_out':[{'player':r['player'],'team':r['team'],'reason':r['reason']} for r in game_rows if r['status']=='Out']}
    if len(hits)==1:result.update(status=hits[0]['status'],reason=hits[0]['reason'],team=hits[0]['team'])
    elif len(hits)>1:result['reason']='ambiguous_player_identity'
    return result

def build(snapshot,grade,benchmarks,diagnostics,reports,history):
    if grade['snapshot_sha256']!=snapshot['sha256'] or benchmarks['snapshot_sha256']!=snapshot['sha256'] or diagnostics['snapshot_sha256']!=snapshot['sha256']:raise ValueError('mixed frozen snapshots')
    by_id={(str(r['event_id']),r['player_key']):r for r in benchmarks['rows']};diag={(str(r['event_id']),r['player_key']):r for r in diagnostics['rows']};result=[]
    for c in snapshot['candidates']:
        key=(str(c['event_id']),c['player_key']);b=by_id[key];states={}
        cutoffs={'forecast_created':snapshot['created_at'],'contest_lock_decision':snapshot['lock_at']}
        cutoffs.update({k:b[k]['captured_at'] for k in ['frozen_line','contest_lock_proxy','game_close_proxy'] if b[k].get('captured_at')})
        for name,cutoff in cutoffs.items():states[name]=state_at(reports,c,snapshot['game_date'],cutoff)
        start=aware(b['scheduled_start']) if b.get('scheduled_start') else None
        timeline=[]
        for report in reports:
            for r in relevant(report,c,snapshot['game_date']):
                if r['player_key']==c['player_key']:
                    timeline.append({**r,'published_at':report['published_at'],'observed_at':report['observed_at'],'report_sha256':report['sha256'],
                                     'source_url':report['source_url'],'observed_before_game':aware(report['observed_at'])<start if start else None,
                                     'published_before_game':aware(report['published_at'])<start if start else None})
        quotes=[]
        if start:
            grouped=defaultdict(list)
            for r in history:
                if r.get('sport')=='nba' and r.get('market')=='player_points' and str(r['game_date'])==snapshot['game_date'] and str(r['event_id'])==key[0] and player_key(r['player'])==key[1] and (team_key(r['home_team_name']),team_key(r['away_team_name']))==(team_key(c['home_team']),team_key(c['away_team'])):grouped[aware(r['captured_at'])].append(r)
            for stamp,rs in sorted(grouped.items()):
                q=choose(rs,start,start)
                if q.get('line') is not None:quotes.append({'captured_at':stamp.isoformat(),'line':q['line'],'source_row_ids':q['source_row_ids'],
                                                          'injury_state':state_at(reports,c,snapshot['game_date'],stamp)})
        actual=diag[key].get('actual',{});forecasts=c.get('opportunity_forecasts',{})
        result.append({'event_id':c['event_id'],'player_key':c['player_key'],'player':c['player'],'states':states,
                       'injury_timeline':sorted(timeline,key=lambda r:(r['published_at'],r['observed_at'])),'quote_timeline':quotes,
                       'line_movement':b['game_close_proxy']['line']-b['frozen_line']['line'] if b['game_close_proxy'].get('line') is not None else None,
                       'outcome_status':b['outcome_status'],'actual_points':b['actual_points'],'actual_minutes':actual.get('minutes'),
                       'actual_fga':actual.get('fga'),'forecast_minutes':forecasts.get('minutes'),'forecast_fga':forecasts.get('fga')})
    summary={key:dict(Counter(r['states'].get(key,{'status':'unknown'})['status'] for r in result)) for key in ['forecast_created','contest_lock_decision','frozen_line','contest_lock_proxy','game_close_proxy']}
    output={'version':VERSION,'created_at':datetime.now(timezone.utc).isoformat(),'snapshot_sha256':snapshot['sha256'],
            'report_digests':[r['sha256'] for r in reports],'summary':summary,'rows':result,
            'interpretation':'Status uses reports actually observed before each cutoff. Later downloads remain retrospective. Not listed is unknown, not confirmed available. Line movement is descriptive, not an injury effect or a projection adjustment. Same-game absences are grouped by reported team; teammate identity is not inferred for unlisted players.'}
    output['sha256']=digest(output);return output

def markdown(report):
    rows=[r for r in report['rows'] if r['injury_timeline']]
    lines=['## Injury evidence and market timing','','Unknown means unavailable evidence, not confirmed participation. Original forecasts are unchanged.','',
           '| Player | Status at forecast | Status at contest lock | Status at last pre-tip quote | Line movement | Actual points |','|---|---|---|---|---:|---:|']
    for r in rows:
        states=r['states'];fmt=lambda v:'Unavailable' if v is None else f'{v:g}'
        lines.append('| '+' | '.join([r['player'],states['forecast_created']['status'],states['contest_lock_decision']['status'],states.get('game_close_proxy',{'status':'unknown'})['status'],fmt(r['line_movement']),fmt(r['actual_points'])])+' |')
    if not rows:lines+=['','No matching archived injury observations.']
    lines+=['','Full publication/observation timestamps, status and quote timelines, game-context absences and observed opportunities are in injury-market-evidence.json. Reports retrieved after tip cannot become pregame evidence. No causal effect, probabilities or automatic scoring bonuses are inferred.','']
    return '\n'.join(lines)

def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--date',type=date.fromisoformat,required=True)
    p.add_argument('--archive-root',type=Path,default=Path('artifacts/nba-injury-evidence'))
    p.add_argument('--report-url');p.add_argument('--pdf',type=Path);p.add_argument('--capture-receipt',type=Path)
    p.add_argument('--availability-output',type=Path,help='Exclusive current-day evidence for the existing ghost --availability input')
    args=p.parse_args();index=None;receipt=None;discovery=None
    if args.capture_receipt:
        if not args.pdf:p.error('--capture-receipt requires --pdf')
        receipt=json.loads(args.capture_receipt.read_text(encoding='utf-8'));url=args.report_url or receipt['source_url'];data=args.pdf.read_bytes();observed=aware(receipt['captured_at'])
    else:
        import requests
        url=args.report_url
        if not url:
            if args.pdf:p.error('--pdf requires --report-url or --capture-receipt')
            with requests.Session() as session:url,data,index,discovery=fetch_latest(args.date,datetime.now(timezone.utc),session)
        else:
            url_time(url)
            if args.pdf:data=args.pdf.read_bytes()
            else:
                response=requests.get(url,timeout=30);response.raise_for_status();data=response.content
        observed=datetime.now(timezone.utc)
    folder,report=archive(data,url,observed,args.archive_root,receipt=receipt,index_html=index,discovery=discovery)
    if args.date.isoformat() not in {r['game_date'] for r in report['team_submissions']}:raise ValueError('requested slate date absent from report')
    evidence,blocked=availability(report,args.date)
    if args.availability_output:write_new(args.availability_output,evidence)
    print(json.dumps({'archive':str(folder),'published_at':report['published_at'],'observed_at':report['observed_at'],'rows':len(report['rows']),'sha256':report['sha256'],
                      'availability_output':str(args.availability_output) if args.availability_output else None,'ambiguous_names_not_exported':blocked}))

if __name__=='__main__':main()
