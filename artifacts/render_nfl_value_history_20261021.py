"""Write a summary and complete per-slate Markdown drilldowns."""
import json
import os
import statistics as st
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo

ROOT=Path('artifacts/nfl-value-history-20261021').resolve()
x=json.loads((ROOT/'assessment.json').read_text(encoding='utf-8'))
inj=json.loads((ROOT/'injury-boundaries.json').read_text(encoding='utf-8'))
obs={(o['slate_key'],o['player_id'],o['source']):o for o in inj['observations']}
pages=ROOT/'slates'
pages.mkdir(exist_ok=True)
num=lambda n: '—' if n is None else f'{n:.2f}'
# Ownership for every case (2026-10-22): enrich_nfl_value_history_ownership_20261022.py.
own=json.loads((ROOT/'ownership.json').read_text(encoding='utf-8'))['reports']
CLASS_TEXT={'observed':'','reconciled_zero':' (none drafted)','unresolved':'','no_contest':''}
def pct(v):return '—' if v is None else f'{v:.1f}%'
def own_of(g,p):return own[g['key']]['rows'][str(p['dk_player_id'])]
def pred_cell(g,p):
    o=own_of(g,p)['predicted']
    if o.get('version') is None:return 'not stored'
    return f"{pct(o['captain'])} / {pct(o['flex'])}" if g['format']=='showdown' else pct(o['total'])
def field_cell(g,p):
    f=own_of(g,p)['field'];c=f['class']
    if c=='no_contest':return 'no contest imported'
    if c=='unresolved':return 'unresolved'
    if c=='reconciled_zero':return '0.0% (none drafted)'
    if g['format']=='showdown' and f.get('captain') is not None:return f"{pct(f.get('captain'))} / {pct(f.get('flex'))}"
    return pct(f.get('total'))
BASE=ROOT
def link(path,label,base=None):
    rel=Path(os.path.relpath(path,base or BASE)).as_posix()
    return f'[{label}]({rel})'
def label(g):return f"Week {g['week']} {g['format'].title()}"+(' '+g['games'][0] if g['format']=='showdown' else ' main slate')
def fname(g):return f"week-{g['week']:02d}-{g['format']}"+('-'+g['games'][0].replace('@','-at-') if g['format']=='showdown' else '')+'.md'
def atime(t):return datetime.fromisoformat(t).astimezone(ZoneInfo('America/New_York')).strftime('%b %d, %I:%M %p ET')
def exp(p,g):return 'Unavailable' if p['entries'] is None else f"{p['entries']}/{g['lineups']} ({p['exposure_pct']:.1f}%)"
def clean(s):return str(s or '—').replace('|','/').replace('\n',' ')
def table(players,g,include_verdict=True):
    head='| Player | Pos | Salary | Projection | Actual | Above | Saved exposure |'
    if g['format']=='showdown':head+=' CPT / FLEX entries |'
    head+=(' Our own. (CPT / FLEX) | Field own. (CPT / FLEX) |' if g['format']=='showdown' else ' Our own. | Field own. |')
    if include_verdict:head+=' Assessment |'
    cols=head.count('|')-1
    lines=[head,'|'+'---|'*cols]
    for p in players:
        name=p['name']+(' †' if p['result_conflict'] else '')
        row=f"| {name} | {p['position']} | ${p['salary']:,} | {num(p['projection'])} | {num(p['actual'])} | {num(p['delta'])} | {exp(p,g)} |"
        if g['format']=='showdown':row+=f" {p['captain_entries'] if p['captain_entries'] is not None else '—'} / {p['flex_entries'] if p['flex_entries'] is not None else '—'} |"
        row+=f" {pred_cell(g,p)} | {field_cell(g,p)} |"
        if include_verdict:row+=f" {p['verdict']} |"
        lines.append(row)
    return '\n'.join(lines)

notes={
 (1,'classic','main'):'Jalen Coker ($4,600, 10.41 → 36.80) and Dalton Kincaid ($4,600, 5.28 → 21.00) lead the observed misses. The latest saved build contains only ONE lineup and was created September 6, a week before lock. Zero appearances here cannot establish your exposure across actual Week 1 entries. Early snapshots also lack the later opportunity-chip coverage.',
 (1,'showdown','NE@SEA'):'No player met the larger-hit screen (10+ actual points and 5+ above projection). Seahawks DST scored 13.00 against 9.15 and appeared in 5/18. Brady Russell at $200 scored 4.00 against 0.10 with no exposure: excellent realized salary value, but only four total points and no verified pre-lock opportunity case. Do not turn that result alone into an allocation recommendation.',
 (2,'classic','main'):'Tre Tucker ($4,500, 7.60 → 25.90, 0/20) and Rashod Bateman ($4,400, 5.30 → 21.80, 0/20) are offensive review candidates. Panthers DST (5.04 → 26.00) and Patriots DST (6.14 → 21.00) were also omitted; separate volatile DST outcomes from established pre-lock errors. The later September 24 test build was excluded because it occurred after lock.',
 (2,'showdown','IND@KC'):'Tyquan Thornton scored 12.60 versus 4.80, but already appeared in 4/20: one Captain and three FLEX. Laquon Treadwell ($2,000, 0.29 → 9.10) and Jalen Royals ($1,200, 0.93 → 8.60) were zero-exposure cheaper outcomes worth inspecting, although neither reached ten actual points and their pre-lock cases remain unproven.',
 (2,'showdown','NYG@LAR'):'Terrance Ferguson ($1,800, 5.28 → 17.40) was a cheap hit we already used in 6/40 FLEX seats. Rams DST scored 11.00 versus 6.99 with only 1/40. The evidence supports reviewing relative allocation, not treating Ferguson as a complete omission.',
 (3,'showdown','ATL@GB'):'Brian Robinson Jr. ($4,400, 4.36 → 11.00) was omitted. He becomes a repeat review item after his Week 4 ATL/NO result. This does not establish that either historical role was known before kickoff. Later September 25 uploads were excluded from pre-lock selection.',
 (3,'classic','main'):'Kenyon Sadiq ($3,500, 7.28 → 26.50), Geno Smith ($4,900, 11.86 → 30.04), Noah Fant ($3,200, 4.58 → 19.30), and Jordan Addison ($4,900, 6.18 → 20.00) all had zero exposure in the 20-lineup build. These are strong realized misses; the saved snapshot lacks the later detailed opportunity tags needed to rank their pre-lock cases confidently.',
 (3,'showdown','PHI@CHI'):'Kalif Raymond ($3,800, 7.64 → 21.00) was already in 11/20 FLEX seats, so more exposure is not the obvious lesson. Bears DST ($4,000, 6.32 → 12.00) appeared once. Makai Lemon beat his mean but scored only 5.90.',
 (4,'showdown','PIT@CLE'):'Roman Wilson ($4,000, 6.31 → 16.40) appeared in 1/20; Darnell Washington ($3,600, 4.95 → 11.70) appeared in none. Both were eligible in the saved build. Their cheap receiving alternatives deserve review, without assuming depth ranks implied their eventual targets.',
 (4,'showdown','IND@WSH'):'Treylon Burks ($2,000, 3.66 → 11.70) was blocked by the cheap-player role gate, not merely passed over by the optimizer. The saved reason says the listed WR4 remained outside the top three after absences. This is a gate-review candidate; absent same-time dual-depth evidence, overriding the gate is not retrospectively proven correct.',
 (4,'classic','main'):'Emanuel Wilson ($4,700, 4.96 → 27.00, 0/107) had a frozen INSIDE_FIVE signal, making his omission worth closer review. Tyquan Thornton ($3,400, 5.47 → 31.10), Kyle Monangai ($4,900, 9.72 → 31.00), and T.J. Hockenson ($3,300, 7.36 → 27.90) were other major zero-exposure hits. These outcome rankings are not calibrated pregame priorities.',
 (4,'showdown','DET@CAR'):'Isaac TeSlaa ($4,200, 6.81 → 13.50) appeared in 11/31 FLEX seats (35.5%). This was a cheap hit we captured, not a clear omission. No other sub-$5,000 player met the larger-hit screen.',
 (4,'showdown','ATL@NO'):'Brian Robinson Jr. ($3,800, 5.92 → 25.70) received just 2/40 FLEX appearances. Jahan Dotson ($3,000, 2.96 → 13.40) received none. Robinson also exceeded his mean without exposure in Week 3 Showdown, making him a repeat projection/role review rather than a one-game anecdote.',
 (5,'showdown','TB@DAL'):'No saved optimizer portfolio exists for this slate. The comparison uses the immutable pre-lock baseline linked to its salary upload, not refreshed slate-row projections. Ryan Flournoy ($3,800, 8.18 → 12.00) and Tez Johnson ($2,200, 3.81 → 7.40) led the small positive misses. Exposure, Captain allocation, and the claim that we should have entered more cannot be established.',
 (5,'showdown','PHI@JAX'):'Darius Cooper ($4,200, 5.21 → 15.10) already appeared in 9/50 FLEX seats (18%). Nate Boerkircher was a low-salary outcome at $600, but scored only 4.00. Neither result independently establishes a need to change allocation.',
 (5,'classic','main'):'Isaiah Likely ($4,300, 8.87 → 32.40, 0/40) is the clearest pool-rule review: the saved preset excluded tight ends priced at $4,000 or above despite his 27 targets over three games and YAC_RUNWAY signal. DeMario Douglas ($3,500, 4.88 → 22.50, 0/40) is another meaningful miss. Likely was heavily used by Week 6 after the preset changed; do not apply today’s rule backward.',
 (5,'showdown','BAL@ATL'):'Kyle Pitts ($4,800, 8.16 → 15.20) was already in 35/100 FLEX seats. Devontez Walker scored 8.90 versus 3.54 and appeared 21 times. These were represented cheap outcomes, not evidence of a broad omission.',
 (5,'showdown','BUF@LAR'):'Blake Corum (5.94 → 16.20, 5/26), Tyler Higbee (7.48 → 13.30, 10/26), and Ty Johnson (5.38 → 11.20, 3/26) were all already represented in FLEX. None had zero exposure, and these results alone do not determine a better allocation.',
 (6,'showdown','SEA@DEN'):'Rashid Shaheed ($3,600, 6.22 → 12.80) appeared in 3/35 FLEX seats (8.6%). Cooper Kupp ($3,200, 6.23 → 10.00) appeared five times. Shaheed is a low-exposure result to review, with no preserved same-time dual-depth confirmation establishing that more was warranted.',
 (6,'showdown','HOU@JAX'):'Jaylin Noel ($3,000, 4.63 → 23.70, 0/50) was excluded by the cheap-player role gate. The frozen snapshot contained ten targets in three games but labeled him WR4. His outcome warrants investigating that gate and the role evidence; it does not justify silently replacing the source rank with the eventual result. Foster Moreau ($1,600, 3.52 → 14.70) appeared twice; Jawhar Jordan already appeared 14 times.',
 (6,'classic','main'):'Tre Tucker ($4,700, 10.31 → 26.70, 2/65) and DeMario Douglas ($4,200, 5.47 → 20.60, 0/65) had frozen air-volume signals. Kyle Pitts ($3,700, 8.88 → 23.00) was excluded at 3.7 targets/game by the 4.0 cutoff. Bernard was already in 21/65, and Likely/Loveland each reached the saved 40% cap. Omar Cooper’s 25.30 came with no supported forecast and remains a coverage review.',
 (6,'showdown','DAL@GB'):'Ryan Flournoy ($4,400, 8.11 → 17.00) had only 1/20 FLEX exposure despite a saved CLOSE_TARGET signal and 18 targets in three games. He is one of the stronger Showdown allocation-review candidates from the frozen evidence, without establishing an exact recommended percentage.',
 (6,'showdown','WSH@SF'):'Chig Okonkwo ($4,600, 6.92 → 13.90) already appeared in 13/26 FLEX seats, reaching the saved 50% cap. Treylon Burks ($3,200, 5.23 → 10.60) appeared in 8/26. These outcomes do not support calling either an overlooked value.',
}

summary=['# NFL value review: Weeks 1–6, 2026','',
 'Reviewed every distinct completed Classic and Showdown slate saved in this project: **6 Classic + 17 Showdown = 23 slates**. Selected 22 pre-lock portfolios containing **789 lineups**. Week 5 TB/DAL has an immutable baseline but no saved portfolio. Repeated salary uploads were consolidated; 99 system-generated experiment runs were excluded from portfolio selection. This does not cover hypothetical DraftKings slates for which the project has no saved salary pool.', '',
 'The strongest process reviews are **Likely’s Week 5 salary-rule exclusion; Tucker and Douglas’s repeated Classic misses; and the low-exposure Showdown cases of Brian Robinson, Roman Wilson, and Ryan Flournoy**. Burks/Noel are role-gate investigations, not proven cases for overriding a safety check.', '',
 'All dollar screens use **base/FLEX salary strictly below $5,000**. Captain costs and points receive the 1.5 multiplier once; the detailed Showdown pages show Captain/FLEX exposure separately and identify which Captain purchases also cost below $5,000. A player who costs $4,400 at FLEX costs $6,600 at Captain and is not a sub-$5,000 Captain.', '',
 'These are saved generated exposures, not verified submitted entries. Actual above mean is an observed outcome, not proof a projection or allocation was wrong. No reoptimized portfolios, calibrated exposure targets, payout/ROI claims, or predictive-validation claims are made.', '',
 '## Priority findings','',
 ]
CASES=[(5,'classic',None,'Isaiah Likely','Excluded by TE salary ceiling despite 27 targets in three games'),
 (2,'classic',None,'Tre Tucker','Repeated low exposure'),(6,'classic',None,'Tre Tucker','Repeated low exposure; W6 frozen air-volume/YAC signals'),
 (5,'classic',None,'DeMario Douglas','Repeated omissions'),(6,'classic',None,'DeMario Douglas','Repeated omissions; W6 deep-target evidence'),
 (4,'classic',None,'Emanuel Wilson','Frozen inside-five opportunity signal'),
 (4,'showdown','ATL@NO','Brian Robinson Jr.','Also beat projection with zero exposure in W3 Showdown'),
 (4,'showdown','PIT@CLE','Roman Wilson','Eligible cheap receiving alternative'),
 (6,'showdown','DAL@GB','Ryan Flournoy','Frozen close-range target signal'),
 (6,'showdown','HOU@JAX','Jaylin Noel','Excluded by role gate; historical role still unresolved')]
summary+=['| Case | Salary | Frozen projection → actual | Saved exposure | Our own. | Field own. | Why review |','|---|---:|---:|---:|---:|---:|---|']
for wk,fmt,game,name,why in CASES:
    g=next(r for r in x['reports'] if r['week']==wk and r['format']==fmt and (game is None or r['games'][0]==game))
    p=next(r for r in g['rows'] if r['name']==name)
    tag=f"W{wk} {'Classic' if fmt=='classic' else game.replace('@','/')+' Showdown'}"
    summary.append(f"| {tag}: {name} | ${p['salary']:,} | {num(p['projection'])} → {num(p['actual'])} | {exp(p,g)} | {pred_cell(g,p)} | {field_cell(g,p)} | {why} |")
summary+=['','Ownership: "Our own." is the frozen pre-lock forecast stored with the selected build (Showdown captain / flex); "Field own." is the slate\'s largest imported contest. A player absent from a complete DraftKings export is 0.0% (none drafted); absent from a stat-api import is unresolved, not zero (docs/nfl-ownership-scorecard-v1.md section 3).','',
 'The frozen Week 5 TE rule differs from the Week 6 rule. The audit explains each saved decision using its own eligibility ledger. Earlier snapshots often lack later opportunity chips; missing chips do not demonstrate an absence of opportunity.', '',
 '## Classic, every saved week','',
 '| Week / full review | Saved lineups | Largest observed misses (salary; forecast → actual; entries; our → field ownership) |',
 '|---|---:|---|']

for g in x['reports']:
    key=(g['week'],g['format'],g['games'][0] if g['format']=='showdown' else 'main')
    path=pages/fname(g)
    notice=notes[key]
    page=[f'# {label(g)}: players below $5,000','',notice,'',
        f"Ownership reference contest: {('`'+own[g['key']]['reference_contest']+'`') if own[g['key']]['reference_contest'] else 'none imported for this slate'}.",'',
        f"Decision snapshot: **{atime(g['selected_created_at'])}**. First lock: {atime(g['lock_at'])}. "
        f"Saved lineups: {g['lineups'] if g['lineups'] is not None else 'unavailable'}. "
        f"Selected run: `{g['selected_run_id'] or 'none — immutable baseline only'}`. "
        f"Projection source: `{g['projection_source']}`.",'',
        'Exposure refers to the latest pre-lock saved build, not verified entries. All comparisons use frozen effective projections. Later uploads/builds are not used to rewrite pregame forecasts.','',
        f"Coverage: {g['metrics']['listed_under_5000']} listed base salaries below $5,000; "
        f"{g['metrics']['supported_scored']} supported projections with an observed result; "
        f"{g['metrics']['outperformers']} above projection. Missing actual evidence: {g['metrics']['missing_actual']} players. "
        'A missing result is not automatically treated as zero. These are within-slate observations; they are not independent validation samples.','',
        '## Larger hits','',
        'Screen: at least ten actual base points and at least five points above a supported frozen projection. These reporting thresholds are descriptive, not allocation rules.','']
    page+=[table(g['notable'],g) if g['notable'] else 'No player met that screen. Smaller positive differences appear below.']
    page+=['','## All supported players who beat projection','',table(g['outperformers'],g),'']
    if g['format']=='showdown':
        page+=['## Captain purchase check','',
            'All points above are base/FLEX points. Multiply each base projection, actual and difference by 1.5 once for Captain. Total exposure counts a player once regardless of seat. A low Captain count alone does not prove missed Captain value.','',
            '| Player | CPT salary | CPT under $5,000? | CPT projected → actual | CPT entries |',
            '|---|---:|---|---:|---:|']
        for p in g['outperformers']:
            page.append(f"| {p['name']} | {'$'+format(p['captain_salary'],',') if p['captain_salary'] is not None else 'Unavailable'} | {'Yes' if p['captain_under_5000'] else 'No / unavailable'} | {num(p['captain_projection'])} → {num(p['captain_actual'])} | {p['captain_entries'] if p['captain_entries'] is not None else 'Unavailable'} |")
        page.append('')
    page+=['## Coverage or role-zero cases','',
        'Positive results of ten points or more without a supported player-specific forecast are separate from valid projection misses. “Position prior” means an average-based estimate; “role/out zero” includes policy-blocked quarterbacks. These are not instructions to override availability checks.','']
    if g['coverage_gaps']:
        page+=['| Player | Salary | Actual | Forecast state | Saved exposure |','|---|---:|---:|---|---:|']
        for p in g['coverage_gaps']:
            page.append(f"| {p['name']} | ${p['salary']:,} | {num(p['actual'])} | {p['forecast_kind']} | {exp(p,g)} |")
    else:page.append('No such ten-point case was identified.')
    page+=['','## Why the low-exposure cases need review','']
    review=[p for p in g['notable'] if p['entries'] is None or p['exposure_pct']<=10]
    if not review:page.append('No larger hit had ten percent or less saved exposure, or no saved portfolio exists. Representation alone does not establish the correct target.')
    for p in review:
        e=p.get('eligibility')
        reason=e['reason'] if e and e.get('reason') else 'Eligible in the saved ledger.' if e and e.get('eligible') else 'No saved eligibility ledger; do not infer a gate decision.'
        signals=' '.join(s['detail'] for s in p['signals']) or 'No frozen opportunity-tag evidence retained for this player; early versions did not provide these chips.'
        c=p.get('scoring_components') or {}
        tdpoints=6*sum(c.get(k,0) or 0 for k in ['rushing_tds','receiving_tds','special_teams_tds','fumble_recovery_tds'])+4*(c.get('passing_tds',0) or 0)
        tdnote=f' Realized touchdowns contributed {tdpoints:.0f} points; that is an outcome decomposition, not a pregame forecast.' if tdpoints else ''
        page.append(f"- **{p['name']}**: {clean(reason)} {clean(signals)}{tdnote}")
    page+=['','## High-exposure cheap underperformers','',
        'For balance, these are the largest shortfalls among cheap players used in more than ten percent of the saved build. Swapping a winning player for one of these after the fact is not a validated strategy.','']
    losers=sorted([p for p in g['rows'] if p['delta'] is not None and p['delta']<0 and (p['exposure_pct'] or 0)>10],key=lambda p:p['delta'])[:5]
    page.append(table(losers,g,False) if losers else 'None identified, or exposure is unavailable.')
    page+=['','## Injury, role and result evidence','',
        'Historical same-time Sleeper/FantasyPros depth reconciliation is unavailable. The source inventory has no retained depth-chart snapshot groups or official inactive imports for these weeks. The injury observations below are week-matched and captured before this build, but an injury API is not a depth chart. Missing observations do not mean healthy. Current depth captures cannot repair the historical decision record, so replacement roles remain unresolved.','',
        '| Larger-hit / coverage player | Frozen Sleeper decision | Latest pre-build FantasyPros injury observation |','|---|---|---|']
    evidence={p['dk_player_id']:p for p in g['notable']+g['coverage_gaps']}
    for p in evidence.values():
        a=p.get('availability') or {}
        state=f"{a.get('status','unknown')}; {a.get('role','role unresolved')}; captured {a.get('capturedAt','unknown')}"
        o=obs.get((g['key'],p['ff_player_id'],'fantasypros'))
        fp=f"{o['normalized_status']}; observed {o['observed_at']}; snapshot {o['source_snapshot_id']}" if o else 'No matching pre-build observation retained'
        page.append(f"| {p['name']} | {clean(state)} | {clean(fp)} |")
    conflicts=[p for p in g['rows'] if p['result_conflict']]
    if conflicts:
        page+=['','† Result disagreement: the displayed contest result differs from the canonical component ledger. Both values remain retained; this is a reconciliation issue, not an allocation conclusion.']
        for p in conflicts:page.append(f"- {p['name']}: contest {num(p['actual'])}, canonical {num(p['canonical_actual'])}; contest `{p['field_contest_id']}`.")
    page+=['',f"Game completion and both-team skill-result coverage checked: {len(g['coverage'])} games. "
        f"Lineup structure/salary/projection checks: {len(g['verification_issues'])} discrepancies. "
        f"Contest point corroboration is available for {sum(p['field_ownership'] is not None for p in g['rows'])} cheap players, with incomplete field ownership coverage possible.", '',
        'Selected upload: `'+g['selected_upload_id']+'`. Related contests: '+(', '.join('`'+c['contest_id']+'`' for c in g['contests']) or 'none retained')+'.','',
        link(ROOT/'review.md','Back to all slates',pages)]
    path.write_text('\n'.join(page)+'\n',encoding='utf-8')
    if g['format']=='classic':
        highlights='; '.join(f"{p['name']} (${p['salary']:,}; {p['projection']:.1f} → {p['actual']:.1f}; {p['entries']}/{g['lineups']}; own {pred_cell(g,p)} → {field_cell(g,p)})" for p in g['notable'][:3])
        summary.append(f"| {link(path,'Week '+str(g['week']))} | {g['lineups']} | {highlights or 'No larger hit'} |")

summary+=['','## Showdown, every saved slate','',
    '| Week / full review | Game | Saved lineups | Larger-hit outcomes, exposure, our → field ownership (CPT / FLEX) |','|---|---|---:|---|']
for g in x['reports']:
    if g['format']!='showdown':continue
    ps=g['notable'] or g['outperformers'][:2]
    details='; '.join(f"{p['name']} (${p['salary']:,}; {p['projection']:.1f} → {p['actual']:.1f}; {str(p['entries'])+'/'+str(g['lineups']) if p['entries'] is not None else 'exposure unavailable'}; own {pred_cell(g,p)} → {field_cell(g,p)})" for p in ps)
    summary.append(f"| {link(pages/fname(g),'Week '+str(g['week']))} | {g['games'][0]} | {g['lineups'] if g['lineups'] is not None else 'None saved'} | {details} |")
rows_all=[(g,p) for g in x['reports'] for p in g['rows'] if p.get('forecast_kind')=='supported' and p.get('actual') is not None and p['position']!='DST']
def rate(ps):return (len(ps), st.mean(p['delta'] for _,p in ps) if ps else float('nan'), 100*sum(p['delta']>=10 for _,p in ps)/len(ps) if ps else float('nan'))
def code(p):
    e=p.get('eligibility')
    if not e:return 'no saved ledger'
    return e.get('reasonCode') or ('eligible' if e.get('eligible') else 'unknown')
summary+=['','## Base rates: the denominator behind the highlights (added 2026-10-22)','',
    'The cases above are the tail. Across every supported, non-DST player below $5,000 with an observed result, the table shows how many cleared their projection by ten points, by the reason the saved build gave for each one. A rule that cost value would show a higher hit rate among the players it excluded.','',
    '| Group | Players | Mean actual − projection | Beat projection by 10+ |','|---|---:|---:|---:|']
n,m,h=rate(rows_all);summary.append(f'| All cheap supported players | {n} | {m:+.2f} | {h:.1f}% |')
groups={}
for g,p in rows_all:groups.setdefault(code(p),[]).append((g,p))
for k,v in sorted(groups.items(),key=lambda t:-len(t[1])):
    n,m,h=rate(v);summary.append(f'| {k} | {n} | {m:+.2f} | {h:.1f}% |')
summary+=['','Low-owned hits are what matter in a tournament. Among players who beat projection by ten or more with an observed or reconciled field share:','']
hits=[(g,p) for g,p in rows_all if p['delta']>=10 and own_of(g,p)['field']['class'] in ('observed','reconciled_zero')]
low=[h for h in hits if (own_of(*h)['field'].get('total') or 0)<5]
summary.append(f'- {len(hits)} such hits; {len(low)} were under 5% field ownership.')
summary+=['','## What we already captured','',
    'Examples include Week 3 Kalif Raymond (11/20), Week 4 Isaac TeSlaa (11/31), Week 5 Pitts (35/100), and Week 6 Okonkwo (13/26, at cap). In Week 6 Classic, Bernard appeared 21/65 and Likely/Loveland each 26/65. Outperforming projection does not by itself make these players underexposed.','',
    '## Evidence limits and verification','',
    '- All 23 slates passed canonical game completion and both-team skill-result coverage. All 789 selected lineups passed seat count, unique-player, salary-cap, salary-sum, projection-sum, and frozen-player/slot projection checks. Captain scoring and salaries were checked separately.',
    '- There are 517 supported cheap-player/slate observations above projection; 162 meet the larger-hit reporting screen. Repeated players and overlapping Classic/Showdown games are correlated. Missing result rows remain unknown, so these counts are not a complete calibration scorecard or a hit rate.',
    '- Two DST outcomes disagree by one point between imported contest evidence and the canonical v4 ledger: Week 1 Buccaneers (contest 9, ledger 8) and Week 5 Seahawks (contest 9, ledger 8). They are explicitly flagged; both values remain available.',
    '- Historical dual-provider depth and official inactive evidence is incomplete. For 261 player/slate decision boundaries checked, 219 source injury observations matched the proper week and pre-build cutoff. This is not 219 independently validated roles or 219 unique players. No current capture is presented as historical pre-lock evidence.',
    '- Week 1 Classic has only one lineup in the latest saved build, created roughly a week before lock. Week 5 TB/DAL has no saved build. Neither supports a confident claim about your actual allocation. Other selected portfolios vary from 18 to 107 lineups.',
    '- Ownership (added 2026-10-22): every case carries our frozen forecast and the field share from the slate\'s largest imported contest with its label class. Slates without an imported contest (IND/KC, PHI/JAX, BAL/ATL, HOU/JAX, DAL/GB) show no field share; builds saved before ownership was stored show "not stored". `ownership.json` holds the per-player values.',
    '- The JSON retains every listed cheap player, supported and unsupported forecasts, source IDs, original eligibility reasons, opportunity signals, actual components, and Captain/FLEX exposures. Original forecasts are preserved; no model or production change was made.', '',
    '## Reproduce or inspect','',
    link(ROOT/'assessment.json','Complete player-level assessment')+' · '+link(ROOT/'inventory.json','Slate and run inventory')+' · '+link(ROOT/'injury-boundaries.json','Historical injury boundaries')+' · '+link(ROOT/'verification.json','Verification results'),'',
    'Offline verification passed: 1,343 non-DST component rescores, 641 Showdown Captain/base-row checks, all 219 retained injury observation boundaries, and exact reproduction of the 63 supported outperformers from the earlier Week 6 Sunday review. The two DST contest/ledger differences remain unresolved.', '',
    'Frozen source evidence: `inputs.json.gz`. Scripts in the parent artifacts folder: `capture_nfl_value_history_20261021.py`, `capture_nfl_value_injuries_20261021.py`, `analyze_nfl_value_history_20261021.py`, and `render_nfl_value_history_20261021.py`. Captures use SELECT-only database transactions. The analysis and report generation run offline from retained evidence.']
(ROOT/'review.md').write_text('\n'.join(summary)+'\n',encoding='utf-8')
print(json.dumps({'summary':str(ROOT/'review.md'),'slate_reports':len(list(pages.glob('*.md')))}))
