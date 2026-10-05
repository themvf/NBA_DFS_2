// Optional browser verification of the actual NFL client with controlled server-action fixtures.
// No database or external API is used. Outputs screenshots and results under artifacts/.
// Requires Playwright and Chromium; NFL_DFS_PLAYWRIGHT and NFL_DFS_CHROMIUM can select
// an existing installation. Run from web: node scripts/verify-nfl-forecast-ui.mjs
// Kept outside the offline CI test discovery; no production dependency is added.

import fs from 'node:fs';
import path from 'node:path';
import http from 'node:http';
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import { fileURLToPath } from 'node:url';
const loadRuntime = createRequire(import.meta.url);
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const web = path.resolve(__dirname, '..'); const out = path.resolve(web, '../artifacts/nfl-dfs-ui-finish'); fs.mkdirSync(out,{recursive:true});
const esbuild = loadRuntime(path.join(web,'node_modules/esbuild'));
const playwright = loadRuntime(process.env.NFL_DFS_PLAYWRIGHT || 'playwright');
const names = [...fs.readFileSync(path.join(web,'src/app/dfs/nfl/client-actions.ts'),'utf8').matchAll(/export const (\w+)/g)].map(m=>m[1]).concat('generateNflLineups');
const mock = `
import {buildSlateCheck} from '@/lib/nfl-dfs/slate-check';
import {specialTeamsStatus} from '@/lib/nfl-dfs/special-teams-status';
const now = Date.now();
let ready=false, adopted=false, pendingJob=false;
function pool() {
 const mode=new URLSearchParams(location.search).get('case')||'classic';
 const archive=mode.startsWith('archive'); const showdown=mode.includes('showdown');
 const firstKickoff=new Date(now+(archive?-3600000:3600000)).toISOString();
 const players=(showdown?['DST','K']:['DST','DST']).map((position,i)=>({
 id:i+1,dkPlayerId:i+1,name:position==='K'?'Kicker':'Defense '+(i+1),team:i?'CAR':'DET',opponent:i?'DET':'CAR',
 position,salary:4000,gameKey:'DET@CAR',gameInfo:'DET@CAR',rosterPositions:[position],isOut:false,ourProj:7,floorFpts:1,ceilingFpts:15,
 avgFptsDk:6,projectionStatus:'historical',historyGames:10,ffPlayerId:i+1,ownPct:12,
 availability:{role:'Starter',status:'ACTIVE',source:'Saved',capturedAt:new Date(now-60000).toISOString()},availabilityState:'confirmed',
 specialTeams:ready?{version:'nfl-special-teams-pregame-v1',status:'candidate',position,mean:8,p10:1,p50:7,p90:18,boom:.3,feature_snapshot:{authority:'candidate_only',opponent_implied_total:21,team_implied_total:24}}:null,
 specialTeamsReason:ready?null:'No matchup forecast was saved in this projection run.'
 }));
 const slate={uploadId:'00000000-0000-4000-8000-000000000001',format:showdown?'showdown':'classic',games:showdown?['DET@CAR']:['DET@CAR','BUF@MIA'],teams:['DET','CAR'],players,warnings:[],firstKickoff,modelVersion:'nfl-dfs-historical-v5',modelAsOf:new Date(now-7200000).toISOString(),refreshAvailable:ready&&!adopted,confirmedStartingQbs:{applied:[],rejected:[]}};
 slate.slateCheck=buildSlateCheck({label:showdown?'DET @ CAR':'Classic',now,firstKickoff,deployedBuild:true,incompleteWarning:null,refreshAvailable:ready,projectionAsOf:slate.modelAsOf,rosterStaleWarning:null,rosterCapturedAt:null,qbs:[],opponentAdjustments:null,ownership:null,availability:null,liveDk:null,upside:null,unmatched:[],specialTeams:specialTeamsStatus({players,firstKickoff,now,refreshAvailable:ready}),experimentalSources:[{label:'Workload (experimental)',usable:false,reason:'Internal study 7ff4d404'}]});
 return slate;
}
const api={
 listSavedNflSlates:()=>[{uploadId:pool().uploadId,label:pool().format==='classic'?'Classic fixture':'Showdown fixture'}],
 loadSavedNflWorkspace:()=>({slate:pool(),runs:[]}), readNflBuildDraft:()=>null, saveNflBuildDraft:()=>{window.__draftWrites=(window.__draftWrites||0)+1;return null},
 readNflSlateResults:()=>null,readNflFieldAudit:()=>null,readNflDataUpdate:()=>{if(pendingJob){pendingJob=false;ready=true;return {update:{id:'fixture-update',requestedAt:new Date().toISOString()},view:{state:'succeeded',headline:'Data updated',lines:[]},blockedReason:null};}return {update:null,view:null,blockedReason:null};},
 readNflDefensiveCaptureStatus:()=>({pending:false}),checkNflSlateFreshness:()=>({refreshAvailable:ready,newestAsOf:pool().modelAsOf}),
 refreshNflSlateProjections:()=>{adopted=true;window.__refreshes=(window.__refreshes||0)+1;return {...pool(),refreshAvailable:false}},
 startNflDataUpdate:()=>{window.__updates=(window.__updates||0)+1;const mode=new URLSearchParams(location.search).get('case');if(mode.startsWith('failed'))throw new Error('The projection update could not start. Please retry.');if(mode.startsWith('pending')){pendingJob=true;return {update:{id:'fixture-update',requestedAt:new Date().toISOString()},view:{state:'running',headline:'Updating forecasts',lines:[]},blockedReason:null};}ready=true;return {update:{id:'fixture-update',requestedAt:new Date().toISOString()},view:{state:'succeeded',headline:'Data updated',lines:[]},blockedReason:null}},
 explainNflPlayerProjection:()=>({ok:false,error:'Historical breakdown is not part of this fixture.'})
};
${names.map(n=>'export const '+n+'=async(...args)=>api.'+n+'?api.'+n+'(...args):null;').join('\n')}
`;
(async()=>{
 await esbuild.build({stdin:{contents:"import React from 'react';import {createRoot} from 'react-dom/client';import Client from './src/app/dfs/nfl/nfl-dfs-client';createRoot(document.getElementById('root')).render(<Client/>);",resolveDir:web,sourcefile:'fixture.tsx',loader:'tsx'},bundle:true,outfile:path.join(out,'fixture.js'),platform:'browser',format:'iife',jsx:'automatic',tsconfig:path.join(web,'tsconfig.json'),define:{'process.env.NODE_ENV':'"development"'},plugins:[{name:'mock-actions',setup(b){b.onResolve({filter:/client-actions$/},()=>({path:'actions',namespace:'mock'}));b.onLoad({filter:/.*/,namespace:'mock'},()=>({contents:mock,loader:'ts',resolveDir:web}));}}]});
 const postcss=loadRuntime(path.join(web,'node_modules/postcss'));const tailwind=loadRuntime(loadRuntime.resolve('@tailwindcss/postcss',{paths:[web]}));
 const css=await postcss([tailwind({base:web})]).process(fs.readFileSync(path.join(web,'src/app/globals.css'),'utf8'),{from:path.join(web,'src/app/globals.css')});
 fs.writeFileSync(path.join(out,'fixture.css'),css.css+fs.readFileSync(path.join(web,'src/app/dfs/nfl/nfl-workspace.css'),'utf8'));
 fs.writeFileSync(path.join(out,'fixture.html'),'<!doctype html><html><head><meta name="viewport" content="width=device-width,initial-scale=1"><link rel="stylesheet" href="/fixture.css"></head><body><main style="padding:16px"><div id="root"></div></main><script src="/fixture.js"></script></body></html>');
 const server=http.createServer((req,res)=>{const name=req.url.split('?')[0].slice(1)||'fixture.html';res.setHeader('Content-Type',name.endsWith('.css')?'text/css':name.endsWith('.js')?'text/javascript':'text/html');if(!fs.existsSync(path.join(out,name))){res.statusCode=404;res.end();return;}res.end(fs.readFileSync(path.join(out,name)));});
 await new Promise(r=>server.listen(0,'127.0.0.1',r));
 let browser;
 try{
 browser=await playwright.chromium.launch({headless:true,executablePath:process.env.NFL_DFS_CHROMIUM});
 const errors=[];const results=[];
 for(const mode of ['classic','showdown','pending-showdown','failed-showdown','archive-classic','archive-showdown']){
 const page=await browser.newPage({viewport:{width:1440,height:1000}});page.on('pageerror',e=>errors.push(mode+': '+e.message));
 await page.goto('http://127.0.0.1:'+server.address().port+'/?case='+mode);
 await page.getByRole('navigation',{name:'Workspace steps'}).waitFor();
 await page.getByRole('navigation',{name:'Workspace steps'}).getByRole('button').filter({hasText:mode.startsWith('archive')?'Saved settings':'Build'}).click();
 await page.getByRole('heading',{name:mode.startsWith('archive')?'Saved build settings':'Build lineups',exact:true}).waitFor();
 const text=await page.locator('body').innerText();
 assert(!text.includes('Experimental model')&&!text.includes('Internal study')&&!text.includes('selected defensive profile'));
 if(mode.startsWith('archive')){
 assert(!text.includes('Refresh projections')&&!text.includes('Update data')&&!text.includes('Confirm the starter'));
 assert(text.includes('Saved forecasts are preserved'));
 assert.equal(await page.getByRole('button',{name:'Generate & save',exact:true}).count(),0);assert.equal(await page.getByRole('button',{name:'Lock Defense 1',exact:true}).isDisabled(),true);assert.equal(await page.getByRole('button',{name:'Exclude Defense 1',exact:true}).isDisabled(),true);
 await page.waitForTimeout(900);assert.equal(await page.evaluate(()=>window.__draftWrites||0),0);
 await page.getByText('View saved build settings',{exact:true}).click();
 assert.equal(await page.getByRole('combobox').filter({has:page.locator('option[value="our"]')}).isDisabled(),true);
 }else{
 assert(text.includes('historical fallback'));
 await page.getByRole('button',{name:'Update data',exact:true}).last().click();
 assert.equal(await page.evaluate(()=>window.__updates),1);if(mode.startsWith('failed')){await page.getByRole('alert').filter({hasText:'projection update could not start'}).waitFor();assert.equal(await page.evaluate(()=>window.__refreshes||0),0);}else{if(mode.startsWith('pending')){await page.getByText('Updating forecasts',{exact:true}).waitFor();assert.equal(await page.getByRole('button',{name:'Update data',exact:true}).last().isDisabled(),true);}await page.getByRole('heading',{name:'Special teams forecasts ready',exact:true}).waitFor();assert.equal(await page.evaluate(()=>window.__refreshes),1);}
 }
 await page.screenshot({path:path.join(out,mode+'-desktop.png'),fullPage:true});
 await page.setViewportSize({width:320,height:900});await page.reload();await page.getByRole('navigation',{name:'Workspace steps'}).waitFor();await page.getByRole('navigation',{name:'Workspace steps'}).getByRole('button').filter({hasText:mode.startsWith('archive')?'Saved settings':'Build'}).click();
 const toggle=page.getByRole('button',{name:mode.startsWith('archive')?'Saved build settings':'Build settings & export',exact:true});
 if(await toggle.isVisible()) await toggle.click();
 const card=page.getByRole('region',{name:'Special teams forecasts'});
 await card.scrollIntoViewIfNeeded();
 await page.evaluate(()=>window.scrollTo(1000,window.scrollY));assert.equal(await page.evaluate(()=>window.scrollX),0,'no horizontal page pan '+mode);assert.equal(await page.evaluate(()=>document.body.scrollWidth<=window.innerWidth),true,'content fits at 320px '+mode);
 const summary=card.locator('summary');await summary.focus();await page.keyboard.press('Enter');assert.equal(await card.locator('details').getAttribute('open'),'');
 await page.screenshot({path:path.join(out,mode+'-mobile.png'),fullPage:false});
 results.push(mode+' desktop/mobile, archive/recovery and keyboard passed'); await page.close();
 }
 assert.deepEqual(errors,[]);fs.writeFileSync(path.join(out,'browser-results.txt'),results.join('\n')+'\nNo browser errors.\n');console.log(results.join('\n'));
 }finally{if(browser)await browser.close();await new Promise(r=>server.close(r));}
})().catch(e=>{console.error(e);process.exitCode=1;});
