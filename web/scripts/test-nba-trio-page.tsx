/** Populated-page layout/grade fixture only; no database calls or stored forecasts. */
import assert from 'node:assert/strict';
import {writeFileSync} from 'node:fs';
import {renderToStaticMarkup} from 'react-dom/server';
import {PgDialect} from 'drizzle-orm/pg-core';
import type {SQL} from 'drizzle-orm';
import registration from '../src/data/nba-trio-math-registration-v2.json';
const names=['Fixture A','Fixture B','Fixture C','Fixture D','Fixture E'];
const players=names.map((player,i)=>({id:`book:fixture${'abcde'[i]}`,player,player_key:`fixture${'abcde'[i]}`,event_id:'book',espn_event_id:'fixture-event',home_team:'Boston Celtics',away_team:'Miami Heat',market_line:30-i,v3_mean:29-i,compound_mean:28-i,raw_p90:40-i,adjusted_p90:39-i,calibration_n:200,availability:{status:'unknown'},opportunities:{minutes:34,fga:20,fta:5}}));
const trios=['abc','abd','abe','acd','ace'].map(t=>[...t].map(k=>`book:fixture${k}`));
const portfolio={trios,market_line_sums:[87,86,85,85,84],scenario_expected_best:{v3:98,compound_joint:97,compound_independent:96},scenario_expected_all_five:{v3:450,compound_joint:440,compound_independent:430}};
const record={version:'nba-trio-math-v1',sha256:'fixture',game_date:'2026-11-04',mode:'prospective',created_at:'2026-11-04T16:38:00Z',source_frozen_at:'2026-11-04T16:37:00Z',lock_at:'2026-11-04T23:00:00Z',protocol_sha256:registration.protocol_sha256,model_sha256:registration.model_sha256,source_snapshot_sha256:'fixture-source',forecast_sha256:'fixture-forecast',pool_n:5,quoted_pool_n:10,modeled_pool_n:8,players,portfolios:{market_baseline:portfolio,joint_best_of_five:portfolio,robust_best_of_five:portfolio},limitations:['TEST FIXTURE — not a recorded forecast.'],calibration:{source:'fixed_January_holdout'}};
let dnp=false;const dialect=new PgDialect();
async function main(){
 // Initialize the driver with a deliberately unreachable fixture URL before imports.
 // execute is replaced below; this test never reads credentials or opens a connection.
 process.env.DATABASE_URL='postgresql://fixture:fixture@127.0.0.1:1/fixture';
 const {db}=await import('../src/db');
 const {default:Page}=await import('../src/app/nba/trio-lab/page');
Object.defineProperty(db,'execute',{value:async(q:SQL)=>{
 const query=dialect.sqlToQuery(q).sql;
 if(query.includes('nba_injury_observations'))return {rows:[{report_sha256:'fixture-injury',published_at:'2026-11-04T18:00:00Z',observed_at:'2026-11-04T18:01:00Z',source_url:'https://example.test/fixture',states:[{game_date:'2026-11-04',player_key:'fixturea',home_key:'bostonceltics',away_key:'miamiheat',status:'Out',reason:'TEST FIXTURE'}]}]};
 if(query.includes('nba_ghost_snapshots'))return {rows:[{candidates:players.map(p=>({...p,home_team:'Boston Celtics',away_team:'Miami Heat'}))}]};
 if(query.includes('count(*)'))return {rows:[{recorded:1,graded:1,complete:dnp?0:1}]};
 if(query.includes('SELECT DISTINCT'))return {rows:[{date:'2026-11-04'}]};
 if(query.includes('double precision'))return {rows:[{game_date:'2026-11-04',record_sha256:'fixture',provisional:dnp,pool_gap:10,audited:true}]};
 if(query.includes('nba_trio_math_snapshots'))return {rows:[{record_sha256:'fixture',record}]};
 if(query.includes('nba_trio_pool_audits'))return {rows:[{audit:{provisional:dnp,full_pool:{eligible:6,verified:dnp?5:6,best_total:130,trio:[]},top_12:{eligible:5,verified:dnp?4:5,best_total:120,trio:[]},pool_gap:10,portfolios:{market_baseline:{best_entry:dnp?null:90,selection_gap:dnp?null:30,total_gap:dnp?null:40,fractional_top_three:dnp?null:2,tie_compatible_top_three:false}}}}]};
 if(query.includes('nba_box_score_players'))return {rows:players.map((p,i)=>({player:p.player,espn_event_id:p.espn_event_id,played:!dnp||i!==4,points:dnp&&i===4?null:10*(i+1),team:'Boston Celtics',home_team:'Boston Celtics',away_team:'Miami Heat',completed:true}))};
 throw Error('Unexpected fixture query');
}});
 const output=renderToStaticMarkup(await Page({searchParams:Promise.resolve({date:'2026-11-04'})}));
 assert.ok(output.includes('Actual best entry: 90'));assert.ok(output.includes('Actual combined five: 380'));
 assert.ok(output.includes('Later reported Out: Fixture A'));
 assert.match(output, /Pool gap: 10 points/);
 assert.ok(output.includes('Complete coverage: 1/1'));
 assert.ok(output.includes('Latest matching audit pending: 0'));
 assert.ok(output.includes('2/3'));
 if(process.env.TRIO_FIXTURE_OUTPUT)writeFileSync(process.env.TRIO_FIXTURE_OUTPUT,output);
 dnp=true;const unresolved=renderToStaticMarkup(await Page({searchParams:Promise.resolve({date:'2026-11-04'})}));
 assert.ok(unresolved.includes('Provisional benchmarks use verified scores so far.'));
 assert.ok(unresolved.includes('Actual best entry: Pending'));assert.ok(unresolved.includes('Actual combined five: Pending'));
 console.log('PASS: populated research page, exact player/game joins, later-Out warning, two portfolio metrics and DNP pending state; no DB calls');
}
main().catch(e=>{console.error(e);process.exitCode=1});
