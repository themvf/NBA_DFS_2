import {isMissingRelation} from "@/lib/database-errors";
import 'server-only';
import {sql} from 'drizzle-orm';
import {db} from '@/db';
import {playerKey,teamKey} from '@/lib/nba-top-scorers';
import registration from '@/data/nba-trio-math-registration-v2.json';
import {summarizePoolObservations,type PoolObservation} from '@/lib/nba-pool-summary';

export type MathPlayer={id:string;player:string;player_key:string;espn_event_id:string;home_team:string;away_team:string;market_line:number;v3_mean:number;compound_mean:number;raw_p90:number;adjusted_p90:number|null;calibration_n:number;availability:{status?:string};opportunities:{minutes:number;fga:number;fta:number}};
export type MathPortfolio={trios:string[][];market_line_sums:number[];scenario_expected_best:Record<string,number>;scenario_expected_all_five:Record<string,number>};
export type MathRecord={version:string;sha256:string;game_date:string;mode:string;created_at:string;lock_at:string;source_frozen_at:string;protocol_sha256:string;model_sha256:string;source_snapshot_sha256:string;forecast_sha256:string;pool_n:number;quoted_pool_n:number;modeled_pool_n:number;players:MathPlayer[];portfolios:Record<string,MathPortfolio|string|number>;limitations:string[];calibration:{source:string}};
export async function getMathResearch(date:string){
 try{
  const result=await db.execute(sql`SELECT record_sha256,record FROM nba_trio_math_snapshots WHERE game_date=${date}::date AND protocol_sha256=${registration.protocol_sha256} ORDER BY created_at DESC LIMIT 1`);
  const row=result.rows[0];if(!row)return null;const r=row.record as MathRecord;
  if(r.version!=='nba-trio-math-v1'||r.mode!=='prospective'||r.sha256!==row.record_sha256||r.game_date!==date||r.protocol_sha256!==registration.protocol_sha256||r.model_sha256!==registration.model_sha256||Date.parse(r.created_at)>=Date.parse(r.lock_at))throw Error('Invalid trio research record');
  if(new Set(r.players.map(p=>p.id)).size!==r.players.length)throw Error('Ambiguous research player identity');
  return r;
 }catch(e){if(isMissingRelation(e))return null;throw e}
}
export async function getMathResearchDates(){
 try{const r=await db.execute(sql`SELECT DISTINCT game_date::text AS date FROM nba_trio_math_snapshots WHERE protocol_sha256=${registration.protocol_sha256} ORDER BY date DESC LIMIT 60`);return r.rows.map(x=>String(x.date))}
 catch(e){if(isMissingRelation(e))return [];throw e}
}
export async function getMathResearchCounts(){
 try{const r=await db.execute(sql`SELECT count(*)::int AS recorded,count(g.grade)::int AS graded,
   count(*) FILTER (WHERE g.grade->>'full_slate_final'='true'
    AND g.grade->'portfolios'->'market_baseline'->>'status'='resolved'
    AND g.grade->'portfolios'->'joint_best_of_five'->>'status'='resolved'
    AND g.grade->'portfolios'->'robust_best_of_five'->>'status'='resolved')::int AS complete
   FROM nba_trio_math_snapshots s LEFT JOIN LATERAL (SELECT grade FROM nba_trio_math_grades
    WHERE record_sha256=s.record_sha256 AND grade->>'protocol_sha256'=s.protocol_sha256
    ORDER BY graded_at DESC,id DESC LIMIT 1) g ON true WHERE s.protocol_sha256=${registration.protocol_sha256}`);
   const row=r.rows[0];return {recorded:Number(row?.recorded??0),graded:Number(row?.graded??0),complete:Number(row?.complete??0)}
 }catch(e){if(isMissingRelation(e))return {recorded:0,graded:0,complete:0};throw e}
}
export async function getMathOutcomes(record:MathRecord){
 const ids=[...new Set(record.players.map(p=>p.espn_event_id))];
 const result=await db.execute(sql`SELECT p.player,p.espn_event_id,p.played,p.points,p.team,g.home_team,g.away_team,g.completed
   FROM nba_box_score_players p JOIN nba_box_score_games g USING(espn_event_id)
   WHERE p.game_date=${record.game_date}::date AND g.game_date=${record.game_date}::date
   AND p.espn_event_id=ANY(${`{${ids.join(',')}}`}::text[])`);
 const rows=result.rows as {player:string;espn_event_id:string;played:boolean;points:number|null;team:string;home_team:string;away_team:string;completed:boolean}[];
 return record.players.map(p=>{
  const hits=rows.filter(r=>r.espn_event_id===p.espn_event_id&&playerKey(r.player)===p.player_key&&teamKey(r.home_team)===teamKey(p.home_team)&&teamKey(r.away_team)===teamKey(p.away_team)&&[teamKey(p.home_team),teamKey(p.away_team)].includes(teamKey(r.team)));
  const r=hits.length===1?hits[0]:null;
  return {id:p.id,status:r?.completed&&r.played&&r.points!==null?'verified':r?.completed&&!r.played?'dnp':hits.length>1?'ambiguous':'pending',points:r?.completed&&r.played&&r.points!==null?Number(r.points):null};
 });
}


type PoolBenchmark = {
  eligible: number;
  verified: number;
  best_total: number | null;
  trio: { player: string; points: number }[];
};

export type TrioPoolAudit = {
  provisional: boolean;
  full_pool: PoolBenchmark;
  top_12: PoolBenchmark;
  pool_gap: number | null;
  portfolios: Record<string, {
    best_entry: number | null;
    selection_gap: number | null;
    total_gap: number | null;
    fractional_top_three: number | null;
    tie_compatible_top_three: boolean | null;
  }>;
};

export async function getTrioPoolAudit(recordSha: string): Promise<TrioPoolAudit | null> {
  try {
    const result = await db.execute(sql`
      SELECT a.audit
      FROM (
        SELECT grade->>'sha256' AS grade_sha256
        FROM nba_trio_math_grades
        WHERE record_sha256 = ${recordSha}
        ORDER BY graded_at DESC, id DESC LIMIT 1
      ) AS latest
      JOIN nba_trio_pool_audits a USING (grade_sha256)
      WHERE a.record_sha256 = ${recordSha}
        AND a.audit->>'version' = 'nba-trio-pool-audit-v1'
    `);
    return (result.rows[0]?.audit as TrioPoolAudit | undefined) ?? null;
  } catch (error) {
    if (isMissingRelation(error)) return null;
    throw error;
  }
}

/** Only the audit for each indexed original's latest exact grade can count. */
export async function getTrioPoolSummary() {
  try {
    const result = await db.execute(sql`
      SELECT s.game_date::text AS game_date,s.record_sha256,
        a.grade_sha256 IS NOT NULL AS audited,
        (a.audit->>'provisional')::boolean AS provisional,
        (a.audit->>'pool_gap')::double precision AS pool_gap
      FROM nba_trio_math_snapshots s
      LEFT JOIN LATERAL (SELECT grade FROM nba_trio_math_grades g WHERE g.record_sha256=s.record_sha256
        AND g.grade->>'protocol_sha256'=s.protocol_sha256 AND g.grade->>'model_sha256'=s.record->>'model_sha256'
        ORDER BY g.graded_at DESC,g.id DESC LIMIT 1) g ON true
      LEFT JOIN nba_trio_pool_audits a ON a.grade_sha256=g.grade->>'sha256' AND a.record_sha256=s.record_sha256
        AND a.audit->>'version'='nba-trio-pool-audit-v1' AND a.audit->>'mode'='prospective'
        AND a.audit->>'game_date'=s.game_date::text
      WHERE s.protocol_sha256=${registration.protocol_sha256} AND s.record->>'model_sha256'=${registration.model_sha256}
    `);
    const rows=result.rows as (PoolObservation & {audited:boolean})[];
    return {...summarizePoolObservations(rows.filter(r=>r.audited)),pending:rows.filter(r=>!r.audited).length};
  } catch (error) {
    if (isMissingRelation(error)) return {...summarizePoolObservations([]),pending:null};
    throw error;
  }
}
