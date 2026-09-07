import 'server-only';
import {sql} from 'drizzle-orm';
import {db} from '@/db';
import type {NflRosterIdentity} from '@/lib/nfl-dfs/identity';

/** Durable aliases identify the person; this season's roster supplies team/role. */
export async function getNflIdentityRoster(season:number): Promise<{available:boolean; candidates:NflRosterIdentity[]}> {
  const existence=await db.execute(sql`SELECT to_regclass('nfl_player_identity_crosswalk') AS name`);
  if (!existence.rows[0]?.name) return {available:false,candidates:[]};
  const result=await db.execute(sql`SELECT p.id, p.canonical_name, p.team_abbrev, p.position, p.gsis_id,p.fetched_at,
    c.status, c.gsis_id AS resolved_gsis,
    COALESCE(array_agg(DISTINCT a.player_name) FILTER (WHERE a.player_name IS NOT NULL), ARRAY[]::text[]) AS aliases,
    COALESCE(array_agg(DISTINCT a.claim_digest) FILTER (WHERE a.claim_digest IS NOT NULL), ARRAY[]::text[]) AS claim_digests
    FROM ff_players p
    LEFT JOIN nfl_player_identity_crosswalk c ON c.namespace='app.ff_players' AND c.external_id=p.id::text
    LEFT JOIN nfl_player_identity_claims a ON a.gsis_id=COALESCE(c.gsis_id,p.gsis_id)
    WHERE p.season=${season}
    GROUP BY p.id,p.canonical_name,p.team_abbrev,p.position,p.gsis_id,p.fetched_at,c.status,c.gsis_id`);
  return {available:true,candidates:result.rows.map(r=>({
    name:String(r.canonical_name),team:r.team_abbrev ? String(r.team_abbrev):null,position:String(r.position),
    gsisId:r.resolved_gsis ? String(r.resolved_gsis):null,
    sourceGsisId:r.gsis_id ? String(r.gsis_id):null,
    aliases:Array.isArray(r.aliases)?r.aliases.map(String):[],
    localPlayerId:Number(r.id),fetchedAt:new Date(r.fetched_at as string).toISOString(),
    claimDigests:Array.isArray(r.claim_digests)?r.claim_digests.map(String):[],
    registryStatus:r.status==='resolved'&&(!r.gsis_id||r.resolved_gsis===r.gsis_id)?'resolved':r.status==='conflict'||r.status==='resolved'?'conflict':'unregistered',
  }))};
}
