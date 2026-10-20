import 'server-only';
import {sql} from 'drizzle-orm';
import {db} from '@/db';
import type {NflRosterIdentity} from '@/lib/nfl-dfs/identity';

/** Durable aliases identify the person; this season's roster supplies team/role. */
export async function getNflIdentityRoster(season:number): Promise<{available:boolean; candidates:NflRosterIdentity[]}> {
  const existence=await db.execute(sql`SELECT to_regclass('nfl_player_identity_crosswalk') AS name`);
  if (!existence.rows[0]?.name) return {available:false,candidates:[]};
  // Aliases come from nfl_identity_gsis_names (gsis_id, player_name), index-only, for this
  // season's players alone. Joining the claims table on COALESCE(crosswalk, roster) gsis could
  // not use an index, so every page open read the whole 169 MB table from disk (~150 MB) and
  // also shipped every claim digest, which nothing reads (2026-10-20).
  const result=await db.execute(sql`WITH ps AS MATERIALIZED (
      SELECT p.id, p.canonical_name, p.team_abbrev, p.position, p.gsis_id, p.fetched_at, c.status, c.gsis_id AS resolved_gsis
      FROM ff_players p
      LEFT JOIN nfl_player_identity_crosswalk c ON c.namespace='app.ff_players' AND c.external_id=p.id::text
      WHERE p.season=${season}
    ), names AS (
      SELECT a.gsis_id, array_agg(DISTINCT a.player_name) AS aliases FROM nfl_player_identity_claims a
      WHERE a.gsis_id IN (SELECT COALESCE(resolved_gsis, gsis_id) FROM ps) GROUP BY a.gsis_id
    )
    SELECT ps.id, ps.canonical_name, ps.team_abbrev, ps.position, ps.gsis_id, ps.fetched_at, ps.status, ps.resolved_gsis,
      COALESCE(n.aliases, ARRAY[]::text[]) AS aliases
    FROM ps LEFT JOIN names n ON n.gsis_id=COALESCE(ps.resolved_gsis, ps.gsis_id)`);
  return {available:true,candidates:result.rows.map(r=>({
    name:String(r.canonical_name),team:r.team_abbrev ? String(r.team_abbrev):null,position:String(r.position),
    gsisId:r.resolved_gsis ? String(r.resolved_gsis):null,
    sourceGsisId:r.gsis_id ? String(r.gsis_id):null,
    aliases:Array.isArray(r.aliases)?r.aliases.map(String):[],
    localPlayerId:Number(r.id),fetchedAt:new Date(r.fetched_at as string).toISOString(),
    registryStatus:r.status==='resolved'&&(!r.gsis_id||r.resolved_gsis===r.gsis_id)?'resolved':r.status==='conflict'||r.status==='resolved'?'conflict':'unregistered',
  }))};
}
