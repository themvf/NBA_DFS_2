import "server-only";
import { sql } from "drizzle-orm";
import { db } from "@/db";
import type { RosterEvidence } from "@/lib/nfl-dfs/availability";

export async function getNflRosterEvidence(season: number, week?: number | null): Promise<Map<number, RosterEvidence>> {
  const result = await db.execute(sql`SELECT id, team_abbrev, position, fetched_at,
    metadata->'sleeper' AS sleeper FROM ff_players WHERE season=${season}`);
  const roster = new Map<number, RosterEvidence>(result.rows.map(r => [Number(r.id), { team: String(r.team_abbrev ?? ""), position: String(r.position), fetchedAt: new Date(r.fetched_at as string).toISOString(), sleeper: r.sleeper }]));
  if (!week) return roster;
  try {
    const observations = await db.execute(sql`SELECT DISTINCT ON (o.player_id, o.source)
      o.id, o.player_id, o.source, CASE WHEN o.source='nfl_official' THEN o.raw_payload->>'status' WHEN NULLIF(TRIM(o.source_status),'') IS NULL THEN 'UNKNOWN' ELSE o.normalized_status END normalized_status, o.practice_status, o.observed_at,
      o.provider_updated_at, o.response_hash,
      o.raw_payload->>'report_type' report_type, o.raw_payload->>'kickoff' kickoff, o.raw_payload->>'url' url,
      o.raw_payload->>'injury_update_date' unverified_update,
      o.raw_payload->>'practice_1' practice_1, o.raw_payload->>'practice_2' practice_2, o.raw_payload->>'practice_3' practice_3,
      COALESCE(o.raw_payload->>'team_id', o.raw_payload->>'team', '') AS team
      FROM ff_player_injury_observations o JOIN ff_source_snapshots s ON s.id=o.source_snapshot_id
      WHERE o.season=${season} AND o.source IN ('fantasypros','nfl_official')
        AND s.request_params->>'week'=${String(week)} AND o.observed_at<=NOW()
      ORDER BY o.player_id, o.source, o.observed_at DESC, o.id DESC`);
    for (const row of observations.rows) {
      const player = roster.get(Number(row.player_id));
      if (player) player.injuries = [...(player.injuries ?? []), {id: String(row.id), source: String(row.source), status: String(row.normalized_status),
        practice: row.practice_status ? String(row.practice_status) : [1,2,3].filter(i => row[`practice_${i}`] != null).map(i => `Report ${i}: ${row[`practice_${i}`]}`).join('; ') || null, team: String(row.team), week,
        observedAt: new Date(row.observed_at as string).toISOString(), updatedAt: row.provider_updated_at ? new Date(row.provider_updated_at as string).toISOString() : null,
        hash: String(row.response_hash), unverifiedUpdate: row.unverified_update ? String(row.unverified_update) : undefined, reportType: row.report_type ? String(row.report_type) : undefined, kickoff: row.kickoff ? String(row.kickoff) : undefined, url: row.url ? String(row.url) : undefined}];
    }
  } catch { for (const player of roster.values()) player.injuryReadFailed = true; }
  const games = await db.execute(sql`SELECT g.kickoff, h.abbreviation home, a.abbreviation away
    FROM nfl_season_games g JOIN nfl_teams h ON h.team_id=g.home_team_id JOIN nfl_teams a ON a.team_id=g.away_team_id
    WHERE g.season=${season} AND g.week=${week} AND g.game_type='REG'`);
  for (const player of roster.values()) {
    const matches = games.rows.filter(g => g.home === player.team || g.away === player.team);
    player.kickoff = matches.length === 1 && matches[0].kickoff ? new Date(matches[0].kickoff as string).toISOString() : null;
  }
  return roster;
}
