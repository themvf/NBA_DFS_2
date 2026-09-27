import "server-only";
import { sql } from "drizzle-orm";
import { db } from "@/db";
import { pfrGame, type PfrEvidence } from "@/lib/nfl/pickem-pfr";

/** Select captures available before each target kickoff, including explicit missing games. */
export async function getPickemPfr(season: number, loadedAt: string): Promise<Record<number, PfrEvidence[]>> {
  const result = await db.execute(sql`
    SELECT target.id target_id, t.abbreviation team, prior.nflverse_game_id prior_game_id,
      prior.week, s.captured_at, s.snapshot_id, s.recorded_at, s.parser_version, s.source_sha256
    FROM nfl_season_games target
    JOIN nfl_teams t ON t.team_id IN (target.home_team_id,target.away_team_id)
    LEFT JOIN LATERAL (
      SELECT g.nflverse_game_id,g.week FROM nfl_season_games g
      WHERE g.season=target.season AND g.completed=TRUE AND g.kickoff<target.kickoff
        AND g.kickoff<${loadedAt}::timestamptz
        AND t.team_id IN (g.home_team_id,g.away_team_id)
      ORDER BY g.kickoff DESC LIMIT 4
    ) prior ON TRUE
    LEFT JOIN LATERAL (
      SELECT captured_at,snapshot_id,recorded_at,parser_version,source_sha256 FROM nfl_pfr_game_snapshots
      WHERE game_id=prior.nflverse_game_id AND captured_at<target.kickoff
        AND captured_at<=${loadedAt}::timestamptz
        AND recorded_at<target.kickoff AND recorded_at<=${loadedAt}::timestamptz
      ORDER BY captured_at DESC,snapshot_id DESC LIMIT 1
    ) s ON TRUE
    WHERE target.season=${season}
    ORDER BY target.id,t.abbreviation,prior.week DESC`);
  // A season reuses the same prior games many times. Fetch each payload once,
  // rather than exceeding the database HTTP response limit with duplicate JSON.
  const ids = [...new Set(result.rows.filter(r => r.snapshot_id != null).map(r => Number(r.snapshot_id)))];
  const payloads = ids.length ? await db.execute(sql`SELECT snapshot_id,payload FROM nfl_pfr_game_snapshots
    WHERE snapshot_id IN (${sql.join(ids.map(id => sql`${id}`), sql`, `)})`) : { rows: [] };
  const byId = new Map(payloads.rows.map(r => [Number(r.snapshot_id), r.payload]));
  const out: Record<number, PfrEvidence[]> = {};
  for (const r of result.rows) {
    const target = Number(r.target_id), team = String(r.team);
    const list = out[target] ??= [];
    let evidence = list.find(x => x.team === team);
    if (!evidence) { evidence = { team, games: [] }; list.push(evidence); }
    if (r.prior_game_id) {
      const game = pfrGame(String(r.prior_game_id), Number(r.week),
        r.captured_at ? new Date(String(r.captured_at)).toISOString() : null, byId.get(Number(r.snapshot_id)),
        r.snapshot_id ? { snapshotId: Number(r.snapshot_id), recordedAt: r.recorded_at ? new Date(String(r.recorded_at)).toISOString() : null,
          parserVersion: r.parser_version ? String(r.parser_version) : null, sourceSha256: r.source_sha256 ? String(r.source_sha256) : null } : undefined);
      game.players = game.players.filter(p => p.team === team || p.section === "passing_advanced");
      evidence.games.push(game);
    }
  }
  return out;
}
