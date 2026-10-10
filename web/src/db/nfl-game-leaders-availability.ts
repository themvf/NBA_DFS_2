import "server-only";

import { sql } from "drizzle-orm";
import { db } from "@/db";
import type { ConfirmedOutPlayer } from "@/lib/nfl/game-leaders-availability";

type SnapshotRow = { source: unknown; fetched_at: unknown };
type InjuryRow = {
  identity: unknown;
  name: unknown;
  team: unknown;
  sleeper_injury: unknown;
  fp_status: unknown;
  fp_observed_at: unknown;
  official_status: unknown;
  official_observed_at: unknown;
};

export type GameLeadersAvailability = {
  checkedAt: string;
  complete: boolean;
  confirmedOut: ConfirmedOutPlayer[];
};

/** Check fresh, week-matched availability without changing saved simulations. */
export async function getGameLeadersAvailability(
  season: number,
  week: number,
  away: string,
  home: string,
  kickoff: string,
): Promise<GameLeadersAvailability> {
  const checkedAt = new Date().toISOString();
  const weekDataset = `game-week-injuries-v2-${season}-${week}`;
  const captures = await db.execute(sql`
    SELECT DISTINCT ON (source) source, fetched_at
    FROM ff_source_snapshots
    WHERE season = ${season} AND status = 'success'
      AND ((source = 'sleeper' AND dataset = 'players')
        OR (source = 'fantasypros' AND dataset = ${weekDataset}))
      AND fetched_at <= ${checkedAt}::timestamptz
    ORDER BY source, fetched_at DESC, id DESC
  `);
  const freshnessMs = 48 * 60 * 60 * 1000;
  const fresh = new Set(
    (captures.rows as SnapshotRow[])
      .filter((row) => Date.parse(String(row.fetched_at)) >= Date.parse(checkedAt) - freshnessMs)
      .map((row) => String(row.source)),
  );
  if (!fresh.has("sleeper") || !fresh.has("fantasypros")) {
    return { checkedAt, complete: false, confirmedOut: [] };
  }

  const rows = await db.execute(sql`
    WITH fp AS (
      SELECT DISTINCT ON (i.player_id) i.player_id, i.normalized_status, i.observed_at
      FROM ff_player_injury_observations i
      JOIN ff_source_snapshots s ON s.id = i.source_snapshot_id
      WHERE i.season = ${season} AND i.source = 'fantasypros'
        AND s.request_params->>'week' = ${String(week)}
        AND i.observed_at <= ${checkedAt}::timestamptz
        AND i.observed_at < ${kickoff}::timestamptz
      ORDER BY i.player_id, i.observed_at DESC, i.id DESC
    ), official AS (
      SELECT DISTINCT ON (i.player_id) i.player_id, i.normalized_status, i.observed_at
      FROM ff_player_injury_observations i
      JOIN ff_source_snapshots s ON s.id = i.source_snapshot_id
      WHERE i.season = ${season} AND i.source = 'nfl_official'
        AND s.week = ${week}
        AND i.observed_at <= ${checkedAt}::timestamptz
        AND i.observed_at < ${kickoff}::timestamptz
      ORDER BY i.player_id, i.observed_at DESC, i.id DESC
    )
    SELECT p.gsis_id identity, p.canonical_name name, p.team_abbrev team,
      COALESCE(NULLIF(p.metadata->'sleeper'->>'injury_status', ''), p.metadata->'sleeper'->>'status') sleeper_injury,
      fp.normalized_status fp_status, fp.observed_at fp_observed_at,
      official.normalized_status official_status,
      official.observed_at official_observed_at
    FROM ff_players p
    LEFT JOIN fp ON fp.player_id = p.id
    LEFT JOIN official ON official.player_id = p.id
    WHERE p.season = ${season} AND p.team_abbrev IN (${away}, ${home})
      AND p.gsis_id IS NOT NULL
  `);
  const ruledOut = new Set(["OUT", "IR", "PUP", "NFI", "SUSPENDED", "INACTIVE"]);
  const confirmedOut: ConfirmedOutPlayer[] = [];
  for (const row of rows.rows as InjuryRow[]) {
    const official = ruledOut.has(String(row.official_status ?? "").toUpperCase());
    const dual = ruledOut.has(String(row.fp_status ?? "").toUpperCase())
      && ruledOut.has(String(row.sleeper_injury ?? "").toUpperCase());
    if (!official && !dual) continue;
    const observed = official ? row.official_observed_at : row.fp_observed_at;
    confirmedOut.push({
      identity: String(row.identity), name: String(row.name), team: String(row.team),
      observedAt: new Date(String(observed)).toISOString(),
      source: official ? "Official" : "Sleeper and FantasyPros",
    });
  }
  return { checkedAt, complete: true, confirmedOut };
}
