import "server-only";

import { sql } from "drizzle-orm";
import { db } from "@/db";

/**
 * Market-anchored leader shares for one game (nfl-leaders-anchored-test-v1,
 * docs/nfl-leaders-anchored-test.md), written by research.nfl_game_leaders_anchored
 * after each pregame prop capture. A test number shown beside our own share,
 * never in place of it.
 */
export type GameLeaderAnchored = {
  computedAt: string;
  propCapturedAt: string;
  origin: "prospective" | "discovery";
  shares: Map<string, Map<string, number>>; // metric -> identity -> share
};

const iso = (v: unknown) => (v instanceof Date ? v.toISOString() : String(v));

export async function getGameLeaderAnchored(gameId: string, forecastDecisionAt: string): Promise<GameLeaderAnchored | null> {
  // Live pricing first; a game priced only after the fact shows its re-created discovery row.
  const result = await db.execute(sql`
    SELECT metric, identity, anchored_share, computed_at, prop_captured_at, origin, forecast_decision_at
    FROM nfl_game_leader_anchored
    WHERE game_id = ${gameId}
      AND computed_at = (SELECT computed_at FROM nfl_game_leader_anchored WHERE game_id = ${gameId}
                         ORDER BY (origin = 'prospective') DESC, computed_at DESC LIMIT 1)`);
  const rows = result.rows as Record<string, unknown>[];
  if (rows.length === 0) return null;
  const first = rows[0];
  // A prospective row priced on a different saved forecast would mislabel this page's numbers.
  if (first.origin === "prospective" && Date.parse(iso(first.forecast_decision_at)) !== Date.parse(forecastDecisionAt)) return null;
  const shares = new Map<string, Map<string, number>>();
  for (const r of rows) {
    const metric = String(r.metric);
    if (!shares.has(metric)) shares.set(metric, new Map());
    shares.get(metric)!.set(String(r.identity), Number(r.anchored_share));
  }
  return { computedAt: iso(first.computed_at), propCapturedAt: iso(first.prop_captured_at),
           origin: first.origin === "discovery" ? "discovery" : "prospective", shares };
}
