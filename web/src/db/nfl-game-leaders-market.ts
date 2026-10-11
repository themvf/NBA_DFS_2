import "server-only";

import { sql } from "drizzle-orm";
import { db } from "@/db";

/** One player's DraftKings line and the leader share it implies, for one market. */
export type GameLeaderMarketRow = {
  metric: string;
  identity: string;
  player: string;
  team: string | null;
  book: string | null;
  line: number | null;
  overPrice: number | null;
  underPrice: number | null;
  fairOver: number | null;
  modelPOver: number | null;
  modelLeaderShare: number;
  marketLeaderShare: number;
};

export type GameLeaderMarket = {
  propCapturedAt: string;
  computedAt: string;
  forecastDecisionAt: string;
  rows: GameLeaderMarketRow[];
};

const num = (v: unknown) => (v == null ? null : Number(v));
const iso = (v: unknown) => (v instanceof Date ? v.toISOString() : String(v));

/**
 * Latest stored pricing for a game (written by research.nfl_game_leaders_market
 * refresh after each pregame prop capture). Null when none exists or the table
 * cannot be read; the page then says the pricing is unavailable.
 */
export async function getGameLeaderMarket(gameId: string, forecastDecisionAt: string): Promise<GameLeaderMarket | null> {
  const result = await db.execute(sql`
    SELECT metric, identity, player, team, book, line, over_price, under_price, fair_over, model_p_over,
           model_leader_share, market_leader_share, prop_captured_at, computed_at, forecast_decision_at
    FROM nfl_game_leader_market
    WHERE game_id = ${gameId}
      AND computed_at = (SELECT max(computed_at) FROM nfl_game_leader_market WHERE game_id = ${gameId})`);
  const rows = result.rows as Record<string, unknown>[];
  if (rows.length === 0) return null;
  const first = rows[0];
  // Pricing applied to a different saved forecast would mislabel this page's numbers.
  if (Date.parse(iso(first.forecast_decision_at)) !== Date.parse(forecastDecisionAt)) return null;
  return {
    propCapturedAt: iso(first.prop_captured_at),
    computedAt: iso(first.computed_at),
    forecastDecisionAt: iso(first.forecast_decision_at),
    rows: rows.map((r) => ({
      metric: String(r.metric),
      identity: String(r.identity),
      player: String(r.player),
      team: r.team == null ? null : String(r.team),
      book: r.book == null ? null : String(r.book),
      line: num(r.line),
      overPrice: num(r.over_price),
      underPrice: num(r.under_price),
      fairOver: num(r.fair_over),
      modelPOver: num(r.model_p_over),
      modelLeaderShare: Number(r.model_leader_share),
      marketLeaderShare: Number(r.market_leader_share),
    })),
  };
}
