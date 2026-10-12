import "server-only";

import { sql } from "drizzle-orm";
import { db } from "@/db";
import { ANYTIME_TD_MARGIN, impliedProb } from "@/lib/nfl-dfs/market-anchor";

/** One player's anytime-touchdown market for a game, from the captured prop lines. */
export type AnytimeTdRow = {
  player: string;
  dkPrice: number | null;
  /** DraftKings' price as a probability, margin included. */
  dkImplied: number | null;
  /** The same with the ~8% margin removed (the market anchor's rule). */
  fairProb: number | null;
  bestBook: string | null;
  bestPrice: number | null;
  /** DraftKings' price for two or more touchdowns. */
  twoPlusPrice: number | null;
};

export type AnytimeTdMarket = { capturedAt: string; rows: AnytimeTdRow[] };

const BOOKS = ["draftkings", "fanduel", "betmgm", "williamhill_us", "fanatics", "pinnacle"];
type Quote = { yes?: number | null; outcomes?: { point?: number | null; price?: number | null }[] } | null;

/**
 * The latest pregame anytime-TD and 2+ TD lines for one game, matched to the
 * prop event by both teams' Odds API names and a kickoff within two hours.
 * Null when nothing has been captured for the game.
 */
export async function getAnytimeTouchdowns(home: string, away: string, kickoff: string): Promise<AnytimeTdMarket | null> {
  const result = await db.execute(sql`
    WITH teams AS (
      SELECT (SELECT COALESCE(odds_api_name, name) FROM nfl_teams WHERE abbreviation = ${home} LIMIT 1) AS home_name,
             (SELECT COALESCE(odds_api_name, name) FROM nfl_teams WHERE abbreviation = ${away} LIMIT 1) AS away_name)
    SELECT DISTINCT ON (p.player, p.market) p.player, p.market, p.captured_at, p.books
    FROM prop_odds_history p, teams t
    WHERE p.sport = 'nfl' AND p.market IN ('player_anytime_td', 'player_tds_over')
      AND p.home_team_name = t.home_name AND p.away_team_name = t.away_name
      AND ABS(EXTRACT(EPOCH FROM (p.commence_time - ${kickoff}::timestamptz))) < 7200
      AND p.captured_at < p.commence_time
    ORDER BY p.player, p.market, p.captured_at DESC`);
  if (!result.rows.length) return null;
  const byPlayer = new Map<string, AnytimeTdRow>();
  let captured = "";
  for (const r of result.rows as { player: string; market: string; captured_at: unknown; books: Record<string, Quote> | null }[]) {
    const at = r.captured_at instanceof Date ? r.captured_at.toISOString() : String(r.captured_at);
    if (at > captured) captured = at;
    const row = byPlayer.get(r.player) ?? { player: r.player, dkPrice: null, dkImplied: null, fairProb: null, bestBook: null, bestPrice: null, twoPlusPrice: null };
    const books = r.books ?? {};
    if (r.market === "player_anytime_td") {
      const dk = books.draftkings?.yes ?? null;
      row.dkPrice = dk;
      row.dkImplied = dk == null ? null : impliedProb(dk);
      row.fairProb = row.dkImplied == null ? null : Math.min(row.dkImplied / ANYTIME_TD_MARGIN, 0.95);
      for (const book of BOOKS) {
        const price = books[book]?.yes;
        if (price != null && (row.bestPrice == null || price > row.bestPrice)) { row.bestPrice = price; row.bestBook = book; }
      }
    } else {
      row.twoPlusPrice = books.draftkings?.outcomes?.find((o) => o.point === 1.5)?.price ?? null;
    }
    byPlayer.set(r.player, row);
  }
  const rows = [...byPlayer.values()].filter((r) => r.dkPrice != null || r.bestPrice != null)
    .sort((a, b) => (b.fairProb ?? -1) - (a.fairProb ?? -1) || a.player.localeCompare(b.player));
  return { capturedAt: captured, rows };
}
