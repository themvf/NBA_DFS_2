/**
 * Cheat-sheet layout: turns the full ranking board into printable position
 * columns.
 *
 * This is a DIFFERENT artifact from the web rankings table, not a restyling of
 * it. The table is 10 columns x 442 rows with hover-only evidence (indicator
 * tooltips, percentile chips, projection notation); none of that survives on
 * paper. A cheat sheet is position-grouped, tier-broken, and readable at arm's
 * length mid-draft.
 *
 * Two deliberate departures from the on-screen board, both grounded in what the
 * DST backtest actually established (see CLAUDE.md, ff-independent-v1.9 and
 * model/ff_dst_projection_backtest.py):
 *
 *   1. DST prints NO TIER. Our DST projections are shrunk hard toward the
 *      league prior (DST_CARRY_FORWARD_WEIGHT=0.05), so all 32 land in a
 *      ~102-108 band that never trips rank_rows()' 0.88 tier breakpoint and
 *      every defense comes back tier 1. Printing "T1" thirty-two times reads as
 *      "these are unranked", which is wrong -- the ORDER is real (prior-season
 *      carry-forward, Spearman ~0.18-0.26 vs next season). Showing rank plus
 *      the 2025 actual it derives from is the honest presentation.
 *   2. DST carries a FantasyPros delta column. Our order is backward-looking
 *      production; theirs embeds forward-looking judgment. They agree at
 *      Spearman 0.80, and the disagreements are the interesting part, so they
 *      belong on the sheet rather than being averaged away. FantasyPros stays
 *      comparison-only per CLAUDE.md -- it is displayed beside our rank, never
 *      substituted for it.
 */

import type { FantasyRankingRow } from "@/db/queries-fantasy-football";

/** How deep each position prints. Sized so the whole sheet fits one landscape page. */
export const CHEAT_SHEET_DEPTH: Record<string, number> = {
  QB: 24,
  RB: 45,
  WR: 60,
  TE: 24,
  K: 12,
  DST: 32,
};

export const CHEAT_SHEET_POSITIONS = ["QB", "RB", "WR", "TE", "K", "DST"] as const;

/** Positions whose tier column is meaningful. DST is excluded -- see file header. */
export const TIERED_POSITIONS = new Set(["QB", "RB", "WR", "TE", "K"]);

export type CheatSheetEntry = {
  playerId: number;
  positionRank: number;
  name: string;
  team: string | null;
  byeWeek: number | null;
  projectedPoints: number | null;
  priorSeasonPoints: number | null;
  adp: number | null;
  /** ADP minus our overall rank. Positive = available later than we value him. */
  adpDelta: number | null;
  /** DST only: FantasyPros' rank at this position minus ours. Positive = we are higher on him. */
  comparisonDelta: number | null;
  /** Tier break BEFORE this entry, for drawing a rule. Never set for DST. */
  startsNewTier: boolean;
  tier: number | null;
  signal: CheatSheetSignal | null;
};

export type CheatSheetSignal = "buy" | "fade" | "injury" | "rookie";

/**
 * One glyph per row, at most. The board carries indicators on 313 of 442
 * players (OUR_FADE alone hits 129), so printing every badge would gray out the
 * page and defeat the point. Order is by draft-day consequence: an injury
 * changes whether you draft him at all, buy/fade changes when.
 */
const SIGNAL_PRIORITY: Array<{ codes: string[]; signal: CheatSheetSignal }> = [
  { codes: ["INJURY"], signal: "injury" },
  { codes: ["OUR_BUY"], signal: "buy" },
  { codes: ["OUR_FADE"], signal: "fade" },
  { codes: ["ROOKIE"], signal: "rookie" },
];

export const SIGNAL_GLYPH: Record<CheatSheetSignal, string> = {
  buy: "+",
  fade: "-",
  injury: "!",
  rookie: "R",
};

export const SIGNAL_LABEL: Record<CheatSheetSignal, string> = {
  buy: "value vs ADP",
  fade: "costs more than we'd pay",
  injury: "injury designation",
  rookie: "rookie",
};

function pickSignal(row: FantasyRankingRow): CheatSheetSignal | null {
  const codes = new Set(row.indicators.map((indicator) => indicator.code));
  for (const { codes: candidates, signal } of SIGNAL_PRIORITY) {
    if (candidates.some((code) => codes.has(code))) return signal;
  }
  return null;
}

/**
 * FantasyPros rank WITHIN a position, derived from their ingested projections.
 *
 * Deliberately derived from `fantasyProsProjectedPoints` rather than a pasted
 * ECR list: the projections arrive through the normal snapshot pipeline with
 * real provenance and refresh themselves, whereas a hardcoded ECR ordering goes
 * stale silently. Note these are two different FantasyPros products and do not
 * agree exactly -- this column is labelled as their projection rank, not ECR.
 */
function fantasyProsRanks(rows: FantasyRankingRow[]): Map<number, number> {
  const ranked = rows
    .filter((row) => row.fantasyProsProjectedPoints !== null)
    .sort((a, b) => (b.fantasyProsProjectedPoints ?? 0) - (a.fantasyProsProjectedPoints ?? 0));
  return new Map(ranked.map((row, index) => [row.playerId, index + 1]));
}

/** Board order within a position: our rank when present, ECR as the fallback. */
function rankOf(row: FantasyRankingRow): number {
  return row.ourRank ?? row.ecr ?? Number.MAX_SAFE_INTEGER;
}

export type CheatSheetColumn = {
  position: string;
  entries: CheatSheetEntry[];
  /** True when this position's tier column is suppressed (DST). */
  tiersSuppressed: boolean;
};

export function buildCheatSheet(rankings: FantasyRankingRow[]): CheatSheetColumn[] {
  return CHEAT_SHEET_POSITIONS.map((position) => {
    const pool = rankings
      .filter((row) => row.position === position)
      .sort((a, b) => rankOf(a) - rankOf(b));
    const comparison = position === "DST" ? fantasyProsRanks(pool) : new Map<number, number>();
    const depth = CHEAT_SHEET_DEPTH[position] ?? 24;
    const tiersSuppressed = !TIERED_POSITIONS.has(position);

    let previousTier: number | null = null;
    const entries = pool.slice(0, depth).map((row, index) => {
      const positionRank = index + 1;
      const tier = tiersSuppressed ? null : row.tier;
      const startsNewTier = tier !== null && previousTier !== null && tier !== previousTier;
      previousTier = tier;
      const overall = row.ourRank ?? row.ecr;
      const fpRank = comparison.get(row.playerId) ?? null;
      return {
        playerId: row.playerId,
        positionRank,
        name: row.name,
        team: row.team,
        byeWeek: row.byeWeek,
        projectedPoints: row.ourProjectedPoints,
        priorSeasonPoints: row.fantasyPoints2025,
        adp: row.adp,
        adpDelta: row.adp !== null && overall !== null ? row.adp - overall : null,
        comparisonDelta: fpRank !== null ? fpRank - positionRank : null,
        startsNewTier,
        tier,
        signal: pickSignal(row),
      };
    });
    return { position, entries, tiersSuppressed };
  });
}
