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

export type CheatSheetVariant = "rankings" | "bestball" | "redraft";

export type CheatSheetVariantConfig = {
  /** Printed in the sheet header so a sheet on the table identifies itself. */
  label: string;
  /** Draft context line -- teams, rounds, and what the depth is sized against. */
  context: string;
  positions: readonly string[];
  /** How deep each position prints. */
  depth: Record<string, number>;
  /**
   * Display overrides. Yahoo's UI calls the DST slot "DEF"; the redraft board
   * already shows that term (REDRAFT_POSITION_LABEL), and the printed sheet has
   * to match what is on screen on draft day. The underlying position code stays
   * "DST" everywhere -- this is presentation only.
   */
  positionLabel?: Record<string, string>;
  /**
   * Rows before a position spills into a continuation column. Best Ball drafts
   * 96 receivers deep, which cannot fit in a single printed column, so a deep
   * position spans several.
   */
  maxRowsPerColumn: number;
};

/**
 * Depths are sized against the real draft each variant serves, not picked to
 * look tidy: total rows are roughly (teams x rounds) split by the positional mix
 * that format actually drafts, so the sheet does not run out before the last
 * round does.
 */
export const CHEAT_SHEET_VARIANTS: Record<CheatSheetVariant, CheatSheetVariantConfig> = {
  // The general board: every position, sized to the meaningful part of a
  // typical 12-team draft rather than to any one league's roster.
  rankings: {
    label: "Draft Cheat Sheet",
    context: "All positions",
    positions: ["QB", "RB", "WR", "TE", "K", "DST"],
    depth: { QB: 24, RB: 45, WR: 60, TE: 24, K: 12, DST: 32 },
    maxRowsPerColumn: 60,
  },
  // DraftKings Best Ball: 12 x 20 = 240 picks, QB/RB/WR/TE only (kickers and
  // defenses are not draftable in this format at all). Depth follows
  // BEST_BALL_TARGETS (QB3/RB6/WR8/TE3) x 12 teams = 36/72/96/36 = 240.
  bestball: {
    label: "Best Ball Cheat Sheet",
    context: "DraftKings · 12 teams · 20 rounds · QB/RB/WR/TE only",
    positions: ["QB", "RB", "WR", "TE"],
    depth: { QB: 36, RB: 72, WR: 96, TE: 36 },
    maxRowsPerColumn: 48,
  },
  // Yahoo redraft: 10 x 15 = 150 picks, and K/DEF are real roster slots here.
  // Shallower than Best Ball because it is a 10-team league with 15 rounds.
  redraft: {
    label: "Redraft Cheat Sheet",
    context: "Yahoo · 10 teams · 15 rounds · full PPR",
    positions: ["QB", "RB", "WR", "TE", "K", "DST"],
    depth: { QB: 20, RB: 40, WR: 50, TE: 18, K: 12, DST: 15 },
    positionLabel: { DST: "DEF" },
    maxRowsPerColumn: 50,
  },
};

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
  /** What to print at the top of the column (e.g. "DEF" for Yahoo redraft). */
  label: string;
  entries: CheatSheetEntry[];
  /** True when this position's tier column is suppressed (DST). */
  tiersSuppressed: boolean;
  /** True when this column continues a position that spilled from the previous one. */
  continued: boolean;
};

export function buildCheatSheet(
  rankings: FantasyRankingRow[],
  variant: CheatSheetVariant = "rankings",
): CheatSheetColumn[] {
  const config = CHEAT_SHEET_VARIANTS[variant];
  const columns: CheatSheetColumn[] = [];

  for (const position of config.positions) {
    const pool = rankings
      .filter((row) => row.position === position)
      .sort((a, b) => rankOf(a) - rankOf(b));
    // FantasyPros ranks are computed over the WHOLE position pool, before the
    // print depth cut. Ranking only the printed subset would silently reshuffle
    // their order and make the delta wrong.
    const comparison = position === "DST" ? fantasyProsRanks(pool) : new Map<number, number>();
    const tiersSuppressed = !TIERED_POSITIONS.has(position);
    const label = config.positionLabel?.[position] ?? position;

    let previousTier: number | null = null;
    const entries = pool.slice(0, config.depth[position] ?? 24).map((row, index) => {
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

    // Spill a deep position across however many columns it needs. An empty pool
    // still emits one column so the printed grid never silently loses a
    // position the format actually drafts.
    const perColumn = Math.max(1, config.maxRowsPerColumn);
    const chunkCount = Math.max(1, Math.ceil(entries.length / perColumn));
    for (let chunk = 0; chunk < chunkCount; chunk += 1) {
      columns.push({
        position,
        label,
        entries: entries.slice(chunk * perColumn, (chunk + 1) * perColumn),
        tiersSuppressed,
        continued: chunk > 0,
      });
    }
  }
  return columns;
}
