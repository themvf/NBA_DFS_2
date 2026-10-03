import type { CfbBookMap, CfbBookQuote, CfbTerminalRow } from "@/db/queries";
import { SPORTSBOOK_NAMES, selectedSportsbooks } from "@/lib/sportsbook-policy";

/**
 * Favorite Watch — descriptive filter, not a signal.
 *
 * Lists UPCOMING games whose current moneyline favorite is priced inside a
 * probability band and whose no-vig win probability has FALLEN since the
 * opening capture (the market walked toward the underdog).
 *
 * Motivation (2026-10-03, 262 completed 2026 FBS games with a verified close):
 * favorites beat their closing price in every band (+4.8% ROI at close), and
 * the 35 favorites whose probability fell >= 2pp went 27-8 against a 65.8%
 * expectation; favorites closing 51-60% ran +8.1% ROI (n=43). That is one
 * partial season and a 2.6-SD pattern of the kind that regresses. The constants below are FROZEN so the list can be graded
 * against future results; do not tune them against 2026 outcomes. Changing
 * any of them is a new version.
 *
 * v2 (2026-10-03): the v1 "at least 3 two-sided books" floor was replaced by an
 * anchor rule: Pinnacle or DraftKings must quote both sides at open and now.
 */
export const CFB_FAVORITE_WATCH_VERSION = "cfb-favorite-watch-v2";
export const FAVORITE_WATCH_MIN_PROB = 0.51;   // inclusive, current consensus
export const FAVORITE_WATCH_MAX_PROB = 0.60;   // exclusive (user choice 2026-10-03: no favorite above 60%)
export const FAVORITE_WATCH_MIN_DROP_PP = 2.0; // open -> current, percentage points
export const FAVORITE_WATCH_ANCHOR_BOOKS = ["pinnacle", "draftkings"] as const; // one of these must quote both sides at open AND now (v2: replaced the v1 3-book floor, user choice 2026-10-03)

export type FavoriteWatchRow = {
  matchupId: number;
  awayTeam: string;
  homeTeam: string;
  commenceTime: string | null;
  network: string | null;
  favorite: "home" | "away";
  favoriteTeam: string;
  underdogTeam: string;
  openProb: number;        // favorite no-vig consensus at opening capture
  currentProb: number;     // favorite no-vig consensus at latest capture
  dropPp: number;          // (open - current) * 100, positive = got cheaper
  openingCapturedAt: string | null;
  latestCapturedAt: string | null;
  openBooks: number;
  currentBooks: number;
  pinnacleProb: number | null;        // favorite no-vig at Pinnacle, latest capture
  bestPrice: { book: string; price: number } | null; // best favorite ML among selected books
};

export type FavoriteWatchExclusion =
  | "completed" | "kicked_off" | "no_opening" | "no_current" | "no_anchor_book"
  | "favorite_flipped" | "outside_band" | "did_not_cheapen";

export type FavoriteWatchResult = {
  version: string;
  rows: FavoriteWatchRow[];
  excluded: Record<FavoriteWatchExclusion, number>;
};

function probability(price: number): number {
  return price > 0 ? 100 / (price + 100) : Math.abs(price) / (Math.abs(price) + 100);
}

function fairHome(book: CfbBookQuote): number | null {
  if (book.ml_home == null || book.ml_away == null) return null;
  const home = probability(Number(book.ml_home));
  const away = probability(Number(book.ml_away));
  return home + away > 0 ? home / (home + away) : null;
}

function lowerMedian(values: number[]): number | null {
  if (!values.length) return null;
  const ordered = [...values].sort((a, b) => a - b);
  return ordered[Math.floor((ordered.length - 1) / 2)];
}

/** Lower-median no-vig HOME probability across the selected sportsbooks, plus book count. */
export function consensusHome(books: CfbBookMap | null | undefined): { prob: number | null; books: number } {
  const values = Object.values(selectedSportsbooks(books)).flatMap((book) => {
    const value = fairHome(book);
    return value == null ? [] : [value];
  });
  return { prob: lowerMedian(values), books: values.length };
}

/** True when Pinnacle or DraftKings quotes BOTH moneyline sides in this capture. */
export function hasAnchorBook(books: CfbBookMap | null | undefined): boolean {
  return FAVORITE_WATCH_ANCHOR_BOOKS.some((key) => fairHome(books?.[key] ?? {}) != null);
}

function bestFavoritePrice(books: CfbBookMap | null | undefined, favorite: "home" | "away"): { book: string; price: number } | null {
  let best: { book: string; price: number } | null = null;
  for (const [key, quote] of Object.entries(selectedSportsbooks(books))) {
    const price = favorite === "home" ? quote.ml_home : quote.ml_away;
    if (price == null) continue;
    const value = Number(price);
    // Higher American price is always better for the bettor (-110 beats -120; +105 beats -105).
    if (!best || value > best.price) best = { book: SPORTSBOOK_NAMES[key] ?? key, price: value };
  }
  return best;
}

type RuleExclusion = Exclude<FavoriteWatchExclusion, "completed" | "kicked_off">;
type RuleInput = Pick<CfbTerminalRow, "matchupId" | "awayTeam" | "homeTeam" | "commenceTime" | "network" | "openingBooks" | "openingCapturedAt">;

/**
 * Apply the frozen v2 rule to one game given the "now" capture (latest
 * pre-kickoff capture on the live tab; the verified close when grading).
 * Lifecycle (completed / kicked off) is the caller's business.
 */
export function evaluateFavoriteWatch(game: RuleInput, nowBooks: CfbBookMap | null | undefined, nowCapturedAt: string | null): { row: FavoriteWatchRow } | { exclusion: RuleExclusion } {
  const open = consensusHome(game.openingBooks);
  const current = consensusHome(nowBooks);
  if (open.prob == null) return { exclusion: "no_opening" };
  if (current.prob == null) return { exclusion: "no_current" };
  if (!hasAnchorBook(game.openingBooks) || !hasAnchorBook(nowBooks)) return { exclusion: "no_anchor_book" };
  const favorite: "home" | "away" = current.prob >= 0.5 ? "home" : "away";
  const currentProb = favorite === "home" ? current.prob : 1 - current.prob;
  const openProb = favorite === "home" ? open.prob : 1 - open.prob;
  // The side must have been the favorite at open too; a flipped favorite is a different proposition.
  if (openProb < 0.5) return { exclusion: "favorite_flipped" };
  if (currentProb < FAVORITE_WATCH_MIN_PROB || currentProb >= FAVORITE_WATCH_MAX_PROB) return { exclusion: "outside_band" };
  const dropPp = (openProb - currentProb) * 100;
  if (dropPp < FAVORITE_WATCH_MIN_DROP_PP) return { exclusion: "did_not_cheapen" };
  const pinnacle = nowBooks?.pinnacle ? fairHome(nowBooks.pinnacle) : null;
  return { row: {
    matchupId: game.matchupId, awayTeam: game.awayTeam, homeTeam: game.homeTeam,
    commenceTime: game.commenceTime, network: game.network,
    favorite, favoriteTeam: favorite === "home" ? game.homeTeam : game.awayTeam,
    underdogTeam: favorite === "home" ? game.awayTeam : game.homeTeam,
    openProb, currentProb, dropPp,
    openingCapturedAt: game.openingCapturedAt, latestCapturedAt: nowCapturedAt,
    openBooks: open.books, currentBooks: current.books,
    pinnacleProb: pinnacle == null ? null : favorite === "home" ? pinnacle : 1 - pinnacle,
    bestPrice: bestFavoritePrice(nowBooks, favorite),
  } };
}

export function buildFavoriteWatch(games: CfbTerminalRow[], nowMs: number): FavoriteWatchResult {
  const excluded: Record<FavoriteWatchExclusion, number> = {
    completed: 0, kicked_off: 0, no_opening: 0, no_current: 0, no_anchor_book: 0,
    favorite_flipped: 0, outside_band: 0, did_not_cheapen: 0,
  };
  const rows: FavoriteWatchRow[] = [];
  for (const game of games) {
    if (game.completed) { excluded.completed += 1; continue; }
    if (game.commenceTime) {
      const kickoff = Date.parse(game.commenceTime);
      if (Number.isFinite(kickoff) && kickoff <= nowMs) { excluded.kicked_off += 1; continue; }
    }
    const verdict = evaluateFavoriteWatch(game, game.currentBooks, game.latestCapturedAt);
    if ("exclusion" in verdict) { excluded[verdict.exclusion] += 1; continue; }
    rows.push(verdict.row);
  }
  rows.sort((a, b) => b.dropPp - a.dropPp || (Date.parse(a.commenceTime ?? "") || 0) - (Date.parse(b.commenceTime ?? "") || 0));
  return { version: CFB_FAVORITE_WATCH_VERSION, rows, excluded };
}

/* ------------------------------------------------------------------ */
/* Results: the same rule graded at the VERIFIED PRE-KICKOFF CLOSE.    */
/* ------------------------------------------------------------------ */

/** One past game with its opening capture, verified close capture and final score. */
export type FavoriteWatchHistoryGame = RuleInput & {
  gameDate: string;
  completed: boolean;
  homeScore: number | null;
  awayScore: number | null;
  closingBooks: CfbBookMap | null;
  closingCapturedAt: string | null;
  closeQuality: string | null;
};

export type FavoriteWatchResultRow = FavoriteWatchRow & {
  gameDate: string;
  outcome: "won" | "lost" | "pending";
  score: string | null;
  /** Units won/lost on a 1-unit stake at the best selected-book favorite price at the close. */
  pnlUnits: number | null;
  closeQuality: string | null;
};

export type FavoriteWatchSummary = {
  qualified: number;
  settled: number;
  won: number;
  lost: number;
  pending: number;
  winRate: number | null;
  expectedWinRate: number | null;   // mean no-vig close probability of the favorite, settled rows
  units: number | null;
  roiPerBet: number | null;
  firstGameDate: string | null;
  lastGameDate: string | null;
};

export type FavoriteWatchHistory = {
  version: string;
  rows: FavoriteWatchResultRow[];
  summary: FavoriteWatchSummary;
  gamesConsidered: number;
  excluded: Record<RuleExclusion | "no_close", number>;
};

function decimalFromAmerican(price: number): number {
  return price > 0 ? 1 + price / 100 : 1 + 100 / Math.abs(price);
}

/**
 * Grade every game whose (opening capture, verified close) satisfied the rule.
 * Because open and close are both pre-kickoff, this is leak-free and does not
 * depend on anyone having looked at the live tab. A game that qualified mid-day
 * but drifted out by the close is NOT counted; the close is the frozen state.
 */
export function gradeFavoriteWatch(games: FavoriteWatchHistoryGame[]): FavoriteWatchHistory {
  const excluded: FavoriteWatchHistory["excluded"] = {
    no_close: 0, no_opening: 0, no_current: 0, no_anchor_book: 0, favorite_flipped: 0, outside_band: 0, did_not_cheapen: 0,
  };
  const rows: FavoriteWatchResultRow[] = [];
  for (const game of games) {
    if (!game.closingBooks) { excluded.no_close += 1; continue; }
    const verdict = evaluateFavoriteWatch(game, game.closingBooks, game.closingCapturedAt);
    if ("exclusion" in verdict) { excluded[verdict.exclusion] += 1; continue; }
    const row = verdict.row;
    const settled = game.completed && game.homeScore != null && game.awayScore != null;
    let outcome: FavoriteWatchResultRow["outcome"] = "pending";
    let pnlUnits: number | null = null;
    if (settled) {
      const homeWon = (game.homeScore as number) > (game.awayScore as number);
      const favoriteWon = row.favorite === "home" ? homeWon : !homeWon;
      outcome = favoriteWon ? "won" : "lost";
      if (row.bestPrice) pnlUnits = favoriteWon ? decimalFromAmerican(row.bestPrice.price) - 1 : -1;
    }
    rows.push({
      ...row, gameDate: game.gameDate, outcome,
      score: settled ? `${game.awayScore}-${game.homeScore}` : null,
      pnlUnits, closeQuality: game.closeQuality,
    });
  }
  rows.sort((a, b) => (Date.parse(b.commenceTime ?? b.gameDate) || 0) - (Date.parse(a.commenceTime ?? a.gameDate) || 0));
  const settledRows = rows.filter((row) => row.outcome !== "pending");
  const priced = settledRows.filter((row) => row.pnlUnits != null);
  const won = settledRows.filter((row) => row.outcome === "won").length;
  const units = priced.length ? priced.reduce((sum, row) => sum + (row.pnlUnits as number), 0) : null;
  const dates = rows.map((row) => row.gameDate).sort();
  return {
    version: CFB_FAVORITE_WATCH_VERSION, rows, gamesConsidered: games.length, excluded,
    summary: {
      qualified: rows.length, settled: settledRows.length, won, lost: settledRows.length - won,
      pending: rows.length - settledRows.length,
      winRate: settledRows.length ? won / settledRows.length : null,
      expectedWinRate: settledRows.length ? settledRows.reduce((sum, row) => sum + row.currentProb, 0) / settledRows.length : null,
      units, roiPerBet: units != null && priced.length ? units / priced.length : null,
      firstGameDate: dates[0] ?? null, lastGameDate: dates[dates.length - 1] ?? null,
    },
  };
}
