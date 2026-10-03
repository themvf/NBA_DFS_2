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
    const open = consensusHome(game.openingBooks);
    const current = consensusHome(game.currentBooks);
    if (open.prob == null) { excluded.no_opening += 1; continue; }
    if (current.prob == null) { excluded.no_current += 1; continue; }
    if (!hasAnchorBook(game.openingBooks) || !hasAnchorBook(game.currentBooks)) { excluded.no_anchor_book += 1; continue; }
    const favorite: "home" | "away" = current.prob >= 0.5 ? "home" : "away";
    const currentProb = favorite === "home" ? current.prob : 1 - current.prob;
    const openProb = favorite === "home" ? open.prob : 1 - open.prob;
    // The side must have been the favorite at open too; a flipped favorite is a different proposition.
    if (openProb < 0.5) { excluded.favorite_flipped += 1; continue; }
    if (currentProb < FAVORITE_WATCH_MIN_PROB || currentProb >= FAVORITE_WATCH_MAX_PROB) { excluded.outside_band += 1; continue; }
    const dropPp = (openProb - currentProb) * 100;
    if (dropPp < FAVORITE_WATCH_MIN_DROP_PP) { excluded.did_not_cheapen += 1; continue; }
    const pinnacle = game.currentBooks?.pinnacle ? fairHome(game.currentBooks.pinnacle) : null;
    rows.push({
      matchupId: game.matchupId, awayTeam: game.awayTeam, homeTeam: game.homeTeam,
      commenceTime: game.commenceTime, network: game.network,
      favorite, favoriteTeam: favorite === "home" ? game.homeTeam : game.awayTeam,
      underdogTeam: favorite === "home" ? game.awayTeam : game.homeTeam,
      openProb, currentProb, dropPp,
      openingCapturedAt: game.openingCapturedAt, latestCapturedAt: game.latestCapturedAt,
      openBooks: open.books, currentBooks: current.books,
      pinnacleProb: pinnacle == null ? null : favorite === "home" ? pinnacle : 1 - pinnacle,
      bestPrice: bestFavoritePrice(game.currentBooks, favorite),
    });
  }
  rows.sort((a, b) => b.dropPp - a.dropPp || (Date.parse(a.commenceTime ?? "") || 0) - (Date.parse(b.commenceTime ?? "") || 0));
  return { version: CFB_FAVORITE_WATCH_VERSION, rows, excluded };
}
