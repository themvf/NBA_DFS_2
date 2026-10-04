import assert from "node:assert/strict";
import type { CfbBookMap, CfbTerminalRow } from "../src/db/queries";
import {
  FAVORITE_WATCH_MAX_PROB, FAVORITE_WATCH_MIN_DROP_PP, FAVORITE_WATCH_MIN_PROB,
  buildFavoriteWatch, consensusHome, favoriteWatchLabel, gradeFavoriteWatch, hasAnchorBook,
} from "../src/lib/cfb-favorite-watch";

const NOW = Date.parse("2026-10-03T15:00:00Z");

function books(home: number, away: number, keys = ["pinnacle", "draftkings", "fanduel"]): CfbBookMap {
  return Object.fromEntries(keys.map((key) => [key, { ml_home: home, ml_away: away, last_update: "2026-10-03T14:50:00Z" }]));
}

function game(overrides: Partial<CfbTerminalRow>): CfbTerminalRow {
  return {
    matchupId: 1, cfbdGameId: 1, oddsEventId: "e", gameDate: "2026-10-03", season: 2026, week: 6,
    commenceTime: "2026-10-03T20:00:00Z", startTimeTbd: false, homeTeam: "Home", awayTeam: "Away",
    venue: null, network: null, neutralSite: false, completed: false, homeScore: null, awayScore: null,
    wentToOvertime: false, overtimePeriods: 0, captures: 2,
    openingBooks: null, currentBooks: null, closingBooks: null,
    openingCapturedAt: "2026-10-01T12:00:00Z", latestCapturedAt: "2026-10-03T14:50:00Z", closingCapturedAt: null,
    closeQuality: null, closeLeadSeconds: null, closeBoundarySource: null, closeVerificationLevel: null, closeCohort: null,
    history: [], ...overrides,
  };
}

// Consensus helper: -150/+130 at every book -> fair home 60/(60+43.5) = 0.580
const c = consensusHome(books(-150, 130));
assert.equal(c.books, 3);
assert.ok(Math.abs((c.prob ?? 0) - 0.5797) < 0.001, `fair home ${c.prob}`);

// 1. Qualifying game: home opened -200 (fair 0.667), now -150 (0.580): drop 8.7pp, inside 51-80.
const qualifying = game({ matchupId: 1, openingBooks: books(-200, 170), currentBooks: books(-150, 130) });
let result = buildFavoriteWatch([qualifying], NOW);
assert.equal(result.rows.length, 1);
assert.equal(result.rows[0].favorite, "home");
assert.equal(result.rows[0].favoriteTeam, "Home");
assert.ok(result.rows[0].dropPp >= FAVORITE_WATCH_MIN_DROP_PP);
assert.ok(result.rows[0].currentProb >= FAVORITE_WATCH_MIN_PROB && result.rows[0].currentProb < FAVORITE_WATCH_MAX_PROB);
assert.equal(result.rows[0].bestPrice?.price, -150);
assert.ok(result.rows[0].pinnacleProb != null);

// 2. Away favorite works the same way.
const awayFav = game({ matchupId: 2, openingBooks: books(170, -200), currentBooks: books(130, -150) });
result = buildFavoriteWatch([awayFav], NOW);
assert.equal(result.rows.length, 1);
assert.equal(result.rows[0].favorite, "away");
assert.equal(result.rows[0].underdogTeam, "Home");

// 3. Favorite got PRICIER inside the band (-130/+110 0.552 -> -150/+130 0.580) -> excluded as did_not_cheapen.
result = buildFavoriteWatch([game({ matchupId: 3, openingBooks: books(-130, 110), currentBooks: books(-150, 130) })], NOW);
assert.equal(result.rows.length, 0);
assert.equal(result.excluded.did_not_cheapen, 1);

// 4. Drop just under the threshold: -165/+145 (0.604) -> -150/+130 (0.580) is 2.4pp (in);
//    -155/+135 (0.591) -> -150/+130 is 1.1pp (out).
result = buildFavoriteWatch([game({ matchupId: 4, openingBooks: books(-155, 135), currentBooks: books(-150, 130) })], NOW);
assert.equal(result.excluded.did_not_cheapen, 1);
result = buildFavoriteWatch([game({ matchupId: 4, openingBooks: books(-165, 145), currentBooks: books(-150, 130) })], NOW);
assert.equal(result.rows.length, 1);

// 5. Above the 60% cap: -200/+170 (0.667) -> -165/+145 (0.604) is excluded even though it cheapened 6pp.
result = buildFavoriteWatch([game({ matchupId: 5, openingBooks: books(-200, 170), currentBooks: books(-165, 145) })], NOW);
assert.equal(result.excluded.outside_band, 1);

// 6. Favorite flipped (home opened dog, now favorite) is excluded; it is a different proposition.
result = buildFavoriteWatch([game({ matchupId: 6, openingBooks: books(130, -150), currentBooks: books(-150, 130) })], NOW);
assert.equal(result.excluded.favorite_flipped, 1);

// 7. Upcoming only: completed and already-kicked games are excluded.
result = buildFavoriteWatch([
  game({ matchupId: 7, completed: true, homeScore: 1, awayScore: 0, openingBooks: books(-200, 170), currentBooks: books(-150, 130) }),
  game({ matchupId: 8, commenceTime: "2026-10-03T14:00:00Z", openingBooks: books(-200, 170), currentBooks: books(-150, 130) }),
], NOW);
assert.equal(result.rows.length, 0);
assert.equal(result.excluded.completed, 1);
assert.equal(result.excluded.kicked_off, 1);

// 8. Anchor rule: Pinnacle OR DraftKings alone is enough, at both captures; FanDuel-only is not.
assert.equal(hasAnchorBook(books(-150, 130, ["pinnacle"])), true);
assert.equal(hasAnchorBook(books(-150, 130, ["draftkings"])), true);
assert.equal(hasAnchorBook(books(-150, 130, ["fanduel", "betmgm", "fanatics"])), false);
assert.equal(hasAnchorBook({ pinnacle: { ml_home: -150, ml_away: null } }), false);
result = buildFavoriteWatch([game({ matchupId: 9, openingBooks: books(-200, 170, ["pinnacle"]), currentBooks: books(-150, 130, ["draftkings"]) })], NOW);
assert.equal(result.rows.length, 1);
result = buildFavoriteWatch([game({ matchupId: 9, openingBooks: books(-200, 170, ["fanduel", "betmgm", "fanatics"]), currentBooks: books(-150, 130) })], NOW);
assert.equal(result.excluded.no_anchor_book, 1);
result = buildFavoriteWatch([game({ matchupId: 9, openingBooks: books(-200, 170), currentBooks: books(-150, 130, ["fanduel", "betmgm", "fanatics"]) })], NOW);
assert.equal(result.excluded.no_anchor_book, 1);
result = buildFavoriteWatch([game({ matchupId: 10, openingBooks: null, currentBooks: books(-150, 130) })], NOW);
assert.equal(result.excluded.no_opening, 1);

// 9. Sorted by drop, largest first.
result = buildFavoriteWatch([
  game({ matchupId: 11, openingBooks: books(-165, 145), currentBooks: books(-150, 130) }),
  game({ matchupId: 12, openingBooks: books(-250, 210), currentBooks: books(-150, 130) }),
], NOW);
assert.deepEqual(result.rows.map((row) => row.matchupId), [12, 11]);

// 10. Grading at the verified close: open -200/+170 (0.667) -> close -150/+130 (0.580) qualifies.
function hist(overrides: Record<string, unknown>) {
  return { matchupId: 100, awayTeam: "Away", homeTeam: "Home", commenceTime: "2026-09-12T20:00:00Z", network: null,
    openingBooks: books(-200, 170), openingCapturedAt: "2026-09-10T12:00:00Z", gameDate: "2026-09-12",
    completed: true, homeScore: 28, awayScore: 24, closingBooks: books(-150, 130), closingCapturedAt: "2026-09-12T19:55:00Z", closeQuality: "A", ...overrides };
}
let graded = gradeFavoriteWatch([
  hist({ matchupId: 100 }),                                                    // favorite (home) won: +0.667u at -150
  hist({ matchupId: 101, homeScore: 20, awayScore: 24 }),                      // favorite lost: -1u
  hist({ matchupId: 102, completed: false, homeScore: null, awayScore: null }), // pending
  hist({ matchupId: 103, closingBooks: null }),                                 // no verified close -> excluded
  hist({ matchupId: 104, openingBooks: books(-150, 130), closingBooks: books(-150, 130) }), // unchanged -> did not cheapen
]);
assert.equal(graded.summary.qualified, 3);
assert.equal(graded.summary.settled, 2);
assert.equal(graded.summary.won, 1);
assert.equal(graded.summary.lost, 1);
assert.equal(graded.summary.pending, 1);
assert.equal(graded.excluded.no_close, 1);
assert.equal(graded.excluded.did_not_cheapen, 1);
assert.ok(Math.abs((graded.summary.units ?? 0) - (0.6667 - 1)) < 0.001, `units ${graded.summary.units}`);
assert.equal(graded.summary.winRate, 0.5);
assert.ok(Math.abs((graded.summary.expectedWinRate ?? 0) - 0.5797) < 0.001);
const wonRow = graded.rows.find((row) => row.matchupId === 100);
assert.equal(wonRow?.outcome, "won"); assert.equal(wonRow?.score, "24-28");
assert.equal(graded.rows.find((row) => row.matchupId === 102)?.outcome, "pending");
assert.equal(graded.rows.find((row) => row.matchupId === 102)?.pnlUnits, null);
// Away favorite that lost grades against the away side, not the home side.
graded = gradeFavoriteWatch([hist({ matchupId: 105, openingBooks: books(170, -200), closingBooks: books(130, -150), homeScore: 30, awayScore: 10 })]);
assert.equal(graded.rows[0].favorite, "away");
assert.equal(graded.rows[0].outcome, "lost");
// Grading ignores the live-tab lifecycle: a completed game is graded, never excluded as "completed".
assert.equal(gradeFavoriteWatch([hist({ matchupId: 106 })]).summary.qualified, 1);

// 11. Sport-neutral input: a minimal NFL-shaped object (no CfbTerminalRow fields) runs the same rule.
const nflShaped = { matchupId: 500, awayTeam: "Cowboys", homeTeam: "Texans", commenceTime: "2026-10-04T17:00:00Z", network: null, completed: false,
  openingBooks: books(-200, 170), openingCapturedAt: "2026-09-27T12:04:00Z", currentBooks: books(-150, 130), latestCapturedAt: "2026-10-04T14:01:00Z" };
const nflResult = buildFavoriteWatch([nflShaped], Date.parse("2026-10-04T15:00:00Z"));
assert.equal(nflResult.rows.length, 1);
assert.equal(nflResult.rows[0].favoriteTeam, "Texans");
assert.equal(nflResult.rows[0].latestCapturedAt, "2026-10-04T14:01:00Z");

// 12. Display label: CFB keeps its historical label, NFL no longer shows "CFB-".
assert.equal(favoriteWatchLabel("CFB", "cfb-favorite-watch-v2"), "CFB-FAVORITE-WATCH-V2");
assert.equal(favoriteWatchLabel("NFL", "cfb-favorite-watch-v2"), "NFL-FAVORITE-WATCH-V2");

console.log("CFB favorite watch checks passed");
