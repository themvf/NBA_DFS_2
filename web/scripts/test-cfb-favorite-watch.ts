import assert from "node:assert/strict";
import type { CfbBookMap, CfbTerminalRow } from "../src/db/queries";
import {
  FAVORITE_WATCH_MAX_PROB, FAVORITE_WATCH_MIN_DROP_PP, FAVORITE_WATCH_MIN_PROB,
  buildFavoriteWatch, consensusHome, hasAnchorBook,
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

console.log("CFB favorite watch checks passed");
