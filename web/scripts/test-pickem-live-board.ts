/**
 * Tests for the pick'em live board (/nfl/pickem/live).
 *
 * The load-bearing checks are the two flip tags, which mean different things
 * and must never be merged: "our flip" is about the pick against the latest
 * pregame favourite, "Vegas flip" is about the market against its own opener.
 *
 * Run: npm run test:pickem-live-board
 */

import {
  buildBoardGames,
  parseEspnScoreboard,
  summarize,
  windowLabel,
  type PickemLiveCard,
  type PickemLiveCardGame,
} from "../src/lib/nfl/pickem-live-board";

let passed = 0;
let failed = 0;

function check(name: string, condition: boolean, detail?: string) {
  if (condition) {
    passed++;
  } else {
    failed++;
    console.error(`FAIL ${name}${detail ? `: ${detail}` : ""}`);
  }
}

const game = (over: Partial<PickemLiveCardGame>): PickemLiveCardGame => ({
  gameId: 1, away: "KC", home: "DEN", pickHome: true, baselinePickHome: false, confidence: 1,
  pHome: 0.45, kickoff: "2026-11-01T20:25:00Z", homeScore: null, awayScore: null, completed: false,
  ...over,
});

const trail = (...ps: number[]) => ps.map((pHome) => ({ pHome, homeSpread: pHome > 0.5 ? -3 : 3 }));

const card = (games: PickemLiveCardGame[], trails: PickemLiveCard["trails"]): PickemLiveCard => ({
  id: 40, season: 2026, week: 8, format: "straight", frozenAt: "2026-10-29T22:49:35Z", games, trails,
});

// DEN picked as the underdog while the frozen model also had DEN under 50%: by choice.
{
  const [g] = buildBoardGames(card([game({})], { 1: trail(0.42, 0.44, 0.43) }), null);
  check("our flip by choice", g.ourFlip && g.ourFlipReason === "strategy", JSON.stringify(g));
  check("no vegas flip when the favourite held", !g.vegasFlip);
  check("pick is DEN", g.pick === "DEN");
  check("trail is from the pick's side", Math.abs(g.trail[0] - 0.42) < 1e-9);
  check("frozen from the pick's side", Math.abs(g.frozen - 0.45) < 1e-9);
  check("spread names the favourite", g.spread === "KC −3", String(g.spread));
}

// Picked the underdog that our own model favoured.
{
  const [g] = buildBoardGames(card([game({ pHome: 0.55 })], { 1: trail(0.45, 0.46) }), null);
  check("our flip from the model", g.ourFlip && g.ourFlipReason === "model");
}

// Vegas flip: the opener favoured the road side, the latest capture the home side.
{
  const [g] = buildBoardGames(card([game({ pHome: 0.6 })], { 1: trail(0.47, 0.5, 0.56) }), null);
  check("vegas flip", g.vegasFlip && g.openFavorite === "KC" && g.favorite === "DEN");
  check("not our flip when we took the new favourite", !g.ourFlip && g.ourFlipReason === null);
}

// A 0.5 opener has no favourite, so it cannot create a Vegas flip.
{
  const [g] = buildBoardGames(card([game({ pHome: 0.6 })], { 1: trail(0.5, 0.56) }), null);
  check("coin-flip opener is not a vegas flip", !g.vegasFlip && g.openFavorite === null);
}

// No line at all: no tags, frozen value stands in.
{
  const [g] = buildBoardGames(card([game({})], {}), null);
  check("no trail, no flip tags", !g.ourFlip && !g.vegasFlip && g.trail.length === 0);
}

// Scores: ESPN first; a final from the season table when ESPN is missing.
{
  const espn = parseEspnScoreboard({
    events: [{
      status: { type: { state: "in", shortDetail: "5:21 - 3rd" } },
      competitions: [{
        competitors: [
          { homeAway: "home", score: "17", team: { id: "7", abbreviation: "DEN" } },
          { homeAway: "away", score: "10", team: { id: "12", abbreviation: "KC" } },
        ],
        situation: { possession: "12", isRedZone: true },
      }],
    }],
  });
  const games = buildBoardGames(card([
    game({}),
    game({ gameId: 2, away: "CAR", home: "GB", pHome: 0.66, completed: true, homeScore: 27, awayScore: 19,
      kickoff: "2026-10-30T00:15:00Z" }),
  ], { 1: trail(0.42, 0.43), 2: trail(0.59, 0.63) }), espn);
  const [live, final] = games;
  check("live state from espn", live.state === "in" && live.scoreSource === "espn");
  check("possession and red zone", live.possession === "KC" && live.redZone);
  check("final from the season table", final.state === "post" && final.scoreSource === "ledger");
  const s = summarize(games);
  check("record", s.won === 1 && s.lost === 0);
  check("live ahead", s.live === 1 && s.liveAhead === 1);
  check("expected correct: final counts 1, live counts its latest pregame chance",
    Math.abs(s.expectedCorrect - (1 + 0.43)) < 1e-9, String(s.expectedCorrect));
  check("our flips list", s.ourFlips.join() === "DEN");
  check("Thursday window in Eastern time", windowLabel("2026-10-30T00:15:00Z") === "Thursday night");
  check("Sunday afternoon window", windowLabel("2026-11-01T20:25:00Z") === "Sunday afternoon");
  check("Sunday 1 PM window", windowLabel("2026-11-01T18:00:00Z") === "Sunday 1:00 PM");
  check("Sunday night window", windowLabel("2026-11-02T01:20:00Z") === "Sunday night");
}

console.log(`${passed} passed, ${failed} failed`);
if (failed) process.exit(1);
