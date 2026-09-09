/**
 * Tests for the pick'em archetype taxonomy and the standings derivation the
 * record-based archetypes read from.
 *
 * The load-bearing tests here are the LEAK tests. `buildStandings` decides what
 * a pool entrant could have known when they submitted their card, and the whole
 * point of a record archetype is that the room reads the standings BEFORE the
 * week starts. If the current week's own result leaks into the record, every
 * record tag becomes a retrodiction dressed as a perception model -- the exact
 * failure mode this repo has been bitten by twice (mlb_matchups.our_prob_home,
 * mlb_bets.event_commence).
 *
 * The second theme is TAG CORRELATION. `narrativeRead` sums tag weights, so two
 * tags telling one story double-count the room's heat. UNDEFEATED and
 * RECORD_GAP are the same story; the suppression that stops them stacking is
 * asserted here rather than left as a comment.
 *
 * Run: npm run test:archetypes
 */

import {
  ARCHETYPES,
  MARQUEE_TEAMS,
  FLYOVER_TEAMS,
  archetype,
  buildStandings,
  narrativeRead,
  tagSeason,
  tagArchetypes,
  type ArchetypeCode,
  type SeasonGame,
  type StandingsGame,
  type TeamGameContext,
} from "../src/lib/nfl/pickem-archetypes";

let passed = 0;
let failed = 0;

function check(name: string, condition: boolean, detail?: string) {
  if (condition) {
    passed += 1;
    console.log(`  PASS  ${name}`);
  } else {
    failed += 1;
    console.error(`  FAIL  ${name}${detail ? ` -- ${detail}` : ""}`);
  }
}

/** A neutral team-game with nothing interesting about it. */
function ctx(over: Partial<TeamGameContext> = {}): TeamGameContext {
  return {
    team: "MIN",
    opp: "DET",
    isHome: true,
    week: 8,
    impliedWin: 0.5,
    rest: 7,
    oppRest: 7,
    weekday: 0,
    hourEt: 13,
    neutralSite: false,
    div: false,
    roof: "outdoors",
    ...over,
  };
}

function game(
  week: number,
  home: string,
  away: string,
  homeScore: number | null,
  awayScore: number | null,
): StandingsGame {
  return { week, home, away, homeScore, awayScore };
}

// ---------------------------------------------------------------------------
// buildStandings
// ---------------------------------------------------------------------------

console.log("\nbuildStandings");

{
  const games = [
    game(1, "KC", "BUF", 27, 20),
    game(2, "KC", "DEN", 31, 10),
    game(3, "LAC", "KC", 17, 14),
  ];
  const s = buildStandings(games, 4);
  const kc = s.get("KC")!;
  check("counts wins and losses", kc.wins === 2 && kc.losses === 1, JSON.stringify(kc));
  check("accumulates points for/against", kc.pointsFor === 72 && kc.pointsAgainst === 47, JSON.stringify(kc));
  check("counts games played", kc.games === 3);
  const buf = s.get("BUF")!;
  check("records the losing side too", buf.wins === 0 && buf.losses === 1);
}

{
  // The leak test. A record used for week 3 must not see week 3's result.
  const games = [game(1, "KC", "BUF", 27, 20), game(2, "KC", "DEN", 31, 10), game(3, "KC", "LV", 40, 0)];
  const s = buildStandings(games, 3);
  const kc = s.get("KC")!;
  check("excludes the as-of week itself (no leak)", kc.wins === 2 && kc.games === 2, JSON.stringify(kc));
  check("excludes later weeks entirely", buildStandings(games, 2).get("KC")!.games === 1);
}

{
  const games = [game(1, "KC", "BUF", 27, 20), game(2, "KC", "DEN", null, null)];
  const kc = buildStandings(games, 5).get("KC")!;
  check("ignores games with no score (unplayed)", kc.games === 1 && kc.wins === 1, JSON.stringify(kc));
}

{
  const games = [game(1, "KC", "BUF", 20, 20)];
  const s = buildStandings(games, 5);
  const kc = s.get("KC")!;
  check("counts a tie as a tie, not a win", kc.ties === 1 && kc.wins === 0 && kc.losses === 0);
  check("a tie still counts as a game played", kc.games === 1);
}

{
  check("a team with no games is absent rather than 0-0", buildStandings([], 5).get("KC") === undefined);
}

// ---------------------------------------------------------------------------
// Brand bias -- the only archetypes with no schedule or market correlation
// ---------------------------------------------------------------------------

console.log("\nMARQUEE_BRAND / FLYOVER_FADE");

{
  check("marquee and flyover sets are disjoint", MARQUEE_TEAMS.every((t) => !FLYOVER_TEAMS.includes(t)));
  check("MARQUEE_BRAND fires for a marquee team", tagArchetypes(ctx({ team: "DAL" })).includes("MARQUEE_BRAND"));
  check("MARQUEE_BRAND does not fire for a flyover team", !tagArchetypes(ctx({ team: "JAX" })).includes("MARQUEE_BRAND"));
  check("FLYOVER_FADE fires for a flyover team", tagArchetypes(ctx({ team: "JAX" })).includes("FLYOVER_FADE"));
  check("FLYOVER_FADE does not fire for a marquee team", !tagArchetypes(ctx({ team: "DAL" })).includes("FLYOVER_FADE"));
  check(
    "a team in neither set carries no brand tag",
    !tagArchetypes(ctx({ team: "MIN" })).some((c) => c === "MARQUEE_BRAND" || c === "FLYOVER_FADE"),
  );
}

{
  // Two marquee teams meeting carries no directional information. The swing
  // cancels naturally, so no special-case collapse is needed -- assert that.
  const read = narrativeRead(["MARQUEE_BRAND"], ["MARQUEE_BRAND"]);
  check("marquee vs marquee reads quiet (heat cancels)", read.verdict === "quiet", read.verdict);
}

// ---------------------------------------------------------------------------
// HEAVY_FAVORITE
// ---------------------------------------------------------------------------

console.log("\nHEAVY_FAVORITE");

{
  check("fires at the 0.80 threshold", tagArchetypes(ctx({ impliedWin: 0.8 })).includes("HEAVY_FAVORITE"));
  check("does not fire just below it", !tagArchetypes(ctx({ impliedWin: 0.79 })).includes("HEAVY_FAVORITE"));
  check("does not fire on the underdog side", !tagArchetypes(ctx({ impliedWin: 0.2 })).includes("HEAVY_FAVORITE"));
  const dog = tagArchetypes(ctx({ impliedWin: 0.2, isHome: true }));
  check("HOME_DOG and HEAVY_FAVORITE never co-fire", !(dog.includes("HOME_DOG") && dog.includes("HEAVY_FAVORITE")));
}

// ---------------------------------------------------------------------------
// Record archetypes
// ---------------------------------------------------------------------------

console.log("\nRECORD_GAP / UNDEFEATED / WINLESS");

const rec = (wins: number, losses: number, ties = 0, pf = 0, pa = 0) => ({
  wins, losses, ties, games: wins + losses + ties, pointsFor: pf, pointsAgainst: pa,
});

{
  const better = ctx({ week: 6, record: rec(5, 1), oppRecord: rec(2, 4) });
  check("RECORD_GAP fires on a 4-game win differential", tagArchetypes(better).includes("RECORD_GAP"));
  check(
    "RECORD_GAP does not fire on the worse side",
    !tagArchetypes(ctx({ week: 6, record: rec(2, 4), oppRecord: rec(5, 1) })).includes("RECORD_GAP"),
  );
  check(
    "RECORD_GAP does not fire below the threshold",
    !tagArchetypes(ctx({ week: 6, record: rec(4, 2), oppRecord: rec(3, 3) })).includes("RECORD_GAP"),
  );
  check(
    "RECORD_GAP does not fire before week 5",
    !tagArchetypes(ctx({ week: 4, record: rec(3, 0), oppRecord: rec(0, 3) })).includes("RECORD_GAP"),
  );
  check(
    "RECORD_GAP fails closed with no standings",
    !tagArchetypes(ctx({ week: 6 })).includes("RECORD_GAP"),
  );
}

{
  check("UNDEFEATED fires at 3-0 in week 4", tagArchetypes(ctx({ week: 4, record: rec(3, 0) })).includes("UNDEFEATED"));
  check("UNDEFEATED does not fire at 2-0", !tagArchetypes(ctx({ week: 3, record: rec(2, 0) })).includes("UNDEFEATED"));
  check(
    "a tie breaks the undefeated framing",
    !tagArchetypes(ctx({ week: 5, record: rec(3, 0, 1) })).includes("UNDEFEATED"),
  );
  check("WINLESS fires at 0-3", tagArchetypes(ctx({ week: 4, record: rec(0, 3) })).includes("WINLESS"));
  check("WINLESS does not fire at 0-2", !tagArchetypes(ctx({ week: 3, record: rec(0, 2) })).includes("WINLESS"));
  check(
    "a tie breaks the winless framing",
    !tagArchetypes(ctx({ week: 5, record: rec(0, 3, 1) })).includes("WINLESS"),
  );
  check("UNDEFEATED and WINLESS cannot both fire", (() => {
    const t = tagArchetypes(ctx({ week: 6, record: rec(0, 5) }));
    return !(t.includes("UNDEFEATED") && t.includes("WINLESS"));
  })());
}

{
  // Three tags for one story would inflate narrativeRead's summed heat.
  const t = tagArchetypes(ctx({ week: 6, record: rec(5, 0), oppRecord: rec(1, 4) }));
  check("UNDEFEATED suppresses RECORD_GAP", t.includes("UNDEFEATED") && !t.includes("RECORD_GAP"), t.join(","));
  const w = tagArchetypes(ctx({ week: 6, record: rec(0, 5), oppRecord: rec(4, 1) }));
  check("WINLESS suppresses nothing it does not overlap", w.includes("WINLESS"));
}

// ---------------------------------------------------------------------------
// Registry integrity
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// tagSeason -- the whole-season pass, which is what a completed prior season
// exercises. 2026 has no played games, so every result- and record-based
// archetype is dormant there; only a prior season proves they fire at all.
// ---------------------------------------------------------------------------

console.log("\ntagSeason (completed prior season)");

function sg(
  gameId: number,
  week: number,
  home: string,
  away: string,
  homeScore: number | null,
  awayScore: number | null,
  over: Partial<SeasonGame> = {},
): SeasonGame {
  return {
    gameId, week, homeAbbrev: home, awayAbbrev: away, homeScore, awayScore,
    pHome: 0.5, kickoff: `2024-09-0${Math.min(week, 9)}T17:00:00Z`,
    homeRest: 7, awayRest: 7, divGame: false, roof: "outdoors",
    ...over,
  };
}

{
  const season: SeasonGame[] = [
    sg(1, 1, "KC", "BUF", 31, 10),
    sg(2, 2, "KC", "MIN", 28, 7),
    sg(3, 2, "BUF", "MIA", 10, 20),
    sg(4, 3, "KC", "DEN", 30, 27),
    sg(5, 3, "BUF", "NYJ", 14, 17),
    sg(6, 4, "KC", "LV", 24, 3),
    sg(7, 4, "BUF", "NE", 13, 16),
    sg(8, 5, "KC", "BUF", 21, 20),
  ];
  const tags = tagSeason(season);

  const wk5 = tags.get(8)!;
  check("UNDEFEATED fires on a real completed season", wk5.home.includes("UNDEFEATED"), wk5.home.join(","));
  check("WINLESS fires on the other side", wk5.away.includes("WINLESS"), wk5.away.join(","));
  check("OFF_BLOWOUT_WIN reads last week's real score", wk5.home.includes("OFF_BLOWOUT_WIN"), wk5.home.join(","));
  check("suppression still applies through the season pass", !wk5.home.includes("RECORD_GAP"), wk5.home.join(","));

  const wk1 = tags.get(1)!;
  check(
    "week 1 carries no record or result archetype",
    !(["UNDEFEATED", "WINLESS", "RECORD_GAP", "OFF_BLOWOUT_WIN", "OFF_BLOWOUT_LOSS"] as ArchetypeCode[]).some(
      (c) => wk1.home.includes(c) || wk1.away.includes(c),
    ),
    `${wk1.home.join(",")} | ${wk1.away.join(",")}`,
  );

  const standings = buildStandings(
    season.map((g) => ({ week: g.week, home: g.homeAbbrev, away: g.awayAbbrev, homeScore: g.homeScore, awayScore: g.awayScore })),
    5,
  );
  check("season pass does not leak the current week", standings.get("KC")!.wins === 4);
}

{
  const future: SeasonGame[] = [sg(1, 1, "KC", "BUF", null, null), sg(2, 2, "KC", "DEN", null, null)];
  const tags = tagSeason(future);
  check("an unplayed season still returns a tag map", tags.size === 2);
  check(
    "no record archetype on an unplayed season",
    !tags.get(2)!.home.some((c) => (["UNDEFEATED", "WINLESS", "RECORD_GAP"] as ArchetypeCode[]).includes(c)),
  );
}

{
  const season: SeasonGame[] = [sg(1, 1, "MIA", "JAX", 40, 3), sg(2, 2, "JAX", "MIA", 10, 13)];
  const tags = tagSeason(season);
  check("OFF_BLOWOUT_WIN attaches to the winner, away side", tags.get(2)!.away.includes("OFF_BLOWOUT_WIN"), tags.get(2)!.away.join(","));
  check("OFF_BLOWOUT_LOSS attaches to the loser, home side", tags.get(2)!.home.includes("OFF_BLOWOUT_LOSS"), tags.get(2)!.home.join(","));
}

console.log("\nregistry");

{
  const codes = ARCHETYPES.map((a) => a.code);
  check("codes are unique", new Set(codes).size === codes.length);
  check("every code resolves", codes.every((c) => archetype(c as ArchetypeCode).code === c));
  check(
    "every definition carries a story and a short label",
    ARCHETYPES.every((a) => a.story.length > 0 && a.short.length > 0),
  );
  const NEW: ArchetypeCode[] = [
    "MARQUEE_BRAND", "FLYOVER_FADE", "HEAVY_FAVORITE", "RECORD_GAP", "UNDEFEATED", "WINLESS",
  ];
  check(
    "newly added archetypes carry no fabricated market gap",
    NEW.every((c) => archetype(c).measuredGapPp === null && archetype(c).measuredN === null),
  );
  check(
    "the seventeen measured archetypes keep their numbers",
    archetype("CROSS_COUNTRY").measuredGapPp === 8.0 && archetype("CROSS_COUNTRY").measuredN === 211,
  );
}

console.log(`\n${passed} passed, ${failed} failed`);
process.exit(failed === 0 ? 0 : 1);
