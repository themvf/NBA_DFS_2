import assert from "node:assert/strict";
import {
  buildCheatSheet,
  CHEAT_SHEET_VARIANTS,
  PLAYOFF_SLATE_VARIANTS,
  TIERED_POSITIONS,
} from "../src/lib/fantasy-football/cheat-sheet";
import { BEST_BALL_ROUNDS, BEST_BALL_TEAM_COUNT } from "../src/lib/fantasy-football/best-ball";
import { REDRAFT_POSITION_LABEL, REDRAFT_ROUNDS, REDRAFT_TEAM_COUNT } from "../src/lib/fantasy-football/redraft";
import type { FantasyRankingRow } from "../src/db/queries-fantasy-football";

let nextId = 1;
function row(overrides: Partial<FantasyRankingRow> & { position: string }): FantasyRankingRow {
  const playerId = nextId++;
  return {
    playerId,
    name: `${overrides.position} ${playerId}`,
    team: "SEA",
    rookie: false,
    byeWeek: 8,
    injuryStatus: null,
    ecr: null,
    positionRank: null,
    ourRank: playerId,
    tier: 1,
    adp: null,
    adpStdev: null,
    adpHigh: null,
    adpLow: null,
    adpSampleSize: null,
    dkBestBallAdp: null,
    dkBestBallRank: null,
    dkBestBallDraftPct: null,
    dkBestBallDraftGroupId: null,
    dkBestBallCapturedAt: null,
    projectionLow: null,
    projectionHigh: null,
    rankMin: null,
    rankMax: null,
    rankStd: null,
    projectedPoints: null,
    fantasyProsProjectedPoints: null,
    fantasyProsProjectionFetchedAt: null,
    fantasyProsProjectionUpdatedAt: null,
    ourProjectedPoints: 200,
    games2025: 17,
    fantasyPoints2025: 150,
    positionFinish2025: null,
    positionFinishTieCount2025: null,
    projectionDetails: null,
    expectedGames: 17,
    confidence: 0.5,
    indicators: [],
    ...overrides,
  };
}

function column(sheet: ReturnType<typeof buildCheatSheet>, position: string) {
  const found = sheet.find((entry) => entry.position === position);
  assert.ok(found, `expected a ${position} column`);
  return found;
}

/** All entries for a position, re-joined across any continuation columns. */
function allEntries(sheet: ReturnType<typeof buildCheatSheet>, position: string) {
  return sheet.filter((col) => col.position === position).flatMap((col) => col.entries);
}

// --- DST: the whole point of this sheet's departure from the web board ------

// All 32 defenses come back tier 1 from rank_rows() because shrinkage
// compresses them into a ~102-108 band that never trips the 0.88 breakpoint.
// Printing "T1" thirty-two times would read as "unranked", so DST must print
// no tier at all. If someone later widens DST_CARRY_FORWARD_WEIGHT and real
// tiers appear, this test should be revisited deliberately, not silently.
{
  const defenses = Array.from({ length: 32 }, (_, index) =>
    row({ position: "DST", tier: 1, ourRank: 110 + index, ourProjectedPoints: 108 - index * 0.2 }));
  const sheet = buildCheatSheet(defenses);
  const dst = column(sheet, "DST");
  assert.equal(dst.tiersSuppressed, true, "DST must suppress tiers");
  assert.ok(dst.entries.every((entry) => entry.tier === null), "no DST entry may carry a tier");
  assert.ok(dst.entries.every((entry) => !entry.startsNewTier), "DST must draw no tier rules");
  assert.equal(TIERED_POSITIONS.has("DST"), false);
}

// Our DST order is prior-season carry-forward; FantasyPros embeds forward
// judgment. The delta is the decision-relevant number, so it must be computed
// from THEIR projections and be signed our-way (positive = we are higher).
{
  const defenses = [
    row({ position: "DST", team: "SEA", ourRank: 110, ourProjectedPoints: 108, fantasyProsProjectedPoints: 110.9 }),
    row({ position: "DST", team: "HOU", ourRank: 111, ourProjectedPoints: 107, fantasyProsProjectedPoints: 120.4 }),
    row({ position: "DST", team: "BAL", ourRank: 130, ourProjectedPoints: 104, fantasyProsProjectedPoints: 118.3 }),
  ];
  const dst = column(buildCheatSheet(defenses), "DST");
  // FP order: HOU(120.4) 1, BAL(118.3) 2, SEA(110.9) 3. Ours: SEA 1, HOU 2, BAL 3.
  const bySea = dst.entries.find((entry) => entry.team === "SEA")!;
  const byBal = dst.entries.find((entry) => entry.team === "BAL")!;
  assert.equal(bySea.comparisonDelta, 2, "FP ranks SEA 3rd, we rank 1st => +2 (we like more)");
  assert.equal(byBal.comparisonDelta, -1, "FP ranks BAL 2nd, we rank 3rd => -1 (FP likes more)");
}

// A defense missing a FantasyPros projection must show no delta rather than
// silently ranking as if it were last.
{
  const defenses = [
    row({ position: "DST", team: "SEA", ourProjectedPoints: 108, fantasyProsProjectedPoints: 120 }),
    row({ position: "DST", team: "NYJ", ourProjectedPoints: 102, fantasyProsProjectedPoints: null }),
  ];
  const dst = column(buildCheatSheet(defenses), "DST");
  assert.equal(dst.entries.find((entry) => entry.team === "NYJ")!.comparisonDelta, null);
}

// --- Skill positions keep tiers and ADP deltas ------------------------------

{
  const backs = [
    row({ position: "RB", tier: 1, ourRank: 1, adp: 3 }),
    row({ position: "RB", tier: 1, ourRank: 2, adp: 2 }),
    row({ position: "RB", tier: 2, ourRank: 3, adp: 9 }),
  ];
  const rb = column(buildCheatSheet(backs), "RB");
  assert.equal(rb.tiersSuppressed, false);
  assert.deepEqual(rb.entries.map((entry) => entry.startsNewTier), [false, false, true],
    "a tier rule is drawn only where the tier actually changes");
  assert.equal(rb.entries[0].adpDelta, 2, "ADP 3 vs our rank 1 => +2");
  assert.equal(rb.entries[2].comparisonDelta, null, "non-DST columns carry no FP delta");
}

// Depth caps keep the sheet on one page.
{
  const receivers = Array.from({ length: 200 }, (_, index) =>
    row({ position: "WR", ourRank: index + 1, tier: 1 }));
  const wr = column(buildCheatSheet(receivers), "WR");
  assert.equal(wr.entries.length, CHEAT_SHEET_VARIANTS.rankings.depth.WR);
  assert.deepEqual(wr.entries.map((entry) => entry.positionRank).slice(0, 3), [1, 2, 3],
    "position rank is the printed 1..N order, not the overall board rank");
}

// One glyph per row, injury outranking buy/fade: an injury changes whether you
// draft him at all, buy/fade only changes when.
{
  const flagged = [row({
    position: "TE",
    indicators: [
      { code: "OUR_BUY", class: "model", label: "BUY", value: null, evidence: {} },
      { code: "INJURY", class: "risk", label: "Q", value: null, evidence: {} },
    ],
  })];
  assert.equal(column(buildCheatSheet(flagged), "TE").entries[0].signal, "injury");
}

// Every position renders a column even with an empty pool, so the printed grid
// never silently loses one.
{
  const sheet = buildCheatSheet([]);
  assert.deepEqual(sheet.map((entry) => entry.position), ["QB", "RB", "WR", "TE", "K", "DST"]);
  assert.ok(sheet.every((entry) => entry.entries.length === 0));
}

// --- format-specific variants ----------------------------------------------

// Best Ball drafts QB/RB/WR/TE only -- kickers and defenses are not in the pool
// at all, so printing them would put un-draftable players on the sheet.
{
  const pool = [
    ...Array.from({ length: 40 }, (_, i) => row({ position: "QB", ourRank: i + 1 })),
    ...Array.from({ length: 80 }, (_, i) => row({ position: "RB", ourRank: i + 100 })),
    ...Array.from({ length: 100 }, (_, i) => row({ position: "WR", ourRank: i + 200 })),
    ...Array.from({ length: 40 }, (_, i) => row({ position: "TE", ourRank: i + 400 })),
    ...Array.from({ length: 32 }, (_, i) => row({ position: "DST", ourRank: i + 500 })),
    ...Array.from({ length: 20 }, (_, i) => row({ position: "K", ourRank: i + 600 })),
  ];
  const sheet = buildCheatSheet(pool, "bestball");
  const positions = new Set(sheet.map((col) => col.position));
  assert.deepEqual([...positions], ["QB", "RB", "WR", "TE"], "Best Ball prints only draftable positions");
  assert.equal(sheet.some((col) => col.position === "K" || col.position === "DST"), false);

  // Depth must cover the whole draft: 12 teams x 20 rounds = 240 picks.
  const printed = sheet.reduce((total, col) => total + col.entries.length, 0);
  assert.equal(printed, BEST_BALL_TEAM_COUNT * BEST_BALL_ROUNDS,
    "Best Ball sheet must not run out before the last round does");

  // 96 receivers cannot fit one printed column, so the position spans several
  // and the continuation is flagged rather than silently truncated.
  const wrColumns = sheet.filter((col) => col.position === "WR");
  assert.ok(wrColumns.length > 1, "a deep position spills into continuation columns");
  assert.equal(wrColumns[0].continued, false);
  assert.ok(wrColumns.slice(1).every((col) => col.continued), "spilled columns are marked continued");
  assert.ok(sheet.every((col) => col.entries.length <= CHEAT_SHEET_VARIANTS.bestball.maxRowsPerColumn));

  // Position rank must run 1..N unbroken ACROSS the split, not restart per column.
  const wr = allEntries(sheet, "WR");
  assert.deepEqual(wr.map((e) => e.positionRank), wr.map((_, i) => i + 1),
    "position rank is continuous across a column split");
}

// Redraft is a 10-team/15-round Yahoo league that DOES roster K and DEF.
{
  const pool = [
    ...Array.from({ length: 40 }, (_, i) => row({ position: "QB", ourRank: i + 1 })),
    ...Array.from({ length: 60 }, (_, i) => row({ position: "RB", ourRank: i + 100 })),
    ...Array.from({ length: 60 }, (_, i) => row({ position: "WR", ourRank: i + 200 })),
    ...Array.from({ length: 30 }, (_, i) => row({ position: "TE", ourRank: i + 400 })),
    ...Array.from({ length: 32 }, (_, i) => row({ position: "DST", ourRank: i + 500 })),
    ...Array.from({ length: 20 }, (_, i) => row({ position: "K", ourRank: i + 600 })),
  ];
  const sheet = buildCheatSheet(pool, "redraft");
  const positions = sheet.map((col) => col.position);
  assert.ok(positions.includes("K") && positions.includes("DST"), "redraft rosters K and DEF");

  // Yahoo's UI calls the slot DEF; the printed sheet has to match the screen.
  assert.equal(column(sheet, "DST").label, REDRAFT_POSITION_LABEL.DST);
  assert.equal(column(sheet, "DST").label, "DEF");
  assert.equal(column(sheet, "QB").label, "QB", "only DST is relabelled");

  // Still no DST tiers, and still an FP delta -- the format changes the roster,
  // not what our defensive projections can support.
  assert.equal(column(sheet, "DST").tiersSuppressed, true);

  const printed = sheet.reduce((total, col) => total + col.entries.length, 0);
  assert.ok(printed >= REDRAFT_TEAM_COUNT * REDRAFT_ROUNDS,
    `redraft sheet covers all ${REDRAFT_TEAM_COUNT * REDRAFT_ROUNDS} picks`);
}

// The default argument keeps the original general board behaviour.
{
  const pool = Array.from({ length: 10 }, () => row({ position: "WR" }));
  assert.deepEqual(buildCheatSheet(pool), buildCheatSheet(pool, "rankings"));
}

// --- weeks 15-17 playoff slate marker --------------------------------------

function withSlate(position: string, code: string) {
  return row({
    position,
    indicators: [{ code, class: "context", label: code, value: null, evidence: {} }],
  });
}

// Best Ball scores weeks 15/16/17 as separate tournament rounds, so the marker
// belongs there.
{
  const sheet = buildCheatSheet([
    withSlate("WR", "PLAYOFF_SOS_SOFT"),
    withSlate("WR", "PLAYOFF_SOS_TOUGH"),
    withSlate("WR", "ROOKIE"),
  ], "bestball");
  const wr = sheet.filter((col) => col.position === "WR").flatMap((col) => col.entries);
  assert.deepEqual(wr.map((e) => e.playoffSlate), ["soft", "tough", null]);
  assert.equal(PLAYOFF_SLATE_VARIANTS.has("bestball"), true);
}

// It must NOT appear on the other sheets. A season-long league does not weight
// those weeks at draft time, and the signal is far too weak to put a marker on
// a board where it is not decision-relevant.
for (const variant of ["rankings", "redraft"] as const) {
  const sheet = buildCheatSheet([withSlate("WR", "PLAYOFF_SOS_SOFT")], variant);
  const wr = sheet.filter((col) => col.position === "WR").flatMap((col) => col.entries);
  assert.equal(wr[0].playoffSlate, null, `${variant} must not show the playoff slate marker`);
  assert.equal(PLAYOFF_SLATE_VARIANTS.has(variant), false);
}

// The slate marker is separate from the one-glyph signal, so a flagged player
// can still carry an injury/buy/fade mark without either displacing the other.
{
  const player = row({
    position: "RB",
    indicators: [
      { code: "PLAYOFF_SOS_SOFT", class: "context", label: "SOS", value: null, evidence: {} },
      { code: "INJURY", class: "risk", label: "Q", value: null, evidence: {} },
    ],
  });
  const rb = buildCheatSheet([player], "bestball").find((col) => col.position === "RB")!;
  assert.equal(rb.entries[0].playoffSlate, "soft");
  assert.equal(rb.entries[0].signal, "injury");
}

console.log("cheat sheet: all assertions passed");
