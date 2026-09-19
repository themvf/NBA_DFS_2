/**
 * Tests for the NFL slate-specials board shaping.
 *
 * The load-bearing checks are the honesty rules, not the arithmetic. This page
 * exists to present an ORDERING, and every way it could quietly overclaim is
 * asserted against here:
 *
 *   - a family with no rows still returns a panel, so a topic that stopped
 *     producing shows up empty instead of vanishing from the page;
 *   - blocked selections are carried with their reason, never dropped and never
 *     given a rank or a value;
 *   - no probability is manufactured from an expected-stats run;
 *   - the calibration note states the measured hit rate where one exists and
 *     admits the absence where it does not;
 *   - lowest-* families keep their own direction;
 *   - bars are scaled within a family's own range, since these ranges are
 *     narrow and a zero-anchored bar would make every row look identical.
 *
 * Run: npm run test:nfl-specials
 */

import {
  FAMILY_META,
  FAMILY_ORDER,
  barWidthPct,
  buildPanels,
  calibrationNote,
  familyMeta,
  formatAmerican,
  formatExpected,
  impliedProb,
  PROJECTION_STALE_AFTER_HOURS,
  projectionAgeHours,
  stalenessWarning,
  type SpecialsBoard,
  type SpecialsRow,
} from "../src/lib/nfl/specials-board";

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

function close(a: number, b: number, tol = 1e-9) {
  return Math.abs(a - b) < tol;
}

function row(over: Partial<SpecialsRow> & { family: string; selectionKey: string }): SpecialsRow {
  return {
    rank: 1,
    selectionLabel: over.selectionKey,
    statKey: "receiving_yards",
    expectedValue: 90,
    isProxy: false,
    pLeads: null,
    context: {},
    status: "ok",
    blockReason: null,
    marketAmerican: null,
    ...over,
  };
}

function board(rows: SpecialsRow[], over: Partial<SpecialsBoard> = {}): SpecialsBoard {
  return {
    season: 2026,
    week: 3,
    scope: "sunday_1pm",
    run: {
      runId: "run-1",
      modelVersion: "nfl-specials-board-v1",
      method: "expected_stats",
      generatedAt: "2026-09-19T12:00:00Z",
      projectionRunId: "proj-1",
      projectionAsOf: "2026-09-19T09:00:00Z",
      gitSha: "abc123",
      games: [],
      blockedReasons: [],
    },
    rows,
    captures: [],
    weeksInScope: [3],
    weeksAnyScope: [3],
    ...over,
  };
}

// ---------------------------------------------------------------------------
console.log("\nThe family contract");
// ---------------------------------------------------------------------------

{
  check("seventeen families are described", FAMILY_META.length === 17, String(FAMILY_META.length));
  check(
    "family order matches Python's FAMILIES tuple so the two boards are diffable",
    FAMILY_ORDER.join(",") ===
      [
        "highest_scoring_game",
        "lowest_scoring_game",
        "highest_scoring_team",
        "lowest_scoring_team",
        "most_passing_yards",
        "most_receiving_yards",
        "most_rushing_yards",
        "first_td_scorer",
        "first_qb_td_pass",
        "first_qb_int",
        "all_teams_td",
        "all_teams_two_td",
        "all_teams_fg",
        "all_teams_td_and_fg",
        "all_teams_passing_td",
        "all_teams_rushing_td",
        "all_teams_score",
      ].join(","),
  );
  check(
    "ranked and proposition families are distinguished",
    FAMILY_META.filter((m) => m.kind === "proposition").length === 7 &&
      FAMILY_META.filter((m) => m.kind === "ranked").length === 10,
  );
  check(
    "a proposition carries no hit-rate claim, since it is not a ranking",
    FAMILY_META.filter((m) => m.kind === "proposition").every((m) => m.topOneLedPct === null),
  );
  check(
    "a proposition's note says it is a probability and discloses the independence gap",
    (() => {
      const note = calibrationNote(familyMeta("all_teams_td")!);
      return note.includes("probability") && note.includes("optimistic");
    })(),
  );
  const proxies = FAMILY_META.filter((m) => m.isProxy).map((m) => m.family);
  check(
    "exactly the timing families are proxies",
    proxies.join(",") === "first_td_scorer,first_qb_td_pass,first_qb_int",
    proxies.join(","),
  );
  check(
    "every proxy family explains what it is a proxy for",
    FAMILY_META.filter((m) => m.isProxy).every((m) => Boolean(m.proxyNote)),
  );
  check(
    "the lowest-* families are the only ascending ones",
    FAMILY_META.filter((m) => m.ascending)
      .map((m) => m.family)
      .join(",") === "lowest_scoring_game,lowest_scoring_team",
  );
  check("an unknown family resolves to null rather than a default", familyMeta("longest_field_goal") === null);
}

// ---------------------------------------------------------------------------
console.log("\nPanels");
// ---------------------------------------------------------------------------

{
  const panels = buildPanels(board([row({ family: "most_receiving_yards", selectionKey: "a" })]));
  check("one panel per family, always", panels.length === 17, String(panels.length));
  check(
    "panels keep family order",
    panels.map((p) => p.meta.family).join(",") === FAMILY_ORDER.join(","),
  );
  const empty = panels.find((p) => p.meta.family === "first_qb_int")!;
  check(
    "a family with no rows is still returned, so a vanished topic is visible",
    empty.ranked.length === 0 && empty.blocked.length === 0,
  );
}

{
  const rows = [
    row({ family: "most_receiving_yards", selectionKey: "c", rank: 3, expectedValue: 60 }),
    row({ family: "most_receiving_yards", selectionKey: "a", rank: 1, expectedValue: 95 }),
    row({ family: "most_receiving_yards", selectionKey: "b", rank: 2, expectedValue: 80 }),
  ];
  const panel = buildPanels(board(rows)).find((p) => p.meta.family === "most_receiving_yards")!;
  check(
    "ranked rows come back in rank order regardless of input order",
    panel.ranked.map((r) => r.selectionKey).join(",") === "a,b,c",
  );
  check("bar scale uses the family's own max and min", panel.barMax === 95 && panel.barMin === 60);
}

{
  const rows = [
    row({ family: "highest_scoring_team", selectionKey: "KC", rank: 1, expectedValue: 25.5 }),
    row({
      family: "highest_scoring_team",
      selectionKey: "CIN",
      rank: null,
      expectedValue: null,
      status: "blocked",
      blockReason: "no_quoted_total",
    }),
  ];
  const panel = buildPanels(board(rows)).find((p) => p.meta.family === "highest_scoring_team")!;
  check("blocked rows are separated from ranked ones", panel.ranked.length === 1 && panel.blocked.length === 1);
  check("a blocked row keeps its reason", panel.blocked[0].blockReason === "no_quoted_total");
  check(
    "a blocked row carries neither a rank nor a value",
    panel.blocked[0].rank === null && panel.blocked[0].expectedValue === null,
  );
  check("a blocked row does not affect the bar scale", panel.barMax === 25.5);
}

// ---------------------------------------------------------------------------
console.log("\nNo probability is invented");
// ---------------------------------------------------------------------------

{
  const panels = buildPanels(
    board([
      row({ family: "most_receiving_yards", selectionKey: "a", expectedValue: 95 }),
      row({ family: "first_td_scorer", selectionKey: "b", expectedValue: 0.9, statKey: "expected_touchdowns" }),
    ]),
  );
  const everyRow = panels.flatMap((p) => [...p.ranked, ...p.blocked]);
  check(
    "an expected_stats run publishes no p_leads anywhere",
    everyRow.length > 0 && everyRow.every((r) => r.pLeads === null),
  );
}

{
  const measured = calibrationNote(familyMeta("most_receiving_yards")!);
  check("a measured family states its real hit rate", measured.includes("11.1%"), measured);
  check("and where the actual leader usually sits", measured.includes("15th"), measured);
  check("and refuses to call it a pick", measured.includes("not a pick"), measured);
  const unmeasured = calibrationNote(familyMeta("highest_scoring_team")!);
  check(
    "an unmeasured family admits the absence rather than borrowing a number",
    unmeasured.includes("has not been measured") && !/\d+\.\d%/.test(unmeasured),
    unmeasured,
  );
}

// ---------------------------------------------------------------------------
console.log("\nBars encode magnitude, not probability");
// ---------------------------------------------------------------------------

{
  check("the leader fills the bar", close(barWidthPct(95, 95, 60), 100));
  check("the trailing row keeps a visible floor", close(barWidthPct(60, 95, 60), 6));
  check(
    "a midpoint lands between the two",
    barWidthPct(77.5, 95, 60) > 50 && barWidthPct(77.5, 95, 60) < 55,
    String(barWidthPct(77.5, 95, 60)),
  );
  check("a single-row family does not divide by zero", close(barWidthPct(25, 25, 25), 100));
  check("a null value has no bar", barWidthPct(null, 95, 60) === 0);
  check("an empty family has no bar", barWidthPct(10, 0, 0) === 0);
}

// ---------------------------------------------------------------------------
console.log("\nFormatting and the optional market");
// ---------------------------------------------------------------------------

{
  check("points show one decimal", formatExpected(25.47, familyMeta("highest_scoring_team")!) === "25.5");
  check("yards show none", formatExpected(92.4, familyMeta("most_receiving_yards")!) === "92");
  check("touchdowns show two", formatExpected(0.856, familyMeta("first_td_scorer")!) === "0.86");
  check("a missing value renders as a dash, not a zero", formatExpected(null, FAMILY_META[0]) === "—");

  check("a positive price keeps its sign", formatAmerican(750) === "+750");
  check("a negative price keeps its sign", formatAmerican(-120) === "-120");
  check("no price renders as a dash", formatAmerican(null) === "—");

  check("implied probability of -110 is about 52.4%", close(impliedProb(-110)!, 110 / 210));
  check("implied probability of +100 is exactly half", close(impliedProb(100)!, 0.5));
  check("no price gives no implied probability", impliedProb(null) === null);
}

{
  const withCapture = board([row({ family: "first_td_scorer", selectionKey: "a", marketAmerican: 750 })], {
    captures: [
      { family: "first_td_scorer", capturedAt: "2026-09-20T13:00:00Z", selections: 180, overround: 1.4 },
    ],
  });
  const panel = buildPanels(withCapture).find((p) => p.meta.family === "first_td_scorer")!;
  check("a capture attaches to its own family", panel.capture?.selections === 180);
  check("and carries the overround", close(panel.capture!.overround!, 1.4));
  const other = buildPanels(withCapture).find((p) => p.meta.family === "most_passing_yards")!;
  check("a family with no capture has none", other.capture === null);
}

{
  const noRun = board([], { run: null, rows: [], weeksInScope: [], weeksAnyScope: [] });
  check("a board with no run still yields panels rather than throwing", buildPanels(noRun).length === 17);
  check(
    "and none of them claim any rows",
    buildPanels(noRun).every((p) => p.ranked.length === 0),
  );
}

// ---------------------------------------------------------------------------
console.log("\nThe page never invents a week");
// ---------------------------------------------------------------------------

{
  // Mirrors the server component's resolution order. It used to end in `?? 1`,
  // which made an empty database announce "no board for week 1" -- naming a
  // week nobody asked about. 0 means "no week to show".
  const resolve = (requested: number | null, inScope: number[], anyScope: number[]) =>
    requested ?? inScope[inScope.length - 1] ?? anyScope[anyScope.length - 1] ?? 0;

  check("an explicit week always wins", resolve(2, [5], [5]) === 2);
  check("otherwise the newest week in this scope", resolve(null, [1, 2], [1, 2, 3]) === 2);
  check(
    "falling back to another scope rather than showing nothing",
    resolve(null, [], [1, 2]) === 2,
  );
  check("and 0 when nothing exists, never a fabricated week 1", resolve(null, [], []) === 0);
}

// ---------------------------------------------------------------------------
console.log("\nThe board checks its own input freshness");
// ---------------------------------------------------------------------------

{
  // refresh_nfl_dfs_projections reports FAILURE on every run for a shadow step
  // unrelated to the projection build, so its red X cannot signal a real
  // outage. The board therefore judges the projections it actually ranked.
  check("the stale threshold is 36 hours", PROJECTION_STALE_AFTER_HOURS === 36);

  const run = (asOf: string | null, generatedAt = "2026-09-19T21:00:00Z", projId: string | null = "p1") => ({
    runId: "r", modelVersion: "nfl-specials-board-v1", method: "expected_stats",
    generatedAt, projectionRunId: projId, projectionAsOf: asOf,
    gitSha: null, games: [], blockedReasons: [],
  });

  check("age is measured from the projections to the board", 
    Math.round(projectionAgeHours(run("2026-09-19T09:00:00Z"))!) === 12);
  check("fresh projections raise no warning", stalenessWarning(run("2026-09-19T09:00:00Z")) === null);
  check(
    "projections a day and a half old do",
    (stalenessWarning(run("2026-09-18T05:00:00Z")) ?? "").includes("not ranking current numbers"),
  );
  check(
    "and the warning says how old in plain units",
    (stalenessWarning(run("2026-09-16T21:00:00Z")) ?? "").includes("3 days"),
    stalenessWarning(run("2026-09-16T21:00:00Z")) ?? "",
  );
  check(
    "no projection run at all is its own warning, not silence",
    (stalenessWarning(run(null, "2026-09-19T21:00:00Z", null)) ?? "").includes("player topics are empty"),
  );
  check("an unknown cutoff does not manufacture a warning", stalenessWarning(run(null)) === null);
  check("no run means nothing to warn about", stalenessWarning(null) === null);
}

console.log(`\n${passed} passed, ${failed} failed\n`);
if (failed > 0) process.exit(1);
