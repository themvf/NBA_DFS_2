/**
 * Tests for the team identity ranks shown on the pick'em live board.
 *
 * Load-bearing: offense ranks highest EPA first, defense ranks LOWEST EPA
 * allowed first, a thin sample gets no rank (never a zero), and the style cut
 * points are the stated constants.
 *
 * Run: npm run test:team-identity-ranks
 */

import type { NflIdentityTotals } from "../src/db/nfl-team-identity";
import {
  MIN_IDENTITY_PLAYS,
  PASS_HEAVY_MIN_PASS_OE,
  RUN_HEAVY_MAX_PASS_OE,
  rankTeamIdentities,
  styleOf,
} from "../src/lib/nfl/team-identity-ranks";

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

const totals = (team: string, offEpa: number, defEpa: number, over: Partial<NflIdentityTotals> = {}): NflIdentityTotals => ({
  team, games: 6, offensePlays: 360, offenseEpaSum: offEpa * 350, offenseEpaCount: 350,
  offenseExplosives: 36, passOeSum: 0, passOeCount: 300, drives: 60, touchdownDrives: 15,
  threeAndOutDrives: 12, defensePlays: 360, defenseEpaSum: defEpa * 350, defenseEpaCount: 350, ...over,
});

const ids = rankTeamIdentities([
  totals("BUF", 0.20, 0.10),
  totals("KC", 0.10, -0.10, { passOeSum: 3 * 300 }),
  totals("DEN", -0.05, -0.12, { passOeSum: -2 * 300 }),
  totals("TB", 0.10, 0.00, { passOeSum: -5.6 * 300 }),
  totals("NEW", 0.50, -0.50, { offensePlays: MIN_IDENTITY_PLAYS - 1 }),
]);

check("offense rank 1 is the highest EPA", ids.BUF.offenseRank === 1);
check("defense rank 1 is the lowest EPA allowed", ids.DEN.defenseRank === 1 && ids.KC.defenseRank === 2);
check("worst defense ranks last", ids.BUF.defenseRank === 4);
check("tied offense EPA shares the better rank", ids.KC.offenseRank === 2 && ids.TB.offenseRank === 2);
check("ranked denominator excludes thin samples", ids.BUF.rankedTeams === 4);
check("thin sample is not ranked", !ids.NEW.enough && ids.NEW.offenseRank === null && ids.NEW.defenseRank === null && ids.NEW.style === null);
check("thin sample does not displace others", ids.BUF.offenseRank === 1);
check("rates", Math.abs(ids.BUF.explosiveRate! - 0.1) < 1e-9 && ids.BUF.touchdownDriveRate === 0.25 && ids.BUF.threeAndOutRate === 0.2);
check("pass-heavy", ids.KC.style === "pass-heavy");
check("balanced", ids.DEN.style === "balanced");
check("run-heavy", ids.TB.style === "run-heavy");
check("cut points are inclusive at the stated constants",
  styleOf(RUN_HEAVY_MAX_PASS_OE) === "run-heavy" && styleOf(PASS_HEAVY_MIN_PASS_OE) === "pass-heavy" && styleOf(null) === null);
check("no passing sample, no style", rankTeamIdentities([totals("X", 0, 0, { passOeCount: 0 })]).X.style === null);

console.log(`${passed} passed, ${failed} failed`);
if (failed) process.exit(1);
