import {
  computeMlbGpp2HitterOrderBonus,
  type MlbOptimizerPlayer,
} from "../src/app/dfs/mlb-optimizer";

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) {
    throw new Error(message);
  }
}

const basePlayer: MlbOptimizerPlayer = {
  id: 1,
  dkPlayerId: 1,
  name: "Order Test",
  teamAbbrev: "TST",
  teamId: 1,
  matchupId: 1,
  eligiblePositions: "OF",
  salary: 4200,
  ourProj: 8.5,
  ourLeverage: 0,
  linestarProj: null,
  projOwnPct: 8,
  projCeiling: 14,
  boomRate: 0.1,
  dkInStartingLineup: true,
  dkStartingLineupOrder: 3,
  dkTeamLineupConfirmed: true,
  isOut: false,
  gameInfo: "TST@OPP",
  teamLogo: null,
  teamName: "Test",
  homeTeamId: 1,
  awayTeamId: 2,
  vegasTotal: 9,
  homeImplied: 5.1,
  awayImplied: 3.9,
  hrProb1Plus: 0.12,
  propPts: null,
  propReb: null,
  propAst: null,
};

const gpp2Bonus = computeMlbGpp2HitterOrderBonus(basePlayer, "gpp2");
assert(gpp2Bonus > 0, `expected positive GPP2 bonus, got ${gpp2Bonus}`);
assert(computeMlbGpp2HitterOrderBonus(basePlayer, "gpp") === 0, "expected no bonus in balanced GPP");
assert(
  computeMlbGpp2HitterOrderBonus({ ...basePlayer, dkStartingLineupOrder: 1 }, "gpp2") === 0,
  "expected no #1 bonus",
);
assert(
  computeMlbGpp2HitterOrderBonus({ ...basePlayer, eligiblePositions: "SP" }, "gpp2") === 0,
  "expected no pitcher bonus",
);

const chalkBonus = computeMlbGpp2HitterOrderBonus({ ...basePlayer, projOwnPct: 28 }, "gpp2");
assert(chalkBonus < gpp2Bonus, `expected chalk penalty in order bonus, got ${chalkBonus} >= ${gpp2Bonus}`);

console.log(`PASS mlb gpp2 order bonus (${gpp2Bonus.toFixed(3)} vs chalk ${chalkBonus.toFixed(3)})`);
