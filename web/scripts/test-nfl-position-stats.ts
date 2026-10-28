/**
 * Expected position stats (`nfl-position-stats-v1`): which source wins each
 * stat, the TD scaling rule, and that team and league totals add up.
 */
import assert from "node:assert/strict";
import { CALIBRATION, anytimeTdRate } from "../src/lib/nfl-dfs/market-anchor";
import { TD_SCALE_CAP, buildWeek, expectedStats, impliedPoints, nameKey, vegasMean, type PlayerInput } from "../src/lib/nfl/position-stats";

const near = (a: number, b: number, tol = 1e-6, msg = "") => assert.ok(Math.abs(a - b) <= tol, `${msg} ${a} vs ${b}`);
const player = (over: Partial<PlayerInput>): PlayerInput => ({
  identity: "id", name: "Player", team: "AAA", position: "WR",
  sim: { rushing_yards: 2, receptions: 5, receiving_yards: 60 },
  model: { rushing_yards: 3, receiving_yards: 70, rushing_tds: 0.05, receiving_tds: 0.45 },
  lines: null, ruledOut: false, ...over,
});

// No lines: simulation for volume, model TDs scaled by sim yards / model yards.
{
  const { stats, sources } = expectedStats(player({}), "WR");
  assert.deepEqual(sources, { rushYds: "sim", rec: "sim", recYds: "sim", tds: "model" });
  near(stats.recYds, 60);
  near(stats.tds, 0.5 * (62 / 73), 1e-9, "scaled TDs");
}

// A backup the simulation uses far more than the model: the TD scale is capped.
{
  const { stats } = expectedStats(player({ sim: { rushing_yards: 0, receptions: 6, receiving_yards: 90 },
    model: { rushing_yards: 0, receiving_yards: 20, rushing_tds: 0, receiving_tds: 0.1 } }), "WR");
  near(stats.tds, 0.1 * TD_SCALE_CAP, 1e-9, "cap");
}

// A player the model gives no yardage gets no TDs from it.
{
  const { stats } = expectedStats(player({ model: { rushing_yards: 0, receiving_yards: 0, rushing_tds: 0.2, receiving_tds: 0 } }), "WR");
  near(stats.tds, 0);
}

// Full set of WR lines: receptions and receiving yards from Vegas, rushing stays simulated.
{
  const lines = { player_receptions: { line: 5.5, over: -120, under: -105 }, player_reception_yds: { line: 64.5, over: -115, under: -110 },
    player_anytime_td: { yes: 150 } };
  const { stats, sources } = expectedStats(player({ lines }), "WR");
  assert.deepEqual(sources, { rushYds: "sim", rec: "vegas", recYds: "vegas", tds: "vegas" });
  near(stats.recYds, vegasMean("player_reception_yds", lines.player_reception_yds)!);
  assert.ok(stats.recYds > 64.5 * 1.1, "receiving yards calibrated up from the median line");
  near(stats.tds, anytimeTdRate(150)! * CALIBRATION.player_anytime_td);
  near(stats.rushYds, 2);
}

// A partial set (receiving yards but no receptions line) is not used; the TD line still is.
{
  const lines = { player_reception_yds: { line: 80.5, over: -115, under: -110 }, player_anytime_td: { yes: 120 } };
  const { stats, sources } = expectedStats(player({ lines }), "WR");
  assert.deepEqual(sources, { rushYds: "sim", rec: "sim", recYds: "sim", tds: "vegas" });
  near(stats.recYds, 60);
}

// Team budget: a ruled-out player's share goes to his teammates, QBs and unknown positions are not counted.
{
  const week = buildWeek([{ gameId: "g1", kickoff: "2026-11-01T17:00:00Z", home: "AAA", away: "BBB", homeSpread: -3, total: 44,
    simTeamReceivingYards: { AAA: 150, BBB: 50 },
    players: [
      player({ identity: "a", position: "RB", sim: { rushing_yards: 80, receptions: 3, receiving_yards: 20 } }),
      player({ identity: "b", position: "WR" }),
      player({ identity: "c", position: "TE", ruledOut: true, sim: { rushing_yards: 0, receptions: 4, receiving_yards: 40 } }),
      player({ identity: "d", position: "QB" }),
      player({ identity: "e", team: "BBB", position: "TE", sim: { rushing_yards: 0, receptions: 4, receiving_yards: 40 } }),
      player({ identity: "f", position: null }),
    ] }]);
  const aaa = week.teams.find((t) => t.team === "AAA")!;
  near(aaa.total.recYds, 120, 1e-9, "budget keeps the ruled-out TE's 40 yards");
  near(aaa.groups.RB.recYds, 120 * 20 / 80, 1e-9, "pro rata");
  near(aaa.groups.TE.recYds, 0);
  near(aaa.total.rec, 12);
  near(aaa.groups.TE.tds, 0);
  assert.deepEqual(aaa.ruledOut, ["Player"]);
  assert.equal(aaa.receivingBudget.source, "sim");
  near(week.league.ALL.recYds, 160);
  near(week.league.TE.recYds, 40);
  assert.equal(week.players.length, 4, "QB and unknown position are not rows");
  assert.deepEqual(week.skipped, { noPosition: 1, ruledOut: 1 });
  near(aaa.impliedPoints!, 23.5);
  near(week.teams.find((t) => t.team === "BBB")!.impliedPoints!, 20.5);
}

// The quarterback's passing line sets receiving yards; lined receivers split it by their lines.
{
  const qbLine = { line: 257.5, over: -112, under: -112 };
  const full = (line: number) => ({ player_receptions: { line: 4.5, over: -110, under: -110 },
    player_reception_yds: { line, over: -110, under: -110 }, player_anytime_td: { yes: 200 } });
  const week = buildWeek([{ gameId: "g2", kickoff: "2026-11-01T17:00:00Z", home: "GB", away: "CAR", homeSpread: null, total: null,
    simTeamReceivingYards: { GB: 250, CAR: 0 },
    players: [
      player({ identity: "q", team: "GB", name: "Jordan Love", position: "QB", lines: { player_pass_yds: qbLine },
        sim: { rushing_yards: 5, receptions: 0, receiving_yards: 0 } }),
      player({ identity: "w1", team: "GB", position: "WR", lines: full(70.5), sim: { rushing_yards: 0, receptions: 5, receiving_yards: 80 } }),
      player({ identity: "w2", team: "GB", position: "WR", lines: full(50.5), sim: { rushing_yards: 0, receptions: 5, receiving_yards: 80 } }),
      player({ identity: "t", team: "GB", position: "TE", sim: { rushing_yards: 0, receptions: 4, receiving_yards: 40 } }),
    ] }]);
  const gb = week.teams.find((t) => t.team === "GB")!;
  const qb = vegasMean("player_pass_yds", qbLine)!;
  assert.equal(gb.receivingBudget.source, "vegas");
  assert.equal(gb.receivingBudget.quarterback, "Jordan Love");
  near(gb.total.recYds, qb * 200 / 250, 1e-9, "QB line times the RB/WR/TE share");
  const [w1, w2] = week.players.filter((p) => p.group === "WR");
  near(w1.stats.recYds / w2.stats.recYds, w1.own.recYds / w2.own.recYds, 1e-9, "split by own estimates");
}

assert.equal(impliedPoints(null, -3, true), null);
assert.equal(nameKey("Marvin Harrison Jr."), nameKey("Marvin Harrison"));
assert.equal(nameKey("Amon-Ra St. Brown"), "amonrastbrown");
assert.equal(nameKey("Kenneth Walker III"), "kennethwalker");
console.log("nfl position stats: ok");
