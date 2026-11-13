/**
 * Team split targets (`nfl-team-split-targets-v1`, Showdown): a split that is
 * given gets that share of the run and the splits left out share the rest; a
 * heavy team narrows 4-2 and 5-1 to that team's larger side; a lineup that
 * cannot be built at its split is built without it and counted; the joint
 * selection keeps the same counts. Nothing here changes a projection.
 */
import assert from "node:assert/strict";
import { optimizeNflLineups, type NflOptimizerPlayer, type NflOptimizerSettings } from "../src/app/dfs/nfl/nfl-optimizer";
import { resolveTeamSplitPlan, teamSplitOf } from "../src/lib/nfl-dfs/team-split";
import { settingsFromForm } from "../src/lib/nfl-dfs/generation-settings";

// --- The plan: which counts of the first team each split allows ---
assert.equal(resolveTeamSplitPlan(undefined, undefined, ["AAA", "BBB"]), null);
assert.equal(resolveTeamSplitPlan({}, "AAA", ["AAA", "BBB"]), null, "a heavy side alone targets nothing");
assert.equal(resolveTeamSplitPlan({ "3-3": 0.5 }, undefined, ["AAA", "BBB", "CCC"]), null, "not a two-team slate");
assert.deepEqual(resolveTeamSplitPlan({ "3-3": 0.5 }, undefined, ["BBB", "AAA", "AAA"]),
  { firstTeam: "AAA", heavyTeam: null, classes: [{ split: "3-3", share: 0.5, counts: [3] }], restCounts: [1, 2, 4, 5] });
assert.deepEqual(resolveTeamSplitPlan({ "5-1": 0.4 }, undefined, ["AAA", "BBB"])!.classes[0].counts, [1, 5]);
assert.deepEqual(resolveTeamSplitPlan({ "5-1": 0.4 }, "AAA", ["AAA", "BBB"])!.classes[0].counts, [5]);
const heavySecond = resolveTeamSplitPlan({ "5-1": 0.4, "4-2": 0.2 }, "BBB", ["AAA", "BBB"])!;
assert.deepEqual(heavySecond.classes.map((c) => c.counts), [[2], [1]], "classes come in 3-3, 4-2, 5-1 order; BBB heavy means AAA holds the small side");
assert.deepEqual(heavySecond.restCounts, [3, 4, 5], "the mirror image stays open to the other lineups");
assert.throws(() => resolveTeamSplitPlan({ "5-1": 0.4 }, "ZZZ", ["AAA", "BBB"]), /not a team on this slate/);

function player(over: Partial<NflOptimizerPlayer> & { dkPlayerId: number; position: NflOptimizerPlayer["position"]; team: string }): NflOptimizerPlayer {
  const opponent = over.team === "AAA" ? "BBB" : "AAA";
  return {
    id: over.dkPlayerId, dkPlayerId: over.dkPlayerId, captainDkPlayerId: over.dkPlayerId + 100_000,
    name: over.name ?? `${over.team}-${over.position}${over.dkPlayerId}`, position: over.position, team: over.team, opponent, gameKey: "AAA@BBB",
    salary: over.salary ?? 5000, captainSalary: Math.round((over.salary ?? 5000) * 1.5), isOut: false, projectionStatus: "historical",
    historyGames: 6, ourProj: over.ourProj ?? 10, floorFpts: 6, ceilingFpts: over.ceilingFpts ?? (over.ourProj ?? 10) * 1.8, boomRate: 0.2,
    avgFptsDk: 10, fantasyprosProj: null, linestarProj: null, linestarOwnPct: null, customProj: null, priorOpportunity: null,
  };
}
function settings(over: Partial<NflOptimizerSettings> = {}): NflOptimizerSettings {
  return {
    format: "showdown", mode: "gpp", projectionSource: "our", allowDkFallback: true,
    nLineups: 20, minSalary: 0, maxExposure: 1, minUnique: 1, stackPassCatchers: 0, bringBack: true, randomness: 0,
    lockedPlayerIds: [], excludedPlayerIds: [], minExposureByPlayer: {}, maxExposureByPlayer: {}, ...over,
  };
}
// AAA is the stronger side, so an unconstrained build leans AAA.
const positions: NflOptimizerPlayer["position"][] = ["QB", "RB", "WR", "WR", "TE", "RB", "WR", "TE", "WR", "RB"];
const pool: NflOptimizerPlayer[] = [];
let id = 300;
for (let i = 0; i < 10; i++) pool.push(player({ dkPlayerId: id++, position: positions[i], team: "AAA", salary: 9000 - 700 * i, ourProj: 21 - 1.7 * i }));
for (let i = 0; i < 10; i++) pool.push(player({ dkPlayerId: id++, position: positions[i], team: "BBB", salary: 8800 - 700 * i, ourProj: 19 - 1.6 * i }));
const first = (l: { slots: { player: { team: string } }[] }) => l.slots.filter((s) => s.player.team === "AAA").length;
const tally = (lineups: { slots: { player: { team: string } }[] }[]) => {
  const out = { "3-3": 0, "4-2": 0, "5-1": 0 };
  for (const l of lineups) out[first(l) === 3 ? "3-3" : first(l) === 2 || first(l) === 4 ? "4-2" : "5-1"] += 1;
  return out;
};
const note = (r: { warnings: string[] }) => r.warnings.find((w) => w.startsWith("Team split targets")) ?? "";

const free = optimizeNflLineups(pool, settings());
assert.equal(free.lineups.length, 20);
assert.equal(note(free), "", "no target, no note");
assert.ok(tally(free.lineups)["3-3"] < 10, `the unconstrained build is not half 3-3 (${JSON.stringify(tally(free.lineups))}), so the next check binds`);

// --- One split given: it gets its share, the rest stay off it ---
const half = optimizeNflLineups(pool, settings({ teamSplitTargets: { "3-3": 0.5 } }));
assert.equal(half.lineups.length, 20, half.warnings.join(" | "));
assert.equal(tally(half.lineups)["3-3"], 10, "50% of 20 lineups are 3-3, and the other ten are not");
assert.match(note(half), /of 20 lineups, 10 are 3-3 \(target 50%\)\.$/);

const fiveOne = optimizeNflLineups(pool, settings({ teamSplitTargets: { "5-1": 0.4 } }));
assert.equal(tally(fiveOne.lineups)["5-1"], 8, "40% of 20 lineups are 5-1");

// --- A heavy side: only that team's larger side counts, and is what gets built ---
const heavy = optimizeNflLineups(pool, settings({ teamSplitTargets: { "5-1": 0.4 }, teamSplitHeavyTeam: "BBB" }));
assert.equal(heavy.lineups.filter((l) => first(l) === 1).length, 8, "eight lineups have five from BBB, the weaker side the free build avoids");
assert.match(note(heavy), /8 are 5-1 with BBB on the larger side \(target 40%\)/);

// --- All three given ---
const all = optimizeNflLineups(pool, settings({ teamSplitTargets: { "3-3": 0.5, "4-2": 0.25, "5-1": 0.25 } }));
assert.deepEqual(tally(all.lineups), { "3-3": 10, "4-2": 5, "5-1": 5 });
const zero = optimizeNflLineups(pool, settings({ teamSplitTargets: { "4-2": 0 } }));
assert.equal(tally(zero.lineups)["4-2"], 0, "a 0% share keeps that split out");

// --- A split the other rules cannot build is relaxed and counted, never dropped ---
const bbb = pool.filter((p) => p.team === "BBB").map((p) => p.dkPlayerId);
const thin = optimizeNflLineups(pool, settings({ nLineups: 6, teamSplitTargets: { "5-1": 1 }, teamSplitHeavyTeam: "BBB", excludedPlayerIds: bbb.slice(4) }));
assert.equal(thin.lineups.length, 6, "four BBB players cannot make a five-BBB lineup; the run still builds");
assert.match(note(thin), /0 are 5-1 with BBB on the larger side \(target 100%\); 6 lineup\(s\) could not be built at their split/);

// --- With the Winners shape and joint selection (projected-points scoring) the counts still hold ---
const joint = optimizeNflLineups(pool, settings({ buildShape: "winners", portfolioSelection: "joint", teamSplitTargets: { "3-3": 0.5 }, randomness: 0.08 }));
assert.equal(joint.lineups.length, 20, joint.warnings.join(" | "));
assert.equal(tally(joint.lineups)["3-3"], 10, `joint selection keeps the split count: ${note(joint)}`);
assert.equal(teamSplitOf(resolveTeamSplitPlan({ "3-3": 0.5 }, undefined, ["AAA", "BBB"])!, joint.lineups.find((l) => first(l) === 3)!), "3-3");

// --- Validation ---
assert.throws(() => optimizeNflLineups(pool, settings({ teamSplitTargets: { "3-3": 0.7, "5-1": 0.5 } })), /more than 100%/);
assert.throws(() => optimizeNflLineups(pool, settings({ teamSplitTargets: { "3-3": 0.5, "4-2": 0.2, "5-1": 0.2 } })), /must add to 100%/);
assert.throws(() => optimizeNflLineups(pool, settings({ teamSplitTargets: { "4-2": 0.5 }, forbidFourTwoSplit: true })), /4-2 lineups are banned/);
assert.throws(() => optimizeNflLineups(pool, settings({ format: "classic", teamSplitTargets: { "3-3": 0.5 } })), /Showdown rule/);
assert.throws(() => optimizeNflLineups(pool, settings({ teamSplitTargets: { "5-1": 0.4 }, teamSplitHeavyTeam: "ZZZ" })), /not a team on this slate/);

// --- The build form: empty means none, Classic never carries it, a stale heavy side is dropped ---
const form = (s: Record<string, unknown>) => ({ settings: { ...settings(), ...s } as never, locked: [], excluded: [], targets: {}, captainTargets: {}, ownership: {},
  planMode: "balanced" as const, quotas: [], favorite: "", fades: [] });
const sent = settingsFromForm(form({ teamSplitTargets: { "5-1": 0.4 }, teamSplitHeavyTeam: "BBB" }), "showdown", ["AAA", "BBB"]);
assert.deepEqual([sent.teamSplitTargets, sent.teamSplitHeavyTeam], [{ "5-1": 0.4 }, "BBB"]);
assert.equal(settingsFromForm(form({ teamSplitTargets: { "5-1": 0.4 }, teamSplitHeavyTeam: "NYG" }), "showdown", ["AAA", "BBB"]).teamSplitHeavyTeam, "", "a heavy side from another slate is dropped");
const classic = settingsFromForm(form({ teamSplitTargets: { "5-1": 0.4 }, teamSplitHeavyTeam: "BBB" }), "classic", ["AAA", "BBB"]);
assert.deepEqual([classic.teamSplitTargets, classic.teamSplitHeavyTeam], [{}, ""]);

console.log("nfl team split targets: ok");
