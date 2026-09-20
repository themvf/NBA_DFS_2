import assert from "node:assert/strict";
import {
  MAX_MULTIPLIER, MIN_OBSERVED_GAMES, POOLS, VERSION, hasObservedOpportunity,
  inheritanceNote, redistributeOutOpportunity, type RedistributionRow,
} from "../src/lib/nfl-dfs/opportunity-redistribution";
import { scoreNflOffense, scoreNflOffenseLinear } from "../src/lib/nfl-dfs/scoring";

const player = (over: Partial<RedistributionRow> & Pick<RedistributionRow, "key" | "name" | "position">): RedistributionRow => ({
  team: "HOU", isOut: false, statMeans: {}, ourProj: 10, floorFpts: 4, ceilingFpts: 20,
  // A real track record by default. `MIN_OBSERVED_GAMES` is exercised
  // deliberately below; every other case here is about allocation, not the gate.
  historyGames: 17, depthOrder: over.isOut ? 1 : 2, ...over,
});

const receiver = (key: number, name: string, receptions: number, over: Partial<RedistributionRow> = {}) =>
  player({
    key, name, position: "WR",
    statMeans: { receptions, receiving_yards: receptions * 12, receiving_tds: receptions * 0.05 },
    ourProj: receptions * 12 * 0.1 + receptions + receptions * 0.05 * 6,
    ...over,
  });

const find = (report: ReturnType<typeof redistributeOutOpportunity>, key: number) =>
  report.applied.find(r => r.key === key);

// ── A ruled-out receiver pays the whole pass-catching group ────────────
{
  // Collins out. Targets do not stay in the WR room: the TE and the
  // pass-catching back are paid too, proportional to their own receptions.
  const rows = [
    receiver(1, "Nico Collins", 6, { isOut: true }),
    receiver(2, "Tank Dell", 4),
    player({ key: 3, name: "Dalton Schultz", position: "TE",
      statMeans: { receptions: 3, receiving_yards: 30, receiving_tds: 0.1 }, ourProj: 6.6 }),
    player({ key: 4, name: "Joe Mixon", position: "RB",
      statMeans: { receptions: 3, receiving_yards: 24, carries: 15, rushing_yards: 65, rushing_tds: 0.4 },
      ourProj: 14.3 }),
  ];
  const report = redistributeOutOpportunity(rows);
  assert.equal(report.version, VERSION);

  const dell = find(report, 2), schultz = find(report, 3), mixon = find(report, 4);
  assert.ok(dell && schultz && mixon, "every available pass catcher inherits, not just the WRs");

  // 6 receptions split across own shares 4 / 3 / 3 = 2.4 / 1.8 / 1.8.
  assert.equal(dell!.inherited[0].gained, 2.4);
  assert.equal(schultz!.inherited[0].gained, 1.8);
  assert.equal(mixon!.inherited[0].gained, 1.8);
  const paid = dell!.inherited[0].gained + schultz!.inherited[0].gained + mixon!.inherited[0].gained;
  assert.ok(Math.abs(paid - 6) < 1e-9, "the pool is conserved: everything the absent man had is paid out");

  // Efficiency is his own: Dell's yards-per-reception is unchanged.
  const before = 4, after = dell!.statMeans.receptions;
  assert.ok(Math.abs(dell!.statMeans.receiving_yards / after - 48 / before) < 1e-9,
    "the recipient keeps his own yards per reception");
  assert.ok(dell!.pointsAfter > dell!.pointsBefore, "and gains points for the extra volume");

  // The absent man himself is never a recipient.
  assert.equal(find(report, 1), undefined);
}

// ── The back who inherits from two pools is scaled once per pool ───────
{
  const rows = [
    player({ key: 1, name: "Starter RB", position: "RB", isOut: true,
      statMeans: { carries: 16, receptions: 4, rushing_yards: 70, receiving_yards: 30 } }),
    player({ key: 2, name: "Backup RB", position: "RB",
      statMeans: { carries: 4, receptions: 2, rushing_yards: 18, receiving_yards: 15, fumbles_lost_total: 0.1 },
      ourProj: 6.3 }),
    receiver(3, "WR1", 6),
  ];
  const report = redistributeOutOpportunity(rows);
  const backup = find(report, 2)!;
  const pools = backup.inherited.map(i => i.pool).sort();
  assert.deepEqual(pools, ["rush", "target"], "carries and receptions are separate inheritances");

  // All 16 carries are offered to the only other back, but 4 -> 20 is a 5x
  // scale-up and the cap holds him to 4x. The unpaid remainder is dropped,
  // NOT quietly handed elsewhere: a third-string back with no carry history
  // is not a better home for it than nowhere. `cappedFrom` is what makes
  // that visible.
  const rush = backup.inherited.find(i => i.pool === "rush")!;
  assert.equal(rush.cappedFrom, 5);
  assert.equal(rush.multiplier, MAX_MULTIPLIER);
  assert.equal(backup.statMeans.carries, 16, "capped at 4x his own 4, not the full 20");
  assert.equal(rush.gained, 12, "so 4 of the 16 offered carries go unpaid");

  assert.ok(backup.statMeans.receptions > 2 && backup.statMeans.receptions < 6);
  // Rushing yards scale with carries, receiving yards with receptions --
  // a single blended multiplier would get both wrong.
  assert.ok(Math.abs(backup.statMeans.rushing_yards - 18 * MAX_MULTIPLIER) < 1e-9);
  assert.ok(backup.statMeans.fumbles_lost_total > 0.1, "fumble risk grows with the workload");
}

// ── Only one quarterback plays ─────────────────────────────────────────
{
  const rows = [
    player({ key: 1, name: "Starter QB", position: "QB", isOut: true,
      statMeans: { attempts: 34, passing_yards: 250, passing_tds: 1.6, passing_interceptions: 0.8 } }),
    player({ key: 2, name: "Backup QB", position: "QB",
      statMeans: { attempts: 12, passing_yards: 70, passing_tds: 0.4, passing_interceptions: 0.5 }, ourProj: 5.1 }),
    player({ key: 3, name: "Third QB", position: "QB",
      statMeans: { attempts: 2, passing_yards: 10, passing_tds: 0.05, passing_interceptions: 0.1 }, ourProj: 1 }),
  ];
  const report = redistributeOutOpportunity(rows);
  assert.equal(report.applied.length, 1, "the pass pool is winner-take-all, never split");
  const backup = find(report, 2)!;
  assert.equal(backup.inherited[0].pool, "pass");

  // Promotion scales 12 attempts to the starter workload of 34.
  assert.ok(backup.inherited[0].cappedFrom === null);
  assert.ok(Math.abs(backup.statMeans.attempts - 34) < 1e-9);
  assert.ok(backup.statMeans.passing_interceptions > 0.5,
    "interceptions scale with attempts too -- a transfer cannot move only the upside");
}

// ── The cap fires, and says so ─────────────────────────────────────────
{
  const rows = [
    player({ key: 1, name: "Starter QB", position: "QB", isOut: true,
      statMeans: { attempts: 40, passing_yards: 280 } }),
    player({ key: 2, name: "Deep backup", position: "QB",
      statMeans: { attempts: 2, passing_yards: 12 }, ourProj: 0.8 }),
  ];
  const backup = find(redistributeOutOpportunity(rows), 2)!;
  assert.equal(backup.inherited[0].multiplier, MAX_MULTIPLIER,
    "a 21x scale-up of two mop-up attempts is noise, not a projection");
  assert.equal(backup.inherited[0].cappedFrom, 20);
  assert.ok(inheritanceNote(backup.inherited).includes("capped"), "and the cap is visible, not hidden");
}

// ── Nobody eligible: reported, never silently dropped ──────────────────
{
  const rows = [
    player({ key: 1, name: "Lone QB", position: "QB", isOut: true, statMeans: { attempts: 30, passing_yards: 210 } }),
    receiver(2, "WR1", 5),
  ];
  const report = redistributeOutOpportunity(rows);
  assert.equal(report.applied.length, 0);
  assert.equal(report.unresolved.length, 1);
  assert.equal(report.unresolved[0].pool, "pass");
  assert.equal(report.unresolved[0].pooled, 30);
  assert.deepEqual(report.unresolved[0].from, ["Lone QB"]);
}

// ── A teammate with no history in the pool is paid nothing ─────────────
{
  const rows = [
    receiver(1, "Starter", 6, { isOut: true }),
    receiver(2, "Real WR2", 4),
    player({ key: 3, name: "Never targeted", position: "WR", statMeans: { receptions: 0 }, ourProj: 0.2 }),
  ];
  const report = redistributeOutOpportunity(rows);
  assert.equal(find(report, 3), undefined,
    "scaling a man who has never caught a pass would invent a number backed by nothing");
  assert.equal(find(report, 2)!.inherited[0].gained, 6, "so the whole pool goes to the one real recipient");
}

// ── Teams do not leak into each other ──────────────────────────────────
{
  const rows = [
    receiver(1, "HOU out", 6, { isOut: true }),
    receiver(2, "HOU in", 4),
    receiver(3, "DAL in", 5, { team: "DAL" }),
  ];
  const report = redistributeOutOpportunity(rows);
  assert.equal(find(report, 3), undefined, "an opposing receiver never inherits");
  assert.equal(find(report, 2)!.inherited[0].gained, 6);
}

// ── Python having already moved it does not double-pay ─────────────────
{
  // What the upstream pipeline leaves behind: status 'out', stats zeroed.
  const rows = [
    receiver(1, "Already handled", 0, { isOut: true, projectionStatus: "out", statMeans: { receptions: 0 } }),
    receiver(2, "WR2", 4),
  ];
  const report = redistributeOutOpportunity(rows);
  assert.equal(report.applied.length, 0, "an empty pool pays nobody, so the two layers cannot stack");
  assert.equal(report.donorsWithoutOpportunity.length, 1);
  assert.match(report.donorsWithoutOpportunity[0].reason, /already ruled him out/);
}

// ── The mean line is never re-scored through the bonus thresholds ──────
{
  // 96 receiving yards: no bonus. 104: the full +3, for an outcome that is
  // only about half likely. The linear scorer must ignore both.
  const under = { recYds: 96 }, over = { recYds: 104 };
  assert.ok(Math.abs(scoreNflOffense(under) - 9.6) < 1e-9);
  assert.ok(Math.abs(scoreNflOffense(over) - (10.4 + 3)) < 1e-9, "the realized scorer does apply the bonus");
  assert.ok(Math.abs(scoreNflOffenseLinear(over) - 10.4) < 1e-9, "the mean scorer does not");

  // So a recipient crossing 100 mean yards gains only the linear value.
  const rows = [
    receiver(1, "Out", 4, { isOut: true }),
    player({ key: 2, name: "Crosses 100", position: "WR",
      statMeans: { receptions: 8, receiving_yards: 96 }, ourProj: 17.6 }),
  ];
  const gained = find(redistributeOutOpportunity(rows), 2)!;
  assert.ok(gained.statMeans.receiving_yards > 100, "his mean line does cross the threshold");
  const linear = (gained.statMeans.receptions - 8) + (gained.statMeans.receiving_yards - 96) * 0.1;
  assert.ok(Math.abs((gained.pointsAfter - gained.pointsBefore) - linear) < 1e-6,
    "but he is paid the marginal linear points only -- no phantom +3 bonus");
}

// ── Pool definitions are the stated contract ───────────────────────────
{
  assert.deepEqual([...POOLS.target.recipients].sort(), ["RB", "TE", "WR"],
    "a receiver's targets reach the whole pass-catching group, not just the WR room");
  assert.deepEqual([...POOLS.pass.recipients], ["QB"]);
  assert.equal(POOLS.target.unit, "receptions",
    "targets are never persisted by either projection path; receptions is the available unit");
  assert.ok(POOLS.pass.single && !POOLS.rush.single && !POOLS.target.single);
}

// ── A player with no career cannot vacate work he never had ───────────
{
  // The live measurement: a never-played receiver carries MORE projected
  // receptions (2.21/gm) than a real one (2.02), because his line is drawn
  // entirely from peers. Ruled out, he used to donate that phantom workload.
  const rows = [
    receiver(1, "Never played", 6, { isOut: true, historyGames: 0, projectionStatus: "position_prior" }),
    receiver(2, "Real WR2", 4),
  ];
  const report = redistributeOutOpportunity(rows);
  assert.equal(report.applied.length, 0, "nobody is paid out of a workload that was never real");
  assert.equal(report.donorsWithoutOpportunity.length, 1);
  assert.match(report.donorsWithoutOpportunity[0].reason, /fewer than 2 games of his own/);
  assert.match(report.donorsWithoutOpportunity[0].reason, /average WR rather than him/);
}

// ── ...and cannot earn a share of someone else's either ───────────────
{
  // The same inversion on the receiving end: the phantom would have
  // out-earned a genuine rotational receiver, since his peer-drawn line is
  // larger. The whole pool goes to the man with a real record.
  const rows = [
    receiver(1, "Starter", 6, { isOut: true }),
    receiver(2, "Real WR2", 4),
    receiver(3, "Never played", 5, { historyGames: 0, projectionStatus: "position_prior" }),
  ];
  const report = redistributeOutOpportunity(rows);
  assert.equal(find(report, 3), undefined, "a peer-drawn line is not evidence he will get the work");
  assert.equal(find(report, 2)!.inherited[0].gained, 6, "so the real receiver takes all of it");
}

// ── The honest refusal: no qualifying teammate means nobody is paid ────
{
  // Deliberate. Inventing a recipient is how the 38% got in; saying "we do
  // not know who picks this up" is the weaker claim and the true one.
  const rows = [
    receiver(1, "Starter", 6, { isOut: true }),
    receiver(2, "Camp body", 5, { historyGames: 1, projectionStatus: "position_prior" }),
  ];
  const report = redistributeOutOpportunity(rows);
  assert.equal(report.applied.length, 0);
  assert.equal(report.unresolved.length, 1, "the work is reported as unplaced, never quietly dropped");
  assert.equal(report.unresolved[0].pooled, 6);
  assert.match(report.unresolved[0].reason, /2\+ games/);
}

// ── The threshold is the model's own, and the boundary is inclusive ────
{
  assert.equal(MIN_OBSERVED_GAMES, 2, "matches minimum_historical_games in nfl_dfs_historical.py");
  assert.ok(!hasObservedOpportunity({ historyGames: 1 }));
  assert.ok(hasObservedOpportunity({ historyGames: 2 }), "2 games qualifies, same as the model");
  assert.ok(!hasObservedOpportunity({}), "unknown history is treated as none, not as a pass");
  assert.ok(!hasObservedOpportunity({ historyGames: null }));
}

// ── A donor the pipeline already placed is not paid a second time ─────
{
  // Python clears the line only when it actually handed the work to someone.
  // A cleared line therefore means "handled", and the reason says so rather
  // than blaming his history.
  const rows = [
    receiver(1, "Handled upstream", 0, { isOut: true, projectionStatus: "out", statMeans: { receptions: 0 } }),
    receiver(2, "WR2", 4),
  ];
  const report = redistributeOutOpportunity(rows);
  assert.equal(report.applied.length, 0, "the two layers cannot stack");
  assert.match(report.donorsWithoutOpportunity[0].reason, /already ruled him out and placed his opportunity/);
}

// ── A ruled-out player the pipeline did NOT place still hands work on ──
{
  // The 79-of-86 case: zeroed by Python, never offered a transfer because he
  // is not a quarterback, stat line preserved. This is the whole point of the
  // upstream change -- his work must still reach his teammates.
  const rows = [
    receiver(1, "Zeroed, unplaced", 6, { isOut: true, projectionStatus: "out" }),
    receiver(2, "WR2", 4),
  ];
  const report = redistributeOutOpportunity(rows);
  assert.equal(find(report, 2)!.inherited[0].gained, 6,
    "an out status alone must not strand the workload");
}

console.log("nfl opportunity redistribution: all assertions passed");

// Caps remain visible at the pool level, not only on a recipient's note.
{
  const report = redistributeOutOpportunity([
    player({key:100,name:'Out RB',position:'RB',isOut:true,statMeans:{carries:16,rushing_yards:64}}),
    player({key:101,name:'Backup',position:'RB',statMeans:{carries:4,rushing_yards:16}}),
  ]);
  assert.deepEqual(report.pools, [{team:'HOU',pool:'rush',offered:16,assigned:12,unassigned:4}]);
  assert.equal(report.unresolved[0].pooled,4);
}
{
  const starter = player({key:100,name:'Healthy QB1',position:'QB',depthOrder:1,statMeans:{attempts:30,passing_yards:250}});
  const absent = player({key:101,name:'Backup on IR',position:'QB',isOut:true,depthOrder:2,statMeans:{attempts:20,passing_yards:150}});
  assert.equal(redistributeOutOpportunity([starter,absent]).applied.length,0);
  absent.depthOrder=1; absent.canDonate=false;
  assert.equal(redistributeOutOpportunity([starter,absent]).applied.length,0,'pipeline-rejected transfers cannot be retried');
}
