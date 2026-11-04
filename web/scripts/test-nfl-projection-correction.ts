/**
 * Projection corrections (`nfl-projection-correction-v1`) and the pool rule
 * that shipped with them: the three rules fire exactly where the evidence
 * says, never elsewhere, every changed row keeps `before`, the optimizer
 * leaves a position-prior row out unless it is locked, and a Classic GPP
 * build defaults to the winners shape.
 */
import assert from "node:assert/strict";
import { correctProjections, expectedStarter, EXPECTED_STARTER_ROLE, PROJECTION_CORRECTION_VERSION, SHOWDOWN_DST_SCALE,
  UNANCHORED_SKILL_SHRINK, type CorrectionPlayer } from "../src/lib/nfl-dfs/projection-correction";
import { BOOM_THRESHOLDS } from "../src/lib/nfl-dfs/replacement-upside";
import { defaultBuildShape } from "../src/lib/nfl-dfs/winners-shape";
import { formFromDraft } from "../src/lib/nfl-dfs/generation-settings";
import { optimizeNflLineups, type NflOptimizerPlayer, type NflOptimizerSettings } from "../src/app/dfs/nfl/nfl-optimizer";

const near = (a: number | null | undefined, b: number, tol = 1e-9, msg = "") => assert.ok(a != null && Math.abs(a - b) <= tol, `${msg} ${a} vs ${b}`);

let nextId = 1;
type P = CorrectionPlayer & { extra?: string };
const player = (over: Partial<P> & { position: string; team: string }): P => ({
  dkPlayerId: nextId++, name: `${over.position}${nextId}`, isOut: false, projectionStatus: "historical", historyGames: 8,
  ourProj: 10, floorFpts: 5, medianFpts: 9, ceilingFpts: 20, boomRate: 0.2, availability: { role: "Listed WR1", blockedReason: null }, ...over,
});

// --- Rule 1: unanchored Classic RB/WR/TE ---------------------------------------
{
  const pool = [
    player({ position: "WR", team: "A", name: "plain" }),
    player({ position: "WR", team: "A", name: "anchored", marketAnchor: { mode: "full" } }),
    player({ position: "WR", team: "A", name: "tdonly", marketAnchor: { mode: "td_only" } }),
    player({ position: "WR", team: "A", name: "committed", absenceCommit: { rule: "own_role_games" } }),
    player({ position: "WR", team: "A", name: "prior", projectionStatus: "position_prior", historyGames: 0 }),
    player({ position: "RB", team: "A", name: "small", ourProj: 2.9, floorFpts: 1, medianFpts: 2.5, ceilingFpts: 7 }),
    player({ position: "TE", team: "A", name: "exactly3", ourProj: 3, floorFpts: 1, medianFpts: 2.8, ceilingFpts: 8 }),
    player({ position: "WR", team: "A", name: "out", isOut: true }),
    player({ position: "QB", team: "A", name: "qb" }),
    player({ position: "DST", team: "A", name: "dst" }),
    player({ position: "WR", team: "A", name: "nomedian", medianFpts: null }),
  ];
  const { players, report } = correctProjections(pool, { format: "classic" });
  const by = Object.fromEntries(players.map((p) => [p.name, p]));
  near(by.plain.ourProj, 7, 1e-9, "plain mean");
  near(by.plain.floorFpts, 3.5, 1e-9, "plain floor");
  near(by.plain.medianFpts, 6.3, 1e-9, "plain median");
  near(by.plain.ceilingFpts, 14, 1e-9, "plain ceiling");
  assert.equal(by.plain.projectionCorrection?.rule, "unanchored_skill_shrink");
  assert.equal(by.plain.projectionCorrection?.version, PROJECTION_CORRECTION_VERSION);
  near(by.plain.projectionCorrection?.before.mean, 10, 1e-9, "before kept");
  assert.equal(by.plain.projectionCorrection?.factor, UNANCHORED_SKILL_SHRINK);
  assert.ok(by.plain.boomRate != null && by.plain.boomRate < 0.2, "boom is re-read off the scaled curve, lower than before");
  assert.ok(by.plain.boomRate! >= 0 && by.plain.boomRate! <= 1);
  for (const name of ["anchored", "committed", "prior", "small", "out", "qb", "dst"]) {
    assert.equal(by[name].ourProj, pool.find((p) => p.name === name)!.ourProj, `${name} untouched`);
    assert.equal(by[name].projectionCorrection, undefined, `${name} carries no correction`);
  }
  near(by.exactly3.ourProj, 2.1, 1e-9, "the floor of the fitted population is inclusive");
  near(by.tdonly.ourProj, 7, 1e-9, "a TD-only anchor does not exempt a player: his volume is still the model's");
  assert.match(by.tdonly.projectionCorrection?.note ?? "", /Only a touchdown line/);
  assert.equal(by.tdonly.projectionCorrection?.anchorMode, "td_only");
  assert.equal(by.plain.projectionCorrection?.anchorMode, "none");
  near(by.nomedian.ourProj, 7, 1e-9, "a row without a median is still scaled");
  assert.equal(by.nomedian.medianFpts, null);
  assert.equal(by.nomedian.boomRate, null, "without the quantiles the boom rate is dropped, not left unscaled");
  assert.equal(report.shrunk, 4);
  assert.equal(report.skillConsidered, 8, "every active RB/WR/TE is counted (6 WRs, 1 RB, 1 TE); the out one is not");
  assert.equal(report.dstScaled, 0);
  assert.equal(report.qbZeroed.length, 0, "one quarterback on the team: nothing to decide");
  // Pure: the input rows are untouched.
  assert.equal(pool[0].ourProj, 10);
  assert.ok(!("projectionCorrection" in pool[0]));
}

// Showdown: the shrink does not apply (its held-out week got worse).
{
  const { players, report } = correctProjections([player({ position: "WR", team: "A" }), player({ position: "RB", team: "B" })], { format: "showdown" });
  assert.ok(players.every((p) => p.ourProj === 10 && p.projectionCorrection === undefined));
  assert.equal(report.shrunk, 0);
  assert.equal(report.skillConsidered, 0);
}

// --- Rule 2: Classic backup quarterbacks ---------------------------------------
{
  const qb = (over: Partial<P>) => player({ position: "QB", team: "A", availability: { role: "QB role unresolved", blockedReason: null }, ...over });
  // Top-projected fallback: no depth chart, nobody confirmed.
  const a = [qb({ name: "starter", ourProj: 18 }), qb({ name: "backup", ourProj: 9, floorFpts: 3, ceilingFpts: 20 }), qb({ name: "third", ourProj: 0.5 }),
    player({ position: "WR", team: "A", name: "wr", marketAnchor: {} })];
  const ra = correctProjections(a, { format: "classic" });
  const ba = Object.fromEntries(ra.players.map((p) => [p.name, p]));
  assert.equal(ba.starter.ourProj, 18);
  assert.equal(ba.starter.isOut, false);
  assert.equal(ba.backup.ourProj, 0);
  assert.equal(ba.backup.floorFpts, 0);
  assert.equal(ba.backup.ceilingFpts, 0);
  assert.equal(ba.backup.boomRate, 0);
  assert.equal(ba.backup.isOut, true, "a zeroed backup is blocked the way a depth-chart backup is");
  assert.match(ba.backup.availability?.blockedReason ?? "", /expected starter \(starter, the top projected quarterback\)/);
  assert.equal(ba.backup.projectionCorrection?.rule, "backup_qb_zero");
  assert.equal(ba.backup.projectionCorrection?.factor, 0);
  near(ba.backup.projectionCorrection?.before.mean, 9, 1e-9, "the model's own number is kept");
  assert.deepEqual(ba.backup.projectionCorrection?.starter, { name: "starter", how: "top_projected" });
  assert.equal(ba.third.ourProj, 0, "the third quarterback is zeroed too");
  assert.equal(ba.third.projectionCorrection?.rule, "backup_qb_zero");
  assert.equal(ba.wr.ourProj, 10, "the anchored receiver is untouched");
  assert.deepEqual(ra.report.starters, [{ team: "A", name: "starter", how: "top_projected" }]);
  assert.equal(ra.report.qbZeroed.length, 2);
  assert.deepEqual(ra.report.qbZeroed.map((q) => q.name), ["backup", "third"]);

  // Depth chart beats projection: the listed QB1 keeps the job even when projected lower.
  const b = [qb({ name: "listed", ourProj: 12, availability: { role: EXPECTED_STARTER_ROLE, blockedReason: null } }), qb({ name: "hot", ourProj: 15 })];
  const rb = Object.fromEntries(correctProjections(b, { format: "classic" }).players.map((p) => [p.name, p]));
  assert.equal(rb.listed.ourProj, 12);
  assert.equal(rb.hot.ourProj, 0);
  assert.deepEqual(rb.hot.projectionCorrection?.starter, { name: "listed", how: "depth_chart" });

  // A listed QB1 who is blocked does not count as the chart's answer.
  const c = [qb({ name: "listedBlocked", ourProj: 12, availability: { role: EXPECTED_STARTER_ROLE, blockedReason: "Unavailable: IR" } }), qb({ name: "hot", ourProj: 15 })];
  const rc = Object.fromEntries(correctProjections(c, { format: "classic" }).players.map((p) => [p.name, p]));
  assert.equal(rc.hot.ourProj, 15, "with the listed starter blocked, the top projection decides");
  assert.equal(rc.listedBlocked.ourProj, 0);

  // Two listed QB1s is conflicting evidence: the top projection decides.
  const d = [qb({ name: "l1", ourProj: 12, availability: { role: EXPECTED_STARTER_ROLE, blockedReason: null } }),
    qb({ name: "l2", ourProj: 13, availability: { role: EXPECTED_STARTER_ROLE, blockedReason: null } })];
  const rd = Object.fromEntries(correctProjections(d, { format: "classic" }).players.map((p) => [p.name, p]));
  assert.equal(rd.l2.ourProj, 13);
  assert.equal(rd.l1.ourProj, 0);
  assert.equal(rd.l1.projectionCorrection?.starter?.how, "top_projected");

  // The user's confirmation outranks both.
  const e = [qb({ name: "listed", dkPlayerId: 501, ourProj: 18, availability: { role: EXPECTED_STARTER_ROLE, blockedReason: null } }),
    qb({ name: "confirmed", dkPlayerId: 502, ourProj: 7 })];
  const re = Object.fromEntries(correctProjections(e, { format: "classic", confirmedStartingQbs: { a: 502 } }).players.map((p) => [p.name, p]));
  assert.equal(re.confirmed.ourProj, 7, "team keys match case-insensitively");
  assert.equal(re.listed.ourProj, 0);
  assert.deepEqual(re.listed.projectionCorrection?.starter, { name: "confirmed", how: "confirmed" });
  assert.match(re.listed.availability?.blockedReason ?? "", /confirmed by you/);
  // ...and so does the availability layer's own confirmed flag.
  const f = [qb({ name: "listed", ourProj: 18, availability: { role: EXPECTED_STARTER_ROLE, blockedReason: null } }),
    qb({ name: "flagged", ourProj: 7, availability: { role: "QB role unresolved", blockedReason: null, confirmedStarter: true } })];
  const rf = Object.fromEntries(correctProjections(f, { format: "classic" }).players.map((p) => [p.name, p]));
  assert.equal(rf.flagged.ourProj, 7);
  assert.equal(rf.listed.ourProj, 0);

  // One active quarterback (the other already out or zeroed): nothing to decide, nothing changed.
  for (const other of [qb({ name: "gone", ourProj: 9, isOut: true }), qb({ name: "gone", ourProj: 9, projectionStatus: "out" }), qb({ name: "gone", ourProj: 0 })]) {
    const g = correctProjections([qb({ name: "only", ourProj: 18 }), other], { format: "classic" });
    assert.equal(g.report.starters.length, 0);
    assert.equal(g.report.qbZeroed.length, 0);
    assert.ok(g.players.every((p) => p.projectionCorrection === undefined));
  }
  assert.equal(expectedStarter([qb({ ourProj: 18 })]), null);

  // Teams are independent, and Showdown is untouched.
  const h = [qb({ name: "a1", team: "A", ourProj: 18 }), qb({ name: "a2", team: "A", ourProj: 9 }), qb({ name: "b1", team: "B", ourProj: 16 }), qb({ name: "b2", team: "B", ourProj: 8 })];
  const rh = correctProjections(h, { format: "classic" });
  assert.deepEqual(rh.report.qbZeroed.map((q) => `${q.team}:${q.name}`), ["A:a2", "B:b2"]);
  const rs = correctProjections(h, { format: "showdown" });
  assert.equal(rs.report.qbZeroed.length, 0);
  assert.ok(rs.players.every((p) => p.ourProj! > 0 && !p.isOut));
}

// --- Rule 3: Showdown DST --------------------------------------------------------
{
  const dst = player({ position: "DST", team: "A", name: "dst", ourProj: 8, floorFpts: 2, medianFpts: 7, ceilingFpts: 16, boomRate: 0.3,
    specialTeams: { mean: 9, p10: 3, p50: 8, p90: 18, boom: 0.35 } });
  const { players, report } = correctProjections([dst, player({ position: "DST", team: "B", name: "outDst", isOut: true })], { format: "showdown" });
  const d = players[0];
  near(d.ourProj, 8 * SHOWDOWN_DST_SCALE, 1e-9, "dst mean");
  near(d.floorFpts, 2 * SHOWDOWN_DST_SCALE, 1e-9, "dst floor");
  near(d.ceilingFpts, 16 * SHOWDOWN_DST_SCALE, 1e-9, "dst ceiling");
  assert.equal(d.projectionCorrection?.rule, "showdown_dst_scale");
  near(d.projectionCorrection?.before.mean, 8, 1e-9, "before kept");
  near(d.specialTeams?.mean, 9 * SHOWDOWN_DST_SCALE, 1e-9, "the special-teams block the optimizer substitutes is scaled too");
  near(d.specialTeams?.p90, 18 * SHOWDOWN_DST_SCALE, 1e-9, "special-teams ceiling");
  assert.ok(d.specialTeams!.boom < 0.35, "special-teams boom re-read lower");
  assert.ok(BOOM_THRESHOLDS.DST > 0);
  assert.equal(players[1].projectionCorrection, undefined, "an out DST is untouched");
  assert.equal(report.dstScaled, 1);
  // Classic DSTs are unbiased and untouched.
  const classic = correctProjections([dst], { format: "classic" });
  assert.equal(classic.players[0].ourProj, 8);
  assert.equal(classic.report.dstScaled, 0);
}

// --- Rule 4: position-prior rows are out of the optimizer pool unless locked ----
{
  const op = (id: number, salary: number, proj: number, games: number, team: string, position: NflOptimizerPlayer["position"] = id % 2 ? "WR" : "RB"): NflOptimizerPlayer => ({
    id, dkPlayerId: id, captainDkPlayerId: id + 1000, captainSalary: Math.round(salary * 1.5),
    name: `P${id}`, position, team, opponent: ({ AAA: "BBB", BBB: "AAA", CCC: "DDD", DDD: "CCC" } as Record<string, string>)[team],
    gameKey: team === "AAA" || team === "BBB" ? "AAA@BBB" : "CCC@DDD", salary, isOut: false, projectionStatus: games ? "historical" : "position_prior",
    historyGames: games, teamSeasonGames: 8, ourProj: proj, floorFpts: proj * 0.7, ceilingFpts: proj * 1.6, boomRate: 0.2,
    avgFptsDk: proj, fantasyprosProj: null, linestarProj: null, linestarOwnPct: null, customProj: null,
  });
  const pool = [
    op(10, 7000, 18, 34, "AAA"), op(11, 6000, 14, 34, "AAA"), op(12, 5000, 12, 20, "AAA"),
    op(20, 7000, 18, 34, "BBB"), op(21, 6000, 14, 34, "BBB"), op(22, 5000, 12, 20, "BBB"),
    op(90, 6000, 15.8, 0, "AAA"), op(91, 3600, 15.5, 0, "BBB"),
  ];
  const base: NflOptimizerSettings = {
    format: "showdown", mode: "gpp", projectionSource: "our", allowDkFallback: false, nLineups: 3,
    minSalary: 0, minPlayerSalary: 0, requireObservedHistory: false, maxExposure: 1, minUnique: 1,
    stackPassCatchers: 0, bringBack: false, randomness: 0, lockedPlayerIds: [], excludedPlayerIds: [],
    minExposureByPlayer: {}, maxExposureByPlayer: {},
  };
  // The observed-history gate is OFF here: the prior rule is unconditional.
  const r = optimizeNflLineups(pool, base);
  assert.ok(r.lineups.length > 0);
  for (const l of r.lineups) for (const id of l.playerIds) assert.ok(id !== 90 && id !== 91, `position-prior player ${id} reached a lineup`);
  for (const id of [90, 91]) {
    const e = r.eligibility?.find((x) => x.dkPlayerId === id);
    assert.equal(e?.eligible, false);
    assert.equal(e?.reasonCode, "POSITION_PRIOR");
    assert.match(e?.reason ?? "", /position's average/);
  }
  assert.ok(r.warnings.some((w) => /2 player\(s\) projected from a position average/.test(w)), "the removal is reported");
  // Classic too.
  // Classic needs players from two games, so the second game's teams fill the other slots.
  const classicPool = [...pool, op(30, 6000, 20, 20, "CCC", "QB"), op(31, 6000, 19, 20, "DDD", "QB"), op(32, 4500, 10, 20, "CCC", "TE"),
    op(33, 4500, 9, 20, "DDD", "TE"), op(34, 3000, 6, 20, "CCC", "DST"), op(35, 3000, 6, 20, "DDD", "DST"), op(36, 5000, 11, 20, "CCC"), op(37, 5000, 11, 20, "DDD"),
    op(38, 5000, 11, 20, "CCC"), op(39, 5000, 11, 20, "DDD")];
  const rc = optimizeNflLineups(classicPool, { ...base, format: "classic", nLineups: 1 });
  assert.ok(rc.lineups.length > 0, "the classic pool solves without the prior rows");
  assert.ok(rc.lineups.every((l) => !l.playerIds.includes(90) && !l.playerIds.includes(91)));
  // A lock keeps him: the user's own instruction.
  const locked = optimizeNflLineups(pool, { ...base, lockedPlayerIds: [90] });
  assert.ok(locked.lineups.every((l) => l.playerIds.includes(90)));
  assert.equal(locked.eligibility?.find((x) => x.dkPlayerId === 90)?.eligible, true);
}

// --- Default build shape --------------------------------------------------------
assert.equal(defaultBuildShape("classic", "gpp"), "winners");
assert.equal(defaultBuildShape("classic", "cash"), "standard");
assert.equal(defaultBuildShape("showdown", "gpp"), "standard");
assert.equal(defaultBuildShape("showdown", "cash"), "standard");
{
  const defaults = { buildShape: "winners", nLineups: 20 };
  // An older draft's "standard" was never a choice: it takes the page default.
  assert.equal(formFromDraft({ schemaVersion: 3, settings: { buildShape: "standard", nLineups: 5 } }, defaults)?.settings.buildShape, "winners");
  assert.equal(formFromDraft({ settings: { buildShape: "standard" } }, defaults)?.settings.buildShape, "winners");
  // A v4 draft carries a deliberate choice and keeps it.
  assert.equal(formFromDraft({ schemaVersion: 4, settings: { buildShape: "standard", nLineups: 5 } }, defaults)?.settings.buildShape, "standard");
  assert.equal(formFromDraft({ schemaVersion: 3, settings: { nLineups: 5 } }, defaults)?.settings.nLineups, 5, "other fields still restore");
}

console.log("Projection corrections: shrink, backup QB, Showdown DST, position-prior pool rule and the default shape all passed.");
