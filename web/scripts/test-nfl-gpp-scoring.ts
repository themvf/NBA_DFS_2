/**
 * Lineup scoring choices on the build form (`nfl-gpp-scoring-default-v1`):
 * plain projection is `ceiling` scoring with its two knobs turned down, Classic
 * GPP starts on it, a plain Classic build caps each defense, and saved runs and
 * drafts written before the form held the knobs restore as what they ran.
 */
import assert from "node:assert/strict";
import { DEFAULT_BOOM_WEIGHT as OPTIMIZER_BOOM_WEIGHT, DEFAULT_MAX_CEILING_MULTIPLE as OPTIMIZER_CEILING_MULTIPLE, optimizeNflLineups,
  type NflOptimizerPlayer, type NflOptimizerSettings } from "../src/app/dfs/nfl/nfl-optimizer";
import { DEFAULT_BOOM_WEIGHT, DEFAULT_MAX_CEILING_MULTIPLE, PLAIN_DST_MAX_EXPOSURE, PLAIN_RANDOMNESS, STANDARD_RANDOMNESS, defaultGppScoringKind,
  scoringKindOf, scoringPreset, unlinedStarters, withPlainDstCap, withoutScoringKnobsForDiversified } from "../src/lib/nfl-dfs/gpp-scoring";
import { formFromDraft, formFromSettings, sameGenerationSettings, settingsFromForm, type NflBuildForm } from "../src/lib/nfl-dfs/generation-settings";

// --- The restated optimizer defaults cannot drift ---
assert.equal(DEFAULT_MAX_CEILING_MULTIPLE, OPTIMIZER_CEILING_MULTIPLE, "ceiling multiple restated for the client = the optimizer's");
assert.equal(DEFAULT_BOOM_WEIGHT, OPTIMIZER_BOOM_WEIGHT, "boom weight restated for the client = the optimizer's");

// --- Three choices, each a fixed set of settings, each read back as itself ---
assert.deepEqual(scoringPreset("mean"), { gppScoring: "ceiling", maxCeilingMultiple: 1, boomWeight: 0, randomness: PLAIN_RANDOMNESS });
assert.deepEqual(scoringPreset("draws"), { gppScoring: "draws", maxCeilingMultiple: 2.5, boomWeight: 2, randomness: STANDARD_RANDOMNESS });
assert.deepEqual(scoringPreset("ceiling"), { gppScoring: "ceiling", maxCeilingMultiple: 2.5, boomWeight: 2, randomness: STANDARD_RANDOMNESS });
for (const kind of ["mean", "draws", "ceiling"] as const) assert.equal(scoringKindOf(scoringPreset(kind)), kind);
assert.equal(scoringKindOf({ gppScoring: "ceiling" }), "ceiling", "absent knobs are the optimizer's defaults, not plain projection");
assert.equal(scoringKindOf({}), "ceiling", "a run saved before simulated games ranked by ceilings");
assert.equal(scoringKindOf({ gppScoring: "draws", maxCeilingMultiple: 1, boomWeight: 0 }), "draws", "draws ignores the ceiling knobs");
assert.equal(scoringKindOf({ gppScoring: "ceiling", maxCeilingMultiple: 1, boomWeight: 2 }), "ceiling", "a boom term is not plain projection");

// --- Which way a new build starts ---
assert.equal(defaultGppScoringKind("classic", "gpp", "winners"), "mean");
assert.equal(defaultGppScoringKind("classic", "gpp", "standard"), "mean");
assert.equal(defaultGppScoringKind("classic", "gpp", "diversified"), "draws", "Diversified keeps the optimizer's own scoring");
assert.equal(defaultGppScoringKind("showdown", "gpp", "winners"), "draws", "Showdown was not tested and does not change");
assert.equal(defaultGppScoringKind("classic", "cash", "standard"), "draws", "cash ignores GPP scoring; its randomness stays 0.08");

function base(over: Partial<NflOptimizerSettings> = {}): NflOptimizerSettings {
  return { format: "classic", mode: "gpp", projectionSource: "our", allowDkFallback: true, nLineups: 10, minSalary: 45000, maxExposure: 0.6,
    minUnique: 1, stackPassCatchers: 0, bringBack: false, randomness: 0, lockedPlayerIds: [], excludedPlayerIds: [],
    minExposureByPlayer: {}, maxExposureByPlayer: {}, ...over };
}
const plain = (over: Partial<NflOptimizerSettings> = {}) => base({ ...scoringPreset("mean"), ...over });

// --- The defense cap: plain-projection Classic GPP only, never over the user ---
{
  const capped = withPlainDstCap(plain(), [52, 50, 51, 50]);
  assert.deepEqual(capped.exposurePolicies?.map((p) => p.playerId), [50, 51, 52], "one policy per defense, in a fixed order");
  for (const policy of capped.exposurePolicies ?? []) {
    assert.equal(policy.overall.maxPct, PLAIN_DST_MAX_EXPOSURE);
    assert.equal(policy.overall.minPct, null);
    assert.equal(policy.overallFromUser, false, "not the user's range: it must not come back into the form as one");
    assert.deepEqual([policy.captain, policy.flex], [{ minPct: null, maxPct: null }, { minPct: null, maxPct: null }]);
  }
  assert.deepEqual(withPlainDstCap(capped, [50, 51, 52]), capped, "applying it twice adds nothing");
  const mine = withPlainDstCap(plain({ lockedPlayerIds: [50], maxExposureByPlayer: { "51": 0.8 }, minExposureByPlayer: { "52": 0.4 } }), [50, 51, 52, 53]);
  assert.deepEqual(mine.exposurePolicies?.map((p) => p.playerId), [53], "a lock or a typed range on a defense wins");
  for (const off of [base(scoringPreset("draws")), base(scoringPreset("ceiling")), plain({ format: "showdown" }), plain({ mode: "cash" }),
    plain({ buildShape: "diversified" }), plain({ nLineups: 3 })]) {
    assert.equal(withPlainDstCap(off, [50, 51]).exposurePolicies, undefined, `no cap for ${off.format}/${off.mode}/${off.gppScoring}/${off.buildShape ?? "standard"}/${off.nLineups}`);
  }
  assert.equal(withPlainDstCap(plain({ nLineups: 4 }), [50]).exposurePolicies?.length, 1, "four lineups: 30% is one lineup, so the cap applies");
}

// --- Diversified never receives the form's knobs ---
{
  const stripped = withoutScoringKnobsForDiversified(plain({ buildShape: "diversified" }));
  assert.equal(stripped.maxCeilingMultiple, undefined); assert.equal(stripped.boomWeight, undefined);
  const kept = plain({ buildShape: "winners" });
  assert.equal(withoutScoringKnobsForDiversified(kept), kept, "other shapes are untouched");
}

// --- Starters without lines: named, biggest first, with what the build will use ---
{
  const shrunk = (name: string, before: number, over: object = {}) => ({ name, position: "RB", team: "NYJ", ourProj: before * 0.7, ownPct: 4,
    projectionCorrection: { rule: "unanchored_skill_shrink", before: { mean: before } }, ...over });
  const list = unlinedStarters([
    shrunk("Backup", 6), shrunk("Starter", 14.4), shrunk("Bigger", 16), shrunk("Hurt", 15, { isOut: true }), shrunk("Ruled out", 15, { projectionStatus: "out" }),
    { name: "Lined", position: "WR", team: "NYJ", ourProj: 18, projectionCorrection: null },
    { name: "Zeroed QB", position: "QB", team: "NYJ", ourProj: 0, projectionCorrection: { rule: "backup_qb_zero", before: { mean: 15 } } },
  ]);
  assert.deepEqual(list.map((p) => p.name), ["Bigger", "Starter"], "only active starters scaled for want of lines, biggest model number first");
  assert.ok(Math.abs(list[1].usedPoints - 10.08) < 1e-9 && list[1].modelPoints === 14.4 && list[1].ownPct === 4);
}

// --- Saved runs and drafts from before the form held the knobs ---
{
  const defaults = { mode: "gpp" as "cash" | "gpp", projectionSource: "our" as const, allowDkFallback: false, nLineups: 20, minSalary: 45000,
    maxExposure: 0.6, minUnique: 2, stackPassCatchers: 1 as 0 | 1 | 2, bringBack: true, buildShape: "winners" as const,
    ...scoringPreset("mean") };   // the form is on plain projection when the restore happens
  type Form = NflBuildForm<typeof defaults>;
  const empty = { locked: [], excluded: [], targets: {}, captainTargets: {}, ownership: {}, planMode: "standard" as const, quotas: [], favorite: "", fades: [] };
  const teams = ["AAA", "BBB"];

  // A run built on simulated games or ceilings before today never sent the knobs.
  const { maxCeilingMultiple: _m, boomWeight: _b, ...withoutKnobs } = settingsFromForm({ ...empty, settings: { ...defaults, ...scoringPreset("ceiling") } } as Form, "classic", teams);
  void _m; void _b;
  const restoredOld = formFromSettings(withoutKnobs as NflOptimizerSettings, defaults);
  assert.equal(scoringKindOf(restoredOld.settings), "ceiling", "an old ceilings run restores as ceilings, not as the form's plain projection");
  assert.deepEqual([restoredOld.settings.maxCeilingMultiple, restoredOld.settings.boomWeight], [2.5, 2]);
  const restoredDraws = formFromSettings({ ...withoutKnobs, gppScoring: "draws" } as NflOptimizerSettings, defaults);
  assert.equal(scoringKindOf(restoredDraws.settings), "draws");

  // A plain-projection run, defense caps and all, reloads as itself.
  const dsts = [50, 51];
  const sent = withPlainDstCap(settingsFromForm({ ...empty, settings: defaults } as Form, "classic", teams), dsts);
  assert.equal(sent.exposurePolicies?.length, 2);
  const reloaded = formFromSettings(sent, { ...defaults, ...scoringPreset("draws") });
  assert.equal(scoringKindOf(reloaded.settings), "mean"); assert.equal(reloaded.settings.randomness, PLAIN_RANDOMNESS);
  assert.deepEqual(reloaded.targets, {}, "the defense caps do not come back as typed ranges");
  assert.deepEqual(reloaded.captainTargets, {});
  assert.ok(sameGenerationSettings(withPlainDstCap(settingsFromForm(reloaded, "classic", teams), dsts), sent), "reloading a plain run is not a settings change");

  // Drafts: before v6 the knobs were never stored.
  const v5ceiling = formFromDraft({ schemaVersion: 5, settings: { gppScoring: "ceiling", randomness: 0.08 } }, defaults);
  assert.equal(v5ceiling && scoringKindOf(v5ceiling.settings), "ceiling", "a v5 draft's deliberate ceilings is not turned into plain projection");
  const v5draws = formFromDraft({ schemaVersion: 5, settings: { gppScoring: "draws" } }, defaults);
  assert.equal(v5draws && scoringKindOf(v5draws.settings), "draws", "a v5 default restores as stored; the page then applies the format default");
  const v6 = formFromDraft({ schemaVersion: 6, settings: scoringPreset("mean") }, { ...defaults, ...scoringPreset("draws") });
  assert.equal(v6 && scoringKindOf(v6.settings), "mean", "a v6 draft keeps what it stored");
}

// --- The optimizer does what the choice says ---
function player(over: Partial<NflOptimizerPlayer> & { dkPlayerId: number; position: NflOptimizerPlayer["position"]; team: string }): NflOptimizerPlayer {
  const opponent = ({ AAA: "BBB", BBB: "AAA", CCC: "DDD", DDD: "CCC" } as Record<string, string>)[over.team];
  return {
    id: over.dkPlayerId, dkPlayerId: over.dkPlayerId, captainDkPlayerId: over.dkPlayerId + 100_000,
    name: over.name ?? `${over.position}${over.dkPlayerId}`, position: over.position, team: over.team, opponent, gameKey: [over.team, opponent].sort().join("@"),
    salary: over.salary ?? 5000, captainSalary: 7500, isOut: false, projectionStatus: "historical",
    historyGames: 6, ourProj: over.ourProj ?? 10, floorFpts: 6, ceilingFpts: over.ceilingFpts ?? (over.ourProj ?? 10) * 1.8, boomRate: over.boomRate ?? 0.2,
    avgFptsDk: 10, fantasyprosProj: null, linestarProj: null, linestarOwnPct: null, customProj: null, priorOpportunity: null,
  };
}
const TEAMS = ["AAA", "BBB", "CCC", "DDD"];
function pool(extra: NflOptimizerPlayer[] = []): NflOptimizerPlayer[] {
  return [
    ...TEAMS.map((team, i) => player({ dkPlayerId: 100 + i, position: "QB", team })),
    ...TEAMS.flatMap((team, i) => [0, 1].map((k) => player({ dkPlayerId: 200 + i * 2 + k, position: "RB", team }))),
    ...TEAMS.flatMap((team, i) => [0, 1, 2].map((k) => player({ dkPlayerId: 300 + i * 3 + k, position: "WR", team }))),
    ...TEAMS.map((team, i) => player({ dkPlayerId: 400 + i, position: "TE", team })),
    ...TEAMS.map((team, i) => player({ dkPlayerId: 500 + i, position: "DST", team, ourProj: i === 0 ? 12 : 6 })),
    ...extra,
  ];
}
const uses = (lineups: { slots: { player: { dkPlayerId: number } }[] }[], id: number) => lineups.filter((l) => l.slots.some((s) => s.player.dkPlayerId === id)).length;

{
  // A steady 14-point back against a 9-point back with a 30-point P90.
  const steady = player({ dkPlayerId: 290, position: "RB", team: "AAA", ourProj: 14, ceilingFpts: 18, name: "Steady" });
  const wide = player({ dkPlayerId: 291, position: "RB", team: "CCC", ourProj: 9, ceilingFpts: 30, boomRate: 0.5, name: "Wide" });
  const one = { nLineups: 1, randomness: 0 };
  const byMean = optimizeNflLineups(pool([steady, wide]), plain(one)).lineups;
  assert.equal(uses(byMean, 290), 1, "plain projection takes the higher projection");
  assert.equal(uses(byMean, 291), 0, "and gives the wide range nothing by itself");
  const byCeiling = optimizeNflLineups(pool([steady, wide]), base({ ...scoringPreset("ceiling"), ...one })).lineups;
  assert.equal(uses(byCeiling, 291), 1, "ceiling scoring takes the wide range");
}
{
  // The best-projected defense fills to the flat cap; the guard holds it to 30%.
  const settings = plain({ nLineups: 10 });
  const open = optimizeNflLineups(pool(), settings).lineups;
  assert.equal(open.length, 10);
  assert.equal(uses(open, 500), 6, "without the guard the top defense sits at the 60% flat cap");
  const guarded = optimizeNflLineups(pool(), withPlainDstCap(settings, [500, 501, 502, 503])).lineups;
  assert.equal(guarded.length, 10);
  assert.equal(uses(guarded, 500), 3, "with it, 30% of ten lineups");
  const mine = optimizeNflLineups(pool(), withPlainDstCap({ ...settings, maxExposureByPlayer: { "500": 0.5 } }, [500, 501, 502, 503])).lineups;
  assert.equal(uses(mine, 500), 5, "a range typed on the defense is used instead");
}
console.log("nfl gpp scoring: ok");
