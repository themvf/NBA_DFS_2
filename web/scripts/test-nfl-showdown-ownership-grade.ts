/**
 * Showdown ownership grade (`nfl-showdown-ownership-grade-v1`), the reading
 * side: two models are compared only on the slates where both were stored,
 * each slate counts once, and the number the build read is the one shown for
 * its family.
 */
import assert from "node:assert/strict";
import { FAMILY_LABEL, headToHead, modelsByFamily, orderedModels, slatesByFamily,
  type GradedModel, type GradeFamily, type GradeRole, type OwnershipGradeRow } from "../src/lib/nfl-dfs/showdown-ownership-grade";

const slot = (miss: number | null, n = 30) => ({ n, miss, bias: 0, chalk_n: 5, chalk_miss: miss, worst: [{ name: "A One", estimate: 10, real: 20 }] });
const model = (role: GradeRole, family: GradeFamily, captain: number | null, flex: number | null): GradedModel =>
  ({ role, family, version: `${family}-v1`, captain: slot(captain), flex: slot(flex) });
const row = (game: string, kickoff: string, models: GradedModel[]): OwnershipGradeRow => ({
  slateKey: `showdown|${game}|${kickoff}`, game, firstKickoff: kickoff, season: 2026, week: 10, contestId: "1", contestName: null,
  entryCount: 70000, labelsComplete: true, captureSource: "ghost_freeze", capturedAt: kickoff, models });
const near = (a: number | null, b: number, label: string) => assert.ok(a != null && Math.abs(a - b) < 1e-9, `${label}: ${a} vs ${b}`);

const rows = [
  // stat-api was shown; our lineup model and the per-player model were kept beside it.
  row("WAS@NYG", "2026-11-13T01:15:00.000Z", [model("shown", "stat_api", 0.7, 1.8), model("ours", "lineup_model", 0.9, 2.6), model("shadow", "fitted_model", 1.3, 4.7)]),
  // The lineup model was shown; the per-player model is the comparison.
  row("NE@DET", "2026-11-15T14:30:00.000Z", [model("shown", "lineup_model", 1.1, 3.0), model("shadow", "fitted_model", 1.0, 3.0)]),
  // Lines were not in: the per-player model was shown, the rule of thumb beside it.
  row("TB@CHI", "2026-11-09T01:20:00.000Z", [model("shown", "fitted_model", 0.8, 4.1), model("shadow", "rule_of_thumb", 1.1, 4.3)]),
];

const pairs = headToHead(rows);
const pair = (a: GradeFamily, b: GradeFamily) => pairs.find((p) => p.a === a && p.b === b);

// Lineup model against the per-player model: the two slates that stored both, and no other.
const lineupVsFitted = pair("lineup_model", "fitted_model")!;
assert.equal(lineupVsFitted.slates, 2);
near(lineupVsFitted.aFlex, (2.6 + 3.0) / 2, "lineup flex");
near(lineupVsFitted.bFlex, (4.7 + 3.0) / 2, "fitted flex on the same two slates, not all three it was stored on");
near(lineupVsFitted.aCaptain, 1.0, "lineup captain");
assert.deepEqual([lineupVsFitted.aCloser, lineupVsFitted.bCloser], [1, 0], "one slate closer, one level; a tie counts for neither");

// A pair stored together once is still shown, and a pair never stored together is not.
assert.equal(pair("lineup_model", "stat_api")!.slates, 1);
assert.equal(pair("fitted_model", "rule_of_thumb")!.slates, 1);
assert.equal(pairs.find((p) => (p.a === "stat_api" && p.b === "rule_of_thumb") || (p.a === "lineup_model" && p.b === "rule_of_thumb")), undefined);
assert.ok(pairs[0].slates >= pairs[pairs.length - 1].slates, "most shared slates first");

// How many slates each model was stored on.
assert.deepEqual(slatesByFamily(rows), [{ family: "lineup_model", slates: 2 }, { family: "stat_api", slates: 1 },
  { family: "fitted_model", slates: 3 }, { family: "rule_of_thumb", slates: 1 }]);

// Display order: the number the build read first.
assert.deepEqual(orderedModels(rows[0]).map((m) => m.role), ["shown", "ours", "shadow"]);

// Two stored numbers of one family: the one the build read is that family's; a model with no flex grade is left out.
const twice = row("X@Y", "2026-10-01T00:00:00.000Z", [model("shadow", "rule_of_thumb", 2, 6), model("shown", "rule_of_thumb", 1, 5), model("ours", "fitted_model", 1, null)]);
const byFamily = modelsByFamily(twice);
assert.equal(byFamily.get("rule_of_thumb")!.role, "shown");
assert.equal(byFamily.has("fitted_model"), false);
assert.deepEqual(headToHead([twice]), []);

// A pair with captain grades on only some slates averages captain over those slates.
const noCaptain = [row("A@B", "2026-10-02T00:00:00.000Z", [model("shown", "fitted_model", null, 4), model("shadow", "rule_of_thumb", null, 5)]),
  row("C@D", "2026-10-03T00:00:00.000Z", [model("shown", "fitted_model", 1, 4), model("shadow", "rule_of_thumb", 2, 5)])];
const partial = headToHead(noCaptain)[0];
near(partial.aCaptain, 1, "captain over the one slate that has it"); near(partial.aFlex, 4, "flex over both");

assert.equal(FAMILY_LABEL.fitted_model, "Per-player model");
console.log("NFL Showdown ownership grade: head-to-head rows use shared slates only, each slate once; the build's own number names its family.");
