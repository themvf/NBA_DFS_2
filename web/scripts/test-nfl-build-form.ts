/**
 * Loading a saved run into the build form must rebuild the same run.
 */
import assert from "node:assert/strict";
import { formFromSettings, sameGenerationSettings, settingsFromForm, type NflBuildForm } from "../src/lib/nfl-dfs/generation-settings";

const defaults = { mode: "gpp" as "cash" | "gpp", projectionSource: "our" as const, allowDkFallback: false, nLineups: 20,
  minSalary: 45000, maxExposure: 0.6, minUnique: 2, stackPassCatchers: 1 as 0 | 1 | 2, bringBack: true, randomness: 0.08 };
type Form = NflBuildForm<typeof defaults>;
const teams = ["ATL", "GB"];

const form: Form = {
  settings: { ...defaults, nLineups: 40, maxExposure: 0.5, randomness: 0.12 },
  locked: [11], excluded: [12, 13],
  targets: { "21": 35, "22": 10 },
  captainTargets: { "31": { min: 20, max: 40 }, "32": { min: null, max: 15 } },
  planMode: "chalk_leverage", quotas: [], favorite: "", fades: [],
};
const sent = settingsFromForm(form, "showdown", teams);
// The server adds these on save; they are not form state.
const saved = { ...sent, favoriteTeam: "GB", underdogTeam: "ATL", ownershipCapability: "unavailable",
  ownershipDisclosure: { capability: "unavailable" } } as unknown as typeof sent;

const loaded = formFromSettings(saved, defaults);
assert.deepEqual(loaded.settings, form.settings, "scalar settings come back");
assert.deepEqual(loaded.locked, [11]); assert.deepEqual(loaded.excluded, [12, 13]);
assert.deepEqual(loaded.captainTargets, form.captainTargets, "captain ranges come back exactly");
assert.equal(loaded.planMode, "chalk_leverage");
assert.equal(loaded.favorite, "", "a favorite the SERVER resolved is not a user choice");
assert.ok(sameGenerationSettings(settingsFromForm(loaded, "showdown", teams), sent), "reloading rebuilds the same run");
assert.equal((loaded.settings as Record<string, unknown>).ownershipDisclosure, undefined, "save-time extras stay out of the form");

// 35% of 40 lineups is 14 lineups -> 35%; 10% -> 4 lineups -> 10%. An uneven
// target comes back as what was applied, which reproduces the run.
const uneven = settingsFromForm({ ...form, settings: { ...form.settings, nLineups: 20 }, targets: { "21": 33 } }, "showdown", teams);
const unevenLoaded = formFromSettings(uneven, defaults);
assert.equal(unevenLoaded.targets["21"], 35, "33% of 20 lineups was applied as 7 lineups = 35%");
assert.ok(sameGenerationSettings(settingsFromForm(unevenLoaded, "showdown", teams), uneven));

// Custom plan: quotas, the user's favorite and fades round-trip.
const custom: Form = { ...form, planMode: "custom", favorite: "ATL", fades: [41, 42],
  quotas: [{ archetypeId: "double_fade", minLineups: 2, maxLineups: 6, enabled: true }] as Form["quotas"] };
const customSent = settingsFromForm(custom, "showdown", teams);
const customLoaded = formFromSettings(customSent, defaults);
assert.equal(customLoaded.favorite, "ATL"); assert.deepEqual(customLoaded.fades, [41, 42]);
assert.deepEqual(customLoaded.quotas, custom.quotas);
assert.ok(sameGenerationSettings(settingsFromForm(customLoaded, "showdown", teams), customSent));

// A run saved before a setting existed keeps the form's current value.
const old = { ...sent } as Record<string, unknown>; delete old.randomness; delete old.archetypeMode;
const oldLoaded = formFromSettings(old as unknown as typeof sent, defaults);
assert.equal(oldLoaded.settings.randomness, defaults.randomness);
assert.equal(oldLoaded.planMode, "standard", "a run with no plan was a plain ceiling build");

console.log("Build form: saved runs load back into the form and rebuild the same run.");
