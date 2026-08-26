import assert from "node:assert/strict";

import { buildInjuryMarkerView, type FantasyInjuryDetails } from "../src/lib/fantasy-football/injury-display";

const base: FantasyInjuryDetails = {
  active: true,
  status: "DOUBTFUL",
  bodyPart: "Hamstring",
  injuryType: null,
  description: "Week-to-week",
  practiceStatus: "DNP",
  expectedReturnMin: null,
  expectedReturnMax: null,
  weeksOutMin: 1,
  weeksOutMax: 2,
  availabilityProbability: 0.25,
  estimateBasis: "provider",
  confidence: null,
  primarySource: "sleeper",
  detailSource: "fantasypros",
  sourceConflict: false,
  firstSeenAt: "2026-08-25T12:00:00Z",
  lastConfirmedAt: "2026-08-26T12:00:00Z",
  providerUpdatedAt: "2026-08-26T11:00:00Z",
  clearedAt: null,
};

const rich = buildInjuryMarkerView("DOUBTFUL", base, new Date("2026-08-26T14:00:00Z"));
assert.equal(rich?.label, "DOUBTFUL · Hamstring · 1-2 wk");
assert.match(rich?.title ?? "", /Provider timeline: 1-2 wk/);
assert.match(rich?.title ?? "", /Practice: DNP/);
assert.match(rich?.title ?? "", /Source: fantasypros/);

const fallback = buildInjuryMarkerView("QUESTIONABLE", null);
assert.equal(fallback?.label, "QUESTIONABLE");
assert.match(fallback?.title ?? "", /Return timeline unavailable/);

const cleared = buildInjuryMarkerView(null, {
  ...base,
  active: false,
  clearedAt: "2026-08-26T08:00:00Z",
}, new Date("2026-08-26T14:00:00Z"));
assert.equal(cleared?.label, "CLEARED · 6h ago");
assert.equal(cleared?.cleared, true);

const conflict = buildInjuryMarkerView("OUT", { ...base, status: "OUT", bodyPart: null, sourceConflict: true });
assert.equal(conflict?.label, "OUT · 1-2 wk · CONFLICT");
assert.equal(conflict?.conflict, true);

console.log("injury-display tests passed");
