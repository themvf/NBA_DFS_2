import assert from "node:assert/strict";
import { coverage, captureAgeHours, type AuditCell } from "../src/lib/nfl-dfs/feature-audit";
const cell: AuditCell = { field_id: "Workload:attempts", position: "QB", season: 2025, n: 2, valid: 1,
  present: 1, missing: 1, invalid: 0, zero: 1, captured: 0, latest_capture: null, status: "retrospective_only" };
assert.equal(coverage(cell), 50);
assert.equal(coverage({ ...cell, n: 0 }), null);
assert.equal(coverage({ ...cell, status: "unsupported" }), null);
assert.equal(coverage(undefined), null);
assert.equal(captureAgeHours(null, Date.now()), null);
assert.equal(captureAgeHours("invalid", Date.now()), null);
assert.equal(captureAgeHours("2026-09-04T12:00:00Z", Date.parse("2026-09-04T14:00:00Z")), 2);
assert.equal(captureAgeHours("2026-09-04T15:00:00Z", Date.parse("2026-09-04T14:00:00Z")), null);
console.log("NFL feature-audit coverage and timestamp checks passed");
