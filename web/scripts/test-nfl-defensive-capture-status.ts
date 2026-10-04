/**
 * Upload-time opponent captures: every outcome reads as its own sentence, and
 * coverage the generation reader sees always wins over the request's state.
 */
import assert from "node:assert/strict";
import { profileCaptureStatus, type CaptureRequestRow } from "../src/lib/nfl-dfs/defensive-capture-status";

const req = (over: Partial<CaptureRequestRow> = {}): CaptureRequestRow => ({ profile: "pfr-efficiency", state: "pending",
  capturedPlayers: null, lastError: null, dispatchError: null, workerRunUrl: null, updatedAt: null, ...over });
const none = { applied: 0, eligible: 50, captured: 0 };
const pre = { started: false };
const st = (r: CaptureRequestRow | null, c = none, o = pre) => profileCaptureStatus("PFR efficiency", r, c, o);

// Coverage first: an applied capture is "applied" whatever the request says (a scheduled run may have supplied it).
assert.equal(st(req({ state: "failed" }), { applied: 12, eligible: 50, captured: 30 }).status, "applied");
assert.match(st(null, { applied: 12, eligible: 50, captured: 30 }).text, /12 of 50 players adjusted \(30 captured\)/);
assert.equal(st(req(), { applied: 0, eligible: 50, captured: 30 }).status, "captured_zero");
assert.equal(st(null, { ...none, error: "connection reset" }).status, "read_error");

// Each request state is distinct.
assert.equal(st(null).status, "not_requested");
assert.equal(st(null).retryable, true, "a missing request can be requested pregame");
assert.equal(st(null, none, { started: true }).retryable, false, "never after kickoff");
assert.equal(st(req()).status, "pending");
assert.match(st(req({ state: "running" })).text, /capture running/);
assert.match(st(req({ dispatchError: "dispatch failed (401)" })).text, /starting it failed \(dispatch failed \(401\)\); it retries within 15 minutes/);
const failed = st(req({ state: "failed", lastError: "ValueError: boom" }));
assert.deepEqual([failed.status, failed.retryable], ["failed", true]);
assert.match(failed.text, /capture failed \(ValueError: boom\)/);
assert.equal(st(req({ state: "failed" }), none, { started: true }).retryable, false);
const inel = st(req({ state: "ineligible", lastError: "slate_started_or_unscheduled (PIT)" }));
assert.deepEqual([inel.status, inel.retryable], ["ineligible", false]);
// The worker verified rows but the reader sees none usable.
assert.match(st(req({ state: "captured", capturedPlayers: 40 })).text, /captured 40 players, but none can be used now/);
console.log("Opponent capture status: applied, captured-but-zero, pending, dispatch-failed, failed, ineligible, not requested and unreadable each read differently; retry only pregame.");
