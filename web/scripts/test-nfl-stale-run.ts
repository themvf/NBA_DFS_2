import assert from "node:assert/strict";
import { staleRunWarning, type RunStamp } from "../src/lib/nfl-dfs/stale-run";

const run = (over: Partial<RunStamp> = {}): RunStamp => ({
  runId: "a", modelVersion: "nfl-dfs-historical-v3", asOfAt: "2026-09-19T12:46:22Z", ...over,
});

// The real case: a 09-18 slate pinned to a 09-16 v2 run while 09-19 v3 exists.
{
  const linked = run({ runId: "3df228c6", modelVersion: "nfl-dfs-historical-v2", asOfAt: "2026-09-16T13:43:51Z" });
  const newest = run({ runId: "52b4b3c9", modelVersion: "nfl-dfs-historical-v3", asOfAt: "2026-09-19T12:46:22Z" });
  const warning = staleRunWarning(linked, newest)!;
  assert.ok(warning, "a stale slate warns");
  assert.match(warning, /2026-09-16/, "names the run it is on");
  assert.match(warning, /2026-09-19/, "names the run available");
  assert.match(warning, /nfl-dfs-historical-v2 to nfl-dfs-historical-v3/, "names the version change");
  assert.match(warning, /Re-upload the same DraftKings CSV/, "gives the remedy");
  assert.match(warning, /hand on opportunity his own run recorded/, "says what it actually costs");
}

// Current slate: silent.
{
  const same = run({ runId: "same" });
  assert.equal(staleRunWarning(same, { ...same }), null);
}

// An intentionally older link is not nagged about -- reproducing a past
// decision is legitimate, and a warning that cries wolf gets ignored.
{
  const linked = run({ runId: "new", asOfAt: "2026-09-19T12:00:00Z" });
  const older = run({ runId: "old", asOfAt: "2026-09-16T13:00:00Z" });
  assert.equal(staleRunWarning(linked, older), null, "an OLDER alternative is not a staleness warning");
}

// Same model version: no version clause, but still warns on the date.
{
  const w = staleRunWarning(
    run({ runId: "x", asOfAt: "2026-09-16T00:00:00Z" }),
    run({ runId: "y", asOfAt: "2026-09-19T00:00:00Z" }),
  )!;
  assert.match(w, /2026-09-16/);
  assert.doesNotMatch(w, /model version also changed/);
}

// Missing inputs degrade quietly rather than inventing a claim.
{
  assert.equal(staleRunWarning(null, run()), null);
  assert.equal(staleRunWarning(run(), null), null);
  assert.equal(staleRunWarning(null, null), null);
  // An unparseable date still warns (the ids differ) without printing garbage.
  const w = staleRunWarning(run({ runId: "x", asOfAt: "not-a-date" }), run({ runId: "y" }))!;
  assert.match(w, /an unknown date/);
}

// Date objects are accepted as well as ISO strings.
{
  const w = staleRunWarning(
    run({ runId: "x", asOfAt: new Date("2026-09-16T13:43:51Z") }),
    run({ runId: "y", asOfAt: new Date("2026-09-19T12:46:22Z") }),
  )!;
  assert.match(w, /2026-09-16/);
  assert.match(w, /2026-09-19/);
}

console.log("nfl stale run: all assertions passed");
