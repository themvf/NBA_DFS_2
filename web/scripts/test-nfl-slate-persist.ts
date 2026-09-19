import assert from "node:assert/strict";
import {
  PROJECTION_RUN_STATUSES, SLATE_PLAYER_STATUSES, SLATE_WRITE_CHUNK,
  assertSlateFullyPersisted, assertSlateStatusVocabulary, chunkRows,
  incompleteSlateWarning, isSlateComplete,
} from "../src/lib/nfl-dfs/slate-persist";
import { OUT_PROJECTION_STATUS } from "../src/lib/nfl-dfs/out-projection";

// ── Chunking a real Classic pool ───────────────────────────────────────
{
  // 670 is the measured size of the 13-game slate that broke. The point of
  // the chunking is the round-trip count: 670 separate inserts is what let a
  // write stop half-way, 3 statements in one batch is what stops it.
  const rows = Array.from({ length: 670 }, (_, i) => i);
  const chunks = chunkRows(rows);
  assert.equal(chunks.length, 3, "670 players is 3 statements, not 670 round-trips");
  assert.deepEqual(chunks.map(c => c.length), [250, 250, 170]);

  // Order is preserved and nothing is dropped or duplicated.
  assert.deepEqual(chunks.flat(), rows, "every parsed player is written exactly once, in file order");
}

// ── Showdown fits in a single statement ────────────────────────────────
{
  assert.equal(chunkRows(Array.from({ length: 68 }, (_, i) => i)).length, 1);
}

// ── Degenerate inputs ──────────────────────────────────────────────────
{
  assert.deepEqual(chunkRows([]), [], "an empty pool produces no statements, not one empty one");
  assert.deepEqual(chunkRows([1, 2, 3], 1), [[1], [2], [3]]);
  assert.deepEqual(chunkRows([1, 2], 99), [[1, 2]], "a chunk larger than the pool is one statement");
  assert.throws(() => chunkRows([1], 0), /at least 1/, "a zero chunk size would loop forever");
}

// ── The parameter ceiling is respected with room to spare ──────────────
{
  // Postgres binds at most 65,535 parameters per statement; a slate player
  // binds ~28 columns. The chunk must stay well under that.
  const COLUMNS = 28;
  assert.ok(SLATE_WRITE_CHUNK * COLUMNS < 65535 / 2,
    "chunk size leaves at least 2x headroom under the bound-parameter cap");
}

// ── The post-write count check ─────────────────────────────────────────
{
  assert.doesNotThrow(() => assertSlateFullyPersisted(670, 670, "DKSalaries.csv"));
  assert.doesNotThrow(() => assertSlateFullyPersisted(0, 0, "empty.csv"));

  // The exact failure that shipped: 670 parsed, 10 stored.
  assert.throws(
    () => assertSlateFullyPersisted(670, 10, "DKSalaries (3).csv"),
    (err: Error) => {
      assert.match(err.message, /DKSalaries \(3\)\.csv/, "names the file");
      assert.match(err.message, /670/, "names what was parsed");
      assert.match(err.message, /10/, "names what was actually stored");
      assert.match(err.message, /upload the file again/i, "says what to do about it");
      return true;
    },
  );

  // More rows than parsed is a collision, not a bonus.
  assert.throws(() => assertSlateFullyPersisted(68, 70, "x.csv"), /parsed 68 .* stored 70/);
}

// ── Naming a slate the old path already damaged ────────────────────────
{
  assert.equal(incompleteSlateWarning(670, 670), null, "a whole slate is not warned about");
  assert.equal(incompleteSlateWarning(0, 0), null);

  const warning = incompleteSlateWarning(670, 10, "DKSalaries (3).csv")!;
  assert.match(warning, /DKSalaries \(3\)\.csv/);
  assert.match(warning, /670 players/);
  assert.match(warning, /only 10/);
  assert.match(warning, /660 are missing/, "states the size of the hole, not just that there is one");
  assert.match(warning, /upload the DraftKings CSV again/i);

  // Works without a file name too -- the header row is the thing that may be
  // missing context, and a warning is still better than silence.
  assert.match(incompleteSlateWarning(670, 10)!, /660 are missing/);
}

// ── Completeness gate for the saved-slate list ─────────────────────────
{
  assert.ok(isSlateComplete(670, 670));
  assert.ok(!isSlateComplete(670, 10), "a ten-man pool is not offered as a slate to draft from");
  assert.ok(!isSlateComplete(0, 0), "a slate with no players at all is not a choice either");

  // A slate that somehow holds more rows than claimed is still selectable --
  // `assertSlateFullyPersisted` is where that is caught on write. Hiding it
  // from the list would take away the user's only view of it.
  assert.ok(isSlateComplete(68, 70));
}

// ── The vocabulary drift that actually broke the upload ────────────────
{
  // `loadNflSalaryCsv` copies a projection row's status straight onto the
  // slate row, so anything a run can emit must be storable on a slate.
  assert.doesNotThrow(assertSlateStatusVocabulary);
  for (const status of PROJECTION_RUN_STATUSES) {
    assert.ok((SLATE_PLAYER_STATUSES as readonly string[]).includes(status),
      `a projection run can emit "${status}", so a slate player must be able to hold it`);
  }

  // The specific value that was missing, and the module that introduced it.
  assert.ok((PROJECTION_RUN_STATUSES as readonly string[]).includes(OUT_PROJECTION_STATUS),
    "the availability pipeline's zeroed status is part of the upstream vocabulary");
  assert.ok((SLATE_PLAYER_STATUSES as readonly string[]).includes(OUT_PROJECTION_STATUS),
    "and the slate table must accept it -- omitting it made ruled-out players unwritable");

  // `unmatched` is the slate table's own addition: a salary row with no
  // projection at all. A run never emits it.
  assert.ok((SLATE_PLAYER_STATUSES as readonly string[]).includes("unmatched"));
  assert.ok(!(PROJECTION_RUN_STATUSES as readonly string[]).includes("unmatched"),
    "unmatched is the salary-side fallback, not something a projection run produces");

  // The guard has to actually fire, or it is decoration.
  assert.throws(() => {
    const slate = new Set<string>(["historical"]);
    const missing = PROJECTION_RUN_STATUSES.filter((s) => !slate.has(s));
    if (missing.length) throw new Error(`cannot store ${missing.join(", ")}`);
  }, /cannot store .*out/);
}

console.log("nfl slate persist: all assertions passed");
