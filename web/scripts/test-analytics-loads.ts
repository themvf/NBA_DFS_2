// A failed analytics query must be reported by name, never rendered as an
// empty table. These checks pin the section-load bookkeeping the page uses.
import assert from "node:assert/strict";
import { loadSection, sectionErrors, sectionValue, skippedSection } from "../src/app/analytics/analytics-loads";

async function main() {
  const ok = await loadSection("Accuracy trend", async () => [1, 2, 3]);
  assert.deepEqual(ok, { label: "Accuracy trend", ok: true, value: [1, 2, 3] });

  const failed = await loadSection("Position breakdown", async () => { throw new Error("relation dk_players does not exist"); });
  assert.equal(failed.ok, false);
  assert.equal(failed.label, "Position breakdown");
  if (!failed.ok) assert.equal(failed.error, "relation dk_players does not exist");

  // Synchronous throws inside the loader are caught too (the old lambda-wrapper reason).
  const syncThrow = await loadSection("Salary tier", () => { throw new TypeError("boom"); });
  assert.equal(syncThrow.ok, false);

  // Non-Error rejections still produce a readable reason, never "[object Object]".
  const weird = await loadSection("Leverage", async () => { throw "string reason"; });
  assert.equal(weird.ok, false);
  if (!weird.ok) assert.equal(weird.error, "string reason");

  // An empty successful read and a failed read are distinguishable.
  const empty = await loadSection("Ownership", async () => []);
  assert.equal(sectionValue(empty, ["fallback"]).length, 0, "empty stays empty");
  assert.deepEqual(sectionValue(failed, [] as number[]), [], "failed gets the fallback for rendering");
  assert.deepEqual(sectionErrors([ok, failed, empty, syncThrow, weird]).map((e) => e.label),
    ["Position breakdown", "Salary tier", "Leverage"], "only the failures are listed, in order");

  // A section the sport does not have is neither loaded nor an error.
  const skipped = skippedSection("Batting order", []);
  assert.deepEqual(sectionErrors([skipped]), []);
  assert.deepEqual(sectionValue(skipped, ["x"]), []);

  console.log("RESULT: analytics-loads checks passed");
}

main().catch((error) => { console.error(error); process.exit(1); });
