/**
 * Live DraftKings status, browser/server side — and the pin that keeps it
 * honest.
 *
 * The rules live twice: here and in `model/nfl_dfs_dk_pool_match.py`. Python
 * captures, TypeScript reads, and both must refuse the same pools for the same
 * reasons — so the last block reads the Python constants off disk and asserts
 * they agree.
 *
 * The dangerous failure is not a missed update; it is applying the WRONG pool.
 * DraftKings lists one game under several contest types at once, each with the
 * same two teams. Most of what is asserted below is refusal.
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import {
  buildLiveStatusOverlay, isLiveOutStatus, statusClass, EMPTY_LIVE_OVERLAY,
  SALARY_AGREEMENT_FLOOR, MIN_MATCHED_FOR_SALARY_CHECK,
  type LivePool, type SlateRowForLive,
} from "../src/lib/nfl-dfs/live-dk-status";

const UPLOADED = new Date("2026-09-22T09:00:00Z");
const CAPTURED = new Date("2026-09-23T12:00:00Z");

// The slate's own normalizer, character for character (actions.ts).
const norm = (value: string) =>
  value.normalize("NFKD").replace(/[̀-ͯ]/g, "").toLowerCase()
    .replace(/\b(jr|sr|ii|iii|iv)\b/g, "").replace(/[^a-z0-9]+/g, "");

const slateRow = (name: string, salary: number, dkStatus: string | null = null): SlateRowForLive =>
  ({ normalizedName: norm(name), salary, dkStatus });

const livePool = (
  players: { name: string; salary: number; status: string | null; isDisabled?: boolean }[],
  over: Partial<LivePool> = {},
): LivePool => ({
  draftGroupId: 153769, format: "classic", teams: ["GB", "LV"], capturedAt: CAPTURED,
  players: players.map((p) => ({
    normalizedName: norm(p.name), name: p.name, team: "LV",
    salary: p.salary, status: p.status, isDisabled: Boolean(p.isDisabled),
  })),
  ...over,
});

function main() {
  // --- Nothing captured ------------------------------------------------------
  assert.equal(buildLiveStatusOverlay([], "classic", ["GB", "LV"], null, UPLOADED), EMPTY_LIVE_OVERLAY);

  // --- Refusals --------------------------------------------------------------
  const one = [slateRow("Brock Bowers", 6600, "D")];
  const bowers = [{ name: "Brock Bowers", salary: 6600, status: "O" }];

  assert.equal(buildLiveStatusOverlay(one, "showdown", ["GB", "LV"],
    livePool(bowers), UPLOADED).applied, false, "format must agree");
  assert.equal(buildLiveStatusOverlay(one, "classic", ["GB", "SF"],
    livePool(bowers), UPLOADED).applied, false, "team set must agree");
  assert.equal(buildLiveStatusOverlay(one, "classic", ["GB", "LV"],
    livePool(bowers, { capturedAt: new Date("2026-09-22T08:00:00Z") }), UPLOADED).applied, false,
    "an observation older than the upload is not news");

  // The real collision: the same game listed as Snake Showdown ranks players
  // 1..N in the salary field, and matches on team set AND format.
  const names = Array.from({ length: 12 }, (_, i) => `Player ${String.fromCharCode(65 + i)}`);
  const ranked = buildLiveStatusOverlay(
    names.map((n, i) => slateRow(n, 5000 + 100 * i)), "classic", ["GB", "LV"],
    livePool(names.map((n, i) => ({ name: n, salary: i + 1, status: null }))), UPLOADED);
  assert.equal(ranked.applied, false);
  assert.match(ranked.reason, /Salaries disagree/);
  assert.equal(ranked.statuses.size, 0, "a refused pool contributes nothing at all");

  // --- Ambiguity is dropped, never resolved ---------------------------------
  const dup = buildLiveStatusOverlay(
    [slateRow("Mike Williams", 5000), slateRow("Mike Williams", 4200), slateRow("Other Guy", 6000)],
    "classic", ["GB", "LV"],
    livePool([{ name: "Mike Williams", salary: 5000, status: "O" },
              { name: "Other Guy", salary: 6000, status: null }]), UPLOADED);
  assert.equal(dup.applied, true);
  assert.equal(dup.statuses.has(norm("Mike Williams")), false);
  assert.deepEqual(dup.ambiguousNames, [norm("Mike Williams")]);
  assert.deepEqual(dup.changes, [], "a dropped name cannot report a change either");

  // --- What it does say ------------------------------------------------------
  const applied = buildLiveStatusOverlay(
    [slateRow("Brock Bowers", 6600, "D"), slateRow("Other Guy", 5000, null)],
    "classic", ["GB", "LV"],
    livePool([{ name: "Brock Bowers", salary: 6600, status: "O" },
              { name: "Other Guy", salary: 5000, status: null }]), UPLOADED);
  assert.equal(applied.applied, true);
  assert.equal(applied.statuses.get(norm("Brock Bowers")), "O");
  assert.equal(applied.statuses.get(norm("Other Guy")), null);
  assert.deepEqual(applied.changes, [{ name: "Brock Bowers", team: "LV", from: "D", to: "O" }]);
  assert.equal(applied.salaryAgreement, 1);

  // A player DraftKings blocks from drafting is out even with no tag; that is
  // a stronger statement than any string in the status column.
  const blocked = buildLiveStatusOverlay(
    [slateRow("Blocked Guy", 5000, null)], "classic", ["GB", "LV"],
    livePool([{ name: "Blocked Guy", salary: 5000, status: null, isDisabled: true }]), UPLOADED);
  assert.equal(blocked.statuses.get(norm("Blocked Guy")), "OUT");

  // Spelling is not a change. The salary file writes "OUT" and the live feed
  // writes "O" for the same player; on the real Thursday ATL@GB slate that
  // difference alone announced three changes where nothing had happened.
  const spelling = buildLiveStatusOverlay(
    [slateRow("Josh Jacobs", 8400, "OUT"), slateRow("Jayden Reed", 6200, "OUT")],
    "classic", ["GB", "LV"],
    livePool([{ name: "Josh Jacobs", salary: 8400, status: "O" },
              { name: "Jayden Reed", salary: 6200, status: "O" }]), UPLOADED);
  assert.equal(spelling.applied, true);
  assert.deepEqual(spelling.changes, [], "OUT and O are the same fact");
  assert.equal(spelling.statuses.get(norm("Josh Jacobs")), "O", "...but the current tag is still reported");
  assert.equal(statusClass("OUT"), statusClass("O"));
  assert.equal(statusClass("IR"), "out");
  assert.equal(statusClass("D"), "doubtful");
  assert.equal(statusClass("Q"), "questionable");
  assert.equal(statusClass(null), "none");
  // A real change still reports.
  assert.equal(buildLiveStatusOverlay(
    [slateRow("Josh Jacobs", 8400, "Q")], "classic", ["GB", "LV"],
    livePool([{ name: "Josh Jacobs", salary: 8400, status: "O" }]), UPLOADED).changes.length, 1);

  // An unchanged pool is applied and reports nothing, which is the common case
  // and must not look like a failure.
  const quiet = buildLiveStatusOverlay(
    [slateRow("Brock Bowers", 6600, "D")], "classic", ["GB", "LV"],
    livePool([{ name: "Brock Bowers", salary: 6600, status: "D" }]), UPLOADED);
  assert.equal(quiet.applied, true);
  assert.deepEqual(quiet.changes, []);

  // --- OUT set --------------------------------------------------------------
  for (const out of ["O", "OUT", "IR", "PUP", "SUSP", "NA", "ir"]) assert.ok(isLiveOutStatus(out));
  // Doubtful and Questionable are judgement calls the optimizer owns.
  for (const kept of ["D", "Q", "", null, undefined]) assert.equal(isLiveOutStatus(kept), false);

  // --- The cross-language pin -----------------------------------------------
  const python = readFileSync(join(process.cwd(), "..", "model", "nfl_dfs_dk_pool_match.py"), "utf8");
  const constant = (name: string) => {
    const match = python.match(new RegExp(`^${name}\\s*=\\s*([0-9.]+)`, "m"));
    assert.ok(match, `${name} not found in model/nfl_dfs_dk_pool_match.py`);
    return Number(match![1]);
  };
  assert.equal(constant("SALARY_AGREEMENT_FLOOR"), SALARY_AGREEMENT_FLOOR);
  assert.equal(constant("MIN_MATCHED_FOR_SALARY_CHECK"), MIN_MATCHED_FOR_SALARY_CHECK);
  const outSet = python.match(/OUT_STATUSES\s*=\s*frozenset\(\{([^}]*)\}\)/);
  assert.ok(outSet, "OUT_STATUSES not found in the Python module");
  const pythonOut = [...outSet![1].matchAll(/"(\w+)"/g)].map((m) => m[1]).sort();
  assert.deepEqual(pythonOut, ["IR", "NA", "O", "OUT", "PUP", "SUSP"]);
  for (const status of pythonOut) assert.ok(isLiveOutStatus(status), `${status} differs across languages`);

  // The normalizer is the join key. If Python ever strips digits the way the
  // field audit does, every team defense silently stops matching.
  assert.ok(python.includes("[^a-z0-9]+"), "Python normalizer must keep digits");
  assert.ok(python.includes("(jr|sr|ii|iii|iv)"), "Python normalizer must strip the same suffixes");
  assert.equal(norm("49ers"), "49ers");
  assert.equal(norm("Michael Penix Jr."), "michaelpenix");

  console.log("Live DraftKings status (browser):");
  console.log("  - a different game, format or contest type is refused whole");
  console.log("  - a repeated name is dropped rather than guessed");
  console.log("  - an observation older than the salary file is not news");
  console.log("  - thresholds, the OUT set and the join key are pinned against Python");
}

main();
