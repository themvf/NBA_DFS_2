/**
 * NBA boom trios (`nba-boom-trios-v1`): the web port picks exactly what
 * research/nba_boom_trios.py picks on a real frozen slate, and grading never
 * scores a missing player as 0.
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { gradeBoomTrios, selectBoomTrios, type BoomSnapshot, type Trio } from "../src/lib/nba-boom-trios";

const fixture = JSON.parse(readFileSync(new URL("../../tests/fixtures/nba_boom_trios_fixture.json", import.meta.url), "utf-8")) as { snapshot: BoomSnapshot; expected: Record<string, unknown> };
const got = selectBoomTrios(fixture.snapshot);
const want = fixture.expected as unknown as Record<string, Trio | null> & { pool: number; counts: boolean };
assert.equal(got.pool, want.pool);
assert.equal(got.counts, want.counts);
for (const name of ["baseline", "boom", "aggressive"] as const) {
  const g = got[name], w = want[name];
  assert.equal(g == null, w == null, `${name} presence`);
  if (!g || !w) continue;
  assert.deepEqual(g.members.map((m) => [m.player_key, m.role]), w.members.map((m) => [m.player_key, m.role]), `${name} members`);
  assert.ok(Math.abs(g.projected_total - w.projected_total) < 0.01, `${name} total`);
  assert.equal(g.why ?? null, w.why ?? null, `${name} reason`);
}
const rows = got.baseline!.members.concat(got.boom?.members ?? [], got.aggressive?.members ?? [])
  .filter((m, i, all) => all.findIndex((x) => x.player_key === m.player_key) === i)
  .map((m) => ({ event_id: m.event_id, player_key: m.player_key, status: "graded", actual_points: 20 }));
const g = gradeBoomTrios(got, rows);
assert.equal(g.baseline, 60);
const missing = gradeBoomTrios(got, rows.map((r, i) => (i === 0 ? { ...r, status: "dnp", actual_points: null } : r)));
assert.equal(missing.baseline, null);
assert.equal(missing.boom.status === "no_alternative" || missing.boom.status === "incomplete", true);
console.log("NBA boom trios: web port matches the Python rules on a real slate; missing players are incomplete, never 0.");
