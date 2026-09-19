import assert from "node:assert/strict";
import {
  DEFAULT_POOL_SORT, POOL_SORT_COLUMNS, isPoolSortKey, nextPoolSort, sortPlayerPool,
  type PoolSortKey, type SortablePoolPlayer,
} from "../src/lib/nfl-dfs/player-pool-sort";
import { buildValueIndex, type ValuePlayerInput } from "../src/lib/nfl-dfs/salary-value";

let id = 0;
type Row = SortablePoolPlayer & ValuePlayerInput;
const row = (over: Partial<Row> & { name?: string } = {}): Row => {
  id += 1;
  return {
    dkPlayerId: id, name: `P${id}`, position: "WR", team: "SF", salary: 5000,
    ourProj: 10, floorFpts: null, ceilingFpts: null, avgFptsDk: null,
    fantasyprosProj: null, linestarProj: null, linestarOwnPct: null,
    projectionStatus: "historical", isOut: false, ...over,
  };
};
/** Value index over the same rows, so `assess` sees a real reference pool. */
const sortBy = (rows: Row[], key: PoolSortKey, direction: "asc" | "desc") => {
  const index = buildValueIndex(rows);
  return sortPlayerPool(rows, { key, direction }, (p) => index.assess(p as Row));
};
const names = (rows: Row[]) => rows.map((r) => r.name);

// ── Column registry and toggle behaviour ───────────────────────────────
{
  assert.ok(isPoolSortKey("value"));
  assert.ok(!isPoolSortKey("nonsense"));
  assert.deepEqual(DEFAULT_POOL_SORT, { key: "ourProj", direction: "desc" },
    "the pool's historical default order is preserved");

  // A fresh numeric column opens biggest-first; text opens A-Z.
  assert.deepEqual(nextPoolSort(DEFAULT_POOL_SORT, "value"), { key: "value", direction: "desc" });
  assert.deepEqual(nextPoolSort(DEFAULT_POOL_SORT, "name"), { key: "name", direction: "asc" });
  // Clicking the active column flips it, and flips back.
  const once = nextPoolSort({ key: "value", direction: "desc" }, "value");
  assert.deepEqual(once, { key: "value", direction: "asc" });
  assert.deepEqual(nextPoolSort(once, "value"), { key: "value", direction: "desc" });
  // Every registered column is sortable without throwing.
  for (const { key } of POOL_SORT_COLUMNS) {
    assert.doesNotThrow(() => sortBy([row(), row()], key, "desc"), `${key} sorts`);
  }
}

// ── Nulls sink in BOTH directions ──────────────────────────────────────
{
  const rows = [
    row({ name: "none", linestarProj: null }),
    row({ name: "low", linestarProj: 4 }),
    row({ name: "high", linestarProj: 20 }),
  ];
  assert.deepEqual(names(sortBy(rows, "linestarProj", "desc")), ["high", "low", "none"]);
  assert.deepEqual(names(sortBy(rows, "linestarProj", "asc")), ["low", "high", "none"],
    "ascending must not open with a wall of dashes");
}

// ── FAILURE MODE: a raw value sort promotes rows carrying no claim ─────
{
  // The measured shape of the real slate's top: a ruled-OUT star and a
  // practice-squad quarterback both out-multiply every genuine play.
  const pool = [
    ...Array.from({ length: 20 }, (_, i) => row({ name: `qb${i}`, position: "QB", salary: 6000, ourProj: 12 + i * 0.3 })),
    row({ name: "ajBrown-OUT", position: "WR", salary: 3000, ourProj: 16.1, isOut: true }),
    row({ name: "fagnano-prior", position: "QB", salary: 4000, ourProj: 16.8, projectionStatus: "position_prior" }),
    row({ name: "purdy-real", position: "QB", salary: 6200, ourProj: 25.16 }),
  ];
  const sorted = sortBy(pool, "value", "desc");

  // Raw multiples: A.J. Brown 5.36x, Fagnano 4.20x, Purdy 4.06x. A naive sort
  // would rank them in exactly that order.
  const index = buildValueIndex(pool);
  assert.ok(index.assess(pool.find((r) => r.name === "ajBrown-OUT")!).multiple! > index.assess(pool.find((r) => r.name === "purdy-real")!).multiple!);
  assert.ok(index.assess(pool.find((r) => r.name === "fagnano-prior")!).multiple! > index.assess(pool.find((r) => r.name === "purdy-real")!).multiple!);

  // ...but the real player leads, and both absent-verdict rows are at the end.
  assert.equal(sorted[0].name, "purdy-real", "a real, proven value play leads the sort");
  const tail = names(sorted).slice(-2);
  assert.ok(tail.includes("ajBrown-OUT"), "a ruled-out player cannot top a value sort");
  assert.ok(tail.includes("fagnano-prior"), "a position-prior multiple cannot top a value sort");
}

// ── Absent verdicts sink ASCENDING too — they are not low values ───────
{
  const pool = [
    ...Array.from({ length: 12 }, (_, i) => row({ name: `wr${i}`, salary: 5000, ourProj: 8 + i })),
    row({ name: "out", salary: 3000, ourProj: 16, isOut: true }),
    row({ name: "prior", salary: 4000, ourProj: 16, projectionStatus: "position_prior" }),
  ];
  const asc = names(sortBy(pool, "value", "asc"));
  assert.ok(asc.slice(-2).every((n) => n === "out" || n === "prior"),
    `absent verdicts sink ascending too, got tail ${asc.slice(-2).join(",")}`);
  assert.equal(asc[0], "wr0", "ascending still opens with the genuinely worst tiered value");
}

// ── A sorted column must LOOK sorted: the tiered block is monotonic ────
{
  const pool = [
    ...Array.from({ length: 30 }, (_, i) => row({ name: `a${i}`, position: "RB", salary: 4000 + i * 100, ourProj: 6 + i * 0.7 })),
    ...Array.from({ length: 10 }, (_, i) => row({ name: `p${i}`, position: "RB", salary: 4000, ourProj: 15 + i, projectionStatus: "position_prior" })),
    row({ name: "o1", position: "RB", salary: 3000, ourProj: 20, isOut: true }),
  ];
  const index = buildValueIndex(pool);
  const sorted = sortPlayerPool(pool, { key: "value", direction: "desc" }, (p) => index.assess(p as Row));
  const tiered = sorted.filter((r) => !["unproven", "unknown"].includes(index.assess(r).tier));
  for (let i = 1; i < tiered.length; i += 1) {
    const prev = index.assess(tiered[i - 1]).multiple!;
    const cur = index.assess(tiered[i]).multiple!;
    assert.ok(cur <= prev + 1e-9, `value column must be monotonic: ${prev} then ${cur}`);
  }
  // The sunk block is ordered, not arbitrary.
  const sunk = sorted.filter((r) => index.assess(r).tier === "unproven");
  for (let i = 1; i < sunk.length; i += 1) {
    assert.ok(index.assess(sunk[i]).multiple! <= index.assess(sunk[i - 1]).multiple! + 1e-9,
      "the sunk block keeps its own multiple order");
  }
}

// ── `ourProj` nulls sink, ordered by the DK average behind them ────────
{
  const rows = [
    row({ name: "noProj-lowDk", ourProj: null, avgFptsDk: 3 }),
    row({ name: "real-low", ourProj: 5 }),
    row({ name: "noProj-highDk", ourProj: null, avgFptsDk: 18 }),
    row({ name: "real-high", ourProj: 22 }),
  ];
  assert.deepEqual(names(sortBy(rows, "ourProj", "desc")),
    ["real-high", "real-low", "noProj-highDk", "noProj-lowDk"],
    "rows with no model projection sink, but stay ordered by DK average");
  // Ascending keeps them sunk, and keeps the same internal order.
  assert.deepEqual(names(sortBy(rows, "ourProj", "asc")),
    ["real-low", "real-high", "noProj-highDk", "noProj-lowDk"]);
}

// ── Text columns ───────────────────────────────────────────────────────
{
  const rows = [row({ name: "Zay Flowers" }), row({ name: "aaron rodgers" }), row({ name: "Mike Evans" })];
  assert.deepEqual(names(sortBy(rows, "name", "asc")), ["aaron rodgers", "Mike Evans", "Zay Flowers"],
    "case-insensitive, so a lowercase source name does not sort to the end");
  assert.deepEqual(names(sortBy(rows, "name", "desc")), ["Zay Flowers", "Mike Evans", "aaron rodgers"]);
}

// ── Determinism: equal rows keep a stable, repeatable order ────────────
{
  const rows = [
    row({ name: "tieA", salary: 5000, ourProj: 12 }),
    row({ name: "tieB", salary: 5000, ourProj: 12 }),
    row({ name: "tieC", salary: 5000, ourProj: 12 }),
  ];
  const once = names(sortBy([...rows], "ourProj", "desc"));
  const twice = names(sortBy([...rows].reverse(), "ourProj", "desc"));
  assert.deepEqual(once, twice, "input order cannot change the result");
}

// ── Sorting never drops or duplicates a row ────────────────────────────
{
  const pool = Array.from({ length: 40 }, (_, i) => row({
    name: `r${i}`, position: ["QB", "RB", "WR", "TE", "DST"][i % 5],
    salary: 3000 + (i % 7) * 800, ourProj: i % 3 === 0 ? null : i,
    avgFptsDk: i, isOut: i % 11 === 0,
    projectionStatus: i % 5 === 0 ? "position_prior" : "historical",
  }));
  for (const { key } of POOL_SORT_COLUMNS) {
    for (const direction of ["asc", "desc"] as const) {
      const out = sortBy(pool, key, direction);
      assert.equal(out.length, pool.length, `${key}/${direction} keeps every row`);
      assert.equal(new Set(out.map((r) => r.dkPlayerId)).size, pool.length, `${key}/${direction} has no duplicates`);
    }
  }
}

// ── The input array is not mutated ─────────────────────────────────────
{
  const rows = [row({ name: "b", ourProj: 1 }), row({ name: "a", ourProj: 9 })];
  const before = names(rows);
  sortBy(rows, "ourProj", "desc");
  assert.deepEqual(names(rows), before, "callers keep their own array order");
}

// ── Empty and single-row pools ─────────────────────────────────────────
{
  assert.deepEqual(sortBy([], "value", "desc"), []);
  const one = [row({ name: "solo" })];
  assert.deepEqual(names(sortBy(one, "value", "asc")), ["solo"]);
}

console.log("nfl player pool sort: all assertions passed");
