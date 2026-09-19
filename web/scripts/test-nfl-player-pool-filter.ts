import assert from "node:assert/strict";
import {
  POOL_POSITION_FILTERS, POOL_FILTER_LABELS, isPoolPositionFilter,
  matchesPoolPosition, poolPositionCounts,
} from "../src/lib/nfl-dfs/player-pool-filter";
import { NFL_FLEX_POSITIONS } from "../src/lib/nfl-dfs/dk-salary-csv";

// ── Option list ────────────────────────────────────────────────────────
{
  assert.deepEqual([...POOL_POSITION_FILTERS], ["ALL", "QB", "RB", "WR", "TE", "FLEX", "K", "DST"]);
  assert.ok(isPoolPositionFilter("FLEX"));
  assert.ok(!isPoolPositionFilter("CPT"), "unknown filter rejected");
}

// ── FLEX is RB/WR/TE, and says so in its own label ─────────────────────
{
  for (const p of NFL_FLEX_POSITIONS) assert.ok(matchesPoolPosition(p, "FLEX"), `FLEX includes ${p}`);
  for (const p of ["QB", "K", "DST"]) assert.ok(!matchesPoolPosition(p, "FLEX"), `FLEX excludes ${p}`);
  // The label must name the rule, so a Showdown reader is not misled about
  // DK's slots (where every player is FLEX-eligible).
  for (const p of NFL_FLEX_POSITIONS) {
    assert.ok(POOL_FILTER_LABELS.FLEX.includes(p), `FLEX label names ${p}`);
  }
}

// ── FLEX draws from the optimizer's own rule, not a second copy ─────────
{
  // If someone changes the DK FLEX slot definition, this filter must follow.
  const flexed = ["QB", "RB", "WR", "TE", "K", "DST"].filter((p) => matchesPoolPosition(p, "FLEX"));
  assert.deepEqual(flexed, [...NFL_FLEX_POSITIONS]);
}

// ── ALL and exact positions ────────────────────────────────────────────
{
  for (const p of ["QB", "RB", "WR", "TE", "K", "DST"]) assert.ok(matchesPoolPosition(p, "ALL"), `ALL keeps ${p}`);
  assert.ok(matchesPoolPosition("WR", "WR") && !matchesPoolPosition("WR", "RB"), "exact match");
}

// ── Counts ─────────────────────────────────────────────────────────────
{
  const pool = [
    { position: "QB" }, { position: "QB" },
    { position: "RB" }, { position: "RB" }, { position: "RB" },
    { position: "WR" }, { position: "WR" }, { position: "WR" }, { position: "WR" },
    { position: "TE" },
    { position: "K" },
    { position: "DST" }, { position: "DST" },
  ];
  const counts = poolPositionCounts(pool);
  assert.equal(counts.ALL, 13);
  assert.equal(counts.QB, 2);
  assert.equal(counts.RB, 3);
  assert.equal(counts.WR, 4);
  assert.equal(counts.TE, 1);
  assert.equal(counts.K, 1);
  assert.equal(counts.DST, 2);
  assert.equal(counts.FLEX, 8, "FLEX = RB + WR + TE, counted once each");
  // FLEX overlaps RB/WR/TE by design; it is not a disjoint bucket, so the
  // per-position counts must NOT sum to ALL once FLEX is included.
  assert.equal(counts.RB + counts.WR + counts.TE, counts.FLEX);
  assert.equal(poolPositionCounts([]).ALL, 0, "empty pool");
}

console.log("nfl player pool filter: all assertions passed");
