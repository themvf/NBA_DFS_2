import assert from "node:assert/strict";
import {
  DELTA_FILL, deltaBucket, deltaScales, delta, isScored,
  matchesReviewPosition, topMovers, REVIEW_POSITIONS,
} from "../src/lib/nfl-dfs/review-insights";
import type { ReportRow } from "../src/lib/nfl-dfs/report-card";

let nextId = 1;
const row = (over: Partial<ReportRow> & { position: string }): ReportRow => ({
  player_id: nextId++, name: `P${nextId}`, team: "GB", opponent: "MIN",
  actual: null, error: null, ...over,
} as ReportRow);
/** A scored row: actual present and error = actual - projected. */
const scored = (position: string, d: number, name?: string) =>
  row({ position, actual: 10 + d, error: d, ...(name ? { name } : {}) });

// ── Position filter, including FLEX ────────────────────────────────────
{
  assert.deepEqual([...REVIEW_POSITIONS], ["ALL", "QB", "RB", "WR", "TE", "FLEX", "DST"]);
  for (const p of ["QB", "RB", "WR", "TE", "DST"]) assert.ok(matchesReviewPosition(p, "ALL"), `ALL keeps ${p}`);
  for (const p of ["RB", "WR", "TE"]) assert.ok(matchesReviewPosition(p, "FLEX"), `FLEX includes ${p}`);
  assert.ok(!matchesReviewPosition("QB", "FLEX"), "FLEX excludes QB");
  assert.ok(!matchesReviewPosition("DST", "FLEX"), "FLEX excludes DST");
  assert.ok(matchesReviewPosition("WR", "WR") && !matchesReviewPosition("WR", "RB"), "exact match");
}

// ── Unscored rows never rank ───────────────────────────────────────────
{
  const pending = row({ position: "QB", actual: null, error: null });
  assert.equal(delta(pending), null);
  assert.ok(!isScored(pending));
  const { exceeded, disappointed } = topMovers([pending, scored("QB", 5)]);
  assert.equal(exceeded.length, 1, "only the scored row ranks");
  assert.equal(disappointed.length, 0);

  // A row with an error but no actual is NOT scored — a stale error must not
  // resurrect a pending player into the leaderboard.
  const stale = row({ position: "QB", actual: null, error: -9 });
  assert.equal(delta(stale), null, "no actual means no delta, whatever error holds");
  assert.equal(topMovers([stale]).disappointed.length, 0);
}

// ── Lists are split by sign, so they cannot overlap ─────────────────────
{
  const rows = [scored("WR", 3), scored("WR", -4), scored("WR", 0)];
  const { exceeded, disappointed } = topMovers(rows, 10);
  assert.equal(exceeded.length, 1);
  assert.equal(disappointed.length, 1);
  const ids = new Set([...exceeded, ...disappointed].map(r => r.player_id));
  assert.equal(ids.size, 2, "no player appears in both lists");
  assert.ok(![...exceeded, ...disappointed].some(r => delta(r) === 0), "a zero delta is in neither list");
}

// ── Ordering: biggest first in each direction, capped at the limit ──────
{
  const rows = [scored("RB", 1), scored("RB", 9), scored("RB", 5), scored("RB", -2), scored("RB", -8)];
  const { exceeded, disappointed } = topMovers(rows, 2);
  assert.deepEqual(exceeded.map(r => delta(r)), [9, 5], "exceeded descends");
  assert.deepEqual(disappointed.map(r => delta(r)), [-8, -2], "disappointed ascends");
}

// ── Ties break deterministically by name ───────────────────────────────
{
  const rows = [scored("TE", 4, "Zeta"), scored("TE", 4, "Alpha")];
  assert.deepEqual(topMovers(rows).exceeded.map(r => r.name), ["Alpha", "Zeta"]);
}

// ── Scales are per position, from scored rows only ─────────────────────
{
  const scales = deltaScales([
    scored("QB", 20), scored("QB", -6), scored("TE", 5),
    row({ position: "TE", actual: null, error: null }),
  ]);
  assert.equal(scales.QB, 20, "QB scale is the largest magnitude, either sign");
  assert.equal(scales.TE, 5, "TE scale is unaffected by QB's larger swings");
  assert.equal(scales.DST, undefined, "a position with no scored rows has no scale");
}

// ── Buckets ────────────────────────────────────────────────────────────
{
  assert.equal(deltaBucket(20, 20), 2, "full-scale positive is the strong arm");
  assert.equal(deltaBucket(-20, 20), -2, "full-scale negative mirrors it");
  assert.equal(deltaBucket(6, 20), 1, "0.30 of scale is the weak arm");
  assert.equal(deltaBucket(2, 20), 0, "0.10 of scale is neutral");
  assert.equal(deltaBucket(0, 20), 0);

  // The same delta shades differently by position — the whole point of
  // per-position scaling.
  assert.equal(deltaBucket(3, 20), 0, "+3 is unremarkable on the QB scale (0.15)");
  assert.equal(deltaBucket(3, 5), 2, "the same +3 is a top week on the TE scale (0.60)");

  // Degenerate scales must not throw or shade.
  for (const bad of [undefined, 0, -1, NaN]) assert.equal(deltaBucket(5, bad as number), 0, `scale ${bad} does not shade`);
  assert.equal(deltaBucket(null, 20), 0, "no delta, no shade");
  assert.equal(deltaBucket(NaN, 20), 0);
  // Fails closed: a non-finite delta is bad data, and painting it as the
  // week's most extreme result would be worse than not painting it.
  assert.equal(deltaBucket(Infinity, 20), 0, "a non-finite delta does not shade");
}

// ── Fill map: neutral is unpainted, arms are distinct ──────────────────
{
  assert.equal(DELTA_FILL[0], undefined, "neutral leaves the surface alone");
  const arms = [DELTA_FILL[-2], DELTA_FILL[-1], DELTA_FILL[1], DELTA_FILL[2]];
  assert.equal(new Set(arms).size, 4, "every non-neutral bucket has its own fill");
  assert.ok(arms.every(c => /^#[0-9a-f]{6}$/.test(c!)), "fills are plain hex");
}

console.log("NFL weekly-review insight checks passed");
