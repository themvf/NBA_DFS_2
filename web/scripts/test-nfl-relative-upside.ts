import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import {
  GPP_CEILING_CAP_MULTIPLE, MIN_SHAPE_PROJECTION, RELATIVE_UPSIDE_VERSION, STARTERS_PER_TEAM,
  relativeUpside, starterBaselines, uncreditedCeiling, upsideShape, weeklySpread,
  type UpsidePlayer,
} from "../src/lib/nfl-dfs/relative-upside";

let id = 0;
const p = (over: Partial<UpsidePlayer>): UpsidePlayer => {
  id += 1;
  return { dkPlayerId: id, position: "WR", team: "T1", ourProj: 10, ceilingFpts: 20, projectionStatus: "historical", ...over };
};

// ── Constants are pinned; changing one is a new version ─────────────────
assert.equal(RELATIVE_UPSIDE_VERSION, "nfl-upside-display-v1");
assert.deepEqual({ ...STARTERS_PER_TEAM }, { QB: 1, RB: 1.5, WR: 2.5, TE: 1, DST: 1, K: 1 });

// ── Starter count follows the slate's teams, rounded half up ────────────
{
  const teams = ["A", "B", "C", "D"];
  const players: UpsidePlayer[] = [];
  for (const team of teams) {
    for (let i = 0; i < 4; i += 1) players.push(p({ team, position: "RB", ourProj: 20 - i * 4, ceilingFpts: 40 - i * 8 }));
  }
  const rb = starterBaselines(players).get("RB")!;
  assert.equal(rb.teams, 4);
  assert.equal(rb.wanted, 6, "4 teams x 1.5 = 6");
  assert.equal(rb.starters, 6);
  // top 6 by projection: four 20s and two 16s
  assert.equal(rb.avgProjection, (4 * 20 + 2 * 16) / 6);
  assert.equal(rb.avgP90, (4 * 40 + 2 * 32) / 6);
}

// ── Out, position-prior and missing-range players never set the baseline ──
{
  const players = [
    p({ team: "A", position: "QB", ourProj: 30, ceilingFpts: 60, isOut: true }),
    p({ team: "B", position: "QB", ourProj: 25, ceilingFpts: 50, projectionStatus: "position_prior" }),
    p({ team: "C", position: "QB", ourProj: 22, ceilingFpts: null }),
    p({ team: "D", position: "QB", ourProj: 20, ceilingFpts: 36 }),
    p({ team: "E", position: "QB", ourProj: 18, ceilingFpts: 30 }),
  ];
  const qb = starterBaselines(players).get("QB")!;
  assert.equal(qb.starters, 2);
  assert.equal(qb.avgP90, 33);
  // an out player gets no number at all
  const out = relativeUpside(players[0], new Map([["QB", qb]]));
  assert.equal(out.vsStarterP90, null);
  assert.equal(out.shape, null);
}

// ── A position with fewer than two eligible starters has no baseline ─────
{
  const players = [p({ team: "A", position: "TE" }), p({ team: "B", position: "QB" })];
  assert.equal(starterBaselines(players).has("TE"), false);
  const r = relativeUpside(players[0], starterBaselines(players));
  assert.equal(r.vsStarterP90, null);
  assert.equal(r.baseline, null);
  assert.equal(r.shape, "normal", "shape needs no baseline");
}

// ── Difference and shape ─────────────────────────────────────────────────
{
  const players = [
    p({ team: "A", position: "WR", ourProj: 16, ceilingFpts: 30 }),
    p({ team: "B", position: "WR", ourProj: 14, ceilingFpts: 26 }),
    p({ team: "C", position: "WR", ourProj: 6, ceilingFpts: 20 }),
  ];
  const base = starterBaselines(players);
  const spiky = relativeUpside(players[2], base);
  // 3 teams x 2.5 = 7.5 -> 8 wanted, 3 found, average P90 = 25.333
  assert.equal(spiky.vsStarterP90, Math.round((20 - 76 / 3) * 10) / 10);
  assert.equal(spiky.shape, "spiky");
  assert.equal(spiky.shapeRatio, 3.33);
}
assert.equal(upsideShape(1.59), "steady");
assert.equal(upsideShape(1.6), "normal");
assert.equal(upsideShape(2.2), "normal");
assert.equal(upsideShape(2.21), "spiky");
assert.equal(upsideShape(null), null);
// a near-zero projection gets no ratio
assert.equal(relativeUpside(p({ ourProj: MIN_SHAPE_PROJECTION - 0.1, ceilingFpts: 9 }), new Map()).shapeRatio, null);

// ── Ceiling the GPP objective does not credit at its cap ─────────────────
assert.equal(uncreditedCeiling(p({ ourProj: 5.5, ceilingFpts: 30.9 }), 2.5), 17.2);
assert.equal(uncreditedCeiling(p({ ourProj: 15, ceilingFpts: 30 }), 2.5), 0);
assert.equal(uncreditedCeiling(p({ ourProj: null, ceilingFpts: 30 }), 2.5), null);

// ── Week-to-week spread: absent weeks are not zeros; starters need 3 games ──
{
  const rows = [
    { playerId: 1, week: 1, points: 10 }, { playerId: 1, week: 2, points: 30 }, { playerId: 1, week: 4, points: 20 },
    { playerId: 2, week: 1, points: 15 }, { playerId: 2, week: 2, points: 15 }, { playerId: 2, week: 3, points: 15 },
    { playerId: 3, week: 1, points: 40 }, { playerId: 3, week: 2, points: 2 }, // only 2 games: not a starter
  ];
  const s = weeklySpread(1, "QB", rows);
  assert.equal(s.games, 3);
  assert.equal(s.mean, 20);
  assert.equal(s.sd, 10);
  assert.deepEqual(s.weeks.map((w) => w.week), [1, 2, 4]);
  assert.equal(s.starters, 2);
  assert.equal(s.starterSd, 5, "mean of 10 and 0");
  assert.equal(s.starterMean, 17.5);
  const none = weeklySpread(99, "QB", rows);
  assert.equal(none.games, 0);
  assert.equal(none.sd, null);
  assert.equal(none.mean, null);
}

// ── Display only: the optimizer never reads this module, and the drawer's cap matches its default ──
{
  const optimizer = readFileSync(new URL("../src/app/dfs/nfl/nfl-optimizer.ts", import.meta.url), "utf8");
  assert.ok(!optimizer.includes("relative-upside"), "the optimizer must not import the display-only upside module");
  const cap = optimizer.match(/export const DEFAULT_MAX_CEILING_MULTIPLE = ([0-9.]+);/);
  assert.ok(cap, "DEFAULT_MAX_CEILING_MULTIPLE not found in nfl-optimizer.ts");
  assert.equal(Number(cap[1]), GPP_CEILING_CAP_MULTIPLE);
}

console.log("relative-upside: all assertions passed");
