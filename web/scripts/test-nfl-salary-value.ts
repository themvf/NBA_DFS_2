import assert from "node:assert/strict";
import {
  ABSOLUTE_VALUE_FLOOR, CLASSIC_ROSTER_SIZE, MIN_POOL_FOR_TIER, VALUE_TIER_LABEL,
  buildValueIndex, rosterPointsAtMultiple, salaryMultiple,
  type ValuePlayerInput,
} from "../src/lib/nfl-dfs/salary-value";
import { DK_SALARY_CAP } from "../src/lib/nfl-dfs/dk-salary-csv";

const p = (over: Partial<ValuePlayerInput> & { position: string; salary: number; ourProj: number | null }): ValuePlayerInput =>
  ({ projectionStatus: "historical", isOut: false, ...over });
/** A pool of `n` players at one position spanning multiples `lo`..`hi`. */
const spread = (position: string, n: number, lo: number, hi: number, over: Partial<ValuePlayerInput> = {}) =>
  Array.from({ length: n }, (_, i) => {
    const multiple = n === 1 ? lo : lo + ((hi - lo) * i) / (n - 1);
    return p({ position, salary: 5000, ourProj: multiple * 5, ...over });
  });

// ── The multiple itself ────────────────────────────────────────────────
{
  // $5,000 and 15 points is 3.0 points per $1K.
  assert.equal(salaryMultiple(15, 5000), 3);
  assert.equal(salaryMultiple(24.89, 7200)!.toFixed(2), "3.46"); // Derrick Henry, real slate row
  assert.equal(salaryMultiple(null, 5000), null);
  assert.equal(salaryMultiple(15, 0), null, "zero salary cannot divide");
  assert.equal(salaryMultiple(15, -100), null);
  assert.equal(salaryMultiple(Number.NaN, 5000), null);
  assert.equal(salaryMultiple(0, 5000), 0, "a real zero projection is 0x, not unknown");
}

// ── The floor is cap arithmetic, not a preference ──────────────────────
{
  assert.equal(DK_SALARY_CAP, 50_000);
  assert.equal(CLASSIC_ROSTER_SIZE, 9);
  // A whole roster at the floor multiple scores this much. If that number ever
  // stops being obviously uncompetitive, the floor is wrong.
  assert.equal(rosterPointsAtMultiple(ABSOLUTE_VALUE_FLOOR), 100);
  assert.equal(rosterPointsAtMultiple(3), 150);
}

// ── FAILURE MODE 1: a flat bar is a quarterback detector ───────────────
/**
 * Measured on a real 13-game DK Classic slate, 567 usable players, BEFORE the
 * thresholds in `salary-value.ts` were chosen. These are observations, not
 * fixture parameters -- they are the reason the rule is per position.
 */
type Measured = { n: number; p25: number; median: number; p75: number; p90: number; max: number; atFlat3x: number };
const MEASURED: Record<"QB" | "RB", Measured> = {
  QB: { n: 83, p25: 2.03, median: 2.94, p75: 3.38, p90: 3.82, max: 4.20, atFlat3x: 40 },
  RB: { n: 128, p25: 0.53, median: 1.40, p75: 2.13, p90: 2.36, max: 3.46, atFlat3x: 2 },
};

{
  const shareAtFlatBar = (m: Measured) => m.atFlat3x / m.n;
  // 48% of quarterbacks against 1.6% of running backs: a flat 3x bar sorts by
  // position, not by value. If a future slate ever stops showing this, the
  // per-position rule is still safe -- but the justification below is stale.
  assert.ok(
    shareAtFlatBar(MEASURED.QB) > 20 * shareAtFlatBar(MEASURED.RB),
    `flat-3x share QB ${(100 * shareAtFlatBar(MEASURED.QB)).toFixed(0)}% vs RB ${(100 * shareAtFlatBar(MEASURED.RB)).toFixed(1)}%`,
  );
}

{
  // Rebuild each position's real shape by inverse-transform sampling its
  // measured quantiles, so the pools carry the true right skew rather than a
  // uniform spread that would flatter running backs.
  const fromQuantiles = (position: string, n: number, m: Measured): ValuePlayerInput[] => {
    const anchors: [number, number][] = [
      [0, Math.max(0.1, m.p25 / 2)], [0.25, m.p25], [0.5, m.median],
      [0.75, m.p75], [0.9, m.p90], [1, m.max],
    ];
    return Array.from({ length: n }, (_, i) => {
      const u = i / (n - 1);
      let k = 0;
      while (k < anchors.length - 2 && u > anchors[k + 1][0]) k += 1;
      const [u0, v0] = anchors[k];
      const [u1, v1] = anchors[k + 1];
      const multiple = v0 + ((v1 - v0) * (u - u0)) / (u1 - u0);
      return p({ position, salary: 5000, ourProj: multiple * 5 });
    });
  };

  const pool = [...fromQuantiles("QB", MEASURED.QB.n, MEASURED.QB), ...fromQuantiles("RB", MEASURED.RB.n, MEASURED.RB)];
  const index = buildValueIndex(pool);
  const share = (position: string) => {
    const group = pool.filter((x) => x.position === position);
    const tiered = group.filter((x) => ["elite", "strong"].includes(index.assess(x).tier));
    return tiered.length / group.length;
  };

  // The whole point: two positions priced very differently surface a
  // comparable share of value plays.
  assert.ok(Math.abs(share("QB") - share("RB")) < 0.05,
    `QB ${(100 * share("QB")).toFixed(0)}% vs RB ${(100 * share("RB")).toFixed(0)}% should be comparable`);
  assert.ok(share("RB") > 0.15, "running backs must be able to reach a value tier at all");
  // And a real, correctly-priced running back reaches it while a flat 3x bar
  // would have rejected him. Derrick Henry, $7,200, 24.89 projected = 3.46x
  // was the single best RB multiple on that slate.
  const henry = p({ position: "RB", salary: 7200, ourProj: 24.89 });
  assert.equal(buildValueIndex([...pool, henry]).assess(henry).tier, "elite");
}

// ── FAILURE MODE 2: a position prior is not evidence of value ──────────
{
  // A min-salary player carrying the position average returns ~4x by
  // construction. That must never outrank a real, proven player.
  const proven = spread("QB", 20, 2.0, 3.4);
  const fakeStar = p({ position: "QB", salary: 4000, ourProj: 16.8, projectionStatus: "position_prior" });
  const index = buildValueIndex([...proven, fakeStar]);

  const verdict = index.assess(fakeStar);
  assert.equal(verdict.tier, "unproven");
  assert.ok(verdict.multiple! > 4, "its multiple is still computed and shown, not hidden");
  assert.match(verdict.reason, /position peers/, "the reason names what the number rests on");

  // And it must not contaminate the bar its own position is graded against.
  assert.equal(index.references.get("QB")!.count, proven.length, "prior rows are excluded from the reference");
  const withoutFake = buildValueIndex(proven).references.get("QB")!;
  assert.equal(index.references.get("QB")!.p90, withoutFake.p90, "p90 is unmoved by the prior row");
}

// ── The absolute floor stops a bad slate crowning its least-bad option ──
{
  // Every player here is under 2x. Top-decile rank alone would still hand out
  // an "elite" chip; the conjunctive floor must prevent it.
  const pool = spread("WR", 30, 0.4, 1.9);
  const index = buildValueIndex(pool);
  const best = pool[pool.length - 1];
  const verdict = index.assess(best);
  assert.ok(verdict.multiple! < ABSOLUTE_VALUE_FLOOR);
  assert.notEqual(verdict.tier, "elite", "rank alone cannot manufacture value on a badly priced slate");
  assert.notEqual(verdict.tier, "strong");

  // Raise the same pool above the floor and the top of it does qualify.
  const healthy = buildValueIndex(spread("WR", 30, 1.5, 3.2));
  const top = spread("WR", 30, 1.5, 3.2)[29];
  assert.equal(healthy.assess(top).tier, "elite");
}

// ── Tier boundaries ────────────────────────────────────────────────────
{
  const pool = spread("TE", 40, 1.0, 3.5);
  const index = buildValueIndex(pool);
  const ref = index.references.get("TE")!;
  const at = (multiple: number) => index.assess(p({ position: "TE", salary: 5000, ourProj: multiple * 5 })).tier;

  assert.equal(at(ref.p90 + 0.01), "elite");
  assert.equal(at(ref.p75 + 0.01), "strong");
  assert.equal(at(ref.median), "fair");
  assert.equal(at(ref.p25 - 0.01), "poor");
  // Boundaries are inclusive on the way up.
  assert.equal(at(ref.p90), "elite");
  assert.equal(at(ref.p75), "strong");
}

// ── Small pools refuse to rank rather than inventing a quantile ────────
{
  const tiny = spread("DST", MIN_POOL_FOR_TIER - 1, 1.5, 3.5);
  const index = buildValueIndex(tiny);
  const verdict = index.assess(tiny[tiny.length - 1]);
  assert.equal(verdict.tier, "unknown");
  assert.ok(verdict.multiple !== null, "the raw multiple survives even with no tier");
  assert.match(verdict.reason, /too few/);

  const enough = spread("DST", MIN_POOL_FOR_TIER, 1.5, 3.5);
  assert.notEqual(buildValueIndex(enough).assess(enough[enough.length - 1]).tier, "unknown");
}

// ── OUT players carry no value read ────────────────────────────────────
{
  const pool = spread("RB", 20, 1.0, 3.0);
  const out = p({ position: "RB", salary: 4000, ourProj: 14, isOut: true });
  const index = buildValueIndex([...pool, out]);
  assert.equal(index.assess(out).tier, "unknown");
  assert.equal(index.references.get("RB")!.count, pool.length, "an OUT row never anchors the reference");
}

// ── Ceiling multiple is reported alongside, never instead ──────────────
{
  const pool = spread("WR", 20, 1.2, 3.0);
  const player = p({ position: "WR", salary: 6700, ourProj: 18.67, ceilingFpts: 33.76 }); // Zay Flowers, real row
  const v = buildValueIndex([...pool, player]).assess(player);
  assert.equal(v.multiple!.toFixed(2), "2.79");
  assert.equal(v.ceilingMultiple!.toFixed(2), "5.04");

  const noCeiling = p({ position: "WR", salary: 6700, ourProj: 18.67 });
  assert.equal(buildValueIndex(pool).assess(noCeiling).ceilingMultiple, null);
}

// ── An unknown position degrades rather than throwing ──────────────────
{
  const index = buildValueIndex(spread("WR", 20, 1.2, 3.0));
  const oddity = p({ position: "P", salary: 3000, ourProj: 9 });
  const v = index.assess(oddity);
  assert.equal(v.tier, "unknown");
  assert.equal(v.multiple, 3);
  assert.equal(v.referenceCount, 0);
}

// ── Every tier has a label ─────────────────────────────────────────────
{
  for (const tier of ["elite", "strong", "fair", "poor", "unproven", "unknown"] as const) {
    assert.ok(VALUE_TIER_LABEL[tier]?.length > 0, `${tier} has a label`);
  }
}

// ── Empty pool ─────────────────────────────────────────────────────────
{
  const index = buildValueIndex([]);
  assert.equal(index.references.size, 0);
  assert.equal(index.assess(p({ position: "QB", salary: 5000, ourProj: 20 })).tier, "unknown");
}

console.log("nfl salary value: all assertions passed");
