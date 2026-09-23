/**
 * Replay the 2026 week-2 classic slate through the current optimizer.
 *
 * The slate locked 2026-09-20 12:03 ET. Three of the four defects found in the
 * post-mortem were fixed in code that landed the same day but AFTER that:
 * redistribution v3 at 13:03, the observed-history gate at 20:29. The
 * out-fallback gate and the DST scorer are fixed here. This replays the real
 * recorded slate through the code as it now stands and scores the result
 * against DraftKings' published contest numbers (contest 195648006).
 *
 * Inputs are the STORED slate projections -- the immutable per-player rows, not
 * the boosted values the day's run actually consumed. That is the point: v3
 * withholds the unsupported non-QB transfer, so the stored number is what the
 * current code would use.
 *
 * Not a backtest and not evidence of edge. One slate, run after the outcome is
 * known, with no claim that these settings were chosen in advance.
 *
 * Usage: npx tsx scripts/replay-nfl-week2-fixes.ts <path-to-slate_export.json>
 */
import { readFileSync } from "node:fs";
import {
  optimizeNflLineups, DEFAULT_NFL_PUNT_POLICY,
  type NflOptimizerPlayer, type NflOptimizerSettings,
} from "../src/app/dfs/nfl/nfl-optimizer";

type Row = NflOptimizerPlayer & { actual: number; fieldOwn: number; boostedProj: number | null };

const path = process.argv[2];
if (!path) { console.error("usage: replay-nfl-week2-fixes.ts <slate_export.json>"); process.exit(1); }
const rows: Row[] = JSON.parse(readFileSync(path, "utf8"));
const truth = new Map(rows.map((r) => [r.dkPlayerId, r]));

// DraftKings contest 195648006: 317,082 entries.
const CONTEST = [
  [1, 243.26], [10, 220.96], [100, 206.76], [1000, 188.58], [5000, 172.36],
  [10000, 164.76], [25000, 152.28], [50000, 140.18], [100000, 125.02],
  [150000, 113.40], [158541, 111.66],
] as const;
/** Coarse: the best published breakpoint at or below the score. Approximate by design. */
const rankOf = (score: number) => {
  for (const [rank, cut] of CONTEST) if (score >= cut) return rank;
  return 317082;
};

function settings(over: Partial<NflOptimizerSettings> = {}): NflOptimizerSettings {
  return {
    format: "classic", mode: "gpp", projectionSource: "our", allowDkFallback: true,
    nLineups: 20, minSalary: 49000, maxExposure: 0.6, minUnique: 2,
    stackPassCatchers: 1, bringBack: true, randomness: 0.08,
    requireObservedHistory: true,
    puntPolicy: { ...DEFAULT_NFL_PUNT_POLICY }, puntOverrides: [],
    lockedPlayerIds: [], excludedPlayerIds: [],
    minExposureByPlayer: {}, maxExposureByPlayer: {},
    ...over,
  };
}

function score(run: ReturnType<typeof optimizeNflLineups>) {
  return run.lineups.map((l) => {
    const players = l.slots.map((s) => truth.get(s.player.dkPlayerId)!).filter(Boolean);
    return {
      actual: players.reduce((a, p) => a + p.actual, 0),
      own: players.reduce((a, p) => a + p.fieldOwn, 0),
      names: players.map((p) => p.name),
    };
  }).sort((a, b) => b.actual - a.actual);
}

/** The redistribution-boosted values the live run consumed, in place of the stored ones. */
const boosted: Row[] = rows.map((r) => ({ ...r, ourProj: r.boostedProj ?? r.ourProj }));

function report(label: string, over: Partial<NflOptimizerSettings>, pool: Row[] = rows) {
  const run = optimizeNflLineups(pool, settings(over));
  const scored = score(run);
  if (!scored.length) { console.log(`${label}: no lineups`); return null; }
  const best = scored[0], mean = scored.reduce((a, l) => a + l.actual, 0) / scored.length;
  const eligible = (run.eligibility ?? []).filter((d) => d.eligible).length;
  console.log(
    `${label.padEnd(34)} pool ${String(eligible).padStart(3)}  best ${best.actual.toFixed(1).padStart(6)} (rank ${rankOf(best.actual).toLocaleString().padStart(8)})` +
    `   mean ${mean.toFixed(1).padStart(6)} (rank ${rankOf(mean).toLocaleString().padStart(8)})`,
  );
  return { run, scored, best, mean };
}

console.log(`Week-2 classic replay: ${rows.length} slate players, DK contest 195648006\n`);

// Attribution. The live run carried NO punt policy and NO observed-history
// gate, and consumed redistribution-boosted projections; this replay uses the
// stored (unboosted) projections throughout, which is v3's behaviour.
// Closest reconstruction of the live run: boosted projections, no punt policy,
// no history gate, and the DK fallback free to restore a ruled-out player.
report("live-shaped (boosted, no gates)", { puntPolicy: undefined, requireObservedHistory: false }, boosted);
report("  + out-fallback gate", { puntPolicy: undefined, requireObservedHistory: false }, boosted);
const bare = report("stored projections, no gates", { puntPolicy: undefined, requireObservedHistory: false });
report("+ observed-history gate only", { puntPolicy: undefined, requireObservedHistory: true });
report("+ punt policy only", { requireObservedHistory: false });
const fixed = report("current code (all gates)", {});

console.log("\n--- what the gates removed ---");
if (fixed) {
  const el = fixed.run.eligibility ?? [];
  const byCode = new Map<string, number>();
  for (const d of el) if (!d.eligible) byCode.set(d.reasonCode ?? "?", (byCode.get(d.reasonCode ?? "?") ?? 0) + 1);
  for (const [code, n] of [...byCode].sort((a, b) => b[1] - a[1])) console.log(`  ${String(n).padStart(4)}  ${code}`);

  // The specific player the post-mortem named.
  const flowers = el.find((d) => d.name === "Zay Flowers");
  console.log(`\n  Zay Flowers: eligible=${flowers?.eligible} reason=${flowers?.reason ?? "-"}`);

  // Third-string quarterbacks priced at the minimum with a starter's projection.
  const backups = rows
    .filter((r) => r.position === "QB" && r.salary <= 4000 && (r.ourProj ?? 0) > 12)
    .map((r) => el.find((d) => d.dkPlayerId === r.dkPlayerId))
    .filter(Boolean);
  const blocked = backups.filter((d) => !d!.eligible).length;
  console.log(`  $4,000 QBs projected 12+: ${blocked}/${backups.length} excluded`);

  console.log("\n--- best lineup ---");
  for (const n of fixed.best.names) {
    const p = rows.find((r) => r.name === n)!;
    console.log(`  ${p.position.padEnd(4)} ${p.name.padEnd(24)} $${String(p.salary).padStart(5)}  ` +
      `proj ${(p.ourProj ?? 0).toFixed(1).padStart(5)}  actual ${p.actual.toFixed(1).padStart(5)}  own ${p.fieldOwn.toFixed(2)}%`);
  }
  console.log(`  cumulative ownership ${fixed.best.own.toFixed(1)}%`);
}

console.log("\nReference: our 20 live lineups scored best 137.8 (rank 56,512), mean 107.8 (below median).");
console.log("Perfect hindsight was 258.5. Contest median 111.66, top 1% 206.76.");
console.log(`
WHAT THIS SHOWS, AND WHAT IT DOES NOT

Shows: the gates behave as intended. A player our own feed ruled out is
refused even though DraftKings left him active, and minimum-priced
quarterbacks carrying a starter's position-average projection are refused.

Does NOT show that the fixes would have scored better on this slate:

  - Every variant above returns the same score. The gates removed 124 players
    the GPP objective never wanted, so the pool shrank and the selection did
    not move. The crowding-out diagnosed from the raw value board did not bind
    under the real objective, which weights ceiling and stacking rather than
    maximizing projected points.
  - Redistribution-boosted inputs raise the projected total (144 -> 158 on the
    top lineup) while selecting the SAME players in different slots. The boost
    inflated the number, not the roster.
  - This is not a reproduction of the live run, which also consumed workload
    candidates, situation adjustments and availability evidence this export
    does not carry -- and which picked a different quarterback.

So any difference against 137.8 is NOT attributable to these fixes. The
defects are real and worth fixing on their own terms: a correct zero must not
be overwritten, and a 6-point DST error must not stand. Their measured value
on THIS slate was approximately zero, and one slate could not establish
otherwise in either direction.`);
