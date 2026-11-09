/**
 * Replay saved 2026 Showdown builds under ceiling scoring and simulated-game
 * scoring (`nfl-lineup-draws-v1`), with and without the v2 absence mixture,
 * and place every lineup in the contest's real field.
 *
 * Usage: replay-nfl-draw-portfolio.ts <replay_input.json> [seeds=3] [out.json]
 *   replay_input.json comes from research/nfl_draw_portfolio_replay_export.py:
 *   per slate the latest saved GPP run (settings + input_snapshot), actual DK
 *   points by DraftKings player id, the largest imported contest's score curve
 *   and, for v1 absence adjustments that moved a player up, the absent
 *   starter's pre-availability range.
 *
 * Every build keeps the saved run's own settings; only the scoring changes.
 * When today's optimizer refuses a saved run's settings (a rule added since,
 * such as a captain minimum on a player the history gate now blocks), the
 * same relaxation is applied to every variant on that slate and printed.
 * Actual points are read only to score finished builds. A player with no
 * recorded points counts 0 and is listed. The field is the largest contest
 * imported for the slate; percentiles are the share of its entries a lineup
 * beat. Descriptive: one slate a week is a small sample, said in the output.
 */
import { readFileSync, writeFileSync } from "node:fs";
import { optimizeNflLineups, type NflOptimizerPlayer, type NflOptimizerSettings } from "../src/app/dfs/nfl/nfl-optimizer";
import { restorePlayers, type FrozenPlayer } from "../src/lib/nfl-dfs/frozen-players";
import { BOOM_THRESHOLDS, mixDistributions } from "../src/lib/nfl-dfs/replacement-upside";
import { calibratePlayers, type CalibrationParts } from "../src/lib/nfl-dfs/range-calibration";
import type { DrawCorrelations } from "../src/lib/nfl-dfs/lineup-draws";
import type { BuildShape } from "../src/lib/nfl-dfs/winners-shape";

type Range = { mean: number; p10: number; median: number; p90: number; boom: number | null; position?: string };
type Slate = {
  upload: string; contest: string; contestName: string; entries: number; week: number; curve: [number, number][];
  runId: string; settings: NflOptimizerSettings; players: FrozenPlayer[]; points: Record<string, number>;
  missingPoints: string[]; starters: Record<string, Range>;
};

const slates = (JSON.parse(readFileSync(process.argv[2], "utf8")) as Slate[]).filter((s) => s.entries >= 1000 && s.curve?.length);
const seeds = Number(process.argv[3] ?? 3);
const outPath = process.argv[4];

/** v2 absence mixture on a frozen v1 snapshot: same mean, mixture shape. */
function applyMixture(players: NflOptimizerPlayer[], starters: Record<string, Range>): string[] {
  const changed: string[] = [];
  for (const p of players as (NflOptimizerPlayer & { absenceCommit?: { rule: string; scale?: number; from?: string[]; before: Range } })[]) {
    const c = p.absenceCommit;
    if (!c || c.rule !== "own_role_games" || !((c.scale ?? 0) > 1)) continue;
    const st = (c.from ?? []).map((n) => starters[n]).filter(Boolean).sort((a, b) => b.mean - a.mean)[0];
    const target = p.ourProj ?? 0;
    if (!st || !(st.mean > target)) continue;
    const pi = (target - c.before.mean) / (st.mean - c.before.mean);
    const m = mixDistributions(c.before, { ...st, boom: st.position === p.position ? st.boom : null }, pi, BOOM_THRESHOLDS[p.position] ?? null);
    Object.assign(p, { floorFpts: m.p10, medianFpts: m.median, ceilingFpts: m.p90, boomRate: m.boom ?? p.boomRate });
    changed.push(`${p.name}: median ${c.before.median}->${m.median.toFixed(2)}, P90 ${(c.before.p90 * (c.scale ?? 1)).toFixed(2)}->${m.p90.toFixed(2)} (pi ${pi.toFixed(2)})`);
  }
  return changed;
}

/** Entries that beat score s, read off the (rank, score) curve; ties count as not beaten. */
function rankOf(curve: [number, number][], s: number): number {
  let lo = 0, hi = curve.length - 1, r = 0;
  while (lo <= hi) { const mid = (lo + hi) >> 1; if (curve[mid][1] > s + 1e-9) { r = curve[mid][0]; lo = mid + 1; } else hi = mid - 1; }
  return r + 1;
}

/** `calibrate`: nfl-range-calibration-v1 on top of whatever main does today (the mixture when there is one). */
type Variant = { name: string; scoring: "ceiling" | "draws"; mixture: boolean; seed?: number; calibrate?: boolean; parts?: CalibrationParts;
  correlations?: DrawCorrelations; shape?: BuildShape };
/** REPLAY_SET=corr: today's main (calibrated draws, v1 stated correlations, Showdown default shape) against fitted correlations and the winners shape, 2x2. */
const corrSet = process.env.REPLAY_SET === "corr";
/** REPLAY_CORR picks the fitted table the corr families use: `fitted` (overall) or `fitted-tail` (joint booms). */
const corrSource = (process.env.REPLAY_CORR ?? "fitted") as DrawCorrelations;
const corrVariants: Variant[] = ([["main", "stated-v1", "standard"], ["corr", corrSource, "standard"], ["shape", "stated-v1", "winners"], ["corr+shape", corrSource, "winners"]] as const)
  .flatMap(([name, correlations, shape]) => Array.from({ length: seeds }, (_, i) => ({ name: `${name}#${i + 1}`, scoring: "draws" as const, mixture: true, calibrate: true, correlations, shape, seed: 20261103 + i })));
const variants: Variant[] = corrSet ? [{ name: "ceiling", scoring: "ceiling", mixture: false }, ...corrVariants] : [
  { name: "ceiling", scoring: "ceiling", mixture: false },
  { name: "ceiling+mix", scoring: "ceiling", mixture: true },
  ...Array.from({ length: seeds }, (_, i) => ({ name: `draws#${i + 1}`, scoring: "draws" as const, mixture: false, seed: 20261103 + i })),
  ...Array.from({ length: seeds }, (_, i) => ({ name: `draws+mix#${i + 1}`, scoring: "draws" as const, mixture: true, seed: 20261103 + i })),
  ...Array.from({ length: seeds }, (_, i) => ({ name: `cal#${i + 1}`, scoring: "draws" as const, mixture: true, calibrate: true, seed: 20261103 + i })),
  // Ablation (REPLAY_PARTS=1): each correction alone.
  ...(process.env.REPLAY_PARTS
    ? ([["cal-notplay", { meanLine: false }], ["cal-meanline", { notPlay: false }]] as const)
      .flatMap(([name, parts]) => Array.from({ length: seeds }, (_, i) => ({ name: `${name}#${i + 1}`, scoring: "draws" as const, mixture: true, calibrate: true, parts, seed: 20261103 + i })))
    : []),
];

/** Relaxations tried in order when the saved settings no longer build. */
const RELAXATIONS: { label: string; apply: (s: NflOptimizerSettings) => NflOptimizerSettings }[] = [
  { label: "as saved", apply: (s) => s },
  { label: "user exposure ranges dropped", apply: (s) => ({ ...s, exposurePolicies: [], minExposureByPlayer: {}, maxExposureByPlayer: {} }) },
  { label: "user ranges dropped, observed-history gate off", apply: (s) => ({ ...s, exposurePolicies: [], minExposureByPlayer: {}, maxExposureByPlayer: {}, requireObservedHistory: false }) },
  { label: "user ranges dropped, history gate off, Our projections", apply: (s) => ({ ...s, exposurePolicies: [], minExposureByPlayer: {}, maxExposureByPlayer: {}, requireObservedHistory: false, projectionSource: "our" }) },
];

const rows: Record<string, unknown>[] = [];
for (const slate of slates) {
  const curve = [...slate.curve].sort((a, b) => a[0] - b[0]);
  const median = curve.find(([r]) => r >= slate.entries / 2)?.[1] ?? NaN;
  const top1 = curve.find(([r]) => r >= slate.entries * 0.01)?.[1] ?? NaN;
  const top10 = curve.find(([r]) => r >= slate.entries * 0.1)?.[1] ?? NaN;
  console.log(`\n=== week ${slate.week} ${slate.contestName.slice(0, 70)} (${slate.entries} entries; median ${median}, top10% ${top10}, top1% ${top1}; run ${slate.runId.slice(0, 8)}, ${slate.settings.nLineups} lineups)`);
  if (slate.missingPoints.length) console.log(`   no recorded points (counted 0): ${slate.missingPoints.join(", ")}`);
  let mixtureNote: string[] | null = null;
  // Pick the first relaxation under which the ceiling build produces the full portfolio; use it for every variant.
  let relax = RELAXATIONS[0];
  for (const r of RELAXATIONS) {
    try {
      const probe = optimizeNflLineups(restorePlayers(structuredClone(slate.players)), { ...r.apply(slate.settings), gppScoring: "ceiling" });
      if (probe.lineups.length >= Math.max(1, Math.floor(slate.settings.nLineups * 0.8))) { relax = r; break; }
      if (r === RELAXATIONS[RELAXATIONS.length - 1]) console.log(`   still short (${probe.lineups.length}): ${probe.warnings.filter((w) => /Stopped|excluded|Cannot/.test(w)).slice(0, 2).join(" | ")}`);
    } catch (e) { if (r === RELAXATIONS[RELAXATIONS.length - 1]) console.log(`   cannot build: ${(e as Error).message.slice(0, 160)}`); }
  }
  if (relax !== RELAXATIONS[0]) console.log(`   settings relaxed for every variant: ${relax.label}`);
  for (const v of variants) {
    let players = restorePlayers(structuredClone(slate.players));
    const mix = v.mixture ? applyMixture(players, slate.starters) : [];
    if (v.mixture && !mix.length && !v.calibrate) continue; // identical to the unmixed variant
    if (v.calibrate) {
      const c = calibratePlayers(players, v.parts);
      players = c.players as NflOptimizerPlayer[];
      if (v.seed === 20261103) console.log(`   calibration: ${c.applied} players, ${c.notPlaying} more likely than not to record no stat line`);
    }
    if (v.mixture && !mixtureNote) { mixtureNote = mix; console.log(`   mixture: ${mix.join("; ")}`); }
    const settings: NflOptimizerSettings = { ...relax.apply(slate.settings), gppScoring: v.scoring, drawSeed: v.seed,
      ...(v.correlations ? { drawCorrelations: v.correlations } : {}), ...(v.shape ? { buildShape: v.shape } : {}) };
    const t = Date.now();
    let result;
    try { result = optimizeNflLineups(players, settings); }
    catch (e) { console.log(`-- ${v.name}: failed: ${(e as Error).message}`); continue; }
    if (!result.lineups.length) { console.log(`-- ${v.name}: no lineups (${result.warnings.slice(-2).join(" | ").slice(0, 200)})`); continue; }
    const scores = result.lineups.map((l) => l.slots.reduce((s, slot) => s + (slate.points[String(slot.player.dkPlayerId)] ?? 0) * slot.multiplier, 0));
    const ranks = scores.map((s) => rankOf(curve, s));
    const n = scores.length;
    const best = Math.max(...scores), bestRank = Math.min(...ranks);
    const counts = new Map<number, number>();
    for (const l of result.lineups) for (const id of l.playerIds) counts.set(id, (counts.get(id) ?? 0) + 1);
    const capCount = Math.floor((slate.settings.maxExposure ?? 1) * n + 1e-9);
    const atCap = [...counts.values()].filter((c) => c === capCount).length;
    const row = {
      slate: slate.contest, week: slate.week, variant: v.name, lineups: n, seconds: (Date.now() - t) / 1000,
      best: +best.toFixed(2), bestRank, bestPct: +(100 * (1 - bestRank / slate.entries)).toFixed(2),
      mean: +(scores.reduce((a, b) => a + b, 0) / n).toFixed(2),
      aboveMedian: scores.filter((s) => s > median).length, top10: scores.filter((s) => s >= top10).length, top1: scores.filter((s) => s >= top1).length,
      atCap, maxExposure: Math.max(...counts.values()), distinctPlayers: counts.size, relaxed: relax.label,
      pairs: Object.fromEntries((() => { const m = new Map<string, number>(); for (const l of result.lineups) { const ids = [...l.playerIds].sort((a, b) => a - b);
        for (let i = 0; i < ids.length; i++) for (let j = i + 1; j < ids.length; j++) { const k = `${ids[i]}|${ids[j]}`; m.set(k, (m.get(k) ?? 0) + 1); } } return [...m]; })()),
      exposures: Object.fromEntries([...counts].sort((a, b) => b[1] - a[1]).map(([id, c]) => [result.lineups.flatMap((l) => l.slots).find((s) => s.player.dkPlayerId === id)!.player.name, c])),
    };
    rows.push(row);
    console.log(`-- ${v.name.padEnd(12)} ${n} lineups ${row.seconds.toFixed(0)}s | best ${row.best} (#${bestRank}, beat ${row.bestPct}%) | mean ${row.mean} | >median ${row.aboveMedian}/${n} top10% ${row.top10} top1% ${row.top1} | at cap ${atCap}, max ${row.maxExposure}, players ${row.distinctPlayers}`);
  }
}

// Paired summary across slates: each variant against ceiling on the same slate.
const by = (variant: string) => rows.filter((r) => String(r.variant).startsWith(variant) && (variant.includes("+mix") || !String(r.variant).includes("+mix")));
console.log("\n=== summary across slates (ceiling vs each variant family; draws averaged over seeds)");
const fam = ["ceiling", "draws", "draws+mix"];
for (const f of fam) {
  const rs = f === "ceiling" ? rows.filter((r) => r.variant === "ceiling") : by(f);
  const slatesIn = new Set(rs.map((r) => r.slate)).size;
  const avg = (k: string) => (rs.reduce((a, r) => a + Number(r[k]), 0) / rs.length).toFixed(2);
  console.log(`${f.padEnd(10)} builds ${rs.length} on ${slatesIn} slates | best beat ${avg("bestPct")}% | mean score ${avg("mean")} | >median ${avg("aboveMedian")} | top10% ${avg("top10")} | top1% ${avg("top1")} | at cap ${avg("atCap")} | players ${avg("distinctPlayers")}`);
}
// Calibration against what main builds today: draws+mix where a mixture applied, else draws.
const famMean = (slate: unknown, prefix: string, k: string) => {
  const rs = rows.filter((r) => r.slate === slate && String(r.variant).replace(/#\d+$/, "") === prefix);
  return rs.length ? rs.reduce((a, r) => a + Number(r[k]), 0) / rs.length : null;
};
const keys = ["bestPct", "mean", "aboveMedian", "top10", "top1", "atCap", "distinctPlayers"];
const boot = (xs: number[]) => { const m: number[] = []; let seed = 7;
  const rnd = () => (seed = (Math.imul(seed, 1103515245) + 12345) >>> 0) / 4294967296;
  for (let b = 0; b < 4000; b++) { let t = 0; for (let i = 0; i < xs.length; i++) t += xs[Math.floor(rnd() * xs.length)]; m.push(t / xs.length); }
  m.sort((a, b) => a - b); return [m[100], m[3899]]; };
const families = corrSet ? ["corr", "shape", "corr+shape"] : ["cal", ...(process.env.REPLAY_PARTS ? ["cal-notplay", "cal-meanline"] : [])];
for (const fam of families) {
  const diffs: Record<string, number[]> = Object.fromEntries(keys.map((k) => [k, []]));
  for (const slate of new Set(rows.map((r) => r.slate))) for (const k of keys) {
    const cal = famMean(slate, fam, k), cur = corrSet ? famMean(slate, "main", k) : famMean(slate, "draws+mix", k) ?? famMean(slate, "draws", k);
    if (cal != null && cur != null) diffs[k].push(cal - cur);
  }
  console.log(`\n=== ${fam} minus today's main, per slate (${corrSet ? "calibrated draws, stated correlations, Showdown default shape" : "draws, mixture where it applies"})`);
  for (const k of keys) { const xs = diffs[k]; if (!xs.length) continue; const avg = xs.reduce((a, b) => a + b, 0) / xs.length; const [lo, hi] = boot(xs);
    console.log(`${k.padEnd(16)} ${avg >= 0 ? "+" : ""}${avg.toFixed(2)} [${lo.toFixed(2)}, ${hi.toFixed(2)}] on ${xs.length} slates; better on ${xs.filter((x) => x > 0).length}`); }
}
if (outPath) writeFileSync(outPath, JSON.stringify(rows, null, 1));
