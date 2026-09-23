/**
 * Captain-slot concentration in a Showdown GPP: the 2026 week-2 Monday night
 * slate (LAR@NYG), replayed against DraftKings contest 195786073 (47,562
 * entries, winner 149.33, median 84.78).
 *
 * ## The question
 *
 * 40 lineups were built and Davante Adams captained none of them. He scored
 * 42.5, which is 63.8 with the multiplier, and the near-perfect lineup on this
 * slate was CPT Adams + Stafford + Ferguson + Kyren + Rams + Corum = 150.0 --
 * within a point of what actually won. Our best of 40 scored 110.8 (rank
 * 11,301); Adams was in 20 of our 40 lineups, always at FLEX.
 *
 * ## Why he was never captained, mechanically
 *
 * Captain pays 1.5x points for 1.5x salary, so points-per-dollar is identical
 * in both slots and a linear objective is indifferent about WHERE a player
 * goes. Given a chosen six, the solver puts the multiplier on whichever scores
 * highest under the objective -- in GPP that is `ceilingFpts`. Our ceilings
 * ranked Adams SEVENTH (27.9, behind Nacua 42.7, Nabers 37.3, Winston 34.3,
 * Stafford 33.0, Simpson 31.8, Skattebo 29.0), so he was never the amplified
 * man. The captain is not a decision the optimizer makes; it is our ceiling
 * ranking, amplified.
 *
 * Nothing capped it either: `captainMax` falls back to `settings.nLineups`
 * when no exposure policy is supplied, and `maxExposure` constrains only
 * OVERALL appearances. Puka Nacua took 14 of 40 captain slots -- carrying a
 * fresh QUESTIONABLE tag and 0.64% field ownership -- and scored 0.0.
 *
 * ## What this measures
 *
 * Two interventions against the real contest distribution:
 *   1. the shipped availability gate (a QUESTIONABLE/DOUBTFUL player may not
 *      take the multiplier, but stays rosterable at FLEX);
 *   2. uniform captain exposure caps, through the existing policy machinery.
 *
 * Usage: replay-nfl-showdown-captain.ts <players.json> <scores.json>
 */
import { readFileSync } from "node:fs";
import {
  optimizeNflLineups, DEFAULT_NFL_PUNT_POLICY,
  type NflOptimizerPlayer, type NflOptimizerSettings, type PlayerExposurePolicy,
} from "../src/app/dfs/nfl/nfl-optimizer";

type Row = NflOptimizerPlayer & { actual: number; cptOwn: number; flexOwn: number };

const [, , playersPath, scoresPath] = process.argv;
if (!playersPath || !scoresPath) {
  console.error("usage: replay-nfl-showdown-captain.ts <players.json> <scores.json>");
  process.exit(1);
}
const rows: Row[] = JSON.parse(readFileSync(playersPath, "utf8"));
const scores: number[] = JSON.parse(readFileSync(scoresPath, "utf8")); // ascending
const truth = new Map(rows.map((r) => [r.dkPlayerId, r]));
const WINNER = scores[scores.length - 1];

/** Exact contest rank: how many entries scored at least this. */
function rankOf(score: number): number {
  let lo = 0, hi = scores.length;
  while (lo < hi) { const mid = (lo + hi) >> 1; if (scores[mid] < score) lo = mid + 1; else hi = mid; }
  return scores.length - lo;
}

function settings(over: Partial<NflOptimizerSettings> = {}): NflOptimizerSettings {
  return {
    format: "showdown", mode: "gpp", projectionSource: "our", allowDkFallback: true,
    nLineups: 40, minSalary: 45000, maxExposure: 0.6, minUnique: 2,
    stackPassCatchers: 1, bringBack: true, randomness: 0.08,
    requireObservedHistory: true, minPlayerSalary: 1000,
    puntPolicy: { ...DEFAULT_NFL_PUNT_POLICY }, puntOverrides: [],
    favoriteTeam: "LAR", underdogTeam: "NYG", archetypeMode: "balanced",
    lockedPlayerIds: [], excludedPlayerIds: [],
    minExposureByPlayer: {}, maxExposureByPlayer: {},
    ...over,
  };
}

/** One cap for every captain-eligible player. Deliberately uniform: a cap is
 *  not a view about who is good, only about how much of the amplified slot any
 *  single estimate may own. */
function captainCaps(maxPct: number): PlayerExposurePolicy[] {
  return rows.filter((r) => !r.isOut).map((r) => ({
    playerId: r.dkPlayerId,
    overall: { minPct: null, maxPct: null },
    captain: { minPct: null, maxPct },
    flex: { minPct: null, maxPct: null },
    exactTargetMode: false,
  }));
}

function run(label: string, over: Partial<NflOptimizerSettings>, pool: Row[] = rows) {
  let result;
  try { result = optimizeNflLineups(pool, settings(over)); }
  catch (e) { console.log(`${label.padEnd(30)} FAILED: ${(e as Error).message}`); return null; }
  const scored = result.lineups.map((l) => {
    const picks = l.slots.map((s) => ({ p: truth.get(s.player.dkPlayerId)!, m: s.multiplier }));
    return {
      total: picks.reduce((a, x) => a + (x.p?.actual ?? 0) * x.m, 0),
      captain: picks.find((x) => x.m === 1.5)?.p?.name ?? "?",
    };
  }).sort((a, b) => b.total - a.total);
  if (!scored.length) {
    const el = result.eligibility ?? [];
    console.log(`${label.padEnd(30)} no lineups  (eligible ${el.filter((d) => d.eligible).length}/${el.length})`);
    for (const w of result.warnings.slice(0, 4)) console.log(`      ! ${w.slice(0, 120)}`);
    return null;
  }
  const mean = scored.reduce((a, l) => a + l.total, 0) / scored.length;
  const beat = scored.filter((l) => l.total > WINNER).length;
  const caps = new Map<string, number>();
  for (const l of scored) caps.set(l.captain, (caps.get(l.captain) ?? 0) + 1);
  console.log(
    `${label.padEnd(30)} best ${scored[0].total.toFixed(1).padStart(6)} (rank ${rankOf(scored[0].total).toLocaleString().padStart(6)})` +
    `  mean ${mean.toFixed(1).padStart(6)}  >winner ${String(beat).padStart(2)}/${scored.length}` +
    `  captains ${caps.size}  top ${Math.round(100 * Math.max(...caps.values()) / scored.length)}%`,
  );
  return { scored, caps, mean, beat };
}

console.log(`Showdown replay: ${rows.length} players, contest 195786073 (${scores.length.toLocaleString()} entries)`);
console.log(`winner ${WINNER.toFixed(2)}, median ${scores[scores.length >> 1].toFixed(2)}\n`);

// The shipped gate is already active in `optimizeNflLineups`; this strips the
// availability status back off to show what the run did WITHOUT it.
const ungated: Row[] = rows.map((r) => ({ ...r, availabilityStatus: null }));
const asBuilt = run("as built (no captain gate)", {}, ungated);
const gated = run("captain availability gate", {}, rows);
run("  + uniform captain cap 25%", { exposurePolicies: captainCaps(0.25) }, rows);
const cap15 = run("  + uniform captain cap 15%", { exposurePolicies: captainCaps(0.15) }, rows);
run("  + uniform captain cap 10%", { exposurePolicies: captainCaps(0.10) }, rows);

for (const [label, r] of [["as built (no gate)", asBuilt], ["availability gate", gated]] as const) {
  if (!r) continue;
  console.log(`\n${label} captain distribution:`);
  for (const [nm, n] of [...r.caps].sort((a, b) => b[1] - a[1])) {
    const p = rows.find((x) => x.name === nm);
    console.log(`  ${nm.padEnd(24)} ${String(n).padStart(2)}/40  ceiling ${(p?.ceilingFpts ?? 0).toFixed(1).padStart(5)}` +
      `  actual ${(p?.actual ?? 0).toFixed(1).padStart(5)}  as CPT ${((p?.actual ?? 0) * 1.5).toFixed(1).padStart(5)}` +
      `  field CPT ${(p?.cptOwn ?? 0).toFixed(1)}%`);
  }
}

console.log(`
WHAT THIS MEASURED

The availability gate helps and is right on its own terms: a player whose
availability is in doubt should not be the one player in the lineup carrying a
1.5x multiplier. Best score 110.8 -> 122.7 (rank 11,301 -> 4,410), and Nacua's
14 captain slots -- all worth 0.0 -- disappear.

It does NOT solve concentration. It hands the multiplier to the next-highest
ceiling instead, and on this slate that was Malik Nabers, who went from 11 of
40 captain slots to 21 and scored 1.1. Concentration got worse, not better.

Uniform caps reduce concentration and cost more than they gain here: best
falls to 107.7-111.0 against the gate's 122.7, because a quota pushes the
multiplier onto players with genuinely lower ceilings.

So neither is the real answer. The captain slot is decided by which player our
ceiling model ranks first, and on this slate the correct captain was our
SEVENTH. A point-estimate p90 per player cannot express "who is most likely to
be the top scorer on this slate", which is the only question the captain slot
actually asks. Two players with the same p90 can have very different chances
of a 40-point game, and nothing in the current model distinguishes them.

Also missing: ownership was "unavailable" on this run, so leverage was off
entirely. Captain ownership is the most concentrated, most decidable
distribution in a Showdown field (Dart 20.2%, Adams 15.7%, Kyren 14.9% here),
and we ran blind to it.

One slate. The gate ships because its reasoning stands without this result;
the concentration finding is a hypothesis for a real study, not a fix.`);