/**
 * Replay saved NFL builds with sequential and joint portfolio selection
 * (nfl-joint-selection-v1) from their frozen inputs, and compare.
 *
 * Usage: replay-nfl-joint-selection.ts <runs.json> [points.json]
 *   runs.json   { [runId]: { settings, players } } -- a saved run's settings and input_snapshot
 *   points.json { [playerName]: dkPoints } -- optional; actual results are read only AFTER
 *               both builds exist, and reported descriptively (one slate proves nothing).
 *
 * Prints, per run and mode: build time, the summed build objective (jitter-free,
 * the scale the joint selector optimises), rule tallies, and the joint note.
 */
import { readFileSync } from "node:fs";
import { optimizeNflLineups, portfolioFeatures, type NflOptimizerSettings } from "../src/app/dfs/nfl/nfl-optimizer";
import { restorePlayers, type FrozenPlayer } from "../src/lib/nfl-dfs/frozen-players";

const runs = JSON.parse(readFileSync(process.argv[2], "utf8")) as Record<string, { settings: NflOptimizerSettings; players: FrozenPlayer[] }>;
const points = process.argv[3] ? JSON.parse(readFileSync(process.argv[3], "utf8")) as Record<string, number> : null;

for (const [runId, { settings, players }] of Object.entries(runs)) {
  const restored = restorePlayers(players);
  console.log(`\n=== run ${runId.slice(0, 8)} (${settings.format}, ${settings.nLineups} lineups)`);
  for (const mode of ["sequential", "joint"] as const) {
    const started = Date.now();
    const result = optimizeNflLineups(restored, { ...settings, portfolioSelection: mode });
    const ms = Date.now() - started;
    const totals: Record<string, number> = {};
    for (const lineup of result.lineups) for (const [k, v] of Object.entries(portfolioFeatures(lineup, settings.format))) {
      if (!/^[ocf]_/.test(k)) totals[k] = (totals[k] ?? 0) + v;
    }
    const maxOverall = Math.max(...(result.exposureReport ?? []).map((r) => r.overall));
    const capBreaches = (result.exposureReport ?? []).filter((r) => r.overall > r.overallMax || r.captain > r.captainMax).length;
    console.log(`-- ${mode}: ${result.lineups.length} lineups in ${(ms / 1000).toFixed(1)}s; projected sum ${result.lineups.reduce((s, l) => s + l.projectedFpts, 0).toFixed(1)}; `
      + `max player exposure ${maxOverall}; cap breaches ${capBreaches}; tallies ${JSON.stringify(totals)}`);
    for (const w of result.warnings.filter((w) => /Joint selection|Showdown distributions|Classic distributions/.test(w))) console.log(`   ${w}`);
    if (points) {
      const scored = result.lineups.map((l) => l.slots.reduce((s, slot) => s + (points[slot.player.name] ?? 0) * slot.multiplier, 0));
      const unknown = new Set(result.lineups.flatMap((l) => l.slots.map((s) => s.player.name)).filter((name) => points[name] == null));
      console.log(`   actual (descriptive): best ${Math.max(...scored).toFixed(2)}, median ${[...scored].sort((a, b) => a - b)[Math.floor(scored.length / 2)].toFixed(2)}`
        + (unknown.size ? `; no points found for ${[...unknown].join(", ")} (counted 0)` : ""));
    }
  }
}
