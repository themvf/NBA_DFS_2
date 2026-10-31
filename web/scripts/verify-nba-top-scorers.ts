/** Read-only integration check against the configured warehouse. */
import assert from "node:assert/strict";
import { sql } from "drizzle-orm";
import { db } from "../src/db";
import { getNbaSeasonStats } from "../src/db/nba-top-scorers";
import { getNbaPointsCalibration } from "../src/db/nba-points-calibration";

async function main() {
  const season = "2025-26";
  const [stats, raw, grades] = await Promise.all([
    getNbaSeasonStats(season),
    db.execute(sql`SELECT player_id, name, points, minutes FROM nba_player_game_logs
      WHERE season = ${season} AND season_type = 'Regular Season'`),
    getNbaPointsCalibration(),
  ]);
  const totals = new Map<number, { sum: number; n: number; names: Set<string> }>();
  for (const row of raw.rows) {
    if (row.points == null || Number(row.minutes) <= 0 || row.minutes == null) continue;
    const id = Number(row.player_id);
    const entry = totals.get(id) ?? { sum: 0, n: 0, names: new Set<string>() };
    entry.sum += Number(row.points); entry.n++; entry.names.add(String(row.name));
    totals.set(id, entry);
  }
  assert.ok(stats.length > 0, "missing season reference data");
  assert.equal(stats.length, totals.size, "one full-season aggregate per player across trades");
  for (const stat of stats) {
    const matches = [...totals.values()].filter((t) => t.names.has(stat.name));
    assert.equal(matches.length, 1, `ambiguous verification identity ${stat.name}`);
    assert.equal(stat.games, matches[0].n, `appearance count for ${stat.name}`);
    assert.ok(Math.abs(stat.ppg! - matches[0].sum / matches[0].n) < 1e-9, `season PPG for ${stat.name}`);
  }
  if (grades?.lastDate) {
    const night = await getNbaPointsCalibration(grades.lastDate);
    assert.equal(night?.gradeVersion, grades.gradeVersion, "pooled and night grades use the same version");
  }
  console.log(JSON.stringify({ verifiedPlayers: stats.length, gradeVersion: grades?.gradeVersion,
    nights: grades?.nights, references: stats.filter((s) => /Schr|Brunson|Anthony Edwards|Jeremiah Fears/.test(s.name)) }));
}
main().catch((error) => { console.error(error); process.exitCode = 1; });
