/**
 * Hindsight boom-weight screen (discovery only; docs/nfl-ghost-factorial-study.md,
 * amendment 2026-10-30). For every frozen ghost slate it rebuilds the HOUSE
 * portfolio at GPP boom weight 0, 1, 2 and 4 (every other setting at the
 * house level) from the frozen input, and writes the lineups to
 * artifacts/nfl-ghost-extra-arms-v1/boom-weight-screen/<freeze>.json for
 * `python -m research.nfl_boom_weight_screen` to grade against the slate's
 * contest. Nothing is saved to the database. Level 2 is the house itself: its
 * lineups are compared with the stored arm-0 build as a reproduction check.
 *
 *   npx tsx --conditions=react-server scripts/research-nfl-boom-weight-screen.ts [--freeze <id>]
 */
import { config } from "dotenv";
import { mkdirSync, writeFileSync } from "node:fs";
config({ path: ".env.local", quiet: true });

const LEVELS = [0, 1, 2, 4];
const OUT = "../artifacts/nfl-ghost-extra-arms-v1/boom-weight-screen";

(async () => {
  const { buildArm } = await import("../src/lib/nfl-dfs/ghost/arm");
  const { ghostDesign, GHOST_DESIGN_VERSION, GHOST_EXTRA_ARMS_VERSION } = await import("../src/lib/nfl-dfs/ghost/design");
  const { optimizeNflLineups, NFL_OPTIMIZER_VERSION } = await import("../src/app/dfs/nfl/nfl-optimizer");
  const { db } = await import("../src/db"); const { sql } = await import("drizzle-orm");
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const rows = (r: unknown) => (r as { rows: Record<string, any>[] }).rows;
  const only = process.argv.includes("--freeze") ? process.argv[process.argv.indexOf("--freeze") + 1] : null;
  mkdirSync(OUT, { recursive: true });

  const freezes = rows(await db.execute(sql`SELECT freeze_id, format, season, week, draft_group_id, first_kickoff, teams
    FROM nfl_dfs_ghost_freezes WHERE design_version = ${GHOST_DESIGN_VERSION} ORDER BY first_kickoff`));
  for (const f of freezes) {
    const freezeId = String(f.freeze_id);
    if (only && freezeId !== only) continue;
    const format = f.format as "classic" | "showdown";
    const full = rows(await db.execute(sql`SELECT input_snapshot, house_settings FROM nfl_dfs_ghost_freezes WHERE freeze_id = ${freezeId}::uuid`))[0];
    const house = full.house_settings;
    const levels0 = ghostDesign(format).arms[0].levels;
    const stored = rows(await db.execute(sql`SELECT l.player_ids FROM nfl_dfs_ghost_arms a JOIN nfl_dfs_lineups l ON l.run_id = a.run_id
      WHERE a.freeze_id = ${freezeId}::uuid AND a.arm = 0 ORDER BY l.lineup_number`)).map(r => JSON.stringify([...(r.player_ids as number[])].sort((a, b) => a - b)));
    const out: Record<string, unknown> = { version: GHOST_EXTRA_ARMS_VERSION, screen: "boom-weight-hindsight", freezeId, format,
      season: f.season, week: f.week, draftGroupId: f.draft_group_id, firstKickoff: new Date(f.first_kickoff).toISOString(),
      teams: f.teams, optimizerVersion: NFL_OPTIMIZER_VERSION, requested: house.nLineups, levels: {} as Record<string, unknown> };
    const line: string[] = [];
    for (const bw of LEVELS) {
      const built = buildArm(format, full.input_snapshot, house, { ...levels0, boomWeight: bw });
      const t = Date.now();
      const r = optimizeNflLineups(built.players, built.settings);
      const lineups = r.lineups.map(l => ({ lineupNumber: l.lineupNumber, slots: l.slots.map(s => ({ slot: s.slot, dkPlayerId: s.player.dkPlayerId, name: s.player.name, team: s.player.team })),
        playerIds: l.playerIds, projectedFpts: l.projectedFpts, ceilingFpts: l.ceilingFpts, projectedOwnership: l.projectedOwnership }));
      const keys = new Set(lineups.map(l => JSON.stringify([...l.playerIds].sort((a, b) => a - b))));
      const matchesStored = bw === 2 && stored.length ? stored.filter(k => keys.has(k)).length : null;
      (out.levels as Record<string, unknown>)[String(bw)] = { boomWeight: bw, built: lineups.length, ms: Date.now() - t, lineups,
        warnings: r.warnings.slice(0, 6), matchesStoredHouse: matchesStored, storedHouseLineups: bw === 2 ? stored.length : null };
      line.push(`bw ${bw}: ${lineups.length}/${house.nLineups}${matchesStored != null ? ` (${matchesStored}/${stored.length} = stored arm 0)` : ""}`);
    }
    writeFileSync(`${OUT}/${freezeId}.json`, JSON.stringify(out));
    console.log(`${f.season} w${f.week} ${format} ${f.draft_group_id} ${freezeId.slice(0, 8)}: ${line.join("; ")}`);
  }
})().catch((e) => { console.error(e); process.exit(1); });
