/**
 * Import an outside ownership range page (`nfl-ownership-bands-v1`) for one
 * slate date. The page is FantasyPros' "DraftKings Projected Roster %" copied
 * as plain text into a file. The newest import for a date bounds the prior on
 * that date's Classic slates (see attachOwnershipBands in dfs/nfl/actions.ts).
 *
 *   node --env-file=.env.local --import tsx ./scripts/import-nfl-ownership-bands.ts <file> <YYYY-MM-DD> [--dry-run]
 *
 * Prints how many rows parsed and, for every Classic upload whose first
 * kickoff falls on that date, how many of its players a range matched.
 */
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { neon } from "@neondatabase/serverless";
import { matchBands, parseFantasyProsOwnership } from "../src/lib/nfl-dfs/ownership-bands";

async function main(): Promise<void> {
  const [file, slateDate] = process.argv.slice(2);
  const dryRun = process.argv.includes("--dry-run");
  if (!file || !/^\d{4}-\d{2}-\d{2}$/.test(slateDate ?? "")) throw new Error("usage: import-nfl-ownership-bands.ts <file> <YYYY-MM-DD> [--dry-run]");

  const text = readFileSync(file, "utf8");
  const { rows, warnings } = parseFantasyProsOwnership(text);
  if (!rows.length) throw new Error("No ranges parsed; is this the FantasyPros Projected Roster % page?");
  const byPos = rows.reduce<Record<string, number>>((acc, r) => ({ ...acc, [r.position]: (acc[r.position] ?? 0) + 1 }), {});
  console.log(`parsed ${rows.length} ranges`, byPos, warnings.length ? { warnings } : "");

  const sql = neon(process.env.DATABASE_URL!);
  const uploads = await sql`SELECT u.upload_id, u.file_name, MIN(p.game_info) AS g FROM nfl_dfs_slate_uploads u
    JOIN nfl_dfs_slate_players p ON p.upload_id = u.upload_id
    WHERE u.format = 'classic' AND u.origin = 'upload' AND u.created_at > NOW() - INTERVAL '7 days'
    GROUP BY u.upload_id, u.file_name ORDER BY MAX(u.created_at) DESC`;
  for (const u of uploads) {
    const players = await sql`SELECT dk_player_id, name, position, team, salary FROM nfl_dfs_slate_players WHERE upload_id = ${u.upload_id}`;
    const matches = matchBands(players.map((p) => ({ dkPlayerId: Number(p.dk_player_id), name: String(p.name), position: String(p.position),
      team: String(p.team), salary: Number(p.salary) })), rows);
    const matchedRows = new Set(matches.values());
    console.log(`upload ${u.upload_id} (${u.file_name}): ${matches.size} of ${players.length} players matched;`,
      `unmatched ranges: ${rows.filter((r) => !matchedRows.has(r)).map((r) => `${r.name} ${r.team} $${r.salary}`).join(", ") || "none"}`);
  }
  if (dryRun) return;

  await sql`CREATE TABLE IF NOT EXISTS nfl_dfs_ownership_bands (
      id BIGSERIAL PRIMARY KEY,
      slate_date DATE NOT NULL,
      format TEXT NOT NULL CHECK (format IN ('classic','showdown')),
      source TEXT NOT NULL,
      captured_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      text_digest TEXT NOT NULL,
      rows JSONB NOT NULL,
      warnings JSONB NOT NULL DEFAULT '[]'::jsonb,
      UNIQUE (slate_date, format, source, text_digest)
    )`;
  const digest = createHash("sha256").update(text).digest("hex");
  const inserted = await sql`INSERT INTO nfl_dfs_ownership_bands (slate_date, format, source, text_digest, rows, warnings)
    VALUES (${slateDate}::date, 'classic', 'fantasypros', ${digest}, ${JSON.stringify(rows)}::jsonb, ${JSON.stringify(warnings)}::jsonb)
    ON CONFLICT (slate_date, format, source, text_digest) DO NOTHING RETURNING id, captured_at`;
  console.log(inserted.length ? `stored as id ${inserted[0].id} at ${inserted[0].captured_at}` : "this exact page is already stored; nothing written");
}

main().catch((error) => { console.error(error); process.exit(1); });
