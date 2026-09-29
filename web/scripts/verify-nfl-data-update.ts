/**
 * Live check of "Update data": dispatches the real workflows, follows the runs
 * GitHub reports back until they finish, and prints what the page would say.
 * Dispatches two ordinary scheduled jobs (injury/depth capture + projection
 * rebuild, DraftKings statuses); writes one nfl_dfs_data_updates row.
 *
 *   GITHUB_DISPATCH_TOKEN=... npm run verify:nfl-data-update -- <uploadId>
 *
 * Skips the kickoff lock on purpose (it calls beginNflDataUpdate, not the
 * page's startNflDataUpdate), so it can run between slates.
 */
import { sql } from "drizzle-orm";
import { db } from "../src/db";
import { beginNflDataUpdate, readNflDataUpdate } from "../src/app/dfs/nfl/data-update-actions";

async function main() {
  const token = process.env.GITHUB_DISPATCH_TOKEN;
  if (!token) throw new Error("Set GITHUB_DISPATCH_TOKEN.");
  const uploadId = process.argv[2] ?? String((await db.execute(sql`SELECT upload_id FROM nfl_dfs_slate_uploads ORDER BY created_at DESC LIMIT 1`)).rows[0]?.upload_id);
  const print = (label: string, r: Awaited<ReturnType<typeof readNflDataUpdate>>) => {
    console.log(`[${new Date().toISOString().slice(11, 19)}] ${label}: ${r.view?.headline ?? "no update"}`);
    for (const line of r.view?.lines ?? []) console.log(`    ${line.state.padEnd(7)} ${line.label}: ${line.text}${line.href ? ` (${line.href})` : ""}`);
  };
  let result = await beginNflDataUpdate(uploadId, token);
  print("started", result);
  const deadline = Date.now() + 25 * 60_000;
  while (result.view?.state === "running" && Date.now() < deadline) {
    await new Promise((r) => setTimeout(r, 20_000));
    result = await readNflDataUpdate(uploadId);
    print("poll", result);
  }
  console.log(`final state: ${result.view?.state}; blockedReason for this slate: ${result.blockedReason ?? "none"}`);
}

main().then(() => process.exit(0)).catch((e) => { console.error(e); process.exit(1); });
