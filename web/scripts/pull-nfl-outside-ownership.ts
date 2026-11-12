/**
 * Pull stat-api's projected ownership for a saved NFL slate and store it
 * (`nfl-ownership-outside-v1`), or turn it off again. The same action the
 * page's button calls; this is the way to run it from a machine that holds
 * the stat-api key when the deployed site does not. Never scheduled.
 *
 *   node --env-file=../.env --env-file=.env.local -r ./scripts/server-only-stub.cjs --import tsx \
 *     scripts/pull-nfl-outside-ownership.ts <uploadId> [--off] [--show]
 *
 * --off   go back to our own model for that slate
 * --show  only print what the page would read now; pull nothing
 */
import { clearNflOutsideOwnership, loadSavedNflWorkspace, pullNflOutsideOwnership } from "../src/app/dfs/nfl/actions";

async function main() {
  const [uploadId, ...flags] = process.argv.slice(2);
  if (!uploadId || !/^[0-9a-f-]{36}$/.test(uploadId)) throw new Error("Give the saved slate's upload id.");
  if (flags.includes("--off")) { await clearNflOutsideOwnership(uploadId); console.log("Back to our own ownership model."); }
  else if (!flags.includes("--show")) {
    const pulled = await pullNflOutsideOwnership(uploadId);
    console.log(`Pulled: ${pulled.withOwnership} of ${pulled.players} players with ownership, from ${pulled.contestName ?? "the slate's main contest"}, stat-api as of ${pulled.sourceAsOf ?? "unknown"}.`);
    for (const problem of pulled.problems) console.log(`  note: ${problem}`);
  }
  const { slate } = await loadSavedNflWorkspace(uploadId);
  const state = slate.outsideOwnership;
  console.log(JSON.stringify({ inUse: state?.inUse ?? false, turnedOff: state?.turnedOff ?? false, capturedAt: state?.capturedAt ?? null, error: state?.error ?? null, report: state?.report ?? null }, null, 1));
  const rows = slate.players.filter((p) => !p.isOut && (p.ownPct ?? 0) > 0).sort((a, b) => (b.ownPct ?? 0) - (a.ownPct ?? 0)).slice(0, 14);
  for (const p of rows) {
    const split = p.captainOwnPct != null && p.flexOwnPct != null ? `cpt ${p.captainOwnPct.toFixed(1)} flex ${p.flexOwnPct.toFixed(1)}` : "";
    console.log(`${p.name.padEnd(22)} ${p.position.padEnd(4)} ${(p.ownPct ?? 0).toFixed(1).padStart(5)}%  ${split.padEnd(24)} ours ${(p.ourOwnPct ?? 0).toFixed(1).padStart(5)}%  ${p.ownSource ?? ""}`);
  }
  console.log("ownership check:", slate.slateCheck?.items.find((i) => i.id === "ownership")?.text ?? "none");
}

main().then(() => process.exit(0), (error) => { console.error(error instanceof Error ? error.message : error); process.exit(1); });
