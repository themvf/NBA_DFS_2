import type { NflGeneratedLineup } from "./nfl-optimizer";
import { summarizeRunRisks } from "@/lib/nfl-dfs/run-risk-summary";

export default function RunRiskSummary({ lineups, uncalibratedLeverage }: { lineups: NflGeneratedLineup[]; uncalibratedLeverage: boolean }) {
  const summary = summarizeRunRisks(lineups);
  if (summary.sourceFamilies.length < 2 && !uncalibratedLeverage && !summary.dstOpponentLineups.length) return null;
  return <section role="status" className="rounded-xl border border-amber-300 bg-amber-50 p-4 text-sm text-amber-950">
    <h2 className="font-bold">Review before export</h2>
    <ul className="mt-2 list-disc space-y-1 pl-5">
      {summary.sourceFamilies.length > 1 ? <li>These saved lineups contain players scored from different projection sources ({summary.sourceFamilies.join(", ")}). Their objective scores may not be comparable. Regenerate from one source before exporting.</li> : null}
      {uncalibratedLeverage ? <li>Uncalibrated ownership leverage influenced this run. Review exposures before exporting.</li> : null}
      {summary.dstOpponentLineups.length ? <li>{summary.dstOpponentLineups.length} of {lineups.length} lineups pair a DST with an opposing offensive player. Review whether each pairing fits your strategy. Lineups: {summary.dstOpponentLineups.join(", ")}.</li> : null}
    </ul>
  </section>;
}
