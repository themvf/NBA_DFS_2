import type { Metadata } from "next";
import NflDfsClient from "./nfl-dfs-client";
import Link from "next/link";
import { nflBuildInfo, shortCommitSha } from "@/lib/nfl-dfs/build-info";

export const metadata: Metadata = {
  title: "NFL DFS Workspace",
  description: "DraftKings NFL Classic and Showdown slate intake and model-readiness workspace.",
};

export default function NflDfsPage() {
  // Phase 0 (spec P0-AC1): surface the exact build identity on the page so a
  // deployed build can be reconciled against local code at a glance.
  const build = nflBuildInfo();
  return <><nav aria-label="NFL research pages" className="mb-4 flex items-center justify-end"><details className="relative"><summary className="cursor-pointer rounded-lg border bg-white px-3 py-2 text-sm font-medium">Explore NFL tools</summary><div className="absolute right-0 z-30 mt-2 grid w-56 gap-1 rounded-xl border bg-white p-2 shadow-lg">{[
    ["pool-review", "Full pool audit"], ["history", "Player context"], ["model", "Model lab"], ["scenarios", "Scenario lab"], ["review", "Weekly player review"], ["availability", "Availability review"],
  ].map(([path, label]) => <Link key={path} className="rounded px-3 py-2 text-sm hover:bg-slate-50" href={`/dfs/nfl/${path}`}>{label}</Link>)}</div></details></nav><NflDfsClient />
    <footer aria-label="Build identity" className="mt-6 flex flex-wrap items-center gap-x-4 gap-y-1 border-t pt-3 text-[11px] text-slate-500">
      <span>build <code className="font-mono text-slate-700">{shortCommitSha(build.commitSha)}</code></span>
      <span>built <span className="font-mono">{build.buildTime}</span></span>
      <span>settings <span className="font-mono">{build.settingsSchemaVersion}</span></span>
      <span>optimizer <span className="font-mono">{build.optimizerVersion}</span></span>
      <span>scorer <span className="font-mono">{build.scorerVersion}</span></span>
    </footer></>;
}
