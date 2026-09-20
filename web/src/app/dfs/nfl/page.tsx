import type { Metadata } from "next";
import NflDfsClient from "./nfl-dfs-client";
import Link from "next/link";

export const metadata: Metadata = {
  title: "NFL DFS Workspace",
  description: "DraftKings NFL Classic and Showdown slate intake and model-readiness workspace.",
};

export default function NflDfsPage() {
  return <><nav aria-label="NFL research pages" className="mb-4 flex items-center justify-end"><details className="relative"><summary className="cursor-pointer rounded-lg border bg-white px-3 py-2 text-sm font-medium">Explore NFL tools</summary><div className="absolute right-0 z-30 mt-2 grid w-56 gap-1 rounded-xl border bg-white p-2 shadow-lg">{[
    ["history", "Player context"], ["model", "Model lab"], ["scenarios", "Scenario lab"], ["review", "Weekly player review"], ["availability", "Availability review"],
  ].map(([path, label]) => <Link key={path} className="rounded px-3 py-2 text-sm hover:bg-slate-50" href={`/dfs/nfl/${path}`}>{label}</Link>)}</div></details></nav><NflDfsClient /></>;
}
