import type { Metadata } from "next";
import NflDfsClient from "./nfl-dfs-client";
import Link from "next/link";

export const metadata: Metadata = {
  title: "NFL DFS Workspace",
  description: "DraftKings NFL Classic and Showdown slate intake and model-readiness workspace.",
};

export default function NflDfsPage() {
  return <><nav className="mx-auto flex max-w-[1600px] flex-wrap gap-3 px-6 pt-4"><Link className="inline-flex rounded-lg border border-emerald-700 px-4 py-2 text-sm font-bold text-emerald-800" href="/dfs/nfl/model">Model Lab →</Link><Link className="inline-flex rounded-lg border border-emerald-700 px-4 py-2 text-sm font-bold text-emerald-800" href="/dfs/nfl/scenarios">Scenario Lab →</Link><Link className="inline-flex rounded-lg border border-emerald-700 px-4 py-2 text-sm font-bold text-emerald-800" href="/dfs/nfl/review">Weekly player review →</Link></nav><NflDfsClient /></>;
}
