import type { Metadata } from "next";
import Link from "next/link";
import ResearchClient from "./research-client";

export const metadata: Metadata = {
  title: "NFL DFS · Research tools",
  description: "Competitor benchmarks and absence experiments for a saved NFL slate.",
};

/**
 * Read-only research that used to sit inside the workspace's "Research & audit"
 * tab, between the user and the results upload. Nothing here changes a lineup
 * build, so it lives off the everyday path.
 */
export default async function ResearchPage({ searchParams }: { searchParams: Promise<{ upload?: string }> }) {
  const { upload } = await searchParams;
  return <main className="mx-auto max-w-[1400px] space-y-4">
    <Link href="/dfs/nfl" className="text-sm font-semibold text-blue-700 underline">← Lineup workspace</Link>
    <header>
      <h1 className="text-2xl font-bold tracking-tight">Research tools</h1>
      <p className="mt-1 text-sm text-slate-500">
        Benchmarks against other projection sources and experimental absence previews for a saved slate.
        Nothing here changes how lineups are built.
      </p>
    </header>
    <ResearchClient uploadId={upload ?? null} />
  </main>;
}
