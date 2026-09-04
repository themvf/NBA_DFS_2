import Link from "next/link";
import { getNflFeatureAudit } from "@/db/nfl-dfs-feature-audit";
import ModelLab from "./model-lab";

export const dynamic = "force-dynamic";
export const metadata = { title: "NFL DFS · Model Lab" };

export default async function Page({ searchParams }: { searchParams: Promise<{ audit?: string }> }) {
  const { audit } = await searchParams;
  let saved;
  let failed = false;
  try {
    if (audit && !/^[a-f0-9]{64}$/.test(audit)) throw new Error("Invalid audit identifier");
    saved = await getNflFeatureAudit(audit);
  } catch {
    failed = true;
  }
  if (!saved) return <main className="mx-auto max-w-5xl space-y-5 p-8">
    <Link href="/dfs/nfl">← NFL DFS workspace</Link><h1 className="text-3xl font-bold">Model Lab</h1>
    <p role="status">{failed ? "Saved input coverage is unavailable. Check the NFL feature-audit job and schema installation." : "No saved audit found. Run the NFL feature-audit job to inspect stored input coverage."}</p>
    <p>Production projections are unchanged. No coverage numbers are substituted.</p>
  </main>;
  return <ModelLab report={saved.report} digest={saved.digest} viewedAt={Date.now()} />;
}
