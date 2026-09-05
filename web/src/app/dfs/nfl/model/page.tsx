import Link from "next/link";
import { getNflEfficiency } from "@/db/nfl-dfs-efficiency";
import { getNflFeatureAudit } from "@/db/nfl-dfs-feature-audit";
import { getNflWorkload } from "@/db/nfl-dfs-workload";
import ModelLab from "./model-lab";

export const dynamic = "force-dynamic";
export const metadata = { title: "NFL DFS · Model Lab" };

export default async function Page({ searchParams }: { searchParams: Promise<{ audit?: string }> }) {
  const { audit } = await searchParams;
  let saved, workload, efficiency;
  let auditFailed = false, workloadFailed = false, efficiencyFailed = false;
  try {
    if (audit && !/^[a-f0-9]{64}$/.test(audit)) throw new Error("Invalid audit identifier");
    saved = await getNflFeatureAudit(audit);
  } catch {
    auditFailed = true;
  }
  try { workload = await getNflWorkload(); } catch { workloadFailed = true; }
  if (workload) try { efficiency = await getNflEfficiency(workload.digest); } catch { efficiencyFailed = true; }
  if (!saved) return <main className="mx-auto max-w-5xl space-y-5 p-8">
    <Link href="/dfs/nfl">← NFL DFS workspace</Link><h1 className="text-3xl font-bold">Model Lab</h1>
    <p role="status">{auditFailed ? "Saved input coverage is unavailable. Check the NFL feature-audit job and schema installation." : "No saved audit found. Run the NFL feature-audit job to inspect stored input coverage."}</p>
    <p>Production projections are unchanged. No coverage numbers are substituted.</p>
  </main>;
  return <ModelLab report={saved.report} digest={saved.digest} viewedAt={Date.now()}
    workload={workload?.report ?? null} workloadDigest={workload?.digest ?? null} workloadFailed={workloadFailed}
    efficiency={efficiency?.report ?? null} efficiencyDigest={efficiency?.digest ?? null} efficiencyFailed={efficiencyFailed} />;
}
