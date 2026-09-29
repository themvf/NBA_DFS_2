import { AlertTriangle, CheckCircle2 } from "lucide-react";
import { readFailingWorkflows, type FailingWorkflow } from "@/lib/workflow-health";

function when(iso: string): string {
  const t = new Date(iso);
  return Number.isFinite(t.getTime())
    ? t.toLocaleString("en-US", { timeZone: "America/New_York", month: "short", day: "numeric", hour: "numeric", minute: "2-digit" }) + " ET"
    : iso;
}

/**
 * Jobs whose latest run failed, read live from GitHub (lib/workflow-health).
 * The freshness table below judges data; this judges the jobs, because a job
 * can fail while its table still looks fresh from another writer.
 */
export default async function FailingJobs() {
  const token = process.env.GITHUB_DISPATCH_TOKEN;
  let failing: FailingWorkflow[] = [];
  let error: string | null = null;
  if (!token) error = "The GitHub token is not configured on this deployment, so job status cannot be read.";
  else {
    try { failing = await readFailingWorkflows(token); }
    catch (reason) { error = `Could not read job status from GitHub: ${reason instanceof Error ? reason.message : String(reason)}`; }
  }

  return (
    <section aria-label="Failing jobs" className="rounded border bg-card p-3 text-sm">
      <h2 className="mb-2 font-semibold">Scheduled jobs whose last run failed</h2>
      {error ? (
        <p className="flex items-start gap-2 text-amber-700 dark:text-amber-400"><AlertTriangle className="mt-0.5 h-4 w-4 shrink-0" />{error}</p>
      ) : failing.length === 0 ? (
        <p className="flex items-center gap-2 text-emerald-700 dark:text-emerald-400"><CheckCircle2 className="h-4 w-4" />Every scheduled job&apos;s latest run in the last two days succeeded.</p>
      ) : (
        <ul className="space-y-1.5">
          {failing.map((job) => (
            <li key={job.workflow} className="flex flex-wrap items-baseline gap-x-3 gap-y-0.5">
              <span className="inline-flex items-center gap-1 rounded border border-rose-500/40 bg-rose-500/10 px-1.5 py-0.5 font-mono text-[10px] text-rose-700 dark:text-rose-400">
                <AlertTriangle className="h-3 w-3" /> FAILING
              </span>
              <span className="font-medium">{job.name}</span>
              <span className="text-xs text-muted-foreground">
                last failed {when(job.failedAt)}
                {job.streakCapped && job.streak > 1 ? ` · all of the last ${job.streak} runs failed (since at least ${when(job.failingSince)})`
                  : job.streak > 1 ? ` · ${job.streak} failures in a row since ${when(job.failingSince)}` : ""}
                {job.lastSuccessAt ? ` · last success ${when(job.lastSuccessAt)}` : " · no recent success"}
              </span>
              <a href={job.url} target="_blank" rel="noopener noreferrer" className="font-mono text-xs underline underline-offset-2">view run</a>
            </li>
          ))}
        </ul>
      )}
    </section>
  );
}
