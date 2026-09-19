export const dynamic = "force-dynamic";

import Link from "next/link";
import { AlertTriangle, CheckCircle2, CircleSlash, HelpCircle } from "lucide-react";
import { getPipelineHealth, type PipelineHealthRow } from "@/db/queries";

export const metadata = {
  title: "Pipeline Health",
  description:
    "Is every scheduled job still writing data? Judged on the data itself rather than workflow status, because status lies in both directions.",
};

const ORDER: Record<PipelineHealthRow["status"], number> = { stale: 0, empty: 1, fresh: 2, dormant: 3 };

const STYLE: Record<PipelineHealthRow["status"], { chip: string; Icon: typeof AlertTriangle; word: string }> = {
  stale: { chip: "border-rose-500/40 bg-rose-500/10 text-rose-700 dark:text-rose-400", Icon: AlertTriangle, word: "STALE" },
  empty: { chip: "border-rose-500/40 bg-rose-500/10 text-rose-700 dark:text-rose-400", Icon: HelpCircle, word: "EMPTY" },
  fresh: { chip: "border-emerald-500/40 bg-emerald-500/10 text-emerald-700 dark:text-emerald-400", Icon: CheckCircle2, word: "OK" },
  dormant: { chip: "border-muted-foreground/30 bg-muted text-muted-foreground", Icon: CircleSlash, word: "DORMANT" },
};

function age(hours: number | null): string {
  if (hours === null) return "—";
  if (hours < 1) return `${Math.round(hours * 60)}m`;
  if (hours < 48) return `${hours.toFixed(1)}h`;
  return `${(hours / 24).toFixed(1)}d`;
}

export default async function HealthPage() {
  const health = await getPipelineHealth();
  const rows = [...health.rows].sort(
    (a, b) => ORDER[a.status] - ORDER[b.status] || a.label.localeCompare(b.label),
  );
  const needing = rows.filter((r) => r.status === "stale" || r.status === "empty");
  // A freshness monitor that has itself stopped is the one failure it cannot
  // otherwise report, so it reports on itself first.
  const monitorStale = health.monitorAgeHours !== null && health.monitorAgeHours > 12;

  return (
    <div className="mx-auto max-w-[1100px] space-y-4 p-4">
      <header className="space-y-2">
        <h1 className="text-2xl font-bold tracking-tight">Pipeline Health</h1>
        <p className="max-w-3xl text-sm text-muted-foreground">
          Whether each scheduled job is still writing data — judged on{" "}
          <strong className="font-semibold text-foreground">the data itself</strong>, not on whether
          its workflow went green. Status lies in both directions: a workflow can pass while writing
          nothing, and fail while writing everything that matters.
        </p>
      </header>

      {health.checkedAt === null ? (
        <div className="rounded border border-dashed p-6 text-sm">
          <p className="font-semibold">The monitor has never run.</p>
          <p className="mt-1 text-muted-foreground">
            Run the <code className="rounded bg-muted px-1 font-mono">Pipeline Health</code> workflow,
            or <code className="rounded bg-muted px-1 font-mono">python -m model.pipeline_health --record</code>.
          </p>
        </div>
      ) : (
        <>
          {monitorStale && (
            <div className="flex items-start gap-2 rounded border border-amber-500/40 bg-amber-500/10 p-3 text-sm text-amber-700 dark:text-amber-400">
              <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0" />
              <span>
                These readings are {age(health.monitorAgeHours)} old — the health check itself has
                stopped running, so nothing below can be trusted as current.
              </span>
            </div>
          )}

          <div className="rounded border bg-card p-3 text-sm">
            {needing.length === 0 ? (
              <span className="font-semibold text-emerald-700 dark:text-emerald-400">
                All {rows.length} pipelines are writing on schedule.
              </span>
            ) : (
              <span className="font-semibold text-rose-700 dark:text-rose-400">
                {needing.length} of {rows.length} pipelines need attention.
              </span>
            )}
            <span className="ml-2 font-mono text-xs text-muted-foreground">
              checked {health.checkedAt.slice(0, 16).replace("T", " ")}
            </span>
          </div>

          <div className="overflow-x-auto rounded border bg-card">
            <table className="w-full text-left text-sm">
              <thead className="text-xs uppercase tracking-wider text-muted-foreground">
                <tr className="border-b">
                  <th className="w-28 p-2 font-medium">Status</th>
                  <th className="p-2 font-medium">Dataset</th>
                  <th className="w-24 p-2 text-right font-medium">Last write</th>
                  <th className="w-20 p-2 text-right font-medium">Budget</th>
                  <th className="p-2 font-medium">Owner</th>
                </tr>
              </thead>
              <tbody>
                {rows.map((row) => {
                  const { chip, Icon, word } = STYLE[row.status];
                  return (
                    <tr key={row.datasetKey} className="border-b last:border-0 align-top">
                      <td className="p-2">
                        <span className={`inline-flex items-center gap-1 rounded border px-1.5 py-0.5 font-mono text-[10px] ${chip}`}>
                          <Icon className="h-3 w-3" /> {word}
                        </span>
                      </td>
                      <td className="p-2">
                        <div className="font-medium">{row.label}</div>
                        {row.note && <div className="text-xs text-muted-foreground">{row.note}</div>}
                        <div className="font-mono text-[11px] text-muted-foreground">{row.table}</div>
                      </td>
                      <td className="p-2 text-right font-mono tabular-nums">{age(row.ageHours)}</td>
                      <td className="p-2 text-right font-mono tabular-nums text-muted-foreground">
                        {row.maxAgeHours}h
                      </td>
                      <td className="p-2">
                        <Link
                          href={`https://github.com/themvf/NBA_DFS_2/actions/workflows/${row.ownerWorkflow}`}
                          className="font-mono text-xs underline underline-offset-2"
                        >
                          {row.ownerWorkflow}
                        </Link>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>

          <p className="text-xs text-muted-foreground">
            <strong className="text-foreground">Dormant</strong> means out of season, not broken — a
            sport between seasons is expected to stop writing, and flagging it would make this page
            noise within a week. Budgets are deliberately loose: GitHub&apos;s scheduler runs up to
            95 minutes late and drops overnight slots, so a threshold at the nominal interval would
            cry wolf constantly.
          </p>
        </>
      )}
    </div>
  );
}
