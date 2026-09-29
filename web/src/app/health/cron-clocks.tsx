import { AlertTriangle, CheckCircle2, Clock } from "lucide-react";
import { cronStatuses, readCronHeartbeats, type CronStatus } from "@/lib/cron-heartbeat";

const CHIP: Record<CronStatus["state"], string> = {
  ok: "border-emerald-500/40 bg-emerald-500/10 text-emerald-700 dark:text-emerald-400",
  failing: "border-rose-500/40 bg-rose-500/10 text-rose-700 dark:text-rose-400",
  late: "border-rose-500/40 bg-rose-500/10 text-rose-700 dark:text-rose-400",
  never: "border-amber-500/40 bg-amber-500/10 text-amber-700 dark:text-amber-400",
};

function ago(iso: string | null): string {
  if (!iso) return "never";
  const minutes = Math.round((Date.now() - Date.parse(iso)) / 60_000);
  return minutes < 60 ? `${minutes} min ago` : minutes < 2880 ? `${(minutes / 60).toFixed(1)} h ago` : `${Math.round(minutes / 1440)} days ago`;
}

async function loadStatuses(): Promise<CronStatus[]> {
  return cronStatuses(await readCronHeartbeats(), Date.now());
}

/**
 * The Vercel cron routes that drive every scheduled job. Each records its last
 * run (lib/cron-heartbeat); a route that fails or stops being called shows here.
 */
export default async function CronClocks() {
  let statuses: CronStatus[] = [];
  let error: string | null = null;
  try { statuses = await loadStatuses(); }
  catch (reason) { error = `Could not read the cron heartbeats: ${reason instanceof Error ? reason.message : String(reason)}`; }
  return (
    <section aria-label="Scheduled clocks" className="rounded border bg-card p-3 text-sm">
      <h2 className="mb-2 font-semibold">Scheduled clocks (Vercel cron)</h2>
      {error ? <p className="flex items-start gap-2 text-amber-700 dark:text-amber-400"><AlertTriangle className="mt-0.5 h-4 w-4 shrink-0" />{error}</p> : (
        <ul className="space-y-1.5">
          {statuses.map((s) => (
            <li key={s.route} className="flex flex-wrap items-baseline gap-x-3 gap-y-0.5">
              <span className={`inline-flex items-center gap-1 rounded border px-1.5 py-0.5 font-mono text-[10px] ${CHIP[s.state]}`}>
                {s.state === "ok" ? <CheckCircle2 className="h-3 w-3" /> : s.state === "never" ? <Clock className="h-3 w-3" /> : <AlertTriangle className="h-3 w-3" />}
                {s.state.toUpperCase()}
              </span>
              <span className="font-medium">{s.label}</span>
              <span className="text-xs text-muted-foreground">
                last run {ago(s.heartbeat?.lastRunAt ?? null)}
                {s.state !== "ok" && s.heartbeat?.lastOkAt ? ` · last success ${ago(s.heartbeat.lastOkAt)}` : ""}
              </span>
              {s.state !== "ok" ? <span className="w-full text-xs text-muted-foreground">{s.text}</span> : null}
            </li>
          ))}
        </ul>
      )}
    </section>
  );
}
