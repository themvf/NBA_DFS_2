export const dynamic = "force-dynamic";

import Link from "next/link";
import { AlertTriangle, CheckCircle2, CircleSlash, XCircle } from "lucide-react";
import { collectHealth, HEALTH_CHECK_EVERY_MINUTES, readStoredHealth, type StoredHealthItem } from "@/lib/health-collector";
import type { HealthGroup, HealthItem, HealthStatus } from "@/lib/health-checklist";

export const metadata = {
  title: "Health Checklist",
  description: "Every scheduled job, data feed, clock and NFL slate check, each with a pass/fail signal and when it was last and will next be checked.",
};

const STATUS: Record<HealthStatus, { word: string; chip: string; Icon: typeof CheckCircle2 }> = {
  fail: { word: "FAIL", chip: "border-rose-500/50 bg-rose-500/10 text-rose-700 dark:text-rose-400", Icon: XCircle },
  pass: { word: "PASS", chip: "border-emerald-500/50 bg-emerald-500/10 text-emerald-700 dark:text-emerald-400", Icon: CheckCircle2 },
  info: { word: "INFO", chip: "border-muted-foreground/30 bg-muted text-muted-foreground", Icon: CircleSlash },
};
const GROUPS: HealthGroup[] = ["NFL DFS", "Checklist", "Clocks", "Scheduled jobs", "Data freshness"];
const STALE_AFTER_MS = (HEALTH_CHECK_EVERY_MINUTES * 2 + 15) * 60_000;

type Row = HealthItem & { failingSince?: string | null };

function fmt(iso: string | null, now: number): { main: string; sub: string } {
  if (!iso) return { main: "—", sub: "" };
  const t = new Date(iso);
  if (!Number.isFinite(t.getTime())) return { main: iso, sub: "" };
  const main = t.toLocaleString("en-US", { timeZone: "America/New_York", month: "short", day: "numeric", hour: "numeric", minute: "2-digit" });
  const diff = t.getTime() - now, abs = Math.abs(diff);
  const span = abs < 3600_000 ? `${Math.round(abs / 60_000)} min` : abs < 172_800_000 ? `${(abs / 3600_000).toFixed(1)} h` : `${Math.round(abs / 86_400_000)} days`;
  return { main, sub: diff < 0 ? `${span} ago` : `in ${span}` };
}

function When({ iso, note, now, overdue }: { iso: string | null; note?: string | null; now: number; overdue?: boolean }) {
  if (!iso && note) return <span className="text-xs text-muted-foreground">{note}</span>;
  const { main, sub } = fmt(iso, now);
  return <span className="whitespace-nowrap"><span className="block text-xs">{main}</span>
    {sub ? <span className={`block text-[11px] ${overdue ? "font-semibold text-rose-700 dark:text-rose-400" : "text-muted-foreground"}`}>{sub}{overdue ? " (overdue)" : ""}</span> : null}</span>;
}

async function loadChecklist(): Promise<{ rows: Row[]; storedAt: number | null; live: boolean; error: string | null; now: number }> {
  const now = Date.now();
  let stored: StoredHealthItem[] = [];
  let error: string | null = null;
  try { stored = await readStoredHealth(); } catch (reason) { error = `The stored checklist could not be read: ${reason instanceof Error ? reason.message : String(reason)}`; }
  const storedAt = stored.reduce((m, r) => Math.max(m, Date.parse(r.runAt)), 0) || null;
  if (storedAt && now - storedAt <= STALE_AFTER_MS) return { rows: stored, storedAt, live: false, error, now };
  // The scheduled checker has not run recently (or never): check live so the page is never silently old.
  try {
    const rows = await collectHealth({ githubToken: process.env.GITHUB_DISPATCH_TOKEN || null, now: new Date(now) });
    return { rows, storedAt, live: true, error, now };
  } catch (reason) {
    return { rows: stored, storedAt, live: false, now, error: `${error ? `${error} ` : ""}A live check also failed: ${reason instanceof Error ? reason.message : String(reason)}` };
  }
}

export default async function HealthPage({ searchParams }: { searchParams: Promise<{ show?: string }> }) {
  const { show } = await searchParams;
  const { rows, storedAt, live, error, now } = await loadChecklist();
  const counts = { fail: rows.filter((r) => r.status === "fail").length, pass: rows.filter((r) => r.status === "pass").length, info: rows.filter((r) => r.status === "info").length };
  const shown = show === "fail" ? rows.filter((r) => r.status === "fail") : rows;
  const lastChecked = rows.reduce((m, r) => Math.max(m, Date.parse(r.lastCheckedAt)), 0);
  const nextCheck = storedAt ? storedAt + HEALTH_CHECK_EVERY_MINUTES * 60_000 : null;

  return (
    <div className="mx-auto max-w-[1400px] space-y-4 p-4">
      <header className="space-y-2">
        <h1 className="text-2xl font-bold tracking-tight">Health checklist</h1>
        <p className="max-w-3xl text-sm text-muted-foreground">
          Every scheduled job, data feed, clock and NFL slate check, one row each. Checked every {HEALTH_CHECK_EVERY_MINUTES} minutes;
          anything that fails is also emailed once a day by the failure sweep.
        </p>
      </header>

      {live || error ? (
        <div className="flex items-start gap-2 rounded border border-amber-500/40 bg-amber-500/10 p-3 text-sm text-amber-800 dark:text-amber-400">
          <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0" />
          <span>
            {live ? (storedAt ? `The scheduled checker has not run since ${fmt(new Date(storedAt).toISOString(), now).main} ET, so this page ran the checks itself just now.` : "The scheduled checker has never run, so this page ran the checks itself just now.") : null}
            {error ? ` ${error}` : null}
          </span>
        </div>
      ) : null}

      <div className="flex flex-wrap items-center gap-3 rounded border bg-card p-3 text-sm">
        <span className={`rounded border px-2 py-0.5 font-mono text-xs font-bold ${counts.fail ? STATUS.fail.chip : STATUS.pass.chip}`}>
          {counts.fail ? `${counts.fail} FAIL` : "ALL PASS"}
        </span>
        <span>{rows.length} items · {counts.pass} pass · {counts.fail} fail · {counts.info} info</span>
        <span className="text-xs text-muted-foreground">
          Last checked {lastChecked ? fmt(new Date(lastChecked).toISOString(), now).main : "—"} ET
          {nextCheck ? ` · next check ${fmt(new Date(Math.max(nextCheck, now)).toISOString(), now).main} ET` : ""}
        </span>
        <span className="ml-auto flex gap-2 text-xs">
          <Link href="/health" className={`rounded border px-2 py-1 ${show !== "fail" ? "bg-muted font-semibold" : ""}`}>All</Link>
          <Link href="/health?show=fail" className={`rounded border px-2 py-1 ${show === "fail" ? "bg-muted font-semibold" : ""}`}>Failing only</Link>
        </span>
      </div>

      {GROUPS.map((group) => {
        const items = shown.filter((r) => r.group === group);
        if (!items.length) return null;
        return (
          <section key={group} aria-label={group} className="overflow-x-auto rounded border bg-card">
            <h2 className="border-b px-3 py-2 text-sm font-semibold">{group} <span className="font-normal text-muted-foreground">({items.length})</span></h2>
            <table className="w-full min-w-[960px] text-left text-sm">
              <thead className="text-[11px] uppercase tracking-wider text-muted-foreground">
                <tr className="border-b">
                  <th className="w-20 p-2 font-medium">Status</th>
                  <th className="p-2 font-medium">Item</th>
                  <th className="p-2 font-medium">Detail</th>
                  <th className="w-32 p-2 font-medium">Last run</th>
                  <th className="w-32 p-2 font-medium">Next run</th>
                  <th className="w-32 p-2 font-medium">Last checked</th>
                  <th className="w-32 p-2 font-medium">Next check</th>
                </tr>
              </thead>
              <tbody>
                {items.map((r) => {
                  const { word, chip, Icon } = STATUS[r.status];
                  const nextRunOverdue = r.nextEventAt != null && Date.parse(r.nextEventAt) < now - 15 * 60_000;
                  const checkOverdue = r.nextCheckAt != null && Date.parse(r.nextCheckAt) < now - 15 * 60_000;
                  return (
                    <tr key={r.key} className="border-b align-top last:border-0">
                      <td className="p-2"><span className={`inline-flex items-center gap-1 rounded border px-1.5 py-0.5 font-mono text-[10px] font-bold ${chip}`}><Icon className="h-3 w-3" />{word}</span></td>
                      <td className="p-2 font-medium">
                        {r.url ? <a href={r.url} target={r.url.startsWith("/") ? undefined : "_blank"} rel="noopener noreferrer" className="underline decoration-muted-foreground/40 underline-offset-2">{r.label}</a> : r.label}
                        {r.failingSince ? <span className="block text-[11px] font-normal text-rose-700 dark:text-rose-400">failing since {fmt(r.failingSince, now).main} ET</span> : null}
                      </td>
                      <td className="p-2 text-xs text-muted-foreground">{r.detail}</td>
                      <td className="p-2"><When iso={r.lastEventAt} now={now} /></td>
                      <td className="p-2"><When iso={r.nextEventAt} note={r.nextEventNote} now={now} overdue={nextRunOverdue} /></td>
                      <td className="p-2"><When iso={r.lastCheckedAt} now={now} /></td>
                      <td className="p-2"><When iso={r.nextCheckAt} now={now} overdue={checkOverdue} /></td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </section>
        );
      })}
    </div>
  );
}
