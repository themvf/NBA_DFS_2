"use client";

import { SHOWDOWN_SPLIT_REFERENCE, type LineupFilter, type PortfolioBreakdown } from "@/lib/nfl-dfs/portfolio-breakdown";

/**
 * The portfolio at a glance: captains, team splits (Showdown) or stacks
 * (Classic), quarterbacks and kickers. Every row is a button that filters the
 * lineup list when `onFilter` is given; the active row is highlighted.
 */
export default function PortfolioBreakdownPanel({ breakdown, filter, onFilter }: {
  breakdown: PortfolioBreakdown;
  filter?: LineupFilter;
  onFilter?: (next: LineupFilter) => void;
}) {
  const { total, format } = breakdown;
  const showdown = format === "showdown";
  // Clicking a row adds that condition to the current filter; clicking the active row removes it.
  const toggle = (patch: LineupFilter, active: boolean) =>
    onFilter?.({ ...filter, ...(active ? Object.fromEntries(Object.keys(patch).map((k) => [k, null])) : patch) });
  return <section className="grid gap-3 md:grid-cols-3" aria-label="Portfolio breakdown">
    {showdown ? <Card title="Captains" note={`${breakdown.captains.length} different · field share is projected captain ownership`}>
      {breakdown.captains.map((c) => {
        const active = filter?.captainId === c.dkPlayerId;
        return <Bar key={c.key} label={c.label} sub={`${c.position} ${c.team}`} count={c.count} pct={c.pct} total={total}
          reference={c.fieldPct} referenceLabel="field" active={active} onClick={onFilter ? () => toggle({ captainId: c.dkPlayerId }, active) : undefined} />;
      })}
    </Card> : null}
    <Card title={showdown ? "Team split" : "Stacks"} note={showdown ? "winners: top-100 lineups this season" : undefined}>
      {breakdown.splitKinds.map((s) => {
        const ref = showdown ? SHOWDOWN_SPLIT_REFERENCE[s.key] : undefined;
        const active = filter?.splitKind === s.key && !filter?.split;
        return <Bar key={s.key} label={s.label} count={s.count} pct={s.pct} total={total} reference={ref?.winners ?? null} referenceLabel="winners"
          fieldPct={ref?.field} active={active} onClick={onFilter ? () => toggle({ splitKind: s.key, split: null }, active) : undefined} />;
      })}
      {showdown && breakdown.splits.some((s) => s.key.includes(" ")) ? <div className="mt-2 border-t pt-2">
        <p className="mb-1 text-[10px] font-semibold uppercase tracking-wide text-slate-400">By team</p>
        {breakdown.splits.filter((s) => s.key.includes(" ")).map((s) => {
          const active = filter?.split === s.key;
          return <Bar key={s.key} label={s.label} count={s.count} pct={s.pct} total={total} compact active={active}
            onClick={onFilter ? () => toggle({ split: s.key, splitKind: null }, active) : undefined} />;
        })}
      </div> : null}
    </Card>
    <Card title="Structure">
      {breakdown.quarterbacks.map((q) => {
        const active = filter?.quarterbacks === q.key;
        return <Bar key={q.key} label={q.label} count={q.count} pct={q.pct} total={total} active={active}
          onClick={onFilter ? () => toggle({ quarterbacks: q.key as LineupFilter["quarterbacks"] }, active) : undefined} />;
      })}
      <Bar label="With a kicker" count={breakdown.kicker} pct={(100 * breakdown.kicker) / Math.max(1, total)} total={total} active={filter?.kicker === true}
        onClick={onFilter ? () => toggle({ kicker: true }, filter?.kicker === true) : undefined} />
      <Bar label="With a defense" count={breakdown.dst} pct={(100 * breakdown.dst) / Math.max(1, total)} total={total} />
      <div className="mt-2 border-t pt-2">
        <p className="mb-1 text-[10px] font-semibold uppercase tracking-wide text-slate-400">Roster spots by team</p>
        {breakdown.teamSlots.map((t) => <Bar key={t.key} label={t.label} count={t.count} pct={t.pct} total={total} compact countLabel={`${Math.round(t.pct)}%`} />)}
      </div>
    </Card>
  </section>;
}

function Card({ title, note, children }: { title: string; note?: string; children: React.ReactNode }) {
  return <div className="rounded-xl border bg-white p-3 shadow-sm">
    <h3 className="text-sm font-bold text-slate-900">{title}</h3>
    {note ? <p className="text-[10px] text-slate-500">{note}</p> : null}
    <div className="mt-2 max-h-72 space-y-1 overflow-auto">{children}</div>
  </div>;
}

function Bar({ label, sub, count, pct, total, reference, referenceLabel, fieldPct, active, onClick, compact, countLabel }: {
  label: string; sub?: string; count: number; pct: number; total: number;
  reference?: number | null; referenceLabel?: string; fieldPct?: number;
  active?: boolean; onClick?: () => void; compact?: boolean; countLabel?: string;
}) {
  const body = <>
    <div className="flex items-baseline justify-between gap-2 text-xs">
      <span className="truncate"><span className="font-semibold text-slate-800">{label}</span>{sub ? <span className="ml-1 text-[10px] text-slate-400">{sub}</span> : null}</span>
      <span className="shrink-0 tabular-nums text-slate-700">{countLabel ?? <><b>{Math.round(pct)}%</b> <span className="text-slate-400">{count}/{total}</span></>}</span>
    </div>
    <div className={`relative mt-0.5 rounded bg-slate-100 ${compact ? "h-1" : "h-1.5"}`}>
      <div className={`h-full rounded ${active ? "bg-blue-700" : "bg-blue-500"}`} style={{ width: `${Math.min(100, pct)}%` }} />
      {reference != null ? <div className="absolute -top-0.5 h-2.5 w-0.5 bg-amber-600" style={{ left: `${Math.min(100, reference)}%` }} title={`${referenceLabel}: ${reference.toFixed(0)}%`} /> : null}
    </div>
    {reference != null ? <p className="mt-0.5 text-[10px] text-slate-500"><span className="font-semibold text-amber-700">▏</span>{referenceLabel} {reference.toFixed(0)}%{fieldPct != null ? ` · field ${fieldPct}%` : ""}</p> : null}
  </>;
  if (!onClick) return <div className="rounded px-1.5 py-1">{body}</div>;
  return <button type="button" onClick={onClick} aria-pressed={active}
    className={`block w-full rounded px-1.5 py-1 text-left hover:bg-blue-50 ${active ? "bg-blue-50 ring-1 ring-blue-400" : ""}`}
    title={active ? "Clear this filter" : "Show only these lineups"}>{body}</button>;
}
