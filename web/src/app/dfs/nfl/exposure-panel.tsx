"use client";

import { useMemo, useState } from "react";
import type { NflExposureReport, NflGeneratedLineup, NflOptimizerPlayer } from "./nfl-optimizer";
import { fieldOwnershipSource } from "@/lib/nfl-dfs/portfolio-breakdown";
import type { CaptainTarget, ExposureTarget } from "@/lib/nfl-dfs/generation-settings";

type Sort = "exposure" | "over" | "under" | "captain";
type FieldPlayer = Pick<NflOptimizerPlayer, "dkPlayerId" | "name" | "team" | "position" | "ownPct" | "captainOwnPct"> & { ownSource?: string | null };

/** Caps for the next build, set from this table; the build form holds the same values. */
export interface NextBuildCaps {
  targets: Record<string, ExposureTarget>;
  captainTargets: Record<string, CaptainTarget>;
  onTarget: (dkPlayerId: number, next: ExposureTarget) => void;
  onCaptainTarget: (dkPlayerId: number, next: CaptainTarget) => void;
  /** Jump to the build step to rebuild with the caps. */
  onRebuild?: () => void;
  disabled?: boolean;
}

/**
 * Exposure against the field: each player's share of our lineups (overall and,
 * in Showdown, as captain) beside the projected field ownership the build
 * used, with the gap. Counted from the lineups themselves, so it works for a
 * restored run whose optimizer report was not kept; the report, when present,
 * adds the caps and binding notes.
 */
export default function ExposurePanel({ lineups, players, report, format, caps, title = "Exposure" }: {
  lineups: NflGeneratedLineup[];
  players: readonly FieldPlayer[];
  report?: NflExposureReport[];
  format: "classic" | "showdown";
  caps?: NextBuildCaps;
  title?: string;
}) {
  const source = useMemo(() => fieldOwnershipSource(players, format), [players, format]);
  const showdown = format === "showdown";
  const total = lineups.length;
  const [sort, setSort] = useState<Sort>("exposure");
  const [team, setTeam] = useState("");
  const [position, setPosition] = useState("");
  const [showUnused, setShowUnused] = useState(false);
  const reportById = useMemo(() => new Map((report ?? []).map((r) => [r.dkPlayerId, r])), [report]);
  const rows = useMemo(() => {
    const counts = new Map<number, { overall: number; captain: number }>();
    for (const l of lineups) for (const { slot, player } of l.slots) {
      const c = counts.get(player.dkPlayerId) ?? { overall: 0, captain: 0 };
      c.overall += 1; if (slot === "CPT") c.captain += 1;
      counts.set(player.dkPlayerId, c);
    }
    return players.map((p) => {
      const c = counts.get(p.dkPlayerId) ?? { overall: 0, captain: 0 };
      const ours = total ? (100 * c.overall) / total : 0;
      const field = p.ownPct ?? null;
      return {
        player: p, count: c.overall, captainCount: c.captain, ours,
        captainPct: total ? (100 * c.captain) / total : 0,
        field, captainField: p.captainOwnPct ?? null,
        gap: field == null ? null : ours - field,
        report: reportById.get(p.dkPlayerId),
      };
    });
  }, [lineups, players, total, reportById]);
  const teams = useMemo(() => [...new Set(players.map((p) => p.team))].sort(), [players]);
  const positions = useMemo(() => [...new Set(players.map((p) => p.position))].sort(), [players]);
  const shown = rows
    .filter((r) => (showUnused ? r.count > 0 || (r.field ?? 0) >= 5 : r.count > 0))
    .filter((r) => (!team || r.player.team === team) && (!position || r.player.position === position))
    .sort((a, b) => sort === "over" ? (b.gap ?? -999) - (a.gap ?? -999)
      : sort === "under" ? (a.gap ?? 999) - (b.gap ?? 999)
      : sort === "captain" ? b.captainCount - a.captainCount || b.count - a.count
      : b.count - a.count);
  const select = "rounded border bg-white px-1.5 py-1 text-[11px]";
  return <section className="rounded-xl border bg-white p-4 shadow-sm">
    <h2 className="font-bold">{title} <span className="text-xs font-normal text-slate-400">ours vs field ({source.short}) · {total} lineups</span></h2>
    <p className="mt-1 rounded border border-amber-200 bg-amber-50 px-2 py-1 text-[11px] text-amber-900">{source.note}</p>
    <div className="mt-2 flex flex-wrap items-center gap-1.5">
      <select aria-label="Sort exposure" className={select} value={sort} onChange={(e) => setSort(e.target.value as Sort)}>
        <option value="exposure">Most used</option>
        <option value="over">Most over the field</option>
        <option value="under">Most under the field</option>
        {showdown ? <option value="captain">Most captained</option> : null}
      </select>
      <select aria-label="Team" className={select} value={team} onChange={(e) => setTeam(e.target.value)}>
        <option value="">All teams</option>{teams.map((t) => <option key={t}>{t}</option>)}
      </select>
      <select aria-label="Position" className={select} value={position} onChange={(e) => setPosition(e.target.value)}>
        <option value="">All positions</option>{positions.map((p) => <option key={p}>{p}</option>)}
      </select>
      <label className="flex items-center gap-1 text-[11px] text-slate-600"><input type="checkbox" checked={showUnused} onChange={(e) => setShowUnused(e.target.checked)} />Include unused players the field plays</label>
    </div>
    <div className="mt-2 max-h-96 overflow-auto">
      <table className="w-full text-[11px]">
        <thead className="sticky top-0 bg-white text-left text-slate-500"><tr>
          <th className="py-1">Player</th><th className="text-right">Ours</th><th className="text-right" title={showdown ? "Field ownership estimate, captain plus flex" : "Field ownership estimate"}>Field {source.short}</th><th className="text-right">Gap</th>
          {showdown ? <><th className="text-right">CPT ours</th><th className="text-right">CPT field {source.short}</th></> : null}
          {caps ? <th className="pl-2 text-right" title="Maximum share of lineups for the next build. Blank = automatic.">Next max %</th> : null}
          {caps && showdown ? <th className="pl-1 text-right" title="Maximum share of lineups with him at captain, next build. Blank = automatic.">CPT max %</th> : null}
        </tr></thead>
        <tbody>{shown.map((r) => {
          const missed = r.report?.binding && /missed/.test(r.report.binding);
          return <tr key={r.player.dkPlayerId} className={`border-t align-top ${missed ? "bg-red-50" : ""}`}>
            <td className="py-1 pr-2">
              <span className="font-semibold text-slate-800">{r.player.name}</span> <span className="text-slate-400">{r.player.position} {r.player.team}</span>
              <div className="mt-0.5 h-1 w-full max-w-[160px] rounded bg-slate-100"><div className="h-full rounded bg-blue-600" style={{ width: `${Math.min(100, r.ours)}%` }} /></div>
              {r.report?.binding ? <div className={missed ? "text-red-700" : "text-slate-400"}>{r.report.binding}</div> : null}
            </td>
            <td className="text-right tabular-nums"><b>{Math.round(r.ours)}%</b> <span className="text-slate-400">{r.count}</span></td>
            <td className="text-right tabular-nums text-slate-600">{r.field == null ? "—" : `${Math.round(r.field)}%`}</td>
            <td className={`text-right tabular-nums font-semibold ${r.gap == null ? "text-slate-400" : r.gap >= 10 ? "text-blue-700" : r.gap <= -10 ? "text-amber-700" : "text-slate-500"}`}>{r.gap == null ? "—" : `${r.gap > 0 ? "+" : ""}${Math.round(r.gap)}`}</td>
            {showdown ? <>
              <td className="text-right tabular-nums">{r.captainCount ? <><b>{Math.round(r.captainPct)}%</b> <span className="text-slate-400">{r.captainCount}</span></> : "—"}</td>
              <td className="text-right tabular-nums text-slate-600">{r.captainField == null ? "—" : `${Math.round(r.captainField)}%`}</td>
            </> : null}
            {caps ? <td className="pl-2 text-right"><CapInput label={`Next-build max exposure for ${r.player.name}`} disabled={caps.disabled}
              value={caps.targets[String(r.player.dkPlayerId)]?.max ?? null}
              onChange={(max) => caps.onTarget(r.player.dkPlayerId, { min: caps.targets[String(r.player.dkPlayerId)]?.min ?? null, max })} /></td> : null}
            {caps && showdown ? <td className="pl-1 text-right"><CapInput label={`Next-build captain max for ${r.player.name}`} disabled={caps.disabled}
              value={caps.captainTargets[String(r.player.dkPlayerId)]?.max ?? null}
              onChange={(max) => caps.onCaptainTarget(r.player.dkPlayerId, { min: caps.captainTargets[String(r.player.dkPlayerId)]?.min ?? null, max })} /></td> : null}
          </tr>;
        })}</tbody>
      </table>
      {!shown.length ? <p className="py-4 text-center text-xs text-slate-500">No players match.</p> : null}
    </div>
    <p className="mt-2 text-[10px] text-slate-500">Field is the ownership estimate this build used{showdown ? " (captain plus flex, so it compares with Ours)" : ""}. Gap = ours minus field, in percentage points: blue means we play him at least 10 more, amber at least 10 less.</p>
    {caps ? <div className="mt-2 flex flex-wrap items-center gap-2 text-[11px] text-slate-600">
      <span>Type a max to cap a player in the next build ({Object.values(caps.targets).filter((t) => t.max != null).length} capped{showdown ? `, ${Object.values(caps.captainTargets).filter((t) => t.max != null).length} captain caps` : ""}). The build form shows the same caps.</span>
      {caps.onRebuild ? <button type="button" onClick={caps.onRebuild} className="rounded border border-blue-300 bg-white px-2 py-1 font-semibold text-blue-800">Rebuild with these caps →</button> : null}
    </div> : null}
  </section>;
}

/** A max % box that commits on blur or Enter, so typing "45" does not apply "4" first. */
function CapInput({ label, value, onChange, disabled }: { label: string; value: number | null; onChange: (max: number | null) => void; disabled?: boolean }) {
  const [draft, setDraft] = useState<string | null>(null);
  const commit = () => {
    if (draft == null) return;
    const text = draft.trim();
    const n = Number(text);
    onChange(text === "" || !Number.isFinite(n) ? null : Math.max(0, Math.min(100, Math.round(n))));
    setDraft(null);
  };
  return <input aria-label={label} title={label} type="number" min={0} max={100} disabled={disabled} placeholder="auto"
    className="w-14 rounded border px-1 py-0.5 text-right text-[11px] disabled:bg-slate-50"
    value={draft ?? (value == null ? "" : String(value))}
    onChange={(e) => setDraft(e.target.value)} onBlur={commit} onKeyDown={(e) => { if (e.key === "Enter") commit(); }} />;
}
