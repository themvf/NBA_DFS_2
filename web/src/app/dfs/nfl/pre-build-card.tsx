"use client";

import type { LockCountdown } from "@/lib/nfl-dfs/lock-countdown";

export type AllowedCheapPlayer = { id: number; name: string | null; at: string | null };

/**
 * What the next build will do, read before pressing Generate: time to lock,
 * the settings in one line, the cheap players you allowed earlier (they carry
 * over between builds), and the entry file that sets the lineup count.
 */
export default function PreBuildCard({ countdown, summary, nLineups, allowed, onClearAllowed, entryFileName, entryRows, onChooseEntryFile, onSetLineups, disabled }: {
  countdown: LockCountdown | null;
  summary: string;
  nLineups: number;
  allowed: AllowedCheapPlayer[];
  onClearAllowed: () => void;
  entryFileName: string | null;
  /** Entries in the chosen file that each need their own lineup; null until a file is read. */
  entryRows: number | null;
  onChooseEntryFile: () => void;
  onSetLineups: (n: number) => void;
  disabled: boolean;
}) {
  const here = allowed.filter((p) => p.name);
  const elsewhere = allowed.length - here.length;
  const day = (at: string | null) => (at && Number.isFinite(Date.parse(at))
    ? new Date(at).toLocaleDateString("en-US", { timeZone: "America/New_York", weekday: "short", month: "short", day: "numeric" }) : null);
  return <div className="space-y-2 rounded-lg border border-slate-200 bg-slate-50 p-3 text-xs text-slate-800">
    {countdown ? <p className={`font-semibold ${countdown.state === "closing" ? "text-red-800" : countdown.state === "locked" ? "text-slate-500" : "text-slate-900"}`}>{countdown.text}{countdown.state === "closing" ? " A rebuild may not fit." : ""}</p> : null}
    <p><b>This build:</b> {nLineups} lineup{nLineups === 1 ? "" : "s"} · {summary}</p>
    {allowed.length ? <div className="rounded border border-amber-300 bg-amber-50 p-2 text-amber-950">
      <p><b>Cheap players you allowed earlier still apply:</b> {here.length ? here.map((p) => `${p.name}${day(p.at) ? ` (${day(p.at)})` : ""}`).join(", ") : "none on this slate"}{elsewhere ? `${here.length ? "; plus " : "; "}${elsewhere} from another slate` : ""}.</p>
      <button type="button" disabled={disabled} onClick={onClearAllowed} className="mt-1 min-h-9 rounded border border-amber-400 bg-white px-2 font-semibold disabled:opacity-40">Clear all {allowed.length}</button>
    </div> : null}
    <div className="flex flex-wrap items-center gap-2">
      <button type="button" disabled={disabled} onClick={onChooseEntryFile} className="min-h-9 rounded border bg-white px-2 font-semibold disabled:opacity-40">{entryFileName ? "Change entry file" : "Choose DraftKings entry file"}</button>
      <span className="text-slate-600">{entryFileName
        ? entryRows == null ? `${entryFileName}: reading…`
          : entryRows === nLineups ? `${entryFileName}: ${entryRows} entries, matches the lineup count.`
          : `${entryFileName}: ${entryRows} entries need a lineup, the build is set to ${nLineups}.`
        : "Optional now: it sets the lineup count to match your entries."}</span>
      {entryRows != null && entryRows !== nLineups && entryRows >= 1 && entryRows <= 150
        ? <button type="button" disabled={disabled} onClick={() => onSetLineups(entryRows)} className="min-h-9 rounded bg-blue-700 px-2 font-bold text-white disabled:opacity-40">Set lineups to {entryRows}</button> : null}
    </div>
  </div>;
}
