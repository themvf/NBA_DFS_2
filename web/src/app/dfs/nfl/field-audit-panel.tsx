"use client";

import { useEffect, useRef, useState, useTransition } from "react";
import { FileUp } from "lucide-react";
import { importNflContestResults, readNflFieldAudit } from "./actions";
import { parseContestExport, DESCRIPTIVE_ONLY_BELOW, type FieldAudit } from "@/lib/nfl-dfs/field-audit";

type Contest = { contestId: string; entryCount: number; winningScore: number | null; medianScore: number | null; format: string };

/**
 * What did the field know that we did not?
 *
 * DraftKings publishes field ownership in the contest standings export, after
 * the slate is over. Too late to pick a lineup with; exactly in time to show
 * where our information was behind.
 *
 * The file is ~64 MB, so it is parsed here in the browser and only the
 * ~850-row ownership summary is sent to the server.
 */
export default function FieldAuditPanel({ uploadId }: { uploadId: string }) {
  const fileRef = useRef<HTMLInputElement>(null);
  const [audit, setAudit] = useState<FieldAudit | null>(null);
  const [contest, setContest] = useState<Contest | null>(null);
  const [message, setMessage] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [pending, startTransition] = useTransition();

  useEffect(() => {
    let live = true;
    setAudit(null); setContest(null); setMessage(null); setError(null);
    readNflFieldAudit(uploadId)
      .then((found) => { if (live && found) { setAudit(found.audit); setContest(found.contest); } })
      .catch(() => { /* nothing imported yet is the normal case */ });
    return () => { live = false; };
  }, [uploadId]);

  function upload(file: File | null) {
    if (!file) return;
    setError(null); setMessage(null);
    startTransition(async () => {
      try {
        const parsed = parseContestExport(await file.text());
        // DraftKings names the file contest-standings-<id>.csv.
        const contestId = (file.name.match(/contest-standings-(\d+)/)?.[1] ?? file.name.replace(/\.csv$/i, "")).trim();
        const result = await importNflContestResults(uploadId, contestId, parsed, file.name);
        setAudit(result.audit); setContest(result.contest);
        setMessage(`Imported ${result.contest.entryCount.toLocaleString()} entries · ${parsed.players.length} players with ownership · ${Math.round(result.overlap * 100)}% matched this slate.`);
      } catch (reason) {
        setError(reason instanceof Error ? reason.message : "That contest file could not be read.");
      }
    });
  }

  const s = audit?.summary;
  return <section className="rounded-xl border border-slate-200 bg-white p-5">
    <div className="flex flex-wrap items-start justify-between gap-3">
      <div>
        <h2 className="font-bold">What the field knew</h2>
        <p className="mt-1 max-w-2xl text-sm text-slate-600">
          Upload the DraftKings contest standings export after the slate. Ownership arrives too late to pick a
          lineup with, but it shows where our information was behind the market. This is <strong>not</strong> a
          reason to fade chalk — the winners of these contests carried more ownership than we did.
        </p>
      </div>
      <input ref={fileRef} type="file" accept=".csv,text/csv" className="hidden" onChange={(e) => upload(e.target.files?.[0] ?? null)} />
      <button type="button" disabled={pending} onClick={() => fileRef.current?.click()}
        className="inline-flex min-h-11 items-center gap-2 rounded-lg bg-blue-600 px-4 text-sm font-bold text-white disabled:opacity-50">
        <FileUp className="h-4 w-4" />{pending ? "Reading…" : contest ? "Replace contest file" : "Upload contest results"}
      </button>
    </div>

    {error ? <p className="mt-3 rounded-lg border border-red-300 bg-red-50 p-3 text-sm text-red-900">{error}</p> : null}
    {message ? <p className="mt-3 rounded-lg border border-emerald-300 bg-emerald-50 p-3 text-sm text-emerald-900">{message}</p> : null}

    {!audit || !s ? <p className="mt-4 text-sm text-slate-500">No contest imported for this slate yet.</p> : <>
      <div className="mt-4 grid grid-cols-2 gap-3 md:grid-cols-4">
        {[["Contest", contest ? `${contest.entryCount.toLocaleString()} entries` : "—"],
          ["Winning score", contest?.winningScore?.toFixed(2) ?? "—"],
          ["Our blind spots", String(s.marketKnew)],
          ["Real edges", String(s.realEdge)]].map(([label, value]) =>
          <div key={label} className="rounded bg-slate-50 p-3"><strong className="text-xl">{value}</strong><p className="text-xs">{label}</p></div>)}
      </div>

      {s.flagged === 0
        ? <p className="mt-4 text-sm">Nothing flagged: no player we ranked highly was ignored by the field.</p>
        : <div className="mt-4 overflow-auto">
            <table className="w-full min-w-[640px] text-left text-sm">
              <thead><tr className="text-xs uppercase text-slate-500">
                {["Player", "Pos", "Salary", "Our proj", "Field %", "Actual", ""].map((h) => <th key={h} className="p-2">{h}</th>)}
              </tr></thead>
              <tbody>{audit.flagged.map((r) => <tr key={`${r.name}-${r.position}`} className="border-t">
                <td className="p-2 font-medium">{r.name}</td>
                <td className="p-2">{r.position}</td>
                <td className="p-2">${r.salary.toLocaleString()}</td>
                <td className="p-2">{r.ourProj.toFixed(1)}</td>
                <td className="p-2">{r.fieldPct.toFixed(2)}%</td>
                <td className="p-2">{r.actual === null ? "—" : r.actual.toFixed(1)}</td>
                <td className="p-2"><span className={`rounded px-2 py-0.5 text-xs font-bold ${r.verdict === "MARKET_KNEW" ? "bg-red-100 text-red-800" : "bg-emerald-100 text-emerald-800"}`}>
                  {r.verdict === "MARKET_KNEW" ? "market knew" : "real edge"}</span></td>
              </tr>)}</tbody>
            </table>
          </div>}

      <p className="mt-3 text-xs text-slate-500">
        {s.projectedPointsOnMarketKnew} projected points went to players the field had already written off.
        {" "}Considered {s.considered} players on this slate.
        {s.flagged < DESCRIPTIVE_ONLY_BELOW
          ? ` Fewer than ${DESCRIPTIVE_ONLY_BELOW} flagged — read this as description, not a rate. Keep importing each week.`
          : ""}
      </p>
    </>}
  </section>;
}
