"use client";

import { useEffect, useState } from "react";
import CompetitorPanel from "../competitor-panel";
import AbsencePreview from "../absence-preview";
import { listSavedNflSlates, loadSavedNflWorkspace, type NflWorkspaceSlate } from "../actions";

export default function ResearchClient({ uploadId }: { uploadId: string | null }) {
  const [slates, setSlates] = useState<Awaited<ReturnType<typeof listSavedNflSlates>>>([]);
  const [selected, setSelected] = useState<string>("");
  const [slate, setSlate] = useState<NflWorkspaceSlate | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    listSavedNflSlates().then((saved) => {
      setSlates(saved);
      let remembered: string | null = null;
      try { remembered = localStorage.getItem("nfl-saved-slate"); } catch { /* optional */ }
      const pick = saved.find((s) => s.uploadId === uploadId) ?? saved.find((s) => s.uploadId === remembered) ?? saved[0];
      if (pick) setSelected(pick.uploadId);
    }).catch(() => setError("Saved slates could not be listed."));
  }, [uploadId]);

  useEffect(() => {
    if (!selected) return;
    let live = true;
    loadSavedNflWorkspace(selected).then((next) => { if (live) { setSlate(next.slate); setError(null); } })
      .catch((reason) => { if (live) setError(reason instanceof Error ? reason.message : "That slate could not be loaded."); });
    return () => { live = false; };
  }, [selected]);

  return <div className="space-y-4">
    <label className="block max-w-md text-xs font-bold text-slate-700">
      <span className="mb-1 block">Slate</span>
      <select className="min-h-10 w-full rounded-lg border bg-white px-3 text-sm" value={selected} onChange={(e) => { setSlate(null); setError(null); setSelected(e.target.value); }}>
        {slates.map((s) => <option key={s.uploadId} value={s.uploadId}>{s.label}</option>)}
      </select>
    </label>
    {error ? <p role="alert" className="rounded-lg border border-red-300 bg-red-50 p-3 text-sm text-red-900">{error}</p> : null}
    {!slate && !error ? <p className="text-sm text-slate-500">Loading slate…</p> : null}
    {slate ? <>
      <CompetitorPanel key={`${slate.uploadId}-benchmark`} slate={slate} />
      <AbsencePreview key={`${slate.uploadId}-${slate.players[0]?.availability?.evaluatedAt}`} slate={slate} />
    </> : null}
  </div>;
}
