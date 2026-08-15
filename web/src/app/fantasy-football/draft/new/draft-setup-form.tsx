"use client";

import { useState } from "react";
import { createFantasyDraft } from "../../actions";
import { calculateRosterSize, ROSTER_PRESETS, SCORING_PRESETS } from "@/lib/fantasy-football/league-config";
import type { FantasyRankingSetSummary } from "@/db/queries-fantasy-football";

export function DraftSetupForm({ rankingSets }: { rankingSets: FantasyRankingSetSummary[] }) {
  const [scoring, setScoring] = useState<keyof typeof SCORING_PRESETS>("HALF");
  const [roster, setRoster] = useState<keyof typeof ROSTER_PRESETS>("hood-rivals");
  const [teamCount, setTeamCount] = useState(10);
  const [controlledSlot, setControlledSlot] = useState(1);
  const rounds = calculateRosterSize(ROSTER_PRESETS[roster].config);
  const matchingRankingSets = rankingSets.filter((set) => set.scoring === scoring && set.season === 2026);
  const rankingSetId = matchingRankingSets[0]?.id;
  const updateTeamCount = (value: number) => {
    const nextCount = Number.isFinite(value) ? value : 8;
    setTeamCount(nextCount);
    setControlledSlot((current) => Math.min(current, nextCount));
  };

  return <form action={createFantasyDraft} className="grid gap-5 rounded-2xl border bg-card p-6 sm:grid-cols-2">
    <label className="sm:col-span-2"><span className="text-sm font-semibold">Draft name</span><input name="name" defaultValue="2026 Home League" maxLength={80} className="mt-1 w-full rounded-lg border bg-background p-2.5" /></label>
    <label><span className="text-sm font-semibold">Teams</span><input name="teamCount" type="number" min="8" max="14" value={teamCount} onChange={(event) => updateTeamCount(event.currentTarget.valueAsNumber)} className="mt-1 w-full rounded-lg border bg-background p-2.5" /></label>
    <label><span className="text-sm font-semibold">Your draft slot</span><input name="controlledSlot" type="number" min="1" max={teamCount} value={controlledSlot} onChange={(event) => setControlledSlot(event.currentTarget.valueAsNumber || 1)} className="mt-1 w-full rounded-lg border bg-background p-2.5" /></label>
    <label><span className="text-sm font-semibold">Rounds</span><input name="rounds" type="number" value={rounds} readOnly aria-describedby="rounds-help" className="mt-1 w-full rounded-lg border bg-muted p-2.5" /><p id="rounds-help" className="mt-1 text-xs text-muted-foreground">Derived from the selected roster&apos;s active and bench slots; IR is not drafted.</p></label>
    <label><span className="text-sm font-semibold">Season</span><input name="season" type="number" min="2026" max="2026" value={2026} readOnly className="mt-1 w-full rounded-lg border bg-muted p-2.5" /></label>
    <label><span className="text-sm font-semibold">Roster format</span><select name="roster" value={roster} onChange={(event) => setRoster(event.target.value as keyof typeof ROSTER_PRESETS)} className="mt-1 w-full rounded-lg border bg-background p-2.5">{Object.entries(ROSTER_PRESETS).map(([key, preset]) => <option key={key} value={key}>{preset.name}</option>)}</select><p className="mt-1 text-xs text-muted-foreground">{ROSTER_PRESETS[roster].description}</p></label>
    <label><span className="text-sm font-semibold">Scoring</span><select name="scoring" value={scoring} onChange={(event) => setScoring(event.target.value as keyof typeof SCORING_PRESETS)} className="mt-1 w-full rounded-lg border bg-background p-2.5">{Object.entries(SCORING_PRESETS).map(([key, preset]) => <option key={key} value={key}>{preset.name}</option>)}</select><p className="mt-1 text-xs text-muted-foreground">{SCORING_PRESETS[scoring].description}</p></label>
    <label className="sm:col-span-2"><span className="text-sm font-semibold">Ranking snapshot</span><select name="rankingSetId" value={rankingSetId ?? ""} disabled={!rankingSetId} className="mt-1 w-full rounded-lg border bg-background p-2.5">{matchingRankingSets.map((set) => <option key={set.id} value={set.id}>{set.scoring} · {set.name}</option>)}</select>{!rankingSetId && <p className="mt-1 text-xs text-amber-700">No 2026 {SCORING_PRESETS[scoring].name} ranking snapshot is available.</p>}</label>
    <button disabled={!rankingSetId} className="sm:col-span-2 rounded-xl bg-emerald-600 px-5 py-3 font-bold text-white hover:bg-emerald-500 disabled:cursor-not-allowed disabled:opacity-50">Create draft room</button>
  </form>;
}
