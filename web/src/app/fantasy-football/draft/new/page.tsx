export const dynamic = "force-dynamic";

import { getFantasyHomeData } from "@/db/queries-fantasy-football";
import { createFantasyDraft } from "../../actions";

export default async function NewFantasyDraftPage() {
  const { rankingSets } = await getFantasyHomeData();
  return <div className="mx-auto max-w-3xl space-y-6"><div><p className="text-xs font-bold uppercase tracking-widest text-emerald-700">Draft setup</p><h1 className="text-3xl font-black">Create a snake draft</h1><p className="text-muted-foreground">Choose a pinned data snapshot. Rankings will not silently change after the draft starts.</p></div>
    {!rankingSets.length ? <div className="rounded-xl border border-amber-300 bg-amber-50 p-5">No ranking set exists yet. Run the Fantasy Football refresh workflow first.</div> :
    <form action={createFantasyDraft} className="grid gap-5 rounded-2xl border bg-card p-6 sm:grid-cols-2">
      <label className="sm:col-span-2"><span className="text-sm font-semibold">Draft name</span><input name="name" defaultValue="2026 Home League" maxLength={80} className="mt-1 w-full rounded-lg border bg-background p-2.5" /></label>
      <label><span className="text-sm font-semibold">Teams</span><input name="teamCount" type="number" min="8" max="14" defaultValue="12" className="mt-1 w-full rounded-lg border bg-background p-2.5" /></label>
      <label><span className="text-sm font-semibold">Your draft slot</span><input name="controlledSlot" type="number" min="1" max="14" defaultValue="1" className="mt-1 w-full rounded-lg border bg-background p-2.5" /></label>
      <label><span className="text-sm font-semibold">Rounds</span><input name="rounds" type="number" min="8" max="20" defaultValue="15" className="mt-1 w-full rounded-lg border bg-background p-2.5" /></label>
      <label><span className="text-sm font-semibold">Season</span><input name="season" type="number" min="2025" max="2030" defaultValue="2026" className="mt-1 w-full rounded-lg border bg-background p-2.5" /></label>
      <label><span className="text-sm font-semibold">Scoring</span><select name="scoring" defaultValue="PPR" className="mt-1 w-full rounded-lg border bg-background p-2.5"><option value="STD">Standard</option><option value="HALF">Half-PPR</option><option value="PPR">PPR</option></select></label>
      <label><span className="text-sm font-semibold">Ranking snapshot</span><select name="rankingSetId" defaultValue={rankingSets[0]?.id} className="mt-1 w-full rounded-lg border bg-background p-2.5">{rankingSets.map((set) => <option key={set.id} value={set.id}>{set.scoring} · {set.name}</option>)}</select></label>
      <button className="sm:col-span-2 rounded-xl bg-emerald-600 px-5 py-3 font-bold text-white hover:bg-emerald-500">Create draft room</button>
    </form>}
  </div>;
}
