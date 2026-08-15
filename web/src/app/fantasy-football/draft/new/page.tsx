export const dynamic = "force-dynamic";

import { getFantasyHomeData } from "@/db/queries-fantasy-football";
import { DraftSetupForm } from "./draft-setup-form";

export default async function NewFantasyDraftPage() {
  const { rankingSets } = await getFantasyHomeData();

  return <div className="mx-auto max-w-3xl space-y-6">
    <div>
      <p className="text-xs font-bold uppercase tracking-widest text-emerald-700">Draft setup</p>
      <h1 className="text-3xl font-black">Create a snake draft</h1>
      <p className="text-muted-foreground">Choose a pinned data snapshot. Rankings will not silently change after the draft starts.</p>
    </div>
    {!rankingSets.length
      ? <div className="rounded-xl border border-amber-300 bg-amber-50 p-5">No ranking set exists yet. Run the Fantasy Football refresh workflow first.</div>
      : <DraftSetupForm rankingSets={rankingSets} />}
  </div>;
}
