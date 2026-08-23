export const dynamic = "force-dynamic";

import Link from "next/link";
import { getFantasyRankings, getLatestRankingSet } from "@/db/queries-fantasy-football";
import CheatSheetView from "./cheat-sheet-view";

export default async function CheatSheetPage({
  searchParams,
}: {
  searchParams: Promise<{ scoring?: string }>;
}) {
  const params = await searchParams;
  const requested = String(params.scoring || "PPR").toUpperCase();
  const scoring = ["STD", "HALF", "PPR"].includes(requested) ? requested : "PPR";
  const set = await getLatestRankingSet(scoring);
  const rankings = set ? await getFantasyRankings(set.id) : [];

  if (!set) {
    return <div className="rounded-2xl border border-amber-300 bg-amber-50 p-6">
      No ranking snapshot available. Run the <code>Refresh Fantasy Football Draft Data</code> workflow.
    </div>;
  }

  return <div className="space-y-3">
    <div className="no-print flex flex-wrap items-center justify-between gap-3 rounded-2xl border bg-card p-3">
      <div>
        <p className="text-sm font-bold">Printable cheat sheet</p>
        <p className="text-xs text-muted-foreground">
          Print with <strong>landscape</strong> orientation and background graphics
          off. Fits one page.
        </p>
      </div>
      <div className="flex gap-2">
        {["STD", "HALF", "PPR"].map((value) => (
          <Link
            key={value}
            href={`/fantasy-football/rankings/print?scoring=${value}`}
            className={`rounded-lg px-3 py-2 text-sm font-bold ${value === scoring ? "bg-slate-900 text-white" : "border hover:bg-muted"}`}
          >
            {value}
          </Link>
        ))}
        <Link href={`/fantasy-football/rankings?scoring=${scoring}`} className="rounded-lg border px-3 py-2 text-sm font-semibold hover:bg-muted">
          Back to board
        </Link>
      </div>
    </div>

    <CheatSheetView
      rankings={rankings}
      setName={set.name}
      createdAt={String(set.createdAt)}
      scoring={scoring}
      season={set.season}
    />
  </div>;
}
