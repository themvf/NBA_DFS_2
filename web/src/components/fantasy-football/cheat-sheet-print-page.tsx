import Link from "next/link";
import { getFantasyRankings, getLatestRankingSet } from "@/db/queries-fantasy-football";
import { CHEAT_SHEET_VARIANTS, type CheatSheetVariant } from "@/lib/fantasy-football/cheat-sheet";
import CheatSheetView from "./cheat-sheet-view";

/**
 * Shared shell for every printable cheat sheet.
 *
 * All three variants read the SAME ranking snapshot through getFantasyRankings
 * -- the format changes which positions print and how deep, never the
 * projections underneath. Keeping one loader means a printed sheet can't drift
 * from the board it claims to represent.
 */
export default async function CheatSheetPrintPage({
  variant,
  scoring: requestedScoring,
  backHref,
  backLabel,
  printHref,
}: {
  variant: CheatSheetVariant;
  scoring?: string;
  backHref: string;
  backLabel: string;
  /** Base path for this sheet's own scoring links. */
  printHref: string;
}) {
  const requested = String(requestedScoring || "PPR").toUpperCase();
  const scoring = ["STD", "HALF", "PPR"].includes(requested) ? requested : "PPR";
  const set = await getLatestRankingSet(scoring);
  const rankings = set ? await getFantasyRankings(set.id) : [];

  if (!set) {
    return <div className="rounded-2xl border border-amber-300 bg-amber-50 p-6">
      No ranking snapshot available. Run the <code>Refresh Fantasy Football Draft Data</code> workflow.
    </div>;
  }

  const config = CHEAT_SHEET_VARIANTS[variant];
  return <div className="space-y-3">
    <div className="no-print flex flex-wrap items-center justify-between gap-3 rounded-2xl border bg-card p-3">
      <div>
        <p className="text-sm font-bold">{config.label}</p>
        <p className="text-xs text-muted-foreground">
          {config.context}. Print in <strong>landscape</strong> with background
          graphics off — fits one page.
        </p>
      </div>
      <div className="flex flex-wrap gap-2">
        {["STD", "HALF", "PPR"].map((value) => (
          <Link
            key={value}
            href={`${printHref}?scoring=${value}`}
            className={`rounded-lg px-3 py-2 text-sm font-bold ${value === scoring ? "bg-slate-900 text-white" : "border hover:bg-muted"}`}
          >
            {value}
          </Link>
        ))}
        <Link href={backHref} className="rounded-lg border px-3 py-2 text-sm font-semibold hover:bg-muted">
          {backLabel}
        </Link>
      </div>
    </div>

    <CheatSheetView
      rankings={rankings}
      setName={set.name}
      createdAt={String(set.createdAt)}
      scoring={scoring}
      season={set.season}
      variant={variant}
    />
  </div>;
}
