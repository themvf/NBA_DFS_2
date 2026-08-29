export const dynamic = "force-dynamic";

import Link from "next/link";
import { getLatestRankingSet, getNoteEditorPlayers } from "@/db/queries-fantasy-football";
import NotesEditor from "./notes-editor";

export default async function FantasyPlayerNotesPage() {
  // Notes are per-player, not per-scoring-format, so the board only supplies the
  // player universe and the rank/ADP shown for context. PPR is the reference
  // board because it is the deepest of the three.
  const set = await getLatestRankingSet("PPR");
  const players = set ? await getNoteEditorPlayers(set.id) : [];
  const withNotes = players.filter((player) => player.notes.length > 0).length;
  const noteCount = players.reduce((total, player) => total + player.notes.length, 0);

  return <div className="space-y-5">
    <div className="flex flex-wrap items-end justify-between gap-4">
      <div>
        <p className="text-xs font-bold uppercase tracking-widest text-violet-700">Admin</p>
        <h1 className="text-3xl font-black">Player Notes</h1>
        <p className="text-sm text-muted-foreground">
          {set
            ? `${noteCount} notes across ${withNotes} of ${players.length} players · board ${set.name}`
            : "No ranking snapshot available"}
        </p>
      </div>
      <div className="flex gap-2">
        <Link href="/fantasy-football/rankings" className="rounded-lg border px-3 py-2 text-sm font-bold hover:bg-muted">Rankings</Link>
        <Link href="/fantasy-football/redraft" className="rounded-lg border px-3 py-2 text-sm font-bold hover:bg-muted">Redraft</Link>
        <Link href="/fantasy-football/best-ball" className="rounded-lg border px-3 py-2 text-sm font-bold hover:bg-muted">Best Ball</Link>
      </div>
    </div>

    <div className="rounded-2xl border border-violet-200 bg-violet-50 p-4 text-sm text-violet-950/80">
      <p><b>What a note does.</b> It renders as a chip beside the player&apos;s name on the redraft board, the Best Ball board, and the Best Ball Shadow panel, with the full text in the hover tooltip.</p>
      <p className="mt-1"><b>What it does not do.</b> Nothing here feeds a projection, VOR, rank, ADP, or draft order &mdash; the board is ordered identically whether or not a player has a note. It is commentary you are writing to yourself.</p>
    </div>

    {!set
      ? <div className="rounded-2xl border border-amber-300 bg-amber-50 p-6">Run the <code>Refresh Fantasy Football Draft Data</code> GitHub workflow to populate the board first.</div>
      : <NotesEditor players={players} />}
  </div>;
}
