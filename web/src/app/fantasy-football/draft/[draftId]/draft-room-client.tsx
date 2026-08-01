"use client";

import { useMemo, useState, useTransition } from "react";
import { useRouter } from "next/navigation";
import type { FantasyDraftState, FantasyRankingRow } from "@/db/queries-fantasy-football";
import { picksUntilControlled } from "@/lib/fantasy-football/draft-engine";
import { recommendPlayers } from "@/lib/fantasy-football/recommendations";
import { recordFantasyPick, undoFantasyPick } from "../../actions";

const badgeStyle: Record<string, string> = { fact: "bg-blue-100 text-blue-800", role: "bg-violet-100 text-violet-800", risk: "bg-red-100 text-red-800", model: "bg-emerald-100 text-emerald-800" };

function PlayerBadges({ player }: { player: FantasyRankingRow }) {
  return <div className="mt-1 flex flex-wrap gap-1">{player.indicators.slice(0, 3).map((badge) => <span key={badge.code} title={JSON.stringify(badge.evidence)} className={`rounded-full px-1.5 py-0.5 text-[9px] font-bold ${badgeStyle[badge.class]??"bg-slate-100"}`}>{badge.label}</span>)}</div>;
}

export default function DraftRoomClient({ initialState }: { initialState: FantasyDraftState }) {
  const router = useRouter();
  const [search, setSearch] = useState("");
  const [position, setPosition] = useState("ALL");
  const [error, setError] = useState<string | null>(null);
  const [pending, startTransition] = useTransition();
  const { draft, board, available } = initialState;
  const current = board.find((slot) => slot.overallPick === draft.currentPick);
  const until = picksUntilControlled(draft.currentPick, draft.controlledSlot, draft.teamCount, draft.roundCount) ?? 0;
  const myRoster = board.filter((slot) => slot.isControlled && slot.playerId);
  const recommendations = useMemo(() => recommendPlayers(available.map((player) => ({ playerId: player.playerId, position: player.position, ourRank: player.ourRank, ecr: player.ecr, adp: player.adp, projectedPoints: player.ourProjectedPoints, tier: player.tier, confidence: player.confidence })), myRoster.flatMap((slot) => slot.position ? [slot.position] : []), until).slice(0, 5), [available, myRoster, until]);
  const filtered = available.filter((player) => (position === "ALL" || player.position === position) && (!search || `${player.name} ${player.team} ${player.position}`.toLowerCase().includes(search.toLowerCase()))).slice(0, 150);

  const draftPlayer = (playerId: number) => startTransition(async () => {
    setError(null);
    const result = await recordFantasyPick({ draftId: draft.id, playerId, revision: draft.revision });
    if (!result.ok) setError(result.error ?? "Pick failed"); else router.refresh();
  });
  const undo = () => startTransition(async () => {
    setError(null);
    const result = await undoFantasyPick({ draftId: draft.id, revision: draft.revision });
    if (!result.ok) setError(result.error ?? "Undo failed"); else router.refresh();
  });

  return <div className="space-y-5">
    <header className="sticky top-14 z-30 rounded-2xl border bg-background/95 p-4 shadow-sm backdrop-blur">
      <div className="flex flex-wrap items-center justify-between gap-4"><div><p className="text-xs font-bold uppercase tracking-widest text-emerald-700">{draft.status}</p><h1 className="text-2xl font-black">{draft.name}</h1></div><div className="flex gap-6 text-center"><div><p className="text-xs text-muted-foreground">On clock</p><p className="font-black">{current?.teamName??"Complete"}</p></div><div><p className="text-xs text-muted-foreground">Current pick</p><p className="font-black">{Math.min(draft.currentPick, board.length)} / {board.length}</p></div><div><p className="text-xs text-muted-foreground">Until yours</p><p className="font-black">{until}</p></div><button disabled={pending || !board.some((slot) => slot.eventId)} onClick={undo} className="rounded-lg border px-3 py-2 text-sm font-semibold disabled:opacity-40">Undo</button></div></div>
      {error && <p className="mt-3 rounded-lg bg-red-50 p-2 text-sm font-semibold text-red-700">{error}</p>}
    </header>

    <section className="rounded-2xl border bg-card p-4"><div className="mb-3 flex items-center justify-between"><h2 className="font-bold">Draft board</h2><p className="text-xs text-muted-foreground">Your team is highlighted</p></div><div className="overflow-x-auto"><div className="grid min-w-max gap-1" style={{ gridTemplateColumns: `repeat(${draft.teamCount}, minmax(125px, 1fr))` }}>{board.map((slot) => <div key={slot.overallPick} className={`min-h-20 rounded-lg border p-2 text-xs ${slot.isControlled?"border-emerald-400 bg-emerald-50":"bg-background"} ${slot.overallPick===draft.currentPick?"ring-2 ring-amber-400":""}`}><p className="flex justify-between text-[10px] text-muted-foreground"><span>{slot.round}.{slot.pickInRound}</span><span>{slot.teamName}</span></p>{slot.playerName ? <><p className="mt-2 font-bold">{slot.playerName}</p><p className="text-muted-foreground">{slot.position} · {slot.nflTeam}</p></> : <p className="mt-3 text-center text-muted-foreground">Available</p>}</div>)}</div></div></section>

    <div className="grid gap-5 xl:grid-cols-[1fr_310px]">
      <section className="rounded-2xl border bg-card"><div className="flex flex-wrap gap-2 border-b p-4"><input value={search} onChange={(event) => setSearch(event.target.value)} placeholder="Search player, team, position" className="min-w-64 flex-1 rounded-lg border bg-background px-3 py-2" />{["ALL","QB","RB","WR","TE","K","DST"].map((value) => <button key={value} onClick={() => setPosition(value)} className={`rounded-lg px-2.5 py-2 text-xs font-bold ${position===value?"bg-slate-900 text-white":"border"}`}>{value}</button>)}</div><div className="max-h-[720px] overflow-auto">{filtered.map((player) => { const rank=player.ourRank??player.ecr; const delta=player.adp!==null&&rank!==null?player.adp-rank:null; return <div key={player.playerId} className="grid grid-cols-[45px_1fr_auto_auto] items-center gap-3 border-b p-3 hover:bg-muted/50"><p className="text-center text-lg font-black">{rank??"—"}</p><div><p className="font-bold">{player.name} <span className="text-xs font-normal text-muted-foreground">{player.position} · {player.team??"FA"}</span></p><PlayerBadges player={player} /></div><div className="text-right text-xs"><p>ADP <b>{player.adp?.toFixed(1)??"—"}</b></p><p className={delta!==null&&delta>=0?"text-emerald-700":"text-red-700"}>{delta===null?"":`${delta>=0?"+":""}${delta.toFixed(1)} value`}</p></div><button disabled={pending || draft.status!=="active"} onClick={() => draftPlayer(player.playerId)} className="rounded-lg bg-emerald-600 px-3 py-2 text-xs font-bold text-white disabled:opacity-40">Draft</button></div>})}</div></section>
      <aside className="space-y-5"><section className="rounded-2xl border bg-card p-4"><h2 className="font-bold">Recommendations</h2><div className="mt-3 space-y-3">{recommendations.map((rec, index) => { const player=available.find((item) => item.playerId===rec.playerId)!; return <button key={rec.playerId} disabled={pending} onClick={() => draftPlayer(rec.playerId)} className="w-full rounded-xl border p-3 text-left hover:border-emerald-400 hover:bg-emerald-50"><p className="text-xs font-black text-emerald-700">#{index+1} · Score {rec.score.toFixed(1)}</p><p className="font-bold">{player.name} <span className="text-xs text-muted-foreground">{player.position}</span></p><p className="mt-1 text-xs text-muted-foreground">{rec.explanation.join(" · ")}</p><PlayerBadges player={player} /></button>})}</div></section><section className="rounded-2xl border bg-card p-4"><h2 className="font-bold">My roster</h2><div className="mt-2 space-y-2">{myRoster.length?myRoster.map((slot) => <div key={slot.overallPick} className="flex justify-between rounded-lg bg-muted p-2 text-sm"><b>{slot.playerName}</b><span>{slot.position}</span></div>):<p className="text-sm text-muted-foreground">No players drafted yet.</p>}</div></section></aside>
    </div>
    <p className="text-center text-xs text-muted-foreground">Rankings and projections powered by FantasyPros. “Our Rank” and recommendations are independent application outputs.</p>
  </div>;
}
