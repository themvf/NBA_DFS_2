"use client";

import { useMemo, useState, useTransition } from "react";
import { useRouter } from "next/navigation";
import type { FantasyDraftState, FantasyRankingRow } from "@/db/queries-fantasy-football";
import { nextControlledPick, picksUntilControlled } from "@/lib/fantasy-football/draft-engine";
import { recommendPlayers } from "@/lib/fantasy-football/recommendations";
import { computeAvailabilityOdds, type AvailabilityOdds } from "@/lib/fantasy-football/availability-odds";
import { fantasyBadgeClass } from "@/lib/fantasy-football/badge-style";
import { recordFantasyPick, undoFantasyPick } from "../../actions";
import { adjustAdpForRoster, getAdjustmentDescription, isRosterDifferentFromBaseline } from "@/lib/fantasy-football/adp-adjustment";

function AvailabilityBadge({ odds }: { odds: AvailabilityOdds | null | undefined }) {
  if (!odds) return <p className="text-[10px] text-muted-foreground">Avail —</p>;
  const pct = Math.round(odds.probability * 100);
  const tone = pct >= 66 ? "text-emerald-700" : pct >= 33 ? "text-amber-700" : "text-red-700";
  return (
    <p className={`text-[10px] font-semibold ${tone}`} title={`FFC ADP ${odds.adjustedAdp.toFixed(1)} ± ${odds.adjustedStdev.toFixed(1)} picks · ${odds.sampleSize ?? "few"} drafts sampled (${odds.confidence} confidence)`}>
      {pct}% avail. at your pick
    </p>
  );
}

function PlayerBadges({ player }: { player: FantasyRankingRow }) {
  return <div className="mt-1 flex flex-wrap gap-1">{player.indicators.slice(0, 3).map((badge) => <span key={badge.code} title={JSON.stringify(badge.evidence)} className={`rounded-full px-1.5 py-0.5 text-[9px] font-bold ring-1 ring-inset ${fantasyBadgeClass(badge)}`}>{badge.label}</span>)}</div>;
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
  const nextPick = nextControlledPick(draft.currentPick, draft.controlledSlot, draft.teamCount, draft.roundCount) ?? draft.currentPick;
  const myRoster = board.filter((slot) => slot.isControlled && slot.playerId);
  
  // Check if roster is different from baseline (to show adjustments)
  const showAdjustments = useMemo(() => 
    isRosterDifferentFromBaseline(draft.rosterConfig as any, draft.teamCount),
    [draft.rosterConfig, draft.teamCount]
  );
  
  const adjustmentDesc = useMemo(() => 
    getAdjustmentDescription(draft.rosterConfig as any, draft.teamCount),
    [draft.rosterConfig, draft.teamCount]
  );
  
  // Apply ADP adjustments to available players
  const availableWithAdjustments = useMemo(() => 
    available.map((player) => {
      const adjustedAdp = adjustAdpForRoster(
        player.adp,
        player.position,
        draft.rosterConfig as any,
        draft.teamCount
      );
      return {
        ...player,
        adjustedAdp,
        adpDelta: player.adp && adjustedAdp ? adjustedAdp - player.adp : null,
      };
    }),
    [available, draft.rosterConfig, draft.teamCount]
  );
  
  // Real per-player P(still available at nextPick | still available now), from
  // FFC's ADP mean/stdev/sample size -- see availability-odds.ts. Computed once
  // per render and reused for both the recommendation ranking and the row badges
  // so the two surfaces never disagree. Use adjusted ADP if available.
  const availabilityById = useMemo(() => new Map(availableWithAdjustments.map((player) => [
    player.playerId,
    computeAvailabilityOdds(
      { adp: player.adjustedAdp ?? player.adp, adpStdev: player.adpStdev, adpSampleSize: player.adpSampleSize },
      { currentPick: draft.currentPick, targetPick: nextPick, teamCount: draft.teamCount },
    ),
  ])), [availableWithAdjustments, draft.currentPick, nextPick, draft.teamCount]);
  
  const recommendations = useMemo(() => recommendPlayers(availableWithAdjustments.map((player) => ({ playerId: player.playerId, position: player.position, ourRank: player.ourRank, ecr: player.ecr, adp: player.adjustedAdp ?? player.adp, projectedPoints: player.ourProjectedPoints, tier: player.tier, confidence: player.confidence, availabilityProbability: availabilityById.get(player.playerId)?.probability ?? null })), myRoster.flatMap((slot) => slot.position ? [slot.position] : []), until).slice(0, 5), [availableWithAdjustments, myRoster, until, availabilityById]);
  const filtered = availableWithAdjustments.filter((player) => (position === "ALL" || player.position === position) && (!search || `${player.name} ${player.team} ${player.position}`.toLowerCase().includes(search.toLowerCase()))).slice(0, 150);

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
      <div className="flex flex-wrap items-center justify-between gap-4"><div><p className="text-xs font-bold uppercase tracking-widest text-emerald-700">{draft.status}</p><h1 className="text-2xl font-black">{draft.name}</h1></div><div className="flex gap-6 text-center"><div><p className="text-xs text-muted-foreground">On clock</p><p className="font-black">{current?.teamName??"Complete"}</p></div><div><p className="text-xs text-muted-foreground">Current pick</p><p className="font-black">{Math.min(draft.currentPick, board.length)} / {board.length}</p></div><div><p className="text-xs text-muted-foreground">Until yours</p><p className="font-black">{until}<span className="ml-1 text-xs font-normal text-muted-foreground">(#{nextPick})</span></p></div><button disabled={pending || !board.some((slot) => slot.eventId)} onClick={undo} className="rounded-lg border px-3 py-2 text-sm font-semibold disabled:opacity-40">Undo</button></div></div>
      {error && <p className="mt-3 rounded-lg bg-red-50 p-2 text-sm font-semibold text-red-700">{error}</p>}
      {showAdjustments && <p className="mt-3 rounded-lg bg-blue-50 p-2 text-xs text-blue-700">📊 {adjustmentDesc}</p>}
    </header>

    <section className="rounded-2xl border bg-card p-4"><div className="mb-3 flex items-center justify-between"><h2 className="font-bold">Draft board</h2><p className="text-xs text-muted-foreground">Your team is highlighted</p></div><div className="overflow-x-auto"><div className="grid min-w-max gap-1" style={{ gridTemplateColumns: `repeat(${draft.teamCount}, minmax(125px, 1fr))` }}>{board.map((slot) => <div key={slot.overallPick} className={`min-h-20 rounded-lg border p-2 text-xs ${slot.isControlled?"border-emerald-400 bg-emerald-50":"bg-background"} ${slot.overallPick===draft.currentPick?"ring-2 ring-amber-400":""}`}><p className="flex justify-between text-[10px] text-muted-foreground"><span>{slot.round}.{slot.pickInRound}</span><span>{slot.teamName}</span></p>{slot.playerName ? <><p className="mt-2 font-bold">{slot.playerName}</p><p className="text-muted-foreground">{slot.position} · {slot.nflTeam}</p></> : <p className="mt-3 text-center text-muted-foreground">Available</p>}</div>)}</div></div></section>

    <div className="grid gap-5 xl:grid-cols-[1fr_310px]">
      <section className="rounded-2xl border bg-card"><div className="flex flex-wrap gap-2 border-b p-4"><input value={search} onChange={(event) => setSearch(event.target.value)} placeholder="Search player, team, position" className="min-w-64 flex-1 rounded-lg border bg-background px-3 py-2" />{["ALL","QB","RB","WR","TE","K","DST"].map((value) => <button key={value} onClick={() => setPosition(value)} className={`rounded-lg px-2.5 py-2 text-xs font-bold ${position===value?"bg-slate-900 text-white":"border"}`}>{value}</button>)}</div><div className="max-h-[720px] overflow-auto">{filtered.map((player) => { const rank=player.ourRank??player.ecr; const displayAdp = showAdjustments ? player.adjustedAdp : player.adp; const delta=displayAdp!==null&&rank!==null?displayAdp-rank:null; return <div key={player.playerId} className="grid grid-cols-[45px_1fr_auto_auto] items-center gap-3 border-b p-3 hover:bg-muted/50"><p className="text-center text-lg font-black">{rank??"—"}</p><div><p className="font-bold">{player.name} <span className="text-xs font-normal text-muted-foreground">{player.position} · {player.team??"FA"}</span></p><PlayerBadges player={player} /></div><div className="text-right text-xs">{showAdjustments && player.adpDelta !== null && Math.abs(player.adpDelta) >= 0.5 ? <><p className="text-[10px] text-muted-foreground line-through">ADP {player.adp?.toFixed(1)}</p><p>Adj <b className={player.adpDelta < 0 ? "text-emerald-700" : "text-amber-700"}>{displayAdp?.toFixed(1)}</b> <span className="text-[9px]">({player.adpDelta > 0 ? "+" : ""}{player.adpDelta.toFixed(1)})</span></p></> : <p>ADP <b>{displayAdp?.toFixed(1)??"—"}</b></p>}<p className={delta!==null&&delta>=0?"text-emerald-700":"text-red-700"}>{delta===null?"":`${delta>=0?"+":""}${delta.toFixed(1)} value`}</p><AvailabilityBadge odds={availabilityById.get(player.playerId)} /></div><button disabled={pending || draft.status!=="active"} onClick={() => draftPlayer(player.playerId)} className="rounded-lg bg-emerald-600 px-3 py-2 text-xs font-bold text-white disabled:opacity-40">Draft</button></div>})}</div></section>
      <aside className="space-y-5"><section className="rounded-2xl border bg-card p-4"><h2 className="font-bold">Recommendations</h2><div className="mt-3 space-y-3">{recommendations.map((rec, index) => { const player=availableWithAdjustments.find((item) => item.playerId===rec.playerId)!; return <button key={rec.playerId} disabled={pending} onClick={() => draftPlayer(rec.playerId)} className="w-full rounded-xl border p-3 text-left hover:border-emerald-400 hover:bg-emerald-50"><p className="text-xs font-black text-emerald-700">#{index+1} · Score {rec.score.toFixed(1)}</p><p className="font-bold">{player.name} <span className="text-xs text-muted-foreground">{player.position}</span></p><p className="mt-1 text-xs text-muted-foreground">{rec.explanation.join(" · ")}</p><PlayerBadges player={player} /></button>})}</div></section><section className="rounded-2xl border bg-card p-4"><h2 className="font-bold">My roster</h2><div className="mt-2 space-y-2">{myRoster.length?myRoster.map((slot) => <div key={slot.overallPick} className="flex justify-between rounded-lg bg-muted p-2 text-sm"><b>{slot.playerName}</b><span>{slot.position}</span></div>):<p className="text-sm text-muted-foreground">No players drafted yet.</p>}</div></section></aside>
    </div>
    <p className="text-center text-xs text-muted-foreground">Our independent rankings use nflverse and Sleeper data. {showAdjustments ? "ADP adjusted for your roster format. " : ""}Availability odds are a directional estimate from FFC&apos;s observed ADP mean, variance, and sample size (rescaled to this draft&apos;s team count) &mdash; not a validated betting-style model.</p>
  </div>;
}
