"use client";

import { useEffect, useMemo, useRef, useState, useTransition } from "react";
import { useRouter } from "next/navigation";
import type { FantasyDraftState, FantasyRankingRow } from "@/db/queries-fantasy-football";
import { nextControlledPick, picksUntilControlled } from "@/lib/fantasy-football/draft-engine";
import { recommendPlayers } from "@/lib/fantasy-football/recommendations";
import { computeAvailabilityOdds, type AvailabilityOdds } from "@/lib/fantasy-football/availability-odds";
import { buildDraftPlan } from "@/lib/fantasy-football/draft-plan";
import { DRAFT_STRATEGY_META, isDraftStrategy, type DraftStrategy } from "@/lib/fantasy-football/draft-strategy";
import { fantasyBadgeClass } from "@/lib/fantasy-football/badge-style";
import { advanceComputerDraft, recordFantasyPick, undoFantasyPick } from "../../actions";
import { getScoringDescription, type RosterConfig, type ScoringConfig } from "@/lib/fantasy-football/league-config";
import { getAdjustmentDescription, adjustAdpForRoster, isRosterDifferentFromBaseline } from "@/lib/fantasy-football/adp-adjustment";
import { DraftIntelligencePanel, type PickReceipt } from "./draft-intelligence-panel";
import InjuryMarker from "@/components/fantasy-football/injury-marker";

function AvailabilityBadge({ odds, targetPick }: { odds: AvailabilityOdds | null | undefined; targetPick: number | null }) {
  if (!odds || targetPick === null) return <p className="text-[10px] text-muted-foreground">Avail —</p>;
  const pct = Math.round(odds.probability * 100);
  const tone = pct >= 66 ? "text-emerald-700" : pct >= 33 ? "text-amber-700" : "text-red-700";
  return <p className={`text-[10px] font-semibold ${tone}`} title={`FFC ADP ${odds.adjustedAdp.toFixed(1)} ± ${odds.adjustedStdev.toFixed(1)} picks · ${odds.sampleSize ?? "few"} drafts sampled (${odds.confidence} confidence)`}>{pct}% avail. at #{targetPick}</p>;
}

function PlayerBadges({ player }: { player: FantasyRankingRow }) {
  const hasInjuryMarker = Boolean(player.injuryStatus || player.injuryDetails);
  const visibleIndicators = player.indicators.filter((badge) => badge.code !== "INJURY");
  return <div className="mt-1 flex flex-wrap gap-1">
    <InjuryMarker injuryStatus={player.injuryStatus} details={player.injuryDetails ?? null} compact />
    {visibleIndicators.slice(0, hasInjuryMarker ? 2 : 3).map((badge) => <span key={badge.code} title={JSON.stringify(badge.evidence)} className={`rounded-full px-1.5 py-0.5 text-[9px] font-bold ring-1 ring-inset ${fantasyBadgeClass(badge)}`}>{badge.label}</span>)}
  </div>;
}

export default function DraftRoomClient({ initialState }: { initialState: FantasyDraftState }) {
  const router = useRouter();
  const [search, setSearch] = useState("");
  const [position, setPosition] = useState("ALL");
  const [error, setError] = useState<string | null>(null);
  const [receipt, setReceipt] = useState<PickReceipt | null>(null);
  const [pending, startTransition] = useTransition();
  const [autoRetry, setAutoRetry] = useState(0);
  const autoAdvanceKey = useRef<string | null>(null);
  const { draft, board, available } = initialState;
  const rosterConfig = draft.rosterConfig as unknown as RosterConfig;
  const scoringDescription = getScoringDescription(draft.scoringConfig as unknown as ScoringConfig);
  const strategyValue = draft.recommendationConfig.strategy;
  const strategy: DraftStrategy = typeof strategyValue === "string" && isDraftStrategy(strategyValue) ? strategyValue : "balanced";
  const simulatorRaw = draft.recommendationConfig.simulator;
  const simulator = simulatorRaw && typeof simulatorRaw === "object" ? simulatorRaw as Record<string, unknown> : null;
  const simulatorEnabled = simulator?.enabled === true;
  const current = board.find((slot) => slot.overallPick === draft.currentPick);
  const computerTurn = simulatorEnabled && draft.status === "active" && Boolean(current && !current.isControlled);
  const canDraftRecommendation = current?.isControlled === true;
  const canUndo = simulatorEnabled
    ? board.some((slot) => slot.isControlled && slot.eventId)
    : board.some((slot) => slot.eventId);
  const until = picksUntilControlled(draft.currentPick, draft.controlledSlot, draft.teamCount, draft.roundCount) ?? 0;
  const targetPick = nextControlledPick(draft.currentPick, draft.controlledSlot, draft.teamCount, draft.roundCount);
  const nextPick = targetPick ?? draft.currentPick;
  const futurePick = targetPick === null ? null : nextControlledPick(targetPick + 1, draft.controlledSlot, draft.teamCount, draft.roundCount);
  const myRoster = board.filter((slot) => slot.isControlled && slot.playerId);
  const showAdjustments = useMemo(() => isRosterDifferentFromBaseline(rosterConfig, draft.teamCount), [rosterConfig, draft.teamCount]);
  const adjustmentDesc = useMemo(() => getAdjustmentDescription(rosterConfig, draft.teamCount), [rosterConfig, draft.teamCount]);

  const availableWithAdjustments = useMemo(() => available.map((player) => {
    const adjustedAdp = adjustAdpForRoster(player.adp, player.position, rosterConfig, draft.teamCount);
    return { ...player, adjustedAdp, adpDelta: player.adp !== null && adjustedAdp !== null ? adjustedAdp - player.adp : null };
  }), [available, rosterConfig, draft.teamCount]);
  const availabilityById = useMemo(() => new Map(availableWithAdjustments.map((player) => [player.playerId, targetPick === null ? null : computeAvailabilityOdds(
    { adp: player.adjustedAdp ?? player.adp, adpStdev: player.adpStdev, adpSampleSize: player.adpSampleSize },
    { currentPick: draft.currentPick, targetPick, teamCount: draft.teamCount },
  )])), [availableWithAdjustments, draft.currentPick, targetPick, draft.teamCount]);
  const nextTurnAvailabilityById = useMemo(() => new Map(availableWithAdjustments.map((player) => [player.playerId, futurePick === null ? null : computeAvailabilityOdds(
    { adp: player.adjustedAdp ?? player.adp, adpStdev: player.adpStdev, adpSampleSize: player.adpSampleSize },
    { currentPick: draft.currentPick, targetPick: futurePick, teamCount: draft.teamCount },
  )])), [availableWithAdjustments, draft.currentPick, futurePick, draft.teamCount]);
  const recommendationInputs = useMemo(() => availableWithAdjustments.map((player) => ({
    playerId: player.playerId, position: player.position, ourRank: player.ourRank, ecr: player.ecr, adp: player.adjustedAdp ?? player.adp,
    projectedPoints: player.ourProjectedPoints, tier: player.tier, confidence: player.confidence, availabilityProbability: availabilityById.get(player.playerId)?.probability ?? null,
  })), [availableWithAdjustments, availabilityById]);
  const recommendations = useMemo(() => recommendPlayers(recommendationInputs, myRoster.flatMap((slot) => slot.position ? [slot.position] : []), until, rosterConfig), [recommendationInputs, myRoster, until, rosterConfig]);
  const draftPlan = useMemo(() => buildDraftPlan({
    recommendations,
    players: availableWithAdjustments.map((player) => ({ playerId: player.playerId, name: player.name, position: player.position, projectedPoints: player.ourProjectedPoints, projectionLow: player.projectionLow, projectionHigh: player.projectionHigh, tier: player.tier, confidence: player.confidence, availabilityProbability: availabilityById.get(player.playerId)?.probability ?? null, nextTurnAvailabilityProbability: nextTurnAvailabilityById.get(player.playerId)?.probability ?? null })),
    roster: myRoster.flatMap((slot) => slot.playerId && slot.playerName && slot.position ? [{ playerId: slot.playerId, name: slot.playerName, position: slot.position, projectedPoints: slot.ourProjectedPoints }] : []),
    rosterConfig, strategy, futurePick,
  }), [recommendations, availableWithAdjustments, availabilityById, nextTurnAvailabilityById, myRoster, rosterConfig, strategy, futurePick]);
  const filtered = availableWithAdjustments.filter((player) => (position === "ALL" || player.position === position) && (!search || `${player.name} ${player.team} ${player.position}`.toLowerCase().includes(search.toLowerCase()))).slice(0, 150);

  useEffect(() => {
    if (!computerTurn || pending) return;
    const key = `${draft.id}:${draft.revision}:${draft.currentPick}`;
    if (autoAdvanceKey.current === key) return;
    autoAdvanceKey.current = key;
    startTransition(async () => {
      setError(null);
      const result = await advanceComputerDraft({ draftId: draft.id, revision: draft.revision });
      if (!result.ok) setError(result.error ?? "Computer picks failed");
      else router.refresh();
    });
  }, [autoRetry, computerTurn, draft.currentPick, draft.id, draft.revision, pending, router]);

  const retryComputerDraft = () => {
    autoAdvanceKey.current = null;
    setAutoRetry((value) => value + 1);
  };

  const draftPlayer = (playerId: number, playerName: string) => {
    if (!window.confirm(`Draft ${playerName}? You can undo the selection afterward.`)) return;
    startTransition(async () => {
      setError(null);
      const selected = canDraftRecommendation ? (draftPlan?.primary.playerId === playerId ? draftPlan.primary : draftPlan?.fallbacks.find((player) => player.playerId === playerId)) : undefined;
      const receiptNextPick = canDraftRecommendation ? futurePick : targetPick;
      const decision = { strategy, impactLabel: selected?.impactLabel ?? "added to the current roster", nextPick: receiptNextPick, score: selected?.score };
      const result = await recordFantasyPick({ draftId: draft.id, playerId, revision: draft.revision, decision });
      if (!result.ok) { setError(result.error ?? "Pick failed"); return; }
      setReceipt({ playerName, position: selected?.position ?? "Player", overallPick: draft.currentPick, impactLabel: decision.impactLabel, strategy, nextPick: receiptNextPick });
      router.refresh();
    });
  };
  const undo = () => startTransition(async () => {
    setError(null);
    const result = await undoFantasyPick({ draftId: draft.id, revision: draft.revision });
    if (!result.ok) setError(result.error ?? "Undo failed"); else { setReceipt(null); router.refresh(); }
  });

  return <div className="space-y-5">
    <header className="sticky top-14 z-30 rounded-2xl border bg-background/95 p-4 shadow-sm backdrop-blur"><div className="flex flex-wrap items-center justify-between gap-4"><div><p className="text-xs font-bold uppercase tracking-widest text-emerald-700">{draft.status} · {DRAFT_STRATEGY_META[strategy].name} strategy</p><h1 className="text-2xl font-black">{draft.name}</h1><p className="text-xs text-muted-foreground">{scoringDescription} · {draft.teamCount} teams · {draft.roundCount} rounds</p></div><div className="flex gap-6 text-center"><div><p className="text-xs text-muted-foreground">On clock</p><p className="font-black">{current?.teamName ?? "Complete"}</p></div><div><p className="text-xs text-muted-foreground">Current pick</p><p className="font-black">{Math.min(draft.currentPick, board.length)} / {board.length}</p></div><div><p className="text-xs text-muted-foreground">Until yours</p><p className="font-black">{until}<span className="ml-1 text-xs font-normal text-muted-foreground">(#{nextPick})</span></p></div><button disabled={pending || !canUndo} onClick={undo} className="rounded-lg border px-3 py-2 text-sm font-semibold disabled:opacity-40">Undo</button></div></div>{error && <div className="mt-3 flex flex-wrap items-center justify-between gap-2 rounded-lg bg-red-50 p-2 text-sm font-semibold text-red-700"><span>{error}</span>{computerTurn && <button type="button" disabled={pending} onClick={retryComputerDraft} className="rounded-md border border-red-300 bg-white px-2 py-1 text-xs disabled:opacity-40">Retry computer picks</button>}</div>}{showAdjustments && <p className="mt-3 rounded-lg bg-blue-50 p-2 text-xs text-blue-700">📊 {adjustmentDesc}</p>}</header>

    {computerTurn && <div role="status" aria-live="polite" className="rounded-2xl border border-blue-200 bg-blue-50 p-4 text-sm text-blue-900"><p className="font-bold">Computer teams are drafting…</p><p className="mt-1 text-xs">The simulator is making seeded, roster-aware selections through your next turn. This page will update automatically.</p></div>}

    <section className="rounded-2xl border bg-card p-4"><div className="mb-3 flex items-center justify-between"><h2 className="font-bold">Draft board</h2><p className="text-xs text-muted-foreground">Your team is highlighted</p></div><div className="overflow-x-auto"><div className="grid min-w-max gap-1" style={{ gridTemplateColumns: `repeat(${draft.teamCount}, minmax(125px, 1fr))` }}>{board.map((slot) => <div key={slot.overallPick} className={`min-h-20 rounded-lg border p-2 text-xs ${slot.isControlled ? "border-emerald-400 bg-emerald-50" : "bg-background"} ${slot.overallPick === draft.currentPick ? "ring-2 ring-amber-400" : ""}`}><p className="flex justify-between text-[10px] text-muted-foreground"><span>{slot.round}.{slot.pickInRound}</span><span>{slot.teamName}</span></p>{slot.playerName ? <><p className="mt-2 font-bold">{slot.playerName}</p><p className="text-muted-foreground">{slot.position} · {slot.nflTeam}</p></> : <p className="mt-3 text-center text-muted-foreground">Available</p>}</div>)}</div></div></section>

    <div className="grid gap-5 xl:grid-cols-[1fr_350px]"><section className="rounded-2xl border bg-card"><div className="flex flex-wrap gap-2 border-b p-4"><input value={search} onChange={(event) => setSearch(event.target.value)} placeholder="Search player, team, position" className="min-w-64 flex-1 rounded-lg border bg-background px-3 py-2" />{["ALL", "QB", "RB", "WR", "TE", "K", "DST"].map((value) => <button key={value} onClick={() => setPosition(value)} className={`rounded-lg px-2.5 py-2 text-xs font-bold ${position === value ? "bg-slate-900 text-white" : "border"}`}>{value}</button>)}</div><div className="max-h-[720px] overflow-auto">{filtered.map((player) => { const rank = player.ourRank ?? player.ecr; const displayAdp = showAdjustments ? player.adjustedAdp : player.adp; const range = player.projectionLow !== null && player.projectionHigh !== null ? `${player.projectionLow.toFixed(0)}–${player.projectionHigh.toFixed(0)}` : "—"; return <div key={player.playerId} className="grid grid-cols-[45px_1fr_auto_auto] items-center gap-3 border-b p-3 hover:bg-muted/50"><p className="text-center text-lg font-black">{rank ?? "—"}</p><div><p className="font-bold">{player.name} <span className="text-xs font-normal text-muted-foreground">{player.position} · {player.team ?? "FA"}</span></p><p className="text-[10px] text-muted-foreground">Tier {player.tier ?? "—"} · Range {range} · {Math.round((player.confidence ?? 0) * 100)}% confidence</p><PlayerBadges player={player} /></div><div className="text-right text-xs"><p>ADP <b>{displayAdp?.toFixed(1) ?? "—"}</b></p><AvailabilityBadge odds={availabilityById.get(player.playerId)} targetPick={targetPick} /></div><button disabled={pending || draft.status !== "active" || (simulatorEnabled && !canDraftRecommendation)} onClick={() => draftPlayer(player.playerId, player.name)} className="rounded-lg bg-emerald-600 px-3 py-2 text-xs font-bold text-white disabled:opacity-40">Draft</button></div>; })}</div></section>
      <DraftIntelligencePanel plan={draftPlan} strategy={strategy} scoring={scoringDescription} rosterName="Configured roster" targetPick={targetPick} canDraftRecommendation={canDraftRecommendation} pending={pending} receipt={receipt} onDraft={draftPlayer} onDismissReceipt={() => setReceipt(null)} />
    </div>
    <p className="text-center text-xs text-muted-foreground">{simulatorEnabled ? "CPU opponents use seeded, roster-aware ADP decisions. Identical seeds reproduce the same computer choices; this is a simulation, not a calibrated prediction of a specific league." : "Decision Desk uses your stored roster, scoring, projection ranges, and directional FFC ADP survival estimates. Manual mode does not model individual opponent selections."}</p>
  </div>;
}
