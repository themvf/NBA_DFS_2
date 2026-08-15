"use client";

import type { DraftPlan } from "@/lib/fantasy-football/draft-plan";
import { DRAFT_STRATEGY_META, type DraftStrategy } from "@/lib/fantasy-football/draft-strategy";

export type PickReceipt = { playerName: string; position: string; overallPick: number; impactLabel: string; strategy: DraftStrategy; nextPick: number | null };
type Props = { plan: DraftPlan | null; strategy: DraftStrategy; scoring: string; rosterName: string; targetPick: number | null; canDraftRecommendation: boolean; pending: boolean; receipt: PickReceipt | null; onDraft: (playerId: number, playerName: string) => void; onDismissReceipt: () => void };

function RiskLine({ player, futurePick }: { player: NonNullable<DraftPlan>["primary"]; futurePick: number | null }) {
  const range = player.projectionLow !== null && player.projectionHigh !== null ? `${player.projectionLow.toFixed(0)}–${player.projectionHigh.toFixed(0)}` : "range unavailable";
  const availability = futurePick === null ? "final controlled turn" : player.nextTurnAvailabilityProbability === null ? `availability at pick #${futurePick} unavailable` : `${Math.round(player.nextTurnAvailabilityProbability * 100)}% available at pick #${futurePick}`;
  return <p className="mt-2 text-xs text-muted-foreground">Tier {player.tier ?? "—"} · {range} projected points · {Math.round((player.confidence ?? 0) * 100)}% projection confidence · {availability}</p>;
}

function ImpactSummary({ impact }: { impact: number }) {
  return impact > 0 ? <p className="mt-2 text-sm"><b>+{impact.toFixed(1)}</b> projected starter points versus your current best lineup.</p> : <p className="mt-2 text-sm">Bench-depth addition with no immediate projected starter-point gain.</p>;
}

export function DraftIntelligencePanel({ plan, strategy, scoring, rosterName, targetPick, canDraftRecommendation, pending, receipt, onDraft, onDismissReceipt }: Props) {
  const deskLabel = !canDraftRecommendation ? `PLAN FOR PICK #${targetPick ?? "—"}` : plan?.primary.decision === "draft-now" ? "DRAFT NOW" : `LOW URGENCY · LIKELY THERE AT #${plan?.futurePick}`;
  const futureLabel = plan?.futurePick === null ? "Other draft options" : `Plan for pick #${plan?.futurePick}`;
  return <aside className="space-y-5">
    <section className="rounded-2xl border border-emerald-200 bg-emerald-50 p-4">
      <p className="text-xs font-black uppercase tracking-widest text-emerald-700">Decision Desk · {DRAFT_STRATEGY_META[strategy].name}</p><p className="mt-1 text-xs text-emerald-900">{DRAFT_STRATEGY_META[strategy].description}</p>
      {!plan ? <p className="mt-3 text-sm text-muted-foreground">No available recommendation.</p> : <><div className="mt-3 flex items-start justify-between gap-3"><div><p className="text-xs font-black text-emerald-700">{deskLabel}</p><h2 className="text-xl font-black">{plan.primary.name}</h2><p className="text-sm font-semibold">{plan.primary.position} · {plan.primary.impactLabel}</p></div><button disabled={pending || !canDraftRecommendation} onClick={() => onDraft(plan.primary.playerId, plan.primary.name)} className="rounded-lg bg-emerald-600 px-3 py-2 text-xs font-bold text-white disabled:opacity-40">{canDraftRecommendation ? plan.primary.decision === "wait" ? "Draft anyway" : "Draft" : "Waiting"}</button></div><ImpactSummary impact={plan.primary.impact} /><RiskLine player={plan.primary} futurePick={plan.futurePick} /></>}
    </section>

    {plan && <section className="rounded-2xl border bg-card p-4"><h2 className="font-bold">{futureLabel}</h2><p className="mt-1 text-xs text-muted-foreground">Alternatives are ranked by roster impact and your active strategy. Availability is a directional ADP-survival estimate, not an opponent-pick simulation.</p><div className="mt-3 space-y-2">{plan.fallbacks.map((player, index) => <div key={player.playerId} className="rounded-lg border p-2"><div className="flex justify-between gap-2"><div><p className="text-xs font-bold text-muted-foreground">{index === 0 ? "FALLBACK" : "ALTERNATIVE"}</p><p className="font-semibold">{player.name} <span className="text-xs text-muted-foreground">{player.position}</span></p><p className="text-xs text-muted-foreground">{player.impactLabel} · {plan.futurePick === null ? "No later controlled pick" : player.nextTurnAvailabilityProbability === null ? `Pick #${plan.futurePick} availability unavailable` : `${Math.round(player.nextTurnAvailabilityProbability * 100)}% available at #${plan.futurePick}`}</p></div><button disabled={pending || !canDraftRecommendation} onClick={() => onDraft(player.playerId, player.name)} className="self-center rounded border px-2 py-1 text-xs font-bold disabled:opacity-40">{canDraftRecommendation ? "Draft" : "Waiting"}</button></div></div>)}</div></section>}

    {plan && <section className="rounded-2xl border bg-card p-4"><h2 className="font-bold">My team blueprint</h2><p className="mt-1 text-xs text-muted-foreground">{rosterName} · {scoring} · {plan.blueprint.benchOpen} bench slots open</p><div className="mt-3 grid grid-cols-2 gap-2">{plan.blueprint.slots.map((slot) => <div key={slot.slot} className={`rounded-lg p-2 text-xs ${slot.status === "filled" ? "bg-muted" : "border border-amber-300 bg-amber-50"}`}><p className="font-bold">{slot.slot}</p><p className="truncate text-muted-foreground">{slot.player ?? "Open"}</p>{slot.projectedPoints !== undefined && <p className="text-[10px] text-muted-foreground">{slot.projectedPoints.toFixed(1)} projected</p>}</div>)}</div><p className="mt-3 text-xs font-semibold text-amber-800">{plan.blueprint.starterNeeds.length ? `Starter needs: ${plan.blueprint.starterNeeds.join(", ")}` : "All starter slots filled"}</p></section>}

    {receipt && <section className="rounded-2xl border border-blue-200 bg-blue-50 p-4"><div className="flex items-start justify-between gap-2"><div><p className="text-xs font-black uppercase tracking-widest text-blue-700">Pick receipt · #{receipt.overallPick}</p><h2 className="font-bold">{receipt.playerName} · {receipt.position}</h2></div><button onClick={onDismissReceipt} className="text-xs font-bold text-blue-700">Dismiss</button></div><p className="mt-2 text-xs text-blue-900">Why: {receipt.impactLabel}. {DRAFT_STRATEGY_META[receipt.strategy].name} strategy was active. {receipt.nextPick === null ? "No controlled picks remain." : `Your next controlled pick is #${receipt.nextPick}.`}</p></section>}
  </aside>;
}
