"use client";

import type { DraftPlan } from "@/lib/fantasy-football/draft-plan";
import { DRAFT_STRATEGY_META, type DraftStrategy } from "@/lib/fantasy-football/draft-strategy";

export type PickReceipt = { playerName: string; position: string; overallPick: number; impactLabel: string; strategy: DraftStrategy; nextPick: number };

type Props = {
  plan: DraftPlan | null;
  strategy: DraftStrategy;
  scoring: string;
  rosterName: string;
  pending: boolean;
  receipt: PickReceipt | null;
  onDraft: (playerId: number, playerName: string) => void;
  onDismissReceipt: () => void;
};

function RiskLine({ player }: { player: NonNullable<DraftPlan>["primary"] }) {
  const range = player.projectionLow !== null && player.projectionHigh !== null
    ? `${player.projectionLow.toFixed(0)}–${player.projectionHigh.toFixed(0)}` : "range unavailable";
  const available = player.availabilityProbability === null ? "availability unavailable" : `${Math.round(player.availabilityProbability * 100)}% available at next turn`;
  return <p className="mt-2 text-xs text-muted-foreground">Tier {player.tier ?? "—"} · {range} projected points · {Math.round((player.confidence ?? 0) * 100)}% projection confidence · {available}</p>;
}

export function DraftIntelligencePanel({ plan, strategy, scoring, rosterName, pending, receipt, onDraft, onDismissReceipt }: Props) {
  return <aside className="space-y-5">
    <section className="rounded-2xl border border-emerald-200 bg-emerald-50 p-4">
      <p className="text-xs font-black uppercase tracking-widest text-emerald-700">Decision Desk · {DRAFT_STRATEGY_META[strategy].name}</p>
      <p className="mt-1 text-xs text-emerald-900">{DRAFT_STRATEGY_META[strategy].description}</p>
      {!plan ? <p className="mt-3 text-sm text-muted-foreground">No available recommendation.</p> : <>
        <div className="mt-3 flex items-start justify-between gap-3"><div><p className="text-xs font-black text-emerald-700">{plan.primary.decision === "draft-now" ? "DRAFT NOW" : "SAFE TO WAIT"}</p><h2 className="text-xl font-black">{plan.primary.name}</h2><p className="text-sm font-semibold">{plan.primary.position} · {plan.primary.impactLabel}</p></div><button disabled={pending} onClick={() => onDraft(plan.primary.playerId, plan.primary.name)} className="rounded-lg bg-emerald-600 px-3 py-2 text-xs font-bold text-white disabled:opacity-40">Draft</button></div>
        <p className="mt-2 text-sm"><b>{plan.primary.impact.toFixed(1)}</b> projected points in the newly filled starter slot.</p><RiskLine player={plan.primary} />
      </>}
    </section>

    {plan && <section className="rounded-2xl border bg-card p-4"><h2 className="font-bold">Next-turn plan</h2><p className="mt-1 text-xs text-muted-foreground">Directional ADP-survival estimates; not an opponent-pick simulation.</p><div className="mt-3 space-y-2">{plan.fallbacks.map((player, index) => <div key={player.playerId} className="rounded-lg border p-2"><div className="flex justify-between gap-2"><div><p className="text-xs font-bold text-muted-foreground">{index === 0 ? "FALLBACK" : "ALTERNATIVE"}</p><p className="font-semibold">{player.name} <span className="text-xs text-muted-foreground">{player.position}</span></p><p className="text-xs text-muted-foreground">{player.impactLabel} · {player.availabilityProbability === null ? "Avail —" : `${Math.round(player.availabilityProbability * 100)}% likely available`}</p></div><button disabled={pending} onClick={() => onDraft(player.playerId, player.name)} className="self-center rounded border px-2 py-1 text-xs font-bold disabled:opacity-40">Draft</button></div></div>)}</div></section>}

    {plan && <section className="rounded-2xl border bg-card p-4"><h2 className="font-bold">My team blueprint</h2><p className="mt-1 text-xs text-muted-foreground">{rosterName} · {scoring} · {plan.blueprint.benchOpen} bench slots open</p><div className="mt-3 grid grid-cols-2 gap-2">{plan.blueprint.slots.map((slot) => <div key={slot.slot} className={`rounded-lg p-2 text-xs ${slot.status === "filled" ? "bg-muted" : "border border-amber-300 bg-amber-50"}`}><p className="font-bold">{slot.slot}</p><p className="truncate text-muted-foreground">{slot.player ?? "Open"}</p></div>)}</div><p className="mt-3 text-xs font-semibold text-amber-800">{plan.blueprint.starterNeeds.length ? `Starter needs: ${plan.blueprint.starterNeeds.join(", ")}` : "All starter slots filled"}</p></section>}

    {receipt && <section className="rounded-2xl border border-blue-200 bg-blue-50 p-4"><div className="flex items-start justify-between gap-2"><div><p className="text-xs font-black uppercase tracking-widest text-blue-700">Pick receipt · #{receipt.overallPick}</p><h2 className="font-bold">{receipt.playerName} · {receipt.position}</h2></div><button onClick={onDismissReceipt} className="text-xs font-bold text-blue-700">Dismiss</button></div><p className="mt-2 text-xs text-blue-900">Why: {receipt.impactLabel}. {DRAFT_STRATEGY_META[receipt.strategy].name} strategy was active. Your next controlled pick is #{receipt.nextPick}.</p></section>}
  </aside>;
}
