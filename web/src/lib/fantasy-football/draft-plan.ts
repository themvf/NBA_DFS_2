import type { RosterConfig } from "./league-config";
import type { DraftStrategy } from "./draft-strategy";
import type { Recommendation } from "./recommendations";

const FLEX_POSITIONS = new Set(["RB", "WR", "TE"]);
type PlanPlayer = { playerId: number; name: string; position: string; projectedPoints: number | null; projectionLow: number | null; projectionHigh: number | null; tier: number | null; confidence: number | null; availabilityProbability: number | null };
type RosterPlayer = Pick<PlanPlayer, "name" | "position" | "projectedPoints">;
export type SlotNeed = { slot: string; status: "filled" | "open"; player?: string };
export type RosterBlueprint = { slots: SlotNeed[]; starterNeeds: string[]; flexOpen: number; benchFilled: number; benchOpen: number };
export type DraftPlan = { primary: Recommendation & PlanPlayer & { impact: number; impactLabel: string; decision: "draft-now" | "wait" }; fallbacks: Array<Recommendation & PlanPlayer & { impact: number; impactLabel: string }>; blueprint: RosterBlueprint };

function points(player: RosterPlayer | PlanPlayer) { return player.projectedPoints ?? 0; }
function countByPosition(players: RosterPlayer[]) { return players.reduce<Record<string, RosterPlayer[]>>((all, player) => { (all[player.position] ??= []).push(player); return all; }, {}); }

export function buildRosterBlueprint(players: RosterPlayer[], roster: RosterConfig): RosterBlueprint {
  const byPosition = countByPosition(players);
  const slots: SlotNeed[] = [];
  for (const position of ["QB", "RB", "WR", "TE", "K", "DST"] as const) {
    const sorted = [...(byPosition[position] ?? [])].sort((a, b) => points(b) - points(a));
    for (let index = 0; index < roster[position]; index += 1) slots.push({ slot: `${position}${roster[position] > 1 ? ` ${index + 1}` : ""}`, status: sorted[index] ? "filled" : "open", player: sorted[index]?.name });
    byPosition[position] = sorted.slice(roster[position]);
  }
  const flexPool = ["RB", "WR", "TE"].flatMap((position) => byPosition[position] ?? []).sort((a, b) => points(b) - points(a));
  for (let index = 0; index < roster.FLEX; index += 1) slots.push({ slot: `FLEX ${index + 1}`, status: flexPool[index] ? "filled" : "open", player: flexPool[index]?.name });
  const starterNeeds = slots.filter((slot) => slot.status === "open").map((slot) => slot.slot);
  const starterCount = slots.length;
  const benchFilled = Math.max(0, players.length - Math.min(players.length, starterCount));
  return { slots, starterNeeds, flexOpen: slots.filter((slot) => slot.slot.startsWith("FLEX") && slot.status === "open").length, benchFilled, benchOpen: Math.max(0, roster.BN - benchFilled) };
}

function impactFor(player: PlanPlayer, blueprint: RosterBlueprint) {
  const directOpen = blueprint.starterNeeds.some((slot) => slot.startsWith(player.position));
  if (directOpen) return { impact: points(player), label: `fills ${player.position} starter` };
  if (FLEX_POSITIONS.has(player.position) && blueprint.flexOpen > 0) return { impact: points(player), label: `fills FLEX ${blueprint.flexOpen}` };
  return { impact: 0, label: "adds bench depth" };
}

function strategyBonus(player: PlanPlayer, strategy: DraftStrategy) {
  if (strategy === "floor") return (player.projectionLow ?? points(player)) * 0.04 + (player.confidence ?? 0) * 8;
  if (strategy === "upside") return (player.projectionHigh ?? points(player)) * 0.04 + Math.max(0, 6 - (player.tier ?? 6)) * 1.5;
  return 0;
}

export function buildDraftPlan(input: { recommendations: Recommendation[]; players: PlanPlayer[]; roster: RosterPlayer[]; rosterConfig: RosterConfig; strategy: DraftStrategy }): DraftPlan | null {
  const blueprint = buildRosterBlueprint(input.roster, input.rosterConfig);
  const byId = new Map(input.players.map((player) => [player.playerId, player]));
  const ranked = input.recommendations.flatMap((recommendation) => {
    const player = byId.get(recommendation.playerId);
    if (!player) return [];
    const impact = impactFor(player, blueprint);
    return [{ ...recommendation, ...player, impact: impact.impact, impactLabel: impact.label, score: recommendation.score + strategyBonus(player, input.strategy) + (impact.impact > 0 ? 12 : 0) }];
  }).sort((a, b) => b.score - a.score);
  const primary = ranked[0];
  if (!primary) return null;
  const decision = primary.impact > 0 || (primary.availabilityProbability ?? 0) <= 0.4 ? "draft-now" : "wait";
  const fallbacks = ranked.filter((candidate) => candidate.playerId !== primary.playerId).slice(0, 3);
  return { primary: { ...primary, decision }, fallbacks, blueprint };
}
