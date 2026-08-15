import type { RosterConfig } from "./league-config";
import type { DraftStrategy } from "./draft-strategy";
import type { Recommendation } from "./recommendations";

const FLEX_POSITIONS = ["RB", "WR", "TE"] as const;
type PlanPlayer = { playerId: number; name: string; position: string; projectedPoints: number | null; projectionLow: number | null; projectionHigh: number | null; tier: number | null; confidence: number | null; availabilityProbability: number | null; nextTurnAvailabilityProbability: number | null };
export type RosterPlayer = Pick<PlanPlayer, "playerId" | "name" | "position" | "projectedPoints">;
export type SlotNeed = { slot: string; status: "filled" | "open"; playerId?: number; player?: string; projectedPoints?: number };
export type RosterBlueprint = { slots: SlotNeed[]; starterNeeds: string[]; flexOpen: number; benchFilled: number; benchOpen: number; starterPoints: number };
type PlannedPlayer = Recommendation & PlanPlayer & { impact: number; impactLabel: string };
export type DraftPlan = { primary: PlannedPlayer & { decision: "draft-now" | "wait" }; fallbacks: PlannedPlayer[]; blueprint: RosterBlueprint; futurePick: number | null };

function points(player: RosterPlayer | PlanPlayer) { return player.projectedPoints ?? 0; }
function slotName(position: string, index: number, count: number) { return `${position}${count > 1 ? ` ${index + 1}` : ""}`; }

function allocateRoster(players: RosterPlayer[], roster: RosterConfig) {
  const remaining = new Map(players.map((player) => [player.playerId, player]));
  const slots: SlotNeed[] = [];
  for (const position of ["QB", "RB", "WR", "TE", "K", "DST"] as const) {
    const eligible = players.filter((player) => player.position === position).sort((a, b) => points(b) - points(a));
    for (let index = 0; index < roster[position]; index += 1) {
      const player = eligible[index];
      if (player) remaining.delete(player.playerId);
      slots.push({ slot: slotName(position, index, roster[position]), status: player ? "filled" : "open", playerId: player?.playerId, player: player?.name, projectedPoints: player ? points(player) : undefined });
    }
  }
  const flexPool = [...remaining.values()].filter((player) => FLEX_POSITIONS.includes(player.position as typeof FLEX_POSITIONS[number])).sort((a, b) => points(b) - points(a));
  for (let index = 0; index < roster.FLEX; index += 1) {
    const player = flexPool[index];
    if (player) remaining.delete(player.playerId);
    slots.push({ slot: `FLEX ${index + 1}`, status: player ? "filled" : "open", playerId: player?.playerId, player: player?.name, projectedPoints: player ? points(player) : undefined });
  }
  return { slots, bench: [...remaining.values()], starterPoints: slots.reduce((sum, slot) => sum + (slot.projectedPoints ?? 0), 0) };
}

export function buildRosterBlueprint(players: RosterPlayer[], roster: RosterConfig): RosterBlueprint {
  const allocation = allocateRoster(players, roster);
  const starterNeeds = allocation.slots.filter((slot) => slot.status === "open").map((slot) => slot.slot);
  const benchFilled = allocation.bench.length;
  return {
    slots: allocation.slots,
    starterNeeds,
    flexOpen: allocation.slots.filter((slot) => slot.slot.startsWith("FLEX") && slot.status === "open").length,
    benchFilled,
    benchOpen: Math.max(0, roster.BN - benchFilled),
    starterPoints: allocation.starterPoints,
  };
}

function impactFor(player: PlanPlayer, roster: RosterPlayer[], rosterConfig: RosterConfig) {
  const before = allocateRoster(roster, rosterConfig);
  const after = allocateRoster([...roster, player], rosterConfig);
  const impact = Math.max(0, after.starterPoints - before.starterPoints);
  const assigned = after.slots.find((slot) => slot.playerId === player.playerId);
  if (!assigned) return { impact: 0, label: "adds bench depth" };

  const afterStarterIds = new Set(after.slots.flatMap((slot) => slot.playerId === undefined ? [] : [slot.playerId]));
  const displaced = before.slots.find((slot) => slot.playerId !== undefined && !afterStarterIds.has(slot.playerId));
  if (displaced?.player) return { impact, label: `improves the starting lineup over ${displaced.player} by ${impact.toFixed(1)}` };
  return { impact, label: `fills ${assigned.slot}` };
}

function strategyBonus(player: PlanPlayer, strategy: DraftStrategy) {
  if (strategy === "floor") return (player.projectionLow ?? points(player)) * 0.04 + (player.confidence ?? 0) * 8;
  if (strategy === "upside") return (player.projectionHigh ?? points(player)) * 0.04 + Math.max(0, 6 - (player.tier ?? 6)) * 1.5;
  return 0;
}

function planPlayer(recommendation: Recommendation, player: PlanPlayer, roster: RosterPlayer[], rosterConfig: RosterConfig, strategy: DraftStrategy): PlannedPlayer {
  const impact = impactFor(player, roster, rosterConfig);
  return { ...recommendation, ...player, impact: impact.impact, impactLabel: impact.label, score: recommendation.score + strategyBonus(player, strategy) + Math.min(20, impact.impact) };
}

const WAIT_AVAILABILITY_THRESHOLD = 0.7;

export function buildDraftPlan(input: {
  recommendations: Recommendation[];
  players: PlanPlayer[];
  roster: RosterPlayer[];
  rosterConfig: RosterConfig;
  strategy: DraftStrategy;
  futurePick: number | null;
}): DraftPlan | null {
  const blueprint = buildRosterBlueprint(input.roster, input.rosterConfig);
  const byId = new Map(input.players.map((player) => [player.playerId, player]));
  const ranked = input.recommendations.flatMap((recommendation) => {
    const player = byId.get(recommendation.playerId);
    return player ? [planPlayer(recommendation, player, input.roster, input.rosterConfig, input.strategy)] : [];
  }).sort((a, b) => b.score - a.score || (a.ourRank ?? 999) - (b.ourRank ?? 999));
  const bestOverall = ranked[0];
  if (!bestOverall) return null;

  const primary = input.futurePick === null ? bestOverall : ranked.find((candidate) =>
    candidate.nextTurnAvailabilityProbability === null
      || candidate.nextTurnAvailabilityProbability < WAIT_AVAILABILITY_THRESHOLD,
  ) ?? bestOverall;
  const canWait = input.futurePick !== null
    && primary.nextTurnAvailabilityProbability !== null
    && primary.nextTurnAvailabilityProbability >= WAIT_AVAILABILITY_THRESHOLD;
  return {
    primary: { ...primary, decision: canWait ? "wait" : "draft-now" },
    fallbacks: ranked.filter((candidate) => candidate.playerId !== primary.playerId).slice(0, 3),
    blueprint,
    futurePick: input.futurePick,
  };
}
