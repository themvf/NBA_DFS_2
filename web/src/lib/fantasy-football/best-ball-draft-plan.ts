import type { AvailabilityOdds } from "./availability-odds";
import {
  BEST_BALL_MINIMUMS,
  BEST_BALL_POSITIONS,
  BEST_BALL_TARGETS,
  canAddCompletableBestBallPlayer,
  getBestBallCompletionStatus,
  getBestBallRosterStatus,
  type BestBallPosition,
  type BestBallRosterPlayer,
} from "./best-ball";

const WAIT_THRESHOLD = 0.7;

export type BestBallPlanPlayer = BestBallRosterPlayer & {
  name: string;
  ourRank: number | null;
  ecr: number | null;
  adp: number | null;
  projectedPoints: number | null;
  projectionLow: number | null;
  projectionHigh: number | null;
  tier: number | null;
  confidence: number | null;
  availabilityAtTarget: AvailabilityOdds | null;
  availabilityAtFuture: AvailabilityOdds | null;
};

export type BestBallBlueprint = {
  positions: Array<{ position: BestBallPosition; count: number; minimum: number; target: number }>;
  remainingPicks: number;
  minimumPicksRequired: number;
  completable: boolean;
  nflTeams: number;
};

export type BestBallPlannedPlayer = BestBallPlanPlayer & {
  score: number;
  impactLabel: string;
};

export type BestBallDraftPlan = {
  primary: BestBallPlannedPlayer & { decision: "draft-now" | "low-urgency" | "plan" };
  fallbacks: BestBallPlannedPlayer[];
  blueprint: BestBallBlueprint;
  targetPick: number | null;
  futurePick: number | null;
};

export function buildBestBallBlueprint(players: BestBallRosterPlayer[]): BestBallBlueprint {
  const status = getBestBallRosterStatus(players);
  const completion = getBestBallCompletionStatus(players);
  return {
    positions: BEST_BALL_POSITIONS.map((position) => ({ position, count: status.counts[position], minimum: BEST_BALL_MINIMUMS[position], target: BEST_BALL_TARGETS[position] })),
    remainingPicks: completion.remainingSlots,
    minimumPicksRequired: completion.minimumPicksRequired,
    completable: completion.completable,
    nflTeams: status.nflTeams,
  };
}

export function describeBestBallRosterImpact(roster: BestBallRosterPlayer[], candidate: BestBallRosterPlayer): string {
  const before = getBestBallRosterStatus(roster);
  const after = getBestBallRosterStatus([...roster, candidate]);
  const position = candidate.position as BestBallPosition;
  if (before.nflTeams < 2 && after.nflTeams >= 2) return "adds the second NFL team required for a valid roster";
  if (before.counts[position] < BEST_BALL_MINIMUMS[position]) {
    return after.counts[position] >= BEST_BALL_MINIMUMS[position]
      ? `meets the ${position} minimum (${after.counts[position]}/${BEST_BALL_MINIMUMS[position]})`
      : `moves ${position} toward its minimum (${after.counts[position]}/${BEST_BALL_MINIMUMS[position]})`;
  }
  if (after.counts[position] === BEST_BALL_TARGETS[position]) return `reaches the ${position} target (${after.counts[position]}/${BEST_BALL_TARGETS[position]})`;
  if (before.counts[position] < BEST_BALL_TARGETS[position]) return `moves ${position} toward its target (${after.counts[position]}/${BEST_BALL_TARGETS[position]})`;
  return `adds ${position} depth beyond the ${BEST_BALL_TARGETS[position]}-player target`;
}

function constructionBonus(roster: BestBallRosterPlayer[], player: BestBallPlanPlayer): number {
  const status = getBestBallRosterStatus(roster);
  const position = player.position as BestBallPosition;
  let bonus = status.counts[position] < BEST_BALL_MINIMUMS[position] ? 25 : status.counts[position] < BEST_BALL_TARGETS[position] ? 8 : -3;
  if (status.nflTeams < 2 && player.team && !roster.some((item) => item.team === player.team)) bonus += 5;
  return bonus;
}

function scorePlayer(player: BestBallPlanPlayer, roster: BestBallRosterPlayer[], isUserOnClock: boolean): BestBallPlannedPlayer {
  const rank = player.ourRank ?? player.ecr ?? 220;
  const adpValue = player.adp === null ? 0 : Math.max(-20, Math.min(20, player.adp - rank)) * 0.5;
  const odds = isUserOnClock ? player.availabilityAtFuture : player.availabilityAtTarget;
  const availabilityScore = isUserOnClock
    ? odds === null ? 4 : (1 - odds.probability) * 14
    : odds === null ? 0 : (odds.probability - 0.5) * 40;
  return {
    ...player,
    impactLabel: describeBestBallRosterImpact(roster, player),
    score: 220 - rank + adpValue + constructionBonus(roster, player) + (player.confidence ?? 0.5) * 3 + availabilityScore,
  };
}

export function buildBestBallDraftPlan(input: { players: BestBallPlanPlayer[]; roster: BestBallRosterPlayer[]; isUserOnClock: boolean; targetPick: number | null; futurePick: number | null }): BestBallDraftPlan | null {
  const ranked = input.players
    .filter((player) => canAddCompletableBestBallPlayer(input.roster, player))
    .map((player) => scorePlayer(player, input.roster, input.isUserOnClock))
    .sort((a, b) => b.score - a.score || (a.ourRank ?? 999) - (b.ourRank ?? 999));
  const bestOverall = ranked[0];
  if (!bestOverall) return null;
  const primary = input.isUserOnClock && input.futurePick !== null
    ? ranked.find((player) => player.availabilityAtFuture === null || player.availabilityAtFuture.probability < WAIT_THRESHOLD) ?? bestOverall
    : bestOverall;
  const canWait = input.isUserOnClock && input.futurePick !== null && primary.availabilityAtFuture !== null && primary.availabilityAtFuture.probability >= WAIT_THRESHOLD;
  return {
    primary: { ...primary, decision: input.isUserOnClock ? canWait ? "low-urgency" : "draft-now" : "plan" },
    fallbacks: ranked.filter((player) => player.playerId !== primary.playerId).slice(0, 3),
    blueprint: buildBestBallBlueprint(input.roster),
    targetPick: input.targetPick,
    futurePick: input.futurePick,
  };
}
