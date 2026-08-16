import { adjustAdpForRoster } from "./adp-adjustment";
import { calculateRosterSize, type RosterConfig } from "./league-config";

export const AUTO_DRAFT_VERSION = "cpu-auto-draft-v1";

export const AUTO_DRAFT_POSITIONS = ["QB", "RB", "WR", "TE", "K", "DST"] as const;
export type AutoDraftPosition = (typeof AUTO_DRAFT_POSITIONS)[number];
export type AutoDraftPlayerId = number | string;

export interface AutoDraftPlayer {
  playerId: AutoDraftPlayerId;
  name: string;
  position: string;
  adp: number | null;
  ecr: number | null;
  ourRank: number | null;
  projectedPoints: number | null;
}

export interface AutoDraftContext {
  availablePlayers: readonly AutoDraftPlayer[];
  roster: readonly AutoDraftPlayer[];
  rosterConfig: RosterConfig;
  teamCount: number;
  teamSlot: number | string;
  overallPick: number;
  seed: number | string;
  draftedPlayerIds?: readonly AutoDraftPlayerId[];
  positionCaps?: Partial<Record<"QB" | "TE" | "K" | "DST", number>>;
}

export interface AutoDraftResult {
  player: AutoDraftPlayer;
  score: number;
  reasons: string[];
  adjustedAdp: number | null;
  rankingSource: "adjusted-adp" | "ecr" | "our-rank" | "projection" | "unranked";
}

type NeedState = {
  direct: Record<AutoDraftPosition, number>;
  flex: number;
  missingRequired: number;
};

const SUPPORTED = new Set<string>(AUTO_DRAFT_POSITIONS);
const FLEX_POSITIONS = new Set<AutoDraftPosition>(["RB", "WR", "TE"]);
function isPosition(position: string): position is AutoDraftPosition {
  return SUPPORTED.has(position);
}

function finitePositive(value: number | null): number | null {
  return value !== null && Number.isFinite(value) && value > 0 ? value : null;
}

function playerKey(playerId: AutoDraftPlayerId): string {
  return `${typeof playerId}:${String(playerId)}`;
}

function countPositions(roster: readonly AutoDraftPlayer[]): Record<AutoDraftPosition, number> {
  const counts = Object.fromEntries(AUTO_DRAFT_POSITIONS.map((position) => [position, 0])) as Record<AutoDraftPosition, number>;
  for (const player of roster) {
    if (isPosition(player.position)) counts[player.position] += 1;
  }
  return counts;
}

function getNeeds(roster: readonly AutoDraftPlayer[], config: RosterConfig): NeedState {
  const counts = countPositions(roster);
  const direct = Object.fromEntries(AUTO_DRAFT_POSITIONS.map((position) => [
    position,
    Math.max(0, config[position] - counts[position]),
  ])) as Record<AutoDraftPosition, number>;
  const flexSurplus = (["RB", "WR", "TE"] as const).reduce(
    (sum, position) => sum + Math.max(0, counts[position] - config[position]),
    0,
  );
  const flex = Math.max(0, config.FLEX - flexSurplus);
  return {
    direct,
    flex,
    missingRequired: AUTO_DRAFT_POSITIONS.reduce((sum, position) => sum + direct[position], 0) + flex,
  };
}

function fillsRequiredSlot(player: AutoDraftPlayer, roster: readonly AutoDraftPlayer[], config: RosterConfig, before: NeedState): boolean {
  return getNeeds([...roster, player], config).missingRequired < before.missingRequired;
}

function configuredCap(value: number | undefined, fallback: number): number {
  return Number.isFinite(value) ? Math.max(0, Math.floor(value as number)) : Math.max(0, fallback);
}

function seededUnit(seed: number | string, teamSlot: number | string, overallPick: number, playerId: AutoDraftPlayerId): number {
  const input = `${seed}|${teamSlot}|${overallPick}|${playerKey(playerId)}`;
  let hash = 2166136261;
  for (let index = 0; index < input.length; index += 1) {
    hash ^= input.charCodeAt(index);
    hash = Math.imul(hash, 16777619);
  }
  hash += hash << 13;
  hash ^= hash >>> 7;
  hash += hash << 3;
  hash ^= hash >>> 17;
  hash += hash << 5;
  return (hash >>> 0) / 4294967296;
}
function projectionRanks(players: readonly AutoDraftPlayer[]): Map<string, number> {
  const ranked = players
    .filter((player) => finitePositive(player.projectedPoints) !== null)
    .sort((left, right) => (right.projectedPoints as number) - (left.projectedPoints as number)
      || playerKey(left.playerId).localeCompare(playerKey(right.playerId)));
  return new Map(ranked.map((player, index) => [playerKey(player.playerId), index + 1]));
}

function rankingFor(
  player: AutoDraftPlayer,
  context: AutoDraftContext,
  projectionRank: number | undefined,
): Pick<AutoDraftResult, "adjustedAdp" | "rankingSource"> & { rank: number } {
  const adp = finitePositive(player.adp);
  const adjustedAdp = adjustAdpForRoster(adp, player.position, context.rosterConfig, context.teamCount);
  if (adjustedAdp !== null) return { rank: adjustedAdp, adjustedAdp, rankingSource: "adjusted-adp" };
  const ecr = finitePositive(player.ecr);
  if (ecr !== null) return { rank: ecr, adjustedAdp: null, rankingSource: "ecr" };
  const ourRank = finitePositive(player.ourRank);
  if (ourRank !== null) return { rank: ourRank, adjustedAdp: null, rankingSource: "our-rank" };
  if (projectionRank !== undefined) return { rank: projectionRank, adjustedAdp: null, rankingSource: "projection" };
  return { rank: 300, adjustedAdp: null, rankingSource: "unranked" };
}

/**
 * Selects one legal CPU pick. K/DST caps are hard; QB/TE caps are soft.
 * The seed jitter only separates nearby options and is stable for a team/pick.
 */
export function selectComputerPick(context: AutoDraftContext): AutoDraftResult | null {
  const rosterSize = calculateRosterSize(context.rosterConfig);
  const remainingPicks = rosterSize - context.roster.length;
  if (remainingPicks <= 0 || context.availablePlayers.length === 0) return null;

  const counts = countPositions(context.roster);
  const drafted = new Set((context.draftedPlayerIds ?? []).map(playerKey));
  for (const player of context.roster) drafted.add(playerKey(player.playerId));

  const qbCap = configuredCap(context.positionCaps?.QB, context.rosterConfig.QB);
  const teCap = configuredCap(context.positionCaps?.TE, context.rosterConfig.TE);
  const kCap = configuredCap(context.positionCaps?.K, context.rosterConfig.K);
  const dstCap = configuredCap(context.positionCaps?.DST, context.rosterConfig.DST);
  const legal = context.availablePlayers.filter((player) => {
    if (!isPosition(player.position) || drafted.has(playerKey(player.playerId))) return false;
    if (player.position === "K" && counts.K >= kCap) return false;
    if (player.position === "DST" && counts.DST >= dstCap) return false;
    return true;
  });
  if (legal.length === 0) return null;

  const needs = getNeeds(context.roster, context.rosterConfig);
  const feasibilityForced = remainingPicks <= needs.missingRequired;
  let candidates = feasibilityForced
    ? legal.filter((player) => fillsRequiredSlot(player, context.roster, context.rosterConfig, needs))
    : legal;
  if (candidates.length === 0) return null;

  const lateSpecialWindow = Math.max(3, kCap + dstCap + 1);
  if (!feasibilityForced && remainingPicks > lateSpecialWindow) {
    const nonSpecial = candidates.filter((player) => player.position !== "K" && player.position !== "DST");
    if (nonSpecial.length > 0) candidates = nonSpecial;
  }
  const projectedRanks = projectionRanks(candidates);
  const scored = candidates.map((player): AutoDraftResult => {
    const position = player.position as AutoDraftPosition;
    const ranking = rankingFor(player, context, projectedRanks.get(playerKey(player.playerId)));
    const reasons = [`${ranking.rankingSource} rank ${ranking.rank.toFixed(1)}`];
    let score = 500 - ranking.rank * 2;

    if (needs.direct[position] > 0) {
      score += 80 + Math.min(3, needs.direct[position]) * 8;
      reasons.push(`${position} starter need`);
    } else if (FLEX_POSITIONS.has(position) && needs.flex > 0) {
      score += 62;
      reasons.push("FLEX starter need");
    } else {
      reasons.push(`${position} depth`);
    }

    if (position === "QB" && counts.QB >= qbCap) {
      score -= 105 + (counts.QB - qbCap) * 25;
      reasons.push(`QB soft cap ${qbCap}`);
    }
    if (position === "TE" && counts.TE >= teCap) {
      score -= 85 + (counts.TE - teCap) * 20;
      reasons.push(`TE soft cap ${teCap}`);
    }
    if ((position === "K" || position === "DST") && remainingPicks <= lateSpecialWindow) {
      score += 18;
      reasons.push("late-round specialist");
    }
    if (feasibilityForced) {
      score += 140;
      reasons.push("required-slot feasibility");
    }

    score += (seededUnit(context.seed, context.teamSlot, context.overallPick, player.playerId) - 0.5) * 8;
    return { player, score, reasons, adjustedAdp: ranking.adjustedAdp, rankingSource: ranking.rankingSource };
  });

  scored.sort((left, right) => right.score - left.score
    || playerKey(left.player.playerId).localeCompare(playerKey(right.player.playerId)));
  return scored[0] ?? null;
}
