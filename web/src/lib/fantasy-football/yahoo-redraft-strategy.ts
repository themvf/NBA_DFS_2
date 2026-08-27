import {
  REDRAFT_FLEX_POSITIONS,
  REDRAFT_POSITIONS,
  REDRAFT_ROSTER_SIZE,
  REDRAFT_STARTER_SLOTS,
  REDRAFT_TEAM_COUNT,
  canAddRedraftPlayer,
  getRedraftRosterStatus,
  type RedraftPosition,
  type RedraftRosterPlayer,
} from "./redraft";

export type YahooRedraftStrategyPlayer = RedraftRosterPlayer & {
  name: string;
  projectedPoints: number | null;
  expectedGames: number | null;
  ourRank: number | null;
  yahooXRank: number | null;
  yahooAdp: number | null;
};

export type YahooStrategyLabel = "best-path" | "position-can-wait" | "tier-drop" | "alternative";
export type YahooTimingAction = "take-now" | "target-soon" | "wait" | "pass-at-price" | "no-market-data";

export type YahooRedraftStrategyCandidate = YahooRedraftStrategyPlayer & {
  onePickValueAdded: number;
  twoPickValueAdded: number;
  futureTargetPlayerId: number | null;
  futureTargetName: string | null;
  futureTargetPosition: string | null;
  samePositionReplacementName: string | null;
  replacementRetention: number | null;
  pointsOverReplacement: number | null;
  yahooMarketPick: number | null;
  yahooTargetPick: number | null;
  yahooTimingAction: YahooTimingAction;
  yahooRankGap: number | null;
  strategyLabel: YahooStrategyLabel;
  strategyExplanation: string;
};

export type YahooRedraftStrategy = {
  model: "yahoo-redraft-two-pick-v1";
  nextPick: number | null;
  followingPick: number | null;
  recommendation: { playerId: number; headline: string; explanation: string } | null;
  candidates: YahooRedraftStrategyCandidate[];
};

const FLEX_SET = new Set<string>(REDRAFT_FLEX_POSITIONS);

function finite(value: number | null | undefined): value is number {
  return value !== null && value !== undefined && Number.isFinite(value);
}

function effectivePoints(player: YahooRedraftStrategyPlayer): number {
  if (!finite(player.projectedPoints)) return 0;
  const activeShare = finite(player.expectedGames) ? Math.max(0, Math.min(1, player.expectedGames / 17)) : 1;
  return player.projectedPoints * activeShare;
}

export function getYahooMarketPick(player: YahooRedraftStrategyPlayer): number | null {
  const values = [player.yahooXRank, player.yahooAdp]
    .filter((value): value is number => finite(value) && value > 0);
  return values.length ? Math.min(...values) : null;
}

export function isLikelyAvailableAtYahooPick(
  player: YahooRedraftStrategyPlayer,
  pick: number | null,
  _teamCount = REDRAFT_TEAM_COUNT,
): boolean {
  if (pick === null) return false;
  const marketPick = getYahooMarketPick(player);
  // Future-path construction is intentionally more conservative than the
  // current-pick timing badge: do not plan around a player whose earlier Yahoo
  // XRank/ADP anchor is already before the following turn.
  if (marketPick !== null) return marketPick >= pick;
  return finite(player.ourRank) && player.ourRank >= pick;
}

function yahooTiming(input: {
  player: YahooRedraftStrategyPlayer;
  nextPick: number | null;
  followingPick: number | null;
  teamCount?: number;
}): { action: YahooTimingAction; marketPick: number | null; targetPick: number | null } {
  const marketPick = getYahooMarketPick(input.player);
  if (marketPick === null || input.nextPick === null) return { action: "no-market-data", marketPick, targetPick: null };
  const safetyBuffer = Math.max(3, Math.ceil((input.teamCount ?? REDRAFT_TEAM_COUNT) / 2));
  const targetPick = Math.max(1, Math.floor(marketPick - safetyBuffer));
  if (finite(input.player.ourRank) && marketPick <= input.player.ourRank - 5 && input.nextPick < input.player.ourRank) {
    return { action: "pass-at-price", marketPick, targetPick: Math.max(1, Math.floor(input.player.ourRank)) };
  }
  if (input.nextPick >= targetPick || (input.followingPick !== null && input.followingPick > marketPick)) {
    return { action: "take-now", marketPick, targetPick };
  }
  if (input.followingPick !== null && input.followingPick >= targetPick) return { action: "target-soon", marketPick, targetPick };
  return { action: "wait", marketPick, targetPick };
}

/** Projected starter value plus a small, position-aware bench-depth credit. */
export function scoreYahooRedraftRoster(players: YahooRedraftStrategyPlayer[]): number {
  const byPosition = new Map<RedraftPosition, YahooRedraftStrategyPlayer[]>(
    REDRAFT_POSITIONS.map((position) => [position, []]),
  );
  for (const player of players) {
    if (REDRAFT_POSITIONS.includes(player.position as RedraftPosition)) {
      byPosition.get(player.position as RedraftPosition)?.push(player);
    }
  }
  for (const rows of byPosition.values()) rows.sort((a, b) => effectivePoints(b) - effectivePoints(a));

  const starters: YahooRedraftStrategyPlayer[] = [];
  for (const position of REDRAFT_POSITIONS) {
    starters.push(...(byPosition.get(position) ?? []).slice(0, REDRAFT_STARTER_SLOTS[position]));
  }
  const starterIds = new Set(starters.map((player) => player.playerId));
  const flex = REDRAFT_FLEX_POSITIONS
    .flatMap((position) => byPosition.get(position) ?? [])
    .filter((player) => !starterIds.has(player.playerId))
    .sort((a, b) => effectivePoints(b) - effectivePoints(a))[0] ?? null;
  if (flex) starters.push(flex);

  const countedIds = new Set(starters.map((player) => player.playerId));
  const benchCredit = players
    .filter((player) => !countedIds.has(player.playerId))
    .reduce((sum, player) => {
      if (player.position === "K" || player.position === "DST") return sum;
      return sum + effectivePoints(player) * (player.position === "RB" || player.position === "WR" ? 0.2 : 0.08);
    }, 0);
  return starters.reduce((sum, player) => sum + effectivePoints(player), 0) + benchCredit;
}

function candidateSelectionScore(
  player: YahooRedraftStrategyPlayer,
  roster: YahooRedraftStrategyPlayer[],
  nextPick: number | null,
): number {
  const status = getRedraftRosterStatus(roster);
  const position = player.position as RedraftPosition;
  const rank = player.ourRank ?? 250;
  const marketGap = finite(player.yahooXRank) && finite(player.ourRank) ? player.yahooXRank - player.ourRank : 0;
  let construction = status.counts[position] < REDRAFT_STARTER_SLOTS[position] ? 12 : -2;
  if (FLEX_SET.has(position) && status.filledStarterSlots < 7) construction += 4;
  if ((position === "QB" || position === "TE") && status.counts[position] >= REDRAFT_STARTER_SLOTS[position]) construction -= 12;
  const round = nextPick === null ? 1 : Math.ceil(nextPick / REDRAFT_TEAM_COUNT);
  if ((position === "K" || position === "DST") && round < 12) construction -= 250;
  if ((position === "K" || position === "DST") && status.counts[position] >= 1) construction -= 250;
  return 300 - rank + Math.max(-20, Math.min(30, marketGap)) * 0.25 + construction;
}

function buildCandidatePool(
  available: YahooRedraftStrategyPlayer[],
  roster: YahooRedraftStrategyPlayer[],
  nextPick: number | null,
): YahooRedraftStrategyPlayer[] {
  const scored = available
    .map((player) => ({ player, score: candidateSelectionScore(player, roster, nextPick) }))
    .sort((a, b) => b.score - a.score || (a.player.ourRank ?? 999) - (b.player.ourRank ?? 999));
  const round = nextPick === null ? 1 : Math.ceil(nextPick / REDRAFT_TEAM_COUNT);
  const comparisonPositions: RedraftPosition[] = round >= 12 ? [...REDRAFT_POSITIONS] : ["QB", "RB", "WR", "TE"];
  const pool = [
    ...scored.slice(0, 4).map((row) => row.player),
    ...comparisonPositions.flatMap((position) => scored.find((row) => row.player.position === position)?.player ?? []),
  ].filter((player, index, rows) => rows.findIndex((row) => row.playerId === player.playerId) === index);
  return pool
    .sort((a, b) => candidateSelectionScore(b, roster, nextPick) - candidateSelectionScore(a, roster, nextPick))
    .slice(0, 6);
}

export function buildYahooRedraftStrategy(input: {
  roster: YahooRedraftStrategyPlayer[];
  availablePlayers: YahooRedraftStrategyPlayer[];
  nextPick: number | null;
  followingPick: number | null;
  teamCount?: number;
}): YahooRedraftStrategy {
  const teamCount = input.teamCount ?? REDRAFT_TEAM_COUNT;
  const available = input.availablePlayers
    .filter((player, index, rows) => rows.findIndex((row) => row.playerId === player.playerId) === index)
    .filter((player) => canAddRedraftPlayer(input.roster, player));
  if (input.roster.length >= REDRAFT_ROSTER_SIZE || !available.length) {
    return { model: "yahoo-redraft-two-pick-v1", nextPick: input.nextPick, followingPick: input.followingPick, recommendation: null, candidates: [] };
  }

  const currentCandidates = buildCandidatePool(available, input.roster, input.nextPick);
  const futurePool = available
    .filter((player) => isLikelyAvailableAtYahooPick(player, input.followingPick, teamCount))
    .sort((a, b) => (a.ourRank ?? 999) - (b.ourRank ?? 999))
    .slice(0, 50);
  const baselineValue = scoreYahooRedraftRoster(input.roster);

  const candidates = currentCandidates.map((candidate): YahooRedraftStrategyCandidate => {
    const rosterWithCandidate = [...input.roster, candidate];
    const onePickValue = scoreYahooRedraftRoster(rosterWithCandidate);
    const futureOptions = futurePool.filter((player) => player.playerId !== candidate.playerId && canAddRedraftPlayer(rosterWithCandidate, player));
    const bestPair = futureOptions
      .map((future) => ({ future, value: scoreYahooRedraftRoster([...rosterWithCandidate, future]) }))
      .sort((a, b) => b.value - a.value || (a.future.ourRank ?? 999) - (b.future.ourRank ?? 999))[0] ?? null;
    const samePositionReplacement = futureOptions
      .filter((player) => player.position === candidate.position)
      .sort((a, b) => (a.ourRank ?? 999) - (b.ourRank ?? 999))[0] ?? null;
    const candidatePoints = effectivePoints(candidate);
    const replacementPoints = samePositionReplacement ? effectivePoints(samePositionReplacement) : null;
    const retention = replacementPoints !== null && candidatePoints > 0 ? replacementPoints / candidatePoints : null;
    const timing = yahooTiming({ player: candidate, nextPick: input.nextPick, followingPick: input.followingPick, teamCount });
    return {
      ...candidate,
      onePickValueAdded: Math.max(0, onePickValue - baselineValue),
      twoPickValueAdded: Math.max(0, (bestPair?.value ?? onePickValue) - baselineValue),
      futureTargetPlayerId: bestPair?.future.playerId ?? null,
      futureTargetName: bestPair?.future.name ?? null,
      futureTargetPosition: bestPair?.future.position ?? null,
      samePositionReplacementName: samePositionReplacement?.name ?? null,
      replacementRetention: retention,
      pointsOverReplacement: replacementPoints === null ? null : candidatePoints - replacementPoints,
      yahooMarketPick: timing.marketPick,
      yahooTargetPick: timing.targetPick,
      yahooTimingAction: timing.action,
      yahooRankGap: finite(candidate.yahooXRank) && finite(candidate.ourRank) ? candidate.yahooXRank - candidate.ourRank : null,
      strategyLabel: "alternative",
      strategyExplanation: "Yahoo two-pick path evaluated.",
    };
  }).sort((a, b) => b.twoPickValueAdded - a.twoPickValueAdded
    || (b.pointsOverReplacement ?? -999) - (a.pointsOverReplacement ?? -999)
    || (a.ourRank ?? 999) - (b.ourRank ?? 999));

  const explained = candidates.map((candidate, index): YahooRedraftStrategyCandidate => {
    const percent = candidate.replacementRetention === null ? null : Math.round(candidate.replacementRetention * 100);
    const positionLabel = candidate.position === "DST" ? "DEF" : candidate.position;
    const replacementText = candidate.samePositionReplacementName === null || percent === null
      ? `No reliable later ${positionLabel} replacement is visible at pick #${input.followingPick ?? "—"}.`
      : percent >= 90
        ? `${positionLabel} can wait: ${candidate.samePositionReplacementName} retains about ${percent}% of this projection.`
        : percent < 82
          ? `${positionLabel} tier drop: ${candidate.samePositionReplacementName} retains only about ${percent}% of this projection.`
          : `Later ${positionLabel} option ${candidate.samePositionReplacementName} retains about ${percent}% of this projection.`;
    const pathText = candidate.futureTargetName
      ? `Best next target is ${candidate.futureTargetName} (${candidate.futureTargetPosition === "DST" ? "DEF" : candidate.futureTargetPosition}) at pick #${input.followingPick ?? "—"}.`
      : "No reliable next-pick partner is available.";
    const strategyLabel: YahooStrategyLabel = index === 0
      ? "best-path"
      : candidate.replacementRetention !== null && candidate.replacementRetention >= 0.9
        ? "position-can-wait"
        : candidate.replacementRetention !== null && candidate.replacementRetention < 0.82
          ? "tier-drop"
          : "alternative";
    return { ...candidate, strategyLabel, strategyExplanation: `${replacementText} ${pathText}` };
  });

  const best = explained[0] ?? null;
  return {
    model: "yahoo-redraft-two-pick-v1",
    nextPick: input.nextPick,
    followingPick: input.followingPick,
    recommendation: best ? {
      playerId: best.playerId,
      headline: best.futureTargetName
        ? `Draft ${best.name} now; target ${best.futureTargetName} at #${input.followingPick ?? "—"}`
        : `Draft ${best.name} now`,
      explanation: `This is the strongest Yahoo two-pick path, not simply the largest XRank discount. ${best.strategyExplanation}`,
    } : null,
    candidates: explained,
  };
}
