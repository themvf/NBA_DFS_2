import { BEST_BALL_POSITIONS, canAddCompletableBestBallPlayer, type BestBallPosition } from "./best-ball";

export type DraftKingsBestBallStatLine = {
  passingYards?: number;
  passingTouchdowns?: number;
  interceptions?: number;
  rushingYards?: number;
  rushingTouchdowns?: number;
  receptions?: number;
  receivingYards?: number;
  receivingTouchdowns?: number;
  returnTouchdowns?: number;
  fumblesLost?: number;
  twoPointConversions?: number;
  offensiveFumbleRecoveryTouchdowns?: number;
};

/** Exact DraftKings Best Ball offensive scoring, including yardage bonuses. */
export function scoreDraftKingsBestBallLine(line: DraftKingsBestBallStatLine): number {
  const passingYards = line.passingYards ?? 0;
  const rushingYards = line.rushingYards ?? 0;
  const receivingYards = line.receivingYards ?? 0;
  return passingYards * 0.04
    + (line.passingTouchdowns ?? 0) * 4
    - (line.interceptions ?? 0)
    + rushingYards * 0.1
    + (line.rushingTouchdowns ?? 0) * 6
    + (line.receptions ?? 0)
    + receivingYards * 0.1
    + (line.receivingTouchdowns ?? 0) * 6
    + (passingYards >= 300 ? 3 : 0)
    + (rushingYards >= 100 ? 3 : 0)
    + (receivingYards >= 100 ? 3 : 0)
    + (line.returnTouchdowns ?? 0) * 6
    - (line.fumblesLost ?? 0)
    + (line.twoPointConversions ?? 0) * 2
    + (line.offensiveFumbleRecoveryTouchdowns ?? 0) * 6;
}

export type ShadowBestBallPlayer = {
  playerId: number;
  name: string;
  position: string;
  team: string | null;
  byeWeek: number | null;
  projectedPoints: number | null;
  projectionLow: number | null;
  projectionHigh: number | null;
  expectedGames: number | null;
  confidence: number | null;
  ourRank?: number | null;
  dkBestBallRank?: number | null;
  dkBestBallAdp?: number | null;
};

export type DraftMarketSignal = "major-discount" | "discount" | "fair" | "premium" | "unavailable";
export type DraftMarketAction = "wait" | "target-soon" | "take-now" | "pass-at-price" | "no-market-data";
export type BestBallStrategyLabel = "best-path" | "position-can-wait" | "tier-drop" | "alternative";

function draftMarketPick(player: ShadowBestBallPlayer): number | null {
  const values = [player.dkBestBallRank, player.dkBestBallAdp]
    .filter((value): value is number => value !== null && value !== undefined && Number.isFinite(value) && value > 0);
  return values.length ? Math.min(...values) : null;
}

/**
 * A player is treated as a plausible next-turn target when DraftKings market
 * pressure has not arrived more than half a round before the user's next pick.
 * This deliberately uses DraftKings Rank/ADP rather than a redraft feed.
 */
export function isLikelyAvailableAtDraftKingsPick(
  player: ShadowBestBallPlayer,
  pick: number | null,
  teamCount = 12,
): boolean {
  if (pick === null) return false;
  const marketPick = draftMarketPick(player);
  const safetyBuffer = Math.max(3, Math.ceil(teamCount / 2));
  if (marketPick !== null) return marketPick >= pick - safetyBuffer;
  return player.ourRank !== null && player.ourRank !== undefined && player.ourRank >= pick - safetyBuffer;
}

export function getDraftMarketSignal(ourRank: number | null, marketRank: number | null): {
  gap: number | null;
  signal: DraftMarketSignal;
} {
  if (ourRank === null || marketRank === null) return { gap: null, signal: "unavailable" };
  const gap = marketRank - ourRank;
  if (gap >= 12) return { gap, signal: "major-discount" };
  if (gap >= 5) return { gap, signal: "discount" };
  if (gap <= -5) return { gap, signal: "premium" };
  return { gap, signal: "fair" };
}

export function getDraftMarketTiming(input: {
  ourRank: number | null;
  marketRank: number | null;
  marketAdp: number | null;
  nextUserPick: number | null;
  followingUserPick: number | null;
  teamCount?: number;
}): {
  action: DraftMarketAction;
  marketPick: number | null;
  targetPick: number | null;
} {
  const marketRanks = [input.marketRank, input.marketAdp]
    .filter((value): value is number => value !== null && Number.isFinite(value) && value > 0);
  if (!marketRanks.length || input.nextUserPick === null) {
    return { action: "no-market-data", marketPick: null, targetPick: null };
  }

  // The earlier of platform rank and ADP is the safer estimate of when a room
  // starts applying pressure. Move half a round ahead of it rather than
  // pretending the player will survive all the way to the raw market rank.
  const marketPick = Math.min(...marketRanks);
  const safetyBuffer = Math.max(3, Math.ceil((input.teamCount ?? 12) / 2));
  const targetPick = Math.max(1, Math.floor(marketPick - safetyBuffer));

  if (input.ourRank !== null && marketPick <= input.ourRank - 5 && input.nextUserPick < input.ourRank) {
    return { action: "pass-at-price", marketPick, targetPick: Math.max(1, Math.floor(input.ourRank)) };
  }
  if (input.nextUserPick >= targetPick || (input.followingUserPick !== null && input.followingUserPick > marketPick)) {
    return { action: "take-now", marketPick, targetPick };
  }
  if (input.followingUserPick !== null && input.followingUserPick >= targetPick) {
    return { action: "target-soon", marketPick, targetPick };
  }
  return { action: "wait", marketPick, targetPick };
}

export type BestBallLineupResult = {
  points: number;
  countedPlayerIds: number[];
};

export type ShadowBestBallCandidateResult = {
  playerId: number;
  name: string;
  position: string;
  marginalCountedPoints: number;
  expectedCountedPoints: number;
  expectedCountedWeeks: number;
  p90RosterDelta: number;
  baselineRosterMean: number;
  rosterMeanWithCandidate: number;
  confidence: number | null;
  ourRank: number | null;
  projectedPoints: number | null;
  dkBestBallRank: number | null;
  dkBestBallAdp: number | null;
  dkRankGap: number | null;
  dkMarketSignal: DraftMarketSignal;
  dkDraftAction: DraftMarketAction;
  dkMarketPick: number | null;
  dkTargetPick: number | null;
  twoPickMarginalPoints: number;
  twoPickP90Delta: number;
  futureTargetPlayerId: number | null;
  futureTargetName: string | null;
  futureTargetPosition: string | null;
  futureTargetMarketPick: number | null;
  samePositionReplacementName: string | null;
  samePositionReplacementPoints: number | null;
  pointsOverReplacement: number | null;
  replacementRetention: number | null;
  strategyLabel: BestBallStrategyLabel;
  strategyExplanation: string;
};

export type ShadowBestBallSimulation = {
  model: "shadow-v0-v1.7-two-pick";
  iterations: number;
  candidates: ShadowBestBallCandidateResult[];
  recommendation: {
    playerId: number;
    headline: string;
    explanation: string;
  } | null;
};

const STARTERS: Record<BestBallPosition, number> = { QB: 1, RB: 2, WR: 3, TE: 1 };

// Explicit shadow-model priors, not calibrated production coefficients. They
// supply weekly variance until the V2 stat/opportunity distributions are live.
const SHADOW_WEEKLY_CV: Record<BestBallPosition, number> = {
  QB: 0.42,
  RB: 0.72,
  WR: 0.82,
  TE: 0.88,
};

function finite(value: number | null | undefined, fallback: number): number {
  return value !== null && value !== undefined && Number.isFinite(value) ? value : fallback;
}

function hash32(value: number): number {
  let result = value | 0;
  result = Math.imul(result ^ (result >>> 16), 0x45d9f3b);
  result = Math.imul(result ^ (result >>> 16), 0x45d9f3b);
  return (result ^ (result >>> 16)) >>> 0;
}

function uniform(seed: number): number {
  return (hash32(seed) + 0.5) / 4294967296;
}

function normal(seed: number): number {
  const u1 = Math.max(1e-9, uniform(seed));
  const u2 = uniform(seed ^ 0x9e3779b9);
  return Math.sqrt(-2 * Math.log(u1)) * Math.cos(2 * Math.PI * u2);
}

function percentile(values: number[], probability: number): number {
  if (!values.length) return 0;
  const ordered = [...values].sort((a, b) => a - b);
  const index = Math.min(ordered.length - 1, Math.max(0, Math.ceil(probability * ordered.length) - 1));
  return ordered[index];
}

function weeklyPoints(player: ShadowBestBallPlayer, week: number, iteration: number): number {
  if (!BEST_BALL_POSITIONS.includes(player.position as BestBallPosition)) return 0;
  if (player.byeWeek === week) return 0;
  const seasonMean = finite(player.projectedPoints, 0);
  if (seasonMean <= 0) return 0;

  const activeProbability = Math.min(1, Math.max(0, finite(player.expectedGames, 17) / 17));
  const availabilitySeed = player.playerId * 1_000_003 + iteration * 7_919 + week * 101;
  if (uniform(availabilitySeed) > activeProbability) return 0;

  const low = finite(player.projectionLow, seasonMean * 0.85);
  const high = finite(player.projectionHigh, seasonMean * 1.15);
  const seasonUncertainty = Math.max(0, high - low) / Math.max(1, seasonMean) / 2.56;
  const scenarioMean = Math.max(0, seasonMean * (1 + normal(player.playerId * 65_537 + iteration * 257) * seasonUncertainty));
  // V1.6 is a 17-active-game baseline and availability is intentionally stored
  // separately. Keep its conditional-on-active PPG here; zeroed inactive weeks
  // then reduce realized roster value without inflating healthy-week scoring.
  const activeMean = scenarioMean / 17;
  const cv = SHADOW_WEEKLY_CV[player.position as BestBallPosition];
  return Math.max(0, activeMean + normal(availabilitySeed ^ 0x85ebca6b) * activeMean * cv);
}

/** Selects the exact legal weekly Best Ball lineup from supplied player scores. */
export function selectBestBallLineup(
  players: readonly ShadowBestBallPlayer[],
  scoreByPlayerId: ReadonlyMap<number, number>,
): BestBallLineupResult {
  const available = new Map<BestBallPosition, Array<{ playerId: number; points: number }>>(
    BEST_BALL_POSITIONS.map((position) => [position, []]),
  );
  for (const player of players) {
    if (!BEST_BALL_POSITIONS.includes(player.position as BestBallPosition)) continue;
    available.get(player.position as BestBallPosition)?.push({
      playerId: player.playerId,
      points: Math.max(0, scoreByPlayerId.get(player.playerId) ?? 0),
    });
  }
  for (const rows of available.values()) rows.sort((a, b) => b.points - a.points || a.playerId - b.playerId);

  const counted: Array<{ playerId: number; points: number }> = [];
  for (const position of BEST_BALL_POSITIONS) counted.push(...(available.get(position) ?? []).slice(0, STARTERS[position]));
  const countedIds = new Set(counted.map((row) => row.playerId));
  const flex = (["RB", "WR", "TE"] as const)
    .flatMap((position) => available.get(position) ?? [])
    .filter((row) => !countedIds.has(row.playerId))
    .sort((a, b) => b.points - a.points || a.playerId - b.playerId)[0];
  if (flex) counted.push(flex);
  return {
    points: counted.reduce((sum, row) => sum + row.points, 0),
    countedPlayerIds: counted.filter((row) => row.points > 0).map((row) => row.playerId),
  };
}

export function simulateShadowBestBallCandidates(input: {
  roster: ShadowBestBallPlayer[];
  candidates: ShadowBestBallPlayer[];
  futureCandidates?: ShadowBestBallPlayer[];
  iterations?: number;
  nextUserPick?: number | null;
  followingUserPick?: number | null;
  teamCount?: number;
}): ShadowBestBallSimulation {
  const iterations = Math.min(500, Math.max(40, Math.round(input.iterations ?? 160)));
  const futureOptions = (input.futureCandidates ?? [])
    .filter((player, index, rows) => BEST_BALL_POSITIONS.includes(player.position as BestBallPosition)
      && rows.findIndex((row) => row.playerId === player.playerId) === index
      && isLikelyAvailableAtDraftKingsPick(player, input.followingUserPick ?? null, input.teamCount))
    .sort((a, b) => (a.ourRank ?? 999) - (b.ourRank ?? 999))
    .slice(0, 36);
  const futureByCandidate = new Map(input.candidates.map((candidate) => [
    candidate.playerId,
    futureOptions.filter((player) => player.playerId !== candidate.playerId
      && canAddCompletableBestBallPlayer([...input.roster, candidate], player)),
  ]));
  const baselineSeasons: number[] = [];
  const candidateSeasons = new Map<number, number[]>();
  const pairSeasons = new Map<string, number[]>();
  const candidateCountedPoints = new Map<number, number>();
  const candidateCountedWeeks = new Map<number, number>();
  for (const candidate of input.candidates) {
    candidateSeasons.set(candidate.playerId, []);
    for (const future of futureByCandidate.get(candidate.playerId) ?? []) {
      pairSeasons.set(`${candidate.playerId}:${future.playerId}`, []);
    }
  }

  const simulationPlayers = [...input.roster, ...input.candidates, ...futureOptions]
    .filter((player, index, rows) => rows.findIndex((row) => row.playerId === player.playerId) === index);

  for (let iteration = 0; iteration < iterations; iteration += 1) {
    let baselineSeason = 0;
    const withCandidateSeason = new Map(input.candidates.map((candidate) => [candidate.playerId, 0]));
    const withPairSeason = new Map<string, number>();
    for (const key of pairSeasons.keys()) withPairSeason.set(key, 0);
    for (let week = 1; week <= 17; week += 1) {
      const scoreMap = new Map<number, number>();
      for (const player of simulationPlayers) {
        if (!scoreMap.has(player.playerId)) scoreMap.set(player.playerId, weeklyPoints(player, week, iteration));
      }
      const baseline = selectBestBallLineup(input.roster, scoreMap);
      baselineSeason += baseline.points;
      for (const candidate of input.candidates) {
        const lineup = selectBestBallLineup([...input.roster, candidate], scoreMap);
        withCandidateSeason.set(candidate.playerId, (withCandidateSeason.get(candidate.playerId) ?? 0) + lineup.points);
        if (lineup.countedPlayerIds.includes(candidate.playerId)) {
          candidateCountedPoints.set(candidate.playerId, (candidateCountedPoints.get(candidate.playerId) ?? 0) + (scoreMap.get(candidate.playerId) ?? 0));
          candidateCountedWeeks.set(candidate.playerId, (candidateCountedWeeks.get(candidate.playerId) ?? 0) + 1);
        }
        for (const future of futureByCandidate.get(candidate.playerId) ?? []) {
          const key = `${candidate.playerId}:${future.playerId}`;
          const pairLineup = selectBestBallLineup([...input.roster, candidate, future], scoreMap);
          withPairSeason.set(key, (withPairSeason.get(key) ?? 0) + pairLineup.points);
        }
      }
    }
    baselineSeasons.push(baselineSeason);
    for (const candidate of input.candidates) candidateSeasons.get(candidate.playerId)?.push(withCandidateSeason.get(candidate.playerId) ?? baselineSeason);
    for (const [key, season] of withPairSeason) pairSeasons.get(key)?.push(season);
  }

  const baselineMean = baselineSeasons.reduce((sum, value) => sum + value, 0) / iterations;
  const baselineP90 = percentile(baselineSeasons, 0.9);
  const candidates = input.candidates.map((candidate): ShadowBestBallCandidateResult => {
    const seasons = candidateSeasons.get(candidate.playerId) ?? [];
    const rosterMeanWithCandidate = seasons.reduce((sum, value) => sum + value, 0) / Math.max(1, seasons.length);
    const pairResults = (futureByCandidate.get(candidate.playerId) ?? []).map((future) => {
      const pairValues = pairSeasons.get(`${candidate.playerId}:${future.playerId}`) ?? [];
      return {
        future,
        values: pairValues,
        mean: pairValues.reduce((sum, value) => sum + value, 0) / Math.max(1, pairValues.length),
        p90: percentile(pairValues, 0.9),
      };
    }).sort((a, b) => b.mean - a.mean || b.p90 - a.p90 || (a.future.ourRank ?? 999) - (b.future.ourRank ?? 999));
    const bestPair = pairResults[0] ?? null;
    const samePositionReplacement = (futureByCandidate.get(candidate.playerId) ?? [])
      .filter((future) => future.position === candidate.position)
      .sort((a, b) => (a.ourRank ?? 999) - (b.ourRank ?? 999))[0] ?? null;
    const candidatePoints = candidate.projectedPoints ?? null;
    const replacementPoints = samePositionReplacement?.projectedPoints ?? null;
    const pointsOverReplacement = candidatePoints !== null && replacementPoints !== null
      ? candidatePoints - replacementPoints
      : null;
    const replacementRetention = candidatePoints !== null && candidatePoints > 0 && replacementPoints !== null
      ? replacementPoints / candidatePoints
      : null;
    const dkMarket = getDraftMarketSignal(candidate.ourRank ?? null, candidate.dkBestBallRank ?? null);
    const dkTiming = getDraftMarketTiming({
      ourRank: candidate.ourRank ?? null,
      marketRank: candidate.dkBestBallRank ?? null,
      marketAdp: candidate.dkBestBallAdp ?? null,
      nextUserPick: input.nextUserPick ?? null,
      followingUserPick: input.followingUserPick ?? null,
      teamCount: input.teamCount,
    });
    return {
      playerId: candidate.playerId,
      name: candidate.name,
      position: candidate.position,
      marginalCountedPoints: Math.max(0, rosterMeanWithCandidate - baselineMean),
      expectedCountedPoints: (candidateCountedPoints.get(candidate.playerId) ?? 0) / iterations,
      expectedCountedWeeks: (candidateCountedWeeks.get(candidate.playerId) ?? 0) / iterations,
      p90RosterDelta: Math.max(0, percentile(seasons, 0.9) - baselineP90),
      baselineRosterMean: baselineMean,
      rosterMeanWithCandidate,
      confidence: candidate.confidence,
      ourRank: candidate.ourRank ?? null,
      projectedPoints: candidate.projectedPoints,
      dkBestBallRank: candidate.dkBestBallRank ?? null,
      dkBestBallAdp: candidate.dkBestBallAdp ?? null,
      dkRankGap: dkMarket.gap,
      dkMarketSignal: dkMarket.signal,
      dkDraftAction: dkTiming.action,
      dkMarketPick: dkTiming.marketPick,
      dkTargetPick: dkTiming.targetPick,
      twoPickMarginalPoints: Math.max(0, (bestPair?.mean ?? rosterMeanWithCandidate) - baselineMean),
      twoPickP90Delta: Math.max(0, (bestPair?.p90 ?? percentile(seasons, 0.9)) - baselineP90),
      futureTargetPlayerId: bestPair?.future.playerId ?? null,
      futureTargetName: bestPair?.future.name ?? null,
      futureTargetPosition: bestPair?.future.position ?? null,
      futureTargetMarketPick: bestPair ? draftMarketPick(bestPair.future) : null,
      samePositionReplacementName: samePositionReplacement?.name ?? null,
      samePositionReplacementPoints: replacementPoints,
      pointsOverReplacement,
      replacementRetention,
      strategyLabel: "alternative",
      strategyExplanation: "Two-pick path evaluated against likely DraftKings availability.",
    };
  }).sort((a, b) => b.twoPickMarginalPoints - a.twoPickMarginalPoints
    || b.twoPickP90Delta - a.twoPickP90Delta
    || b.marginalCountedPoints - a.marginalCountedPoints);

  const explainedCandidates = candidates.map((candidate, index): ShadowBestBallCandidateResult => {
    const percent = candidate.replacementRetention === null ? null : Math.round(candidate.replacementRetention * 100);
    const replacementExplanation = candidate.samePositionReplacementName === null || percent === null
      ? `No reliable later ${candidate.position} replacement is visible at pick #${input.followingUserPick ?? "—"}.`
      : percent >= 90
        ? `${candidate.position} can wait: ${candidate.samePositionReplacementName} preserves about ${percent}% of this projection.`
        : percent < 82
          ? `${candidate.position} tier drop: ${candidate.samePositionReplacementName} preserves only about ${percent}% of this projection.`
          : `Later ${candidate.position} option ${candidate.samePositionReplacementName} preserves about ${percent}% of this projection.`;
    const pathExplanation = candidate.futureTargetName
      ? `Best next target is ${candidate.futureTargetName} (${candidate.futureTargetPosition}) around pick #${input.followingUserPick ?? "—"}.`
      : "No reliable next-pick partner is available.";
    const strategyLabel: BestBallStrategyLabel = index === 0
      ? "best-path"
      : candidate.replacementRetention !== null && candidate.replacementRetention >= 0.9
        ? "position-can-wait"
        : candidate.replacementRetention !== null && candidate.replacementRetention < 0.82
          ? "tier-drop"
          : "alternative";
    return {
      ...candidate,
      strategyLabel,
      strategyExplanation: `${replacementExplanation} ${pathExplanation}`,
    };
  });

  const best = explainedCandidates[0] ?? null;
  const recommendation = best ? {
    playerId: best.playerId,
    headline: best.futureTargetName
      ? `Draft ${best.name} now; target ${best.futureTargetName} at #${input.followingUserPick ?? "—"}`
      : `Draft ${best.name} now`,
    explanation: `This is the strongest simulated two-pick path, not simply the largest DraftKings discount. ${best.strategyExplanation}`,
  } : null;

  return { model: "shadow-v0-v1.7-two-pick", iterations, candidates: explainedCandidates, recommendation };
}
