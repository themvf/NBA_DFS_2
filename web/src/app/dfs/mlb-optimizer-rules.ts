export const MLB_MAX_HITTERS_PER_TEAM = 5;

export type MlbTeamStackSize = 2 | 3 | 4 | 5;

export type MlbTeamStackRule = {
  teamId: number;
  stackSize: MlbTeamStackSize;
};

export type MlbRuleSettings = {
  playerLocks?: readonly number[];
  playerBlocks?: readonly number[];
  blockedTeamIds?: readonly number[];
  requiredTeamStacks?: readonly MlbTeamStackRule[];
};

export type MlbRulePlayer = {
  id: number;
  name: string;
  teamId: number | null;
  teamAbbrev: string;
  eligiblePositions: string;
  isOut: boolean | null;
  salary: number;
  ourProj: number | null;
  linestarProj?: number | null;
};

export type NormalizedMlbRuleSelections = {
  playerLocks: number[];
  playerBlocks: number[];
  blockedTeamIds: number[];
  requiredTeamStacks: MlbTeamStackRule[];
};

type MlbRuleValidationResult =
  | { ok: true; normalized: NormalizedMlbRuleSelections }
  | { ok: false; normalized: NormalizedMlbRuleSelections; error: string };

function uniqueIds(values: readonly number[] | undefined): number[] {
  if (!values) return [];
  const seen = new Set<number>();
  const next: number[] = [];
  for (const value of values) {
    if (!Number.isInteger(value) || value <= 0 || seen.has(value)) continue;
    seen.add(value);
    next.push(value);
  }
  return next;
}

function normalizeStackSize(value: number): MlbTeamStackSize {
  if (value >= 5) return 5;
  if (value <= 2) return 2;
  if (value === 4) return 4;
  return 3;
}

function uniqueTeamStacks(values: readonly MlbTeamStackRule[] | undefined): MlbTeamStackRule[] {
  if (!values) return [];
  const byTeam = new Map<number, MlbTeamStackRule>();
  for (const value of values) {
    if (!Number.isInteger(value?.teamId) || value.teamId <= 0) continue;
    byTeam.set(value.teamId, {
      teamId: value.teamId,
      stackSize: normalizeStackSize(value.stackSize),
    });
  }
  return Array.from(byTeam.values());
}

function nameList(values: string[]): string {
  return values.join(", ");
}

function teamName(teamId: number, teamAbbrevById: Map<number, string>): string {
  return teamAbbrevById.get(teamId) ?? `team ${teamId}`;
}

function isPitcher(eligiblePositions: string): boolean {
  return eligiblePositions.includes("SP") || eligiblePositions.includes("RP");
}

function effectiveProjection(player: Pick<MlbRulePlayer, "ourProj" | "linestarProj">): number | null {
  const projection = player.ourProj ?? player.linestarProj ?? null;
  return projection != null && Number.isFinite(projection) ? projection : null;
}

function hasPositiveProjection(player: Pick<MlbRulePlayer, "ourProj" | "linestarProj">): boolean {
  return (effectiveProjection(player) ?? 0) > 0;
}

export function normalizeMlbRuleSelections(settings: MlbRuleSettings): NormalizedMlbRuleSelections {
  return {
    playerLocks: uniqueIds(settings.playerLocks),
    playerBlocks: uniqueIds(settings.playerBlocks),
    blockedTeamIds: uniqueIds(settings.blockedTeamIds),
    requiredTeamStacks: uniqueTeamStacks(settings.requiredTeamStacks),
  };
}

export function validateMlbRuleSelections(
  pool: readonly MlbRulePlayer[],
  settings: MlbRuleSettings,
): MlbRuleValidationResult {
  const normalized = normalizeMlbRuleSelections(settings);
  const playerById = new Map(pool.map((player) => [player.id, player]));
  const teamAbbrevById = new Map<number, string>();

  for (const player of pool) {
    if (player.teamId != null && !teamAbbrevById.has(player.teamId)) {
      teamAbbrevById.set(player.teamId, player.teamAbbrev);
    }
  }

  if (normalized.playerLocks.length > 10) {
    return {
      ok: false,
      normalized,
      error: `You locked ${normalized.playerLocks.length} players, but an MLB lineup only has 10 roster spots.`,
    };
  }

  const blockedPlayers = new Set(normalized.playerBlocks);
  const blockedTeams = new Set(normalized.blockedTeamIds);

  const lockBlockOverlap = normalized.playerLocks
    .filter((playerId) => blockedPlayers.has(playerId))
    .map((playerId) => playerById.get(playerId)?.name ?? `player ${playerId}`);
  if (lockBlockOverlap.length > 0) {
    return {
      ok: false,
      normalized,
      error: `These players are both locked and blocked: ${nameList(lockBlockOverlap)}.`,
    };
  }

  const blockedRequiredTeams = normalized.requiredTeamStacks
    .filter((rule) => blockedTeams.has(rule.teamId))
    .map((rule) => teamName(rule.teamId, teamAbbrevById));
  if (blockedRequiredTeams.length > 0) {
    return {
      ok: false,
      normalized,
      error: `These teams are both stacked and blocked: ${nameList(blockedRequiredTeams)}.`,
    };
  }

  const missingLocks = normalized.playerLocks
    .filter((playerId) => !playerById.has(playerId));
  if (missingLocks.length > 0) {
    return {
      ok: false,
      normalized,
      error: "Some locked players are not available in the current game selection.",
    };
  }

  const lockedPlayers = normalized.playerLocks
    .map((playerId) => playerById.get(playerId))
    .filter((player): player is MlbRulePlayer => !!player);

  const lockedOut = lockedPlayers
    .filter((player) => !!player.isOut)
    .map((player) => player.name);
  if (lockedOut.length > 0) {
    return {
      ok: false,
      normalized,
      error: `Remove invalid locks before optimizing. These locked players are unavailable: ${nameList(lockedOut)}.`,
    };
  }

  const lockedOnBlockedTeams = lockedPlayers
    .filter((player) => player.teamId != null && blockedTeams.has(player.teamId))
    .map((player) => player.name);
  if (lockedOnBlockedTeams.length > 0) {
    return {
      ok: false,
      normalized,
      error: `These locked players are on blocked teams: ${nameList(lockedOnBlockedTeams)}.`,
    };
  }

  const lockedIneligible = lockedPlayers
    .filter((player) => (!hasPositiveProjection(player) || player.salary <= 0))
    .map((player) => player.name);
  if (lockedIneligible.length > 0) {
    return {
      ok: false,
      normalized,
      error: `These locked players are not optimizer-eligible: ${nameList(lockedIneligible)}.`,
    };
  }

  const lockedPitchers = lockedPlayers.filter((player) => isPitcher(player.eligiblePositions));
  if (lockedPitchers.length > 2) {
    return {
      ok: false,
      normalized,
      error: `You locked ${lockedPitchers.length} pitchers, but an MLB lineup only has 2 pitcher slots.`,
    };
  }

  const lockedBatters = lockedPlayers.filter((player) => !isPitcher(player.eligiblePositions));
  if (lockedBatters.length > 8) {
    return {
      ok: false,
      normalized,
      error: `You locked ${lockedBatters.length} hitters, but an MLB lineup only has 8 hitter slots.`,
    };
  }

  const lockedBatterCountsByTeam = new Map<number, number>();
  for (const player of lockedBatters) {
    if (player.teamId == null) continue;
    lockedBatterCountsByTeam.set(player.teamId, (lockedBatterCountsByTeam.get(player.teamId) ?? 0) + 1);
  }
  const overTeamLimitLocks = Array.from(lockedBatterCountsByTeam.entries())
    .filter(([, count]) => count > MLB_MAX_HITTERS_PER_TEAM)
    .map(([teamId, count]) => `${teamName(teamId, teamAbbrevById)} (${count})`);
  if (overTeamLimitLocks.length > 0) {
    return {
      ok: false,
      normalized,
      error: `DraftKings allows at most ${MLB_MAX_HITTERS_PER_TEAM} hitters from one MLB team. These locks exceed that limit: ${nameList(overTeamLimitLocks)}.`,
    };
  }

  const missingStackTeams = normalized.requiredTeamStacks
    .filter((rule) => !teamAbbrevById.has(rule.teamId));
  if (missingStackTeams.length > 0) {
    return {
      ok: false,
      normalized,
      error: "Some stacked teams are not available in the current game selection.",
    };
  }

  const availableBatterCounts = new Map<number, number>();
  for (const player of pool) {
    if (player.teamId == null || isPitcher(player.eligiblePositions)) continue;
    if (blockedPlayers.has(player.id) || blockedTeams.has(player.teamId)) continue;
    if (player.isOut) continue;
    if (!hasPositiveProjection(player) || player.salary <= 0) continue;
    availableBatterCounts.set(player.teamId, (availableBatterCounts.get(player.teamId) ?? 0) + 1);
  }

  const impossibleStacks = normalized.requiredTeamStacks
    .filter((rule) => (availableBatterCounts.get(rule.teamId) ?? 0) < rule.stackSize)
    .map((rule) => `${teamName(rule.teamId, teamAbbrevById)} (${rule.stackSize})`);
  if (impossibleStacks.length > 0) {
    return {
      ok: false,
      normalized,
      error: `These team stacks cannot be satisfied with the current pool: ${nameList(impossibleStacks)}.`,
    };
  }

  return { ok: true, normalized };
}
