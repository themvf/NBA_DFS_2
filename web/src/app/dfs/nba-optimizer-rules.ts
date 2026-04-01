export type NbaTeamStackSize = 2 | 3 | 4 | 5;

export type NbaTeamStackRule = {
  teamId: number;
  stackSize: NbaTeamStackSize;
};

export type NbaRuleSettings = {
  playerLocks?: readonly number[];
  playerBlocks?: readonly number[];
  blockedTeamIds?: readonly number[];
  requiredTeamStacks?: readonly NbaTeamStackRule[];
};

export type NbaRulePlayer = {
  id: number;
  name: string;
  teamId: number | null;
  teamAbbrev: string;
  isOut: boolean | null;
  salary: number;
  ourProj: number | null;
};

export type NormalizedNbaRuleSelections = {
  playerLocks: number[];
  playerBlocks: number[];
  blockedTeamIds: number[];
  requiredTeamStacks: NbaTeamStackRule[];
};

type NbaRuleValidationResult =
  | { ok: true; normalized: NormalizedNbaRuleSelections }
  | { ok: false; normalized: NormalizedNbaRuleSelections; error: string };

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

function normalizeStackSize(value: number): NbaTeamStackSize {
  if (value >= 5) return 5;
  if (value <= 2) return 2;
  if (value === 4) return 4;
  return 3;
}

function uniqueTeamStacks(values: readonly NbaTeamStackRule[] | undefined): NbaTeamStackRule[] {
  if (!values) return [];
  const byTeam = new Map<number, NbaTeamStackRule>();
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

export function normalizeNbaRuleSelections(settings: NbaRuleSettings): NormalizedNbaRuleSelections {
  return {
    playerLocks: uniqueIds(settings.playerLocks),
    playerBlocks: uniqueIds(settings.playerBlocks),
    blockedTeamIds: uniqueIds(settings.blockedTeamIds),
    requiredTeamStacks: uniqueTeamStacks(settings.requiredTeamStacks),
  };
}

export function validateNbaRuleSelections(
  pool: readonly NbaRulePlayer[],
  settings: NbaRuleSettings,
): NbaRuleValidationResult {
  const normalized = normalizeNbaRuleSelections(settings);
  const playerById = new Map(pool.map((player) => [player.id, player]));
  const teamAbbrevById = new Map<number, string>();

  for (const player of pool) {
    if (player.teamId != null && !teamAbbrevById.has(player.teamId)) {
      teamAbbrevById.set(player.teamId, player.teamAbbrev);
    }
  }

  if (normalized.playerLocks.length > 8) {
    return {
      ok: false,
      normalized,
      error: `You locked ${normalized.playerLocks.length} players, but an NBA lineup only has 8 roster spots.`,
    };
  }

  const blockedPlayers = new Set(normalized.playerBlocks);
  const blockedTeams = new Set(normalized.blockedTeamIds);
  const lockedPlayers = new Set(normalized.playerLocks);

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
    .filter((playerId) => !playerById.has(playerId))
    .map((playerId) => `player ${playerId}`);
  if (missingLocks.length > 0) {
    return {
      ok: false,
      normalized,
      error: "Some locked players are not available in the current game selection.",
    };
  }

  const lockedOut = normalized.playerLocks
    .map((playerId) => playerById.get(playerId))
    .filter((player): player is NbaRulePlayer => !!player && !!player.isOut)
    .map((player) => player.name);
  if (lockedOut.length > 0) {
    return {
      ok: false,
      normalized,
      error: `Remove invalid locks before optimizing. These locked players are OUT: ${nameList(lockedOut)}.`,
    };
  }

  const lockedOnBlockedTeams = normalized.playerLocks
    .map((playerId) => playerById.get(playerId))
    .filter((player): player is NbaRulePlayer => !!player && player.teamId != null && blockedTeams.has(player.teamId))
    .map((player) => player.name);
  if (lockedOnBlockedTeams.length > 0) {
    return {
      ok: false,
      normalized,
      error: `These locked players are on blocked teams: ${nameList(lockedOnBlockedTeams)}.`,
    };
  }

  const lockedIneligible = normalized.playerLocks
    .map((playerId) => playerById.get(playerId))
    .filter((player): player is NbaRulePlayer => !!player && !player.isOut && (!(player.ourProj != null && player.ourProj > 0) || player.salary <= 0))
    .map((player) => player.name);
  if (lockedIneligible.length > 0) {
    return {
      ok: false,
      normalized,
      error: `These locked players are not optimizer-eligible: ${nameList(lockedIneligible)}.`,
    };
  }

  const missingStackTeams = normalized.requiredTeamStacks
    .filter((rule) => !teamAbbrevById.has(rule.teamId))
    .map((rule) => `team ${rule.teamId}`);
  if (missingStackTeams.length > 0) {
    return {
      ok: false,
      normalized,
      error: "Some stacked teams are not available in the current game selection.",
    };
  }

  const availableStackCounts = new Map<number, number>();
  for (const player of pool) {
    if (player.teamId == null) continue;
    if (blockedPlayers.has(player.id) || blockedTeams.has(player.teamId)) continue;
    if (player.isOut) continue;
    if (!(player.ourProj != null && player.ourProj > 0) || player.salary <= 0) continue;
    availableStackCounts.set(player.teamId, (availableStackCounts.get(player.teamId) ?? 0) + 1);
  }

  const impossibleStacks = normalized.requiredTeamStacks
    .filter((rule) => (availableStackCounts.get(rule.teamId) ?? 0) < rule.stackSize)
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
