import type { GeneratedLineup, OptimizerPlayer, OptimizerSettings } from "../src/app/dfs/optimizer";

type RequiredTeamStackRule = NonNullable<OptimizerSettings["requiredTeamStacks"]>[number];

type SyntheticPoolOptions = {
  gameCount: number;
  valuePlayersPerTeam?: number;
};

const TEAM_PLAYER_TEMPLATES: Array<{
  role: string;
  eligiblePositions: string;
  salary: number;
  projection: number;
  leverage: number;
}> = [
  { role: "Primary PG", eligiblePositions: "PG", salary: 9600, projection: 48, leverage: 9.5 },
  { role: "Combo G", eligiblePositions: "PG/SG", salary: 8400, projection: 41, leverage: 8.2 },
  { role: "Scoring SG", eligiblePositions: "SG", salary: 7600, projection: 36, leverage: 7.4 },
  { role: "Wing", eligiblePositions: "SG/SF", salary: 6900, projection: 33, leverage: 6.4 },
  { role: "Forward", eligiblePositions: "SF/PF", salary: 6300, projection: 29, leverage: 5.7 },
  { role: "Power F", eligiblePositions: "PF", salary: 5800, projection: 27, leverage: 4.8 },
  { role: "Stretch Big", eligiblePositions: "PF/C", salary: 5400, projection: 24, leverage: 4.2 },
  { role: "Center", eligiblePositions: "C", salary: 5000, projection: 22, leverage: 3.8 },
];

const VALUE_PLAYER_TEMPLATES: Array<{
  role: string;
  eligiblePositions: string;
  salary: number;
  projection: number;
  leverage: number;
}> = [
  { role: "Value G", eligiblePositions: "PG/SG", salary: 4100, projection: 18, leverage: 3.2 },
  { role: "Value Wing", eligiblePositions: "SF/PF", salary: 3800, projection: 16, leverage: 2.8 },
  { role: "Value C", eligiblePositions: "C", salary: 3500, projection: 14, leverage: 2.4 },
];

function buildGameKey(gameIndex: number): string {
  const away = `T${String(gameIndex * 2 + 1).padStart(2, "0")}`;
  const home = `T${String(gameIndex * 2 + 2).padStart(2, "0")}`;
  return `${away}@${home}`;
}

export function createSyntheticNbaPool(options: SyntheticPoolOptions): OptimizerPlayer[] {
  const valuePlayersPerTeam = options.valuePlayersPerTeam ?? 2;
  const pool: OptimizerPlayer[] = [];
  let id = 1;
  let dkPlayerId = 10_000;

  for (let gameIndex = 0; gameIndex < options.gameCount; gameIndex++) {
    const matchupId = gameIndex + 1;
    const awayTeamId = gameIndex * 2 + 1;
    const homeTeamId = gameIndex * 2 + 2;
    const gameKey = buildGameKey(gameIndex);
    const gameBoost = (options.gameCount - gameIndex) * 0.35;

    for (const teamId of [awayTeamId, homeTeamId]) {
      const isHome = teamId === homeTeamId;
      const teamOffset = isHome ? 0.8 : 0;
      const teamAbbrev = `T${String(teamId).padStart(2, "0")}`;
      const templates = [
        ...TEAM_PLAYER_TEMPLATES,
        ...VALUE_PLAYER_TEMPLATES.slice(0, valuePlayersPerTeam),
      ];

      templates.forEach((template, index) => {
        const salaryOffset = (templates.length - index) * (isHome ? 40 : 25);
        const projectionOffset = Math.max(0, (templates.length - index) * 0.18);
        pool.push({
          id,
          dkPlayerId,
          name: `${teamAbbrev} ${template.role}`,
          teamAbbrev,
          teamId,
          matchupId,
          eligiblePositions: template.eligiblePositions,
          salary: template.salary + salaryOffset,
          gameInfo: `${gameKey} 7:00PM ET`,
          ourProj: Number((template.projection + teamOffset + gameBoost + projectionOffset).toFixed(2)),
          ourLeverage: Number((template.leverage + teamOffset + gameBoost * 0.2).toFixed(2)),
          linestarProj: Number((template.projection - 1.5 + teamOffset).toFixed(2)),
          projOwnPct: Number((6 + index * 0.9 + (isHome ? 0.4 : 0)).toFixed(2)),
          projCeiling: Number((template.projection * 1.2 + teamOffset + gameBoost + projectionOffset).toFixed(2)),
          boomRate: Number((0.1 + index * 0.012 + gameBoost * 0.01).toFixed(3)),
          propPts: Number((template.projection * 0.52 + teamOffset).toFixed(2)),
          isOut: false,
          homeTeamId,
          teamLogo: null,
          teamName: `${teamAbbrev} Test Team`,
        });
        id++;
        dkPlayerId++;
      });
    }
  }

  return pool;
}

function canFillSlot(slot: string, eligiblePositions: string): boolean {
  switch (slot) {
    case "PG": return eligiblePositions.includes("PG");
    case "SG": return eligiblePositions.includes("SG");
    case "SF": return eligiblePositions.includes("SF");
    case "PF": return eligiblePositions.includes("PF");
    case "C": return eligiblePositions.includes("C");
    case "G": return eligiblePositions.includes("G");
    case "F": return eligiblePositions.includes("F");
    case "UTIL": return true;
    default: return false;
  }
}

function assignSlots(players: OptimizerPlayer[]): boolean {
  const slots = ["PG", "SG", "SF", "PF", "C", "G", "F", "UTIL"] as const;
  const slotOptions = players
    .map((player) => ({
      playerId: player.id,
      slots: slots.filter((slot) => canFillSlot(slot, player.eligiblePositions)),
    }))
    .sort((a, b) => a.slots.length - b.slots.length || a.playerId - b.playerId);

  const used = new Set<string>();
  function visit(index: number): boolean {
    if (index >= slotOptions.length) return true;
    for (const slot of slotOptions[index].slots) {
      if (used.has(slot)) continue;
      used.add(slot);
      if (visit(index + 1)) return true;
      used.delete(slot);
    }
    return false;
  }

  return visit(0);
}

function getStackThreshold(teamId: number, settings: OptimizerSettings): number {
  const required = settings.requiredTeamStacks?.find((rule) => rule.teamId === teamId);
  return required?.stackSize ?? settings.minStack;
}

function buildOpponentMap(pool: OptimizerPlayer[]): Map<number, number> {
  const teamsByMatchup = new Map<number, number[]>();
  for (const player of pool) {
    if (player.matchupId == null || player.teamId == null) continue;
    const current = teamsByMatchup.get(player.matchupId) ?? [];
    if (!current.includes(player.teamId)) current.push(player.teamId);
    teamsByMatchup.set(player.matchupId, current);
  }

  const opponentByTeam = new Map<number, number>();
  for (const teams of teamsByMatchup.values()) {
    if (teams.length !== 2) continue;
    opponentByTeam.set(teams[0], teams[1]);
    opponentByTeam.set(teams[1], teams[0]);
  }
  return opponentByTeam;
}

function getPlayerGameKey(player: OptimizerPlayer): string | null {
  if (player.matchupId != null) return `matchup:${player.matchupId}`;
  const slateGameKey = player.gameInfo?.split(" ")[0]?.trim();
  return slateGameKey ? `game:${slateGameKey}` : null;
}

function buildGameKeyByTeam(pool: OptimizerPlayer[]): Map<number, string> {
  const gameKeyByTeam = new Map<number, string>();
  for (const player of pool) {
    if (player.teamId == null || gameKeyByTeam.has(player.teamId)) continue;
    const gameKey = getPlayerGameKey(player);
    if (gameKey) gameKeyByTeam.set(player.teamId, gameKey);
  }
  return gameKeyByTeam;
}

function countDistinctStackGames(
  teamIds: readonly number[],
  gameKeyByTeam: Map<number, string>,
): number {
  return new Set(teamIds.map((teamId) => gameKeyByTeam.get(teamId) ?? `team:${teamId}`)).size;
}

function countableTeamIds(
  lineup: GeneratedLineup,
  settings: OptimizerSettings,
  opponentByTeam: Map<number, number>,
): number[] {
  const teamCounts = new Map<number, number>();
  for (const player of lineup.players) {
    if (player.teamId == null) continue;
    teamCounts.set(player.teamId, (teamCounts.get(player.teamId) ?? 0) + 1);
  }

  const bringBackSize = settings.bringBackEnabled ? (settings.bringBackSize ?? 1) : 0;
  return Array.from(teamCounts.keys()).filter((teamId) => {
    if ((teamCounts.get(teamId) ?? 0) < getStackThreshold(teamId, settings)) return false;
    if (bringBackSize <= 0) return true;
    const opponentTeamId = opponentByTeam.get(teamId);
    if (opponentTeamId == null) return false;
    return (teamCounts.get(opponentTeamId) ?? 0) >= bringBackSize;
  });
}

export function assertValidLineups(
  lineups: GeneratedLineup[],
  pool: OptimizerPlayer[],
  settings: OptimizerSettings,
  options?: { salaryFloor?: number },
): void {
  const opponentByTeam = buildOpponentMap(pool);
  const gameKeyByTeam = buildGameKeyByTeam(pool);
  const exposureCap = Math.ceil(settings.nLineups * settings.maxExposure);
  const salaryFloor = options?.salaryFloor ?? 49_000;
  const exposureCounts = new Map<number, number>();

  lineups.forEach((lineup, index) => {
    const label = `lineup #${index + 1}`;
    if (lineup.players.length !== 8) {
      throw new Error(`${label}: expected 8 players, got ${lineup.players.length}`);
    }
    const uniqueCount = new Set(lineup.players.map((player) => player.id)).size;
    if (uniqueCount !== 8) {
      throw new Error(`${label}: duplicate players found`);
    }
    if (!assignSlots(lineup.players)) {
      throw new Error(`${label}: slot assignment failed`);
    }
    if (lineup.totalSalary > 50_000 || lineup.totalSalary < salaryFloor) {
      throw new Error(`${label}: salary ${lineup.totalSalary} outside expected bounds`);
    }

    const lockedPlayers = new Set(settings.playerLocks ?? []);
    const blockedPlayers = new Set(settings.playerBlocks ?? []);
    const blockedTeams = new Set(settings.blockedTeamIds ?? []);
    for (const player of lineup.players) {
      if (blockedPlayers.has(player.id)) {
        throw new Error(`${label}: blocked player ${player.name} was used`);
      }
      if (player.teamId != null && blockedTeams.has(player.teamId)) {
        throw new Error(`${label}: blocked team ${player.teamAbbrev} was used`);
      }
      exposureCounts.set(player.id, (exposureCounts.get(player.id) ?? 0) + 1);
    }

    for (const playerId of lockedPlayers) {
      if (!lineup.players.some((player) => player.id === playerId)) {
        throw new Error(`${label}: locked player ${playerId} missing`);
      }
    }

    const countableTeams = countableTeamIds(lineup, settings, opponentByTeam);
    const requiredStackCount = settings.teamStackCount ?? 1;
    if (countableTeams.length < requiredStackCount) {
      throw new Error(`${label}: expected ${requiredStackCount} stack teams, found ${countableTeams.length}`);
    }
    const distinctStackGames = countDistinctStackGames(countableTeams, gameKeyByTeam);
    if (distinctStackGames < requiredStackCount) {
      throw new Error(`${label}: expected stacks across ${requiredStackCount} games, found ${distinctStackGames}`);
    }

    const requiredStacks = settings.requiredTeamStacks ?? [];
    if (
      requiredStacks.length > 0
      && !requiredStacks.some((rule) => countableTeams.includes(rule.teamId))
    ) {
      throw new Error(`${label}: none of the required team stacks were satisfied`);
    }
  });

  for (const [playerId, count] of exposureCounts) {
    if ((settings.playerLocks ?? []).includes(playerId)) continue;
    if (count > exposureCap) {
      throw new Error(`player ${playerId} exceeded exposure cap ${exposureCap} with ${count} uses`);
    }
  }
}

export function formatBenchmark(debugDurations: number[]): {
  min: number;
  avg: number;
  max: number;
  p95: number;
} {
  const sorted = [...debugDurations].sort((a, b) => a - b);
  const sum = sorted.reduce((total, value) => total + value, 0);
  const p95Index = Math.min(sorted.length - 1, Math.ceil(sorted.length * 0.95) - 1);
  return {
    min: sorted[0] ?? 0,
    avg: sorted.length ? Math.round(sum / sorted.length) : 0,
    max: sorted[sorted.length - 1] ?? 0,
    p95: sorted[p95Index] ?? 0,
  };
}

export function findPlayerId(pool: OptimizerPlayer[], teamAbbrev: string, role: string): number {
  const player = pool.find((candidate) => candidate.teamAbbrev === teamAbbrev && candidate.name.includes(role));
  if (!player) {
    throw new Error(`Unable to find ${teamAbbrev} ${role}`);
  }
  return player.id;
}

export function buildRequiredTeamStack(
  teamId: number,
  stackSize: RequiredTeamStackRule["stackSize"],
): RequiredTeamStackRule {
  return { teamId, stackSize };
}
