import { buildSnakeSlots } from "./draft-engine";
import type { RosterConfig } from "./league-config";

export const BEST_BALL_POSITIONS = ["QB", "RB", "WR", "TE"] as const;
export type BestBallPosition = typeof BEST_BALL_POSITIONS[number];
export const BEST_BALL_TEAM_COUNT = 12;
export const BEST_BALL_ROUNDS = 20;
export const BEST_BALL_ROSTER_SIZE = 20;

export const BEST_BALL_TARGETS: Record<BestBallPosition, number> = {
  QB: 3,
  RB: 6,
  WR: 8,
  TE: 3,
};

export const BEST_BALL_MINIMUMS: Record<BestBallPosition, number> = {
  QB: 1,
  RB: 2,
  WR: 3,
  TE: 1,
};

export type BestBallRosterPlayer = {
  playerId: number;
  position: string;
  team: string | null;
};

export type BestBallGate = {
  code: string;
  label: string;
  pass: boolean;
};

export type BestBallRosterStatus = {
  size: number;
  counts: Record<BestBallPosition, number>;
  nflTeams: number;
  valid: boolean;
  gates: BestBallGate[];
};

export type BestBallDraftState = {
  userSlot: number;
  playerIds: number[];
  /** CPU opponents draft the other 11 teams via `local-auto-draft.ts`. Off by default -- existing self-play behavior is unchanged unless a user opts in. */
  cpuEnabled: boolean;
};

/**
 * A synthetic "roster config" that lets the shared CPU engine
 * (`auto-draft.ts`'s `selectComputerPick`, built around slot-based leagues
 * with FLEX/K/DST) drive a Best Ball bot. Best Ball has no fixed lineup --
 * DraftKings' own auto-draft guardrails (`BEST_BALL_TARGETS`) are the
 * closest thing to "required slots," so they're treated as the direct
 * position requirements, with FLEX/K/DST zeroed out (no flex spot, and K/DST
 * aren't draftable in this format at all). Derived from `BEST_BALL_TARGETS`
 * so the two can't drift apart.
 */
export const BEST_BALL_AUTO_DRAFT_ROSTER_CONFIG: RosterConfig = {
  QB: BEST_BALL_TARGETS.QB,
  RB: BEST_BALL_TARGETS.RB,
  WR: BEST_BALL_TARGETS.WR,
  TE: BEST_BALL_TARGETS.TE,
  FLEX: 0,
  K: 0,
  DST: 0,
  BN: 0,
};

export type BestBallDraftBoardCell = {
  round: number;
  pickInRound: number;
  teamSlot: number;
  overallPick: number;
  playerId: number | null;
};

export function buildBestBallDraftBoard(playerIds: number[]): BestBallDraftBoardCell[][] {
  const slots = buildSnakeSlots(BEST_BALL_TEAM_COUNT, BEST_BALL_ROUNDS);
  const byRoundAndTeam = new Map(
    slots.map((slot) => [`${slot.round}:${slot.teamSlot}`, slot]),
  );
  return Array.from({ length: BEST_BALL_ROUNDS }, (_, roundIndex) => {
    const round = roundIndex + 1;
    return Array.from({ length: BEST_BALL_TEAM_COUNT }, (_, teamIndex) => {
      const teamSlot = teamIndex + 1;
      const slot = byRoundAndTeam.get(`${round}:${teamSlot}`);
      if (!slot) throw new Error(`Missing draft slot for round ${round}, team ${teamSlot}`);
      return {
        round: slot.round,
        pickInRound: slot.pickInRound,
        teamSlot: slot.teamSlot,
        overallPick: slot.overallPick,
        playerId: playerIds[slot.overallPick - 1] ?? null,
      };
    });
  });
}

export function parseBestBallDraftState(value: string): BestBallDraftState {
  try {
    const parsed = JSON.parse(value) as Partial<BestBallDraftState>;
    const userSlot = Number.isInteger(parsed.userSlot) && Number(parsed.userSlot) >= 1 && Number(parsed.userSlot) <= BEST_BALL_TEAM_COUNT
      ? Number(parsed.userSlot)
      : 1;
    const playerIds = Array.isArray(parsed.playerIds)
      ? parsed.playerIds.filter((id): id is number => Number.isInteger(id) && id > 0).slice(0, BEST_BALL_TEAM_COUNT * BEST_BALL_ROUNDS)
      : [];
    const cpuEnabled = typeof parsed.cpuEnabled === "boolean" ? parsed.cpuEnabled : false;
    return { userSlot, playerIds: [...new Set(playerIds)], cpuEnabled };
  } catch {
    return { userSlot: 1, playerIds: [], cpuEnabled: false };
  }
}

export function getBestBallRosterStatus(players: BestBallRosterPlayer[]): BestBallRosterStatus {
  const counts: Record<BestBallPosition, number> = { QB: 0, RB: 0, WR: 0, TE: 0 };
  let eligible = true;
  for (const player of players) {
    if (!BEST_BALL_POSITIONS.includes(player.position as BestBallPosition)) {
      eligible = false;
      continue;
    }
    counts[player.position as BestBallPosition] += 1;
  }
  const nflTeams = new Set(players.flatMap((player) => player.team ? [player.team] : [])).size;
  const gates: BestBallGate[] = [
    { code: "ROSTER_SIZE", label: `${players.length}/20 players`, pass: players.length === 20 },
    { code: "ELIGIBLE", label: "Only QB/RB/WR/TE", pass: eligible },
    ...BEST_BALL_POSITIONS.map((position) => ({
      code: `MIN_${position}`,
      label: `${position} minimum ${BEST_BALL_MINIMUMS[position]}`,
      pass: counts[position] >= BEST_BALL_MINIMUMS[position],
    })),
    { code: "MAX_QB", label: "QB maximum 5", pass: counts.QB <= 5 },
    { code: "MAX_TE", label: "TE maximum 5", pass: counts.TE <= 5 },
    { code: "NFL_TEAMS", label: "At least 2 NFL teams", pass: nflTeams >= 2 },
  ];
  return { size: players.length, counts, nflTeams, valid: gates.every((gate) => gate.pass), gates };
}

export type BestBallCompletionStatus = {
  remainingSlots: number;
  positionalMinimumsMissing: number;
  teamDiversityMissing: number;
  minimumPicksRequired: number;
  completable: boolean;
};

export function getBestBallCompletionStatus(players: BestBallRosterPlayer[]): BestBallCompletionStatus {
  const status = getBestBallRosterStatus(players);
  const remainingSlots = Math.max(0, BEST_BALL_ROSTER_SIZE - status.size);
  const positionalMinimumsMissing = BEST_BALL_POSITIONS.reduce(
    (sum, position) => sum + Math.max(0, BEST_BALL_MINIMUMS[position] - status.counts[position]),
    0,
  );
  const teamDiversityMissing = Math.max(0, 2 - status.nflTeams);
  const minimumPicksRequired = Math.max(positionalMinimumsMissing, teamDiversityMissing);
  const immediatelyLegal = status.size <= BEST_BALL_ROSTER_SIZE
    && new Set(players.map((player) => player.playerId)).size === players.length
    && players.every((player) => BEST_BALL_POSITIONS.includes(player.position as BestBallPosition))
    && status.counts.QB <= 5
    && status.counts.TE <= 5;
  return {
    remainingSlots,
    positionalMinimumsMissing,
    teamDiversityMissing,
    minimumPicksRequired,
    completable: immediatelyLegal && minimumPicksRequired <= remainingSlots,
  };
}

export function canAddBestBallPlayer(players: BestBallRosterPlayer[], candidate: BestBallRosterPlayer): boolean {
  if (players.length >= BEST_BALL_ROSTER_SIZE || players.some((player) => player.playerId === candidate.playerId)) return false;
  if (!BEST_BALL_POSITIONS.includes(candidate.position as BestBallPosition)) return false;
  const status = getBestBallRosterStatus(players);
  if (candidate.position === "QB" && status.counts.QB >= 5) return false;
  if (candidate.position === "TE" && status.counts.TE >= 5) return false;
  return true;
}

export function canAddCompletableBestBallPlayer(players: BestBallRosterPlayer[], candidate: BestBallRosterPlayer): boolean {
  return canAddBestBallPlayer(players, candidate)
    && getBestBallCompletionStatus([...players, candidate]).completable;
}
