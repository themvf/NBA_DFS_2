export const BEST_BALL_POSITIONS = ["QB", "RB", "WR", "TE"] as const;
export type BestBallPosition = typeof BEST_BALL_POSITIONS[number];

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

export function canAddBestBallPlayer(players: BestBallRosterPlayer[], candidate: BestBallRosterPlayer): boolean {
  if (players.length >= 20 || players.some((player) => player.playerId === candidate.playerId)) return false;
  if (!BEST_BALL_POSITIONS.includes(candidate.position as BestBallPosition)) return false;
  const status = getBestBallRosterStatus(players);
  if (candidate.position === "QB" && status.counts.QB >= 5) return false;
  if (candidate.position === "TE" && status.counts.TE >= 5) return false;
  return true;
}
