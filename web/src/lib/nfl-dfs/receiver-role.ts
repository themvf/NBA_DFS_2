/** Shared, snapshot-local evidence for injury-driven receiver depth changes. */
export type ReceiverRolePlayer = {
  dkPlayerId: number;
  name: string;
  team: string;
  position: string;
  depthRole?: string | null;
  isOut: boolean;
  dkStatus?: string | null;
  availabilityState?: "confirmed" | "probable" | "unknown" | "stale";
  availability?: { status?: string | null; source?: string | null; capturedAt?: string | null } | null;
};

export type ReceiverAbsence = {
  playerId: number;
  name: string;
  rank: number;
  status: string;
  source: string;
  capturedAt: string | null;
};

export type ReceiverRoleChange = {
  listedRank: number;
  effectiveRank: number;
  absentAhead: ReceiverAbsence[];
};

export function receiverChartRank(player: Pick<ReceiverRolePlayer, "position" | "depthRole">): number | null {
  const match = player.position === "WR" ? player.depthRole?.match(/\bWR\s*(\d+)\b/i) : null;
  return match ? Number(match[1]) : null;
}

function verifiedReceiverAbsence(player: ReceiverRolePlayer, rank: number): ReceiverAbsence | null {
  if (!player.isOut) return null;
  const dkStatus = player.dkStatus?.trim().toUpperCase();
  if (dkStatus && ["OUT", "O", "IR", "PUP", "SUSP", "NA"].includes(dkStatus)) {
    return { playerId: player.dkPlayerId, name: player.name, rank, status: dkStatus,
      source: "DraftKings status", capturedAt: null };
  }
  const status = player.availability?.status?.trim().toUpperCase();
  if ((player.availabilityState === "confirmed" || player.availabilityState === "probable")
    && status && ["OUT", "IR", "PUP", "NFI", "SUSPENDED", "INACTIVE"].includes(status)) {
    return { playerId: player.dkPlayerId, name: player.name, rank, status,
      source: player.availability?.source ?? "Current roster/injury status",
      capturedAt: player.availability?.capturedAt ?? null };
  }
  return null;
}

/** One calculation feeds both the optimizer gate and the pre-build player pool. */
export function deriveReceiverRoleChanges<T extends ReceiverRolePlayer>(players: readonly T[]): Map<number, ReceiverRoleChange> {
  const absentByTeam = new Map<string, ReceiverAbsence[]>();
  for (const player of players) {
    const rank = receiverChartRank(player);
    if (rank === null) continue;
    const absent = verifiedReceiverAbsence(player, rank);
    if (absent) absentByTeam.set(player.team, [...(absentByTeam.get(player.team) ?? []), absent]);
  }
  const result = new Map<number, ReceiverRoleChange>();
  for (const player of players) {
    const listedRank = receiverChartRank(player);
    if (listedRank === null || player.isOut) continue;
    // A duplicate chart number is one vacancy, not two promotions.
    const ahead = [...new Map((absentByTeam.get(player.team) ?? [])
      .filter(absent => absent.rank < listedRank)
      .map(absent => [absent.rank, absent] as const)).values()];
    if (!ahead.length) continue;
    result.set(player.dkPlayerId, {
      listedRank, effectiveRank: Math.max(1, listedRank - ahead.length),
      absentAhead: ahead.sort((a, b) => a.rank - b.rank || a.name.localeCompare(b.name)),
    });
  }
  return result;
}
