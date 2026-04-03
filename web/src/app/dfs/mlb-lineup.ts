export type MlbPendingLineupPolicy = "ignore" | "downgrade" | "exclude";
export type MlbLineupStatus = "pitcher" | "confirmed_in" | "confirmed_out" | "pending";

export const MLB_PENDING_LINEUP_DOWNGRADE_FACTOR = 0.85;

type MlbLineupSignalPlayer = {
  eligiblePositions: string;
  dkStartingLineupOrder?: number | null;
  dkInStartingLineup?: boolean | null;
  dkTeamLineupConfirmed?: boolean | null;
  isOut?: boolean | null;
  ourProj?: number | null;
  ourLeverage?: number | null;
  linestarProj?: number | null;
};

type MlbApiLineupSignalPlayer = {
  teamAbbrev: string;
  eligiblePositions: string;
  startingLineupOrder?: number | null;
  inStartingLineup?: boolean | null;
};

function scaleNullable(value: number | null | undefined, factor: number): number | null | undefined {
  if (value == null || !Number.isFinite(value)) return value;
  return Math.round(value * factor * 100) / 100;
}

export function isMlbPitcherEligiblePositions(pos: string): boolean {
  return pos.includes("SP") || pos.includes("RP");
}

export function isPositiveMlbLineupOrder(order: number | null | undefined): order is number {
  return order != null && Number.isInteger(order) && order >= 1 && order <= 9;
}

export function normalizeMlbPendingLineupPolicy(
  policy: MlbPendingLineupPolicy | null | undefined,
): MlbPendingLineupPolicy {
  return policy === "ignore" || policy === "exclude" || policy === "downgrade"
    ? policy
    : "downgrade";
}

export function inferMlbTeamLineupConfirmed(players: MlbApiLineupSignalPlayer[]): Map<string, boolean> {
  const grouped = new Map<string, MlbApiLineupSignalPlayer[]>();
  for (const player of players) {
    if (isMlbPitcherEligiblePositions(player.eligiblePositions)) continue;
    const key = (player.teamAbbrev ?? "").toUpperCase();
    if (!key) continue;
    const bucket = grouped.get(key) ?? [];
    bucket.push(player);
    grouped.set(key, bucket);
  }

  const confirmed = new Map<string, boolean>();
  for (const [teamAbbrev, hitters] of grouped) {
    const orderedHitters = hitters.filter((player) => isPositiveMlbLineupOrder(player.startingLineupOrder)).length;
    const explicitFlags = hitters.filter((player) => player.inStartingLineup != null).length;
    confirmed.set(teamAbbrev, orderedHitters > 0 || explicitFlags >= 5);
  }
  return confirmed;
}

export function getMlbLineupStatus(player: MlbLineupSignalPlayer): MlbLineupStatus {
  if (isMlbPitcherEligiblePositions(player.eligiblePositions)) return "pitcher";
  if (player.dkTeamLineupConfirmed !== true) return "pending";
  if (isPositiveMlbLineupOrder(player.dkStartingLineupOrder)) return "confirmed_in";
  if (player.dkInStartingLineup === true) return "confirmed_in";
  return "confirmed_out";
}

export function isMlbRowUnavailable(player: MlbLineupSignalPlayer): boolean {
  return !!player.isOut || getMlbLineupStatus(player) === "confirmed_out";
}

export function applyMlbPendingLineupPolicy<T extends MlbLineupSignalPlayer>(
  pool: T[],
  policy: MlbPendingLineupPolicy | null | undefined,
  factor: number = MLB_PENDING_LINEUP_DOWNGRADE_FACTOR,
): T[] {
  const normalizedPolicy = normalizeMlbPendingLineupPolicy(policy);
  return pool.flatMap((player) => {
    const status = getMlbLineupStatus(player);
    if (status === "confirmed_out") return [];
    if (status !== "pending" || normalizedPolicy === "ignore") return [player];
    if (normalizedPolicy === "exclude") return [];
    return [{
      ...player,
      ourProj: scaleNullable(player.ourProj, factor) ?? null,
      ourLeverage: scaleNullable(player.ourLeverage, factor) ?? null,
      linestarProj: scaleNullable(player.linestarProj, factor) ?? null,
    } as T];
  });
}
