export const MLB_BLOWUP_CANDIDATE_VERSION = "mlb_blowup_v1";
export const MLB_BLOWUP_DEFAULT_LIMIT = 12;
const MLB_BASELINE_TEAM_TOTAL = 4.5;

export type MlbBlowupPlayerLike = {
  eligiblePositions?: string | null;
  salary: number;
  isOut?: boolean | null;
  liveProj?: number | null;
  blendProj?: number | null;
  ourProj?: number | null;
  projCeiling?: number | null;
  teamTotal?: number | null;
  teamId?: number | null;
  isHome?: boolean | null;
  homeTeamId?: number | null;
  homeImplied?: number | null;
  awayImplied?: number | null;
  vegasTotal?: number | null;
  homeMl?: number | null;
  awayMl?: number | null;
  name?: string | null;
};

export type MlbBlowupCandidate<T extends MlbBlowupPlayerLike> = {
  player: T;
  blowupScore: number;
  teamTotal: number;
  proj: number;
  ceiling: number;
  value: number;
};

function mlToProb(ml: number): number {
  return ml >= 0 ? 100 / (ml + 100) : Math.abs(ml) / (Math.abs(ml) + 100);
}

export function computeMlbTeamImpliedTotal(
  vegasTotal: number,
  homeMl: number | null | undefined,
  awayMl: number | null | undefined,
  isHome: boolean,
): number {
  if (homeMl == null || awayMl == null) return vegasTotal / 2;
  const rawHome = mlToProb(homeMl);
  const rawAway = mlToProb(awayMl);
  const vig = rawHome + rawAway;
  if (!Number.isFinite(vig) || vig <= 0) return vegasTotal / 2;
  const homeProbClean = rawHome / vig;
  const impliedSpread = Math.max(-15, Math.min(15, (homeProbClean - 0.5) / 0.025));
  const homeImplied = vegasTotal / 2 + impliedSpread / 2;
  return isHome ? homeImplied : vegasTotal - homeImplied;
}

export function isMlbPitcherEligiblePositions(eligiblePositions: string | null | undefined): boolean {
  const positions = eligiblePositions ?? "";
  return positions.includes("SP") || positions.includes("RP");
}

function resolveTeamTotal<T extends MlbBlowupPlayerLike>(player: T): number | null {
  if (player.teamTotal != null && Number.isFinite(player.teamTotal)) return player.teamTotal;

  const inferredIsHome = player.isHome ?? (
    player.teamId != null && player.homeTeamId != null
      ? player.teamId === player.homeTeamId
      : null
  );

  if (inferredIsHome == null) return null;
  const explicitTeamTotal = inferredIsHome ? player.homeImplied : player.awayImplied;
  if (explicitTeamTotal != null && Number.isFinite(explicitTeamTotal)) return explicitTeamTotal;

  if (player.vegasTotal == null || !Number.isFinite(player.vegasTotal)) return null;
  return computeMlbTeamImpliedTotal(player.vegasTotal, player.homeMl, player.awayMl, inferredIsHome);
}

function resolveProjection<T extends MlbBlowupPlayerLike>(player: T): number | null {
  const proj = player.liveProj ?? player.blendProj ?? player.ourProj ?? null;
  return proj != null && Number.isFinite(proj) ? Math.max(0, proj) : null;
}

function resolveCeiling<T extends MlbBlowupPlayerLike>(player: T, proj: number | null): number | null {
  const ceiling = player.projCeiling ?? (proj != null ? proj * 1.25 : null);
  return ceiling != null && Number.isFinite(ceiling) ? Math.max(0, ceiling) : null;
}

export function buildMlbBlowupCandidates<T extends MlbBlowupPlayerLike>(
  players: T[],
  limit = MLB_BLOWUP_DEFAULT_LIMIT,
): Array<MlbBlowupCandidate<T>> {
  return players
    .filter((player) => !player.isOut && !isMlbPitcherEligiblePositions(player.eligiblePositions) && player.salary > 0)
    .map((player) => {
      const teamTotal = resolveTeamTotal(player);
      const proj = resolveProjection(player);
      const ceiling = resolveCeiling(player, proj);
      const value = proj != null ? proj / (player.salary / 1000) : null;
      if (
        teamTotal == null || !Number.isFinite(teamTotal)
        || proj == null || !Number.isFinite(proj)
        || ceiling == null || !Number.isFinite(ceiling)
        || value == null || !Number.isFinite(value)
      ) {
        return null;
      }
      return {
        player,
        teamTotal,
        proj,
        ceiling,
        value,
        blowupScore: (teamTotal / MLB_BASELINE_TEAM_TOTAL) * ceiling * value / 10,
      };
    })
    .filter((candidate): candidate is MlbBlowupCandidate<T> => candidate !== null)
    .sort((a, b) =>
      b.blowupScore - a.blowupScore
      || b.teamTotal - a.teamTotal
      || ((a.player.name ?? "").localeCompare(b.player.name ?? ""))
    )
    .slice(0, Math.max(0, limit));
}
