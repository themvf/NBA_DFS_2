import type { NflAirDefenseEvidence, NflPlayerSignalEvidence } from "./player-signals";

export const NFL_AIR_MATCHUP_EVIDENCE_VERSION = "nfl-air-matchup-evidence-v1-shadow";
export type NflAirTeamEvidence = { games: number; targets: number; targetAirYards: number };
export type NflAirMarketEvidence = { gameId: string; opponent: string; kickoff: string | null;
  quoteId: number | null; capturedAt: string | null; spread: number | null; total: number | null; moneyline: number | null };
export type NflAirMatchupEvidence = {
  version: string; asOf: string; state: "ready" | "unavailable"; reason: string | null;
  player: { games: number; targets: number; targetAirYards: number; deepTargets: number;
    targetShare: number | null; airYardShare: number | null; airYardsPerTarget: number | null };
  defense: { team: string | null; games: number | null; targets: number | null; targetAirYards: number | null;
    airYardsPerTarget: number | null; regressedAirYardsPerTarget: number | null; leagueAirYardsPerTarget: number | null };
  projectedTeamPassAttempts: number | null;
  neutralTargetAirYards: number | null; matchupTargetAirYards: number | null; matchupFactor: number | null;
  market: NflAirMarketEvidence | null;
};

/** Descriptive shadow opportunity; the fixed shrinkage/cap is a sensitivity assumption, not a fitted forecast. */
export function buildNflAirMatchupEvidence(input: {
  asOf: string; opponent: string | null; player: NflPlayerSignalEvidence | null;
  team: NflAirTeamEvidence | null; defense: NflAirDefenseEvidence | null;
  allDefenses: ReadonlyMap<string, NflAirDefenseEvidence>; projectedTeamPassAttempts: number | null;
  market: NflAirMarketEvidence | null;
}): NflAirMatchupEvidence {
  const { player, team, defense, allDefenses } = input;
  const supported = [...allDefenses.values()].filter(row => row.games >= 2 && row.targets >= 40 && Number.isFinite(row.targetAirYards));
  const totalTargets = supported.reduce((sum, row) => sum + row.targets, 0);
  const leagueDepth = supported.length >= 16 && totalTargets > 0
    ? supported.reduce((sum, row) => sum + row.targetAirYards, 0) / totalTargets : null;
  const playerDepth = player && player.targets > 0 ? player.targetAirYards / player.targets : null;
  const targetShare = player && team && team.targets > 0 ? player.targets / team.targets : null;
  const airShare = player && team && team.targetAirYards > 0 ? player.targetAirYards / team.targetAirYards : null;
  const defenseDepth = defense && defense.targets > 0 ? defense.targetAirYards / defense.targets : null;
  const regressed = defense && leagueDepth != null ? (defense.targetAirYards + 60 * leagueDepth) / (defense.targets + 60) : null;
  const market = input.market?.opponent === input.opponent ? input.market : null;
  const attempts = input.projectedTeamPassAttempts;
  const reason = !player || player.games < 2 || player.targets < 4 ? "Player target sample is insufficient."
    : !team || team.games < 2 || !team.targets ? "Team target sample is insufficient."
    : targetShare == null || targetShare > 1 || targetShare < 0 ? "Player/team target denominator did not reconcile."
    : !defense || defense.games < 2 || defense.targets < 40 || leagueDepth == null || leagueDepth <= 0 ? "Opponent air-yard coverage is insufficient."
    : !Number.isFinite(attempts) || attempts == null || attempts <= 0 ? "Projected team pass attempts are unavailable."
    : playerDepth == null || !Number.isFinite(playerDepth) || playerDepth <= 0 ? "Player target depth is unavailable."
    : null;
  const factor = reason || regressed == null || leagueDepth == null ? null : Math.max(.8, Math.min(1.2, regressed / leagueDepth));
  const neutral = reason || targetShare == null || playerDepth == null || attempts == null ? null : attempts * targetShare * playerDepth;
  return { version: NFL_AIR_MATCHUP_EVIDENCE_VERSION, asOf: input.asOf,
    state: reason ? "unavailable" : "ready", reason,
    player: { games: player?.games ?? 0, targets: player?.targets ?? 0, targetAirYards: player?.targetAirYards ?? 0,
      deepTargets: player?.deepTargets ?? 0, targetShare, airYardShare: airShare, airYardsPerTarget: playerDepth },
    defense: { team: input.opponent, games: defense?.games ?? null, targets: defense?.targets ?? null,
      targetAirYards: defense?.targetAirYards ?? null, airYardsPerTarget: defenseDepth,
      regressedAirYardsPerTarget: regressed, leagueAirYardsPerTarget: leagueDepth },
    projectedTeamPassAttempts: attempts, neutralTargetAirYards: neutral,
    matchupTargetAirYards: neutral == null || factor == null ? null : neutral * factor,
    matchupFactor: factor, market };
}
