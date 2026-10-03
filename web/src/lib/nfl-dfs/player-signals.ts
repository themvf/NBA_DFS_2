/** Observable pregame opportunity flags. These are not fantasy-point or ownership forecasts. */
export const NFL_PLAYER_SIGNAL_VERSION = "nfl-dfs-player-signals-v1";

export type NflPlayerSignalCode = "AIR_VOLUME" | "YAC_RUNWAY" | "INSIDE_FIVE" | "CLOSE_TARGET";
export type NflPlayerSignal = {
  code: NflPlayerSignalCode;
  label: string;
  detail: string;
  evidence: Record<string, number>;
};

export type NflPlayerSignalEvidence = {
  games: number;
  targets: number;
  catches: number;
  targetAirYards: number;
  caughtAirYards: number;
  deepTargets: number;
  yardsAfterCatch: number;
  expectedYac: number;
  carries: number;
  carriesInsideFive: number;
  targetsInsideTen: number;
};

/** Fixed descriptive thresholds. Revisit them with held-out slate outcomes before changing optimizer defaults. */
export function classifyNflPlayerSignals(
  position: string,
  evidence: NflPlayerSignalEvidence | null,
): NflPlayerSignal[] {
  if (!evidence || evidence.games < 2) return [];
  const flags: NflPlayerSignal[] = [];
  if (["WR", "TE"].includes(position) && evidence.targets >= 12
      && evidence.targetAirYards >= 200 && evidence.deepTargets >= 4) {
    flags.push({ code: "AIR_VOLUME", label: "Air-yard volume",
      detail: `${evidence.targetAirYards.toFixed(0)} target air yards on ${evidence.targets} targets; ${evidence.deepTargets} deep targets in ${evidence.games} games.`,
      evidence: { games: evidence.games, targets: evidence.targets,
        targetAirYards: evidence.targetAirYards, caughtAirYards: evidence.caughtAirYards,
        deepTargets: evidence.deepTargets } });
  }
  if (["RB", "WR", "TE"].includes(position) && evidence.targets >= 12
      && evidence.catches >= 8 && evidence.expectedYac >= 50) {
    flags.push({ code: "YAC_RUNWAY", label: "YAC opportunity",
      detail: `${evidence.expectedYac.toFixed(0)} expected yards after catch across ${evidence.catches} catches; ${evidence.yardsAfterCatch.toFixed(0)} actual YAC.`,
      evidence: { games: evidence.games, targets: evidence.targets, catches: evidence.catches,
        expectedYac: evidence.expectedYac, yardsAfterCatch: evidence.yardsAfterCatch } });
  }
  if (position === "RB" && evidence.carries >= 8 && evidence.carriesInsideFive >= 3) {
    flags.push({ code: "INSIDE_FIVE", label: "Inside-5 work",
      detail: `${evidence.carriesInsideFive} carries inside the opponent's 5 on ${evidence.carries} rushes in ${evidence.games} games.`,
      evidence: { games: evidence.games, carries: evidence.carries,
        carriesInsideFive: evidence.carriesInsideFive } });
  }
  if (["RB", "WR", "TE"].includes(position) && evidence.targetsInsideTen >= 2) {
    flags.push({ code: "CLOSE_TARGET", label: "Close-range targets",
      detail: `${evidence.targetsInsideTen} targets inside the opponent's 10 in ${evidence.games} games.`,
      evidence: { games: evidence.games, targetsInsideTen: evidence.targetsInsideTen } });
  }
  return flags;
}

export function isNflGppSignalPlayer(
  signals: readonly NflPlayerSignal[] | null | undefined,
  selected: readonly NflPlayerSignalCode[] = ["AIR_VOLUME", "YAC_RUNWAY", "INSIDE_FIVE", "CLOSE_TARGET"],
): boolean {
  return Boolean(signals?.some(signal => selected.includes(signal.code)));
}
