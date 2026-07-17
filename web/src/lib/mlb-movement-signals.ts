export const MLB_MEANINGFUL_MOVE_PP = 0.5;
export const MLB_MODEL_NEUTRAL_GAP_PP = 0.5;
export const MLB_MAX_DISPLAYABLE_MODEL_GAP_PP = 15;

export type MlbMovementAgreement = "agree" | "neutral" | "disagree" | "unavailable";

export type MlbMovementSignal = {
  movementSide: "home" | "away" | null;
  movementTeam: string | null;
  openProbability: number | null;
  currentProbability: number | null;
  movementPp: number;
  modelProbability: number | null;
  modelGapPp: number | null;
  agreement: MlbMovementAgreement;
};

type MovementSignalInput = {
  openHomeProbability: number;
  currentHomeProbability: number;
  modelHomeProbability: number | null;
  homeTeam: string;
  awayTeam: string;
};

function validProbability(value: number | null): value is number {
  // MLB pregame moneylines almost never imply a true 98%+ favorite. The
  // current point-in-time scaling defect can emit 0.1%/99.9% predictions;
  // suppress those rather than presenting a fabricated 50pp model edge.
  return value != null && Number.isFinite(value) && value > 0.02 && value < 0.98;
}

/**
 * Describe the team receiving open-to-current movement, then compare the
 * frozen market-anchored model probability with the current vig-free market
 * probability for that same team. Percentage-point gaps avoid the ambiguity
 * of relative percent changes in American odds.
 */
export function buildMlbMovementSignal(input: MovementSignalInput): MlbMovementSignal {
  const homeMovePp = (input.currentHomeProbability - input.openHomeProbability) * 100;
  if (Math.abs(homeMovePp) < MLB_MEANINGFUL_MOVE_PP) {
    return {
      movementSide: null,
      movementTeam: null,
      openProbability: null,
      currentProbability: null,
      movementPp: Math.abs(homeMovePp),
      modelProbability: null,
      modelGapPp: null,
      agreement: "unavailable",
    };
  }

  const movementSide = homeMovePp > 0 ? "home" : "away";
  const openProbability = movementSide === "home"
    ? input.openHomeProbability
    : 1 - input.openHomeProbability;
  const currentProbability = movementSide === "home"
    ? input.currentHomeProbability
    : 1 - input.currentHomeProbability;
  const modelProbability = validProbability(input.modelHomeProbability)
    ? movementSide === "home" ? input.modelHomeProbability : 1 - input.modelHomeProbability
    : null;
  const uncheckedModelGapPp = modelProbability == null ? null : (modelProbability - currentProbability) * 100;
  const modelGapPp = uncheckedModelGapPp != null
    && Math.abs(uncheckedModelGapPp) <= MLB_MAX_DISPLAYABLE_MODEL_GAP_PP
    ? uncheckedModelGapPp
    : null;
  const agreement: MlbMovementAgreement = modelGapPp == null
    ? "unavailable"
    : modelGapPp > MLB_MODEL_NEUTRAL_GAP_PP
      ? "agree"
      : modelGapPp < -MLB_MODEL_NEUTRAL_GAP_PP
        ? "disagree"
        : "neutral";

  return {
    movementSide,
    movementTeam: movementSide === "home" ? input.homeTeam : input.awayTeam,
    openProbability,
    currentProbability,
    movementPp: Math.abs(homeMovePp),
    modelProbability: modelGapPp == null ? null : modelProbability,
    modelGapPp,
    agreement,
  };
}
