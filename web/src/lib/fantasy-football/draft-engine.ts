export type DraftSlot = {
  overallPick: number;
  round: number;
  pickInRound: number;
  teamSlot: number;
};

export function buildSnakeSlots(teamCount: number, rounds: number): DraftSlot[] {
  if (!Number.isInteger(teamCount) || teamCount < 2 || teamCount > 20) {
    throw new Error("Team count must be between 2 and 20");
  }
  if (!Number.isInteger(rounds) || rounds < 1 || rounds > 30) {
    throw new Error("Rounds must be between 1 and 30");
  }
  const slots: DraftSlot[] = [];
  for (let round = 1; round <= rounds; round += 1) {
    for (let pickInRound = 1; pickInRound <= teamCount; pickInRound += 1) {
      const teamSlot = round % 2 === 1 ? pickInRound : teamCount - pickInRound + 1;
      slots.push({
        overallPick: (round - 1) * teamCount + pickInRound,
        round,
        pickInRound,
        teamSlot,
      });
    }
  }
  return slots;
}

export function nextControlledPick(
  currentPick: number,
  controlledSlot: number,
  teamCount: number,
  rounds: number,
): number | null {
  return buildSnakeSlots(teamCount, rounds).find(
    (slot) => slot.overallPick >= currentPick && slot.teamSlot === controlledSlot,
  )?.overallPick ?? null;
}

export function picksUntilControlled(
  currentPick: number,
  controlledSlot: number,
  teamCount: number,
  rounds: number,
): number | null {
  const next = nextControlledPick(currentPick, controlledSlot, teamCount, rounds);
  return next === null ? null : Math.max(0, next - currentPick);
}
