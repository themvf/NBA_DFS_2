export function formatPriorSeasonFantasyPoints(
  points: number | null,
  positionFinish: number | null,
  positionFinishTieCount: number | null,
  digits = 1,
): string {
  if (points === null) return "—";
  const pointsLabel = points.toFixed(digits);
  if (positionFinish === null) return pointsLabel;
  const tieLabel = (positionFinishTieCount ?? 0) > 1 ? "T" : "";
  return `${pointsLabel} (${tieLabel}${positionFinish})`;
}
