export type TennisEloGateLike = {
  tour: string;
  status: string;
};

const REQUIRED_TOURS = ["ATP", "WTA"] as const;

export function canPromoteTennisSurfaceElo(gates: TennisEloGateLike[]): boolean {
  return REQUIRED_TOURS.every((tour) =>
    gates.some((gate) => gate.tour === tour && gate.status === "PASS"),
  );
}

export function tennisSurfaceActionMessage(gates: TennisEloGateLike[]): string {
  if (canPromoteTennisSurfaceElo(gates)) {
    return "The surface adjustment passed chronological validation and can advance to the decision model.";
  }
  return "The 2025 validation did not prove that surface Elo improves overall Elo. Action: do not place a bet because of the surface rating difference alone. Use these ratings to understand the matchup while the decision model continues to rely on promoted signals.";
}
