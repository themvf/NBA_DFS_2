/** A saved forecast must be rerun when a currently ruled-out player still has a share. */
export type ConfirmedOutPlayer = {
  identity: string;
  name: string;
  team: string;
  observedAt: string;
  source: "Official" | "Sleeper and FantasyPros";
};

export type SavedLeaderFamily = {
  players: Array<{ identity: string; residual: boolean }>;
};

export function outPlayersInForecast(
  families: Record<string, SavedLeaderFamily>,
  confirmedOut: ConfirmedOutPlayer[],
): ConfirmedOutPlayer[] {
  const forecastIds = new Set(
    Object.values(families).flatMap((family) =>
      family.players.filter((player) => !player.residual).map((player) => player.identity),
    ),
  );
  return confirmedOut.filter((player) => forecastIds.has(player.identity));
}
