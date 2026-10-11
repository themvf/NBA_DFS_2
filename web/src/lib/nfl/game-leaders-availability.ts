export type RuledOutSource = "Official" | "Sleeper reserve list" | "Sleeper and FantasyPros";

const RESERVE = new Set(["IR", "PUP", "NFI", "SUSPENDED"]);
const RULED_OUT = new Set(["OUT", ...RESERVE]);

/**
 * Which evidence rules a player out this week, or null. Mirrors
 * availability_status in research/nfl_game_leaders.py: an official OUT/INACTIVE
 * settles it; a Sleeper reserve-list designation (IR, PUP, NFI, suspended)
 * settles it on its own; a plain weekly OUT needs FantasyPros to agree.
 */
export function classifyRuledOut(sleeper: unknown, fantasypros: unknown, official: unknown): RuledOutSource | null {
  const up = (v: unknown) => String(v ?? "").toUpperCase();
  if (up(official) === "OUT" || up(official) === "INACTIVE") return "Official";
  if (RESERVE.has(up(sleeper))) return "Sleeper reserve list";
  if (RULED_OUT.has(up(sleeper)) && RULED_OUT.has(up(fantasypros))) return "Sleeper and FantasyPros";
  return null;
}

/** A saved forecast must be rerun when a currently ruled-out player still has a share. */
export type ConfirmedOutPlayer = {
  identity: string;
  name: string;
  team: string;
  observedAt: string;
  source: RuledOutSource;
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
