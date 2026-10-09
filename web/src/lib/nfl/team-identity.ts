import type { NflIdentityGame } from "@/db/nfl-team-identity";

export type NflIdentityMetrics = {
  games: number;
  offensePlays: number;
  defensePlays: number;
  drives: number;
  neutralEarlyPlays: number;
  dropbackRate: number | null;
  neutralDropbackRate: number | null;
  passOe: number | null;
  airYards: number | null;
  offenseEpa: number | null;
  offenseSuccessRate: number | null;
  offenseExplosiveRate: number | null;
  defenseEpaAllowed: number | null;
  defenseSuccessAllowed: number | null;
  defenseExplosiveRate: number | null;
  touchdownDriveRate: number | null;
  threeAndOutRate: number | null;
  fieldGoalDriveRate: number | null;
  turnoverDriveRate: number | null;
  formationCoverage: number | null;
  personnelCoverage: number | null;
  pressureCoverage: number | null;
  marketGames: number;
  impliedPoints: number | null;
};

const rate = (numerator: number, denominator: number): number | null => denominator > 0 ? numerator / denominator : null;

export function aggregateNflIdentityGames(games: NflIdentityGame[]): NflIdentityMetrics {
  const sum = (key: keyof NflIdentityGame): number => games.reduce((total, game) => total + Number(game[key] ?? 0), 0);
  const offensePlays = sum("offensePlays");
  const defensePlays = sum("defensePlays");
  const drives = sum("drives");
  const neutralEarlyPlays = sum("neutralEarlyPlays");
  const marketGames = games.filter(game => game.market?.impliedPoints != null).length;
  return {
    games: games.length, offensePlays, defensePlays, drives, neutralEarlyPlays,
    dropbackRate: rate(sum("dropbacks"), offensePlays),
    neutralDropbackRate: rate(sum("neutralEarlyDropbacks"), neutralEarlyPlays),
    passOe: rate(sum("passOeSum"), sum("passOeCount")),
    airYards: rate(sum("airYardsSum"), sum("airYardsCount")),
    offenseEpa: rate(sum("offenseEpaSum"), sum("offenseEpaCount")),
    offenseSuccessRate: rate(sum("offenseSuccesses"), offensePlays),
    offenseExplosiveRate: rate(sum("offenseExplosives"), offensePlays),
    defenseEpaAllowed: rate(sum("defenseEpaSum"), sum("defenseEpaCount")),
    defenseSuccessAllowed: rate(sum("defenseSuccessesAllowed"), defensePlays),
    defenseExplosiveRate: rate(sum("defenseExplosivesAllowed"), defensePlays),
    touchdownDriveRate: rate(sum("touchdownDrives"), drives),
    threeAndOutRate: rate(sum("threeAndOutDrives"), drives),
    fieldGoalDriveRate: rate(sum("fieldGoalDrives"), drives),
    turnoverDriveRate: rate(sum("turnoverDrives"), drives),
    formationCoverage: rate(sum("formationRows"), offensePlays),
    personnelCoverage: rate(sum("personnelRows"), offensePlays),
    pressureCoverage: rate(sum("pressureRows"), offensePlays),
    marketGames,
    impliedPoints: rate(games.reduce((total, game) => total + (game.market?.impliedPoints ?? 0), 0), marketGames),
  };
}

export type NflIdentityWeek = {
  week: number;
  current: NflIdentityMetrics;
  prior: NflIdentityMetrics;
  previous: NflIdentityMetrics | null;
  game: NflIdentityGame;
};

export function buildNflIdentityWeeks(current: NflIdentityGame[], prior: NflIdentityGame[]): NflIdentityWeek[] {
  const ordered = [...current].sort((a, b) => a.week - b.week || a.gameId.localeCompare(b.gameId));
  return ordered.map((game, index) => ({
    week: game.week,
    game,
    current: aggregateNflIdentityGames(ordered.filter(row => row.week <= game.week)),
    prior: aggregateNflIdentityGames(prior.filter(row => row.week <= game.week)),
    previous: index ? aggregateNflIdentityGames(ordered.filter(row => row.week < game.week)) : null,
  }));
}

export function identitySummary(team: string, week: NflIdentityWeek, priorSeason: number): string {
  const { current, prior } = week;
  const usage = current.neutralDropbackRate;
  const usageDelta = usage != null && prior.neutralDropbackRate != null
    ? (usage - prior.neutralDropbackRate) * 100 : null;
  const usageText = usage == null ? "Neutral early-down usage is unavailable"
    : `${team} dropped back on ${(usage * 100).toFixed(1)}% of neutral early downs`;
  const comparison = usageDelta == null ? ""
    : `, ${Math.abs(usageDelta).toFixed(1)} percentage points ${usageDelta >= 0 ? "above" : "below"} the same ${priorSeason} weeks`;
  const epa = current.offenseEpa == null ? " Offensive EPA is unavailable."
    : ` Offensive EPA was ${signed(current.offenseEpa, 3)} per play${prior.offenseEpa == null ? "." : ` versus ${signed(prior.offenseEpa, 3)} in those ${priorSeason} weeks.`}`;
  return `${usageText}${comparison}.${epa} This describes ${current.games} completed ${current.games === 1 ? "game" : "games"}; it does not isolate a coaching effect.`;
}

export function signed(value: number, digits: number): string {
  return `${value > 0 ? "+" : ""}${value.toFixed(digits)}`;
}

export type NflMarketSettlement = {
  winner: "Won" | "Lost" | "Tied" | null;
  margin: number | null;
  spreadResult: "Covered" | "Missed" | "Push" | null;
  spreadEdge: number | null;
  totalPoints: number | null;
  totalResult: "Over" | "Under" | "Push" | null;
  totalEdge: number | null;
  impliedPointsEdge: number | null;
};

/** Grades the selected pregame team line against the final score. No odds are treated as a wager. */
export function settleNflIdentityMarket(game: Pick<NflIdentityGame, "teamScore" | "opponentScore" | "market">): NflMarketSettlement {
  const { teamScore, opponentScore, market } = game;
  if (teamScore == null || opponentScore == null) {
    return { winner: null, margin: null, spreadResult: null, spreadEdge: null,
      totalPoints: null, totalResult: null, totalEdge: null, impliedPointsEdge: null };
  }
  const margin = teamScore - opponentScore;
  const spreadEdge = market?.spread == null ? null : margin + market.spread;
  const totalPoints = teamScore + opponentScore;
  const totalEdge = market?.total == null ? null : totalPoints - market.total;
  const result = (edge: number | null, positive: string, negative: string) =>
    edge == null ? null : Math.abs(edge) < 1e-9 ? "Push" : edge > 0 ? positive : negative;
  return {
    winner: margin > 0 ? "Won" : margin < 0 ? "Lost" : "Tied",
    margin,
    spreadResult: result(spreadEdge, "Covered", "Missed") as NflMarketSettlement["spreadResult"],
    spreadEdge,
    totalPoints,
    totalResult: result(totalEdge, "Over", "Under") as NflMarketSettlement["totalResult"],
    totalEdge,
    impliedPointsEdge: market?.impliedPoints == null ? null : teamScore - market.impliedPoints,
  };
}
