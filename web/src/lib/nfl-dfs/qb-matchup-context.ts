/** Pregame QB matchup evidence. Descriptive only: it never changes fantasy points. */
export const QB_MATCHUP_CONTEXT_VERSION = "nfl-qb-matchup-context-v1";

export type QbContextPlay = {
  gameId: string;
  posteam: string;
  defteam: string;
  playType: string | null;
  qbDropback: boolean;
  scoreDifferential: number | null;
  epa: number | null;
  drive: number | null;
  driveArchetype: string | null;
};

export type QbContextMarket = {
  gameId: string;
  home: string;
  away: string;
  kickoff: string;
  oddsId: number | null;
  oddsCapturedAt: string | null;
  bookmakerCount: number | null;
  homeSpread: number | null;
  total: number | null;
};

export type QbMatchupContext = {
  version: typeof QB_MATCHUP_CONTEXT_VERSION;
  status: "ready" | "unavailable";
  reason: string | null;
  asOf: string;
  gameId: string;
  team: string;
  opponent: string;
  kickoff: string;
  oddsId: number | null;
  oddsCapturedAt: string | null;
  bookmakerCount: number | null;
  teamSpread: number | null;
  gameTotal: number | null;
  impliedPoints: number | null;
  closeDropbackRate: number | null;
  closePlays: number;
  trailingDropbackRate: number | null;
  trailingPlays: number;
  leadingDropbackRate: number | null;
  leadingPlays: number;
  opponentAdjustedEpa: number | null;
  opponentDropbacks: number;
  opponentGames: number;
  opponentTouchdownDrives: number;
  opponentCompetitiveDrives: number;
  sourceGameIds: string[];
};

type GamePass = { gameId: string; offense: string; defense: string; sum: number; n: number };

const valid = (value: number | null): value is number => value !== null && Number.isFinite(value);

/** Positive means opponents passed more efficiently against this defense than in their other games. */
export function opponentAdjustedPassEffect(plays: readonly QbContextPlay[], defense: string): {
  effect: number | null; dropbacks: number; games: number;
} {
  const byGame = new Map<string, GamePass>();
  for (const play of plays) {
    if (!play.qbDropback || !valid(play.epa)) continue;
    const key = `${play.gameId}:${play.posteam}`;
    const row = byGame.get(key) ?? { gameId: play.gameId, offense: play.posteam, defense: play.defteam, sum: 0, n: 0 };
    row.sum += play.epa;
    row.n++;
    byGame.set(key, row);
  }
  const games = [...byGame.values()];
  const target = games.filter(row => row.defense === defense);
  let weightedDifference = 0, dropbacks = 0, comparableGames = 0;
  for (const game of target) {
    const other = games.filter(row => row.offense === game.offense && row.gameId !== game.gameId);
    const otherN = other.reduce((sum, row) => sum + row.n, 0);
    if (!otherN) continue;
    const otherMean = other.reduce((sum, row) => sum + row.sum, 0) / otherN;
    weightedDifference += game.n * (game.sum / game.n - otherMean);
    dropbacks += game.n;
    comparableGames++;
  }
  return { effect: comparableGames >= 2 && dropbacks >= 60 ? weightedDifference / dropbacks : null,
    dropbacks, games: comparableGames };
}

function stateRate(plays: readonly QbContextPlay[], state: "close" | "trailing" | "leading") {
  const selected = plays.filter(play => (play.playType === "run" || play.playType === "pass")
    && valid(play.scoreDifferential) && (state === "close" ? Math.abs(play.scoreDifferential) <= 7
      : state === "trailing" ? play.scoreDifferential < -7 : play.scoreDifferential > 7));
  return { plays: selected.length, rate: selected.length >= 20
    ? selected.filter(play => play.qbDropback).length / selected.length : null };
}

function competitiveDrives(plays: readonly QbContextPlay[], defense: string) {
  const drives = new Map<string, string>();
  for (const play of plays) {
    if (play.defteam !== defense || play.drive === null || !play.driveArchetype) continue;
    drives.set(`${play.gameId}:${play.posteam}:${play.drive}`, play.driveArchetype);
  }
  const eligible = [...drives.values()].filter(label => label !== "CLOCK_EXPIRED" && label !== "KNEEL_DOWN");
  return { competitive: eligible.length, touchdowns: eligible.filter(label => label === "TOUCHDOWN").length };
}

export function buildQbMatchupContexts(plays: readonly QbContextPlay[], markets: readonly QbContextMarket[], asOf: string): QbMatchupContext[] {
  return markets.flatMap(market => [market.home, market.away].map((team): QbMatchupContext => {
    const opponent = team === market.home ? market.away : market.home;
    const own = plays.filter(play => play.posteam === team);
    const close = stateRate(own, "close"), trailing = stateRate(own, "trailing"), leading = stateRate(own, "leading");
    const defense = opponentAdjustedPassEffect(plays, opponent);
    const drives = competitiveDrives(plays, opponent);
    const opponentOffenses = new Set(plays.filter(play => play.defteam === opponent).map(play => play.posteam));
    const teamSpread = valid(market.homeSpread) ? (team === market.home ? market.homeSpread : -market.homeSpread) : null;
    const impliedPoints = valid(market.total) && valid(teamSpread) ? (market.total - teamSpread) / 2 : null;
    const reasons = [
      !market.oddsId || !market.oddsCapturedAt || !valid(market.total) || !valid(market.homeSpread) ? "Pregame odds unavailable." : null,
      close.plays < 40 ? "Fewer than 40 close-state plays." : null,
      defense.effect === null ? "Opponent-adjusted defense lacks comparable games." : null,
      drives.competitive < 15 ? "Opponent drive sample is incomplete." : null,
    ].filter((reason): reason is string => Boolean(reason));
    return {
      version: QB_MATCHUP_CONTEXT_VERSION, status: reasons.length ? "unavailable" : "ready",
      reason: reasons.length ? reasons.join(" ") : null, asOf, gameId: market.gameId,
      team, opponent, kickoff: market.kickoff, oddsId: market.oddsId,
      oddsCapturedAt: market.oddsCapturedAt, bookmakerCount: market.bookmakerCount,
      teamSpread, gameTotal: market.total, impliedPoints,
      closeDropbackRate: close.rate, closePlays: close.plays,
      trailingDropbackRate: trailing.rate, trailingPlays: trailing.plays,
      leadingDropbackRate: leading.rate, leadingPlays: leading.plays,
      opponentAdjustedEpa: defense.effect, opponentDropbacks: defense.dropbacks,
      opponentGames: defense.games, opponentTouchdownDrives: drives.touchdowns,
      opponentCompetitiveDrives: drives.competitive,
      sourceGameIds: [...new Set(plays.filter(play => play.posteam === team || play.defteam === opponent || opponentOffenses.has(play.posteam)).map(play => play.gameId))].sort(),
    };
  }));
}
