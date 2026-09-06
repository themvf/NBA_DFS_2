import { scoreNflOffense, type NflOffenseStats } from "./scoring";

export type ContextMember = { id: string; name: string; position: string; status: string; recordedPlays: number | null };
export type ContextGame = { week: number; date: string; team: string; opponent: string; plays: number; covered: number; roster: ContextMember[] };
export type ContextRow = { playerId: string; gameKey: string; stats: NflOffenseStats | null; targets: number | null; attempts: number | null };
export type PlayerContext = {
  version: number; season: number;
  sources: Record<string, { responseHash: string; fetchedAt: string; sourcePublishedAt: string | null; url: string }>;
  players: { id: string; name: string; position: string }[];
  games: Record<string, ContextGame>; rows: ContextRow[];
  audit: { scheduledGames: number; teamGames: number; playerRows: number; scoredRows: number; recordedPlays: number; scrimmagePlays: number; unknownRosterStatuses: number; routesAvailable: boolean; pregameInjuryReasonsAvailable: boolean };
};

export function contextPoints(row: ContextRow) {
  return row.stats ? Math.round(scoreNflOffense(row.stats) * 100) / 100 : null;
}

export function participationLabel(member: ContextMember, game: ContextGame) {
  if (member.recordedPlays === null) return "Participation unavailable";
  if (member.recordedPlays > 0) return `${member.recordedPlays} recorded plays`;
  return game.covered === game.plays && game.plays > 0 ? "No recorded scrimmage participation" : "Not observed · coverage incomplete";
}

/** Historical inverse-CDF sample percentiles; never a predictive lineup range. */
export function historicalRange(rows: ContextRow[]) {
  const values = rows.map(contextPoints).filter((v): v is number => v !== null).sort((a, b) => a - b);
  if (!values.length) return null;
  const q = (p: number) => values[Math.max(0, Math.ceil(values.length * p) - 1)];
  return { n: values.length, p10: q(.1), p50: q(.5), p90: q(.9) };
}
