import { canonicalNflTeam } from './dk-salary-csv';

export type ScheduledGame = { season: number; week: number; homeTeam: string; awayTeam: string; kickoff: string | Date };
export type SlateGame = { gameKey: string | null; gameInfo: string | null };

/** Match every dated DK matchup; never infer a week from whichever run is newest. */
export function resolveSlateWeek(players: readonly SlateGame[], schedule: readonly ScheduledGame[]) {
  const games = new Map<string, SlateGame>();
  for (const player of players) {
    if (!player.gameKey || !player.gameInfo) throw new Error('Every salary player needs a dated Game Info matchup.');
    games.set(`${player.gameKey}|${player.gameInfo}`, player);
  }
  if (!games.size) throw new Error('The salary slate has no dated games.');
  const weeks = new Map<string, { season: number; week: number }>();
  const dateFormat = new Intl.DateTimeFormat('en-US', { timeZone: 'America/New_York', year: 'numeric', month: '2-digit', day: '2-digit' });
  for (const game of games.values()) {
    const date = game.gameInfo!.match(/\b(\d{2}\/\d{2}\/\d{4})\b/)?.[1];
    const key = game.gameKey!.split('@').map(canonicalNflTeam).join('@');
    const matches = schedule.filter(g => `${canonicalNflTeam(g.awayTeam)}@${canonicalNflTeam(g.homeTeam)}` === key
      && Number.isFinite(new Date(g.kickoff).getTime()) && dateFormat.format(new Date(g.kickoff)) === date);
    if (matches.length !== 1) throw new Error(`Cannot uniquely match ${game.gameInfo} to the NFL schedule. Refresh the schedule or check the salary file.`);
    const match = matches[0];
    weeks.set(`${match.season}:${match.week}`, { season: match.season, week: match.week });
  }
  if (weeks.size !== 1) throw new Error('This salary file spans multiple NFL weeks; upload one game-week slate.');
  return [...weeks.values()][0];
}
