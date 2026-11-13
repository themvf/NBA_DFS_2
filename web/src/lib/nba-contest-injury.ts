// The official NBA injury report as the contest builder reads it (nba-points-sit-injury-v1).
// Pure: the report row for each quoted player in his own game, and whether both teams in
// that game had filed. The rates and the drop rule live in nba-contest-optimizer.ts
// (sitProbInjury), pinned to research/nba_wta_trios.py by the fixture.

export type InjuryStateRow = { game_date: string; player_key: string; home_key: string; away_key: string; status: string; reason: string };
export type InjurySubmission = { team: string; game_date: string; submission: string };
export type InjuryReport = {
  sha: string; publishedAt: string; observedAt: string; sourceUrl: string;
  states: InjuryStateRow[]; submissions: InjurySubmission[];
};
export type PlayerInjury = { status: string | null; reason: string | null; teamsSubmitted: boolean };

/** The report's team key (accent-free, lower case, letters and digits), with its one alias. */
export const injuryTeamKey = (name: string) => {
  const k = name.normalize("NFD").replace(/[\u0300-\u036f]/g, "").toLowerCase().replace(/[^a-z0-9]/g, "");
  return k === "laclippers" ? "losangelesclippers" : k;
};

/**
 * Each player's status on the report, keyed by player key. Not listed is `status: null`.
 * Two rows for one player in one game is an identity problem: he reads "unverified", which
 * neither clears him nor drops him. `teamsSubmitted` is true only when both teams in his
 * game have a "submitted" row for the date.
 */
export function playerInjuries(date: string, report: InjuryReport | null,
  players: { key: string; eventId: string }[], games: { eventId: string; homeTeam: string; awayTeam: string }[]): Map<string, PlayerInjury> {
  const out = new Map<string, PlayerInjury>();
  if (!report) return out;
  const filed = new Set(report.submissions.filter((s) => s.game_date === date && s.submission === "submitted").map((s) => injuryTeamKey(s.team)));
  const gameOf = new Map(games.map((g) => [g.eventId, { home: injuryTeamKey(g.homeTeam), away: injuryTeamKey(g.awayTeam) }]));
  const rows = report.states.filter((s) => s.game_date === date);
  for (const p of players) {
    const g = gameOf.get(p.eventId);
    if (!g) continue;
    const hits = rows.filter((s) => s.player_key === p.key && s.home_key === g.home && s.away_key === g.away);
    const teamsSubmitted = filed.has(g.home) && filed.has(g.away);
    out.set(p.key, hits.length === 1 ? { status: hits[0].status, reason: hits[0].reason, teamsSubmitted }
      : hits.length > 1 ? { status: "unverified", reason: "Listed twice in this game", teamsSubmitted }
      : { status: null, reason: null, teamsSubmitted });
  }
  return out;
}
