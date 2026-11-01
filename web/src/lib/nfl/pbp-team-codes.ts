/**
 * nfl_teams codes that nflverse play-by-play spells differently. Every query
 * that filters nfl_pbp_archetypes.posteam/defteam by a team must translate
 * through this, or the Rams and Washington silently match nothing.
 */
export const PBP_TEAM_CODES: Readonly<Record<string, string>> = { LAR: "LA", WSH: "WAS" };

/** The play-by-play code for an nfl_teams code. */
export function pbpTeamCode(team: string): string {
  return PBP_TEAM_CODES[team] ?? team;
}

