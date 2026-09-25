/**
 * College football DraftKings projections: `cfb-dfs-baseline-v1`.
 *
 * A port of model/cfb_dfs_baseline.py (the reference; a parity test keeps the
 * two identical). UNVALIDATED: every constant is a stated prior, chosen on
 * 2026-09-25 and not fitted to results.
 *
 *   rate  = (2026 points + W_PRIOR * 2025 points)
 *           / (2026 appearances + MISSED_GAME_WEIGHT * 2026 missed team games
 *              + W_PRIOR * 2025 appearances)
 *   env   = clamp(team implied total / team 2026 points per game, 0.6, 1.4) ^ ENV_POWER
 *   proj  = rate * env; 0 for OUT / O / D / IR.
 *
 * Matching: normalized full name, preferring the CFBD id that played for this
 * DK team in 2026 (the id follows a transfer). Nicknames fall back to the one
 * 2026 player on the team with the same surname, accepted only when DK's own
 * season average agrees with CFBD's within SURNAME_MATCH_TOLERANCE.
 */
import type { CfbSlatePlayer } from "./salary-csv";

export const CFB_PROJECTION_VERSION = "cfb-dfs-baseline-v1";
export const W_PRIOR = 0.1;
export const MISSED_GAME_WEIGHT = 0.5;
export const SURNAME_MATCH_TOLERANCE = 1.0;
export const ENV_POWER = 0.5;
export const EXCLUDED_STATUS = new Set(["OUT", "O", "D", "DOUBTFUL", "IR"]);
const SUFFIXES = new Set(["jr", "sr", "ii", "iii", "iv", "v"]);

export interface HistoryRow { cfbdPlayerId: string; playerName: string; team: string; season: number; dkPoints: number }

export interface ProjectionContext {
  season: number;
  history: HistoryRow[];
  /** Distinct games per (team, season), from the same stats table. */
  teamGames: Map<string, number>;
  /** 2026 points per game by CFBD team name. */
  teamPpg: Map<string, number>;
  /** Implied team total tonight by DK team abbreviation. */
  implied: Map<string, number>;
  /** DK abbreviation -> CFBD team name. */
  teamName: (dkAbbrev: string) => string;
}

export interface CfbProjection {
  dkId: number;
  proj: number;
  rate: number;
  env: number;
  games2026: number;
  games2025: number;
  cfbdPlayerId: string | null;
  match: "name" | "team+surname" | null;
  excluded: boolean;
}

function words(name: string): string[] {
  return name.normalize("NFKD").replace(/[̀-ͯ]/g, "").toLowerCase()
    .split(/[^a-z0-9]+/).filter((w) => w && !SUFFIXES.has(w));
}
export const normalizeName = (name: string) => words(name).join("");
export const surname = (name: string) => words(name).at(-1) ?? "";
const round = (value: number, digits: number) => Math.round(value * 10 ** digits) / 10 ** digits;
export const teamSeasonKey = (team: string, season: number) => `${team}|${season}`;

export function projectCfbPlayers(players: readonly CfbSlatePlayer[], ctx: ProjectionContext): CfbProjection[] {
  const { season } = ctx;
  const byName = new Map<string, HistoryRow[]>();
  const byId = new Map<string, HistoryRow[]>();
  const byTeamSurname = new Map<string, Set<string>>();
  for (const row of ctx.history) {
    const n = normalizeName(row.playerName);
    byName.set(n, [...(byName.get(n) ?? []), row]);
    byId.set(row.cfbdPlayerId, [...(byId.get(row.cfbdPlayerId) ?? []), row]);
    if (row.season === season) {
      const key = `${row.team}|${surname(row.playerName)}`;
      byTeamSurname.set(key, new Set([...(byTeamSurname.get(key) ?? []), row.cfbdPlayerId]));
    }
  }
  return players.map((p) => {
    const team = ctx.teamName(p.team);
    const rows = byName.get(normalizeName(p.name)) ?? [];
    let ids = new Set(rows.filter((r) => r.season === season && r.team === team).map((r) => r.cfbdPlayerId));
    if (ids.size !== 1) {
      const all = new Set(rows.map((r) => r.cfbdPlayerId));
      ids = all.size === 1 ? all : new Set();
    }
    let match: CfbProjection["match"] = ids.size ? "name" : null;
    if (!ids.size) {
      const same = byTeamSurname.get(`${team}|${surname(p.name)}`) ?? new Set();
      if (same.size === 1) {
        const id = [...same][0];
        const games = (byId.get(id) ?? []).filter((r) => r.season === season).map((r) => r.dkPoints);
        if (games.length && Math.abs(games.reduce((a, b) => a + b, 0) / games.length - p.dkAvg) <= SURNAME_MATCH_TOLERANCE) {
          ids = same; match = "team+surname";
        }
      }
    }
    const mine = [...ids].flatMap((id) => byId.get(id) ?? []);
    const current = mine.filter((r) => r.season === season);
    const prior = mine.filter((r) => r.season === season - 1);
    const missed = Math.max(0, (ctx.teamGames.get(teamSeasonKey(team, season)) ?? 0) - current.length);
    const denom = current.length + MISSED_GAME_WEIGHT * missed + W_PRIOR * prior.length;
    const sum = (list: HistoryRow[]) => list.reduce((a, r) => a + r.dkPoints, 0);
    const rate = denom ? (sum(current) + W_PRIOR * sum(prior)) / denom : 0;
    let env = 1;
    const implied = ctx.implied.get(p.team);
    const ppg = ctx.teamPpg.get(team);
    if (implied != null && ppg) env = Math.max(0.6, Math.min(1.4, implied / ppg)) ** ENV_POWER;
    const excluded = EXCLUDED_STATUS.has(p.status);
    return {
      dkId: p.dkId, proj: excluded ? 0 : round(rate * env, 2), rate: round(rate, 2), env: round(env, 3),
      games2026: current.length, games2025: prior.length, cfbdPlayerId: ids.size === 1 ? [...ids][0] : null, match, excluded,
    };
  });
}

export interface TeamResolution { team: string | null; hits: number; runnerUp: number }

/**
 * Which CFBD team each DraftKings team code means, from the players themselves:
 * the CFBD team with the most exact-name 2026 matches among that code's players.
 * Accepted only with at least MIN_TEAM_HITS matches and a clear lead, so an
 * unknown code stays unresolved instead of borrowing a similar team's history.
 */
export const MIN_TEAM_HITS = 2;

export function resolveCfbTeams(players: readonly CfbSlatePlayer[], history: readonly HistoryRow[], season: number): Map<string, TeamResolution> {
  const teamsByName = new Map<string, Set<string>>();
  for (const row of history) {
    if (row.season !== season) continue;
    const n = normalizeName(row.playerName);
    teamsByName.set(n, new Set([...(teamsByName.get(n) ?? []), row.team]));
  }
  const out = new Map<string, TeamResolution>();
  for (const code of new Set(players.map((p) => p.team))) {
    const hits = new Map<string, number>();
    for (const p of players.filter((q) => q.team === code)) {
      for (const team of teamsByName.get(normalizeName(p.name)) ?? []) hits.set(team, (hits.get(team) ?? 0) + 1);
    }
    const ranked = [...hits].sort((a, b) => b[1] - a[1]);
    const [best, second] = [ranked[0], ranked[1]];
    const clear = best && best[1] >= MIN_TEAM_HITS && (!second || best[1] >= 2 * second[1]);
    out.set(code, { team: clear ? best[0] : null, hits: best?.[1] ?? 0, runnerUp: second?.[1] ?? 0 });
  }
  return out;
}
