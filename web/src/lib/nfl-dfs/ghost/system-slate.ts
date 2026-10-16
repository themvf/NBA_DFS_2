/**
 * A DraftKings slate built from DraftKings' own pool snapshot
 * (`nfl_dfs_dk_pool_player_status`, written by ingest/nfl_dfs_dk_pool.py),
 * in exactly the shape the salary-CSV parser produces, so the house and ghost
 * builds run through the same workspace read as an uploaded file.
 *
 * Verified 2026-10-16 against the uploaded SEA@DEN Showdown: all 56 players
 * agree on name, position, team and salary, and every captain salary in the
 * CSV is exactly 1.5x the flex salary.
 *
 * What differs from a CSV, by construction:
 *  - ids: the pool carries DraftKings' PLAYER id, not the draftable id the CSV
 *    carries, so a system slate's ids are the pool ids and a captain's is
 *    `CAPTAIN_ID_OFFSET + id`. System lineups are never exported, so no id
 *    here ever reaches DraftKings.
 *  - the pool lists only flex rows for Showdown; the captain purchase is
 *    derived (1.5x salary).
 *  - game info is written from the canonical schedule in the CSV's own
 *    format ("AWAY@HOME MM/DD/YYYY HH:MMPM ET").
 *
 * Pure: no I/O.
 */
import { canonicalNflTeam, DK_OUT_STATUSES, SHOWDOWN_CAPTAIN_MULTIPLIER,
  type NflDkPlayer, type NflDkSlate, type NflPosition } from "../dk-salary-csv";

export const SYSTEM_SLATE_VERSION = "nfl-system-slate-v1";
export const CAPTAIN_ID_OFFSET = 1_000_000_000;

export type PoolRow = { dk_pid: number; name: string; position: string | null; team: string | null; opponent: string | null;
  salary: number | null; status: string | null; is_disabled: boolean; ppg: number | null };
export type ScheduledGame = { gameId: number | string; away: string; home: string; kickoff: string };

const POSITIONS = new Set<NflPosition>(["QB", "RB", "WR", "TE", "K", "DST"]);
const FLEX = new Set(["RB", "WR", "TE"]);

/** "SEA@DEN 10/15/2026 08:15PM ET", the DraftKings CSV's Game Info, from a kickoff instant. */
export function dkGameInfo(away: string, home: string, kickoffIso: string): string {
  const parts = new Intl.DateTimeFormat("en-US", { timeZone: "America/New_York", year: "numeric", month: "2-digit",
    day: "2-digit", hour: "2-digit", minute: "2-digit", hour12: true }).formatToParts(new Date(kickoffIso));
  const get = (t: string) => parts.find(p => p.type === t)?.value ?? "";
  return `${away}@${home} ${get("month")}/${get("day")}/${get("year")} ${get("hour")}:${get("minute")}${get("dayPeriod").toUpperCase()} ET`;
}

export function systemSlate(format: "classic" | "showdown", rows: readonly PoolRow[], games: readonly ScheduledGame[]): NflDkSlate {
  const warnings: string[] = [];
  const gameOf = new Map<string, ScheduledGame>();
  for (const g of games) {
    gameOf.set(canonicalNflTeam(g.away), g);
    gameOf.set(canonicalNflTeam(g.home), g);
  }
  const players: NflDkPlayer[] = [];
  const seen = new Set<number>();
  for (const r of [...rows].sort((a, b) => a.dk_pid - b.dk_pid)) {
    const position = String(r.position ?? "").toUpperCase() as NflPosition;
    const team = canonicalNflTeam(r.team ?? "");
    if (!POSITIONS.has(position) || (format === "classic" && position === "K")) { warnings.push(`${r.name}: position ${r.position} not on this slate - skipped`); continue; }
    if (!team || !r.salary || r.salary <= 0) { warnings.push(`${r.name}: missing team or salary - skipped`); continue; }
    if (seen.has(r.dk_pid)) { warnings.push(`${r.name}: duplicate pool id ${r.dk_pid} - skipped`); continue; }
    const game = gameOf.get(team);
    if (!game) { warnings.push(`${r.name}: ${team} has no scheduled game on this slate - skipped`); continue; }
    seen.add(r.dk_pid);
    const away = canonicalNflTeam(game.away), home = canonicalNflTeam(game.home);
    const status = r.is_disabled ? "DISABLED" : (r.status ? String(r.status).toUpperCase() : null);
    players.push({
      dkPlayerId: r.dk_pid,
      name: r.name,
      position,
      rosterPositions: format === "showdown" ? ["FLEX"] : FLEX.has(position) ? [position, "FLEX"] : [position],
      teamAbbrev: team,
      opponent: team === away ? home : away,
      homeAway: team === home ? "home" : "away",
      gameKey: `${away}@${home}`,
      gameInfo: dkGameInfo(away, home, game.kickoff),
      salary: r.salary,
      avgFptsDk: r.ppg ?? null,
      status,
      isOut: r.is_disabled || (status !== null && DK_OUT_STATUSES.has(status)),
      captain: format === "showdown"
        ? { dkPlayerId: CAPTAIN_ID_OFFSET + r.dk_pid, salary: Math.round(r.salary * SHOWDOWN_CAPTAIN_MULTIPLIER) }
        : null,
    });
  }
  if (!players.length) throw new Error("System slate: no usable players in the pool snapshot");
  const gameKeys = [...new Set(players.map(p => p.gameKey!))].sort();
  const teams = [...new Set(players.map(p => p.teamAbbrev))].sort();
  if (format === "showdown" && (gameKeys.length !== 1 || teams.length !== 2)) throw new Error(`System slate: a Showdown pool must be one game, found ${gameKeys.join(", ")}`);
  if (format === "classic" && gameKeys.length < 2) throw new Error("System slate: a Classic pool needs at least two games");
  if (players.some(p => p.avgFptsDk == null)) warnings.push(`${players.filter(p => p.avgFptsDk == null).length} players have no DraftKings average in the pool snapshot (captured before 2026-10-16, or blank on DraftKings).`);
  return { format, players, games: gameKeys, teams, warnings };
}
