/**
 * DraftKings NFL salary-CSV parser (Classic + Showdown Captain Mode).
 *
 * Deliberately separate from `web/src/app/dfs/actions.ts`'s `parseDkCsv`,
 * which serves NBA/MLB. That one splits each line on `,` with no quote
 * handling, which is survivable for NBA but not here: NFL `Game Info`
 * and DST `Name` fields are the ones DK quotes. This parser uses the
 * repo's existing quote-aware `parseCsvLine`.
 *
 * The trap this module exists to handle: a DK **Showdown** salary file
 * lists every player TWICE -- one `CPT` row and one `FLEX` row, with a
 * different `ID` and a different `Salary`. Treating those as two players
 * lets an optimizer roster the same human twice, which DK rejects. Here
 * they are collapsed into one `NflDkPlayer` carrying a `captain`
 * purchase option.
 *
 * See docs/nfl-dfs-spec.md section 1.
 */

import { parseCsvLine } from "@/app/dfs/csv";

export type NflSlateFormat = "classic" | "showdown";

export type NflPosition = "QB" | "RB" | "WR" | "TE" | "DST" | "K";

/** The set DK actually uses in NFL Classic. No kicker. */
export const NFL_CLASSIC_POSITIONS: readonly NflPosition[] = ["QB", "RB", "WR", "TE", "DST"];

/** Positions eligible for the Classic FLEX slot. */
export const NFL_FLEX_POSITIONS: readonly NflPosition[] = ["RB", "WR", "TE"];

/** Showdown captain multipliers, applied to both salary and points. */
export const SHOWDOWN_CAPTAIN_MULTIPLIER = 1.5;

export const DK_SALARY_CAP = 50_000;

/**
 * DK team abbreviations that differ from the canonical `nfl_teams`
 * abbreviation. Kept here rather than inline so the same map serves the
 * later matchup/roster join.
 *
 * `JAC`/`AZ` are the pair that already silently NULLed a whole team's
 * rows once in this repo's fantasy-football ingestion -- see CLAUDE.md.
 */
export const DK_NFL_TEAM_OVERRIDES: Readonly<Record<string, string>> = {
  JAC: "JAX",
  AZ: "ARI",
  LA: "LAR",
  WSH: "WAS",
  OAK: "LV",
  SD: "LAC",
  STL: "LAR",
};

export function canonicalNflTeam(abbrev: string): string {
  const upper = abbrev.trim().toUpperCase();
  return DK_NFL_TEAM_OVERRIDES[upper] ?? upper;
}

/** One purchasable roster slot for a player: DK sells CPT and FLEX separately. */
export type NflDkPurchase = {
  dkPlayerId: number;
  salary: number;
};

export type NflDkPlayer = {
  /**
   * Canonical identity of the human. For Showdown this is the FLEX row's
   * DK ID -- the CPT purchase carries its own id under `captain`.
   */
  dkPlayerId: number;
  name: string;
  /** Real football position, from the `Position` column. */
  position: NflPosition;
  /** Slots DK says this row may fill, from `Roster Position`. */
  rosterPositions: string[];
  teamAbbrev: string;
  opponent: string | null;
  homeAway: "home" | "away" | null;
  gameKey: string | null;
  gameInfo: string | null;
  salary: number;
  avgFptsDk: number | null;
  /** Showdown only. `null` on Classic slates and on any player DK did not price at CPT. */
  captain: NflDkPurchase | null;
};

export type NflDkSlate = {
  format: NflSlateFormat;
  players: NflDkPlayer[];
  /** Distinct `AWAY@HOME` keys present in the pool. */
  games: string[];
  /** Distinct canonical team abbreviations present in the pool. */
  teams: string[];
  /**
   * Non-fatal data problems. Always surface these -- a silently dropped
   * player is indistinguishable from a player DK did not list.
   */
  warnings: string[];
};

export class NflDkCsvError extends Error {}

const REQUIRED_COLUMNS = ["Name", "ID", "Roster Position", "Salary", "TeamAbbrev"] as const;

function normalizeHeaderCell(cell: string): string {
  return cell.trim().replace(/^﻿/, "");
}

/**
 * DK exports occasionally carry a preamble above the real header, and the
 * combined entry+salary file carries two tables in one document. Locate
 * the salary header rather than assuming line 0.
 */
function findHeaderIndex(lines: string[]): number {
  for (let i = 0; i < lines.length; i++) {
    const cells = parseCsvLine(lines[i]).map(normalizeHeaderCell);
    if (cells.includes("Roster Position") && cells.includes("Salary") && cells.includes("ID")) {
      return i;
    }
  }
  return -1;
}

function parsePosition(raw: string, warnings: string[], name: string): NflPosition | null {
  const value = raw.trim().toUpperCase();
  if (value === "DST" || value === "D" || value === "DEF") return "DST";
  if (value === "QB" || value === "RB" || value === "WR" || value === "TE" || value === "K") {
    return value as NflPosition;
  }
  warnings.push(`Unrecognized position "${raw}" for ${name || "unnamed player"} - row skipped`);
  return null;
}

/**
 * `Game Info` looks like `KC@BUF 09/07/2026 08:20PM ET`. Only the leading
 * `AWAY@HOME` key is used; the kickoff time is DK-formatted display text
 * and is not parsed into a timestamp here (nfl_matchups is the authority
 * on start times).
 */
function parseGameKey(gameInfo: string): string | null {
  const first = gameInfo.trim().split(/\s+/)[0] ?? "";
  if (!/^[A-Za-z]{2,4}@[A-Za-z]{2,4}$/.test(first)) return null;
  const [away, home] = first.split("@");
  return `${canonicalNflTeam(away)}@${canonicalNflTeam(home)}`;
}

type RawRow = {
  name: string;
  dkPlayerId: number;
  position: NflPosition;
  rosterPositions: string[];
  teamAbbrev: string;
  gameInfo: string | null;
  gameKey: string | null;
  salary: number;
  avgFptsDk: number | null;
};

/**
 * Parse a DK NFL salary export.
 *
 * Throws `NflDkCsvError` only when the file is not a DK salary export at
 * all (no locatable header, no usable rows). Row-level problems become
 * `warnings` so a mostly-good slate still loads and the user can see what
 * was dropped.
 */
export function parseNflDkSalaryCsv(content: string): NflDkSlate {
  const lines = content.split(/\r?\n/).filter((line) => line.trim().length > 0);
  const headerIndex = findHeaderIndex(lines);
  if (headerIndex === -1) {
    throw new NflDkCsvError(
      "Could not find a DraftKings salary header row (expected columns including " +
        "\"Roster Position\", \"Salary\" and \"ID\"). Is this the salary export rather than the entry file?",
    );
  }

  const header = parseCsvLine(lines[headerIndex]).map(normalizeHeaderCell);
  const missing = REQUIRED_COLUMNS.filter((column) => !header.includes(column));
  if (missing.length > 0) {
    throw new NflDkCsvError(`DraftKings CSV is missing required column(s): ${missing.join(", ")}`);
  }

  const col = (name: string) => header.indexOf(name);
  const nameCol = col("Name");
  const idCol = col("ID");
  const rosterPosCol = col("Roster Position");
  const positionCol = col("Position");
  const salaryCol = col("Salary");
  const teamCol = col("TeamAbbrev");
  const gameInfoCol = col("Game Info");
  const avgCol = col("AvgPointsPerGame");

  const warnings: string[] = [];
  const rows: RawRow[] = [];
  const seenIds = new Set<number>();

  for (let i = headerIndex + 1; i < lines.length; i++) {
    const cells = parseCsvLine(lines[i]).map((cell) => cell.trim());
    // A second table (the entry-file section) starts with its own header.
    if (cells.includes("Roster Position") && cells.includes("Salary")) break;

    const name = cells[nameCol] ?? "";
    const idRaw = cells[idCol] ?? "";
    if (!name || !idRaw) continue;

    const dkPlayerId = Number.parseInt(idRaw.replace(/[^0-9]/g, ""), 10);
    if (!Number.isFinite(dkPlayerId) || dkPlayerId <= 0) {
      warnings.push(`Unparseable DK ID "${idRaw}" for ${name} - row skipped`);
      continue;
    }
    if (seenIds.has(dkPlayerId)) {
      warnings.push(`Duplicate DK ID ${dkPlayerId} (${name}) - later row skipped`);
      continue;
    }

    const rosterPositions = (cells[rosterPosCol] ?? "")
      .split("/")
      .map((value) => value.trim().toUpperCase())
      .filter(Boolean);
    if (rosterPositions.length === 0) {
      warnings.push(`Missing Roster Position for ${name} - row skipped`);
      continue;
    }

    // `Position` is the real football position; `Roster Position` is the
    // slot. On Showdown they diverge completely (Roster Position is
    // CPT/FLEX), which is exactly why both columns are read.
    const positionSource = positionCol >= 0 ? (cells[positionCol] ?? "") : rosterPositions[0];
    const position = parsePosition(positionSource, warnings, name);
    if (!position) continue;

    const salary = Number.parseInt((cells[salaryCol] ?? "").replace(/[^0-9]/g, ""), 10);
    if (!Number.isFinite(salary) || salary <= 0) {
      warnings.push(`Missing or zero salary for ${name} - row skipped`);
      continue;
    }

    const teamAbbrev = canonicalNflTeam(cells[teamCol] ?? "");
    if (!teamAbbrev) {
      warnings.push(`Missing team for ${name} - row skipped`);
      continue;
    }

    const gameInfo = gameInfoCol >= 0 ? (cells[gameInfoCol] ?? "") : "";
    const gameKey = gameInfo ? parseGameKey(gameInfo) : null;
    if (gameInfo && !gameKey) {
      warnings.push(`Unparseable Game Info "${gameInfo}" for ${name}`);
    }

    const avgRaw = avgCol >= 0 ? (cells[avgCol] ?? "") : "";
    const avgParsed = Number.parseFloat(avgRaw);
    // 0.0 is a legitimate DK value for a rookie; only a blank is "unknown".
    const avgFptsDk = avgRaw.trim() === "" || !Number.isFinite(avgParsed) ? null : avgParsed;

    seenIds.add(dkPlayerId);
    rows.push({
      name,
      dkPlayerId,
      position,
      rosterPositions,
      teamAbbrev,
      gameInfo: gameInfo || null,
      gameKey,
      salary,
      avgFptsDk,
    });
  }

  if (rows.length === 0) {
    throw new NflDkCsvError("No players parsed from the DraftKings CSV.");
  }

  const format: NflSlateFormat = rows.some((row) => row.rosterPositions.includes("CPT"))
    ? "showdown"
    : "classic";

  const players = format === "showdown" ? collapseShowdownRows(rows, warnings) : rows.map(toClassicPlayer);

  const games = [...new Set(players.map((p) => p.gameKey).filter((k): k is string => Boolean(k)))].sort();
  const teams = [...new Set(players.map((p) => p.teamAbbrev))].sort();

  // Structural checks. These are the conditions under which a DK-legal
  // lineup cannot be built at all, so they are worth saying out loud
  // before the optimizer returns an unexplained empty result.
  if (format === "classic" && games.length < 2) {
    warnings.push(
      `Only ${games.length} game in this pool. DK Classic requires players from at least 2 different games, ` +
        "so no legal lineup exists - did you mean to upload a Showdown file?",
    );
  }
  if (format === "showdown") {
    if (games.length > 1) {
      warnings.push(`Showdown pool spans ${games.length} games (${games.join(", ")}) - expected exactly 1.`);
    }
    if (teams.length !== 2) {
      warnings.push(`Showdown pool has ${teams.length} team(s) (${teams.join(", ")}) - expected exactly 2.`);
    }
  }

  return { format, players, games, teams, warnings };
}

function toClassicPlayer(row: RawRow): NflDkPlayer {
  return {
    dkPlayerId: row.dkPlayerId,
    name: row.name,
    position: row.position,
    rosterPositions: row.rosterPositions,
    teamAbbrev: row.teamAbbrev,
    opponent: opponentOf(row.gameKey, row.teamAbbrev),
    homeAway: homeAwayOf(row.gameKey, row.teamAbbrev),
    gameKey: row.gameKey,
    gameInfo: row.gameInfo,
    salary: row.salary,
    avgFptsDk: row.avgFptsDk,
    captain: null,
  };
}

/**
 * Collapse the CPT and FLEX rows DK writes for the same human into one
 * player with two purchase options.
 *
 * Pairing key is name + team, not DK ID: the two rows deliberately carry
 * different IDs, which is the whole reason a naive parser double-rosters.
 */
function collapseShowdownRows(rows: RawRow[], warnings: string[]): NflDkPlayer[] {
  const key = (row: RawRow) => `${row.name.trim().toLowerCase()}|${row.teamAbbrev}`;

  const flexByKey = new Map<string, RawRow>();
  const captainByKey = new Map<string, RawRow>();
  for (const row of rows) {
    const isCaptain = row.rosterPositions.includes("CPT");
    const bucket = isCaptain ? captainByKey : flexByKey;
    const existing = bucket.get(key(row));
    if (existing) {
      warnings.push(
        `Two ${isCaptain ? "CPT" : "FLEX"} rows for ${row.name} (${row.teamAbbrev}) - kept DK ID ${existing.dkPlayerId}`,
      );
      continue;
    }
    bucket.set(key(row), row);
  }

  const players: NflDkPlayer[] = [];
  for (const [playerKey, flex] of flexByKey) {
    const captain = captainByKey.get(playerKey);
    players.push({
      ...toClassicPlayer(flex),
      captain: captain ? { dkPlayerId: captain.dkPlayerId, salary: captain.salary } : null,
    });
    if (!captain) {
      warnings.push(`${flex.name} has no CPT row - usable in FLEX only`);
    }
  }

  // A CPT row with no FLEX partner still identifies a real, rosterable
  // player. Keep it (captain-only) rather than dropping him silently.
  for (const [playerKey, captain] of captainByKey) {
    if (flexByKey.has(playerKey)) continue;
    warnings.push(`${captain.name} has no FLEX row - usable at CPT only`);
    players.push({
      ...toClassicPlayer(captain),
      salary: Math.round(captain.salary / SHOWDOWN_CAPTAIN_MULTIPLIER),
      captain: { dkPlayerId: captain.dkPlayerId, salary: captain.salary },
    });
  }

  return players;
}

function opponentOf(gameKey: string | null, team: string): string | null {
  if (!gameKey) return null;
  const [away, home] = gameKey.split("@");
  if (team === home) return away;
  if (team === away) return home;
  return null;
}

function homeAwayOf(gameKey: string | null, team: string): "home" | "away" | null {
  if (!gameKey) return null;
  const [away, home] = gameKey.split("@");
  if (team === home) return "home";
  if (team === away) return "away";
  return null;
}

/**
 * Salary DK charges to roster this player in a given slot. Showdown CPT
 * is priced by DK directly rather than derived, because DK rounds; the
 * multiplier is only the fallback when a CPT row is absent.
 */
export function salaryForSlot(player: NflDkPlayer, slot: "CPT" | "FLEX" | "CLASSIC"): number {
  if (slot !== "CPT") return player.salary;
  return player.captain?.salary ?? Math.round(player.salary * SHOWDOWN_CAPTAIN_MULTIPLIER);
}

/** DK ID to write into an entry-file cell for this player in this slot. */
export function dkIdForSlot(player: NflDkPlayer, slot: "CPT" | "FLEX" | "CLASSIC"): number {
  if (slot !== "CPT") return player.dkPlayerId;
  return player.captain?.dkPlayerId ?? player.dkPlayerId;
}
