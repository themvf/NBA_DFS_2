/**
 * DraftKings CFB salary import. Classic requires S-FLEX eligibility and two
 * games; Showdown pairs CPT with FLEX/UTIL draftables from exactly one game.
 * The CSV does not reliably identify the sport; the user must supply CFB.
 */

export type CfbMode = "classic" | "showdown";

export type CfbPosition = "QB" | "RB" | "WR" | "K";
export const CFB_POSITIONS: readonly CfbPosition[] = ["QB", "RB", "WR"];

export interface CfbSlatePlayer {
  dkId: number;
  captainId?: number;
  captainSalary?: number;
  name: string;
  position: CfbPosition;
  rosterPositions: string[];
  salary: number;
  team: string;
  opponent: string;
  /** "AWAY@HOME", the DK game key. */
  game: string;
  /** ISO kickoff parsed from Game Info (Eastern time). */
  kickoff: string | null;
  dkAvg: number;
  status: string;
}

export interface ParsedCfbSlate {
  mode: CfbMode;
  players: CfbSlatePlayer[];
  games: Array<{ game: string; kickoff: string | null }>;
  firstKickoff: string | null;
}

const REQUIRED = ["Position", "Name", "ID", "Roster Position", "Salary", "Game Info", "TeamAbbrev", "AvgPointsPerGame"];

export function splitCsvLine(line: string): string[] {
  const out: string[] = [];
  let cur = "", quoted = false;
  for (let i = 0; i < line.length; i += 1) {
    const ch = line[i];
    if (quoted) {
      if (ch === '"' && line[i + 1] === '"') { cur += '"'; i += 1; }
      else if (ch === '"') quoted = false;
      else cur += ch;
    } else if (ch === '"') quoted = true;
    else if (ch === ",") { out.push(cur); cur = ""; }
    else cur += ch;
  }
  out.push(cur);
  return out;
}

/** One CSV line, quoting any cell that needs it. */
export function joinCsvLine(cells: readonly string[]): string {
  return cells.map((cell) => (/[",\r\n]/.test(cell) ? `"${cell.replace(/"/g, '""')}"` : cell)).join(",");
}

/** Eastern-time Game Info ("NW@IU 09/25/2026 08:00PM ET") to an ISO instant. */
export function parseCfbKickoff(gameInfo: string): string | null {
  const m = gameInfo.match(/(\d{2})\/(\d{2})\/(\d{4})\s+(\d{1,2}):(\d{2})(AM|PM)\s+ET/i);
  if (!m) return null;
  const [, mm, dd, yyyy, hh, min, ampm] = m;
  let hour = Number(hh) % 12;
  if (ampm.toUpperCase() === "PM") hour += 12;
  // Offset of New York at that wall-clock time, from Intl (handles DST).
  const guess = Date.UTC(Number(yyyy), Number(mm) - 1, Number(dd), hour, Number(min));
  const parts = new Intl.DateTimeFormat("en-US", { timeZone: "America/New_York", hourCycle: "h23",
    year: "numeric", month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit" }).formatToParts(new Date(guess));
  const get = (type: string) => Number(parts.find((p) => p.type === type)?.value);
  const asNy = Date.UTC(get("year"), get("month") - 1, get("day"), get("hour"), get("minute"));
  return new Date(guess + (guess - asNy)).toISOString();
}

export function parseCfbSalaryCsv(text: string, mode: CfbMode = "classic"): ParsedCfbSlate {
  const lines = text.replace(/^﻿/, "").split(/\r?\n/).filter((l) => l.trim());
  if (lines.length < 2) throw new Error("The salary file is empty.");
  const header = splitCsvLine(lines[0]).map((h) => h.trim());
  const missing = REQUIRED.filter((h) => !header.includes(h));
  if (missing.length) throw new Error(`Not a DraftKings salary file: missing ${missing.join(", ")}.`);
  const col = (row: string[], name: string) => (row[header.indexOf(name)] ?? "").trim();

  const players: CfbSlatePlayer[] = [];
  for (const line of lines.slice(1)) {
    const row = splitCsvLine(line);
    const position = col(row, "Position");
    const rosterPositions = col(row, "Roster Position").split("/").map((s) => s.trim()).filter(Boolean);
    if (!CFB_POSITIONS.includes(position as CfbPosition) && !(mode === "showdown" && position === "K")) {
      throw new Error(`"${col(row, "Name")}" is listed at ${position}. CFB Classic has only QB, RB and WR; is this an NFL file?`);
    }
    if (mode === "showdown" && (rosterPositions.length !== 1 || !["CPT", "FLEX", "UTIL"].includes(rosterPositions[0]))) throw new Error("Showdown requires CPT and FLEX/UTIL salary rows.");
    if (mode === "classic" && !rosterPositions.includes("S-FLEX")) {
      throw new Error(`"${col(row, "Name")}" has no S-FLEX slot. This does not look like a DraftKings CFB Classic file.`);
    }
    const gameInfo = col(row, "Game Info");
    const game = gameInfo.split(/\s+/)[0] ?? "";
    const [away, home] = game.split("@");
    const team = col(row, "TeamAbbrev");
    if (!away || !home || (team !== away && team !== home)) throw new Error(`Could not read the game for "${col(row, "Name")}": ${gameInfo}`);
    const salary = Number(col(row, "Salary"));
    const dkId = Number(col(row, "ID"));
    if (!Number.isInteger(salary) || salary <= 0 || !Number.isSafeInteger(dkId) || dkId <= 0) throw new Error(`Bad salary or ID for "${col(row, "Name")}".`);
    players.push({
      dkId, name: col(row, "Name"), position: position as CfbPosition, rosterPositions, salary, team,
      opponent: team === away ? home : away, game, kickoff: parseCfbKickoff(gameInfo),
      dkAvg: Number(col(row, "AvgPointsPerGame")) || 0, status: col(row, "Status").toUpperCase(),
    });
  }
  const ids = new Set<number>();
  for (const p of players) {
    if (ids.has(p.dkId)) throw new Error(`Player ID ${p.dkId} appears twice.`);
    ids.add(p.dkId);
  }
  const byGame = new Map<string, string | null>();
  for (const p of players) if (!byGame.has(p.game)) byGame.set(p.game, p.kickoff);
  if (mode === "showdown" && byGame.size !== 1) throw new Error("Showdown needs exactly one game.");
  if (mode === "classic" && byGame.size < 2) throw new Error("CFB Classic needs at least two games; this file has one.");
  const games = [...byGame].map(([game, kickoff]) => ({ game, kickoff })).sort((a, b) => (a.kickoff ?? "").localeCompare(b.kickoff ?? ""));
  const kicks = games.map((g) => g.kickoff).filter((k): k is string => k != null).sort();
  if (mode === "showdown") {
    // Keep one base player per athlete; captain IDs are separate DK draftables.
    const key = (p: CfbSlatePlayer) => `${p.team}|${p.name.trim().toLowerCase()}`;
    const base = players.filter((p) => p.rosterPositions[0] !== "CPT");
    const captains = players.filter((p) => p.rosterPositions[0] === "CPT");
    if (base.length < 6 || new Set(base.map(key)).size !== base.length || new Set(captains.map(key)).size !== captains.length)
      throw new Error("Showdown needs six distinct players and unambiguous Captain/FLEX pairs.");
    if (base.length !== captains.length) throw new Error("Every Showdown player needs both a Captain and FLEX row.");
    for (const p of base) {
      const c = captains.find((c) => key(c) === key(p));
      if (!c || c.position !== p.position || c.kickoff !== p.kickoff || c.salary !== p.salary * 1.5)
        throw new Error(`Invalid Captain pair or 1.5x salary for ${p.name}.`);
      p.captainId = c.dkId; p.captainSalary = c.salary;
    }
    return { mode, players: base, games, firstKickoff: kicks[0] ?? null };
  }
  return { mode, players, games, firstKickoff: kicks[0] ?? null };
}
