/**
 * DraftKings College Football Classic salary file.
 *
 * Separate from the NFL reader on purpose: CFB Classic has no TE, K or DST,
 * adds a SUPER FLEX slot (DK writes it "S-FLEX"), and must draw from at least
 * two games. A file that does not look like CFB Classic is rejected rather
 * than coerced, so an NFL file cannot land here by mistake.
 */

export type CfbPosition = "QB" | "RB" | "WR";
export const CFB_POSITIONS: readonly CfbPosition[] = ["QB", "RB", "WR"];

export interface CfbSlatePlayer {
  dkId: number;
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
  players: CfbSlatePlayer[];
  games: Array<{ game: string; kickoff: string | null }>;
  firstKickoff: string | null;
}

const REQUIRED = ["Position", "Name", "ID", "Roster Position", "Salary", "Game Info", "TeamAbbrev", "AvgPointsPerGame"];

function splitCsvLine(line: string): string[] {
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

export function parseCfbSalaryCsv(text: string): ParsedCfbSlate {
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
    if (!CFB_POSITIONS.includes(position as CfbPosition)) {
      throw new Error(`"${col(row, "Name")}" is listed at ${position}. CFB Classic has only QB, RB and WR; is this an NFL file?`);
    }
    if (!rosterPositions.includes("S-FLEX")) {
      throw new Error(`"${col(row, "Name")}" has no S-FLEX slot. This does not look like a DraftKings CFB Classic file.`);
    }
    const gameInfo = col(row, "Game Info");
    const game = gameInfo.split(/\s+/)[0] ?? "";
    const [away, home] = game.split("@");
    const team = col(row, "TeamAbbrev");
    if (!away || !home || (team !== away && team !== home)) throw new Error(`Could not read the game for "${col(row, "Name")}": ${gameInfo}`);
    const salary = Number(col(row, "Salary"));
    const dkId = Number(col(row, "ID"));
    if (!Number.isFinite(salary) || !Number.isInteger(dkId)) throw new Error(`Bad salary or ID for "${col(row, "Name")}".`);
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
  if (byGame.size < 2) throw new Error("CFB Classic needs at least two games; this file has one.");
  const games = [...byGame].map(([game, kickoff]) => ({ game, kickoff })).sort((a, b) => (a.kickoff ?? "").localeCompare(b.kickoff ?? ""));
  const kicks = games.map((g) => g.kickoff).filter((k): k is string => k != null).sort();
  return { players, games, firstKickoff: kicks[0] ?? null };
}
