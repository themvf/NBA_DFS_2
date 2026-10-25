/**
 * Outside ownership ranges as a bound on our prior — `nfl-ownership-bands-v1`.
 *
 * Owner's decision (2026-10-24, week 7 main slate): read FantasyPros' "DraftKings
 * Projected Roster %" ranges (e.g. 10-20%) and hold our prior inside them. A
 * player our prior puts below the range is raised to its low end, one above it
 * is lowered to its high end, and anything inside keeps our number. Players the
 * range list does not cover keep our number. Nothing is renormalised, so the
 * slate total moves; the report says by how much.
 *
 * Pure: no database. The text is the page copied as plain text: each player is
 * a name line (sometimes repeated with a status letter appended), a
 * "TEAM at|vs. OPP<tab>$salary" line and a "LOW-HIGH%" line, under a position
 * heading. The Flex section repeats players and is skipped.
 */

import { canonicalNflTeam } from "./dk-salary-csv";

export const NFL_OWNERSHIP_BANDS_VERSION = "nfl-ownership-bands-v1";

export type OwnershipBand = { name: string; position: string; team: string; opponent: string; salary: number; low: number; high: number };

export type OwnershipBandsParse = { rows: OwnershipBand[]; warnings: string[] };

const SECTIONS: [RegExp, string | null][] = [
  [/^Quarterbacks - Projected Roster %/i, "QB"],
  [/^Running Backs - Projected Roster %/i, "RB"],
  [/^Wide Receivers - Projected Roster %/i, "WR"],
  [/^Tight Ends - Projected Roster %/i, "TE"],
  [/^Flex Position/i, null],
  [/^DST - Projected Roster %/i, "DST"],
];
const GAME = /^([A-Z]{2,3})\s+(?:at|vs\.?)\s+([A-Z]{2,3})\s+\$([\d,]+)\s*$/;
const BAND = /^(\d+(?:\.\d+)?)\s*-\s*(\d+(?:\.\d+)?)\s*%$/;

export function normalizeBandName(name: string): string {
  return name.normalize("NFKD").toLowerCase().replace(/\b(jr|sr|ii|iii|iv|v)\b\.?/g, "").replace(/[^a-z]/g, "");
}

/** Strip a status letter FantasyPros appends to the repeated name ("Lamar JacksonO"). */
function nameFrom(lines: string[], i: number): string | null {
  const prev = lines[i - 1]?.trim() ?? "";
  const before = lines[i - 2]?.trim() ?? "";
  if (!prev) return null;
  if (before && prev.startsWith(before) && prev.length - before.length <= 2) return before;
  return prev;
}

export function parseFantasyProsOwnership(text: string): OwnershipBandsParse {
  const lines = text.replace(/\r/g, "").split("\n");
  const rows: OwnershipBand[] = [];
  const warnings: string[] = [];
  let position: string | null | undefined;
  for (let i = 0; i < lines.length; i += 1) {
    const line = lines[i].trim();
    const section = SECTIONS.find(([re]) => re.test(line));
    if (section) { position = section[1]; continue; }
    const g = GAME.exec(line);
    if (!g) continue;
    if (position === undefined) { warnings.push(`player line before any position heading: "${line}"`); continue; }
    if (position === null) continue;
    const band = BAND.exec((lines[i + 1] ?? "").trim());
    const name = nameFrom(lines, i);
    if (!band || !name) { warnings.push(`no name or range next to "${line}"`); continue; }
    const low = Number(band[1]), high = Number(band[2]);
    if (!(low <= high)) { warnings.push(`range ${band[0]} for ${name} runs backwards`); continue; }
    rows.push({ name, position, team: canonicalNflTeam(g[1]), opponent: canonicalNflTeam(g[2]),
      salary: Number(g[3].replace(/,/g, "")), low, high });
  }
  return { rows, warnings };
}

export type BandPlayer = { dkPlayerId: number; name: string; position: string; team: string; salary: number; isOut?: boolean };

export type BandReport = {
  version: string;
  source: string;
  capturedAt: string | null;
  matched: number;
  raised: number;
  lowered: number;
  kept: number;
  /** Range rows that matched no slate player (other slates' players, salary changes). */
  unmatched: number;
  /** Slate players with ownership that no range covers: they keep our number. */
  uncovered: number;
  /** Sum of ownership before and after, percent. */
  totalBefore: number;
  totalAfter: number;
};

/**
 * A defense matches on its nickname as well as team and salary, because the
 * page can pair a name with the wrong game line ("Minnesota Vikings / MIA at
 * NYJ" on 2026-10-24); such a row stays unmatched rather than bounding Miami.
 */
function sameNickname(a: string, b: string): boolean {
  const last = (s: string) => normalizeBandName(s.trim().split(/\s+/).pop() ?? "");
  return last(a) !== "" && last(a) === last(b);
}

/** Match ranges to slate players: same team and position, same salary, same name (a DST by nickname). */
export function matchBands(players: BandPlayer[], rows: OwnershipBand[]): Map<number, OwnershipBand> {
  const out = new Map<number, OwnershipBand>();
  const used = new Set<OwnershipBand>();
  for (const p of players) {
    const team = canonicalNflTeam(p.team), pos = p.position.toUpperCase();
    const hits = rows.filter((r) => !used.has(r) && r.team === team && r.position === pos && r.salary === p.salary
      && (pos === "DST" ? sameNickname(r.name, p.name) : normalizeBandName(r.name) === normalizeBandName(p.name)));
    if (hits.length === 1) { out.set(p.dkPlayerId, hits[0]); used.add(hits[0]); }
  }
  return out;
}

export function clampToBand(ours: number, band: Pick<OwnershipBand, "low" | "high">): number {
  return Math.min(band.high, Math.max(band.low, ours));
}

export function bandReport(source: string, capturedAt: string | null, rows: OwnershipBand[], matches: Map<number, OwnershipBand>,
  before: Map<number, number>, after: Map<number, number>): BandReport {
  let raised = 0, lowered = 0, kept = 0, totalBefore = 0, totalAfter = 0;
  for (const [id, b] of before) {
    const a = after.get(id) ?? b;
    totalBefore += b; totalAfter += a;
    if (!matches.has(id)) continue;
    if (a > b + 1e-9) raised += 1; else if (a < b - 1e-9) lowered += 1; else kept += 1;
  }
  const covered = new Set(matches.values());
  return { version: NFL_OWNERSHIP_BANDS_VERSION, source, capturedAt, matched: matches.size, raised, lowered, kept,
    unmatched: rows.filter((r) => !covered.has(r)).length, uncovered: [...before.keys()].filter((id) => !matches.has(id)).length,
    totalBefore: Math.round(totalBefore * 10) / 10, totalAfter: Math.round(totalAfter * 10) / 10 };
}
