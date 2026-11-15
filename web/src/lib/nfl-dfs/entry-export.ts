import { parseCsvLine, stringifyCsvLine } from "@/app/dfs/csv";
import type { NflGeneratedLineup } from "@/app/dfs/nfl/nfl-optimizer";
import { assertDstGameScript, assertShowdownLineup } from "./showdown-legality";
import { kickerRoleBlockedReason } from './availability';
import { SINGLE_STAT_ROSTER_SIZE, type NflSlateFormat } from "./dk-salary-csv";

function normalized(value: string): string {
  return value.replace(/^\uFEFF/, "").trim().toUpperCase();
}

function readEntryFile(content: string) {
  const rawLines = content.split(/\r?\n/).filter((line) => line.trim());
  const headerIndex = rawLines.findIndex((line) => parseCsvLine(line).some((cell) => normalized(cell) === "ENTRY ID"));
  if (headerIndex < 0) throw new Error("Could not find the DraftKings Entry ID header.");
  const header = parseCsvLine(rawLines[headerIndex]);
  // Classic starts at QB, Showdown at CPT, Single Stat at its first FLEX.
  const firstSlot = header.findIndex((cell) => ["QB", "CPT", "FLEX"].includes(normalized(cell)));
  if (firstSlot < 0) throw new Error("Could not find NFL roster columns in the DraftKings entry file.");
  const first = normalized(header[firstSlot]);
  // A Single Stat entry file carries exactly three FLEX columns. Count them
  // rather than assume, so a DraftKings change to the roster shows up as a
  // mismatch against the lineups instead of a silently truncated row.
  const flexColumns = header.slice(firstSlot).findIndex((cell) => normalized(cell) !== "FLEX");
  const expectedSlots = first === "CPT" ? 6 : first === "QB" ? 9 : flexColumns < 0 ? header.length - firstSlot : flexColumns;
  if (first === "FLEX" && expectedSlots !== SINGLE_STAT_ROSTER_SIZE) {
    throw new Error(`This entry file has ${expectedSlots} FLEX columns; a DraftKings Single Stat entry has ${SINGLE_STAT_ROSTER_SIZE}.`);
  }
  const entryRows = rawLines.slice(headerIndex + 1).map(parseCsvLine).filter((row) => row[0]?.trim());
  return { rawLines, headerIndex, header, firstSlot, expectedSlots, entryRows };
}

/**
 * How many entries the DraftKings file holds. Pre-export QA uses
 * `describeNflDkEntryFile` for the full check (single-entry rows, empty rows).
 */
export function countNflDkEntryRows(content: string): number {
  return readEntryFile(content).entryRows.length;
}

/** The same legality every export applies, whichever file it writes. */
function exportCells(lineup: NflGeneratedLineup, expectedSlots: number): string[] {
  for (const { player } of lineup.slots) {
    const reason = kickerRoleBlockedReason(player);
    if (reason) throw new Error(`${player.name}: ${reason} Regenerate before exporting.`);
  }
  if (lineup.slots.length !== expectedSlots) throw new Error("Lineup format does not match the DraftKings roster columns.");
  if (expectedSlots === 6) assertShowdownLineup(lineup);
  else if (expectedSlots === SINGLE_STAT_ROSTER_SIZE) {
    if (lineup.slots.some((entry) => entry.slot !== "FLEX")) throw new Error("A Single Stat lineup holds three FLEX picks only.");
    if (new Set(lineup.slots.map((entry) => entry.player.dkPlayerId)).size !== SINGLE_STAT_ROSTER_SIZE) throw new Error("A Single Stat lineup repeats a player.");
  }
  else assertDstGameScript("classic", lineup.slots);
  return lineup.slots.map((entry) => {
    const id = entry.slot === "CPT" ? entry.player.captainDkPlayerId : entry.player.dkPlayerId;
    return `${entry.player.name} (${id})`;
  });
}

/** A contest name DraftKings caps at one entry per user ("Single Entry", "[Single Entry]", "SE"). */
export function isSingleEntryContest(name: string): boolean {
  return /single[\s-]*entry|\bse\b/i.test(name);
}

export type NflDkEntryContest = { key: string; name: string; rows: number; singleEntry: boolean };

/** The shape of a DraftKings entry file, as export and pre-export QA read it. */
export type NflDkEntryFileShape = {
  /** Every entry row in the file. */
  rows: number;
  /** Rows in single-entry contests. They all get the run's top lineup and use no lineup of their own. */
  singleEntryRows: number;
  /** Rows in multi-entry contests: each needs its own lineup. */
  multiEntryRows: number;
  /** Multi-entry rows holding no roster yet; one left without a lineup would be a paid entry with no lineup. */
  emptyMultiEntryRows: number;
  contests: NflDkEntryContest[];
};

function entryLayout(content: string) {
  const file = readEntryFile(content);
  const nameCol = file.header.findIndex((cell) => normalized(cell) === "CONTEST NAME");
  const idCol = file.header.findIndex((cell) => normalized(cell) === "CONTEST ID");
  const rows = file.entryRows.map((row, index) => {
    const name = nameCol >= 0 ? (row[nameCol] ?? "").trim() : "";
    const id = idCol >= 0 ? (row[idCol] ?? "").trim() : "";
    const empty = row.slice(file.firstSlot, file.firstSlot + file.expectedSlots).every((cell) => !cell?.trim());
    return { index, key: id || name || "(one contest)", name, singleEntry: Boolean(name) && isSingleEntryContest(name), empty };
  });
  return { ...file, rows };
}

/** How the entry file is laid out: its contests, single-entry rows and rows still empty. */
export function describeNflDkEntryFile(content: string): NflDkEntryFileShape {
  const { rows } = entryLayout(content);
  const contests = new Map<string, NflDkEntryContest>();
  for (const row of rows) {
    const contest = contests.get(row.key) ?? { key: row.key, name: row.name, rows: 0, singleEntry: row.singleEntry };
    contest.rows += 1;
    contests.set(row.key, contest);
  }
  const multi = rows.filter((row) => !row.singleEntry);
  return {
    rows: rows.length,
    singleEntryRows: rows.length - multi.length,
    multiEntryRows: multi.length,
    emptyMultiEntryRows: multi.filter((row) => row.empty).length,
    contests: [...contests.values()],
  };
}

/**
 * Order the lineups so any stretch of them holds a proportional mix of the
 * run's lineup types. The optimizer returns lineups grouped by archetype
 * (every "standard ceiling" build first), and export used to fill rows in
 * that order, so the first contest of a multi-contest entry file got one
 * block of near-identical builds. Deterministic: same lineups, same order.
 */
function interleaveByArchetype(lineups: NflGeneratedLineup[]): NflGeneratedLineup[] {
  const groups = new Map<string, NflGeneratedLineup[]>();
  for (const lineup of lineups) {
    const key = lineup.archetype?.id ?? "none";
    groups.set(key, [...(groups.get(key) ?? []), lineup]);
  }
  return [...groups.values()]
    .flatMap((group, groupIndex) => group.map((lineup, i) => ({ lineup, position: (i + 0.5) / group.length, groupIndex })))
    .sort((a, b) => a.position - b.position || a.groupIndex - b.groupIndex)
    .map((entry) => entry.lineup);
}

/** The run's strongest consensus lineup: most projected points, then highest estimated ownership. */
export function topNflLineup(lineups: NflGeneratedLineup[]): NflGeneratedLineup | null {
  return [...lineups].sort((a, b) =>
    b.projectedFpts - a.projectedFpts
    || (b.projectedOwnership ?? 0) - (a.projectedOwnership ?? 0)
    || a.lineupNumber - b.lineupNumber)[0] ?? null;
}

/** The message pre-export QA and the export show when the lineups do not cover the file. */
export function entryCoverageProblem(shape: NflDkEntryFileShape, built: number): string | null {
  const se = shape.singleEntryRows
    ? ` (${shape.singleEntryRows} single-entry entr${shape.singleEntryRows === 1 ? "y takes" : "ies take"} your top lineup and ${shape.singleEntryRows === 1 ? "needs" : "need"} none of its own)`
    : "";
  if (built > shape.multiEntryRows) {
    return `The entry file has room for ${shape.multiEntryRows} lineup${shape.multiEntryRows === 1 ? "" : "s"}${se} but there are ${built}. Download an entry file with more entries, or build ${shape.multiEntryRows}.`;
  }
  if (built < shape.emptyMultiEntryRows) {
    return `The entry file has ${shape.multiEntryRows} entr${shape.multiEntryRows === 1 ? "y" : "ies"} for ${built} lineup${built === 1 ? "" : "s"}${se}. Build ${shape.multiEntryRows} lineups, or remove entries from the file.`;
  }
  if (shape.singleEntryRows && !built) return "There is no lineup to put in the single-entry contests.";
  return null;
}

export type NflDkEntryPlan = { csv: string; note: string };

/**
 * Write lineups into a DraftKings entry file.
 * - A single-entry contest gets the run's top lineup (the same lineup may go into several such contests).
 * - Every other row gets its own lineup, dealt so each contest holds a mix of the run's lineup types.
 * - A count mismatch does not stop the file: it is written with what there is, and the note says what was left over.
 */
export function planNflDkEntries(content: string, lineups: NflGeneratedLineup[]): NflDkEntryPlan {
  const { rawLines, headerIndex, header, firstSlot, expectedSlots, entryRows, rows } = entryLayout(content);
  const problem = entryCoverageProblem(describeNflDkEntryFile(content), lineups.length);
  // Not a stop (owner, 2026-11-15): the file is written with what there is, and the note says what was left over.
  const multi = rows.filter((row) => !row.singleEntry);
  const single = rows.filter((row) => row.singleEntry);
  // Empty rows first, so a row left over is one that keeps the lineup DraftKings already has.
  const targets = [...multi].sort((a, b) => Number(b.empty) - Number(a.empty) || a.index - b.index).slice(0, lineups.length);
  const byContest = new Map<string, typeof targets>();
  for (const row of targets) byContest.set(row.key, [...(byContest.get(row.key) ?? []), row]);
  for (const list of byContest.values()) list.sort((a, b) => a.index - b.index);
  const filled = new Map<string, number>();
  const assigned = new Map<number, NflGeneratedLineup>();
  for (const lineup of interleaveByArchetype(lineups)) {
    // The next lineup goes to the contest furthest behind its share; larger contest first on a tie.
    let best: string | null = null;
    let bestShare = Infinity;
    let bestSize = -1;
    for (const [key, list] of byContest) {
      const done = filled.get(key) ?? 0;
      if (done >= list.length) continue;
      const share = done / list.length;
      if (share < bestShare - 1e-12 || (Math.abs(share - bestShare) <= 1e-12 && list.length > bestSize)) {
        best = key; bestShare = share; bestSize = list.length;
      }
    }
    if (best == null) break; // more lineups than entries: the rest are not written
    const done = filled.get(best) ?? 0;
    assigned.set(byContest.get(best)![done].index, lineup);
    filled.set(best, done + 1);
  }
  const top = single.length ? topNflLineup(lineups) : null;
  if (top) for (const row of single) assigned.set(row.index, top);
  const output = rawLines.slice(0, headerIndex).concat(stringifyCsvLine(header));
  for (let index = 0; index < entryRows.length; index++) {
    const row = entryRows[index];
    const lineup = assigned.get(index);
    if (lineup) exportCells(lineup, expectedSlots).forEach((cell, slotIndex) => { row[firstSlot + slotIndex] = cell; });
    output.push(stringifyCsvLine(row));
  }
  const note = entryAssignmentNote(describeNflDkEntryFile(content), lineups);
  const leftOver = lineups.length > multi.length ? "the extra lineups were left out" : "the entries without a lineup keep whatever DraftKings already has";
  return { csv: `${output.join("\r\n")}\r\n`, note: problem ? `${problem} The file was written anyway: ${leftOver}.${note ? ` ${note}` : ""}` : note };
}

/** Plain words on how the export spreads the lineups over the file's contests. */
export function entryAssignmentNote(shape: NflDkEntryFileShape, lineups: NflGeneratedLineup[]): string {
  const multiContests = shape.contests.filter((c) => !c.singleEntry).length;
  const seContests = shape.contests.filter((c) => c.singleEntry).length;
  const types = new Set(lineups.map((l) => l.archetype?.id ?? "none")).size;
  const parts: string[] = [];
  if (multiContests > 1) parts.push(types > 1 ? `Each of the ${multiContests} contests gets a mix of the ${types} lineup types` : `Lineups are spread over ${multiContests} contests`);
  else if (multiContests === 1 && lineups.length) parts.push(`${lineups.length} lineup${lineups.length === 1 ? "" : "s"} go into 1 contest`);
  const top = topNflLineup(lineups);
  if (seContests && top) parts.push(`${seContests} single-entry contest${seContests === 1 ? "" : "s"} get${seContests === 1 ? "s" : ""} your top lineup (#${top.lineupNumber}, ${top.projectedFpts.toFixed(1)} projected)`);
  return parts.length ? `${parts.join("; ")}.` : "";
}

export function exportNflDkEntries(content: string, lineups: NflGeneratedLineup[]): string {
  return planNflDkEntries(content, lineups).csv;
}

/** DraftKings' roster-column headers: the format its Upload Lineups box accepts without entry IDs. */
export const NFL_DK_LINEUP_HEADERS: Record<NflSlateFormat, readonly string[]> = {
  classic: ["QB", "RB", "RB", "WR", "WR", "WR", "TE", "FLEX", "DST"],
  showdown: ["CPT", "FLEX", "FLEX", "FLEX", "FLEX", "FLEX"],
  // Not yet verified against a real DraftKings Single Stat entry file: the
  // slot names come from the salary export's `Roster Position` column.
  single_stat_yards: ["FLEX", "FLEX", "FLEX"],
  single_stat_touchdowns: ["FLEX", "FLEX", "FLEX"],
};

/**
 * A lineups-only CSV (no entry IDs): one roster per row under DraftKings'
 * slot headers, each cell `Name (id)`. For DraftKings' Upload Lineups box,
 * or as a plain record of the run; the entry-template export above is the
 * one that edits existing entries.
 */
export function exportNflDkLineups(format: NflSlateFormat, lineups: NflGeneratedLineup[]): string {
  if (!lineups.length) throw new Error("There are no lineups to export.");
  const header = NFL_DK_LINEUP_HEADERS[format];
  if (!header) throw new Error(`Unknown NFL format: ${format}`);
  const output = [stringifyCsvLine([...header])];
  for (const lineup of lineups) output.push(stringifyCsvLine(exportCells(lineup, header.length)));
  return `${output.join("\r\n")}\r\n`;
}
