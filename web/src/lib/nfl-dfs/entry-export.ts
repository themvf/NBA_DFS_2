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
 * How many entries the DraftKings file holds. Export fills the first N and
 * leaves the rest untouched, so pre-export QA compares this with the lineups.
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

export function exportNflDkEntries(content: string, lineups: NflGeneratedLineup[]): string {
  const { rawLines, headerIndex, header, firstSlot, expectedSlots, entryRows } = readEntryFile(content);
  if (lineups.length > entryRows.length) {
    throw new Error(`Generated ${lineups.length} lineups but the entry file has only ${entryRows.length} entries.`);
  }
  const output = rawLines.slice(0, headerIndex).concat(stringifyCsvLine(header));
  for (let index = 0; index < entryRows.length; index++) {
    const row = entryRows[index];
    const lineup = lineups[index];
    if (lineup) exportCells(lineup, expectedSlots).forEach((cell, slotIndex) => { row[firstSlot + slotIndex] = cell; });
    output.push(stringifyCsvLine(row));
  }
  return `${output.join("\r\n")}\r\n`;
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
