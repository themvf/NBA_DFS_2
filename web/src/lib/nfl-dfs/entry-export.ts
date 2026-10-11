import { parseCsvLine, stringifyCsvLine } from "@/app/dfs/csv";
import type { NflGeneratedLineup } from "@/app/dfs/nfl/nfl-optimizer";
import { assertDstGameScript, assertShowdownLineup } from "./showdown-legality";
import { kickerRoleBlockedReason } from './availability';

function normalized(value: string): string {
  return value.replace(/^\uFEFF/, "").trim().toUpperCase();
}

function readEntryFile(content: string) {
  const rawLines = content.split(/\r?\n/).filter((line) => line.trim());
  const headerIndex = rawLines.findIndex((line) => parseCsvLine(line).some((cell) => normalized(cell) === "ENTRY ID"));
  if (headerIndex < 0) throw new Error("Could not find the DraftKings Entry ID header.");
  const header = parseCsvLine(rawLines[headerIndex]);
  const firstSlot = header.findIndex((cell) => ["QB", "CPT"].includes(normalized(cell)));
  if (firstSlot < 0) throw new Error("Could not find NFL roster columns in the DraftKings entry file.");
  const expectedSlots = normalized(header[firstSlot]) === "CPT" ? 6 : 9;
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
export const NFL_DK_LINEUP_HEADERS = {
  classic: ["QB", "RB", "RB", "WR", "WR", "WR", "TE", "FLEX", "DST"],
  showdown: ["CPT", "FLEX", "FLEX", "FLEX", "FLEX", "FLEX"],
} as const;

/**
 * A lineups-only CSV (no entry IDs): one roster per row under DraftKings'
 * slot headers, each cell `Name (id)`. For DraftKings' Upload Lineups box,
 * or as a plain record of the run; the entry-template export above is the
 * one that edits existing entries.
 */
export function exportNflDkLineups(format: "classic" | "showdown", lineups: NflGeneratedLineup[]): string {
  if (!lineups.length) throw new Error("There are no lineups to export.");
  const header = NFL_DK_LINEUP_HEADERS[format];
  const output = [stringifyCsvLine([...header])];
  for (const lineup of lineups) output.push(stringifyCsvLine(exportCells(lineup, header.length)));
  return `${output.join("\r\n")}\r\n`;
}
