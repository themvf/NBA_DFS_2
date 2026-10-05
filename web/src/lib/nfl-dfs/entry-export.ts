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

export function exportNflDkEntries(content: string, lineups: NflGeneratedLineup[]): string {
  const { rawLines, headerIndex, header, firstSlot, expectedSlots, entryRows } = readEntryFile(content);
  if (lineups.length > entryRows.length) {
    throw new Error(`Generated ${lineups.length} lineups but the entry file has only ${entryRows.length} entries.`);
  }
  const output = rawLines.slice(0, headerIndex).concat(stringifyCsvLine(header));
  for (let index = 0; index < entryRows.length; index++) {
    const row = entryRows[index];
    const lineup = lineups[index];
    if (lineup) {
      for (const { player } of lineup.slots) {
        const reason = kickerRoleBlockedReason(player);
        if (reason) throw new Error(`${player.name}: ${reason} Regenerate before exporting.`);
      }
      if (lineup.slots.length !== expectedSlots) throw new Error("Lineup format does not match the DraftKings entry file.");
      if (expectedSlots === 6) assertShowdownLineup(lineup);
      else assertDstGameScript("classic", lineup.slots);
      lineup.slots.forEach((entry, slotIndex) => {
        const id = entry.slot === "CPT" ? entry.player.captainDkPlayerId : entry.player.dkPlayerId;
        row[firstSlot + slotIndex] = `${entry.player.name} (${id})`;
      });
    }
    output.push(stringifyCsvLine(row));
  }
  return `${output.join("\r\n")}\r\n`;
}
