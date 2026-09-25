/**
 * Fill a DraftKings CFB Classic entries template with generated lineups.
 *
 * The college twin of the NFL entry export (lib/nfl-dfs/entry-export.ts), kept
 * separate on purpose. DraftKings' "Edit entries" file supplies the Entry IDs;
 * each generated lineup fills one entry row's eight roster columns as
 * "Name (ID)", in DraftKings' slot order. Every other cell is left exactly as
 * DraftKings wrote it. A template for any other game type is refused.
 */
import { joinCsvLine, splitCsvLine } from "./salary-csv";
import { CFB_SLOTS, cfbLineupProblems, type CfbLineup } from "./settings";

const norm = (value: string) => value.replace(/^\uFEFF/, "").trim().toUpperCase().replace(/\s+/g, " ");
/** DraftKings has written SUPER FLEX a few ways; all mean the same slot. */
const SLOT_ALIASES: Record<string, string> = { "S-FLEX": "S-FLEX", "SFLEX": "S-FLEX", "SUPER FLEX": "S-FLEX", "SUPERFLEX": "S-FLEX" };
const canonical = (cell: string) => SLOT_ALIASES[norm(cell)] ?? norm(cell);

export interface CfbEntryExport { csv: string; filled: number; entries: number }

export function exportCfbDkEntries(content: string, lineups: readonly CfbLineup[]): CfbEntryExport {
  const lines = content.split(/\r?\n/).filter((line) => line.trim());
  const headerIndex = lines.findIndex((line) => splitCsvLine(line).some((cell) => norm(cell) === "ENTRY ID"));
  if (headerIndex < 0) throw new Error("Could not find the DraftKings Entry ID header. Download the entries file from DraftKings' Edit Entries page.");
  const header = splitCsvLine(lines[headerIndex]);
  const first = header.findIndex((cell) => norm(cell) === "QB");
  const slots = first < 0 ? [] : header.slice(first, first + CFB_SLOTS.length).map(canonical);
  if (first < 0 || slots.join() !== CFB_SLOTS.join()) {
    const seen = header.filter((c) => c.trim()).slice(4, 14).join(", ");
    throw new Error(`This is not a CFB Classic entries file. Expected ${CFB_SLOTS.join(", ")}; found ${seen || "no roster columns"}.`);
  }
  const rows = lines.slice(headerIndex + 1).map(splitCsvLine).filter((row) => row[0]?.trim());
  if (!rows.length) throw new Error("The entries file has no entries. Enter the contest on DraftKings first, then download Edit Entries.");
  if (lineups.length > rows.length) {
    throw new Error(`You have ${lineups.length} lineups but the file has only ${rows.length} entries. Enter more on DraftKings or build fewer.`);
  }
  const out = [...lines.slice(0, headerIndex), joinCsvLine(header)];
  rows.forEach((row, index) => {
    const lineup = lineups[index];
    if (lineup) {
      const problems = cfbLineupProblems(lineup.slots.map((s) => s.player));
      if (problems.length) throw new Error(`Lineup ${lineup.lineupNumber} is not legal: ${problems.join("; ")}.`);
      if (lineup.slots.map((s) => s.slot).join() !== CFB_SLOTS.join()) throw new Error(`Lineup ${lineup.lineupNumber} is not in DraftKings slot order.`);
      while (row.length < first + CFB_SLOTS.length) row.push("");
      lineup.slots.forEach((s, i) => { row[first + i] = `${s.player.name} (${s.player.dkId})`; });
    }
    out.push(joinCsvLine(row));
  });
  return { csv: `${out.join("\r\n")}\r\n`, filled: Math.min(lineups.length, rows.length), entries: rows.length };
}
