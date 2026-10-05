import { parseCsvLine } from "@/app/dfs/csv";

export type NflComparisonCsvRow = {
  name: string;
  team: string | null;
  projection: number | null;
  ownership: number | null;
  format: "classic" | "showdown" | null;
};

export type NflComparisonCsv = { rows: NflComparisonCsvRow[]; warnings: string[] };

export function assertComparisonOwnershipFormat(
  rows: readonly { ownership?: number | null; format?: "classic" | "showdown" | null }[],
  slateFormat: string,
): void {
  if (slateFormat !== "classic" && slateFormat !== "showdown") throw new Error("Unknown saved slate format.");
  for (const row of rows) {
    if (row.ownership == null || !Number.isFinite(row.ownership)) continue;
    if (!row.format) throw new Error("Ownership import needs a Format column with Classic or Showdown. No rows were changed.");
    if (row.format !== slateFormat)
      throw new Error(`This is ${row.format} ownership, but the saved slate is ${slateFormat}. No rows were changed.`);
  }
}

/** Older imports without a recorded format cannot influence a new build. */
export function verifiedComparisonOwnership(evidence: unknown, slateFormat: string): boolean {
  if (slateFormat !== "classic" && slateFormat !== "showdown") return false;
  if (!evidence || typeof evidence !== "object") return false;
  const lineStar = (evidence as Record<string, unknown>).linestar;
  return !!lineStar && typeof lineStar === "object"
    && (lineStar as Record<string, unknown>).format === slateFormat;
}

const NAME_HEADERS = ["name", "player", "playername"];
const TEAM_HEADERS = ["team", "teamabbrev", "teamabbr", "tm"];
const PROJECTION_HEADERS = ["projection", "proj", "projectedpoints", "fantasypoints", "fpts", "projfpts"];
const OWNERSHIP_HEADERS = ["ownership", "own", "own%", "projown", "projown%", "projectedownership", "pown%"];
const FORMAT_HEADERS = ["format", "slateformat", "contestformat"];

function normalizedHeader(value: string): string {
  return value.replace(/^\uFEFF/, "").trim().toLowerCase().replace(/[ _-]+/g, "");
}

function findColumn(headers: string[], candidates: string[]): number {
  return headers.findIndex((header) => candidates.includes(header));
}

function numberCell(raw: string | undefined, percent = false): number | null {
  if (!raw?.trim()) return null;
  const value = Number.parseFloat(raw.replace(/[$,%]/g, "").trim());
  if (!Number.isFinite(value)) return null;
  return percent && value > 0 && value <= 1 && !raw.includes("%") ? value * 100 : value;
}

export function parseNflComparisonCsv(content: string): NflComparisonCsv {
  const lines = content.split(/\r?\n/).filter((line) => line.trim());
  if (!lines.length) throw new Error("The comparison file is empty.");
  const rawHeaders = parseCsvLine(lines[0]);
  const headers = rawHeaders.map(normalizedHeader);
  const nameColumn = findColumn(headers, NAME_HEADERS);
  const teamColumn = findColumn(headers, TEAM_HEADERS);
  const projectionColumn = findColumn(headers, PROJECTION_HEADERS);
  const ownershipColumn = findColumn(headers, OWNERSHIP_HEADERS);
  const formatColumn = findColumn(headers, FORMAT_HEADERS);
  if (nameColumn < 0) throw new Error("Comparison CSV needs a Name or Player column.");
  if (projectionColumn < 0 && ownershipColumn < 0) {
    throw new Error("Comparison CSV needs a projection and/or ownership column.");
  }
  const warnings: string[] = [];
  const rows: NflComparisonCsvRow[] = [];
  for (let index = 1; index < lines.length; index++) {
    const cells = parseCsvLine(lines[index]);
    const name = cells[nameColumn]?.trim() ?? "";
    if (!name) continue;
    const projection = projectionColumn >= 0 ? numberCell(cells[projectionColumn]) : null;
    const ownership = ownershipColumn >= 0 ? numberCell(cells[ownershipColumn], true) : null;
    const rawFormat = formatColumn >= 0 ? cells[formatColumn]?.trim().toLowerCase() : null;
    const format = rawFormat === "classic" || rawFormat === "showdown" ? rawFormat : null;
    if (ownership !== null && !format) throw new Error(`Row ${index + 1} (${name}) needs Format = Classic or Showdown before ownership can be imported.`);
    if (projection == null && ownership == null) {
      warnings.push(`Row ${index + 1} (${name}) has no numeric projection or ownership and was skipped.`);
      continue;
    }
    rows.push({ name, team: teamColumn >= 0 ? cells[teamColumn]?.trim().toUpperCase() || null : null, projection, ownership, format });
  }
  if (!rows.length) throw new Error("No usable comparison rows were found.");
  return { rows, warnings };
}
