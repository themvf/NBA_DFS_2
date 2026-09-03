import { parseCsvLine } from "@/app/dfs/csv";

export type NflComparisonCsvRow = {
  name: string;
  team: string | null;
  projection: number | null;
  ownership: number | null;
};

export type NflComparisonCsv = { rows: NflComparisonCsvRow[]; warnings: string[] };

const NAME_HEADERS = ["name", "player", "playername"];
const TEAM_HEADERS = ["team", "teamabbrev", "teamabbr", "tm"];
const PROJECTION_HEADERS = ["projection", "proj", "projectedpoints", "fantasypoints", "fpts", "projfpts"];
const OWNERSHIP_HEADERS = ["ownership", "own", "own%", "projown", "projown%", "projectedownership", "pown%"];

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
    if (projection == null && ownership == null) {
      warnings.push(`Row ${index + 1} (${name}) has no numeric projection or ownership and was skipped.`);
      continue;
    }
    rows.push({ name, team: teamColumn >= 0 ? cells[teamColumn]?.trim().toUpperCase() || null : null, projection, ownership });
  }
  if (!rows.length) throw new Error("No usable comparison rows were found.");
  return { rows, warnings };
}
