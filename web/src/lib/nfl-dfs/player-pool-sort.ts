/**
 * Column sorting for the DFS Lab player pool.
 *
 * Pure: no React, no database, so the ordering rules can be tested directly
 * rather than inferred from clicking a header.
 *
 * Two rules do the real work here, and both exist because the naive sort was
 * measured against a real 13-game Classic slate (670 rows) and was actively
 * misleading.
 *
 * 1. NULLS SINK IN BOTH DIRECTIONS. A row with no number is not a small
 *    number. Letting nulls lead an ascending sort would open the pool with a
 *    wall of dashes.
 *
 * 2. A ROW THAT CARRIES NO VALUE CLAIM SINKS BELOW EVERY ROW THAT DOES.
 *    Sorting the Value column by the raw multiple put **A.J. Brown at the top
 *    at 5.36x while ruled OUT**, and 12 of the top 20 were $4,000
 *    practice-squad quarterbacks whose multiple is an artifact of the
 *    position-prior fallback (see `salary-value.ts`). Only 4 of the top 20
 *    were real. `unproven` and `unknown` therefore sink as a block, in both
 *    directions -- they are not extreme values, they are absent verdicts, the
 *    same category as a null. After sinking, the top 20 is Purdy, Howell,
 *    Stick, Lawrence, Njoku, Jaguars DST, Derrick Henry ... and the visible
 *    Value column stays monotonic across all 402 tiered rows, so a sorted
 *    column still looks sorted.
 *
 * The sunk block is itself ordered rather than arbitrary, so scrolling past
 * the break is still useful: sunk Value rows keep their multiple order, and
 * rows with no model projection keep their DK-average order.
 */
import type { ValueAssessment } from "./salary-value";

export type PoolSortKey =
  | "name" | "position" | "team" | "salary" | "value" | "ourProj"
  | "floorFpts" | "ceilingFpts" | "avgFptsDk" | "fantasyprosProj"
  | "linestarProj" | "linestarOwnPct";

export type SortDirection = "asc" | "desc";
export type PoolSort = { key: PoolSortKey; direction: SortDirection };

/**
 * `kind` picks the first click's direction: text reads naturally A-Z, while a
 * number is almost always wanted biggest-first.
 */
export const POOL_SORT_COLUMNS: readonly { key: PoolSortKey; kind: "text" | "number" }[] = [
  { key: "name", kind: "text" },
  { key: "position", kind: "text" },
  { key: "team", kind: "text" },
  { key: "salary", kind: "number" },
  { key: "value", kind: "number" },
  { key: "ourProj", kind: "number" },
  { key: "floorFpts", kind: "number" },
  { key: "ceilingFpts", kind: "number" },
  { key: "avgFptsDk", kind: "number" },
  { key: "fantasyprosProj", kind: "number" },
  { key: "linestarProj", kind: "number" },
  { key: "linestarOwnPct", kind: "number" },
];

const KIND = new Map(POOL_SORT_COLUMNS.map((c) => [c.key, c.kind]));

/** Preserves the pool's historical default: our projection, best first. */
export const DEFAULT_POOL_SORT: PoolSort = { key: "ourProj", direction: "desc" };

export function isPoolSortKey(value: string): value is PoolSortKey {
  return KIND.has(value as PoolSortKey);
}

/** Clicking the active column flips it; clicking a new one starts at its natural direction. */
export function nextPoolSort(current: PoolSort, key: PoolSortKey): PoolSort {
  if (current.key === key) {
    return { key, direction: current.direction === "asc" ? "desc" : "asc" };
  }
  return { key, direction: KIND.get(key) === "text" ? "asc" : "desc" };
}

export type SortablePoolPlayer = {
  dkPlayerId: number;
  name: string;
  position: string;
  team: string;
  salary: number;
  ourProj: number | null;
  floorFpts: number | null;
  ceilingFpts: number | null;
  avgFptsDk: number | null;
  fantasyprosProj: number | null;
  linestarProj: number | null;
  linestarOwnPct: number | null;
};

/**
 * What a row sorts as: a `group` that always sorts ascending regardless of
 * direction (so absent verdicts sink either way), and a `primary` compared in
 * the chosen direction. `secondary` orders the sunk block so it is not
 * arbitrary, and is likewise direction-independent.
 */
type SortSlot = { group: number; primary: number | string | null; secondary: number | null };

const numericSlot = (value: number | null | undefined): SortSlot =>
  value === null || value === undefined || !Number.isFinite(value)
    ? { group: 1, primary: null, secondary: null }
    : { group: 0, primary: value, secondary: null };

function slotFor(
  player: SortablePoolPlayer,
  key: PoolSortKey,
  assess: (player: SortablePoolPlayer) => ValueAssessment,
): SortSlot {
  switch (key) {
    case "name": return { group: 0, primary: player.name.toLowerCase(), secondary: null };
    case "position": return { group: 0, primary: player.position.toLowerCase(), secondary: null };
    case "team": return { group: 0, primary: player.team.toLowerCase(), secondary: null };
    case "salary": return numericSlot(player.salary);
    case "value": {
      const verdict = assess(player);
      // An absent verdict is not a low multiple. `unproven` (a position-prior
      // projection) and `unknown` (no projection, or ruled out) both sink, and
      // keep their multiple order inside the sunk block.
      const absent = verdict.tier === "unproven" || verdict.tier === "unknown";
      if (absent) return { group: 1, primary: null, secondary: verdict.multiple };
      return { group: 0, primary: verdict.multiple, secondary: null };
    }
    case "ourProj": {
      // The 29 rows with no model projection all carry a DK average; order the
      // sunk block by it so the tail of the default view stays meaningful.
      if (player.ourProj === null || !Number.isFinite(player.ourProj)) {
        return { group: 1, primary: null, secondary: player.avgFptsDk };
      }
      return { group: 0, primary: player.ourProj, secondary: null };
    }
    case "floorFpts": return numericSlot(player.floorFpts);
    case "ceilingFpts": return numericSlot(player.ceilingFpts);
    case "avgFptsDk": return numericSlot(player.avgFptsDk);
    case "fantasyprosProj": return numericSlot(player.fantasyprosProj);
    case "linestarProj": return numericSlot(player.linestarProj);
    case "linestarOwnPct": return numericSlot(player.linestarOwnPct);
  }
}

/** Descending on a possibly-null number, with null last. */
function compareSecondary(a: number | null, b: number | null): number {
  if (a === null && b === null) return 0;
  if (a === null) return 1;
  if (b === null) return -1;
  return b - a;
}

export function sortPlayerPool<T extends SortablePoolPlayer>(
  rows: readonly T[],
  sort: PoolSort,
  assess: (player: SortablePoolPlayer) => ValueAssessment,
): T[] {
  const sign = sort.direction === "asc" ? 1 : -1;
  // Decorate once: `assess` and `toLowerCase` are not free across 670 rows.
  const decorated = rows.map((row) => ({ row, slot: slotFor(row, sort.key, assess) }));

  decorated.sort((a, b) => {
    if (a.slot.group !== b.slot.group) return a.slot.group - b.slot.group;

    const x = a.slot.primary;
    const y = b.slot.primary;
    if (x !== null && y !== null) {
      const cmp = typeof x === "string" && typeof y === "string"
        ? x.localeCompare(y)
        : (x as number) - (y as number);
      if (cmp !== 0) return sign * cmp;
    } else if (x !== y) {
      // Within a group one side can still be null (a sunk row with no number
      // at all). Direction must not float it to the top.
      return x === null ? 1 : -1;
    }

    const secondary = compareSecondary(a.slot.secondary, b.slot.secondary);
    if (secondary !== 0) return secondary;

    // Deterministic: the same slate and sort always render the same order.
    return a.row.dkPlayerId - b.row.dkPlayerId;
  });

  return decorated.map((d) => d.row);
}
