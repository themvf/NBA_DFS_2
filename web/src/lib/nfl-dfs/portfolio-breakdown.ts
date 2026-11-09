/**
 * How a built NFL portfolio is shaped, per lineup and in total: who captains,
 * the team split (Showdown), the stack (Classic), quarterbacks and kickers.
 * Pure; the lineup review renders it and filters lineups by it.
 *
 * Showdown split reference (winners-shape v10 note, 12 stored 2026 flagship
 * Showdowns): the top-100 lineups split 3-3 / 4-2 / 5-1 as 50% / 35% / 15%
 * against a field of 33% / 47% / 19%. Shown beside the build for comparison,
 * never enforced.
 */

export interface BreakdownPlayer {
  dkPlayerId: number;
  name: string;
  team: string;
  position: string;
  ownPct?: number | null;
  captainOwnPct?: number | null;
}

export interface BreakdownLineup {
  lineupNumber: number;
  slots: { slot: string; player: BreakdownPlayer }[];
  stackSummary?: { quarterback: string | null; passCatchers: string[]; bringBack: string | null };
}

export type PortfolioFormat = "classic" | "showdown";

export const SHOWDOWN_SPLIT_REFERENCE: Readonly<Record<string, { winners: number; field: number }>> = {
  "3-3": { winners: 50, field: 33 },
  "4-2": { winners: 35, field: 47 },
  "5-1": { winners: 15, field: 19 },
};

export interface LineupShape {
  captainId: number | null;
  /** Showdown: larger side first with its team, e.g. "BUF 4-2"; "3-3" for an even split. Classic: the stack, e.g. "QB+2 + bring-back". */
  split: string;
  /** Showdown: "3-3" / "4-2" / "5-1"; Classic: same as `split`. */
  splitKind: string;
  quarterbacks: number;
  kicker: boolean;
  dst: boolean;
  playerIds: number[];
}

export function lineupShape(lineup: BreakdownLineup, format: PortfolioFormat): LineupShape {
  const captain = lineup.slots.find((s) => s.slot === "CPT");
  const positions = lineup.slots.map((s) => s.player.position);
  const base = {
    captainId: captain?.player.dkPlayerId ?? null,
    quarterbacks: positions.filter((p) => p === "QB").length,
    kicker: positions.includes("K"),
    dst: positions.includes("DST"),
    playerIds: lineup.slots.map((s) => s.player.dkPlayerId),
  };
  if (format === "showdown") {
    const teams = new Map<string, number>();
    for (const s of lineup.slots) teams.set(s.player.team, (teams.get(s.player.team) ?? 0) + 1);
    const sides = [...teams].sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]));
    const big = sides[0]?.[1] ?? 0, small = sides[1]?.[1] ?? 0;
    const splitKind = `${big}-${small}`;
    return { ...base, splitKind, split: big === small ? splitKind : `${sides[0][0]} ${splitKind}` };
  }
  const stack = lineup.stackSummary;
  const label = !stack?.quarterback ? "No QB stack"
    : `QB+${stack.passCatchers.length}${stack.bringBack ? " + bring-back" : ""}`;
  return { ...base, split: label, splitKind: label };
}

export interface CountRow { key: string; label: string; count: number; pct: number }
export interface CaptainRow extends CountRow { dkPlayerId: number; team: string; position: string; fieldPct: number | null }

export interface PortfolioBreakdown {
  total: number;
  format: PortfolioFormat;
  captains: CaptainRow[];
  splits: CountRow[];
  splitKinds: CountRow[];
  quarterbacks: CountRow[];
  kicker: number;
  dst: number;
  teamSlots: CountRow[];
}

const pct = (n: number, total: number) => (total ? (100 * n) / total : 0);

function rows(counts: Map<string, number>, total: number, order?: string[]): CountRow[] {
  const list = [...counts].map(([key, count]) => ({ key, label: key, count, pct: pct(count, total) }));
  return order
    ? list.sort((a, b) => (order.indexOf(a.key) + 1 || 99) - (order.indexOf(b.key) + 1 || 99) || b.count - a.count)
    : list.sort((a, b) => b.count - a.count || a.key.localeCompare(b.key));
}

/** `field`: current slate players by DraftKings id, for captain ownership when the lineup's own player copy lacks it (a restored run). */
export function breakdownPortfolio(lineups: readonly BreakdownLineup[], format: PortfolioFormat,
  field?: ReadonlyMap<number, Pick<BreakdownPlayer, "captainOwnPct">>): PortfolioBreakdown {
  const total = lineups.length;
  const captainCount = new Map<number, { player: BreakdownPlayer; count: number }>();
  const split = new Map<string, number>(), kind = new Map<string, number>(), qb = new Map<string, number>(), team = new Map<string, number>();
  let kicker = 0, dst = 0, slots = 0;
  for (const lineup of lineups) {
    const shape = lineupShape(lineup, format);
    const captain = lineup.slots.find((s) => s.slot === "CPT")?.player;
    if (captain) {
      const entry = captainCount.get(captain.dkPlayerId) ?? { player: captain, count: 0 };
      entry.count += 1;
      captainCount.set(captain.dkPlayerId, entry);
    }
    split.set(shape.split, (split.get(shape.split) ?? 0) + 1);
    kind.set(shape.splitKind, (kind.get(shape.splitKind) ?? 0) + 1);
    const q = shape.quarterbacks >= 2 ? "Both QBs" : shape.quarterbacks === 1 ? "One QB" : "No QB";
    qb.set(q, (qb.get(q) ?? 0) + 1);
    if (shape.kicker) kicker += 1;
    if (shape.dst) dst += 1;
    for (const s of lineup.slots) { team.set(s.player.team, (team.get(s.player.team) ?? 0) + 1); slots += 1; }
  }
  const captains: CaptainRow[] = [...captainCount.values()]
    .map(({ player, count }) => ({
      key: String(player.dkPlayerId), label: player.name, dkPlayerId: player.dkPlayerId, team: player.team, position: player.position,
      count, pct: pct(count, total), fieldPct: player.captainOwnPct ?? field?.get(player.dkPlayerId)?.captainOwnPct ?? null,
    }))
    .sort((a, b) => b.count - a.count || a.label.localeCompare(b.label));
  return {
    total, format, captains,
    splits: rows(split, total),
    splitKinds: rows(kind, total, format === "showdown" ? ["3-3", "4-2", "5-1", "6-0"] : undefined),
    quarterbacks: rows(qb, total, ["Both QBs", "One QB", "No QB"]),
    kicker, dst,
    teamSlots: rows(team, slots),
  };
}

export interface LineupFilter {
  captainId?: number | null;
  split?: string | null;
  splitKind?: string | null;
  quarterbacks?: "Both QBs" | "One QB" | "No QB" | null;
  kicker?: boolean | null;
  includes?: number | null;
  excludes?: number | null;
}

export function matchesFilter(shape: LineupShape, filter: LineupFilter): boolean {
  if (filter.captainId != null && shape.captainId !== filter.captainId) return false;
  if (filter.split && shape.split !== filter.split) return false;
  if (filter.splitKind && shape.splitKind !== filter.splitKind) return false;
  if (filter.quarterbacks) {
    const q = shape.quarterbacks >= 2 ? "Both QBs" : shape.quarterbacks === 1 ? "One QB" : "No QB";
    if (q !== filter.quarterbacks) return false;
  }
  if (filter.kicker != null && shape.kicker !== filter.kicker) return false;
  if (filter.includes != null && !shape.playerIds.includes(filter.includes)) return false;
  if (filter.excludes != null && shape.playerIds.includes(filter.excludes)) return false;
  return true;
}

export const isFilterActive = (filter: LineupFilter) => Object.values(filter).some((v) => v != null && v !== "");
