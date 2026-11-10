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

// ---------------------------------------------------------------------------
// Pairs: which players the portfolio puts together.

export interface PairPlayer { dkPlayerId: number; name: string; team: string; position: string }

export interface PairRow {
  key: string;
  a: PairPlayer;
  b: PairPlayer;
  count: number;
  pct: number;
  /** Share expected if the two exposures were independent: pctA x pctB / 100. */
  independentPct: number;
}

export interface PairBreakdown {
  /** Any two players in the same lineup, either slot. */
  together: PairRow[];
  /** Showdown only: captain `a` with flex `b`. */
  captainWith: PairRow[];
}

type PairAcc = Map<string, { a: BreakdownPlayer; b: BreakdownPlayer; count: number }>;
const brief = (p: BreakdownPlayer): PairPlayer => ({ dkPlayerId: p.dkPlayerId, name: p.name, team: p.team, position: p.position });
const bump = (map: Map<number, number>, id: number) => map.set(id, (map.get(id) ?? 0) + 1);

/** The most common pairs, at least `minCount` lineups each, most common first; `limit` per list. */
export function pairBreakdown(lineups: readonly BreakdownLineup[], minCount = 2, limit = 40): PairBreakdown {
  const total = lineups.length;
  const exposure = new Map<number, number>(), captained = new Map<number, number>(), flex = new Map<number, number>();
  const together: PairAcc = new Map(), withCaptain: PairAcc = new Map();
  const add = (map: PairAcc, key: string, a: BreakdownPlayer, b: BreakdownPlayer) => {
    const row = map.get(key) ?? { a, b, count: 0 }; row.count += 1; map.set(key, row);
  };
  for (const lineup of lineups) {
    const ordered = lineup.slots.map((s) => s.player).sort((x, y) => x.dkPlayerId - y.dkPlayerId);
    for (const p of ordered) bump(exposure, p.dkPlayerId);
    for (let i = 0; i < ordered.length; i++) for (let j = i + 1; j < ordered.length; j++)
      add(together, `${ordered[i].dkPlayerId}|${ordered[j].dkPlayerId}`, ordered[i], ordered[j]);
    const captain = lineup.slots.find((s) => s.slot === "CPT")?.player;
    if (!captain) continue;
    bump(captained, captain.dkPlayerId);
    for (const s of lineup.slots) if (s.slot !== "CPT") {
      bump(flex, s.player.dkPlayerId);
      add(withCaptain, `${captain.dkPlayerId}>${s.player.dkPlayerId}`, captain, s.player);
    }
  }
  const share = (n: number) => pct(n, total);
  const finish = (map: PairAcc, aCounts: Map<number, number>, bCounts: Map<number, number>): PairRow[] =>
    [...map].filter(([, r]) => r.count >= minCount)
      .map(([key, r]) => ({ key, a: brief(r.a), b: brief(r.b), count: r.count, pct: share(r.count),
        independentPct: (share(aCounts.get(r.a.dkPlayerId) ?? 0) * share(bCounts.get(r.b.dkPlayerId) ?? 0)) / 100 }))
      .sort((x, y) => y.count - x.count || x.key.localeCompare(y.key))
      .slice(0, limit);
  return { together: finish(together, exposure, exposure), captainWith: finish(withCaptain, captained, flex) };
}

// ---------------------------------------------------------------------------
// Two portfolios side by side.

export interface ExposureChange extends PairPlayer {
  pctA: number; pctB: number; captainPctA: number; captainPctB: number;
}

export interface PortfolioComparison {
  a: PortfolioBreakdown;
  b: PortfolioBreakdown;
  players: ExposureChange[];
  /** Lineups of B whose roster also appears in A. */
  sharedLineups: number;
}

/** Same roster, slot for slot (a Showdown captain differs from the same player at flex). */
export function rosterKey(lineup: Pick<BreakdownLineup, "slots">): string {
  return lineup.slots.map((s) => `${s.slot === "CPT" ? "C" : "F"}${s.player.dkPlayerId}`).sort().join(",");
}

function exposureCounts(lineups: readonly BreakdownLineup[]) {
  const counts = new Map<number, { player: BreakdownPlayer; count: number; captain: number }>();
  for (const l of lineups) for (const s of l.slots) {
    const row = counts.get(s.player.dkPlayerId) ?? { player: s.player, count: 0, captain: 0 };
    row.count += 1; if (s.slot === "CPT") row.captain += 1;
    counts.set(s.player.dkPlayerId, row);
  }
  return counts;
}

/** Players sorted by the size of their exposure change from A to B, largest first. */
export function comparePortfolios(a: readonly BreakdownLineup[], b: readonly BreakdownLineup[], format: PortfolioFormat,
  field?: ReadonlyMap<number, Pick<BreakdownPlayer, "captainOwnPct">>): PortfolioComparison {
  const ca = exposureCounts(a), cb = exposureCounts(b);
  const players = [...new Set([...ca.keys(), ...cb.keys()])].map((id) => {
    const ra = ca.get(id), rb = cb.get(id), p = (ra ?? rb)!.player;
    return { ...brief(p), pctA: pct(ra?.count ?? 0, a.length), pctB: pct(rb?.count ?? 0, b.length),
      captainPctA: pct(ra?.captain ?? 0, a.length), captainPctB: pct(rb?.captain ?? 0, b.length) };
  }).sort((x, y) => Math.abs(y.pctB - y.pctA) - Math.abs(x.pctB - x.pctA) || y.pctB - x.pctB || x.name.localeCompare(y.name));
  const keysA = new Set(a.map(rosterKey));
  return { a: breakdownPortfolio(a, format, field), b: breakdownPortfolio(b, format, field), players,
    sharedLineups: b.filter((l) => keysA.has(rosterKey(l))).length };
}

// ---------------------------------------------------------------------------
// After the games: how each group of lineups scored.

export interface ShapeScoreRow {
  key: string;
  label: string;
  lineups: number;
  scored: number;
  average: number | null;
  best: number | null;
  /** Lineups that beat the field median; null when the median is unknown. */
  aboveMedian: number | null;
}

export interface ShapeScores {
  captains: ShapeScoreRow[];
  splitKinds: ShapeScoreRow[];
  splits: ShapeScoreRow[];
  quarterbacks: ShapeScoreRow[];
  kicker: ShapeScoreRow[];
}

type ScoreAcc = Map<string, { label: string; values: (number | null)[] }>;

/**
 * `actual`: DraftKings points by lineup number, null when a player was missing
 * from the contest export. Lineups absent from the map are skipped entirely.
 */
export function scoreByShape(lineups: readonly BreakdownLineup[], format: PortfolioFormat,
  actual: ReadonlyMap<number, number | null>, median: number | null): ShapeScores {
  const captains: ScoreAcc = new Map(), splitKinds: ScoreAcc = new Map(), splits: ScoreAcc = new Map(),
    quarterbacks: ScoreAcc = new Map(), kicker: ScoreAcc = new Map();
  const add = (map: ScoreAcc, key: string, label: string, value: number | null) => {
    const g = map.get(key) ?? { label, values: [] }; g.values.push(value); map.set(key, g);
  };
  for (const lineup of lineups) {
    if (!actual.has(lineup.lineupNumber)) continue;
    const value = actual.get(lineup.lineupNumber) ?? null;
    const shape = lineupShape(lineup, format);
    const captain = lineup.slots.find((s) => s.slot === "CPT")?.player;
    if (captain) add(captains, String(captain.dkPlayerId), captain.name, value);
    add(splitKinds, shape.splitKind, shape.splitKind, value);
    if (format === "showdown" && shape.split.includes(" ")) add(splits, shape.split, shape.split, value);
    const q = shape.quarterbacks >= 2 ? "Both QBs" : shape.quarterbacks === 1 ? "One QB" : "No QB";
    add(quarterbacks, q, q, value);
    add(kicker, shape.kicker ? "kicker" : "no-kicker", shape.kicker ? "With a kicker" : "No kicker", value);
  }
  const finish = (map: ScoreAcc, order?: string[]): ShapeScoreRow[] => [...map].map(([key, g]) => {
    const scored = g.values.filter((v): v is number => v != null);
    return { key, label: g.label, lineups: g.values.length, scored: scored.length,
      average: scored.length ? scored.reduce((s, v) => s + v, 0) / scored.length : null,
      best: scored.length ? Math.max(...scored) : null,
      aboveMedian: median == null ? null : scored.filter((v) => v > median).length };
  }).sort((x, y) => order
    ? (order.indexOf(x.key) + 1 || 99) - (order.indexOf(y.key) + 1 || 99) || y.lineups - x.lineups
    : y.lineups - x.lineups || (y.average ?? -1) - (x.average ?? -1));
  return { captains: finish(captains), splitKinds: finish(splitKinds, ["3-3", "4-2", "5-1", "6-0"]), splits: finish(splits),
    quarterbacks: finish(quarterbacks, ["Both QBs", "One QB", "No QB"]), kicker: finish(kicker, ["kicker", "no-kicker"]) };
}

// ---------------------------------------------------------------------------
// Where the field numbers come from.

export interface FieldOwnershipSource {
  /** Short tag for a column header: "est." or "LineStar". */
  short: string;
  /** One sentence for the panel note. */
  note: string;
}

/**
 * The page's field ownership is our own estimate unless LineStar supplied it.
 * The quoted misses come from the ownership calibration notes in CLAUDE.md:
 * Showdown flex ownership off by about 4-5 points a player (weeks 4-5, v3
 * prior), Classic about 1 point (week-4 Millionaire).
 */
export function fieldOwnershipSource(players: readonly { ownSource?: string | null; ownPct?: number | null }[],
  format: PortfolioFormat): FieldOwnershipSource {
  const withOwn = players.filter((p) => p.ownPct != null);
  const linestar = withOwn.filter((p) => p.ownSource === "linestar").length;
  if (withOwn.length && linestar === withOwn.length)
    return { short: "LineStar", note: "Field is LineStar's projected ownership, not DraftKings' final numbers." };
  const fitted = withOwn.some((p) => p.ownSource?.startsWith("nfl-ownership-fit"));
  const miss = format === "showdown"
    ? "on recent Showdowns it missed by about 4-5 points a player, most on cheap players and kickers. Treat gaps under about 5 points as noise"
    : "on recent Classic slates it missed by about 1 point a player";
  return { short: "est.",
    note: `Field is our own ${fitted ? "fitted " : ""}ownership estimate${linestar ? ` (LineStar for ${linestar} players)` : ""}, not DraftKings' numbers: ${miss}.` };
}
