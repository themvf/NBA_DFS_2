/**
 * Weakest lineups and their upgrade (nfl-lineup-upgrade-v1, 2026-11-15).
 *
 * Review lists the lineups that project lowest in a saved set. "Replace" looks
 * for the swap of one or two players that adds the most projected points to
 * that lineup while it stays the same kind of lineup:
 *  - every hard rule of the inactive-player swap (salary cap, slot position,
 *    captain rules, no repeated player, both teams / two games, the DST rule,
 *    the lineup's own fades, the run's overlap cap against every other lineup);
 *  - the run's salary floor and player exposure limit (never relaxed here);
 *  - the same shape: Showdown keeps its team split, Classic keeps its
 *    quarterback, at least as many stacked pass catchers, and its bring-back.
 * A lineup with no swap worth MIN_GAIN points is left alone and said so: it is
 * low because the exposure limit and the min-unique rule hold it there.
 * Pure: no database.
 */
import type { NflGeneratedLineup } from "@/app/dfs/nfl/nfl-optimizer";
import { fitsSlot, illegalReason, rebuildLineup, shared, slotFor, type SwapCandidate, type SwapRules } from "./inactive-swap";

export const NFL_LINEUP_UPGRADE_VERSION = "nfl-lineup-upgrade-v1";
/** A swap must add at least this many projected points to be made. */
export const MIN_GAIN = 0.5;
const CANDIDATES_PER_SLOT = 25;

export type WeakLineup = {
  lineupNumber: number;
  projectedFpts: number;
  /** Projected points below the set's median lineup. */
  belowMedian: number;
  salaryLeft: number;
  /** The slot that projects least for its salary. */
  weakestPlayer: { name: string; slot: string; projection: number; salary: number };
};

const median = (values: number[]) => { const v = [...values].sort((a, b) => a - b); const m = Math.floor(v.length / 2); return v.length % 2 ? v[m] : (v[m - 1] + v[m]) / 2; };

/** The lowest-projected lineups of a set (at most `count`, and only those below the median). */
export function weakestLineups(lineups: readonly NflGeneratedLineup[], count = 5): WeakLineup[] {
  if (lineups.length < 4) return [];
  const mid = median(lineups.map((l) => l.projectedFpts));
  return [...lineups].filter((l) => l.projectedFpts < mid - 1e-9)
    .sort((a, b) => a.projectedFpts - b.projectedFpts || a.lineupNumber - b.lineupNumber).slice(0, count)
    .map((l) => {
      const weakest = [...l.slots].sort((a, b) => a.projection / Math.max(1, a.salary) - b.projection / Math.max(1, b.salary))[0];
      return { lineupNumber: l.lineupNumber, projectedFpts: l.projectedFpts, belowMedian: mid - l.projectedFpts, salaryLeft: 50_000 - l.totalSalary,
        weakestPlayer: { name: weakest.player.name, slot: weakest.slot, projection: weakest.projection, salary: weakest.salary } };
    });
}

export type LineupUpgrade = {
  lineupNumber: number;
  before: number;
  after: number;
  swaps: { slot: string; out: { dkPlayerId: number; name: string; projection: number }; in: { dkPlayerId: number; name: string; team: string; position: string; salary: number; projection: number } }[];
};
export type LineupUpgradeResult = {
  version: typeof NFL_LINEUP_UPGRADE_VERSION;
  lineups: NflGeneratedLineup[];
  upgrades: LineupUpgrade[];
  /** Lineups asked for that no legal swap improved. */
  unchanged: number[];
  summary: string[];
};

function teamSplit(lineup: Pick<NflGeneratedLineup, "slots">): string {
  const counts = new Map<string, number>();
  for (const { player } of lineup.slots) counts.set(player.team, (counts.get(player.team) ?? 0) + 1);
  return [...counts].sort((a, b) => a[0].localeCompare(b[0])).map(([team, n]) => `${team}${n}`).join("|");
}

export function upgradeLineups(input: {
  lineups: readonly NflGeneratedLineup[];
  targets: readonly number[];
  candidates: readonly SwapCandidate[];
  rules: SwapRules;
}): LineupUpgradeResult {
  const { rules } = input;
  const ordered = [...input.lineups].sort((a, b) => a.lineupNumber - b.lineupNumber);
  const current = new Map(ordered.map((l) => [l.lineupNumber, l]));
  const usage = new Map<number, number>();
  for (const l of ordered) for (const id of l.playerIds) usage.set(id, (usage.get(id) ?? 0) + 1);
  const upgrades: LineupUpgrade[] = [];
  const unchanged: number[] = [];
  const targets = [...new Set(input.targets)].filter((n) => current.has(n))
    .sort((a, b) => current.get(a)!.projectedFpts - current.get(b)!.projectedFpts || a - b);

  for (const number of targets) {
    const lineup = current.get(number)!;
    const faded = new Set(lineup.archetype?.fadedPlayerIds ?? []);
    const others = ordered.filter((l) => l.lineupNumber !== number).map((l) => new Set(current.get(l.lineupNumber)!.playerIds));
    const inLineup = new Set(lineup.playerIds);
    const split = rules.format === "showdown" ? teamSplit(lineup) : null;
    const floor = rules.minSalary > 0 && lineup.totalSalary >= rules.minSalary ? rules.minSalary : 0;
    // Per slot: players who fit it, are not already here, and have room under the exposure limit.
    const lists = lineup.slots.map((slot) => {
      // The quarterback of a Classic lineup stays: the stack is built on him.
      if (rules.format === "classic" && slot.player.position === "QB") return [];
      return input.candidates
        .filter((c) => fitsSlot(c, slot.slot, rules) && !inLineup.has(c.player.dkPlayerId) && !faded.has(c.player.dkPlayerId)
          && (usage.get(c.player.dkPlayerId) ?? 0) + 1 <= rules.maxLineupsFor(c.player.dkPlayerId))
        .map((c) => ({ c, slot: slotFor(c, slot.slot) }))
        .filter((x) => x.slot.projection > slot.projection)
        .sort((a, b) => b.slot.projection - a.slot.projection || a.c.player.dkPlayerId - b.c.player.dkPlayerId)
        .slice(0, CANDIDATES_PER_SLOT);
    });
    let best: { picks: Map<number, ReturnType<typeof slotFor>>; gain: number } | null = null;
    const consider = (picks: Map<number, ReturnType<typeof slotFor>>) => {
      const slots = lineup.slots.map((s, i) => picks.get(i) ?? s);
      const gain = slots.reduce((sum, s) => sum + s.projection, 0) - lineup.projectedFpts;
      if (gain < MIN_GAIN || (best && gain <= best.gain)) return;
      if (illegalReason(slots, rules.format)) return;
      const salary = slots.reduce((sum, s) => sum + s.salary, 0);
      if (salary < floor) return;
      const ids = slots.map((s) => s.player.dkPlayerId);
      for (const other of others) if (shared(ids, other) > rules.overlapCap) return;
      const rebuilt = rebuildLineup(lineup, slots, [], []);
      if (split != null && teamSplit(rebuilt) !== split) return;
      if (rules.format === "classic") {
        if (rebuilt.stackSummary.passCatchers.length < lineup.stackSummary.passCatchers.length) return;
        if (Boolean(rebuilt.stackSummary.bringBack) !== Boolean(lineup.stackSummary.bringBack)) return;
      }
      best = { picks: new Map(picks), gain };
    };
    for (let i = 0; i < lists.length; i++) for (const a of lists[i]) {
      consider(new Map([[i, a.slot]]));
      // Two players at once: a cheaper upgrade in one slot can pay for a bigger one in another.
      for (let j = 0; j < lineup.slots.length; j++) {
        if (j === i || (rules.format === "classic" && lineup.slots[j].player.position === "QB")) continue;
        for (const c of input.candidates) {
          if (c.player.dkPlayerId === a.c.player.dkPlayerId || inLineup.has(c.player.dkPlayerId) || faded.has(c.player.dkPlayerId)) continue;
          if (!fitsSlot(c, lineup.slots[j].slot, rules)) continue;
          if ((usage.get(c.player.dkPlayerId) ?? 0) + 1 > rules.maxLineupsFor(c.player.dkPlayerId)) continue;
          const second = slotFor(c, lineup.slots[j].slot);
          // Only pairs that could beat the best so far are checked in full.
          if (a.slot.projection + second.projection - lineup.slots[i].projection - lineup.slots[j].projection <= Math.max(MIN_GAIN, (best as { gain: number } | null)?.gain ?? 0)) continue;
          consider(new Map([[i, a.slot], [j, second]]));
        }
      }
    }
    const found = best as { picks: Map<number, ReturnType<typeof slotFor>>; gain: number } | null;
    if (!found) { unchanged.push(number); continue; }
    const slots = lineup.slots.map((s, i) => found.picks.get(i) ?? s);
    const removed = [...found.picks.keys()].map((i) => lineup.slots[i]);
    const added = [...found.picks.values()];
    const rebuilt = rebuildLineup(lineup, slots, removed, added);
    current.set(number, rebuilt);
    for (const s of removed) usage.set(s.player.dkPlayerId, (usage.get(s.player.dkPlayerId) ?? 1) - 1);
    for (const s of added) usage.set(s.player.dkPlayerId, (usage.get(s.player.dkPlayerId) ?? 0) + 1);
    upgrades.push({ lineupNumber: number, before: lineup.projectedFpts, after: rebuilt.projectedFpts,
      swaps: [...found.picks].map(([i, s]) => ({ slot: s.slot,
        out: { dkPlayerId: lineup.slots[i].player.dkPlayerId, name: lineup.slots[i].player.name, projection: Math.round(lineup.slots[i].projection * 100) / 100 },
        in: { dkPlayerId: s.player.dkPlayerId, name: s.player.name, team: s.player.team, position: s.player.position, salary: s.salary, projection: Math.round(s.projection * 100) / 100 } })) });
  }
  const summary = [
    ...upgrades.map((u) => `Lineup ${u.lineupNumber}: ${u.swaps.map((s) => `${s.out.name} out, ${s.in.name} in`).join("; ")}. Projected ${u.before.toFixed(1)} to ${u.after.toFixed(1)} (+${(u.after - u.before).toFixed(1)}).`),
    ...(unchanged.length ? [`Lineup${unchanged.length === 1 ? "" : "s"} ${unchanged.join(", ")}: no legal swap of one or two players adds ${MIN_GAIN} points while keeping the lineup's shape. The exposure limit and the min-unique rule are what hold ${unchanged.length === 1 ? "it" : "them"} there.`] : []),
  ];
  return { version: NFL_LINEUP_UPGRADE_VERSION, lineups: ordered.map((l) => current.get(l.lineupNumber)!), upgrades, unchanged, summary };
}
