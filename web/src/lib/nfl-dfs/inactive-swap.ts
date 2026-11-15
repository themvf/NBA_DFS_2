/**
 * Inactive-player swap (nfl-inactive-swap-v1, 2026-11-15).
 *
 * After inactives come out, a saved run can hold a player who is now ruled out.
 * Rebuilding the whole run throws away every lineup that is still fine and
 * reshuffles which lineup sits in which entry row. This rebuilds ONLY the
 * lineups that hold an out player: every other player in that lineup stays in
 * his slot (a Captain stays the Captain), and each out player's slot is refilled
 * with the best legal replacement under the run's own rules. Lineup numbers do
 * not change, so the export still maps each lineup to the same entry row.
 *
 * Hard rules (never relaxed): salary cap, the slot's position (Showdown Captain:
 * a captain-eligible player at Captain salary, limited to the run's captain
 * positions), no repeated player, Showdown players from both teams, Classic
 * players from two games, the DST game-script rule, the lineup's own archetype
 * fades, and the run's overlap cap against every other lineup in the set.
 * Soft rules (relaxed only when nothing else fits, and said): the run's lineup
 * salary floor, then the run's player exposure limit.
 *
 * "Best" is the run's own objective, simplified to one player: cash by mean
 * projection, GPP by ceiling capped at maxCeilingMultiple x projection (the
 * optimizer's ceiling cap), at the slot's multiplier. Pure: no database.
 */
import type { NflGeneratedLineup, NflLineupSlot, NflOptimizerPlayer } from "@/app/dfs/nfl/nfl-optimizer";
import { assertDstGameScript, assertShowdownLineup } from "@/lib/nfl-dfs/showdown-legality";

export const NFL_INACTIVE_SWAP_VERSION = "nfl-inactive-swap-v1";
const SALARY_CAP = 50_000;
const DEFAULT_CEILING_MULTIPLE = 2.5;
/** Replacements tried per out slot; enough to clear the overlap cap, small enough to search jointly. */
const CANDIDATES_PER_SLOT = 30;

export type SwapCandidate = {
  player: NflOptimizerPlayer;
  /** The run's resolved projection for him, at 1x. */
  projection: number;
  source: NflLineupSlot["projectionSource"];
  floor: number;
  ceiling: number;
  /** May fill a FLEX / Classic slot under the run's pool rules. */
  flexEligible: boolean;
  /** May be the Showdown Captain under the run's pool rules. */
  captainEligible: boolean;
};

export type SwapRules = {
  format: "classic" | "showdown";
  mode: "cash" | "gpp";
  /** Lineup salary floor the run was built with (0 = none). Soft. */
  minSalary: number;
  /** Most players any two lineups may share (nflOverlapCap). Hard. */
  overlapCap: number;
  /** Lineups a player may appear in across the set. Soft. */
  maxLineupsFor: (dkPlayerId: number) => number;
  captainPositions?: readonly NflOptimizerPlayer["position"][];
  maxCeilingMultiple?: number;
};

export type SlotSwap = {
  lineupNumber: number;
  slot: string;
  out: { dkPlayerId: number; name: string; reason: string };
  in: { dkPlayerId: number; name: string; team: string; position: string; salary: number; projection: number } | null;
};

export type InactiveSwapResult = {
  version: typeof NFL_INACTIVE_SWAP_VERSION;
  /** Every lineup of the run, in lineup order: untouched ones as they were, swapped ones rebuilt. */
  lineups: NflGeneratedLineup[];
  swaps: SlotSwap[];
  /** Lineups that still hold an out player because no legal replacement existed. */
  unresolved: number[];
  /** Soft rules relaxed, per lineup, in words. */
  relaxed: { lineupNumber: number; note: string }[];
  /** Plain-language summary lines for the page. */
  summary: string[];
};

const CLASSIC_SLOT_POSITIONS: Record<string, readonly string[]> = {
  QB: ["QB"], RB1: ["RB"], RB2: ["RB"], WR1: ["WR"], WR2: ["WR"], WR3: ["WR"], TE: ["TE"], FLEX: ["RB", "WR", "TE"], DST: ["DST"],
};

const identity = (p: Pick<NflOptimizerPlayer, "team" | "name">) => `${p.team}|${p.name.trim().toLowerCase()}`;
const isCaptain = (slot: string) => slot === "CPT";
const captainSalary = (p: NflOptimizerPlayer) => p.captainSalary ?? Math.round(p.salary * 1.5);

/** The one-player version of the run's objective, at the slot's multiplier. */
export function swapScore(candidate: Pick<SwapCandidate, "projection" | "ceiling">, multiplier: number, rules: Pick<SwapRules, "mode" | "maxCeilingMultiple">): number {
  if (rules.mode === "cash") return candidate.projection * multiplier;
  const cap = candidate.projection * (rules.maxCeilingMultiple ?? DEFAULT_CEILING_MULTIPLE);
  return Math.min(candidate.ceiling, cap) * multiplier;
}

/** Whether a candidate may fill this slot at all (position, captain rules). */
export function fitsSlot(candidate: SwapCandidate, slot: string, rules: SwapRules): boolean {
  if (rules.format === "showdown") {
    if (isCaptain(slot)) {
      if (!candidate.captainEligible || candidate.player.captainDkPlayerId == null) return false;
      return !rules.captainPositions?.length || rules.captainPositions.includes(candidate.player.position);
    }
    return candidate.flexEligible;
  }
  return candidate.flexEligible && (CLASSIC_SLOT_POSITIONS[slot] ?? []).includes(candidate.player.position);
}

export function slotFor(candidate: SwapCandidate, slot: string): NflLineupSlot {
  const multiplier = isCaptain(slot) ? 1.5 : 1;
  return { slot, player: candidate.player, salary: isCaptain(slot) ? captainSalary(candidate.player) : candidate.player.salary,
    multiplier, projection: candidate.projection * multiplier, projectionSource: candidate.source };
}

/** A lineup's range contribution for one slot: the player's own range when present, else the optimizer's 0.74/1.28 mean heuristic. */
function rangeOf(slot: NflLineupSlot, which: "floor" | "ceiling"): number {
  const own = which === "floor" ? slot.player.floorFpts : slot.player.ceilingFpts;
  if (own != null && Number.isFinite(own)) return own * slot.multiplier;
  return slot.projection * (which === "floor" ? 0.74 : 1.28);
}
const ownOf = (p: NflOptimizerPlayer) => p.ownPct ?? p.linestarOwnPct ?? null;

/** Hard whole-lineup legality; throws nothing, returns the reason it fails or null. */
export function illegalReason(slots: NflLineupSlot[], format: SwapRules["format"]): string | null {
  const total = slots.reduce((s, x) => s + x.salary, 0);
  if (total > SALARY_CAP) return "over the salary cap";
  if (new Set(slots.map((s) => s.player.dkPlayerId)).size !== slots.length) return "repeats a player";
  if (new Set(slots.map((s) => identity(s.player))).size !== slots.length) return "repeats a player";
  try {
    if (format === "showdown") assertShowdownLineup({ slots, playerIds: slots.map((s) => s.player.dkPlayerId), totalSalary: total });
    else {
      if (new Set(slots.map((s) => s.player.gameKey ?? s.player.team)).size < 2) return "uses one game only";
      assertDstGameScript("classic", slots);
    }
  } catch (e) { return e instanceof Error ? e.message : "illegal roster"; }
  return null;
}

export function shared(a: readonly number[], b: ReadonlySet<number>): number {
  let n = 0;
  for (const id of a) if (b.has(id)) n++;
  return n;
}

export function rebuildLineup(original: NflGeneratedLineup, slots: NflLineupSlot[], removed: NflLineupSlot[], added: NflLineupSlot[]): NflGeneratedLineup {
  const delta = (which: "floor" | "ceiling") => added.reduce((s, x) => s + rangeOf(x, which), 0) - removed.reduce((s, x) => s + rangeOf(x, which), 0);
  const ownDelta = added.reduce((s, x) => s + (ownOf(x.player) ?? 0), 0) - removed.reduce((s, x) => s + (ownOf(x.player) ?? 0), 0);
  const qb = slots.find((s) => s.player.position === "QB")?.player ?? null;
  const passCatchers = qb ? slots.filter((s) => ["WR", "TE"].includes(s.player.position) && s.player.team === qb.team).map((s) => s.player.name) : [];
  const bringBack = qb ? slots.find((s) => ["RB", "WR", "TE"].includes(s.player.position) && s.player.team === qb.opponent)?.player.name ?? null : null;
  return {
    ...original,
    slots,
    playerIds: slots.map((s) => s.player.dkPlayerId),
    totalSalary: slots.reduce((s, x) => s + x.salary, 0),
    projectedFpts: slots.reduce((s, x) => s + x.projection, 0),
    floorFpts: original.floorFpts + delta("floor"),
    ceilingFpts: original.ceilingFpts + delta("ceiling"),
    projectedOwnership: original.projectedOwnership == null ? null : original.projectedOwnership + ownDelta,
    stackSummary: { quarterback: qb?.name ?? null, passCatchers, bringBack },
  };
}

/**
 * Rebuild only the lineups holding an out player.
 * `outReasons`: dkPlayerId -> why he is out now (the current-pool QA reason).
 * `candidates`: players who are available now and usable under the run's rules.
 */
export function swapInactivePlayers(input: {
  lineups: readonly NflGeneratedLineup[];
  outReasons: ReadonlyMap<number, string>;
  candidates: readonly SwapCandidate[];
  rules: SwapRules;
}): InactiveSwapResult {
  const { rules } = input;
  const ordered = [...input.lineups].sort((a, b) => a.lineupNumber - b.lineupNumber);
  const current = new Map(ordered.map((l) => [l.lineupNumber, l]));
  const usage = new Map<number, number>();
  for (const l of ordered) for (const id of l.playerIds) usage.set(id, (usage.get(id) ?? 0) + 1);
  const candidates = input.candidates.filter((c) => !input.outReasons.has(c.player.dkPlayerId));
  const swaps: SlotSwap[] = [];
  const unresolved: number[] = [];
  const relaxed: { lineupNumber: number; note: string }[] = [];

  for (const lineup of ordered) {
    const outSlots = lineup.slots.filter((s) => input.outReasons.has(s.player.dkPlayerId));
    if (!outSlots.length) continue;
    // Captain first, then the most expensive slot: the hardest slots choose first.
    outSlots.sort((a, b) => Number(isCaptain(b.slot)) - Number(isCaptain(a.slot)) || b.salary - a.salary);
    const kept = lineup.slots.filter((s) => !outSlots.includes(s));
    const keptIds = new Set(kept.map((s) => s.player.dkPlayerId));
    const keptIdentities = new Set(kept.map((s) => identity(s.player)));
    const faded = new Set(lineup.archetype?.fadedPlayerIds ?? []);
    const others = ordered.filter((l) => l.lineupNumber !== lineup.lineupNumber);
    const lists = outSlots.map((slot) => candidates
      .filter((c) => fitsSlot(c, slot.slot, rules) && !keptIds.has(c.player.dkPlayerId) && !keptIdentities.has(identity(c.player)) && !faded.has(c.player.dkPlayerId))
      .map((c) => ({ c, score: swapScore(c, isCaptain(slot.slot) ? 1.5 : 1, rules) }))
      .sort((a, b) => b.score - a.score || b.c.projection - a.c.projection || a.c.player.dkPlayerId - b.c.player.dkPlayerId)
      .slice(0, CANDIDATES_PER_SLOT));

    // Tier 0: every rule. Tier 1: no salary floor. Tier 2: no floor, no exposure limit.
    let best: { picks: SwapCandidate[]; score: number } | null = null;
    let tierUsed = 0;
    for (let tier = 0; tier < 3 && !best; tier++) {
      const chosen: SwapCandidate[] = [];
      const search = (i: number, score: number) => {
        if (i === outSlots.length) {
          const slots = lineup.slots.map((s) => { const k = outSlots.indexOf(s); return k < 0 ? s : slotFor(chosen[k], s.slot); });
          if (illegalReason(slots, rules.format)) return;
          const ids = slots.map((s) => s.player.dkPlayerId);
          if (tier < 1 && rules.minSalary > 0 && slots.reduce((s, x) => s + x.salary, 0) < rules.minSalary) return;
          for (const other of others) {
            const otherIds = new Set(current.get(other.lineupNumber)!.playerIds);
            if (shared(ids, otherIds) > rules.overlapCap) return;
          }
          if (!best || score > best.score) best = { picks: [...chosen], score };
          return;
        }
        const remainingBound = lists.slice(i).reduce((s, list) => s + (list[0]?.score ?? 0), 0);
        if (best && score + remainingBound <= best.score) return;
        for (const { c, score: sc } of lists[i]) {
          if (chosen.some((x) => x.player.dkPlayerId === c.player.dkPlayerId || identity(x.player) === identity(c.player))) continue;
          if (tier < 2 && (usage.get(c.player.dkPlayerId) ?? 0) + 1 > rules.maxLineupsFor(c.player.dkPlayerId)) continue;
          chosen.push(c);
          search(i + 1, score + sc);
          chosen.pop();
        }
      };
      search(0, 0);
      tierUsed = tier;
    }

    const picked = (best as { picks: SwapCandidate[]; score: number } | null)?.picks ?? null;
    if (!picked) {
      unresolved.push(lineup.lineupNumber);
      for (const slot of outSlots) swaps.push({ lineupNumber: lineup.lineupNumber, slot: slot.slot,
        out: { dkPlayerId: slot.player.dkPlayerId, name: slot.player.name, reason: input.outReasons.get(slot.player.dkPlayerId)! }, in: null });
      continue;
    }
    const added = picked.map((c, k) => slotFor(c, outSlots[k].slot));
    const slots = lineup.slots.map((s) => { const k = outSlots.indexOf(s); return k < 0 ? s : added[k]; });
    const rebuilt = rebuildLineup(lineup, slots, outSlots, added);
    current.set(lineup.lineupNumber, rebuilt);
    for (const s of outSlots) usage.set(s.player.dkPlayerId, (usage.get(s.player.dkPlayerId) ?? 1) - 1);
    for (const s of added) usage.set(s.player.dkPlayerId, (usage.get(s.player.dkPlayerId) ?? 0) + 1);
    if (tierUsed >= 1) relaxed.push({ lineupNumber: lineup.lineupNumber, note: tierUsed === 1
      ? `lineup ${lineup.lineupNumber} is below the $${rules.minSalary.toLocaleString("en-US")} salary floor (no replacement fit above it)`
      : `lineup ${lineup.lineupNumber} goes past a player's exposure limit (no replacement fit within it)` });
    outSlots.forEach((slot, k) => swaps.push({ lineupNumber: lineup.lineupNumber, slot: slot.slot,
      out: { dkPlayerId: slot.player.dkPlayerId, name: slot.player.name, reason: input.outReasons.get(slot.player.dkPlayerId)! },
      in: { dkPlayerId: added[k].player.dkPlayerId, name: added[k].player.name, team: added[k].player.team, position: added[k].player.position,
        salary: added[k].salary, projection: Math.round(added[k].projection * 100) / 100 } }));
  }

  const lineups = ordered.map((l) => current.get(l.lineupNumber)!);
  return { version: NFL_INACTIVE_SWAP_VERSION, lineups, swaps, unresolved, relaxed, summary: swapSummary(swaps, unresolved, relaxed) };
}

/** "3 lineups had Amon-Ra St. Brown (DraftKings lists him OUT); replaced with Jameson Williams in 2, Sam LaPorta in 1." */
export function swapSummary(swaps: readonly SlotSwap[], unresolved: readonly number[], relaxed: readonly { note: string }[]): string[] {
  if (!swaps.length) return ["No lineup holds a player who is out now. Nothing was changed."];
  const byOut = new Map<number, SlotSwap[]>();
  for (const s of swaps) byOut.set(s.out.dkPlayerId, [...(byOut.get(s.out.dkPlayerId) ?? []), s]);
  const lines: string[] = [];
  for (const group of byOut.values()) {
    const lineupCount = new Set(group.map((s) => s.lineupNumber)).size;
    const inCounts = new Map<string, number>();
    for (const s of group) if (s.in) inCounts.set(s.in.name, (inCounts.get(s.in.name) ?? 0) + 1);
    const replaced = [...inCounts.entries()].sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
      .map(([name, n]) => lineupCount === 1 ? name : `${name} in ${n}`).join(", ");
    const stuck = group.filter((s) => !s.in).map((s) => s.lineupNumber);
    const head = `${lineupCount} ${lineupCount === 1 ? "lineup" : "lineups"} had ${group[0].out.name} (${group[0].out.reason})`;
    lines.push(replaced ? `${head}; replaced with ${replaced}.${stuck.length ? ` No legal replacement in lineup${stuck.length === 1 ? "" : "s"} ${stuck.join(", ")}.` : ""}`
      : `${head}; no legal replacement was found.`);
  }
  if (unresolved.length) lines.push(`Lineup${unresolved.length === 1 ? "" : "s"} ${unresolved.join(", ")} still ${unresolved.length === 1 ? "holds" : "hold"} an out player: nothing fit the salary cap, roster rules and overlap limit. Change ${unresolved.length === 1 ? "it" : "them"} by hand or build again.`);
  for (const r of relaxed) lines.push(`Note: ${r.note}.`);
  lines.push("Every other lineup and every other player is unchanged, and lineup numbers are kept, so each lineup goes to the same entry row.");
  return lines;
}
