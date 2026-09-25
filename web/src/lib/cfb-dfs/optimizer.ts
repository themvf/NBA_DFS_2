/**
 * DraftKings CFB Classic lineup builder.
 *
 * Slots: QB, RB, RB, WR, WR, WR, FLEX (RB/WR), SUPER FLEX (QB/RB/WR), under
 * $50,000, from at least two games. Solved as the exactly equivalent counting
 * problem: 8 players, 1-2 QBs, at least 2 RBs, at least 3 WRs; and "at least
 * two games" is "no game supplies all 8", one constraint per game. Slots are
 * assigned after the solve.
 *
 * Portfolio controls: locks, excludes, per-player maximum exposure, a global
 * maximum exposure, minimum unique players against every earlier lineup, and
 * seeded per-lineup noise for GPP variety (lineup 1 is always the unperturbed
 * optimum).
 */
import { cfbRandom } from "./random";
import type { CfbPosition } from "./salary-csv";
import {
  CFB_OPTIMIZER_VERSION, CFB_ROSTER_SIZE, CFB_SALARY_CAP, CFB_SLOTS, cfbLineupProblems,
  type CfbLineup, type CfbLineupSlot, type CfbOptimizerResult, type CfbOptimizerSettings, type CfbPoolPlayer,
} from "./settings";

export { CFB_OPTIMIZER_VERSION, CFB_ROSTER_SIZE, CFB_SALARY_CAP, CFB_SLOTS, DEFAULT_CFB_SETTINGS, cfbLineupProblems } from "./settings";
export type { CfbLineup, CfbLineupSlot, CfbOptimizerResult, CfbOptimizerSettings, CfbPoolPlayer } from "./settings";

type SolverModel = { optimize: string; opType: "max"; constraints: Record<string, { min?: number; max?: number; equal?: number }>;
  variables: Record<string, Record<string, number>>; binaries: Record<string, 1> };

function gaussian(random: () => number): number {
  let u = 0;
  while (u === 0) u = random();
  return Math.sqrt(-2 * Math.log(u)) * Math.cos(2 * Math.PI * random());
}

/** QB, RB, RB, WR, WR, WR, then FLEX and SUPER FLEX from what is left. */
export function assignCfbSlots(players: CfbPoolPlayer[]): CfbLineupSlot[] {
  const by = (pos: CfbPosition) => players.filter((p) => p.position === pos).sort((a, b) => b.proj - a.proj);
  const qbs = by("QB"), rbs = by("RB"), wrs = by("WR");
  if (qbs.length < 1 || rbs.length < 2 || wrs.length < 3) throw new Error("Lineup does not satisfy CFB Classic positions");
  const rest = [...rbs.slice(2), ...wrs.slice(3)];
  const flex = rest.shift();
  const superFlex = qbs[1] ?? rest.shift();
  if (!flex || !superFlex) throw new Error("Lineup does not fill FLEX and SUPER FLEX");
  const order: Array<[CfbLineupSlot["slot"], CfbPoolPlayer]> = [["QB", qbs[0]], ["RB", rbs[0]], ["RB", rbs[1]],
    ["WR", wrs[0]], ["WR", wrs[1]], ["WR", wrs[2]], ["FLEX", flex], ["S-FLEX", superFlex]];
  return order.map(([slot, player]) => ({ slot, player }));
}

export function optimizeCfbLineups(pool: readonly CfbPoolPlayer[], settings: CfbOptimizerSettings): CfbOptimizerResult {
  // eslint-disable-next-line @typescript-eslint/no-require-imports
  const solver = require("javascript-lp-solver") as { Solve: (model: SolverModel) => Record<string, number | boolean> };
  const excluded = new Set(settings.excludedIds);
  const locked = new Set(settings.lockedIds.filter((id) => !excluded.has(id)));
  const players = pool.filter((p) => !excluded.has(p.dkId) && (p.proj > 0 || locked.has(p.dkId)));
  const games = [...new Set(players.map((p) => p.game))];
  const random = cfbRandom(settings.seed);
  const cap = (p: CfbPoolPlayer) => {
    const pct = settings.maxExposureById[String(p.dkId)];
    const share = pct != null ? pct / 100 : settings.maxExposure;
    return locked.has(p.dkId) ? settings.nLineups : Math.max(0, Math.floor(share * settings.nLineups + 1e-9));
  };
  const counts = new Map<number, number>();
  const lineups: CfbLineup[] = [];
  let stoppedEarly: string | null = null;

  for (let n = 0; n < settings.nLineups; n += 1) {
    const available = players.filter((p) => (counts.get(p.dkId) ?? 0) < cap(p));
    const constraints: SolverModel["constraints"] = {
      roster: { equal: CFB_ROSTER_SIZE }, salary: { max: CFB_SALARY_CAP, min: settings.minSalary },
      qb: settings.requireTwoQbs ? { equal: 2 } : { min: 1, max: 2 }, rb: { min: 2 }, wr: { min: 3 },
    };
    for (const g of games) constraints[`game_${g}`] = { max: CFB_ROSTER_SIZE - 1 };
    for (const id of locked) constraints[`lock_${id}`] = { equal: 1 };
    for (const q of available.filter((p) => p.position === "QB")) {
      if (settings.stackQb) constraints[`stack_${q.dkId}`] = { min: 0 };
      if (settings.bringBack) constraints[`bring_${q.dkId}`] = { min: 0 };
    }
    lineups.forEach((_, i) => { constraints[`prior_${i}`] = { max: CFB_ROSTER_SIZE - settings.minUnique }; });
    const variables: SolverModel["variables"] = {};
    const binaries: SolverModel["binaries"] = {};
    for (const p of available) {
      const noise = n === 0 || settings.randomness <= 0 ? 1 : Math.exp(gaussian(random) * settings.randomness);
      const v: Record<string, number> = { score: p.proj * noise, roster: 1, salary: p.salary,
        [p.position.toLowerCase()]: 1, [`game_${p.game}`]: 1 };
      if (locked.has(p.dkId)) v[`lock_${p.dkId}`] = 1;
      // Stack / bring-back: for each QB q, (teammates or opponents picked) - x_q >= 0,
      // so choosing q forces at least one partner and leaving q out forces nothing.
      if (settings.stackQb || settings.bringBack) {
        for (const q of available) {
          if (q.position !== "QB") continue;
          if (p.dkId === q.dkId) {
            if (settings.stackQb) v[`stack_${q.dkId}`] = -1;
            if (settings.bringBack) v[`bring_${q.dkId}`] = -1;
          } else if (p.position !== "QB") {
            if (settings.stackQb && p.team === q.team) v[`stack_${q.dkId}`] = 1;
            if (settings.bringBack && p.game === q.game && p.team !== q.team) v[`bring_${q.dkId}`] = 1;
          }
        }
      }
      lineups.forEach((lineup, i) => { if (lineup.slots.some((s) => s.player.dkId === p.dkId)) v[`prior_${i}`] = 1; });
      variables[`x_${p.dkId}`] = v;
      binaries[`x_${p.dkId}`] = 1;
    }
    const solved = solver.Solve({ optimize: "score", opType: "max", constraints, variables, binaries });
    if (solved.feasible === false) {
      stoppedEarly = `Stopped at ${lineups.length} of ${settings.nLineups}: no further lineup satisfies the locks, exposure caps and uniqueness.`;
      break;
    }
    const chosen = available.filter((p) => Number(solved[`x_${p.dkId}`] ?? 0) > 0.5);
    const problems = cfbLineupProblems(chosen);
    if (problems.length) {
      stoppedEarly = `Stopped at ${lineups.length}: the solver returned an illegal lineup (${problems.join("; ")}).`;
      break;
    }
    for (const p of chosen) counts.set(p.dkId, (counts.get(p.dkId) ?? 0) + 1);
    lineups.push({ lineupNumber: lineups.length + 1, slots: assignCfbSlots(chosen),
      salary: chosen.reduce((a, p) => a + p.salary, 0), projection: Math.round(chosen.reduce((a, p) => a + p.proj, 0) * 100) / 100 });
  }
  return { lineups, stoppedEarly, version: CFB_OPTIMIZER_VERSION };
}

/** Which optional build rules a lineup breaks; empty when it honours them all. */
export function cfbBuildRuleProblems(players: CfbPoolPlayer[], settings: Pick<CfbOptimizerSettings, "requireTwoQbs" | "stackQb" | "bringBack">): string[] {
  const problems: string[] = [];
  const qbs = players.filter((p) => p.position === "QB");
  if (settings.requireTwoQbs && qbs.length !== 2) problems.push(`${qbs.length} QBs`);
  for (const q of qbs) {
    const partners = players.filter((p) => p.position !== "QB");
    if (settings.stackQb && !partners.some((p) => p.team === q.team)) problems.push(`${q.name} has no teammate`);
    if (settings.bringBack && !partners.some((p) => p.game === q.game && p.team !== q.team)) problems.push(`${q.name} has no bring-back`);
  }
  return problems;
}

/** DraftKings upload rows: one per lineup, IDs in slot order. */
export function cfbUploadCsv(lineups: readonly CfbLineup[]): string {
  return [CFB_SLOTS.join(","), ...lineups.map((l) => l.slots.map((s) => s.player.dkId).join(","))].join("\n") + "\n";
}
