import type { CbbEstimates } from "./projection";
import { parseCbbSalaries, type CbbMode, type CbbPlayer } from "./salary-csv";

export interface CbbSlot { slot: string; id: string; player: CbbPlayer; salary: number; projection: number }
export interface CbbLineup { slots: CbbSlot[]; salary: number; projection: number }
export const cbbSlots = (mode: CbbMode) => mode === "classic" ? ["G", "G", "G", "F", "F", "F", "UTIL", "UTIL"] : ["CPT", "UTIL", "UTIL", "UTIL", "UTIL", "UTIL"];
export function cbbUnavailable(status: string): boolean { return ["O", "OUT", "IR", "SUSP", "SUSPENDED", "INACTIVE"].includes(status.toUpperCase()); }

/** Slot-assignment integer program. Ownership does not stand in for fantasy production. */
export function buildCbbLineups(salaryCsv: string, mode: CbbMode, estimates: CbbEstimates, count: number, now = Date.now()): CbbLineup[] {
  if (mode !== "classic" && mode !== "showdown") throw new Error("Unsupported CBB mode.");
  if (salaryCsv.length > 2_000_000 || Object.keys(estimates).length > 2000) throw new Error("Slate is too large.");
  const slate = parseCbbSalaries(salaryCsv, mode);
  if (Date.parse(slate.firstTipoff) <= now) throw new Error("This slate has started. Build a new pre-lock slate; late swap is not supported yet.");
  if (!Number.isInteger(count) || count < 1 || count > 20) throw new Error("Build between 1 and 20 lineups.");
  const players = slate.players.filter(p => {
    const e = estimates[p.id];
    if (!e || e.excluded || cbbUnavailable(p.status)) return false;
    if (e.projection === null) return false;
    if (!Number.isFinite(e.projection) || e.projection < 0 || e.projection > 500 || !e.source || !Number.isFinite(Date.parse(e.asOf)) || Date.parse(e.asOf) >= Date.parse(slate.firstTipoff) || Date.parse(e.asOf) > now) throw new Error(`Invalid projection or provenance for ${p.name}.`);
    return true;
  });
  if (players.length > 800) throw new Error("Limit the player pool to 800 players.");
  const slots = cbbSlots(mode);
  // eslint-disable-next-line @typescript-eslint/no-require-imports
  const solver = require("javascript-lp-solver") as { Solve: (model: unknown) => Record<string, number | boolean> };
  const lineups: CbbLineup[] = [];
  for (let run = 0; run < count; run++) {
    const constraints: Record<string, { max?: number; equal?: number }> = { salary: { max: 50000 } };
    const variables: Record<string, Record<string, number>> = {};
    const binaries: Record<string, number> = {};
    const candidates = new Map<string, CbbSlot>();
    slots.forEach((_, i) => { constraints[`slot_${i}`] = { equal: 1 }; });
    players.forEach(p => { constraints[`player_${p.id}`] = { max: 1 }; });
    // At least two teams; Classic additionally draws from at least two games.
    new Set(players.map(p => p.team)).forEach(team => { constraints[`team_${team}`] = { max: slots.length - 1 }; });
    if (mode === "classic") new Set(players.map(p => p.game)).forEach(game => { constraints[`game_${game}`] = { max: slots.length - 1 }; });
    lineups.forEach((_, i) => { constraints[`prior_${i}`] = { max: slots.length - 1 }; });
    for (const p of players) slots.forEach((slot, i) => {
      if (mode === "classic" && slot !== "UTIL" && !p.positions.includes(slot)) return;
      const captain = slot === "CPT";
      const projection = estimates[p.id].projection! * (captain ? 1.5 : 1);
      const salary = captain ? p.captainSalary! : p.salary;
      const key = `p${p.id}_s${i}`;
      const v: Record<string, number> = { value: projection, salary, [`slot_${i}`]: 1, [`player_${p.id}`]: 1, [`team_${p.team}`]: 1 };
      if (mode === "classic") v[`game_${p.game}`] = 1;
      lineups.forEach((lineup, j) => { if (lineup.slots.some(s => s.player.id === p.id)) v[`prior_${j}`] = 1; });
      variables[key] = v; binaries[key] = 1;
      candidates.set(key, { slot, id: captain ? p.captainId! : p.id, player: p, salary, projection });
    });
    const solved = solver.Solve({ optimize: "value", opType: "max", constraints, variables, binaries, options: { timeout: 1500 } });
    if (!solved.feasible || !solved.bounded) break;
    const picked = [...candidates].filter(([key]) => typeof solved[key] === "number" && Number(solved[key]) > .5).map(([, p]) => p);
    picked.sort((a, b) => slots.indexOf(a.slot) - slots.indexOf(b.slot));
    const lineup = { slots: picked, salary: picked.reduce((s, p) => s + p.salary, 0), projection: picked.reduce((s, p) => s + p.projection, 0) };
    if (picked.length !== slots.length || lineup.salary > 50000 || new Set(picked.map(p => p.player.id)).size !== slots.length || new Set(picked.map(p => p.player.team)).size < 2) throw new Error("Solver returned an invalid lineup.");
    if (mode === "classic" && new Set(picked.map(p => p.player.game)).size < 2) throw new Error("Classic lineup needs two games.");
    if (slots.some(slot => picked.filter(p => p.slot === slot).length !== slots.filter(s => s === slot).length)) throw new Error("Solver returned invalid slots.");
    lineups.push(lineup);
  }
  if (!lineups.length) throw new Error("No legal lineup. Check projection coverage, exclusions, positions, and salaries.");
  return lineups;
}
