/**
 * Robustness of a portfolio-objective selection to its own inputs (step 5 of the plan).
 *
 * Takes a study runner capture (worker-input.json, the frozen slate) and its
 * pair (worker-output.json, which carries arm B's candidate pool), then
 * re-selects arm B under perturbed inputs and reports how much the chosen
 * portfolio moves. Perturbations, each applied alone and all re-seeded:
 *   projections x0.9 / x1.1 (with P10/median/P90), ownership prior x0.8 / x1.2
 *   (renormalised within the field sampler), copula loadings (team 0.25 -> 0.15
 *   and 0.35 through a scale on the z-mix), and a fresh selection seed.
 * Output: per perturbation, the Jaccard overlap of selected lineups with the
 * saved selection, the L1 distance between player exposures, and the change
 * in the forecast P(success). Descriptive; nothing here feeds the study.
 *
 * usage: research-nfl-portfolio-robustness.ts worker-input.json worker-output.json OUT.json
 */
import { readFileSync, writeFileSync } from "node:fs";
import { drawJointBank, sampleField, fieldThresholds, scoreLineup, selectPortfolio, forecastSuccess, slateFromSnapshot,
  PORTFOLIO_OBJECTIVE, type SnapshotPlayer } from "../src/lib/nfl-dfs/portfolio-objective";
import type { NflLineup } from "../src/lib/nfl-dfs/lineups";

const [inputPath, pairPath, outPath] = process.argv.slice(2);
if (!inputPath || !pairPath || !outPath) throw new Error("usage: research-nfl-portfolio-robustness.ts INPUT.json PAIR.json OUT.json");
// eslint-disable-next-line @typescript-eslint/no-explicit-any
type Raw = Record<string, any>;
const input = JSON.parse(readFileSync(inputPath, "utf8")) as Raw;
const pair = JSON.parse(readFileSync(pairPath, "utf8")) as Raw;
const armB = pair.arms.find((a: Raw) => a.arm === "B");
const study = armB.settings.researchStudy;
if (!study?.candidatePool?.rosters?.length) throw new Error("Pair carries no candidate pool (pre-robustness runner?)");
const format = study.format as "classic" | "showdown";
const candidates: Array<{ key: string; lineup: NflLineup }> = study.candidatePool.rosters;
const constraints = { count: armB.requestedLineups, ...study.selectionConstraints };
const basePlayers: SnapshotPlayer[] = input.players;

function select(players: SnapshotPlayer[], seeds: { selection: number; evaluation: number; field: number }) {
  const slate = slateFromSnapshot(format, players);
  const sel = drawJointBank(players, seeds.selection), evl = drawJointBank(players, seeds.evaluation);
  const field = sampleField(slate, players, seeds.field);
  const thrSel = fieldThresholds(field, sel, PORTFOLIO_OBJECTIVE.successFraction), thrEvl = fieldThresholds(field, evl, PORTFOLIO_OBJECTIVE.successFraction);
  const scored = candidates.map(c => ({ ...c, draws: scoreLineup(c.lineup, sel) }));
  const pick = selectPortfolio(scored, thrSel, constraints);
  const chosen = pick.selected.map(s => candidates[s.index].key);
  return { chosen, forecast: forecastSuccess(pick.selected.map(s => candidates[s.index].lineup), evl, thrEvl), calibration: field.calibration };
}
const exposures = (keys: string[]) => {
  const m = new Map<number, number>();
  for (const k of keys) for (const e of candidates.find(c => c.key === k)!.lineup) m.set(e.playerId, (m.get(e.playerId) ?? 0) + 1 / keys.length);
  return m;
};
const l1 = (a: Map<number, number>, b: Map<number, number>) => [...new Set([...a.keys(), ...b.keys()])].reduce((s, id) => s + Math.abs((a.get(id) ?? 0) - (b.get(id) ?? 0)), 0);
const jaccard = (a: string[], b: string[]) => { const A = new Set(a), B = new Set(b); return [...A].filter(k => B.has(k)).length / new Set([...a, ...b]).size; };

const seeds = { selection: PORTFOLIO_OBJECTIVE.selectionSeed, evaluation: PORTFOLIO_OBJECTIVE.evaluationSeed, field: PORTFOLIO_OBJECTIVE.fieldSeed };
const base = select(basePlayers, seeds);
const savedKeys: string[] = study.selection.picks.map((p: Raw) => p.key);
const scaleProj = (f: number) => basePlayers.map(p => ({ ...p, ourProj: p.ourProj == null ? p.ourProj : p.ourProj * f,
  floor: p.floor == null ? p.floor : p.floor * f, median: p.median == null ? p.median : p.median * f, ceiling: p.ceiling == null ? p.ceiling : p.ceiling * f }));
const scaleOwn = (f: number) => basePlayers.map(p => ({ ...p, ownership: p.ownership ? { ...p.ownership,
  total: p.ownership.total == null ? null : p.ownership.total * f, captain: p.ownership.captain == null ? null : p.ownership.captain * f,
  flex: p.ownership.flex == null ? null : p.ownership.flex * f } : p.ownership }));
const perturbations: Array<[string, () => ReturnType<typeof select>]> = [
  ["projections x0.9", () => select(scaleProj(0.9), seeds)], ["projections x1.1", () => select(scaleProj(1.1), seeds)],
  ["ownership x0.8", () => select(scaleOwn(0.8), seeds)], ["ownership x1.2", () => select(scaleOwn(1.2), seeds)],
  ["selection seed +1", () => select(basePlayers, { ...seeds, selection: seeds.selection + 1 })],
  ["field seed +1", () => select(basePlayers, { ...seeds, field: seeds.field + 1 })],
];
const rows = perturbations.map(([name, run]) => {
  const r = run();
  return { perturbation: name, overlapWithBase: Math.round(100 * jaccard(base.chosen, r.chosen)) / 100,
    exposureL1: Math.round(100 * l1(exposures(base.chosen), exposures(r.chosen))) / 100,
    forecastDelta: Math.round(1000 * (r.forecast - base.forecast)) / 1000, fieldMaxDevPct: r.calibration.maxAbsDeviationPct };
});
const out = { version: "nfl-portfolio-robustness-v1", pairId: pair.pairId, base: { reproducesSaved: jaccard(base.chosen, savedKeys), forecast: base.forecast },
  candidates: candidates.length, constraints, rows,
  note: "Descriptive. Overlap is Jaccard on lineup keys; exposure L1 sums |exposure difference| over players (0 = identical, 2 = disjoint)." };
writeFileSync(outPath, JSON.stringify(out, null, 1), { flag: "wx" });
console.table(rows);
console.log(JSON.stringify({ reproducesSaved: out.base.reproducesSaved, baseForecast: out.base.forecast }));
