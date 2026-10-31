/**
 * Fitted Classic field ownership (`nfl-ownership-fit-v1`), run as a SHADOW
 * beside the v5 prior since 2026-10-31 (owner's decision): the page shows it and
 * every saved build records it, but builds keep reading v5. A port of
 * model/nfl_ownership_fit.py; the coefficients are frozen in
 * src/data/nfl-ownership-fit-v1.json by `python -m research.nfl_ownership_fit
 * --freeze`, and the shared fixture (tests/fixtures/nfl_ownership_fit_fixture.json)
 * pins both languages. The switch rule is registered in
 * docs/nfl-ownership-model.md ("Classic shadow").
 *
 * Leave-one-slate-out on the seven week 1-7 Millionaires: rank agreement 0.826
 * vs v5's 0.797, MAE 1.243 vs 1.256; walk-forward it was better on rank and
 * worse on points (1.50 vs 1.39), and 20%+ chalk is still predicted ~16 against ~29.
 */
import model from "@/data/nfl-ownership-fit-v1.json";
import { allocateBudgetDetailed } from "./ownership-prior";

export const NFL_OWNERSHIP_FIT_CLASSIC_VERSION = "nfl-ownership-fit-v1";
export const NFL_OWNERSHIP_FIT_CLASSIC_FITTED_AT: string = model.fitted_at;
const POSITIONS = new Set(["QB", "RB", "WR", "TE", "DST"]);
const VALUE_SALARY_FLOOR = 3000;
const TEAM_POS_RANK_CLIP = 4;

export interface ClassicFitPlayer {
  id: number;
  position: string;
  salary: number;
  team: string | null;
  /** Our projection; null when the model has none. */
  proj: number | null;
  /** DraftKings' printed average; null when absent. */
  dkAvg: number | null;
  dkStatus: string | null;
  /** "AWAY@HOME" (read from DraftKings' game info). */
  gameKey: string | null;
  /** Out players and anyone outside QB/RB/WR/TE/DST get 0 and are left out of every rank. */
  active: boolean;
}

/** The game key the model was trained on: the leading "AAA@BBB" of DraftKings' game info. */
export function classicFitGameKey(gameInfo: string | null | undefined): string {
  const info = gameInfo ?? "";
  const m = /^\s*([A-Z]{2,4}@[A-Z]{2,4})/.exec(info);
  return m ? m[1] : info.slice(0, 12);
}

/** Ascending rank with ties averaged, 1-based (pandas method="average"). */
function averageRanks(values: readonly number[]): number[] {
  const order = values.map((v, i) => [v, i] as const).sort((a, b) => a[0] - b[0]);
  const out = new Array<number>(values.length);
  for (let i = 0; i < order.length;) {
    let j = i;
    while (j + 1 < order.length && order[j + 1][0] === order[i][0]) j += 1;
    for (let k = i; k <= j; k += 1) out[order[k][1]] = (i + j) / 2 + 1;
    i = j + 1;
  }
  return out;
}

/** Tie-averaged percentile rank in [0, 1]; a lone value is 0.5. */
function pctRanks(values: readonly number[]): number[] {
  if (values.length <= 1) return values.map(() => 0.5);
  return averageRanks(values).map((r) => (r - 1) / (values.length - 1));
}

function groupIndices(keys: readonly string[]): Map<string, number[]> {
  const groups = new Map<string, number[]>();
  keys.forEach((k, i) => { const g = groups.get(k); if (g) g.push(i); else groups.set(k, [i]); });
  return groups;
}

/** Feature rows in model.features order, one per active player given (one slate). */
export function classicFitFeatures(players: readonly ClassicFitPlayer[]): number[][] {
  const pos = players.map((p) => (p.position ?? "").toUpperCase());
  const projMissing = players.map((p) => (p.proj == null || !Number.isFinite(p.proj) ? 1 : 0));
  const dkMissing = players.map((p) => (p.dkAvg == null || !Number.isFinite(p.dkAvg) ? 1 : 0));
  const proj = players.map((p, i) => (projMissing[i] ? 0 : Math.max(p.proj as number, 0)));
  const dk = players.map((p, i) => (dkMissing[i] ? 0 : Math.max(p.dkAvg as number, 0)));
  const salary = players.map((p) => Number(p.salary));
  const value = proj.map((v, i) => v / (Math.max(salary[i], VALUE_SALARY_FLOOR) / 1000));
  const n = players.length;
  const rankProj = new Array<number>(n), rankValue = new Array<number>(n), rankDk = new Array<number>(n),
    rankSalary = new Array<number>(n), logPool = new Array<number>(n);
  for (const ix of groupIndices(pos).values()) {
    const pick = (arr: number[]) => ix.map((i) => arr[i]);
    const set = (target: number[], ranks: number[]) => ix.forEach((i, k) => { target[i] = ranks[k]; });
    set(rankProj, pctRanks(pick(proj)));
    set(rankValue, pctRanks(pick(value)));
    set(rankDk, pctRanks(pick(dk)));
    set(rankSalary, pctRanks(pick(salary)));
    ix.forEach((i) => { logPool[i] = Math.log(ix.length); });
  }
  const slateTotal = proj.reduce((a, b) => a + b, 0) || 1;
  const teamKey = players.map((p) => p.team ?? "\u0000none");
  const teamTotal = new Map<string, number>();
  teamKey.forEach((t, i) => teamTotal.set(t, (teamTotal.get(t) ?? 0) + proj[i]));
  const gameKey = players.map((p) => p.gameKey ?? "");
  const gameTotals = new Map<string, number>();
  gameKey.forEach((g, i) => gameTotals.set(g, (gameTotals.get(g) ?? 0) + proj[i]));
  const games = [...gameTotals.keys()];
  const gameRanks = pctRanks(games.map((g) => gameTotals.get(g) as number));
  const gameRank = new Map(games.map((g, i) => [g, gameRanks[i]]));
  // Within team and position, descending, ties by order of appearance (pandas method="first").
  const teamPosRank = new Array<number>(n);
  for (const ix of groupIndices(players.map((_, i) => `${teamKey[i]}|${pos[i]}`)).values()) {
    [...ix].sort((a, b) => proj[b] - proj[a] || a - b).forEach((i, k) => { teamPosRank[i] = Math.min(k + 1, TEAM_POS_RANK_CLIP); });
  }
  return players.map((p, i) => {
    const status = (p.dkStatus ?? "").trim().toUpperCase();
    const row: Record<string, number> = {
      log_salary: Math.log(Math.max(salary[i], 100) / 1000), proj: proj[i], proj_missing: projMissing[i], dk_avg: dk[i],
      dk_missing: dkMissing[i], value: value[i], rank_proj: rankProj[i], rank_value: rankValue[i], rank_dk: rankDk[i],
      rank_salary: rankSalary[i], team_share: (teamTotal.get(teamKey[i]) as number) / slateTotal,
      game_rank: gameRank.get(gameKey[i]) as number, team_pos_rank: teamPosRank[i],
      status_q: status === "Q" ? 1 : 0, status_d: status === "D" ? 1 : 0, log_pool: logPool[i],
    };
    return model.features.map((f) => row[f]);
  });
}

type Group = { coefficients: number[]; intercept: number; mean: number[]; scale: number[] };

/** Ownership percent by id; inactive players get 0. Each position is allocated to its mean recorded total, capped at 60. */
export function projectClassicFit(players: readonly ClassicFitPlayer[]): Map<number, number> {
  const out = new Map(players.map((p) => [p.id, 0]));
  const ok = players.filter((p) => p.active && POSITIONS.has((p.position ?? "").toUpperCase()) && p.salary > 0);
  if (!ok.length) return out;
  const x = classicFitFeatures(ok);
  const groups = model.groups as Record<string, Group>;
  const budgets = model.budgets as Record<string, number>;
  for (const [position, budget] of Object.entries(budgets)) {
    const group = groups[position];
    if (!group) continue;
    const scores = new Map<number, number>();
    ok.forEach((p, i) => {
      if ((p.position ?? "").toUpperCase() !== position) return;
      const yhat = x[i].reduce((acc, v, k) => acc + ((v - group.mean[k]) / group.scale[k]) * group.coefficients[k], group.intercept);
      scores.set(p.id, Math.max(Math.exp(Math.min(Math.max(yhat, -30), 30)) - model.target_offset, 0));
    });
    for (const [id, pct] of allocateBudgetDetailed(scores, budget, model.cap).shares) out.set(id, Math.round(pct * 100) / 100);
  }
  return out;
}
