/**
 * Fitted Showdown field ownership (`nfl-ownership-fit-showdown-v1`), the default
 * Showdown ownership since 2026-10-31 (owner's decision). A port of
 * model/nfl_showdown_ownership_fit.py; the coefficients are fitted there on every
 * imported full-field Showdown contest and written to
 * src/data/nfl-showdown-ownership-fit-v1.json. The shared fixture
 * (tests/fixtures/nfl_showdown_ownership_fit_fixture.json) pins both languages.
 *
 * Leave-one-slate-out on 17 slates, against v5 on the same rows: flex MAE 5.23
 * vs 6.16, captain MAE 1.04 vs 1.53, rank agreement 0.78/0.79 vs 0.64/0.65.
 * Still an estimate from a dozen-odd games, so it stays labelled uncalibrated.
 */
import model from "@/data/nfl-showdown-ownership-fit-v1.json";

export const NFL_OWNERSHIP_FIT_SHOWDOWN_VERSION = "nfl-ownership-fit-showdown-v1";
const POSITIONS = ["QB", "RB", "WR", "TE", "K", "DST"] as const;
const VALUE_SALARY_FLOOR = 3000;
const PUNT_SALARY_MAX = 3000;
const FLEX_BUDGET = 500, CAPTAIN_BUDGET = 100, FLEX_MAX = 90, CAPTAIN_MAX = 50, TOTAL_MAX = 95;

export interface ShowdownFitPlayer {
  id: number;
  position: string;
  salary: number;
  team: string | null;
  dkStatus: string | null;
  /** The field's points (fieldPoints in ownership-prior.ts); <= 0 means not ownable. */
  pts: number;
  isOut: boolean;
}

/** Descending rank, ties averaged (pandas rank(ascending=False, method="average")). */
function descRank(values: number[]): number[] {
  const order = values.map((v, i) => [v, i] as const).sort((a, b) => b[0] - a[0]);
  const out = new Array<number>(values.length);
  for (let i = 0; i < order.length;) {
    let j = i;
    while (j + 1 < order.length && order[j + 1][0] === order[i][0]) j += 1;
    const rank = (i + j) / 2 + 1;
    for (let k = i; k <= j; k += 1) out[order[k][1]] = rank;
    i = j + 1;
  }
  return out;
}

function groupRank(values: number[], keys: string[]): number[] {
  const out = new Array<number>(values.length);
  const groups = new Map<string, number[]>();
  keys.forEach((k, i) => { const g = groups.get(k); if (g) g.push(i); else groups.set(k, [i]); });
  for (const ix of groups.values()) {
    const ranks = descRank(ix.map((i) => values[i]));
    ix.forEach((i, n) => { out[i] = ranks[n]; });
  }
  return out;
}

/** Feature rows in model.features order, for the ownable players given (one slate). */
export function showdownFitFeatures(players: readonly ShowdownFitPlayer[]): number[][] {
  const pts = players.map((p) => p.pts);
  const total = pts.reduce((a, b) => a + b, 0);
  const pos = players.map((p) => (p.position ?? "").toUpperCase());
  const rankPts = descRank(pts);
  // pandas groupby drops a null key; such a player gets no team rank there and
  // fails the fixture, so a missing team is its own group here.
  const teamRank = groupRank(pts, players.map((p) => p.team ?? "\u0000none"));
  const posRank = groupRank(pts, pos);
  return players.map((p, i) => {
    const posR = Math.min(posRank[i], 5);
    const row: Record<string, number> = {
      pts: pts[i], logpts: Math.log(pts[i] + 1), logsal: Math.log(Math.max(p.salary, 100) / 1000),
      value: pts[i] / (Math.max(p.salary, VALUE_SALARY_FLOOR) / 1000), rank_pts: Math.min(rankPts[i], 20),
      share: pts[i] / total, team_rank: Math.min(teamRank[i], 8), pos_rank: posR,
      punt: p.salary < PUNT_SALARY_MAX ? 1 : 0, q: (p.dkStatus ?? "").trim().toUpperCase() === "Q" ? 1 : 0,
    };
    for (const position of POSITIONS) { row[`is_${position}`] = pos[i] === position ? 1 : 0; row[`r_${position}`] = pos[i] === position ? posR : 0; }
    return model.features.map((f) => row[f]);
  });
}

function scores(group: { intercept: number; coefficients: number[]; mean: number[]; scale: number[] }, x: number[][]): number[] {
  return x.map((row) => {
    const y = row.reduce((acc, v, k) => acc + ((v - group.mean[k]) / group.scale[k]) * group.coefficients[k], group.intercept);
    return Math.max(y, 0) ** 2;
  });
}

function allocate(s: number[], budget: number, caps: number[]): number[] {
  const out = s.map(() => 0);
  const open = s.map((v) => v > 0);
  let remaining = budget;
  for (let pass = 0; pass < 16; pass += 1) {
    const total = s.reduce((a, v, i) => a + (open[i] ? v : 0), 0);
    if (!open.some(Boolean) || total <= 0) break;
    const share = s.map((v, i) => (open[i] ? (remaining * v) / total : 0));
    const over = share.map((v, i) => open[i] && v > caps[i]);
    if (!over.some(Boolean)) { share.forEach((v, i) => { if (open[i]) out[i] = v; }); break; }
    over.forEach((o, i) => { if (o) { out[i] = caps[i]; remaining -= caps[i]; open[i] = false; } });
  }
  return out;
}

/** Flex and captain percent by player id; players who cannot be owned get 0. */
export function projectShowdownFit(players: readonly ShowdownFitPlayer[]): Map<number, { flex: number; captain: number }> {
  const out = new Map(players.map((p) => [p.id, { flex: 0, captain: 0 }]));
  const ok = players.filter((p) => !p.isOut && p.salary > 0 && p.pts > 0);
  if (!ok.length) return out;
  const x = showdownFitFeatures(ok);
  const captain = allocate(scores(model.captain, x), CAPTAIN_BUDGET, ok.map(() => CAPTAIN_MAX));
  const flex = allocate(scores(model.flex, x), FLEX_BUDGET, captain.map((c) => Math.min(FLEX_MAX, TOTAL_MAX - c)));
  ok.forEach((p, i) => out.set(p.id, { flex: flex[i], captain: captain[i] }));
  return out;
}
