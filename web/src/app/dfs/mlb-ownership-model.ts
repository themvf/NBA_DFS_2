import "server-only";

import { existsSync, readFileSync } from "node:fs";
import path from "node:path";

const TARGET_OFFSET = 0.1;
const MIN_SCORE = 0.05;

type Role = "hitter" | "pitcher";
type ProjectionMode = "field" | "our";

type RoleArtifact = {
  featureOrder: string[];
  intercept: number;
  coefficients: number[];
  means: number[];
  scales: number[];
  fillValues: Record<string, number>;
  budget: number;
  minScore?: number;
};

type OwnershipArtifact = {
  modelVersion: string;
  roles: Partial<Record<Role, RoleArtifact>>;
};

type FeatureRow = {
  originalIndex: number;
  eligiblePositions: string;
  salaryK: number | null;
  baselineOwn: number | null;
  baselineOwnRank?: number;
  projection: number | null;
  valueX: number | null;
  projectionRank?: number;
  salaryRank?: number;
  valueRank?: number;
  lineupOrderNorm: number | null;
  hasLineupOrder: number;
  lineupConfirmed: number;
  isTop4: number;
  isLeadoff: number;
  teamImplied: number | null;
  teamImpliedRank?: number;
  oppImplied: number | null;
  oppImpliedRank?: number;
  teamWinProb: number | null;
  teamWinProbRank?: number;
  vegasTotal: number | null;
  isHome: number;
  primaryPos: string;
};

export type MlbOwnershipPlayerLike = {
  eligiblePositions: string;
  salary: number;
  isOut: boolean | null;
  ourProj: number | null;
  linestarProj?: number | null;
  linestarOwnPct?: number | null;
  projOwnPct?: number | null;
  avgFptsDk?: number | null;
  ourOwnPct?: number | null;
  ourLeverage?: number | null;
  dkStartingLineupOrder?: number | null;
  dkTeamLineupConfirmed?: boolean | null;
  teamImplied?: number | null;
  oppImplied?: number | null;
  teamMl?: number | null;
  vegasTotal?: number | null;
  isHome?: boolean | null;
};

let artifactCache: OwnershipArtifact | null | undefined;

function safeNumber(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string" && value.trim().length > 0) {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) return parsed;
  }
  return null;
}

function sanitizeOwnershipPct(value: unknown): number | null {
  const numeric = safeNumber(value);
  if (numeric == null) return null;
  return Math.max(0, Math.min(100, numeric));
}

function sanitizeProjection(value: unknown): number | null {
  const numeric = safeNumber(value);
  if (numeric == null) return null;
  return Math.max(0, numeric);
}

function isPitcherPos(eligiblePositions: string | null | undefined): boolean {
  if (!eligiblePositions) return false;
  return eligiblePositions.split("/").some((token) => {
    const pos = token.trim().toUpperCase();
    return pos === "SP" || pos === "RP" || pos === "P";
  });
}

function primaryHitterPosition(eligiblePositions: string | null | undefined): string {
  if (!eligiblePositions) return "UNK";
  for (const token of eligiblePositions.split("/")) {
    const pos = token.trim().toUpperCase();
    if (pos !== "SP" && pos !== "RP" && pos !== "P") return pos || "UNK";
  }
  return "UNK";
}

function moneylineToProb(value: unknown): number | null {
  const numeric = safeNumber(value);
  if (numeric == null) return null;
  if (numeric >= 0) return 100 / (numeric + 100);
  const abs = Math.abs(numeric);
  return abs / (abs + 100);
}

function firstProjection(player: MlbOwnershipPlayerLike, mode: ProjectionMode): number | null {
  const preferred = mode === "our"
    ? [player.ourProj, player.linestarProj, player.avgFptsDk]
    : [player.linestarProj, player.ourProj, player.avgFptsDk];
  for (const value of preferred) {
    const projection = sanitizeProjection(value);
    if (projection != null && projection > 0) return projection;
  }
  return null;
}

function percentileRank(value: number | null, values: Array<number | null>): number {
  if (value == null) return 0.5;
  const numeric = values.filter((entry): entry is number => entry != null && Number.isFinite(entry));
  if (numeric.length === 0) return 0.5;
  let below = 0;
  let equal = 0;
  for (const entry of numeric) {
    if (entry < value) below++;
    else if (entry === value) equal++;
  }
  return (below + equal * 0.5) / numeric.length;
}

function normalizeScores(scores: Array<{ idx: number; score: number }>, budget: number): Map<number, number> {
  const valid = scores.filter(({ score }) => Number.isFinite(score) && score > 0);
  const total = valid.reduce((sum, entry) => sum + entry.score, 0);
  if (!Number.isFinite(total) || total <= 0) return new Map();
  const result = new Map<number, number>();
  for (const { idx, score } of valid) {
    const ownPct = sanitizeOwnershipPct((score / total) * budget);
    if (ownPct != null) result.set(idx, ownPct);
  }
  return result;
}

function resolveArtifactPath(): string | null {
  const candidates = [
    path.resolve(process.cwd(), "model", "mlb_ownership_v1.json"),
    path.resolve(process.cwd(), "..", "model", "mlb_ownership_v1.json"),
    path.resolve(process.cwd(), "data", "reports", "mlb_ownership_v1.json"),
    path.resolve(process.cwd(), "..", "data", "reports", "mlb_ownership_v1.json"),
  ];
  for (const candidate of candidates) {
    if (existsSync(candidate)) return candidate;
  }
  return null;
}

function loadArtifact(): OwnershipArtifact | null {
  if (artifactCache !== undefined) return artifactCache;
  const artifactPath = resolveArtifactPath();
  if (!artifactPath) {
    artifactCache = null;
    return artifactCache;
  }
  try {
    artifactCache = JSON.parse(readFileSync(artifactPath, "utf8")) as OwnershipArtifact;
  } catch {
    artifactCache = null;
  }
  return artifactCache;
}

function buildFeatureRows(players: MlbOwnershipPlayerLike[], role: Role, mode: ProjectionMode): FeatureRow[] {
  const baselineKey = mode === "our" ? "ourOwnPct" : "linestarOwnPct";
  const rows = players.flatMap((player, originalIndex) => {
    if (player.isOut || player.salary <= 0) return [];
    const pitcher = isPitcherPos(player.eligiblePositions);
    if ((role === "pitcher") !== pitcher) return [];
    const salaryK = player.salary > 0 ? player.salary / 1000 : null;
    const projection = firstProjection(player, mode);
    const baselineOwn = sanitizeOwnershipPct(player[baselineKey] ?? null);
    const dkOrder = safeNumber(player.dkStartingLineupOrder ?? null);
    const teamImplied = safeNumber(player.teamImplied ?? null);
    const oppImplied = safeNumber(player.oppImplied ?? null);
    return [{
      originalIndex,
      eligiblePositions: player.eligiblePositions,
      salaryK,
      baselineOwn,
      projection,
      valueX: projection != null && salaryK != null && salaryK > 0 ? projection / salaryK : null,
      lineupOrderNorm: dkOrder != null && dkOrder > 0 ? (10 - dkOrder) / 9 : null,
      hasLineupOrder: dkOrder != null && dkOrder > 0 ? 1 : 0,
      lineupConfirmed: player.dkTeamLineupConfirmed ? 1 : 0,
      isTop4: dkOrder != null && dkOrder <= 4 ? 1 : 0,
      isLeadoff: dkOrder === 1 ? 1 : 0,
      teamImplied,
      oppImplied,
      teamWinProb: moneylineToProb(player.teamMl ?? null),
      vegasTotal: safeNumber(player.vegasTotal ?? null),
      isHome: player.isHome ? 1 : 0,
      primaryPos: primaryHitterPosition(player.eligiblePositions),
    }];
  });

  const baselineOwns = rows.map((row) => row.baselineOwn);
  const projections = rows.map((row) => row.projection);
  const salaries = rows.map((row) => row.salaryK);
  const values = rows.map((row) => row.valueX);
  const teamImplieds = rows.map((row) => row.teamImplied);
  const oppImplieds = rows.map((row) => row.oppImplied);
  const teamWinProbs = rows.map((row) => row.teamWinProb);

  return rows.map((row) => ({
    ...row,
    baselineOwnRank: percentileRank(row.baselineOwn, baselineOwns),
    projectionRank: percentileRank(row.projection, projections),
    salaryRank: percentileRank(row.salaryK, salaries),
    valueRank: percentileRank(row.valueX, values),
    teamImpliedRank: percentileRank(row.teamImplied, teamImplieds),
    oppImpliedRank: percentileRank(row.oppImplied, oppImplieds),
    teamWinProbRank: percentileRank(row.teamWinProb, teamWinProbs),
  }));
}

function featureValue(row: ReturnType<typeof buildFeatureRows>[number], feature: string): number | null {
  switch (feature) {
    case "baseline_own":
      return row.baselineOwn;
    case "baseline_own_rank":
      return row.baselineOwnRank ?? null;
    case "projection":
      return row.projection;
    case "salary_k":
      return row.salaryK;
    case "value_x":
      return row.valueX;
    case "projection_rank":
      return row.projectionRank ?? null;
    case "salary_rank":
      return row.salaryRank ?? null;
    case "value_rank":
      return row.valueRank ?? null;
    case "lineup_order_norm":
      return row.lineupOrderNorm;
    case "has_lineup_order":
      return row.hasLineupOrder;
    case "lineup_confirmed":
      return row.lineupConfirmed;
    case "is_top4":
      return row.isTop4;
    case "is_leadoff":
      return row.isLeadoff;
    case "team_implied":
      return row.teamImplied;
    case "team_implied_rank":
      return row.teamImpliedRank ?? null;
    case "opp_implied":
      return row.oppImplied;
    case "opp_implied_rank":
      return row.oppImpliedRank ?? null;
    case "team_win_prob":
      return row.teamWinProb;
    case "team_win_prob_rank":
      return row.teamWinProbRank ?? null;
    case "vegas_total":
      return row.vegasTotal;
    case "is_home":
      return row.isHome;
    case "pos_c":
      return row.primaryPos === "C" ? 1 : 0;
    case "pos_1b":
      return row.primaryPos === "1B" ? 1 : 0;
    case "pos_2b":
      return row.primaryPos === "2B" ? 1 : 0;
    case "pos_3b":
      return row.primaryPos === "3B" ? 1 : 0;
    case "pos_ss":
      return row.primaryPos === "SS" ? 1 : 0;
    case "pos_of":
      return row.primaryPos === "OF" ? 1 : 0;
    default:
      return null;
  }
}

function predictRole(players: MlbOwnershipPlayerLike[], role: Role, mode: ProjectionMode, artifact: RoleArtifact): Map<number, number> {
  const featureRows = buildFeatureRows(players, role, mode).filter((row) => row.baselineOwn != null);
  if (!featureRows.length) return new Map();

  const scored = featureRows.map((row) => {
    const scaled = artifact.featureOrder.map((feature, index) => {
      const raw = featureValue(row, feature);
      const fill = artifact.fillValues[feature] ?? 0;
      const value = raw != null && Number.isFinite(raw) ? raw : fill;
      const mean = artifact.means[index] ?? 0;
      const scale = artifact.scales[index] && artifact.scales[index] !== 0 ? artifact.scales[index] : 1;
      return ((value - mean) / scale) * (artifact.coefficients[index] ?? 0);
    });
    const residual = artifact.intercept + scaled.reduce((sum, value) => sum + value, 0);
    const baseline = row.baselineOwn ?? 0;
    const score = Math.max(
      artifact.minScore ?? MIN_SCORE,
      Math.exp(residual) * (baseline + TARGET_OFFSET) - TARGET_OFFSET,
    );
    return { idx: row.originalIndex, score };
  });

  return normalizeScores(scored, artifact.budget);
}

export function applyMlbOwnershipModelV1(players: MlbOwnershipPlayerLike[]): number {
  const artifact = loadArtifact();
  if (!artifact) return 0;
  const hitterArtifact = artifact.roles.hitter;
  const pitcherArtifact = artifact.roles.pitcher;
  if (!hitterArtifact || !pitcherArtifact) return 0;

  const fieldMap = new Map<number, number>([
    ...predictRole(players, "hitter", "field", hitterArtifact),
    ...predictRole(players, "pitcher", "field", pitcherArtifact),
  ]);
  const ourMap = new Map<number, number>([
    ...predictRole(players, "hitter", "our", hitterArtifact),
    ...predictRole(players, "pitcher", "our", pitcherArtifact),
  ]);

  let applied = 0;
  for (let i = 0; i < players.length; i++) {
    if (players[i].isOut) continue;
    const fieldOwn = fieldMap.get(i);
    if (fieldOwn != null) {
      players[i].projOwnPct = sanitizeOwnershipPct(fieldOwn);
      applied++;
    }
    const ourOwn = ourMap.get(i);
    if (ourOwn != null) players[i].ourOwnPct = sanitizeOwnershipPct(ourOwn);
  }
  return applied;
}
