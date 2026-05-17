import fs from "fs";
import path from "path";

import { sql } from "drizzle-orm";

type Row = {
  slateDate: string;
  dkPlayerId: number;
  name: string;
  salary: number;
  ourProj: number | null;
  actualFpts: number | null;
  actualHr: number | null;
  expectedHr: number | null;
  hrProb1Plus: number | null;
  homeImplied: number | null;
  awayImplied: number | null;
  vegasTotal: number | null;
  isHome: boolean | null;
  ballpark: string | null;
  weatherTemp: number | null;
  windSpeed: number | null;
  windDirection: string | null;
  parkHrFactor: number | null;
  storedProjCeiling: number | null;
  storedBoomRate: number | null;
};

type VersionMetrics = {
  rows: number;
  slates: number;
  ceilingCoverage: number;
  avgCeilingGap: number;
  avgMissAmount: number;
  ceilingCorr: number | null;
  boomBrier: number;
  boomAccuracy: number;
  top10AvgActual: number;
  top10Hit25Rate: number;
  top10Hit30Rate: number;
};

type CeilingParams = {
  name: string;
  powerHrWeight: number;
  envGateHrProb: number;
  envGateExpHr: number;
  teamBoostWeight: number;
  envBoostWeight: number;
  impliedBonusWeight: number;
  boomProjectionDivisor: number;
  boomTeamWeight: number;
  boomEnvWeight: number;
};

const MLB_LEAGUE_AVG_TEAM_TOTAL = 4.5;
const MLB_DEFAULT_OUTFIELD_BEARING = 45;
const MLB_DEFAULT_WIND_EXPOSURE = 1;
const MLB_PARK_ENV_METADATA: Array<{ alias: string; bearing: number; exposure: number }> = [
  { alias: "american family field", bearing: 10, exposure: 0.35 },
  { alias: "angel stadium", bearing: 25, exposure: 1 },
  { alias: "busch stadium", bearing: 20, exposure: 1 },
  { alias: "chase field", bearing: 20, exposure: 0.35 },
  { alias: "citi field", bearing: 25, exposure: 1 },
  { alias: "citizens bank park", bearing: 25, exposure: 1 },
  { alias: "comerica park", bearing: 20, exposure: 1 },
  { alias: "coors field", bearing: 25, exposure: 1 },
  { alias: "daikin park", bearing: 32, exposure: 0.35 },
  { alias: "fenway park", bearing: 35, exposure: 1 },
  { alias: "globe life field", bearing: 35, exposure: 0.35 },
  { alias: "kauffman stadium", bearing: 15, exposure: 1 },
  { alias: "loandepot park", bearing: 10, exposure: 0.35 },
  { alias: "nationals park", bearing: 20, exposure: 1 },
  { alias: "oracle park", bearing: 60, exposure: 1 },
  { alias: "oriole park at camden yards", bearing: 45, exposure: 1 },
  { alias: "camden yards", bearing: 45, exposure: 1 },
  { alias: "petco park", bearing: 35, exposure: 1 },
  { alias: "pnc park", bearing: 25, exposure: 1 },
  { alias: "progressive field", bearing: 35, exposure: 1 },
  { alias: "rate field", bearing: 35, exposure: 1 },
  { alias: "rogers centre", bearing: 35, exposure: 0.35 },
  { alias: "sutter health park", bearing: 35, exposure: 1 },
  { alias: "t-mobile park", bearing: 45, exposure: 0.45 },
  { alias: "target field", bearing: 30, exposure: 1 },
  { alias: "tropicana field", bearing: 45, exposure: 0 },
  { alias: "truist park", bearing: 35, exposure: 1 },
  { alias: "wrigley field", bearing: 40, exposure: 1 },
  { alias: "yankee stadium", bearing: 60, exposure: 1 },
  { alias: "dodger stadium", bearing: 55, exposure: 1 },
  { alias: "uniqlo field at dodger stadium", bearing: 55, exposure: 1 },
  { alias: "great american ball park", bearing: 30, exposure: 1 },
];

function loadDatabaseUrl() {
  if (process.env.DATABASE_URL) return;
  const candidates = [
    path.join(process.cwd(), ".env.local"),
    path.join(process.cwd(), "..", ".env"),
  ];
  for (const envPath of candidates) {
    if (!fs.existsSync(envPath)) continue;
    const line = fs.readFileSync(envPath, "utf8")
      .split(/\r?\n/)
      .find((entry) => entry.startsWith("DATABASE_URL="));
    if (line) {
      process.env.DATABASE_URL = line.slice("DATABASE_URL=".length);
      return;
    }
  }
  throw new Error("DATABASE_URL missing in web/.env.local or repo .env");
}

function clamp(v: number, lo: number, hi: number) {
  return Math.max(lo, Math.min(hi, v));
}

function roundMetric(value: number, digits = 4) {
  const scale = 10 ** digits;
  return Math.round(value * scale) / scale;
}

function compassDegrees(direction: string): number | null {
  const normalized = direction.trim().toLowerCase();
  const mapping: Record<string, number> = {
    n: 0, ne: 45, e: 90, se: 135, s: 180, sw: 225, w: 270, nw: 315,
  };
  return mapping[normalized] ?? null;
}

function angularDifference(a: number, b: number): number {
  const diff = Math.abs(a - b) % 360;
  return diff > 180 ? 360 - diff : diff;
}

function parkMeta(ballpark: string | null | undefined): { bearing: number; exposure: number } {
  const normalized = ballpark?.trim().toLowerCase() ?? "";
  const exact = MLB_PARK_ENV_METADATA.find((entry) => entry.alias === normalized);
  if (exact) return exact;
  const partial = MLB_PARK_ENV_METADATA.find((entry) => normalized.includes(entry.alias));
  if (partial) return partial;
  return { bearing: MLB_DEFAULT_OUTFIELD_BEARING, exposure: MLB_DEFAULT_WIND_EXPOSURE };
}

function windDirectionalLean(row: Row): number {
  const rawDirection = row.windDirection;
  if (!rawDirection) return 0;
  const normalized = rawDirection.trim().toLowerCase();
  if (normalized.includes("out")) return 1;
  if (normalized.includes("in")) return -1;
  const fromDegrees = compassDegrees(rawDirection);
  if (fromDegrees == null) return 0;
  const toDegrees = (fromDegrees + 180) % 360;
  const diff = angularDifference(toDegrees, parkMeta(row.ballpark).bearing);
  if (diff <= 45) return 1;
  if (diff >= 135) return -1;
  return 0;
}

function environmentHrFactor(row: Row): number {
  let hrPf = clamp(row.parkHrFactor ?? 1.0, 0.7, 1.5);
  if (row.weatherTemp != null) {
    const tempDelta = clamp((row.weatherTemp - 72) / 18, -1, 1);
    hrPf *= 1 + tempDelta * 0.05;
  }
  if (row.windSpeed != null) {
    const windLean = windDirectionalLean(row);
    if (windLean !== 0) {
      const windScale = clamp((row.windSpeed - 4) / 16, 0, 1) * parkMeta(row.ballpark).exposure;
      hrPf *= 1 + windLean * 0.10 * windScale;
    }
  }
  return clamp(hrPf, 0.65, 1.65);
}

function impliedTeamTotal(row: Row): number {
  if (row.isHome == null) return MLB_LEAGUE_AVG_TEAM_TOTAL;
  return row.isHome
    ? (row.homeImplied ?? (row.vegasTotal ?? 9) / 2)
    : (row.awayImplied ?? (row.vegasTotal ?? 9) / 2);
}

function legacyDistribution(row: Row): { ceiling: number; boom: number } | null {
  const projection = row.ourProj;
  if (projection == null || projection <= 0) return null;
  const hrProb = clamp(row.hrProb1Plus ?? 0, 0, 0.9999);
  const expHr = Math.max(0, row.expectedHr ?? 0);
  return {
    ceiling: (projection * 1.75) + (hrProb * 14) + (expHr * 4),
    boom: clamp(0.035 + (projection / 220) + (hrProb * 0.62) + (expHr * 0.12), 0.02, 0.55),
  };
}

function currentDistribution(row: Row): { ceiling: number; boom: number } | null {
  return tunedDistribution(row, {
    name: "current",
    powerHrWeight: 4,
    envGateHrProb: 0.16,
    envGateExpHr: 0.16,
    teamBoostWeight: 0.04,
    envBoostWeight: 0.04,
    impliedBonusWeight: 0.65,
    boomProjectionDivisor: 255,
    boomTeamWeight: 0.01,
    boomEnvWeight: 0.01,
  });
}

function tunedDistribution(row: Row, params: CeilingParams): { ceiling: number; boom: number } | null {
  const projection = row.ourProj;
  if (projection == null || projection <= 0) return null;
  const hrProb = clamp(row.hrProb1Plus ?? 0, 0, 0.9999);
  const expHr = Math.max(0, row.expectedHr ?? 0);
  const implied = impliedTeamTotal(row);
  const hrPf = environmentHrFactor(row);
  const teamTotalBoost = clamp((implied - MLB_LEAGUE_AVG_TEAM_TOTAL) / 2, -0.35, 0.75);
  const hrEnvBoost = clamp((hrPf - 1.0) / 0.35, -0.4, 0.85);
  const envGate = (hrProb >= params.envGateHrProb || expHr >= params.envGateExpHr) ? 1 : 0;
  const gatedTeamBoost = Math.max(0, teamTotalBoost) * params.teamBoostWeight * envGate;
  const gatedEnvBoost = Math.max(0, hrEnvBoost) * params.envBoostWeight * envGate;
  const ceilingMult = clamp(1.72 + gatedTeamBoost + gatedEnvBoost, 1.55, 2.0);
  return {
    ceiling: (projection * ceilingMult) + (hrProb * 14) + (expHr * params.powerHrWeight) + Math.max(0, implied - 4.6) * params.impliedBonusWeight * envGate,
    boom: clamp(
      0.03
      + (projection / params.boomProjectionDivisor)
      + (hrProb * 0.62)
      + (expHr * 0.12)
      + Math.max(0, teamTotalBoost) * params.boomTeamWeight * envGate
      + Math.max(0, hrEnvBoost) * params.boomEnvWeight * envGate,
      0.02,
      0.62,
    ),
  };
}

function pearson(xs: number[], ys: number[]): number | null {
  if (xs.length !== ys.length || xs.length < 2) return null;
  const mx = xs.reduce((a, b) => a + b, 0) / xs.length;
  const my = ys.reduce((a, b) => a + b, 0) / ys.length;
  let num = 0;
  let dx = 0;
  let dy = 0;
  for (let i = 0; i < xs.length; i++) {
    const xv = xs[i] - mx;
    const yv = ys[i] - my;
    num += xv * yv;
    dx += xv * xv;
    dy += yv * yv;
  }
  if (dx <= 0 || dy <= 0) return null;
  return num / Math.sqrt(dx * dy);
}

function evaluateVersion(
  rows: Row[],
  pick: (row: Row) => { ceiling: number; boom: number } | null,
): VersionMetrics {
  const usable = rows
    .map((row) => ({ row, dist: pick(row) }))
    .filter((entry): entry is { row: Row; dist: { ceiling: number; boom: number } } => (
      entry.dist != null
      && entry.row.actualFpts != null
    ));

  const bySlate = new Map<string, Array<{ row: Row; dist: { ceiling: number; boom: number } }>>();
  for (const entry of usable) {
    const slateRows = bySlate.get(entry.row.slateDate) ?? [];
    slateRows.push(entry);
    bySlate.set(entry.row.slateDate, slateRows);
  }

  let covered = 0;
  let ceilingGapSum = 0;
  let missAmountSum = 0;
  let missCount = 0;
  let brierSum = 0;
  let boomCorrect = 0;
  const ceilingValues: number[] = [];
  const actualValues: number[] = [];
  let top10ActualSum = 0;
  let top10Count = 0;
  let top10Hit25 = 0;
  let top10Hit30 = 0;

  for (const entry of usable) {
    const actual = entry.row.actualFpts as number;
    const ceiling = entry.dist.ceiling;
    const boom = entry.dist.boom;
    ceilingValues.push(ceiling);
    actualValues.push(actual);
    ceilingGapSum += ceiling - actual;
    if (actual <= ceiling) {
      covered += 1;
    } else {
      missCount += 1;
      missAmountSum += actual - ceiling;
    }
    const hitBoom = actual >= 25 ? 1 : 0;
    brierSum += (boom - hitBoom) ** 2;
    if ((boom >= 0.5 ? 1 : 0) === hitBoom) boomCorrect += 1;
  }

  for (const slateRows of bySlate.values()) {
    const top = [...slateRows]
      .sort((a, b) => b.dist.ceiling - a.dist.ceiling)
      .slice(0, 10);
    for (const entry of top) {
      const actual = entry.row.actualFpts as number;
      top10ActualSum += actual;
      top10Count += 1;
      if (actual >= 25) top10Hit25 += 1;
      if (actual >= 30) top10Hit30 += 1;
    }
  }

  return {
    rows: usable.length,
    slates: bySlate.size,
    ceilingCoverage: roundMetric(covered / usable.length),
    avgCeilingGap: roundMetric(ceilingGapSum / usable.length, 3),
    avgMissAmount: roundMetric(missCount > 0 ? missAmountSum / missCount : 0, 3),
    ceilingCorr: pearson(ceilingValues, actualValues) == null ? null : roundMetric(pearson(ceilingValues, actualValues) as number),
    boomBrier: roundMetric(brierSum / usable.length),
    boomAccuracy: roundMetric(boomCorrect / usable.length),
    top10AvgActual: roundMetric(top10ActualSum / Math.max(1, top10Count), 3),
    top10Hit25Rate: roundMetric(top10Hit25 / Math.max(1, top10Count)),
    top10Hit30Rate: roundMetric(top10Hit30 / Math.max(1, top10Count)),
  };
}

async function main() {
  loadDatabaseUrl();
  const { db } = await import("../src/db");

  const result = await db.execute<Row>(sql`
    WITH latest_park AS (
      SELECT DISTINCT ON (team_id)
        team_id,
        hr_factor
      FROM mlb_park_factors
      ORDER BY team_id, season DESC, id DESC
    )
    SELECT
      ds.slate_date::text AS "slateDate",
      dp.dk_player_id AS "dkPlayerId",
      dp.name,
      dp.salary,
      dp.our_proj AS "ourProj",
      dp.actual_fpts AS "actualFpts",
      dp.actual_hr AS "actualHr",
      dp.expected_hr AS "expectedHr",
      dp.hr_prob_1plus AS "hrProb1Plus",
      mm.home_implied AS "homeImplied",
      mm.away_implied AS "awayImplied",
      mm.vegas_total AS "vegasTotal",
      CASE
        WHEN dp.mlb_team_id = mm.home_team_id THEN true
        WHEN dp.mlb_team_id = mm.away_team_id THEN false
        ELSE null
      END AS "isHome",
      mm.ballpark,
      mm.weather_temp::double precision AS "weatherTemp",
      mm.wind_speed::double precision AS "windSpeed",
      mm.wind_direction AS "windDirection",
      park.hr_factor::double precision AS "parkHrFactor",
      dp.proj_ceiling AS "storedProjCeiling",
      dp.boom_rate AS "storedBoomRate"
    FROM dk_players dp
    JOIN dk_slates ds ON ds.id = dp.slate_id
    LEFT JOIN mlb_matchups mm ON mm.id = dp.matchup_id
    LEFT JOIN latest_park park ON park.team_id = mm.home_team_id
    WHERE ds.sport = 'mlb'
      AND dp.actual_fpts IS NOT NULL
      AND dp.expected_hr IS NOT NULL
      AND dp.hr_prob_1plus IS NOT NULL
      AND NOT COALESCE(dp.is_out, false)
      AND COALESCE(dp.eligible_positions, '') NOT LIKE '%SP%'
      AND COALESCE(dp.eligible_positions, '') NOT LIKE '%RP%'
    ORDER BY ds.slate_date, dp.dk_player_id
  `);

  const rows = result.rows;
  const legacy = evaluateVersion(rows, legacyDistribution);
  const current = evaluateVersion(rows, currentDistribution);
  const storedSubset = rows.filter((row) => row.storedProjCeiling != null && row.storedBoomRate != null);
  const stored = evaluateVersion(storedSubset, (row) => (
    row.storedProjCeiling == null || row.storedBoomRate == null
      ? null
      : { ceiling: row.storedProjCeiling, boom: row.storedBoomRate }
  ));

  const candidates: CeilingParams[] = [];
  for (const powerHrWeight of [4, 4.5, 5]) {
    for (const envGateHrProb of [0.08, 0.12, 0.16]) {
      for (const envGateExpHr of [0.08, 0.12, 0.16]) {
        for (const teamBoostWeight of [0.04, 0.06, 0.08]) {
          for (const envBoostWeight of [0.04, 0.06, 0.08]) {
            for (const impliedBonusWeight of [0.35, 0.5, 0.65]) {
              for (const boomProjectionDivisor of [235, 245, 255]) {
                for (const boomTeamWeight of [0.01, 0.02, 0.03]) {
                  for (const boomEnvWeight of [0.01, 0.02, 0.03]) {
                    candidates.push({
                      name: `g${envGateHrProb}/${envGateExpHr}-c${teamBoostWeight}/${envBoostWeight}-hr${powerHrWeight}-ib${impliedBonusWeight}-bp${boomProjectionDivisor}-bb${boomTeamWeight}/${boomEnvWeight}`,
                      powerHrWeight,
                      envGateHrProb,
                      envGateExpHr,
                      teamBoostWeight,
                      envBoostWeight,
                      impliedBonusWeight,
                      boomProjectionDivisor,
                      boomTeamWeight,
                      boomEnvWeight,
                    });
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  const scoredCandidates = candidates
    .map((params) => {
      const metrics = evaluateVersion(rows, (row) => tunedDistribution(row, params));
      const score = (
        (metrics.top10Hit30Rate * 8)
        + (metrics.top10Hit25Rate * 5)
        + (metrics.ceilingCoverage * 1.5)
        - (metrics.boomBrier * 8)
        - (metrics.avgCeilingGap * 0.03)
      );
      return { params, metrics, score: roundMetric(score, 5) };
    })
    .filter((entry) => (
      entry.metrics.boomBrier <= legacy.boomBrier + 0.0015
      && entry.metrics.top10Hit25Rate >= legacy.top10Hit25Rate
      && entry.metrics.top10Hit30Rate >= legacy.top10Hit30Rate
    ))
    .sort((a, b) => b.score - a.score);

  const best = scoredCandidates[0] ?? null;

  console.log(JSON.stringify({
    sample: {
      rows: rows.length,
      slates: new Set(rows.map((row) => row.slateDate)).size,
      dateRange: {
        start: rows[0]?.slateDate ?? null,
        end: rows[rows.length - 1]?.slateDate ?? null,
      },
    },
    versions: {
      legacy,
      current,
      stored_baseline_subset: stored,
    },
    tuned_search: {
      candidatesTried: candidates.length,
      best: best ? {
        params: best.params,
        metrics: best.metrics,
        score: best.score,
      } : null,
      top5: scoredCandidates.slice(0, 5).map((entry) => ({
        params: entry.params,
        metrics: entry.metrics,
        score: entry.score,
      })),
    },
  }, null, 2));
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
