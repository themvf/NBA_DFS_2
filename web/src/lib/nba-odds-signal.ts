import { eq, sql } from "drizzle-orm";

import { db } from "@/db";
import { ensureOddsSignalTables } from "@/db/ensure-schema";
import { oddsSignalRuns } from "@/db/schema";

export const NBA_ODDS_SIGNAL_ANALYSIS_VERSION = "v1";

type PlayerSample = {
  slateDate: string;
  slateId: number;
  name: string;
  team: string;
  teamId: number | null;
  salary: number;
  proj: number;
  actual: number;
  isOut: boolean;
  vegasTotal: number;
  teamTotal: number;
  beatProj: boolean;
  hit5x: boolean;
  hit6x: boolean;
  surplus: number;
  gameKey: string;
};

export type OddsSignalSummary = {
  n: number;
  beatProjRate: number | null;
  hit5xRate: number | null;
  hit6xRate: number | null;
  avgSurplus: number | null;
  avgActual: number | null;
  avgProj: number | null;
  avgSalary: number | null;
};

export type OddsSignalBucket = OddsSignalSummary & {
  bucket: string;
};

export type NbaOddsSignalReport = {
  sample: {
    totalProjectedWithActuals: number;
    activeProjectedWithActuals: number;
    slatesWithOddsAndActuals: string[];
  };
  overallActive: OddsSignalSummary;
  correlations: {
    vegasToActual: number | null;
    vegasToSurplus: number | null;
    vegasToHit5x: number | null;
    teamTotalToActual: number | null;
    teamTotalToSurplus: number | null;
    teamTotalToHit5x: number | null;
  };
  topTwoGamesPerSlate: {
    topGames: OddsSignalSummary;
    otherGames: OddsSignalSummary;
  };
  topTwoTeamsPerSlate: {
    topTeams: OddsSignalSummary;
    otherTeams: OddsSignalSummary;
  };
  vegasBuckets: OddsSignalBucket[];
  teamTotalBuckets: OddsSignalBucket[];
};

function avg(values: number[]): number | null {
  if (values.length === 0) return null;
  return values.reduce((sum, value) => sum + value, 0) / values.length;
}

function roundOrNull(value: number | null, digits = 4): number | null {
  if (value == null || !Number.isFinite(value)) return null;
  const factor = 10 ** digits;
  return Math.round(value * factor) / factor;
}

function corr(xs: number[], ys: number[]): number | null {
  if (xs.length === 0 || xs.length !== ys.length) return null;
  const mx = avg(xs);
  const my = avg(ys);
  if (mx == null || my == null) return null;
  let num = 0;
  let dx = 0;
  let dy = 0;
  for (let i = 0; i < xs.length; i++) {
    const a = xs[i] - mx;
    const b = ys[i] - my;
    num += a * b;
    dx += a * a;
    dy += b * b;
  }
  return dx > 0 && dy > 0 ? num / Math.sqrt(dx * dy) : null;
}

function mlToProb(ml: number): number {
  return ml >= 0 ? 100 / (ml + 100) : Math.abs(ml) / (Math.abs(ml) + 100);
}

function computeTeamImpliedTotal(
  vegasTotal: number,
  homeMl: number | null,
  awayMl: number | null,
  isHome: boolean,
): number {
  if (homeMl == null || awayMl == null) return vegasTotal / 2;
  const rawHome = mlToProb(homeMl);
  const rawAway = mlToProb(awayMl);
  const vig = rawHome + rawAway;
  const homeProbClean = rawHome / vig;
  const impliedSpread = Math.max(-15, Math.min(15, (homeProbClean - 0.5) / 0.025));
  const homeImplied = vegasTotal / 2 + impliedSpread / 2;
  return isHome ? homeImplied : vegasTotal - homeImplied;
}

function summarize(samples: PlayerSample[]): OddsSignalSummary {
  return {
    n: samples.length,
    beatProjRate: roundOrNull(avg(samples.map((sample) => (sample.beatProj ? 1 : 0)))),
    hit5xRate: roundOrNull(avg(samples.map((sample) => (sample.hit5x ? 1 : 0)))),
    hit6xRate: roundOrNull(avg(samples.map((sample) => (sample.hit6x ? 1 : 0)))),
    avgSurplus: roundOrNull(avg(samples.map((sample) => sample.surplus))),
    avgActual: roundOrNull(avg(samples.map((sample) => sample.actual))),
    avgProj: roundOrNull(avg(samples.map((sample) => sample.proj))),
    avgSalary: roundOrNull(avg(samples.map((sample) => sample.salary))),
  };
}

function bucketByRange(samples: PlayerSample[], key: "vegasTotal" | "teamTotal", boundaries: number[]): OddsSignalBucket[] {
  const rows: OddsSignalBucket[] = [];
  for (let i = 0; i < boundaries.length - 1; i++) {
    const low = boundaries[i];
    const high = boundaries[i + 1];
    const bucket = samples.filter((sample) =>
      sample[key] >= low && (i === boundaries.length - 2 ? sample[key] <= high : sample[key] < high),
    );
    if (bucket.length === 0) continue;
    rows.push({
      bucket: `${low}-${high}`,
      ...summarize(bucket),
    });
  }
  return rows;
}

async function loadSamplesForSlate(slateId: number): Promise<PlayerSample[]> {
  const result = await db.execute(sql`
    select
      ds.slate_date as "slateDate",
      ds.id as "slateId",
      dp.name,
      dp.team_abbrev as "teamAbbrev",
      dp.team_id as "teamId",
      dp.salary,
      dp.our_proj as "ourProj",
      dp.actual_fpts as "actualFpts",
      dp.is_out as "isOut",
      m.vegas_total as "vegasTotal",
      m.home_ml as "homeMl",
      m.away_ml as "awayMl",
      m.home_team_id as "homeTeamId",
      m.away_team_id as "awayTeamId"
    from dk_players dp
    join dk_slates ds on ds.id = dp.slate_id
    left join nba_matchups m on m.id = dp.matchup_id
    where dp.slate_id = ${slateId}
      and dp.actual_fpts is not null
      and dp.our_proj is not null
      and m.vegas_total is not null
    order by dp.team_abbrev, dp.name
  `);

  return result.rows.map((row) => {
    const vegasTotal = Number(row.vegasTotal);
    const homeMl = row.homeMl == null ? null : Number(row.homeMl);
    const awayMl = row.awayMl == null ? null : Number(row.awayMl);
    const teamId = row.teamId == null ? null : Number(row.teamId);
    const homeTeamId = row.homeTeamId == null ? null : Number(row.homeTeamId);
    const awayTeamId = row.awayTeamId == null ? null : Number(row.awayTeamId);
    const isHome = teamId != null && homeTeamId != null && teamId === homeTeamId;
    const teamTotal = computeTeamImpliedTotal(vegasTotal, homeMl, awayMl, isHome);
    const proj = Number(row.ourProj);
    const actual = Number(row.actualFpts);
    const salary = Number(row.salary);
    const slateValue = Number(row.slateId);
    const gameKey = `${slateValue}:${Math.min(homeTeamId ?? -1, awayTeamId ?? -1)}-${Math.max(homeTeamId ?? -1, awayTeamId ?? -1)}`;

    return {
      slateDate: String(row.slateDate),
      slateId: slateValue,
      name: String(row.name),
      team: String(row.teamAbbrev),
      teamId,
      salary,
      proj,
      actual,
      isOut: Boolean(row.isOut),
      vegasTotal,
      teamTotal,
      beatProj: actual >= proj,
      hit5x: actual >= salary / 200,
      hit6x: actual >= salary * 0.006,
      surplus: actual - proj,
      gameKey,
    };
  });
}

export async function buildNbaOddsSignalReport(slateId: number): Promise<NbaOddsSignalReport> {
  const samples = await loadSamplesForSlate(slateId);
  const active = samples.filter((sample) => !sample.isOut);

  const topGamesBySlate = new Map<string, Set<string>>();
  const topTeamsBySlate = new Map<string, Set<number>>();
  for (const sample of active) {
    const slateKey = `${sample.slateDate}:${sample.slateId}`;
    if (!topGamesBySlate.has(slateKey)) {
      const slatePlayers = active.filter((candidate) => `${candidate.slateDate}:${candidate.slateId}` === slateKey);
      const uniqueGames = Array.from(new Map(
        slatePlayers.map((candidate) => [candidate.gameKey, candidate.vegasTotal]),
      ).entries()).sort((a, b) => b[1] - a[1]);
      topGamesBySlate.set(slateKey, new Set(uniqueGames.slice(0, 2).map(([key]) => key)));

      const uniqueTeams = Array.from(new Map(
        slatePlayers
          .filter((candidate) => candidate.teamId != null)
          .map((candidate) => [candidate.teamId as number, candidate.teamTotal]),
      ).entries()).sort((a, b) => b[1] - a[1]);
      topTeamsBySlate.set(slateKey, new Set(uniqueTeams.slice(0, 2).map(([teamId]) => teamId)));
    }
  }

  const topGamePlayers = active.filter((sample) => topGamesBySlate.get(`${sample.slateDate}:${sample.slateId}`)?.has(sample.gameKey));
  const nonTopGamePlayers = active.filter((sample) => !topGamesBySlate.get(`${sample.slateDate}:${sample.slateId}`)?.has(sample.gameKey));
  const topTeamPlayers = active.filter((sample) => sample.teamId != null && topTeamsBySlate.get(`${sample.slateDate}:${sample.slateId}`)?.has(sample.teamId));
  const nonTopTeamPlayers = active.filter((sample) => sample.teamId == null || !topTeamsBySlate.get(`${sample.slateDate}:${sample.slateId}`)?.has(sample.teamId));

  return {
    sample: {
      totalProjectedWithActuals: samples.length,
      activeProjectedWithActuals: active.length,
      slatesWithOddsAndActuals: Array.from(new Set(active.map((sample) => sample.slateDate))),
    },
    overallActive: summarize(active),
    correlations: {
      vegasToActual: roundOrNull(corr(active.map((sample) => sample.vegasTotal), active.map((sample) => sample.actual))),
      vegasToSurplus: roundOrNull(corr(active.map((sample) => sample.vegasTotal), active.map((sample) => sample.surplus))),
      vegasToHit5x: roundOrNull(corr(active.map((sample) => sample.vegasTotal), active.map((sample) => (sample.hit5x ? 1 : 0)))),
      teamTotalToActual: roundOrNull(corr(active.map((sample) => sample.teamTotal), active.map((sample) => sample.actual))),
      teamTotalToSurplus: roundOrNull(corr(active.map((sample) => sample.teamTotal), active.map((sample) => sample.surplus))),
      teamTotalToHit5x: roundOrNull(corr(active.map((sample) => sample.teamTotal), active.map((sample) => (sample.hit5x ? 1 : 0)))),
    },
    topTwoGamesPerSlate: {
      topGames: summarize(topGamePlayers),
      otherGames: summarize(nonTopGamePlayers),
    },
    topTwoTeamsPerSlate: {
      topTeams: summarize(topTeamPlayers),
      otherTeams: summarize(nonTopTeamPlayers),
    },
    vegasBuckets: bucketByRange(active, "vegasTotal", [0, 225, 232, 240, 260]),
    teamTotalBuckets: bucketByRange(active, "teamTotal", [0, 112, 117, 122, 140]),
  };
}

export async function persistNbaOddsSignalReport(slateId: number): Promise<NbaOddsSignalReport> {
  await ensureOddsSignalTables();
  const report = await buildNbaOddsSignalReport(slateId);
  await db.insert(oddsSignalRuns).values({
    sport: "nba",
    slateId,
    analysisVersion: NBA_ODDS_SIGNAL_ANALYSIS_VERSION,
    sampleSize: report.sample.activeProjectedWithActuals,
    reportJson: report,
  }).onConflictDoUpdate({
    target: oddsSignalRuns.slateId,
    set: {
      analysisVersion: NBA_ODDS_SIGNAL_ANALYSIS_VERSION,
      sampleSize: report.sample.activeProjectedWithActuals,
      reportJson: report,
      updatedAt: sql`NOW()`,
    },
  });
  return report;
}

export async function getLatestPersistedNbaOddsSignalReport() {
  await ensureOddsSignalTables();
  const rows = await db
    .select()
    .from(oddsSignalRuns)
    .where(eq(oddsSignalRuns.sport, "nba"))
    .orderBy(sql`${oddsSignalRuns.updatedAt} DESC`, sql`${oddsSignalRuns.createdAt} DESC`)
    .limit(1);
  return rows[0] ?? null;
}
