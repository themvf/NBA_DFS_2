import fs from "fs";
import path from "path";

import { sql } from "drizzle-orm";

type Sport = "nba" | "mlb";
type Market = "ou" | "spread" | "ml";
type Version = "legacy" | "current";

type MatchupRow = {
  gameDate: string;
  homeAbbrev: string;
  awayAbbrev: string;
  vegasTotal: number | null;
  homeMl: number | null;
  awayMl: number | null;
  homeSpread: number | null;
  homeWinProb: number | null;
  homeImplied: number | null;
  awayImplied: number | null;
  homeScore: number | null;
  awayScore: number | null;
  homeSpXfip: number | null;
  awaySpXfip: number | null;
};

type TeamState = {
  n: number;
  nImplied: number;
  impliedSum: number;
  actualSum: number;
  biasSum: number;
  gameOverN: number;
  gameOverCount: number;
  atsN: number;
  atsCoverCount: number;
};

type TierState = {
  n: number;
  overCount: number;
  coverCount: number;
};

type ScoreSignal = { label: string; value: number; weight: number };

type EvalAccumulator = {
  n: number;
  resolved: number;
  correct: number;
  brierSum: number;
};

type RecommendationMarket = "ou" | "spread" | "ml";

type MlbOuParams = {
  spWeight: number;
  tierWeight: number;
  homeWeight: number;
  awayWeight: number;
  tierAlpha: number;
  teamAlpha: number;
  tierStableSample: number;
  teamStableSample: number;
};

type NbaOuParams = {
  tierWeight: number;
  homeWeight: number;
  awayWeight: number;
  avgImpliedWeight: number;
  tierAlpha: number;
  teamAlpha: number;
  tierStableSample: number;
  teamStableSample: number;
};

type NbaMlParams = {
  vegasWeight: number;
  impliedEdgeWeight: number;
  biasWeight: number;
  biasDivisor: number;
  biasStableSample: number;
};

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

function shrinkRate(
  rate: number | null | undefined,
  n: number | null | undefined,
  prior: number,
  alpha: number,
): number | null {
  if (rate == null) return null;
  const sample = n ?? 0;
  if (sample <= 0) return prior;
  return (rate * sample + alpha * prior) / (sample + alpha);
}

function scaleSignalConfidence(
  n: number | null | undefined,
  stableSample: number,
): number {
  const sample = n ?? 0;
  if (sample <= 0) return 0;
  return clamp(sample / stableSample, 0, 1);
}

function blendTowardNeutral(
  value: number | null,
  confidence: number,
  neutral = 0.5,
): number | null {
  if (value == null) return null;
  return neutral + (value - neutral) * clamp(confidence, 0, 1);
}

function blendSignals(signals: ScoreSignal[]): number | null {
  if (signals.length === 0) return null;
  const totalWeight = signals.reduce((sum, row) => sum + row.weight, 0);
  if (totalWeight <= 0) return null;
  return signals.reduce((sum, row) => sum + row.value * row.weight, 0) / totalWeight;
}

function getRecommendationBand(sport: Sport, market: RecommendationMarket): number {
  if (sport === "nba") {
    if (market === "ou") return 0.03;
    return 0.04;
  }
  if (market === "ou") return 0.025;
  return 0.03;
}

function thresholdProbability(
  probability: number | null,
  sport: Sport,
  market: RecommendationMarket,
): number | null {
  if (probability == null) return null;
  return Math.abs(probability - 0.5) >= getRecommendationBand(sport, market)
    ? probability
    : null;
}

function getOuTierKey(total: number | null, sport: Sport): string | null {
  if (total == null) return null;
  if (sport === "mlb") {
    if (total < 7.5) return "Under 7.5";
    if (total < 8.0) return "7.5";
    if (total < 8.5) return "8.0";
    if (total < 9.0) return "8.5";
    if (total < 9.5) return "9.0";
    if (total < 10.0) return "9.5";
    if (total < 10.5) return "10.0";
    return "10.5+";
  }
  if (total < 215) return "Under 215";
  if (total < 220) return "215-220";
  if (total < 225) return "220-225";
  if (total < 230) return "225-230";
  if (total < 235) return "230-235";
  if (total < 240) return "235-240";
  return "240+";
}

function getSpreadTierKey(spread: number | null, sport: Sport): string | null {
  if (spread == null) return null;
  const abs = Math.abs(spread);
  if (sport === "mlb") {
    if (abs < 1.0) return "Pick";
    if (abs < 2.0) return "±1.5 (Run Line)";
    return "2.0+";
  }
  if (abs <= 1.5) return "Pick / ±1.5";
  if (abs <= 3.5) return "2-3.5";
  if (abs <= 6.5) return "4-6.5";
  if (abs <= 9.5) return "7-9.5";
  if (abs <= 13.5) return "10-13.5";
  return "14+";
}

function safeRate(count: number, n: number) {
  return n > 0 ? count / n : null;
}

function getTeamState(
  map: Map<string, TeamState>,
  abbrev: string,
): TeamState {
  return map.get(abbrev) ?? {
    n: 0,
    nImplied: 0,
    impliedSum: 0,
    actualSum: 0,
    biasSum: 0,
    gameOverN: 0,
    gameOverCount: 0,
    atsN: 0,
    atsCoverCount: 0,
  };
}

function computeOuLegacy(
  row: MatchupRow,
  sport: Sport,
  ouTiers: Map<string, TierState>,
  teamMap: Map<string, TeamState>,
): number | null {
  const tier = getOuTierKey(row.vegasTotal, sport);
  const tierState = tier ? ouTiers.get(tier) : undefined;
  const home = getTeamState(teamMap, row.homeAbbrev);
  const away = getTeamState(teamMap, row.awayAbbrev);
  const signals: ScoreSignal[] = [];

  if (sport === "mlb" && row.homeSpXfip != null && row.awaySpXfip != null) {
    const avgXfip = (row.homeSpXfip + row.awaySpXfip) / 2;
    const edge = (avgXfip - 4.2) / 4.2;
    signals.push({ label: "sp", value: clamp(0.5 + edge * 1.5, 0.3, 0.7), weight: 0.15 });
  }

  const tierRate = shrinkRate(safeRate(tierState?.overCount ?? 0, tierState?.n ?? 0), tierState?.n, 0.5, 50);
  if (tierRate != null) signals.push({ label: "tier", value: tierRate, weight: sport === "mlb" ? 0.30 : 0.40 });

  const homeRate = shrinkRate(safeRate(home.gameOverCount, home.gameOverN), home.n, 0.5, 20);
  if (homeRate != null) signals.push({ label: "home", value: homeRate, weight: sport === "mlb" ? 0.15 : 0.30 });

  const awayRate = shrinkRate(safeRate(away.gameOverCount, away.gameOverN), away.n, 0.5, 20);
  if (awayRate != null) signals.push({ label: "away", value: awayRate, weight: sport === "mlb" ? 0.15 : 0.30 });

  return blendSignals(signals);
}

function computeOuCurrent(
  row: MatchupRow,
  sport: Sport,
  ouTiers: Map<string, TierState>,
  teamMap: Map<string, TeamState>,
): number | null {
  const tier = getOuTierKey(row.vegasTotal, sport);
  const tierState = tier ? ouTiers.get(tier) : undefined;
  const home = getTeamState(teamMap, row.homeAbbrev);
  const away = getTeamState(teamMap, row.awayAbbrev);
  const signals: ScoreSignal[] = [];

  if (sport === "mlb" && row.homeSpXfip != null && row.awaySpXfip != null) {
    const avgXfip = (row.homeSpXfip + row.awaySpXfip) / 2;
    const edge = (avgXfip - 4.2) / 4.2;
    signals.push({ label: "sp", value: clamp(0.5 + edge * 1.5, 0.3, 0.7), weight: 0.15 });
  }

  const tierRate = shrinkRate(
    safeRate(tierState?.overCount ?? 0, tierState?.n ?? 0),
    tierState?.n,
    0.5,
    sport === "mlb" ? 60 : 50,
  );
  if (tierRate != null) {
    const confidence = sport === "mlb"
      ? scaleSignalConfidence(tierState?.n, 12)
      : scaleSignalConfidence(tierState?.n, 60);
    const value = blendTowardNeutral(tierRate, confidence);
    if (value != null) signals.push({ label: "tier", value, weight: 0.20 });
  }

  const homeRate = shrinkRate(safeRate(home.gameOverCount, home.gameOverN), home.n, 0.5, 40);
  if (homeRate != null) {
    const confidence = sport === "mlb"
      ? scaleSignalConfidence(home.n, 30)
      : scaleSignalConfidence(home.n, 40);
    const value = blendTowardNeutral(homeRate, confidence);
    if (value != null) signals.push({ label: "home", value, weight: 0.20 });
  }

  const awayRate = shrinkRate(safeRate(away.gameOverCount, away.gameOverN), away.n, 0.5, 40);
  if (awayRate != null) {
    const confidence = sport === "mlb"
      ? scaleSignalConfidence(away.n, 30)
      : scaleSignalConfidence(away.n, 40);
    const value = blendTowardNeutral(awayRate, confidence);
    if (value != null) signals.push({ label: "away", value, weight: 0.20 });
  }

  return blendSignals(signals);
}

function computeMlbOuWithParams(
  row: MatchupRow,
  ouTiers: Map<string, TierState>,
  teamMap: Map<string, TeamState>,
  params: MlbOuParams,
): number | null {
  const tier = getOuTierKey(row.vegasTotal, "mlb");
  const tierState = tier ? ouTiers.get(tier) : undefined;
  const home = getTeamState(teamMap, row.homeAbbrev);
  const away = getTeamState(teamMap, row.awayAbbrev);
  const signals: ScoreSignal[] = [];

  if (row.homeSpXfip != null && row.awaySpXfip != null && params.spWeight > 0) {
    const avgXfip = (row.homeSpXfip + row.awaySpXfip) / 2;
    const edge = (avgXfip - 4.2) / 4.2;
    signals.push({
      label: "sp",
      value: clamp(0.5 + edge * 1.5, 0.3, 0.7),
      weight: params.spWeight,
    });
  }

  const tierRate = shrinkRate(
    safeRate(tierState?.overCount ?? 0, tierState?.n ?? 0),
    tierState?.n,
    0.5,
    params.tierAlpha,
  );
  if (tierRate != null && params.tierWeight > 0) {
    const confidence = scaleSignalConfidence(tierState?.n, params.tierStableSample);
    const value = blendTowardNeutral(tierRate, confidence);
    if (value != null) signals.push({ label: "tier", value, weight: params.tierWeight });
  }

  const homeRate = shrinkRate(safeRate(home.gameOverCount, home.gameOverN), home.n, 0.5, params.teamAlpha);
  if (homeRate != null && params.homeWeight > 0) {
    const confidence = scaleSignalConfidence(home.n, params.teamStableSample);
    const value = blendTowardNeutral(homeRate, confidence);
    if (value != null) signals.push({ label: "home", value, weight: params.homeWeight });
  }

  const awayRate = shrinkRate(safeRate(away.gameOverCount, away.gameOverN), away.n, 0.5, params.teamAlpha);
  if (awayRate != null && params.awayWeight > 0) {
    const confidence = scaleSignalConfidence(away.n, params.teamStableSample);
    const value = blendTowardNeutral(awayRate, confidence);
    if (value != null) signals.push({ label: "away", value, weight: params.awayWeight });
  }

  return blendSignals(signals);
}

function computeNbaOuWithParams(
  row: MatchupRow,
  ouTiers: Map<string, TierState>,
  teamMap: Map<string, TeamState>,
  params: NbaOuParams,
): number | null {
  const tier = getOuTierKey(row.vegasTotal, "nba");
  const tierState = tier ? ouTiers.get(tier) : undefined;
  const home = getTeamState(teamMap, row.homeAbbrev);
  const away = getTeamState(teamMap, row.awayAbbrev);
  const signals: ScoreSignal[] = [];

  const tierRate = shrinkRate(
    safeRate(tierState?.overCount ?? 0, tierState?.n ?? 0),
    tierState?.n,
    0.5,
    params.tierAlpha,
  );
  if (tierRate != null && params.tierWeight > 0) {
    const confidence = scaleSignalConfidence(tierState?.n, params.tierStableSample);
    const value = blendTowardNeutral(tierRate, confidence);
    if (value != null) signals.push({ label: "tier", value, weight: params.tierWeight });
  }

  const homeRate = shrinkRate(safeRate(home.gameOverCount, home.gameOverN), home.n, 0.5, params.teamAlpha);
  if (homeRate != null && params.homeWeight > 0) {
    const confidence = scaleSignalConfidence(home.n, params.teamStableSample);
    const value = blendTowardNeutral(homeRate, confidence);
    if (value != null) signals.push({ label: "home", value, weight: params.homeWeight });
  }

  const awayRate = shrinkRate(safeRate(away.gameOverCount, away.gameOverN), away.n, 0.5, params.teamAlpha);
  if (awayRate != null && params.awayWeight > 0) {
    const confidence = scaleSignalConfidence(away.n, params.teamStableSample);
    const value = blendTowardNeutral(awayRate, confidence);
    if (value != null) signals.push({ label: "away", value, weight: params.awayWeight });
  }

  if (row.homeImplied != null && row.awayImplied != null && params.avgImpliedWeight > 0) {
    const avgImplied = (row.homeImplied + row.awayImplied) / 2;
    signals.push({
      label: "avg_team_total",
      value: clamp(0.5 + ((avgImplied - 116) / 8) * 0.08, 0.43, 0.57),
      weight: params.avgImpliedWeight,
    });
  }

  return blendSignals(signals);
}

function computeNbaMlWithParams(
  row: MatchupRow,
  teamMap: Map<string, TeamState>,
  params: NbaMlParams,
): number | null {
  if (row.homeWinProb == null) return null;
  const home = getTeamState(teamMap, row.homeAbbrev);
  const away = getTeamState(teamMap, row.awayAbbrev);
  const homeBias = home.nImplied > 0 ? home.biasSum / home.nImplied : 0;
  const awayBias = away.nImplied > 0 ? away.biasSum / away.nImplied : 0;
  const signals: ScoreSignal[] = [];

  signals.push({ label: "vegas", value: row.homeWinProb, weight: params.vegasWeight });

  if (row.homeImplied != null && row.awayImplied != null && params.impliedEdgeWeight > 0) {
    const impliedEdge = clamp((row.homeImplied - row.awayImplied) / 24, -0.04, 0.04);
    signals.push({
      label: "implied_total_edge",
      value: clamp(0.5 + impliedEdge, 0.46, 0.54),
      weight: params.impliedEdgeWeight,
    });
  }

  if (params.biasWeight > 0) {
    const rawBiasAdj = clamp((awayBias - homeBias) / params.biasDivisor, -0.02, 0.02);
    const confidence = Math.min(
      scaleSignalConfidence(home.nImplied, params.biasStableSample),
      scaleSignalConfidence(away.nImplied, params.biasStableSample),
    );
    if (confidence > 0) {
      signals.push({
        label: "bias",
        value: clamp(0.5 + rawBiasAdj * confidence, 0.48, 0.52),
        weight: params.biasWeight * confidence,
      });
    }
  }

  return blendSignals(signals);
}

function computeSpreadLegacy(
  row: MatchupRow,
  sport: Sport,
  spreadTiers: Map<string, TierState>,
  teamMap: Map<string, TeamState>,
): number | null {
  const tier = getSpreadTierKey(row.homeSpread, sport);
  const tierState = tier ? spreadTiers.get(tier) : undefined;
  const home = getTeamState(teamMap, row.homeAbbrev);
  const away = getTeamState(teamMap, row.awayAbbrev);
  const signals: ScoreSignal[] = [];

  const tierRate = shrinkRate(safeRate(tierState?.coverCount ?? 0, tierState?.n ?? 0), tierState?.n, 0.5, 50);
  if (tierRate != null && row.homeSpread != null) {
    const baseCoverRate =
      row.homeSpread < 0 ? tierRate : Math.abs(row.homeSpread) < 0.5 ? 0.5 : 1 - tierRate;
    signals.push({ label: "tier", value: baseCoverRate, weight: 0.40 });
  }

  const homeRate = shrinkRate(safeRate(home.atsCoverCount, home.atsN), home.atsN, 0.5, 20);
  if (homeRate != null) signals.push({ label: "home_ats", value: homeRate, weight: 0.35 });

  const awayRate = shrinkRate(safeRate(away.atsCoverCount, away.atsN), away.atsN, 0.5, 20);
  if (awayRate != null) signals.push({ label: "away_ats", value: 1 - awayRate, weight: 0.25 });

  return blendSignals(signals);
}

function computeSpreadCurrent(
  row: MatchupRow,
  sport: Sport,
  spreadTiers: Map<string, TierState>,
  teamMap: Map<string, TeamState>,
): number | null {
  if (sport === "nba") return null;
  return computeSpreadLegacy(row, sport, spreadTiers, teamMap);
}

function computeMlLegacy(
  row: MatchupRow,
  sport: Sport,
  teamMap: Map<string, TeamState>,
): number | null {
  if (row.homeWinProb == null) return null;
  const home = getTeamState(teamMap, row.homeAbbrev);
  const away = getTeamState(teamMap, row.awayAbbrev);
  const homeBias = home.nImplied > 0 ? home.biasSum / home.nImplied : 0;
  const awayBias = away.nImplied > 0 ? away.biasSum / away.nImplied : 0;
  const biasDivisor = sport === "mlb" ? 3 : 30;
  const biasAdj = clamp((homeBias - awayBias) / biasDivisor, -0.05, 0.05);
  return clamp(row.homeWinProb + biasAdj, 0.05, 0.95);
}

function computeMlCurrent(
  row: MatchupRow,
  sport: Sport,
  teamMap: Map<string, TeamState>,
): number | null {
  if (row.homeWinProb == null) return null;

  if (sport === "nba") {
    return row.homeWinProb;
  }

  const home = getTeamState(teamMap, row.homeAbbrev);
  const away = getTeamState(teamMap, row.awayAbbrev);
  const homeBias = home.nImplied > 0 ? home.biasSum / home.nImplied : 0;
  const awayBias = away.nImplied > 0 ? away.biasSum / away.nImplied : 0;
  const rawBiasAdj = clamp((homeBias - awayBias) / 3, -0.05, 0.05);
  const confidence = Math.min(
    scaleSignalConfidence(home.nImplied, 20),
    scaleSignalConfidence(away.nImplied, 20),
  );
  return clamp(row.homeWinProb + rawBiasAdj * confidence, 0.05, 0.95);
}

function initEval(): EvalAccumulator {
  return { n: 0, resolved: 0, correct: 0, brierSum: 0 };
}

function updateEval(
  acc: EvalAccumulator,
  probability: number | null,
  outcome: number | null,
) {
  if (probability == null || outcome == null) return;
  acc.n += 1;
  acc.brierSum += (probability - outcome) ** 2;
  if (probability !== 0.5) {
    acc.resolved += 1;
    const pick = probability > 0.5 ? 1 : 0;
    if (pick === outcome) acc.correct += 1;
  }
}

function updateTeamStates(
  teamMap: Map<string, TeamState>,
  row: MatchupRow,
  sport: Sport,
) {
  if (
    row.homeScore == null ||
    row.awayScore == null ||
    row.vegasTotal == null
  ) {
    return;
  }

  const actualTotal = row.homeScore + row.awayScore;
  const homeState = getTeamState(teamMap, row.homeAbbrev);
  const awayState = getTeamState(teamMap, row.awayAbbrev);
  const gameOver = actualTotal > row.vegasTotal ? 1 : 0;

  homeState.n += 1;
  awayState.n += 1;
  homeState.gameOverN += 1;
  awayState.gameOverN += 1;
  homeState.gameOverCount += gameOver;
  awayState.gameOverCount += gameOver;

  if (row.homeImplied != null) {
    homeState.nImplied += 1;
    homeState.impliedSum += row.homeImplied;
    homeState.actualSum += row.homeScore;
    homeState.biasSum += row.homeImplied - row.homeScore;
  }
  if (row.awayImplied != null) {
    awayState.nImplied += 1;
    awayState.impliedSum += row.awayImplied;
    awayState.actualSum += row.awayScore;
    awayState.biasSum += row.awayImplied - row.awayScore;
  }

  if (row.homeSpread != null) {
    let homeCovered: boolean | null = null;
    let awayCovered: boolean | null = null;
    if (sport === "mlb") {
      homeCovered = (row.homeScore - row.awayScore) > -row.homeSpread;
      awayCovered = (row.awayScore - row.homeScore) > row.homeSpread;
    } else {
      homeCovered = (row.homeScore - row.awayScore) > -row.homeSpread;
      awayCovered = (row.awayScore - row.homeScore) > row.homeSpread;
    }
    if (homeCovered != null) {
      homeState.atsN += 1;
      homeState.atsCoverCount += homeCovered ? 1 : 0;
    }
    if (awayCovered != null) {
      awayState.atsN += 1;
      awayState.atsCoverCount += awayCovered ? 1 : 0;
    }
  }

  teamMap.set(row.homeAbbrev, homeState);
  teamMap.set(row.awayAbbrev, awayState);
}

async function loadGamesForSport(sport: Sport): Promise<MatchupRow[]> {
  const mod = await import("../src/db/index");
  const db = mod.db;

  if (sport === "nba") {
    const result = await db.execute(sql`
      select
        nm.game_date::text as "gameDate",
        ht.abbreviation as "homeAbbrev",
        at.abbreviation as "awayAbbrev",
        nm.vegas_total as "vegasTotal",
        nm.home_ml as "homeMl",
        nm.away_ml as "awayMl",
        nm.home_spread as "homeSpread",
        nm.vegas_prob_home as "homeWinProb",
        nm.home_implied as "homeImplied",
        nm.away_implied as "awayImplied",
        nm.home_score as "homeScore",
        nm.away_score as "awayScore",
        null::double precision as "homeSpXfip",
        null::double precision as "awaySpXfip"
      from nba_matchups nm
      join teams ht on ht.team_id = nm.home_team_id
      join teams at on at.team_id = nm.away_team_id
      where nm.vegas_total is not null
        and nm.home_score is not null
        and nm.away_score is not null
      order by nm.game_date asc, nm.id asc
    `);
    return result.rows as MatchupRow[];
  }

  const result = await db.execute(sql`
    with latest_pitcher as (
      select distinct on (player_id)
        player_id, name, xfip
      from mlb_pitcher_stats
      order by player_id, season desc, fetched_at desc, id desc
    ),
    latest_pitcher_by_name as (
      select distinct on (lower(name))
        lower(name) as name_key,
        name,
        xfip
      from mlb_pitcher_stats
      order by lower(name), season desc, fetched_at desc, id desc
    )
    select
      m.game_date::text as "gameDate",
      ht.abbreviation as "homeAbbrev",
      at.abbreviation as "awayAbbrev",
      m.vegas_total as "vegasTotal",
      m.home_ml as "homeMl",
      m.away_ml as "awayMl",
      m.home_spread as "homeSpread",
      m.vegas_prob_home as "homeWinProb",
      m.home_implied as "homeImplied",
      m.away_implied as "awayImplied",
      m.home_score as "homeScore",
      m.away_score as "awayScore",
      coalesce(hsp_id.xfip, hsp_name.xfip) as "homeSpXfip",
      coalesce(asp_id.xfip, asp_name.xfip) as "awaySpXfip"
    from mlb_matchups m
    join mlb_teams ht on ht.team_id = m.home_team_id
    join mlb_teams at on at.team_id = m.away_team_id
    left join latest_pitcher hsp_id on hsp_id.player_id = m.home_sp_id
    left join latest_pitcher asp_id on asp_id.player_id = m.away_sp_id
    left join latest_pitcher_by_name hsp_name on hsp_name.name_key = lower(m.home_sp_name)
    left join latest_pitcher_by_name asp_name on asp_name.name_key = lower(m.away_sp_name)
    where m.vegas_total is not null
      and m.home_score is not null
      and m.away_score is not null
    order by m.game_date asc, m.id asc
  `);
  return result.rows as MatchupRow[];
}

function emptyMarketReport(): Record<Version, EvalAccumulator> {
  return { legacy: initEval(), current: initEval() };
}

async function backtestSport(sport: Sport) {
  const games = await loadGamesForSport(sport);
  const teamMap = new Map<string, TeamState>();
  const ouTiers = new Map<string, TierState>();
  const spreadTiers = new Map<string, TierState>();

  const results: Record<Market, Record<Version, EvalAccumulator>> = {
    ou: emptyMarketReport(),
    spread: emptyMarketReport(),
    ml: emptyMarketReport(),
  };
  const actionable: Record<Market, Record<Version, EvalAccumulator>> = {
    ou: emptyMarketReport(),
    spread: emptyMarketReport(),
    ml: emptyMarketReport(),
  };

  for (const row of games) {
    if (
      row.homeScore == null ||
      row.awayScore == null ||
      row.vegasTotal == null ||
      row.homeMl == null ||
      row.awayMl == null ||
      row.homeWinProb == null
    ) {
      continue;
    }

    const actualTotal = row.homeScore + row.awayScore;
    const ouOutcome =
      actualTotal === row.vegasTotal
        ? null
        : actualTotal > row.vegasTotal
        ? 1
        : 0;

    const ouLegacy = computeOuLegacy(row, sport, ouTiers, teamMap);
    const ouCurrent = computeOuCurrent(row, sport, ouTiers, teamMap);
    updateEval(results.ou.legacy, ouLegacy, ouOutcome);
    updateEval(results.ou.current, ouCurrent, ouOutcome);
    updateEval(actionable.ou.legacy, thresholdProbability(ouLegacy, sport, "ou"), ouOutcome);
    updateEval(actionable.ou.current, thresholdProbability(ouCurrent, sport, "ou"), ouOutcome);

    const mlOutcome = row.homeScore > row.awayScore ? 1 : 0;
    const mlLegacy = computeMlLegacy(row, sport, teamMap);
    const mlCurrent = computeMlCurrent(row, sport, teamMap);
    updateEval(results.ml.legacy, mlLegacy, mlOutcome);
    updateEval(results.ml.current, mlCurrent, mlOutcome);
    updateEval(actionable.ml.legacy, thresholdProbability(mlLegacy, sport, "ml"), mlOutcome);
    updateEval(actionable.ml.current, thresholdProbability(mlCurrent, sport, "ml"), mlOutcome);

    if (row.homeSpread != null) {
      const spreadOutcome =
        (row.homeScore - row.awayScore) === -row.homeSpread
          ? null
          : (row.homeScore - row.awayScore) > -row.homeSpread
          ? 1
          : 0;
      const spreadLegacy = computeSpreadLegacy(row, sport, spreadTiers, teamMap);
      const spreadCurrent = computeSpreadCurrent(row, sport, spreadTiers, teamMap);
      updateEval(results.spread.legacy, spreadLegacy, spreadOutcome);
      updateEval(results.spread.current, spreadCurrent, spreadOutcome);
      updateEval(actionable.spread.legacy, thresholdProbability(spreadLegacy, sport, "spread"), spreadOutcome);
      updateEval(actionable.spread.current, thresholdProbability(spreadCurrent, sport, "spread"), spreadOutcome);
    }

    const ouTier = getOuTierKey(row.vegasTotal, sport);
    if (ouTier) {
      const state = ouTiers.get(ouTier) ?? { n: 0, overCount: 0, coverCount: 0 };
      state.n += 1;
      state.overCount += actualTotal > row.vegasTotal ? 1 : 0;
      ouTiers.set(ouTier, state);
    }

    if (row.homeSpread != null) {
      const spreadTier = getSpreadTierKey(row.homeSpread, sport);
      if (spreadTier) {
        const state = spreadTiers.get(spreadTier) ?? { n: 0, overCount: 0, coverCount: 0 };
        state.n += 1;
        const favoriteCovered =
          row.homeSpread < 0
            ? (row.homeScore - row.awayScore) > Math.abs(row.homeSpread)
            : (row.awayScore - row.homeScore) > Math.abs(row.homeSpread);
        state.coverCount += favoriteCovered ? 1 : 0;
        spreadTiers.set(spreadTier, state);
      }
    }

    updateTeamStates(teamMap, row, sport);
  }

  return {
    sport,
    games: games.length,
    results,
    actionable,
  };
}

async function searchMlbOuParams() {
  const games = await loadGamesForSport("mlb");
  const candidates: MlbOuParams[] = [];

  for (const spWeight of [0.15, 0.2, 0.25, 0.3, 0.35]) {
    for (const tierWeight of [0.2, 0.25, 0.3, 0.35, 0.4]) {
      for (const teamWeight of [0.05, 0.1, 0.15, 0.2]) {
        for (const tierAlpha of [30, 40, 50, 60, 70]) {
          for (const teamAlpha of [10, 20, 30, 40]) {
            for (const tierStableSample of [12, 18, 24, 30]) {
              for (const teamStableSample of [10, 15, 20, 25, 30]) {
                candidates.push({
                  spWeight,
                  tierWeight,
                  homeWeight: teamWeight,
                  awayWeight: teamWeight,
                  tierAlpha,
                  teamAlpha,
                  tierStableSample,
                  teamStableSample,
                });
              }
            }
          }
        }
      }
    }
  }

  const ranked = candidates.map((params) => {
    const teamMap = new Map<string, TeamState>();
    const ouTiers = new Map<string, TierState>();
    const evalResult = initEval();

    for (const row of games) {
      if (
        row.homeScore == null ||
        row.awayScore == null ||
        row.vegasTotal == null
      ) {
        continue;
      }

      const actualTotal = row.homeScore + row.awayScore;
      const ouOutcome =
        actualTotal === row.vegasTotal
          ? null
          : actualTotal > row.vegasTotal
          ? 1
          : 0;

      updateEval(evalResult, computeMlbOuWithParams(row, ouTiers, teamMap, params), ouOutcome);

      const ouTier = getOuTierKey(row.vegasTotal, "mlb");
      if (ouTier) {
        const state = ouTiers.get(ouTier) ?? { n: 0, overCount: 0, coverCount: 0 };
        state.n += 1;
        state.overCount += actualTotal > row.vegasTotal ? 1 : 0;
        ouTiers.set(ouTier, state);
      }

      updateTeamStates(teamMap, row, "mlb");
    }

    return {
      params,
      ...summarizeEval(evalResult),
    };
  }).sort((a, b) => {
    const brierA = a.brier ?? Number.POSITIVE_INFINITY;
    const brierB = b.brier ?? Number.POSITIVE_INFINITY;
    if (brierA !== brierB) return brierA - brierB;
    const accA = a.accuracy ?? 0;
    const accB = b.accuracy ?? 0;
    return accB - accA;
  });

  return {
    searched: candidates.length,
    best: ranked.slice(0, 10),
  };
}

async function searchNbaOuParams() {
  const games = await loadGamesForSport("nba");
  const candidates: NbaOuParams[] = [];

  for (const tierWeight of [0.2, 0.3, 0.4, 0.5, 0.55, 0.6]) {
    for (const teamWeight of [0, 0.05, 0.1, 0.15, 0.2]) {
      for (const avgImpliedWeight of [0, 0.05, 0.1, 0.15, 0.2]) {
        for (const tierAlpha of [50, 60, 70, 80, 90, 100]) {
          for (const teamAlpha of [10, 20, 30, 40, 50, 60]) {
            for (const tierStableSample of [60, 80, 100, 120, 140, 160]) {
              for (const teamStableSample of [40, 50, 65, 80, 100]) {
                candidates.push({
                  tierWeight,
                  homeWeight: teamWeight,
                  awayWeight: teamWeight,
                  avgImpliedWeight,
                  tierAlpha,
                  teamAlpha,
                  tierStableSample,
                  teamStableSample,
                });
              }
            }
          }
        }
      }
    }
  }

  const ranked = candidates.map((params) => {
    const teamMap = new Map<string, TeamState>();
    const ouTiers = new Map<string, TierState>();
    const evalResult = initEval();

    for (const row of games) {
      if (row.homeScore == null || row.awayScore == null || row.vegasTotal == null) {
        continue;
      }

      const actualTotal = row.homeScore + row.awayScore;
      const ouOutcome =
        actualTotal === row.vegasTotal ? null : actualTotal > row.vegasTotal ? 1 : 0;

      updateEval(evalResult, computeNbaOuWithParams(row, ouTiers, teamMap, params), ouOutcome);

      const ouTier = getOuTierKey(row.vegasTotal, "nba");
      if (ouTier) {
        const state = ouTiers.get(ouTier) ?? { n: 0, overCount: 0, coverCount: 0 };
        state.n += 1;
        state.overCount += actualTotal > row.vegasTotal ? 1 : 0;
        ouTiers.set(ouTier, state);
      }

      updateTeamStates(teamMap, row, "nba");
    }

    return {
      params,
      ...summarizeEval(evalResult),
    };
  }).sort((a, b) => {
    const brierA = a.brier ?? Number.POSITIVE_INFINITY;
    const brierB = b.brier ?? Number.POSITIVE_INFINITY;
    if (brierA !== brierB) return brierA - brierB;
    const accA = a.accuracy ?? 0;
    const accB = b.accuracy ?? 0;
    return accB - accA;
  });

  return {
    searched: candidates.length,
    best: ranked.slice(0, 10),
  };
}

async function searchNbaMlParams() {
  const games = await loadGamesForSport("nba");
  const candidates: NbaMlParams[] = [];

  for (const vegasWeight of [0.7, 0.75, 0.8, 0.85, 0.9, 1.0]) {
    for (const impliedEdgeWeight of [0, 0.05, 0.1, 0.15, 0.2]) {
      for (const biasWeight of [0, 0.05, 0.1, 0.15, 0.2]) {
        for (const biasDivisor of [20, 30, 40, 50, 60, 80]) {
          for (const biasStableSample of [40, 50, 60, 70, 80, 100]) {
            candidates.push({
              vegasWeight,
              impliedEdgeWeight,
              biasWeight,
              biasDivisor,
              biasStableSample,
            });
          }
        }
      }
    }
  }

  const ranked = candidates.map((params) => {
    const teamMap = new Map<string, TeamState>();
    const evalResult = initEval();

    for (const row of games) {
      if (
        row.homeScore == null ||
        row.awayScore == null ||
        row.homeWinProb == null
      ) {
        continue;
      }

      const mlOutcome = row.homeScore > row.awayScore ? 1 : 0;
      updateEval(evalResult, computeNbaMlWithParams(row, teamMap, params), mlOutcome);
      updateTeamStates(teamMap, row, "nba");
    }

    return {
      params,
      ...summarizeEval(evalResult),
    };
  }).sort((a, b) => {
    const brierA = a.brier ?? Number.POSITIVE_INFINITY;
    const brierB = b.brier ?? Number.POSITIVE_INFINITY;
    if (brierA !== brierB) return brierA - brierB;
    const accA = a.accuracy ?? 0;
    const accB = b.accuracy ?? 0;
    return accB - accA;
  });

  return {
    searched: candidates.length,
    best: ranked.slice(0, 10),
  };
}

function summarizeEval(evalResult: EvalAccumulator) {
  return {
    n: evalResult.n,
    resolved: evalResult.resolved,
    accuracy: evalResult.resolved > 0 ? evalResult.correct / evalResult.resolved : null,
    brier: evalResult.n > 0 ? evalResult.brierSum / evalResult.n : null,
  };
}

async function main() {
  loadDatabaseUrl();
  const sportArg = process.argv.find((entry) => entry.startsWith("--sport="));
  const searchMlbOu = process.argv.includes("--search-mlb-ou");
  const searchNbaOu = process.argv.includes("--search-nba-ou");
  const searchNbaMl = process.argv.includes("--search-nba-ml");
  const sports: Sport[] = sportArg
    ? [sportArg.slice("--sport=".length) as Sport]
    : ["nba", "mlb"];

  if (searchMlbOu) {
    console.log(JSON.stringify(await searchMlbOuParams(), null, 2));
    return;
  }

  if (searchNbaOu) {
    console.log(JSON.stringify(await searchNbaOuParams(), null, 2));
    return;
  }

  if (searchNbaMl) {
    console.log(JSON.stringify(await searchNbaMlParams(), null, 2));
    return;
  }

  for (const sport of sports) {
    const report = await backtestSport(sport);
    const output = {
      sport,
      games: report.games,
      recommendationBands: {
        ou: getRecommendationBand(sport, "ou"),
        spread: getRecommendationBand(sport, "spread"),
        ml: getRecommendationBand(sport, "ml"),
      },
      markets: {
        ou: {
          legacy: summarizeEval(report.results.ou.legacy),
          current: summarizeEval(report.results.ou.current),
          actionableLegacy: summarizeEval(report.actionable.ou.legacy),
          actionableCurrent: summarizeEval(report.actionable.ou.current),
        },
        spread: {
          legacy: summarizeEval(report.results.spread.legacy),
          current: summarizeEval(report.results.spread.current),
          actionableLegacy: summarizeEval(report.actionable.spread.legacy),
          actionableCurrent: summarizeEval(report.actionable.spread.current),
        },
        ml: {
          legacy: summarizeEval(report.results.ml.legacy),
          current: summarizeEval(report.results.ml.current),
          actionableLegacy: summarizeEval(report.actionable.ml.legacy),
          actionableCurrent: summarizeEval(report.actionable.ml.current),
        },
      },
    };
    console.log(JSON.stringify(output, null, 2));
  }
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
