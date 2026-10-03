import { sql } from "drizzle-orm";
import { db } from ".";

export type CfbAnalyticsFeature = {
  version: string;
  asOf: string;
  gamesPlayed: number;
  currentWeight: number;
  completeness: number | null;
  values: Record<string, unknown>;
};

export type CfbAnalyticsTeam = {
  id: number;
  name: string;
  conference: string | null;
  classification: string | null;
  feature: CfbAnalyticsFeature | null;
};

export type CfbAnalyticsGame = {
  id: number;
  cfbdGameId: number;
  gameDate: string;
  observedAt: string;
  kickoff: string | null;
  kickoffTbd: boolean;
  completed: boolean;
  homeScore: number | null;
  awayScore: number | null;
  home: CfbAnalyticsTeam;
  away: CfbAnalyticsTeam;
  oddsEventMapped: boolean;
  capturedAt: string | null;
  bookmakerCount: number;
  homeMoneyline: number | null;
  awayMoneyline: number | null;
  homeSpread: number | null;
  total: number | null;
  forecast: CfbResearchForecast | null;
  challenger: CfbResearchForecast | null;
};

export type CfbResearchForecast = {
  version: string;
  generatedAt: string;
  status: "RESEARCH_ONLY" | "BENCHMARK_PASSED";
  homePoints: number;
  awayPoints: number;
  homeWinProbability: number;
  homeCurrentGames: number;
  awayCurrentGames: number;
  homePpaPlays: number;
  awayPpaPlays: number;
  explanation: Record<string, unknown>;
};

export type CfbForecastValidation = {
  version: string;
  generatedAt: string;
  status: "RESEARCH_ONLY" | "BENCHMARK_PASSED";
  trainingGames: number;
  trainingSeasons: number[];
  holdoutSeason: number;
  holdoutGames: number;
  forwardSeason: number;
  matchedMarketGames: number;
  prospectiveGames: number;
  modelMarginMae: number | null;
  marketMarginMae: number | null;
  modelTotalMae: number | null;
  marketTotalMae: number | null;
  modelBrier: number | null;
  marketBrier: number | null;
  prospectiveModelMarginMae: number | null;
  prospectiveMarketMarginMae: number | null;
  prospectiveModelTotalMae: number | null;
  prospectiveMarketTotalMae: number | null;
  prospectiveModelBrier: number | null;
  prospectiveMarketBrier: number | null;
};

export type CfbChallengerValidation = {
  version: string;
  generatedAt: string;
  holdoutGames: number;
  forwardGames: number;
  prospectiveGames: number;
  holdout: Record<string, { n: number; mean: number | null }>;
  forward: Record<string, { n: number; mean: number | null }>;
  prospective: Record<string, { n: number; mean: number | null }>;
};

export type CfbOpponentPpaValidation = {
  version: string; generatedAt: string;
  holdoutGames: number; forwardGames: number;
  holdout: Record<string, { n: number; mean: number | null }>;
  forward: Record<string, { n: number; mean: number | null }>;
};

export type CfbAnalyticsResult = {
  id: number;
  date: string;
  opponent: string;
  home: boolean;
  pointsFor: number;
  pointsAgainst: number;
};

const numberOrNull = (value: unknown): number | null => value == null ? null : Number(value);
const record = (value: unknown): Record<string, unknown> => value && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : {};

function featureFromRow(row: Record<string, unknown>, prefix: string): CfbAnalyticsFeature | null {
  if (row[`${prefix}FeatureVersion`] == null) return null;
  return {
    version: String(row[`${prefix}FeatureVersion`]),
    asOf: String(row[`${prefix}FeatureAsOf`]),
    gamesPlayed: Number(row[`${prefix}GamesPlayed`] ?? 0),
    currentWeight: Number(row[`${prefix}CurrentWeight`] ?? 0),
    completeness: numberOrNull(row[`${prefix}Completeness`]),
    values: record(row[`${prefix}Values`]),
  };
}

function gameFromRow(row: Record<string, unknown>): CfbAnalyticsGame {
  return {
    id: Number(row.id),
    cfbdGameId: Number(row.cfbdGameId),
    gameDate: String(row.gameDate),
    observedAt: String(row.observedAt),
    kickoff: row.kickoff == null ? null : String(row.kickoff),
    kickoffTbd: Boolean(row.kickoffTbd),
    completed: Boolean(row.completed),
    homeScore: numberOrNull(row.homeScore),
    awayScore: numberOrNull(row.awayScore),
    home: {
      id: Number(row.homeTeamId), name: String(row.homeTeam),
      conference: row.homeConference == null ? null : String(row.homeConference),
      classification: row.homeClassification == null ? null : String(row.homeClassification),
      feature: featureFromRow(row, "home"),
    },
    away: {
      id: Number(row.awayTeamId), name: String(row.awayTeam),
      conference: row.awayConference == null ? null : String(row.awayConference),
      classification: row.awayClassification == null ? null : String(row.awayClassification),
      feature: featureFromRow(row, "away"),
    },
    oddsEventMapped: Boolean(row.oddsEventMapped),
    capturedAt: row.capturedAt == null ? null : String(row.capturedAt),
    bookmakerCount: Number(row.bookmakerCount ?? 0),
    homeMoneyline: numberOrNull(row.homeMoneyline),
    awayMoneyline: numberOrNull(row.awayMoneyline),
    homeSpread: numberOrNull(row.homeSpread),
    total: numberOrNull(row.total),
    forecast: row.forecastVersion == null ? null : {
      version: String(row.forecastVersion),
      generatedAt: String(row.forecastGeneratedAt),
      status: String(row.forecastStatus) as CfbResearchForecast["status"],
      homePoints: Number(row.forecastHomePoints),
      awayPoints: Number(row.forecastAwayPoints),
      homeWinProbability: Number(row.forecastHomeWinProbability),
      homeCurrentGames: Number(row.forecastHomeCurrentGames),
      awayCurrentGames: Number(row.forecastAwayCurrentGames),
      homePpaPlays: Number(row.forecastHomePpaPlays),
      awayPpaPlays: Number(row.forecastAwayPpaPlays),
      explanation: record(row.forecastExplanation),
    },
    challenger: row.challengerVersion == null ? null : {
      version: String(row.challengerVersion),
      generatedAt: String(row.challengerGeneratedAt),
      status: String(row.challengerStatus) as CfbResearchForecast["status"],
      homePoints: Number(row.challengerHomePoints),
      awayPoints: Number(row.challengerAwayPoints),
      homeWinProbability: Number(row.challengerHomeWinProbability),
      homeCurrentGames: Number(row.challengerHomeCurrentGames),
      awayCurrentGames: Number(row.challengerAwayCurrentGames),
      homePpaPlays: Number(row.challengerHomePpaPlays),
      awayPpaPlays: Number(row.challengerAwayPpaPlays),
      explanation: record(row.challengerExplanation),
    },
  };
}

const GAME_SELECT = sql`
  SELECT m.id, m.cfbd_game_id AS "cfbdGameId", m.game_date::text AS "gameDate",
         NOW()::text AS "observedAt",
         m.commence_time::text AS kickoff, m.start_time_tbd AS "kickoffTbd",
         m.completed, m.home_score AS "homeScore", m.away_score AS "awayScore",
         m.home_team_id AS "homeTeamId", ht.name AS "homeTeam",
         ht.conference AS "homeConference", ht.classification AS "homeClassification",
         m.away_team_id AS "awayTeamId", at.name AS "awayTeam",
         at.conference AS "awayConference", at.classification AS "awayClassification",
         (m.odds_event_id IS NOT NULL) AS "oddsEventMapped",
         h.captured_at::text AS "capturedAt", COALESCE(h.bookmaker_count, 0) AS "bookmakerCount",
         h.home_ml AS "homeMoneyline", h.away_ml AS "awayMoneyline",
         h.home_spread AS "homeSpread", h.vegas_total AS total,
         fc.version AS "forecastVersion", fc.generated_at::text AS "forecastGeneratedAt",
         fc.status AS "forecastStatus", fc.home_points AS "forecastHomePoints",
         fc.away_points AS "forecastAwayPoints",
         fc.home_win_probability AS "forecastHomeWinProbability",
         fc.home_current_games AS "forecastHomeCurrentGames",
         fc.away_current_games AS "forecastAwayCurrentGames",
         fc.home_ppa_plays AS "forecastHomePpaPlays",
         fc.away_ppa_plays AS "forecastAwayPpaPlays",
         fc.explanation AS "forecastExplanation",
         v2.version AS "challengerVersion", v2.generated_at::text AS "challengerGeneratedAt",
         v2.status AS "challengerStatus", v2.home_points AS "challengerHomePoints",
         v2.away_points AS "challengerAwayPoints",
         v2.home_win_probability AS "challengerHomeWinProbability",
         v2.home_current_games AS "challengerHomeCurrentGames",
         v2.away_current_games AS "challengerAwayCurrentGames",
         v2.home_ppa_plays AS "challengerHomePpaPlays",
         v2.away_ppa_plays AS "challengerAwayPpaPlays",
         v2.explanation AS "challengerExplanation",
         hf.feature_version AS "homeFeatureVersion", hf.as_of_at::text AS "homeFeatureAsOf",
         hf.games_played AS "homeGamesPlayed", hf.current_weight AS "homeCurrentWeight",
         hf.source_completeness AS "homeCompleteness", hf.features_json AS "homeValues",
         af.feature_version AS "awayFeatureVersion", af.as_of_at::text AS "awayFeatureAsOf",
         af.games_played AS "awayGamesPlayed", af.current_weight AS "awayCurrentWeight",
         af.source_completeness AS "awayCompleteness", af.features_json AS "awayValues"
  FROM cfb_matchups m
  JOIN cfb_teams ht ON ht.team_id=m.home_team_id
  JOIN cfb_teams at ON at.team_id=m.away_team_id
  LEFT JOIN LATERAL (
    SELECT captured_at, bookmaker_count, home_ml, away_ml, home_spread, vegas_total
    FROM game_odds_history
    WHERE sport='cfb' AND matchup_id=m.id AND captured_at < m.commence_time
    ORDER BY captured_at DESC, id DESC LIMIT 1
  ) h ON TRUE
  LEFT JOIN LATERAL (
    SELECT r.version, r.generated_at, r.status,
           f.home_points, f.away_points, f.home_win_probability,
           f.home_current_games, f.away_current_games,
           f.home_ppa_plays, f.away_ppa_plays,
           r.report_json->'explanations'->(m.id::text) AS explanation
    FROM cfb_game_forecasts f
    JOIN cfb_forecast_runs r ON r.id=f.run_id
    WHERE f.game_id=m.id AND f.kickoff=m.commence_time
      AND r.version='cfb-score-context-v1'
      AND r.generated_at < m.commence_time
    ORDER BY r.generated_at DESC, f.id DESC LIMIT 1
  ) fc ON TRUE
  LEFT JOIN LATERAL (
    SELECT r.version, r.generated_at, r.status,
           f.home_points, f.away_points, f.home_win_probability,
           f.home_current_games, f.away_current_games,
           f.home_ppa_plays, f.away_ppa_plays,
           r.report_json->'explanations'->(m.id::text) AS explanation
    FROM cfb_game_forecasts f
    JOIN cfb_forecast_runs r ON r.id=f.run_id
    WHERE f.game_id=m.id AND f.kickoff=m.commence_time
      AND r.version='cfb-score-possession-v2'
      AND r.generated_at < m.commence_time
    ORDER BY r.generated_at DESC, f.id DESC LIMIT 1
  ) v2 ON TRUE
  LEFT JOIN LATERAL (
    SELECT feature_version, as_of_at, games_played, current_weight, source_completeness, features_json
    FROM cfb_team_game_features
    WHERE game_id=m.id AND team_id=m.home_team_id AND feature_version='cfb-team-context-v2'
      AND available_at <= LEAST(NOW(), m.commence_time)
    ORDER BY as_of_at DESC, id DESC LIMIT 1
  ) hf ON TRUE
  LEFT JOIN LATERAL (
    SELECT feature_version, as_of_at, games_played, current_weight, source_completeness, features_json
    FROM cfb_team_game_features
    WHERE game_id=m.id AND team_id=m.away_team_id AND feature_version='cfb-team-context-v2'
      AND available_at <= LEAST(NOW(), m.commence_time)
    ORDER BY as_of_at DESC, id DESC LIMIT 1
  ) af ON TRUE
`;

export async function getCfbAnalyticsGames(): Promise<CfbAnalyticsGame[]> {
  const rows = await db.execute(sql`${GAME_SELECT}
    WHERE m.game_date BETWEEN (NOW() AT TIME ZONE 'America/New_York')::date
                          AND ((NOW() AT TIME ZONE 'America/New_York')::date + 14)
      AND m.commence_time >= NOW() - INTERVAL '1 hour'
    ORDER BY m.commence_time NULLS LAST, m.id LIMIT 180`);
  return rows.rows.map((row) => gameFromRow(row as Record<string, unknown>));
}

export async function getCfbAnalyticsGame(id: number): Promise<CfbAnalyticsGame | null> {
  const rows = await db.execute(sql`${GAME_SELECT} WHERE m.id=${id} LIMIT 1`);
  return rows.rows.length ? gameFromRow(rows.rows[0] as Record<string, unknown>) : null;
}

export async function getCfbAnalyticsTeams(): Promise<CfbAnalyticsTeam[]> {
  const rows = await db.execute(sql`
    SELECT team_id AS id, name, conference, classification
    FROM cfb_teams WHERE active=TRUE AND LOWER(classification)='fbs'
    ORDER BY name`);
  return rows.rows.map((row) => ({
    id: Number(row.id), name: String(row.name),
    conference: row.conference == null ? null : String(row.conference),
    classification: row.classification == null ? null : String(row.classification), feature: null,
  }));
}

export async function getCfbAnalyticsTeam(id: number): Promise<{ team: CfbAnalyticsTeam; results: CfbAnalyticsResult[]; games: CfbAnalyticsGame[] } | null> {
  const rows = await db.execute(sql`
    SELECT t.team_id AS id, t.name, t.conference, t.classification,
           f.feature_version AS "teamFeatureVersion", f.as_of_at::text AS "teamFeatureAsOf",
           f.games_played AS "teamGamesPlayed", f.current_weight AS "teamCurrentWeight",
           f.source_completeness AS "teamCompleteness", f.features_json AS "teamValues"
    FROM cfb_teams t
    LEFT JOIN LATERAL (
      SELECT feature_version, as_of_at, games_played, current_weight, source_completeness, features_json
      FROM cfb_team_game_features
      WHERE team_id=t.team_id AND feature_version='cfb-team-context-v2'
        AND available_at <= NOW()
      ORDER BY as_of_at DESC, id DESC LIMIT 1
    ) f ON TRUE
    WHERE t.team_id=${id} LIMIT 1`);
  if (!rows.rows.length) return null;
  const row = rows.rows[0] as Record<string, unknown>;
  const team: CfbAnalyticsTeam = {
    id: Number(row.id), name: String(row.name),
    conference: row.conference == null ? null : String(row.conference),
    classification: row.classification == null ? null : String(row.classification),
    feature: featureFromRow(row, "team"),
  };
  const resultRows = await db.execute(sql`
    SELECT m.id, m.game_date::text AS date,
           CASE WHEN m.home_team_id=${id} THEN at.name ELSE ht.name END AS opponent,
           (m.home_team_id=${id}) AS home,
           CASE WHEN m.home_team_id=${id} THEN m.home_score ELSE m.away_score END AS "pointsFor",
           CASE WHEN m.home_team_id=${id} THEN m.away_score ELSE m.home_score END AS "pointsAgainst"
    FROM cfb_matchups m
    JOIN cfb_teams ht ON ht.team_id=m.home_team_id
    JOIN cfb_teams at ON at.team_id=m.away_team_id
    WHERE (m.home_team_id=${id} OR m.away_team_id=${id})
      AND m.completed=TRUE AND m.home_score IS NOT NULL AND m.away_score IS NOT NULL
    ORDER BY m.commence_time DESC LIMIT 8`);
  const results = resultRows.rows.map((r) => ({
    id: Number(r.id), date: String(r.date), opponent: String(r.opponent),
    home: Boolean(r.home), pointsFor: Number(r.pointsFor), pointsAgainst: Number(r.pointsAgainst),
  }));
  const allGames = await getCfbAnalyticsGames();
  const games = allGames.filter((game) => game.home.id === id || game.away.id === id);
  return { team, results, games };
}

export type CfbAnalyticsCoverage = {
  season: number;
  playRows: number;
  ppaRows: number;
  driveRows: number;
  featureGames: number;
  rosterTeams: number;
};

export async function getCfbAnalyticsCoverage(): Promise<CfbAnalyticsCoverage[]> {
  const rows = await db.execute(sql`
    WITH seasons AS (
      SELECT generate_series(EXTRACT(YEAR FROM NOW())::int - 4, EXTRACT(YEAR FROM NOW())::int) AS season
    ), plays AS (
      SELECT season, COUNT(*)::int AS play_rows,
             COUNT(*) FILTER (WHERE ppa IS NOT NULL)::int AS ppa_rows
      FROM cfb_plays WHERE season >= EXTRACT(YEAR FROM NOW())::int - 4 GROUP BY season
    ), drives AS (
      SELECT season, COUNT(*)::int AS drive_rows
      FROM cfb_drives WHERE season >= EXTRACT(YEAR FROM NOW())::int - 4 GROUP BY season
    ), features AS (
      SELECT m.season, COUNT(DISTINCT f.game_id)::int AS feature_games
      FROM cfb_team_game_features f JOIN cfb_matchups m ON m.id=f.game_id
      WHERE f.feature_version='cfb-team-context-v2'
        AND m.season >= EXTRACT(YEAR FROM NOW())::int - 4 GROUP BY m.season
    ), rosters AS (
      SELECT season, COUNT(DISTINCT team_id)::int AS roster_teams
      FROM cfb_roster_snapshots
      WHERE point_in_time_eligible=TRUE AND season >= EXTRACT(YEAR FROM NOW())::int - 4
      GROUP BY season
    )
    SELECT s.season, COALESCE(p.play_rows,0) AS "playRows", COALESCE(p.ppa_rows,0) AS "ppaRows",
           COALESCE(d.drive_rows,0) AS "driveRows", COALESCE(f.feature_games,0) AS "featureGames",
           COALESCE(r.roster_teams,0) AS "rosterTeams"
    FROM seasons s LEFT JOIN plays p USING (season) LEFT JOIN drives d USING (season)
    LEFT JOIN features f USING (season) LEFT JOIN rosters r USING (season)
    ORDER BY s.season DESC`);
  return rows.rows.map((row) => ({
    season: Number(row.season), playRows: Number(row.playRows), ppaRows: Number(row.ppaRows),
    driveRows: Number(row.driveRows), featureGames: Number(row.featureGames), rosterTeams: Number(row.rosterTeams),
  }));
}

export async function getCfbForecastValidation(): Promise<CfbForecastValidation | null> {
  const rows = await db.execute(sql`
    SELECT version, generated_at::text AS "generatedAt", status,
           report_json AS report
    FROM cfb_forecast_runs WHERE version='cfb-score-context-v1' ORDER BY id DESC LIMIT 1`);
  if (!rows.rows.length) return null;
  const row = rows.rows[0] as Record<string, unknown>;
  const report = record(row.report);
  const training = record(report.training);
  const holdout = record(report.holdout ?? report.holdout_2025);
  const forward = record(report.forward ?? report.forward_2026);
  const comparison = record(forward.market_comparison);
  const mean = (key: string) => numberOrNull(record(comparison[key]).mean);
  const prospective = record(report.prospective);
  const prospectiveComparison = record(prospective.market_comparison);
  const prospectiveMean = (key: string) => numberOrNull(record(prospectiveComparison[key]).mean);
  return {
    version: String(row.version),
    generatedAt: String(row.generatedAt),
    status: String(row.status) as CfbForecastValidation["status"],
    trainingGames: Number(training.games ?? 0),
    trainingSeasons: Array.isArray(training.seasons) ? training.seasons.map(Number) : [],
    holdoutSeason: Number(holdout.season ?? 2025),
    holdoutGames: Number(holdout.games ?? 0),
    forwardSeason: Number(forward.season ?? 2026),
    matchedMarketGames: Number(forward.matched_market_games ?? 0),
    prospectiveGames: Number(prospective.games ?? 0),
    modelMarginMae: mean("model_margin_error"),
    marketMarginMae: mean("market_margin_error"),
    modelTotalMae: mean("model_total_error"),
    marketTotalMae: mean("market_total_error"),
    modelBrier: mean("model_brier"),
    marketBrier: mean("market_brier"),
    prospectiveModelMarginMae: prospectiveMean("model_margin_error"),
    prospectiveMarketMarginMae: prospectiveMean("market_margin_error"),
    prospectiveModelTotalMae: prospectiveMean("model_total_error"),
    prospectiveMarketTotalMae: prospectiveMean("market_total_error"),
    prospectiveModelBrier: prospectiveMean("model_brier"),
    prospectiveMarketBrier: prospectiveMean("market_brier"),
  };
}

export async function getCfbChallengerValidation(): Promise<CfbChallengerValidation | null> {
  const rows = await db.execute(sql`
    SELECT version, generated_at::text AS "generatedAt", report_json AS report
    FROM cfb_forecast_runs WHERE version='cfb-score-possession-v2'
    ORDER BY id DESC LIMIT 1`);
  if (!rows.rows.length) return null;
  const row = rows.rows[0] as Record<string, unknown>;
  const report = record(row.report);
  const holdout = record(report.holdout);
  const forward = record(report.forward);
  const prospective = record(report.prospective);
  const metrics = (value: unknown) => {
    const output: Record<string, { n: number; mean: number | null }> = {};
    for (const [key, raw] of Object.entries(record(value))) {
      const metric = record(raw);
      output[key] = { n: Number(metric.n ?? 0), mean: numberOrNull(metric.mean) };
    }
    return output;
  };
  return {
    version: String(row.version), generatedAt: String(row.generatedAt),
    holdoutGames: Number(holdout.games ?? 0),
    forwardGames: Number(forward.games ?? 0),
    prospectiveGames: Number(prospective.games ?? 0),
    holdout: metrics(holdout.metrics),
    forward: metrics(forward.metrics),
    prospective: metrics(record(prospective.market_comparison)),
  };
}

export async function getCfbOpponentPpaValidation(): Promise<CfbOpponentPpaValidation | null> {
  const rows = await db.execute(sql`
    SELECT version,generated_at::text AS "generatedAt",report_json AS report
    FROM cfb_forecast_runs WHERE version='cfb-score-opponent-ppa-v3'
    ORDER BY id DESC LIMIT 1`);
  if (!rows.rows.length) return null;
  const row = rows.rows[0] as Record<string, unknown>;
  const report = record(row.report);
  const holdout = record(report.holdout);
  const forward = record(report.forward);
  const metrics = (value: unknown) => Object.fromEntries(Object.entries(record(value)).map(([key, raw]) => {
    const metric = record(raw);
    return [key, { n: Number(metric.n ?? 0), mean: numberOrNull(metric.mean) }];
  }));
  return { version: String(row.version), generatedAt: String(row.generatedAt),
    holdoutGames: Number(holdout.games ?? 0), forwardGames: Number(forward.games ?? 0),
    holdout: metrics(holdout.metrics), forward: metrics(forward.metrics) };
}
