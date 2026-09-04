import {
  pgTable,
  serial,
  bigserial,
  text,
  integer,
  bigint,
  doublePrecision,
  real,
  boolean,
  date,
  timestamp,
  jsonb,
  uuid,
  unique,
  index,
} from "drizzle-orm/pg-core";

// ── NBA tables ────────────────────────────────────────────────

export const nflDfsFeatureAudits = pgTable("nfl_dfs_feature_audits", {
  auditDigest: text("audit_digest").primaryKey(),
  version: text("version").notNull(),
  payload: jsonb("payload").notNull(),
  inputEvidence: jsonb("input_evidence").notNull(),
  createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
});

export const nflDfsWeeklyReportCards = pgTable("nfl_dfs_weekly_report_cards", {
  reportDigest: text("report_digest").primaryKey(),
  season: integer("season").notNull(),
  week: integer("week").notNull(),
  payload: jsonb("payload").notNull(),
  createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
});

export const teams = pgTable("teams", {
  teamId: serial("team_id").primaryKey(),
  name: text("name").notNull().unique(),
  abbreviation: text("abbreviation").notNull().unique(),
  conference: text("conference").default(""),
  division: text("division").default(""),
  nbaId: integer("nba_id"),
  logoUrl: text("logo_url").default(""),
  createdAt: timestamp("created_at").defaultNow(),
});

export const nbaTeamStats = pgTable(
  "nba_team_stats",
  {
    id: serial("id").primaryKey(),
    teamId: integer("team_id")
      .notNull()
      .references(() => teams.teamId),
    season: text("season").notNull(),
    pace: doublePrecision("pace"),
    offRtg: doublePrecision("off_rtg"),
    defRtg: doublePrecision("def_rtg"),
    fetchedAt: timestamp("fetched_at").defaultNow(),
  },
  (t) => [unique("nba_team_stats_team_season_key").on(t.teamId, t.season)]
);

export const nbaPlayerStats = pgTable(
  "nba_player_stats",
  {
    id: serial("id").primaryKey(),
    playerId: integer("player_id").notNull(),
    season: text("season").notNull(),
    teamId: integer("team_id").references(() => teams.teamId),
    name: text("name").notNull(),
    position: text("position"),
    games: integer("games"),
    avgMinutes: doublePrecision("avg_minutes"),
    ppg: doublePrecision("ppg"),
    rpg: doublePrecision("rpg"),
    apg: doublePrecision("apg"),
    spg: doublePrecision("spg"),
    bpg: doublePrecision("bpg"),
    tovpg: doublePrecision("tovpg"),
    threefgmPg: doublePrecision("threefgm_pg"),
    usageRate: doublePrecision("usage_rate"),
    ddRate: doublePrecision("dd_rate"),
    fptsStd: doublePrecision("fpts_std"),
    fetchedAt: timestamp("fetched_at").defaultNow(),
  },
  (t) => [unique("nba_player_stats_player_season_key").on(t.playerId, t.season)]
);

export const nbaPlayerGameLogs = pgTable(
  "nba_player_game_logs",
  {
    id: serial("id").primaryKey(),
    season: text("season").notNull(),
    seasonType: text("season_type").notNull(),
    playerId: integer("player_id").notNull(),
    name: text("name").notNull(),
    teamId: integer("team_id").references(() => teams.teamId),
    opponentTeamId: integer("opponent_team_id").references(() => teams.teamId),
    gameId: text("game_id").notNull(),
    gameDate: date("game_date"),
    matchup: text("matchup"),
    teamAbbreviation: text("team_abbreviation"),
    opponentAbbreviation: text("opponent_abbreviation"),
    isHome: boolean("is_home"),
    winLoss: text("win_loss"),
    minutes: doublePrecision("minutes"),
    points: doublePrecision("points"),
    rebounds: doublePrecision("rebounds"),
    assists: doublePrecision("assists"),
    steals: doublePrecision("steals"),
    blocks: doublePrecision("blocks"),
    turnovers: doublePrecision("turnovers"),
    fgm: doublePrecision("fgm"),
    fga: doublePrecision("fga"),
    fg3m: doublePrecision("fg3m"),
    fg3a: doublePrecision("fg3a"),
    ftm: doublePrecision("ftm"),
    fta: doublePrecision("fta"),
    plusMinus: doublePrecision("plus_minus"),
    fetchedAt: timestamp("fetched_at").defaultNow(),
  },
  (t) => [
    unique("nba_player_game_logs_unique_key").on(t.season, t.seasonType, t.playerId, t.gameId),
    index("idx_nba_player_game_logs_player_date").on(t.playerId, t.gameDate),
    index("idx_nba_player_game_logs_team_date").on(t.teamId, t.gameDate),
  ]
);

export const nbaTeamGameLogs = pgTable(
  "nba_team_game_logs",
  {
    id: serial("id").primaryKey(),
    season: text("season").notNull(),
    seasonType: text("season_type").notNull(),
    teamId: integer("team_id")
      .notNull()
      .references(() => teams.teamId),
    opponentTeamId: integer("opponent_team_id").references(() => teams.teamId),
    teamName: text("team_name").notNull(),
    teamAbbreviation: text("team_abbreviation"),
    opponentAbbreviation: text("opponent_abbreviation"),
    gameId: text("game_id").notNull(),
    gameDate: date("game_date"),
    matchup: text("matchup"),
    isHome: boolean("is_home"),
    winLoss: text("win_loss"),
    fg3m: doublePrecision("fg3m"),
    fg3a: doublePrecision("fg3a"),
    oppFg3m: doublePrecision("opp_fg3m"),
    oppFg3a: doublePrecision("opp_fg3a"),
    pts: doublePrecision("pts"),
    oppPts: doublePrecision("opp_pts"),
    ast: doublePrecision("ast"),
    reb: doublePrecision("reb"),
    oppAst: doublePrecision("opp_ast"),
    oppReb: doublePrecision("opp_reb"),
    fga: doublePrecision("fga"),
    fta: doublePrecision("fta"),
    oreb: doublePrecision("oreb"),
    tov: doublePrecision("tov"),
    oppFga: doublePrecision("opp_fga"),
    oppFta: doublePrecision("opp_fta"),
    oppOreb: doublePrecision("opp_oreb"),
    oppTov: doublePrecision("opp_tov"),
    plusMinus: doublePrecision("plus_minus"),
    fetchedAt: timestamp("fetched_at").defaultNow(),
  },
  (t) => [
    unique("nba_team_game_logs_unique_key").on(t.season, t.seasonType, t.teamId, t.gameId),
    index("idx_nba_team_game_logs_team_date").on(t.teamId, t.gameDate),
    index("idx_nba_team_game_logs_opp_date").on(t.opponentTeamId, t.gameDate),
  ]
);

export const nbaMatchups = pgTable(
  "nba_matchups",
  {
    id: serial("id").primaryKey(),
    gameDate: date("game_date").notNull(),
    gameId: text("game_id").unique(),
    homeTeamId: integer("home_team_id").references(() => teams.teamId),
    awayTeamId: integer("away_team_id").references(() => teams.teamId),
    homeMl: integer("home_ml"),
    awayMl: integer("away_ml"),
    homeSpread: doublePrecision("home_spread"),
    vegasTotal: doublePrecision("vegas_total"),
    homeWinProb: doublePrecision("vegas_prob_home"),
    homeImplied: doublePrecision("home_implied"),
    awayImplied: doublePrecision("away_implied"),
    homeScore: integer("home_score"),
    awayScore: integer("away_score"),
    ourGameTotalPred: doublePrecision("our_game_total_pred"),
    fetchedAt: timestamp("fetched_at").defaultNow(),
  },
  (t) => [
    unique("nba_matchups_date_teams_key").on(t.gameDate, t.homeTeamId, t.awayTeamId),
    index("idx_nba_matchups_date").on(t.gameDate),
  ]
);

// ── MLB tables ────────────────────────────────────────────────

export const mlbTeams = pgTable("mlb_teams", {
  teamId: serial("team_id").primaryKey(),
  name: text("name").notNull().unique(),
  abbreviation: text("abbreviation").notNull().unique(),
  dkAbbrev: text("dk_abbrev"),
  ballpark: text("ballpark"),
  city: text("city"),
  division: text("division"),
  mlbId: integer("mlb_id").unique(),
  logoUrl: text("logo_url").default(""),
  createdAt: timestamp("created_at").defaultNow(),
});

export const nflTeams = pgTable("nfl_teams", {
  teamId: serial("team_id").primaryKey(),
  name: text("name").notNull().unique(),
  abbreviation: text("abbreviation").notNull().unique(),
  oddsApiName: text("odds_api_name").notNull().unique(),
  city: text("city"),
  conference: text("conference"),
  division: text("division"),
  active: boolean("active").notNull().default(true),
  logoUrl: text("logo_url").default(""),
  updatedAt: timestamp("updated_at", { withTimezone: true }).notNull().defaultNow(),
});

export const nflMatchups = pgTable(
  "nfl_matchups",
  {
    id: serial("id").primaryKey(),
    eventId: text("event_id").notNull().unique(),
    season: integer("season"),
    seasonType: text("season_type"),
    week: integer("week"),
    gameDate: date("game_date").notNull(),
    commenceTime: timestamp("commence_time", { withTimezone: true }).notNull(),
    homeTeamId: integer("home_team_id").notNull().references(() => nflTeams.teamId),
    awayTeamId: integer("away_team_id").notNull().references(() => nflTeams.teamId),
    gameStatus: text("game_status"),
    completed: boolean("completed").notNull().default(false),
    homeScore: integer("home_score"),
    awayScore: integer("away_score"),
    vegasTotal: doublePrecision("vegas_total"),
    homeMl: integer("home_ml"),
    awayMl: integer("away_ml"),
    homeSpread: doublePrecision("home_spread"),
    vegasProbHome: doublePrecision("vegas_prob_home"),
    homeImplied: doublePrecision("home_implied"),
    awayImplied: doublePrecision("away_implied"),
    fetchedAt: timestamp("fetched_at", { withTimezone: true }).notNull().defaultNow(),
    scoreFetchedAt: timestamp("score_fetched_at", { withTimezone: true }),
    finalAt: timestamp("final_at", { withTimezone: true }),
  },
  (t) => [
    index("idx_nfl_matchups_date").on(t.gameDate, t.commenceTime),
    index("idx_nfl_matchups_upcoming").on(t.commenceTime),
  ],
);

export const cfbTeams = pgTable("cfb_teams", {
  teamId: serial("team_id").primaryKey(),
  cfbdTeamId: integer("cfbd_team_id").notNull().unique(),
  name: text("name").notNull().unique(),
  abbreviation: text("abbreviation"),
  conference: text("conference"),
  classification: text("classification"),
  logoUrl: text("logo_url").default(""),
  active: boolean("active").notNull().default(true),
  updatedAt: timestamp("updated_at", { withTimezone: true }).notNull().defaultNow(),
});

export const cfbTeamAliases = pgTable(
  "cfb_team_aliases",
  {
    id: serial("id").primaryKey(),
    provider: text("provider").notNull(),
    alias: text("alias").notNull(),
    teamId: integer("team_id").notNull().references(() => cfbTeams.teamId),
    reviewed: boolean("reviewed").notNull().default(false),
    createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
  },
  (t) => [unique("cfb_team_aliases_provider_alias_key").on(t.provider, t.alias)],
);

export const cfbVenues = pgTable("cfb_venues", {
  venueId: serial("venue_id").primaryKey(),
  cfbdVenueId: integer("cfbd_venue_id").unique(),
  name: text("name").notNull(),
  city: text("city"),
  state: text("state"),
  latitude: doublePrecision("latitude"),
  longitude: doublePrecision("longitude"),
  timezone: text("timezone"),
  updatedAt: timestamp("updated_at", { withTimezone: true }).notNull().defaultNow(),
});

export const cfbMatchups = pgTable(
  "cfb_matchups",
  {
    id: serial("id").primaryKey(),
    cfbdGameId: bigint("cfbd_game_id", { mode: "number" }).notNull().unique(),
    oddsEventId: text("odds_event_id").unique(),
    season: integer("season").notNull(),
    seasonType: text("season_type").notNull(),
    week: integer("week").notNull(),
    gameDate: date("game_date").notNull(),
    commenceTime: timestamp("commence_time", { withTimezone: true }),
    startTimeTbd: boolean("start_time_tbd").notNull().default(false),
    homeTeamId: integer("home_team_id").notNull().references(() => cfbTeams.teamId),
    awayTeamId: integer("away_team_id").notNull().references(() => cfbTeams.teamId),
    venueId: integer("venue_id").references(() => cfbVenues.venueId),
    neutralSite: boolean("neutral_site").notNull().default(false),
    conferenceGame: boolean("conference_game").notNull().default(false),
    network: text("network"),
    gameStatus: text("game_status"),
    completed: boolean("completed").notNull().default(false),
    homeScore: integer("home_score"),
    awayScore: integer("away_score"),
    homeLineScores: jsonb("home_line_scores"),
    awayLineScores: jsonb("away_line_scores"),
    wentToOvertime: boolean("went_to_overtime").notNull().default(false),
    overtimePeriods: integer("overtime_periods").notNull().default(0),
    vegasTotal: doublePrecision("vegas_total"),
    homeMl: integer("home_ml"),
    awayMl: integer("away_ml"),
    homeSpread: doublePrecision("home_spread"),
    vegasProbHome: doublePrecision("vegas_prob_home"),
    homeImplied: doublePrecision("home_implied"),
    awayImplied: doublePrecision("away_implied"),
    fetchedAt: timestamp("fetched_at", { withTimezone: true }).notNull().defaultNow(),
    oddsFetchedAt: timestamp("odds_fetched_at", { withTimezone: true }),
    scoreFetchedAt: timestamp("score_fetched_at", { withTimezone: true }),
    finalAt: timestamp("final_at", { withTimezone: true }),
  },
  (t) => [
    index("idx_cfb_matchups_date").on(t.gameDate, t.commenceTime),
    index("idx_cfb_matchups_upcoming").on(t.commenceTime),
  ],
);

export const cfbHistoricalGameLines = pgTable(
  "cfb_historical_game_lines",
  {
    id: bigserial("id", { mode: "number" }).primaryKey(),
    gameId: integer("game_id").notNull().references(() => cfbMatchups.id, { onDelete: "cascade" }),
    provider: text("provider").notNull(),
    marketType: text("market_type").notNull(),
    homeValue: doublePrecision("home_value"),
    awayValue: doublePrecision("away_value"),
    homePrice: integer("home_price"),
    awayPrice: integer("away_price"),
    lineDesignation: text("line_designation").notNull(),
    homeConference: text("home_conference"),
    awayConference: text("away_conference"),
    homeClassification: text("home_classification"),
    awayClassification: text("away_classification"),
    sourceEventId: text("source_event_id"),
    sourceUpdatedAt: timestamp("source_updated_at", { withTimezone: true }),
    availableAt: timestamp("available_at", { withTimezone: true }),
    capturedAt: timestamp("captured_at", { withTimezone: true }).notNull().defaultNow(),
    rawPayloadHash: text("raw_payload_hash").notNull(),
    isCanonicalReference: boolean("is_canonical_reference").notNull().default(false),
  },
  (t) => [
    unique("cfb_historical_game_lines_source_key").on(
      t.gameId, t.provider, t.marketType, t.lineDesignation, t.rawPayloadHash,
    ),
    index("idx_cfb_history_game_market").on(t.gameId, t.marketType, t.lineDesignation),
  ],
);

export const cfbStaffRegimes = pgTable(
  "cfb_staff_regimes",
  {
    id: bigserial("id", { mode: "number" }).primaryKey(),
    teamId: integer("team_id").notNull().references(() => cfbTeams.teamId, { onDelete: "cascade" }),
    role: text("role").notNull(),
    sourcePersonId: text("source_person_id"),
    personName: text("person_name").notNull(),
    startSeason: integer("start_season").notNull(),
    startWeek: integer("start_week").notNull().default(0),
    endSeason: integer("end_season"),
    endWeek: integer("end_week"),
    schemeLabel: text("scheme_label"),
    source: text("source").notNull(),
    availableAt: timestamp("available_at", { withTimezone: true }).notNull(),
    capturedAt: timestamp("captured_at", { withTimezone: true }).notNull().defaultNow(),
    sourceJson: jsonb("source_json").notNull().default({}),
  },
  (t) => [
    unique("cfb_staff_regimes_identity_key").on(t.teamId, t.role, t.personName, t.startSeason, t.startWeek),
    index("idx_cfb_staff_regime_team").on(t.teamId, t.startSeason, t.endSeason),
  ],
);

export const cfbRosterSnapshots = pgTable(
  "cfb_roster_snapshots",
  {
    id: bigserial("id", { mode: "number" }).primaryKey(),
    teamId: integer("team_id").notNull().references(() => cfbTeams.teamId, { onDelete: "cascade" }),
    season: integer("season").notNull(),
    source: text("source").notNull(),
    sourceUpdatedAt: timestamp("source_updated_at", { withTimezone: true }),
    availableAt: timestamp("available_at", { withTimezone: true }).notNull(),
    capturedAt: timestamp("captured_at", { withTimezone: true }).notNull().defaultNow(),
    payloadHash: text("payload_hash").notNull(),
    confidence: doublePrecision("confidence").notNull().default(0.5),
    isComplete: boolean("is_complete").notNull().default(false),
    pointInTimeEligible: boolean("point_in_time_eligible").notNull().default(false),
    summaryJson: jsonb("summary_json").notNull().default({}),
  },
  (t) => [
    unique("cfb_roster_snapshots_source_key").on(t.teamId, t.season, t.source, t.payloadHash),
    index("idx_cfb_roster_snapshot_asof").on(t.teamId, t.season, t.availableAt),
  ],
);

export const cfbRosterPlayers = pgTable(
  "cfb_roster_players",
  {
    id: bigserial("id", { mode: "number" }).primaryKey(),
    snapshotId: bigint("snapshot_id", { mode: "number" }).notNull().references(() => cfbRosterSnapshots.id, { onDelete: "cascade" }),
    sourcePlayerId: text("source_player_id").notNull(),
    normalizedName: text("normalized_name").notNull(),
    displayName: text("display_name").notNull(),
    position: text("position"),
    positionGroup: text("position_group"),
    classYear: integer("class_year"),
    previousTeamId: integer("previous_team_id").references(() => cfbTeams.teamId),
    depthRole: text("depth_role"),
    availabilityStatus: text("availability_status"),
    availabilityConfidence: doublePrecision("availability_confidence"),
    attributesJson: jsonb("attributes_json").notNull().default({}),
  },
  (t) => [
    unique("cfb_roster_players_snapshot_player_key").on(t.snapshotId, t.sourcePlayerId),
    index("idx_cfb_roster_players_snapshot").on(t.snapshotId, t.positionGroup),
  ],
);

export const cfbTeamGameFeatures = pgTable(
  "cfb_team_game_features",
  {
    id: bigserial("id", { mode: "number" }).primaryKey(),
    gameId: integer("game_id").notNull().references(() => cfbMatchups.id, { onDelete: "cascade" }),
    teamId: integer("team_id").notNull().references(() => cfbTeams.teamId, { onDelete: "cascade" }),
    opponentTeamId: integer("opponent_team_id").notNull().references(() => cfbTeams.teamId),
    featureVersion: text("feature_version").notNull(),
    asOfAt: timestamp("as_of_at", { withTimezone: true }).notNull(),
    availableAt: timestamp("available_at", { withTimezone: true }).notNull(),
    gamesPlayed: integer("games_played").notNull().default(0),
    effectiveGames: doublePrecision("effective_games").notNull().default(0),
    currentWeight: doublePrecision("current_weight").notNull().default(0),
    priorWeight: doublePrecision("prior_weight").notNull().default(1),
    featuresJson: jsonb("features_json").notNull().default({}),
    sourceCompleteness: doublePrecision("source_completeness"),
    createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
  },
  (t) => [
    unique("cfb_team_game_features_snapshot_identity_key").on(t.gameId, t.teamId, t.featureVersion, t.asOfAt),
    index("idx_cfb_team_features_asof").on(t.teamId, t.asOfAt, t.featureVersion),
  ],
);

export const cfbHypotheses = pgTable(
  "cfb_hypotheses",
  {
    id: bigserial("id", { mode: "number" }).primaryKey(),
    hypothesisKey: text("hypothesis_key").notNull(),
    version: text("version").notNull(),
    name: text("name").notNull(),
    claim: text("claim").notNull(),
    status: text("status").notNull(),
    outcomeDefinitionJson: jsonb("outcome_definition_json").notNull(),
    populationFilterJson: jsonb("population_filter_json").notNull(),
    featureDefinitionJson: jsonb("feature_definition_json").notNull().default({}),
    bucketDefinitionJson: jsonb("bucket_definition_json").notNull().default({}),
    minSampleJson: jsonb("min_sample_json").notNull().default({}),
    splitPlanJson: jsonb("split_plan_json").notNull().default({}),
    testPlanJson: jsonb("test_plan_json").notNull().default({}),
    promotionRulesJson: jsonb("promotion_rules_json").notNull().default({}),
    multipleTestFamily: text("multiple_test_family"),
    registeredAt: timestamp("registered_at", { withTimezone: true }).notNull().defaultNow(),
    frozenAt: timestamp("frozen_at", { withTimezone: true }),
    retiredAt: timestamp("retired_at", { withTimezone: true }),
    notes: text("notes"),
  },
  (t) => [unique("cfb_hypotheses_key_version_key").on(t.hypothesisKey, t.version)],
);

export const cfbHypothesisResults = pgTable("cfb_hypothesis_results", {
  id: bigserial("id", { mode: "number" }).primaryKey(),
  hypothesisId: bigint("hypothesis_id", { mode: "number" }).notNull().references(() => cfbHypotheses.id, { onDelete: "cascade" }),
  evaluationType: text("evaluation_type").notNull(),
  trainStart: date("train_start"),
  trainEnd: date("train_end"),
  testStart: date("test_start"),
  testEnd: date("test_end"),
  n: integer("n").notNull().default(0),
  wins: integer("wins").notNull().default(0),
  losses: integer("losses").notNull().default(0),
  pushes: integer("pushes").notNull().default(0),
  effect: doublePrecision("effect"),
  standardError: doublePrecision("standard_error"),
  ciLow: doublePrecision("ci_low"),
  ciHigh: doublePrecision("ci_high"),
  pValue: doublePrecision("p_value"),
  qValue: doublePrecision("q_value"),
  roi: doublePrecision("roi"),
  avgClv: doublePrecision("avg_clv"),
  calibrationJson: jsonb("calibration_json").notNull().default({}),
  dataVersion: text("data_version").notNull(),
  codeVersion: text("code_version").notNull(),
  evaluatedAt: timestamp("evaluated_at", { withTimezone: true }).notNull().defaultNow(),
  resultPayloadHash: text("result_payload_hash").notNull(),
});

export const cfbGameSignalSnapshots = pgTable(
  "cfb_game_signal_snapshots",
  {
    id: bigserial("id", { mode: "number" }).primaryKey(),
    gameId: integer("game_id").notNull().references(() => cfbMatchups.id, { onDelete: "cascade" }),
    teamId: integer("team_id").references(() => cfbTeams.teamId),
    hypothesisId: bigint("hypothesis_id", { mode: "number" }).notNull().references(() => cfbHypotheses.id, { onDelete: "cascade" }),
    signalStatus: text("signal_status").notNull(),
    signalValue: doublePrecision("signal_value"),
    confidence: doublePrecision("confidence"),
    evidenceLevel: text("evidence_level").notNull(),
    inputsJson: jsonb("inputs_json").notNull(),
    modelVersion: text("model_version").notNull(),
    capturedAt: timestamp("captured_at", { withTimezone: true }).notNull().defaultNow(),
    qualifiedForTracking: boolean("qualified_for_tracking").notNull().default(false),
  },
  (t) => [unique("cfb_game_signal_snapshots_identity_key").on(t.gameId, t.teamId, t.hypothesisId, t.modelVersion)],
);

// Fantasy Football draft assistant
export const ffSourceSnapshots = pgTable("ff_source_snapshots", {
  id: bigserial("id", { mode: "number" }).primaryKey(),
  source: text("source").notNull(),
  dataset: text("dataset").notNull(),
  season: integer("season").notNull(),
  scoring: text("scoring"),
  rankingType: text("ranking_type"),
  week: integer("week"),
  contractKey: text("contract_key"),
  requestParams: jsonb("request_params").notNull().default({}),
  sourceUpdatedAt: timestamp("source_updated_at", { withTimezone: true }),
  sourcePublishedAt: timestamp("source_published_at", { withTimezone: true }),
  fetchedAt: timestamp("fetched_at", { withTimezone: true }).notNull().defaultNow(),
  availableAt: timestamp("available_at", { withTimezone: true }),
  asOfAt: timestamp("as_of_at", { withTimezone: true }),
  responseHash: text("response_hash").notNull(),
  rowCount: integer("row_count").notNull(),
  matchedCount: integer("matched_count").notNull().default(0),
  unmatchedCount: integer("unmatched_count").notNull().default(0),
  missingness: jsonb("missingness").notNull().default({}),
  fallbackTier: text("fallback_tier"),
  confidenceMultiplier: doublePrecision("confidence_multiplier").notNull().default(1),
  modelEligible: boolean("model_eligible").notNull().default(true),
  eligibilityReason: text("eligibility_reason"),
  status: text("status").notNull(),
  errorSummary: text("error_summary"),
});

export const ffV2ContextRuns = pgTable("ff_v2_context_runs", {
  runId: uuid("run_id").primaryKey(),
  transformVersion: text("transform_version").notNull(),
  seasons: jsonb("seasons").notNull(),
  sourceSnapshotIds: jsonb("source_snapshot_ids").notNull(),
  coverageReport: jsonb("coverage_report").notNull(),
  artifactDigest: text("artifact_digest").notNull(),
  createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
});

export const ffV2TeamWeekContext = pgTable("ff_v2_team_week_context", {
  id: bigserial("id", { mode: "number" }).primaryKey(),
  runId: uuid("run_id").notNull().references(() => ffV2ContextRuns.runId, { onDelete: "cascade" }),
  season: integer("season").notNull(),
  week: integer("week").notNull(),
  team: text("team").notNull(),
  isBye: boolean("is_bye").notNull(),
  gameId: text("game_id"),
  gameDate: date("game_date"),
  kickoffAt: timestamp("kickoff_at", { withTimezone: true }),
  opponent: text("opponent"),
  isHome: boolean("is_home"),
  location: text("location"),
  stadium: text("stadium"),
  stadiumId: text("stadium_id"),
  roof: text("roof"),
  surface: text("surface"),
  quarterbackGsisId: text("quarterback_gsis_id"),
  quarterbackName: text("quarterback_name"),
  headCoach: text("head_coach"),
  playCallerId: text("play_caller_id"),
  sourceSnapshotId: bigint("source_snapshot_id", { mode: "number" }).notNull().references(() => ffSourceSnapshots.id),
  rowDigest: text("row_digest").notNull(),
  observedAt: timestamp("observed_at", { withTimezone: true }).notNull(),
});

export const ffV2RosterWeeks = pgTable("ff_v2_roster_weeks", {
  id: bigserial("id", { mode: "number" }).primaryKey(),
  runId: uuid("run_id").notNull().references(() => ffV2ContextRuns.runId, { onDelete: "cascade" }),
  season: integer("season").notNull(),
  week: integer("week").notNull(),
  playerGsisId: text("player_gsis_id").notNull(),
  playerName: text("player_name").notNull(),
  position: text("position"),
  depthChartPosition: text("depth_chart_position"),
  team: text("team").notNull(),
  rosterStatus: text("roster_status"),
  resolutionMethod: text("resolution_method").notNull(),
  effectiveAt: timestamp("effective_at", { withTimezone: true }).notNull(),
  sourceSnapshotId: bigint("source_snapshot_id", { mode: "number" }).notNull().references(() => ffSourceSnapshots.id),
  rowDigest: text("row_digest").notNull(),
  observedAt: timestamp("observed_at", { withTimezone: true }).notNull(),
});

export const ffV2Transactions = pgTable("ff_v2_transactions", {
  id: bigserial("id", { mode: "number" }).primaryKey(),
  runId: uuid("run_id").notNull().references(() => ffV2ContextRuns.runId, { onDelete: "cascade" }),
  playerGsisId: text("player_gsis_id").notNull(),
  playerName: text("player_name").notNull(),
  fromTeam: text("from_team"),
  toTeam: text("to_team").notNull(),
  effectiveAt: timestamp("effective_at", { withTimezone: true }).notNull(),
  transactionType: text("transaction_type").notNull(),
  sourceSnapshotId: bigint("source_snapshot_id", { mode: "number" }).notNull().references(() => ffSourceSnapshots.id),
  evidence: jsonb("evidence").notNull().default({}),
  rowDigest: text("row_digest").notNull(),
  observedAt: timestamp("observed_at", { withTimezone: true }).notNull(),
});

export const ffV2TeamWeekFacts = pgTable("ff_v2_team_week_facts", {
  id: bigserial("id", { mode: "number" }).primaryKey(),
  runId: uuid("run_id").notNull().references(() => ffV2ContextRuns.runId, { onDelete: "cascade" }),
  season: integer("season").notNull(),
  week: integer("week").notNull(),
  gameId: text("game_id").notNull(),
  gameDate: date("game_date").notNull(),
  team: text("team").notNull(),
  opponent: text("opponent").notNull(),
  plays: integer("plays").notNull(),
  drives: integer("drives").notNull(),
  passAttempts: integer("pass_attempts").notNull(),
  dropbacks: integer("dropbacks").notNull(),
  sacks: integer("sacks").notNull(),
  allocatableTargets: integer("allocatable_targets").notNull(),
  rushAttempts: integer("rush_attempts").notNull(),
  rbCarries: integer("rb_carries").notNull(),
  rbTargets: integer("rb_targets").notNull(),
  passTouchdowns: integer("pass_touchdowns").notNull(),
  rushTouchdowns: integer("rush_touchdowns").notNull(),
  redZoneTrips: integer("red_zone_trips").notNull(),
  goalLineCarries: integer("goal_line_carries").notNull(),
  endZoneTargets: integer("end_zone_targets").notNull(),
  neutralPassRate: doublePrecision("neutral_pass_rate"),
  secondsPerPlay: doublePrecision("seconds_per_play"),
  scoreStateFeatures: jsonb("score_state_features").notNull().default({}),
  quarterbackGsisId: text("quarterback_gsis_id"),
  quarterbackName: text("quarterback_name"),
  playCallerId: text("play_caller_id"),
  sourceSnapshotIds: jsonb("source_snapshot_ids").notNull(),
  derivation: jsonb("derivation").notNull(),
  factDigest: text("fact_digest").notNull(),
  observedAt: timestamp("observed_at", { withTimezone: true }).notNull(),
});

export const ffV2TeamOpportunityForecastRuns = pgTable("ff_v2_team_opportunity_forecast_runs", {
  runId: uuid("run_id").primaryKey(),
  contractVersion: text("contract_version").notNull(),
  contextRunId: uuid("context_run_id").notNull().references(() => ffV2ContextRuns.runId),
  modelVersion: text("model_version").notNull(),
  calibrationVersion: text("calibration_version").notNull(),
  asOfAt: timestamp("as_of_at", { withTimezone: true }).notNull(),
  sourceSnapshotIds: jsonb("source_snapshot_ids").notNull(),
  modelConfig: jsonb("model_config").notNull().default({}),
  forecastCount: integer("forecast_count").notNull(),
  artifactDigest: text("artifact_digest").notNull(),
  createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
});

export const ffV2TeamOpportunityForecasts = pgTable("ff_v2_team_opportunity_forecasts", {
  id: bigserial("id", { mode: "number" }).primaryKey(),
  forecastRunId: uuid("forecast_run_id").notNull().references(() => ffV2TeamOpportunityForecastRuns.runId, { onDelete: "cascade" }),
  contextFactId: bigint("context_fact_id", { mode: "number" }).notNull().references(() => ffV2TeamWeekFacts.id),
  contextFactDigest: text("context_fact_digest").notNull(),
  season: integer("season").notNull(),
  week: integer("week").notNull(),
  gameId: text("game_id").notNull(),
  gameDate: date("game_date").notNull(),
  team: text("team").notNull(),
  opponent: text("opponent").notNull(),
  fallbackTier: text("fallback_tier").notNull(),
  confidenceMultiplier: doublePrecision("confidence_multiplier").notNull(),
  sourceSnapshotIds: jsonb("source_snapshot_ids").notNull(),
  featureProvenance: jsonb("feature_provenance").notNull(),
  forecastDigest: text("forecast_digest").notNull(),
  asOfAt: timestamp("as_of_at", { withTimezone: true }).notNull(),
  createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
});

export const ffV2TeamOpportunityDistributions = pgTable("ff_v2_team_opportunity_distributions", {
  id: bigserial("id", { mode: "number" }).primaryKey(),
  forecastId: bigint("forecast_id", { mode: "number" }).notNull().references(() => ffV2TeamOpportunityForecasts.id, { onDelete: "cascade" }),
  opportunityType: text("opportunity_type").notNull(),
  expectedValue: doublePrecision("expected_value").notNull(),
  dispersion: doublePrecision("dispersion").notNull(),
  p10: doublePrecision("p10").notNull(),
  p50: doublePrecision("p50").notNull(),
  p90: doublePrecision("p90").notNull(),
  distributionFamily: text("distribution_family").notNull(),
  parameters: jsonb("parameters").notNull().default({}),
  distributionDigest: text("distribution_digest").notNull(),
});

export const ffV2BacktestRuns = pgTable("ff_v2_backtest_runs", {
  runId: uuid("run_id").primaryKey(),
  harnessVersion: text("harness_version").notNull(),
  status: text("status").notNull(),
  contextRunId: uuid("context_run_id").notNull().references(() => ffV2ContextRuns.runId),
  modelVersion: text("model_version").notNull(),
  calibrationVersion: text("calibration_version").notNull(),
  seed: bigint("seed", { mode: "number" }).notNull(),
  evaluationSeasons: jsonb("evaluation_seasons").notNull(),
  preseasonCutoffs: jsonb("preseason_cutoffs").notNull(),
  sourceSnapshotIds: jsonb("source_snapshot_ids").notNull(),
  cohortCounts: jsonb("cohort_counts").notNull(),
  config: jsonb("config").notNull(),
  outputDigest: text("output_digest").notNull(),
  artifactPath: text("artifact_path").notNull(),
  createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
});

export const ffV2BacktestSplits = pgTable("ff_v2_backtest_splits", {
  id: bigserial("id", { mode: "number" }).primaryKey(),
  runId: uuid("run_id").notNull().references(() => ffV2BacktestRuns.runId, { onDelete: "cascade" }),
  evaluationSeason: integer("evaluation_season").notNull(),
  preseasonCutoff: timestamp("preseason_cutoff", { withTimezone: true }).notNull(),
  trainingSeasons: jsonb("training_seasons").notNull(),
  trainingRowCounts: jsonb("training_row_counts").notNull(),
  evaluationRowCounts: jsonb("evaluation_row_counts").notNull(),
  trainingDigest: text("training_digest").notNull(),
  evaluationDigest: text("evaluation_digest").notNull(),
  splitDigest: text("split_digest").notNull(),
  scorable: boolean("scorable").notNull(),
  exclusionReason: text("exclusion_reason"),
});

export const nflDfsProjectionRuns = pgTable(
  "nfl_dfs_projection_runs",
  {
    runId: uuid("run_id").primaryKey(),
    modelVersion: text("model_version").notNull(),
    scoring: text("scoring").notNull().default("DK"),
    slateDate: date("slate_date"),
    season: integer("season").notNull(),
    week: integer("week"),
    asOfAt: timestamp("as_of_at", { withTimezone: true }).notNull(),
    seed: bigint("seed", { mode: "number" }).notNull(),
    historyCutoffSeason: integer("history_cutoff_season").notNull(),
    historyCutoffWeek: integer("history_cutoff_week"),
    sourceSnapshotIds: jsonb("source_snapshot_ids").notNull().default([]),
    modelConfig: jsonb("model_config").notNull().default({}),
    playerCount: integer("player_count").notNull(),
    artifactDigest: text("artifact_digest").notNull(),
    createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
  },
  (t) => [
    unique("nfl_dfs_projection_runs_artifact_key").on(t.modelVersion, t.artifactDigest),
    index("idx_nfl_dfs_projection_runs_slate").on(t.season, t.week, t.asOfAt),
  ],
);

export const nflDfsPlayerProjections = pgTable(
  "nfl_dfs_player_projections",
  {
    id: bigserial("id", { mode: "number" }).primaryKey(),
    runId: uuid("run_id").notNull().references(() => nflDfsProjectionRuns.runId, { onDelete: "cascade" }),
    dkPlayerId: bigint("dk_player_id", { mode: "number" }),
    // Python DDL owns the FK to ff_players. Keeping this as a scalar here
    // avoids a forward-reference during module initialization (ffPlayers is
    // declared below the V2 run tables in this file).
    playerId: bigint("player_id", { mode: "number" }),
    playerGsisId: text("player_gsis_id"),
    playerName: text("player_name").notNull(),
    normalizedName: text("normalized_name").notNull(),
    team: text("team"),
    opponent: text("opponent"),
    position: text("position").notNull(),
    salary: integer("salary"),
    identityMethod: text("identity_method").notNull(),
    projectionStatus: text("projection_status").notNull(),
    historyGames: integer("history_games").notNull(),
    priorGames: integer("prior_games").notNull(),
    modelProjFpts: doublePrecision("model_proj_fpts"),
    baselineFpts: doublePrecision("baseline_fpts"),
    floorFpts: doublePrecision("floor_fpts"),
    medianFpts: doublePrecision("median_fpts"),
    ceilingFpts: doublePrecision("ceiling_fpts"),
    boomRate: doublePrecision("boom_rate"),
    confidence: doublePrecision("confidence").notNull(),
    statMeans: jsonb("stat_means").notNull().default({}),
    featureSnapshot: jsonb("feature_snapshot").notNull().default({}),
    sourceEvidence: jsonb("source_evidence").notNull().default({}),
    createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
  },
  (t) => [
    unique("nfl_dfs_player_projections_run_player_key").on(t.runId, t.playerId),
    index("idx_nfl_dfs_player_projections_run").on(t.runId, t.position),
    index("idx_nfl_dfs_player_projections_player").on(t.playerId, t.createdAt),
  ],
);

export const nflDfsSlateUploads = pgTable(
  "nfl_dfs_slate_uploads",
  {
    uploadId: uuid("upload_id").primaryKey(),
    slateSignature: text("slate_signature").notNull(),
    fileName: text("file_name").notNull(),
    fileDigest: text("file_digest").notNull(),
    format: text("format").notNull(),
    games: jsonb("games").notNull().default([]),
    teams: jsonb("teams").notNull().default([]),
    warnings: jsonb("warnings").notNull().default([]),
    playerCount: integer("player_count").notNull(),
    projectionRunId: uuid("projection_run_id").references(() => nflDfsProjectionRuns.runId),
    createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
  },
  (t) => [
    unique("nfl_dfs_slate_uploads_file_projection_key").on(t.fileDigest, t.projectionRunId),
    index("idx_nfl_dfs_slate_uploads_created").on(t.createdAt),
  ],
);

export const nflDfsSlatePlayers = pgTable(
  "nfl_dfs_slate_players",
  {
    id: bigserial("id", { mode: "number" }).primaryKey(),
    uploadId: uuid("upload_id").notNull().references(() => nflDfsSlateUploads.uploadId, { onDelete: "cascade" }),
    dkPlayerId: bigint("dk_player_id", { mode: "number" }).notNull(),
    captainDkPlayerId: bigint("captain_dk_player_id", { mode: "number" }),
    ffPlayerId: bigint("ff_player_id", { mode: "number" }),
    name: text("name").notNull(),
    normalizedName: text("normalized_name").notNull(),
    position: text("position").notNull(),
    rosterPositions: jsonb("roster_positions").notNull(),
    team: text("team").notNull(),
    opponent: text("opponent"),
    gameKey: text("game_key"),
    gameInfo: text("game_info"),
    salary: integer("salary").notNull(),
    captainSalary: integer("captain_salary"),
    avgFptsDk: doublePrecision("avg_fpts_dk"),
    dkStatus: text("dk_status"),
    isOut: boolean("is_out").notNull().default(false),
    identityMethod: text("identity_method").notNull(),
    projectionStatus: text("projection_status").notNull(),
    ourProj: doublePrecision("our_proj"),
    floorFpts: doublePrecision("floor_fpts"),
    medianFpts: doublePrecision("median_fpts"),
    ceilingFpts: doublePrecision("ceiling_fpts"),
    boomRate: doublePrecision("boom_rate"),
    modelConfidence: doublePrecision("model_confidence"),
    historyGames: integer("history_games"),
    fantasyprosProj: doublePrecision("fantasypros_proj"),
    linestarProj: doublePrecision("linestar_proj"),
    linestarOwnPct: doublePrecision("linestar_own_pct"),
    customProj: doublePrecision("custom_proj"),
    comparisonEvidence: jsonb("comparison_evidence").notNull().default({}),
    createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
    updatedAt: timestamp("updated_at", { withTimezone: true }).notNull().defaultNow(),
  },
  (t) => [
    unique("nfl_dfs_slate_players_upload_player_key").on(t.uploadId, t.dkPlayerId),
    index("idx_nfl_dfs_slate_players_upload").on(t.uploadId, t.position),
  ],
);

export const nflDfsOptimizerRuns = pgTable(
  "nfl_dfs_optimizer_runs",
  {
    runId: uuid("run_id").primaryKey(),
    uploadId: uuid("upload_id").notNull().references(() => nflDfsSlateUploads.uploadId, { onDelete: "cascade" }),
    projectionRunId: uuid("projection_run_id").references(() => nflDfsProjectionRuns.runId),
    optimizerVersion: text("optimizer_version").notNull(),
    mode: text("mode").notNull(),
    projectionSource: text("projection_source").notNull(),
    settings: jsonb("settings").notNull(),
    inputSnapshot: jsonb("input_snapshot").notNull(),
    inputDigest: text("input_digest").notNull(),
    requestedLineups: integer("requested_lineups").notNull(),
    generatedLineups: integer("generated_lineups").notNull(),
    status: text("status").notNull(),
    failureReason: text("failure_reason"),
    createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
  },
  (t) => [index("idx_nfl_dfs_optimizer_runs_upload").on(t.uploadId, t.createdAt)],
);

export const nflDfsLineups = pgTable(
  "nfl_dfs_lineups",
  {
    id: bigserial("id", { mode: "number" }).primaryKey(),
    runId: uuid("run_id").notNull().references(() => nflDfsOptimizerRuns.runId, { onDelete: "cascade" }),
    lineupNumber: integer("lineup_number").notNull(),
    slots: jsonb("slots").notNull(),
    playerIds: jsonb("player_ids").notNull(),
    totalSalary: integer("total_salary").notNull(),
    projectedFpts: doublePrecision("projected_fpts").notNull(),
    floorFpts: doublePrecision("floor_fpts"),
    ceilingFpts: doublePrecision("ceiling_fpts"),
    projectedOwnership: doublePrecision("projected_ownership"),
    stackSummary: jsonb("stack_summary").notNull().default({}),
    actualFpts: doublePrecision("actual_fpts"),
    createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
  },
  (t) => [
    unique("nfl_dfs_lineups_run_number_key").on(t.runId, t.lineupNumber),
    index("idx_nfl_dfs_lineups_run").on(t.runId, t.lineupNumber),
  ],
);

export const ffPlayers = pgTable(
  "ff_players",
  {
    id: bigserial("id", { mode: "number" }).primaryKey(),
    season: integer("season").notNull(),
    canonicalName: text("canonical_name").notNull(),
    normalizedName: text("normalized_name").notNull(),
    position: text("position").notNull(),
    nflTeamId: integer("nfl_team_id").references(() => nflTeams.teamId),
    teamAbbrev: text("team_abbrev"),
    fantasyprosPlayerId: integer("fantasypros_player_id"),
    sleeperPlayerId: text("sleeper_player_id"),
    gsisId: text("gsis_id"),
    espnId: text("espn_id"),
    yahooId: text("yahoo_id"),
    mflId: text("mfl_id"),
    draftkingsId: text("draftkings_id"),
    active: boolean("active").notNull().default(true),
    rookie: boolean("rookie").notNull().default(false),
    byeWeek: integer("bye_week"),
    injuryStatus: text("injury_status"),
    metadata: jsonb("metadata").notNull().default({}),
    fetchedAt: timestamp("fetched_at", { withTimezone: true }).notNull().defaultNow(),
  },
  (t) => [unique("ff_players_season_fp_key").on(t.season, t.fantasyprosPlayerId)],
);

export const ffPlayerInjuryObservations = pgTable(
  "ff_player_injury_observations",
  {
    id: bigserial("id", { mode: "number" }).primaryKey(),
    playerId: bigint("player_id", { mode: "number" }).notNull().references(() => ffPlayers.id, { onDelete: "cascade" }),
    season: integer("season").notNull(),
    source: text("source").notNull(),
    sourceSnapshotId: bigint("source_snapshot_id", { mode: "number" }).references(() => ffSourceSnapshots.id, { onDelete: "set null" }),
    sourceStatus: text("source_status"),
    normalizedStatus: text("normalized_status").notNull(),
    bodyPart: text("body_part"),
    injuryType: text("injury_type"),
    description: text("description"),
    practiceStatus: text("practice_status"),
    injuryStartedAt: timestamp("injury_started_at", { withTimezone: true }),
    providerUpdatedAt: timestamp("provider_updated_at", { withTimezone: true }),
    expectedReturnMin: date("expected_return_min"),
    expectedReturnMax: date("expected_return_max"),
    weeksOutMin: doublePrecision("weeks_out_min"),
    weeksOutMax: doublePrecision("weeks_out_max"),
    availabilityProbability: doublePrecision("availability_probability"),
    rawPayload: jsonb("raw_payload").notNull().default({}),
    responseHash: text("response_hash").notNull(),
    observedAt: timestamp("observed_at", { withTimezone: true }).notNull().defaultNow(),
  },
  (t) => [
    unique("ff_injury_observation_snapshot_player_source_key").on(t.sourceSnapshotId, t.playerId, t.source),
    index("idx_ff_injury_observations_player").on(t.playerId, t.observedAt),
  ],
);

export const ffPlayerInjuries = pgTable(
  "ff_player_injuries",
  {
    id: bigserial("id", { mode: "number" }).primaryKey(),
    playerId: bigint("player_id", { mode: "number" }).notNull().references(() => ffPlayers.id, { onDelete: "cascade" }),
    season: integer("season").notNull(),
    status: text("status").notNull(),
    bodyPart: text("body_part"),
    injuryType: text("injury_type"),
    firstSeenAt: timestamp("first_seen_at", { withTimezone: true }).notNull(),
    lastConfirmedAt: timestamp("last_confirmed_at", { withTimezone: true }).notNull(),
    clearedAt: timestamp("cleared_at", { withTimezone: true }),
    expectedReturnMin: date("expected_return_min"),
    expectedReturnMax: date("expected_return_max"),
    weeksOutMin: doublePrecision("weeks_out_min"),
    weeksOutMax: doublePrecision("weeks_out_max"),
    estimateBasis: text("estimate_basis").notNull().default("unknown"),
    confidence: doublePrecision("confidence"),
    primarySource: text("primary_source").notNull(),
    sourceConflict: boolean("source_conflict").notNull().default(false),
    active: boolean("active").notNull().default(true),
  },
  (t) => [index("idx_ff_player_injuries_active").on(t.season, t.active, t.status)],
);

export const ffInjuryEvents = pgTable(
  "ff_injury_events",
  {
    id: bigserial("id", { mode: "number" }).primaryKey(),
    injuryId: bigint("injury_id", { mode: "number" }).references(() => ffPlayerInjuries.id, { onDelete: "cascade" }),
    playerId: bigint("player_id", { mode: "number" }).notNull().references(() => ffPlayers.id, { onDelete: "cascade" }),
    observationId: bigint("observation_id", { mode: "number" }).references(() => ffPlayerInjuryObservations.id, { onDelete: "set null" }),
    eventType: text("event_type").notNull(),
    previousState: jsonb("previous_state").notNull().default({}),
    newState: jsonb("new_state").notNull().default({}),
    source: text("source").notNull(),
    eventKey: text("event_key").notNull().unique(),
    occurredAt: timestamp("occurred_at", { withTimezone: true }).notNull().defaultNow(),
  },
  (t) => [index("idx_ff_injury_events_recent").on(t.playerId, t.occurredAt)],
);

export const ffRankingSets = pgTable("ff_ranking_sets", {
  id: bigserial("id", { mode: "number" }).primaryKey(),
  season: integer("season").notNull(),
  name: text("name").notNull(),
  source: text("source").notNull(),
  sourceSnapshotId: bigint("source_snapshot_id", { mode: "number" }).references(() => ffSourceSnapshots.id),
  sourceDate: date("source_date"),
  scoringProfile: jsonb("scoring_profile").notNull(),
  rankingType: text("ranking_type").notNull().default("DRAFT"),
  isBaseline: boolean("is_baseline").notNull().default(false),
  importSummary: jsonb("import_summary"),
  createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
});

export const ffPlayerRankings = pgTable(
  "ff_player_rankings",
  {
    id: bigserial("id", { mode: "number" }).primaryKey(),
    rankingSetId: bigint("ranking_set_id", { mode: "number" }).notNull().references(() => ffRankingSets.id),
    playerId: bigint("player_id", { mode: "number" }).notNull().references(() => ffPlayers.id),
    overallRank: integer("overall_rank"),
    positionRank: integer("position_rank"),
    tier: integer("tier"),
    adp: doublePrecision("adp"),
    projectedPoints: doublePrecision("projected_points"),
    projectionLow: doublePrecision("projection_low"),
    projectionHigh: doublePrecision("projection_high"),
    projectedStats: jsonb("projected_stats"),
    rankMin: doublePrecision("rank_min"),
    rankMax: doublePrecision("rank_max"),
    rankStd: doublePrecision("rank_std"),
    ourRank: integer("our_rank"),
    ourProjectedPoints: doublePrecision("our_projected_points"),
    expectedGames: doublePrecision("expected_games"),
    confidence: doublePrecision("confidence"),
    sourceRow: jsonb("source_row"),
    notes: text("notes"),
  },
  (t) => [unique("ff_player_rankings_set_player_key").on(t.rankingSetId, t.playerId)],
);

export const ffPlayerSeasonFeatures = pgTable(
  "ff_player_season_features",
  {
    id: bigserial("id", { mode: "number" }).primaryKey(),
    playerId: bigint("player_id", { mode: "number" }).notNull().references(() => ffPlayers.id),
    season: integer("season").notNull(),
    source: text("source").notNull(),
    games: integer("games"),
    fantasyPointsStd: doublePrecision("fantasy_points_std"),
    fantasyPointsPpr: doublePrecision("fantasy_points_ppr"),
    targets: doublePrecision("targets"),
    receptions: doublePrecision("receptions"),
    receivingYards: doublePrecision("receiving_yards"),
    receivingTds: doublePrecision("receiving_tds"),
    carries: doublePrecision("carries"),
    rushingYards: doublePrecision("rushing_yards"),
    rushingTds: doublePrecision("rushing_tds"),
    targetShare: doublePrecision("target_share"),
    rushShare: doublePrecision("rush_share"),
    teamTargetRank: integer("team_target_rank"),
    teamRushRank: integer("team_rush_rank"),
    nflTargetRank: integer("nfl_target_rank"),
    nflRushTdRank: integer("nfl_rush_td_rank"),
    sourceRow: jsonb("source_row").notNull().default({}),
    fetchedAt: timestamp("fetched_at", { withTimezone: true }).notNull().defaultNow(),
  },
  (t) => [unique("ff_player_features_player_season_source_key").on(t.playerId, t.season, t.source)],
);

// Python-owned (ingest/ff_teammate_correlation.py) -- read-only from the web app.
export const ffTeammateCorrelations = pgTable(
  "ff_teammate_correlations",
  {
    id: bigserial("id", { mode: "number" }).primaryKey(),
    season: integer("season").notNull(),
    playerAId: bigint("player_a_id", { mode: "number" }).notNull().references(() => ffPlayers.id),
    playerBId: bigint("player_b_id", { mode: "number" }).notNull().references(() => ffPlayers.id),
    teamAbbrev: text("team_abbrev").notNull(),
    relationshipType: text("relationship_type").notNull(),
    sampleWeeks: integer("sample_weeks").notNull(),
    rawCorrelation: doublePrecision("raw_correlation"),
    priorCorrelation: doublePrecision("prior_correlation").notNull(),
    shrunkCorrelation: doublePrecision("shrunk_correlation").notNull(),
    shrinkageWeight: doublePrecision("shrinkage_weight").notNull(),
    computedAt: timestamp("computed_at", { withTimezone: true }).notNull().defaultNow(),
  },
  (t) => [unique("ff_teammate_correlations_season_pair_key").on(t.season, t.playerAId, t.playerBId)],
);

export const ffPlayerIndicators = pgTable(
  "ff_player_indicators",
  {
    id: bigserial("id", { mode: "number" }).primaryKey(),
    rankingSetId: bigint("ranking_set_id", { mode: "number" }).notNull().references(() => ffRankingSets.id),
    playerId: bigint("player_id", { mode: "number" }).notNull().references(() => ffPlayers.id),
    indicatorCode: text("indicator_code").notNull(),
    indicatorClass: text("indicator_class").notNull(),
    label: text("label").notNull(),
    metricValue: doublePrecision("metric_value"),
    leagueRank: integer("league_rank"),
    percentile: doublePrecision("percentile"),
    confidence: doublePrecision("confidence"),
    season: integer("season"),
    relatedPlayerId: bigint("related_player_id", { mode: "number" }).references(() => ffPlayers.id),
    evidence: jsonb("evidence").notNull().default({}),
    createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
  },
  (t) => [unique("ff_player_indicators_set_player_code_key").on(t.rankingSetId, t.playerId, t.indicatorCode)],
);

export const ffDraftSessions = pgTable("ff_draft_sessions", {
  id: uuid("id").primaryKey(),
  ownerKey: text("owner_key"),
  name: text("name").notNull(),
  season: integer("season").notNull(),
  status: text("status").notNull().default("ready"),
  draftType: text("draft_type").notNull().default("snake"),
  teamCount: integer("team_count").notNull(),
  controlledSlot: integer("controlled_slot").notNull(),
  roundCount: integer("round_count").notNull(),
  rosterConfig: jsonb("roster_config").notNull(),
  scoringConfig: jsonb("scoring_config").notNull(),
  recommendationConfig: jsonb("recommendation_config").notNull().default({}),
  rankingSetId: bigint("ranking_set_id", { mode: "number" }).notNull().references(() => ffRankingSets.id),
  sleeperDraftId: text("sleeper_draft_id"),
  currentPick: integer("current_pick").notNull().default(1),
  revision: integer("revision").notNull().default(0),
  startedAt: timestamp("started_at", { withTimezone: true }),
  completedAt: timestamp("completed_at", { withTimezone: true }),
  createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
  updatedAt: timestamp("updated_at", { withTimezone: true }).notNull().defaultNow(),
});

export const ffDraftTeams = pgTable(
  "ff_draft_teams",
  {
    id: bigserial("id", { mode: "number" }).primaryKey(),
    draftId: uuid("draft_id").notNull().references(() => ffDraftSessions.id),
    slot: integer("slot").notNull(),
    name: text("name").notNull(),
    isControlled: boolean("is_controlled").notNull().default(false),
    externalRosterId: text("external_roster_id"),
  },
  (t) => [unique("ff_draft_teams_draft_slot_key").on(t.draftId, t.slot)],
);

export const ffDraftSlots = pgTable(
  "ff_draft_slots",
  {
    id: bigserial("id", { mode: "number" }).primaryKey(),
    draftId: uuid("draft_id").notNull().references(() => ffDraftSessions.id),
    overallPick: integer("overall_pick").notNull(),
    round: integer("round").notNull(),
    pickInRound: integer("pick_in_round").notNull(),
    draftTeamId: bigint("draft_team_id", { mode: "number" }).notNull().references(() => ffDraftTeams.id),
  },
  (t) => [unique("ff_draft_slots_draft_pick_key").on(t.draftId, t.overallPick)],
);

export const ffDraftEvents = pgTable("ff_draft_events", {
  id: bigserial("id", { mode: "number" }).primaryKey(),
  draftId: uuid("draft_id").notNull().references(() => ffDraftSessions.id),
  eventType: text("event_type").notNull(),
  overallPick: integer("overall_pick"),
  playerId: bigint("player_id", { mode: "number" }).references(() => ffPlayers.id),
  draftTeamId: bigint("draft_team_id", { mode: "number" }).references(() => ffDraftTeams.id),
  source: text("source").notNull(),
  externalPickId: text("external_pick_id"),
  reversesEventId: bigint("reverses_event_id", { mode: "number" }),
  payload: jsonb("payload").notNull().default({}),
  createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
});

export const ffDraftPlayerPreferences = pgTable("ff_draft_player_preferences", {
  draftId: uuid("draft_id").notNull().references(() => ffDraftSessions.id),
  playerId: bigint("player_id", { mode: "number" }).notNull().references(() => ffPlayers.id),
  preference: text("preference").notNull(),
  note: text("note"),
  createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
});

export const mlbParkFactors = pgTable(
  "mlb_park_factors",
  {
    id: serial("id").primaryKey(),
    teamId: integer("team_id")
      .notNull()
      .references(() => mlbTeams.teamId),
    season: text("season").notNull(),
    runsFactor: doublePrecision("runs_factor").default(1.0),
    hrFactor: doublePrecision("hr_factor").default(1.0),
  },
  (t) => [unique("mlb_park_factors_team_season_key").on(t.teamId, t.season)]
);

export const mlbMatchups = pgTable(
  "mlb_matchups",
  {
    id: serial("id").primaryKey(),
    gameDate: date("game_date").notNull(),
    gameId: text("game_id").unique(),
    homeTeamId: integer("home_team_id").references(() => mlbTeams.teamId),
    awayTeamId: integer("away_team_id").references(() => mlbTeams.teamId),
    homeSpId: integer("home_sp_id"),
    homeSpName: text("home_sp_name"),
    awaySpId: integer("away_sp_id"),
    awaySpName: text("away_sp_name"),
    vegasTotal: doublePrecision("vegas_total"),
    homeMl: integer("home_ml"),
    awayMl: integer("away_ml"),
    homeSpread: doublePrecision("home_spread"),
    vegasProbHome: doublePrecision("vegas_prob_home"),
    homeImplied: doublePrecision("home_implied"),
    awayImplied: doublePrecision("away_implied"),
    homeScore: integer("home_score"),
    awayScore: integer("away_score"),
    ballpark: text("ballpark"),
    weatherTemp: integer("weather_temp"),
    windSpeed: integer("wind_speed"),
    windDirection: text("wind_direction"),
    ourTotalPred: doublePrecision("our_total_pred"),
    ourProbHome: doublePrecision("our_prob_home"),
    commenceTime: timestamp("commence_time", { withTimezone: true }),
    fetchedAt: timestamp("fetched_at").defaultNow(),
  },
  (t) => [
    unique("mlb_matchups_date_teams_key").on(t.gameDate, t.homeTeamId, t.awayTeamId),
    index("idx_mlb_matchups_date").on(t.gameDate),
  ]
);

export const mlbBatterStats = pgTable(
  "mlb_batter_stats",
  {
    id: serial("id").primaryKey(),
    playerId: integer("player_id").notNull(),
    season: text("season").notNull(),
    teamId: integer("team_id").references(() => mlbTeams.teamId),
    name: text("name").notNull(),
    battingOrder: integer("batting_order"),
    games: integer("games"),
    paPg: doublePrecision("pa_pg"),
    avg: doublePrecision("avg"),
    obp: doublePrecision("obp"),
    slg: doublePrecision("slg"),
    iso: doublePrecision("iso"),
    babip: doublePrecision("babip"),
    wrcPlus: doublePrecision("wrc_plus"),
    kPct: doublePrecision("k_pct"),
    bbPct: doublePrecision("bb_pct"),
    hrPg: doublePrecision("hr_pg"),
    singlesPg: doublePrecision("singles_pg"),
    doublesPg: doublePrecision("doubles_pg"),
    triplesPg: doublePrecision("triples_pg"),
    rbiPg: doublePrecision("rbi_pg"),
    runsPg: doublePrecision("runs_pg"),
    sbPg: doublePrecision("sb_pg"),
    hbpPg: doublePrecision("hbp_pg"),
    wrcPlusVsL: doublePrecision("wrc_plus_vs_l"),
    wrcPlusVsR: doublePrecision("wrc_plus_vs_r"),
    avgFptsPg: doublePrecision("avg_fpts_pg"),
    fptsStd: doublePrecision("fpts_std"),
    fetchedAt: timestamp("fetched_at").defaultNow(),
  },
  (t) => [
    unique("mlb_batter_stats_player_season_key").on(t.playerId, t.season),
    index("idx_mlb_batter_stats_team").on(t.teamId, t.season),
  ]
);

export const mlbPitcherStats = pgTable(
  "mlb_pitcher_stats",
  {
    id: serial("id").primaryKey(),
    playerId: integer("player_id").notNull(),
    season: text("season").notNull(),
    teamId: integer("team_id").references(() => mlbTeams.teamId),
    name: text("name").notNull(),
    hand: text("hand"),
    games: integer("games"),
    ipPg: doublePrecision("ip_pg"),
    era: doublePrecision("era"),
    fip: doublePrecision("fip"),
    xfip: doublePrecision("xfip"),
    kPer9: doublePrecision("k_per_9"),
    bbPer9: doublePrecision("bb_per_9"),
    hrPer9: doublePrecision("hr_per_9"),
    kPct: doublePrecision("k_pct"),
    bbPct: doublePrecision("bb_pct"),
    hrFbPct: doublePrecision("hr_fb_pct"),
    whip: doublePrecision("whip"),
    avgFptsPg: doublePrecision("avg_fpts_pg"),
    fptsStd: doublePrecision("fpts_std"),
    winPct: doublePrecision("win_pct"),
    qsPct: doublePrecision("qs_pct"),
    fetchedAt: timestamp("fetched_at").defaultNow(),
  },
  (t) => [
    unique("mlb_pitcher_stats_player_season_key").on(t.playerId, t.season),
    index("idx_mlb_pitcher_stats_team").on(t.teamId, t.season),
  ]
);

export const mlbTeamStats = pgTable(
  "mlb_team_stats",
  {
    id: serial("id").primaryKey(),
    teamId: integer("team_id")
      .notNull()
      .references(() => mlbTeams.teamId),
    season: text("season").notNull(),
    teamWrcPlus: doublePrecision("team_wrc_plus"),
    teamKPct: doublePrecision("team_k_pct"),
    teamBbPct: doublePrecision("team_bb_pct"),
    teamIso: doublePrecision("team_iso"),
    teamOps: doublePrecision("team_ops"),
    bullpenEra: doublePrecision("bullpen_era"),
    bullpenFip: doublePrecision("bullpen_fip"),
    staffKPct: doublePrecision("staff_k_pct"),
    staffBbPct: doublePrecision("staff_bb_pct"),
    fetchedAt: timestamp("fetched_at").defaultNow(),
  },
  (t) => [unique("mlb_team_stats_team_season_key").on(t.teamId, t.season)]
);

// ── Shared DFS tables ─────────────────────────────────────────

export const dkSlates = pgTable(
  "dk_slates",
  {
    id: serial("id").primaryKey(),
    sport: text("sport").default("nba"),
    slateDate: date("slate_date").notNull(),
    gameCount: integer("game_count").default(0),
    dkDraftGroupId: integer("dk_draft_group_id"),
    linestarPeriodId: integer("linestar_period_id"),
    cashLine: doublePrecision("cash_line"),
    contestType: text("contest_type").default("main"),
    fieldSize: integer("field_size"),
    contestFormat: text("contest_format").default("gpp"),
    createdAt: timestamp("created_at").defaultNow(),
  },
  (t) => [
    unique("dk_slates_date_type_format_sport_key").on(
      t.slateDate, t.contestType, t.contestFormat, t.sport
    ),
    index("idx_dk_slates_sport_date").on(t.sport, t.slateDate),
  ]
);

export const dkPlayers = pgTable(
  "dk_players",
  {
    id: serial("id").primaryKey(),
    slateId: integer("slate_id")
      .notNull()
      .references(() => dkSlates.id),
    dkPlayerId: bigint("dk_player_id", { mode: "number" }).notNull(),
    name: text("name").notNull(),
    teamAbbrev: text("team_abbrev").notNull(),
    // team_id: NBA team (NULL for MLB players)
    teamId: integer("team_id").references(() => teams.teamId),
    // mlb_team_id: MLB team (NULL for NBA players)
    mlbTeamId: integer("mlb_team_id").references(() => mlbTeams.teamId),
    // matchup_id is a plain integer — references nba_matchups OR mlb_matchups
    // depending on the parent slate's sport column. No FK enforced here.
    matchupId: integer("matchup_id"),
    eligiblePositions: text("eligible_positions").notNull(),
    salary: integer("salary").notNull(),
    gameInfo: text("game_info"),
    avgFptsDk: real("avg_fpts_dk"),
    linestarProj: real("linestar_proj"),
    linestarOwnPct: real("linestar_own_pct"),
    projOwnPct: real("proj_own_pct"),
    ourProj: real("our_proj"),
    liveProj: real("live_proj"),
    expectedHr: real("expected_hr"),
    hrProb1Plus: real("hr_prob_1plus"),
    ourLeverage: real("our_leverage"),
    ourOwnPct: real("our_own_pct"),
    liveLeverage: real("live_leverage"),
    liveOwnPct: real("live_own_pct"),
    propPts: real("prop_pts"),
    propPtsPrice: integer("prop_pts_price"),
    propPtsBook: text("prop_pts_book"),
    propReb: real("prop_reb"),
    propRebPrice: integer("prop_reb_price"),
    propRebBook: text("prop_reb_book"),
    propAst: real("prop_ast"),
    propAstPrice: integer("prop_ast_price"),
    propAstBook: text("prop_ast_book"),
    propBlk: real("prop_blk"),
    propBlkPrice: integer("prop_blk_price"),
    propBlkBook: text("prop_blk_book"),
    propStl: real("prop_stl"),
    propStlPrice: integer("prop_stl_price"),
    propStlBook: text("prop_stl_book"),
    projFloor: real("proj_floor"),
    projCeiling: real("proj_ceiling"),
    boomRate: real("boom_rate"),
    dkInStartingLineup: boolean("dk_in_starting_lineup"),
    dkStartingLineupOrder: integer("dk_starting_lineup_order"),
    dkTeamLineupConfirmed: boolean("dk_team_lineup_confirmed"),
    dkStatus: text("dk_status"),
    isOut: boolean("is_out").default(false),
    actualFpts: real("actual_fpts"),
    actualOwnPct: real("actual_own_pct"),
    actualHr: integer("actual_hr"),
    actualPts: real("actual_pts"),
    actualReb: real("actual_reb"),
    actualAst: real("actual_ast"),
    actualStl: real("actual_stl"),
    actualBlk: real("actual_blk"),
    actualTov: real("actual_tov"),
    actual3pm: real("actual_3pm"),
  },
  (t) => [
    unique("dk_players_slate_player_key").on(t.slateId, t.dkPlayerId),
    index("idx_dk_players_mlb_team").on(t.mlbTeamId, t.slateId),
  ]
);

export const dkLineups = pgTable(
  "dk_lineups",
  {
    id: serial("id").primaryKey(),
    slateId: integer("slate_id")
      .notNull()
      .references(() => dkSlates.id),
    strategy: text("strategy").notNull(),
    lineupNum: integer("lineup_num").notNull(),
    playerIds: text("player_ids").notNull(),
    totalSalary: integer("total_salary"),
    projFpts: doublePrecision("proj_fpts"),
    leverage: doublePrecision("leverage"),
    stackTeam: text("stack_team"),
    actualFpts: doublePrecision("actual_fpts"),
    createdAt: timestamp("created_at").defaultNow(),
  },
  (t) => [unique("dk_lineups_slate_strategy_num_key").on(t.slateId, t.strategy, t.lineupNum)]
);

export const gameOddsHistory = pgTable(
  "game_odds_history",
  {
    id: serial("id").primaryKey(),
    sport: text("sport").notNull(),
    matchupId: integer("matchup_id").notNull(),
    eventId: text("event_id"),
    gameDate: date("game_date").notNull(),
    homeTeamId: integer("home_team_id"),
    awayTeamId: integer("away_team_id"),
    homeTeamName: text("home_team_name"),
    awayTeamName: text("away_team_name"),
    bookmakerCount: integer("bookmaker_count").notNull().default(0),
    homeMl: integer("home_ml"),
    awayMl: integer("away_ml"),
    homeSpread: doublePrecision("home_spread"),
    vegasTotal: doublePrecision("vegas_total"),
    homeWinProb: doublePrecision("vegas_prob_home"),
    homeImplied: doublePrecision("home_implied"),
    awayImplied: doublePrecision("away_implied"),
    books: jsonb("books"),
    vegasTotalRaw: doublePrecision("vegas_total_raw"),
    drawMl: integer("draw_ml"),
    captureKey: text("capture_key").notNull(),
    capturedAt: timestamp("captured_at").defaultNow().notNull(),
  },
  (t) => [
    unique("game_odds_history_capture_key").on(t.sport, t.matchupId, t.captureKey),
    index("idx_game_odds_history_lookup").on(t.sport, t.gameDate, t.capturedAt),
    index("idx_game_odds_history_matchup").on(t.sport, t.matchupId, t.capturedAt),
  ]
);

export const playerPropHistory = pgTable(
  "player_prop_history",
  {
    id: serial("id").primaryKey(),
    sport: text("sport").notNull(),
    slateId: integer("slate_id").references(() => dkSlates.id),
    dkPlayerId: bigint("dk_player_id", { mode: "number" }).notNull(),
    playerName: text("player_name").notNull(),
    teamId: integer("team_id"),
    eventId: text("event_id"),
    marketKey: text("market_key").notNull(),
    line: doublePrecision("line"),
    price: integer("price"),
    bookmakerKey: text("bookmaker_key"),
    bookmakerTitle: text("bookmaker_title"),
    bookCount: integer("book_count").notNull().default(0),
    captureKey: text("capture_key").notNull(),
    capturedAt: timestamp("captured_at").defaultNow().notNull(),
  },
  (t) => [
    unique("player_prop_history_capture_key").on(t.sport, t.slateId, t.dkPlayerId, t.marketKey, t.captureKey),
    index("idx_player_prop_history_lookup").on(t.sport, t.slateId, t.marketKey, t.capturedAt),
    index("idx_player_prop_history_player").on(t.sport, t.dkPlayerId, t.marketKey, t.capturedAt),
  ]
);

export const projectionRuns = pgTable(
  "projection_runs",
  {
    id: serial("id").primaryKey(),
    sport: text("sport").notNull(),
    slateId: integer("slate_id")
      .notNull()
      .references(() => dkSlates.id),
    modelVersion: text("model_version").notNull(),
    source: text("source").notNull(),
    configJson: jsonb("config_json").notNull().default({}),
    notes: text("notes"),
    createdAt: timestamp("created_at").defaultNow(),
  },
  (t) => [
    index("idx_projection_runs_slate").on(t.slateId, t.createdAt),
    index("idx_projection_runs_model").on(t.modelVersion, t.createdAt),
  ]
);

export const projectionPlayerSnapshots = pgTable(
  "projection_player_snapshots",
  {
    id: serial("id").primaryKey(),
    runId: integer("run_id")
      .notNull()
      .references(() => projectionRuns.id),
    slateId: integer("slate_id")
      .notNull()
      .references(() => dkSlates.id),
    dkPlayerId: bigint("dk_player_id", { mode: "number" }).notNull(),
    name: text("name").notNull(),
    teamId: integer("team_id"),
    salary: integer("salary").notNull(),
    isOut: boolean("is_out").default(false),
    modelProjFpts: real("model_proj_fpts"),
    marketProjFpts: real("market_proj_fpts"),
    linestarProjFpts: real("linestar_proj_fpts"),
    finalProjFpts: real("final_proj_fpts"),
    modelConfidence: real("model_confidence"),
    marketConfidence: real("market_confidence"),
    lsConfidence: real("ls_confidence"),
    modelWeight: real("model_weight"),
    marketWeight: real("market_weight"),
    lsWeight: real("ls_weight"),
    flagsJson: jsonb("flags_json").notNull().default([]),
    modelStatsJson: jsonb("model_stats_json"),
    marketStatsJson: jsonb("market_stats_json"),
    actualFpts: real("actual_fpts"),
    createdAt: timestamp("created_at").defaultNow(),
  },
  (t) => [
    index("idx_projection_snapshots_run").on(t.runId, t.dkPlayerId),
    index("idx_projection_snapshots_slate").on(t.slateId, t.dkPlayerId),
    unique("projection_snapshots_run_player_key").on(t.runId, t.dkPlayerId),
  ]
);

export const ownershipRuns = pgTable(
  "ownership_runs",
  {
    id: serial("id").primaryKey(),
    sport: text("sport").notNull(),
    slateId: integer("slate_id")
      .notNull()
      .references(() => dkSlates.id),
    ownershipVersion: text("ownership_version").notNull(),
    source: text("source").notNull(),
    configJson: jsonb("config_json").notNull().default({}),
    notes: text("notes"),
    createdAt: timestamp("created_at").defaultNow(),
  },
  (t) => [
    index("idx_ownership_runs_slate").on(t.slateId, t.createdAt),
    index("idx_ownership_runs_model").on(t.ownershipVersion, t.createdAt),
  ]
);

export const ownershipPlayerSnapshots = pgTable(
  "ownership_player_snapshots",
  {
    id: serial("id").primaryKey(),
    runId: integer("run_id")
      .notNull()
      .references(() => ownershipRuns.id),
    slateId: integer("slate_id")
      .notNull()
      .references(() => dkSlates.id),
    dkPlayerId: bigint("dk_player_id", { mode: "number" }).notNull(),
    name: text("name").notNull(),
    teamId: integer("team_id"),
    salary: integer("salary").notNull(),
    eligiblePositions: text("eligible_positions"),
    isOut: boolean("is_out").default(false),
    linestarProjFpts: real("linestar_proj_fpts"),
    ourProjFpts: real("our_proj_fpts"),
    liveProjFpts: real("live_proj_fpts"),
    linestarOwnPct: real("linestar_own_pct"),
    fieldOwnPct: real("field_own_pct"),
    ourOwnPct: real("our_own_pct"),
    liveOwnPct: real("live_own_pct"),
    actualOwnPct: real("actual_own_pct"),
    lineupOrder: integer("lineup_order"),
    lineupConfirmed: boolean("lineup_confirmed"),
    createdAt: timestamp("created_at").defaultNow(),
  },
  (t) => [
    index("idx_ownership_snapshots_run").on(t.runId, t.dkPlayerId),
    index("idx_ownership_snapshots_slate").on(t.slateId, t.dkPlayerId),
    unique("ownership_snapshots_run_player_key").on(t.runId, t.dkPlayerId),
  ]
);

export const mlbBlowupRuns = pgTable(
  "mlb_blowup_runs",
  {
    id: serial("id").primaryKey(),
    slateId: integer("slate_id")
      .notNull()
      .references(() => dkSlates.id),
    analysisVersion: text("analysis_version").notNull(),
    source: text("source").notNull(),
    configJson: jsonb("config_json").notNull().default({}),
    notes: text("notes"),
    createdAt: timestamp("created_at").defaultNow(),
  },
  (t) => [
    index("idx_mlb_blowup_runs_slate").on(t.slateId, t.createdAt),
    index("idx_mlb_blowup_runs_model").on(t.analysisVersion, t.createdAt),
  ]
);

export const mlbBlowupPlayerSnapshots = pgTable(
  "mlb_blowup_player_snapshots",
  {
    id: serial("id").primaryKey(),
    runId: integer("run_id")
      .notNull()
      .references(() => mlbBlowupRuns.id),
    slateId: integer("slate_id")
      .notNull()
      .references(() => dkSlates.id),
    dkPlayerId: bigint("dk_player_id", { mode: "number" }).notNull(),
    name: text("name").notNull(),
    teamId: integer("team_id"),
    teamAbbrev: text("team_abbrev"),
    salary: integer("salary").notNull(),
    eligiblePositions: text("eligible_positions"),
    lineupOrder: integer("lineup_order"),
    teamTotal: real("team_total"),
    projectedFpts: real("projected_fpts"),
    projectedCeiling: real("projected_ceiling"),
    projectedValue: real("projected_value"),
    blowupScore: real("blowup_score"),
    candidateRank: integer("candidate_rank"),
    actualFpts: real("actual_fpts"),
    actualOwnPct: real("actual_own_pct"),
    createdAt: timestamp("created_at").defaultNow(),
  },
  (t) => [
    index("idx_mlb_blowup_snapshots_run").on(t.runId, t.dkPlayerId),
    index("idx_mlb_blowup_snapshots_slate").on(t.slateId, t.candidateRank),
    unique("mlb_blowup_snapshots_run_player_key").on(t.runId, t.dkPlayerId),
  ]
);

export const mlbHomerunRuns = pgTable(
  "mlb_homerun_runs",
  {
    id: serial("id").primaryKey(),
    slateId: integer("slate_id")
      .notNull()
      .references(() => dkSlates.id),
    analysisVersion: text("analysis_version").notNull(),
    source: text("source").notNull(),
    configJson: jsonb("config_json").notNull().default({}),
    notes: text("notes"),
    createdAt: timestamp("created_at").defaultNow(),
  },
  (t) => [
    index("idx_mlb_homerun_runs_slate").on(t.slateId, t.createdAt),
    index("idx_mlb_homerun_runs_model").on(t.analysisVersion, t.createdAt),
  ]
);

export const mlbHomerunPlayerSnapshots = pgTable(
  "mlb_homerun_player_snapshots",
  {
    id: serial("id").primaryKey(),
    runId: integer("run_id")
      .notNull()
      .references(() => mlbHomerunRuns.id),
    slateId: integer("slate_id")
      .notNull()
      .references(() => dkSlates.id),
    dkPlayerId: bigint("dk_player_id", { mode: "number" }).notNull(),
    name: text("name").notNull(),
    teamId: integer("team_id"),
    teamAbbrev: text("team_abbrev"),
    salary: integer("salary").notNull(),
    eligiblePositions: text("eligible_positions"),
    isOut: boolean("is_out").default(false),
    lineupOrder: integer("lineup_order"),
    lineupConfirmed: boolean("lineup_confirmed"),
    expectedHr: real("expected_hr"),
    hrProb1Plus: real("hr_prob_1plus"),
    hitterHrPg: real("hitter_hr_pg"),
    hitterIso: real("hitter_iso"),
    hitterSlug: real("hitter_slg"),
    hitterPaPg: real("hitter_pa_pg"),
    hitterWrcPlus: real("hitter_wrc_plus"),
    hitterSplitWrcPlus: real("hitter_split_wrc_plus"),
    teamTotal: real("team_total"),
    vegasTotal: real("vegas_total"),
    parkHrFactor: real("park_hr_factor"),
    weatherTemp: real("weather_temp"),
    windSpeed: real("wind_speed"),
    opposingPitcherName: text("opposing_pitcher_name"),
    opposingPitcherHand: text("opposing_pitcher_hand"),
    opposingPitcherHrPer9: real("opposing_pitcher_hr_per_9"),
    opposingPitcherHrFbPct: real("opposing_pitcher_hr_fb_pct"),
    opposingPitcherXfip: real("opposing_pitcher_xfip"),
    opposingPitcherEra: real("opposing_pitcher_era"),
    actualHr: integer("actual_hr"),
    hitHr1Plus: boolean("hit_hr_1plus"),
    actualFpts: real("actual_fpts"),
    actualOwnPct: real("actual_own_pct"),
    createdAt: timestamp("created_at").defaultNow(),
  },
  (t) => [
    index("idx_mlb_homerun_snapshots_run").on(t.runId, t.dkPlayerId),
    index("idx_mlb_homerun_snapshots_slate").on(t.slateId, t.hrProb1Plus),
    unique("mlb_homerun_snapshots_run_player_key").on(t.runId, t.dkPlayerId),
  ]
);

export const oddsSignalRuns = pgTable(
  "odds_signal_runs",
  {
    id: serial("id").primaryKey(),
    sport: text("sport").notNull(),
    slateId: integer("slate_id")
      .notNull()
      .references(() => dkSlates.id),
    analysisVersion: text("analysis_version").notNull(),
    sampleSize: integer("sample_size").notNull().default(0),
    reportJson: jsonb("report_json").notNull().default({}),
    createdAt: timestamp("created_at").defaultNow(),
    updatedAt: timestamp("updated_at").defaultNow(),
  },
  (t) => [
    unique("odds_signal_runs_slate_key").on(t.slateId),
    index("idx_odds_signal_runs_sport_created").on(t.sport, t.createdAt),
  ]
);

export const optimizerJobs = pgTable(
  "optimizer_jobs",
  {
    id: serial("id").primaryKey(),
    sport: text("sport").notNull(),
    slateId: integer("slate_id")
      .notNull()
      .references(() => dkSlates.id),
    clientToken: text("client_token").notNull(),
    status: text("status").notNull().default("queued"),
    requestedLineups: integer("requested_lineups").notNull(),
    builtLineups: integer("built_lineups").notNull().default(0),
    eligibleCount: integer("eligible_count"),
    settingsJson: jsonb("settings_json").notNull(),
    snapshotJson: jsonb("snapshot_json").notNull(),
    selectedMatchupsJson: jsonb("selected_matchups_json").notNull(),
    poolSnapshotJson: jsonb("pool_snapshot_json").notNull(),
    effectiveSettingsJson: jsonb("effective_settings_json"),
    probeSummaryJson: jsonb("probe_summary_json").notNull().default([]),
    relaxedConstraintsJson: jsonb("relaxed_constraints_json").notNull().default([]),
    workflowRunId: text("workflow_run_id"),
    probeMs: integer("probe_ms"),
    totalMs: integer("total_ms"),
    terminationReason: text("termination_reason"),
    warning: text("warning"),
    error: text("error"),
    startedAt: timestamp("started_at"),
    finishedAt: timestamp("finished_at"),
    heartbeatAt: timestamp("heartbeat_at"),
    createdAt: timestamp("created_at").defaultNow(),
  },
  (t) => [
    index("idx_optimizer_jobs_lookup").on(t.clientToken, t.sport, t.slateId, t.status),
    index("idx_optimizer_jobs_created").on(t.createdAt),
  ]
);

export const optimizerJobLineups = pgTable(
  "optimizer_job_lineups",
  {
    id: serial("id").primaryKey(),
    jobId: integer("job_id")
      .notNull()
      .references(() => optimizerJobs.id),
    lineupNum: integer("lineup_num").notNull(),
    slotPlayerIdsJson: jsonb("slot_player_ids_json").notNull(),
    playerIdsJson: jsonb("player_ids_json").notNull(),
    totalSalary: integer("total_salary").notNull(),
    projFpts: doublePrecision("proj_fpts").notNull(),
    leverage: doublePrecision("leverage").notNull(),
    actualFpts: doublePrecision("actual_fpts"),
    mlbHrSignalJson: jsonb("mlb_hr_signal_json"),
    durationMs: integer("duration_ms").notNull(),
    winningStage: text("winning_stage"),
    attemptsJson: jsonb("attempts_json").notNull().default([]),
    createdAt: timestamp("created_at").defaultNow(),
  },
  (t) => [
    unique("optimizer_job_lineups_job_num_key").on(t.jobId, t.lineupNum),
    index("idx_optimizer_job_lineups_job").on(t.jobId, t.lineupNum),
  ]
);

// ── YouTube video analysis (sport-agnostic) ────────────────────
// User pastes a video URL; we fetch its transcript and ask DeepSeek for a
// structured, per-team/per-player breakdown of what was discussed. Not
// tied to any one sport -- sport is a best-guess field per subject, not a
// hard filter, since a single video can span multiple sports.

// Canonical Tennis identity and immutable 2023+ source foundation (SCRUM-20).
// The existing Tennis page still reads legacy raw SQL tables while the new
// source-aware API is built in dependency order.
export const tennisPlayers = pgTable(
  "tennis_players",
  {
    id: bigserial("id", { mode: "number" }).primaryKey(),
    tour: text("tour").notNull(),
    canonicalName: text("canonical_name").notNull(),
    normName: text("norm_name").notNull(),
    birthDate: date("birth_date"),
    countryCode: text("country_code"),
    active: boolean("active").notNull().default(true),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow(),
    updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow(),
  },
  (t) => [
    unique("tennis_players_tour_norm_key").on(t.tour, t.normName),
    index("idx_tennis_players_tour_name").on(t.tour, t.normName),
  ],
);

export const tennisPlayerAliases = pgTable(
  "tennis_player_aliases",
  {
    id: bigserial("id", { mode: "number" }).primaryKey(),
    playerId: bigint("player_id", { mode: "number" }).notNull().references(() => tennisPlayers.id),
    provider: text("provider").notNull(),
    tour: text("tour").notNull(),
    providerPlayerId: text("provider_player_id"),
    rawName: text("raw_name").notNull(),
    normName: text("norm_name").notNull(),
    matchMethod: text("match_method").notNull().default("exact_normalized"),
    matchConfidence: doublePrecision("match_confidence").notNull().default(1),
    verified: boolean("verified").notNull().default(false),
    effectiveFrom: date("effective_from"),
    effectiveTo: date("effective_to"),
    sourceAvailableAt: timestamp("source_available_at", { withTimezone: true }),
    capturedAt: timestamp("captured_at", { withTimezone: true }).defaultNow(),
    rawChecksum: text("raw_checksum"),
  },
  (t) => [
    unique("tennis_alias_provider_tour_norm_player_key").on(t.provider, t.tour, t.normName, t.playerId),
    index("idx_tennis_aliases_lookup").on(t.provider, t.tour, t.normName),
  ],
);

export const tennisSourcePartitions = pgTable(
  "tennis_source_partitions",
  {
    id: bigserial("id", { mode: "number" }).primaryKey(),
    runId: text("run_id").notNull(),
    provider: text("provider").notNull(),
    dataset: text("dataset").notNull(),
    tour: text("tour").notNull(),
    season: integer("season").notNull(),
    sourceUrl: text("source_url"),
    expected: boolean("expected").notNull().default(true),
    status: text("status").notNull(),
    rowCount: integer("row_count").notNull().default(0),
    acceptedCount: integer("accepted_count").notNull().default(0),
    rejectedCount: integer("rejected_count").notNull().default(0),
    minMatchDate: date("min_match_date"),
    maxMatchDate: date("max_match_date"),
    missingness: jsonb("missingness").notNull().default({}),
    rawChecksum: text("raw_checksum"),
    parserVersion: text("parser_version").notNull(),
    sourceAvailableAt: timestamp("source_available_at", { withTimezone: true }),
    retrievalStartedAt: timestamp("retrieval_started_at", { withTimezone: true }).notNull(),
    retrievalCompletedAt: timestamp("retrieval_completed_at", { withTimezone: true }),
    errorMessage: text("error_message"),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow(),
  },
  (t) => [
    unique("tennis_partition_run_source_key").on(t.runId, t.provider, t.dataset, t.tour, t.season),
    index("idx_tennis_partitions_latest").on(t.provider, t.dataset, t.tour, t.season, t.createdAt),
  ],
);

export const tennisEvents = pgTable(
  "tennis_events",
  {
    id: bigserial("id", { mode: "number" }).primaryKey(),
    tour: text("tour").notNull(),
    canonicalTournament: text("canonical_tournament").notNull(),
    playerOneId: bigint("player_one_id", { mode: "number" }).notNull().references(() => tennisPlayers.id),
    playerTwoId: bigint("player_two_id", { mode: "number" }).notNull().references(() => tennisPlayers.id),
    scheduledAt: timestamp("scheduled_at", { withTimezone: true }),
    round: text("round"),
    bestOf: integer("best_of"),
    surface: text("surface"),
    indoor: boolean("indoor"),
    status: text("status").notNull().default("scheduled"),
    currentRevisionId: bigint("current_revision_id", { mode: "number" }),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow(),
    updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow(),
  },
  (t) => [index("idx_tennis_events_schedule").on(t.tour, t.scheduledAt, t.canonicalTournament)],
);

export const tennisEventRevisions = pgTable(
  "tennis_event_revisions",
  {
    id: bigserial("id", { mode: "number" }).primaryKey(),
    eventId: bigint("event_id", { mode: "number" }).notNull().references(() => tennisEvents.id),
    revisionNo: integer("revision_no").notNull(),
    provider: text("provider").notNull(),
    providerEventId: text("provider_event_id"),
    tournamentRaw: text("tournament_raw"),
    commenceTime: timestamp("commence_time", { withTimezone: true }),
    playerOneId: bigint("player_one_id", { mode: "number" }).notNull().references(() => tennisPlayers.id),
    playerTwoId: bigint("player_two_id", { mode: "number" }).notNull().references(() => tennisPlayers.id),
    surface: text("surface"),
    indoor: boolean("indoor"),
    round: text("round"),
    status: text("status"),
    sourceAvailableAt: timestamp("source_available_at", { withTimezone: true }),
    capturedAt: timestamp("captured_at", { withTimezone: true }).notNull(),
    rawChecksum: text("raw_checksum").notNull(),
    parserVersion: text("parser_version").notNull(),
    rawPayload: jsonb("raw_payload"),
    supersedesRevisionId: bigint("supersedes_revision_id", { mode: "number" }),
  },
  (t) => [
    unique("tennis_event_revision_number_key").on(t.eventId, t.revisionNo),
    unique("tennis_event_revision_provider_checksum_key").on(t.provider, t.providerEventId, t.rawChecksum),
    index("idx_tennis_event_revisions_event").on(t.eventId, t.revisionNo),
  ],
);

export const tennisEventAliases = pgTable(
  "tennis_event_aliases",
  {
    id: bigserial("id", { mode: "number" }).primaryKey(),
    eventId: bigint("event_id", { mode: "number" }).notNull().references(() => tennisEvents.id),
    eventRevisionId: bigint("event_revision_id", { mode: "number" }).references(() => tennisEventRevisions.id),
    provider: text("provider").notNull(),
    providerEventId: text("provider_event_id").notNull(),
    firstSeenAt: timestamp("first_seen_at", { withTimezone: true }).notNull(),
    lastSeenAt: timestamp("last_seen_at", { withTimezone: true }).notNull(),
    rawChecksum: text("raw_checksum"),
  },
  (t) => [unique("tennis_event_alias_provider_key").on(t.provider, t.providerEventId)],
);

export const tennisHistoricalMatches = pgTable(
  "tennis_historical_matches",
  {
    id: bigserial("id", { mode: "number" }).primaryKey(),
    source: text("source").notNull(),
    sourceMatchKey: text("source_match_key").notNull(),
    sourcePartitionId: bigint("source_partition_id", { mode: "number" }).references(() => tennisSourcePartitions.id),
    tour: text("tour").notNull(),
    season: integer("season").notNull(),
    matchDate: date("match_date").notNull(),
    startTime: timestamp("start_time", { withTimezone: true }),
    tournament: text("tournament").notNull(),
    round: text("round"),
    bestOf: integer("best_of"),
    surface: text("surface"),
    indoor: boolean("indoor"),
    winnerPlayerId: bigint("winner_player_id", { mode: "number" }).notNull().references(() => tennisPlayers.id),
    loserPlayerId: bigint("loser_player_id", { mode: "number" }).notNull().references(() => tennisPlayers.id),
    score: text("score"),
    completionStatus: text("completion_status").notNull().default("completed"),
    retired: boolean("retired").notNull().default(false),
    walkover: boolean("walkover").notNull().default(false),
    winnerRank: integer("winner_rank"),
    loserRank: integer("loser_rank"),
    winnerRankPoints: integer("winner_rank_points"),
    loserRankPoints: integer("loser_rank_points"),
    winnerDecimalOdds: doublePrecision("winner_decimal_odds"),
    loserDecimalOdds: doublePrecision("loser_decimal_odds"),
    oddsSource: text("odds_source"),
    oddsTiming: text("odds_timing"),
    sourceAvailableAt: timestamp("source_available_at", { withTimezone: true }),
    statsThroughAt: timestamp("stats_through_at", { withTimezone: true }),
    capturedAt: timestamp("captured_at", { withTimezone: true }).notNull(),
    transformationVersion: text("transformation_version").notNull(),
    rawChecksum: text("raw_checksum").notNull(),
    rawPayload: jsonb("raw_payload"),
    correctionOfId: bigint("correction_of_id", { mode: "number" }),
    isCurrent: boolean("is_current").notNull().default(true),
    supersededAt: timestamp("superseded_at", { withTimezone: true }),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow(),
  },
  (t) => [
    unique("tennis_history_source_transform_key").on(
      t.source,
      t.sourceMatchKey,
      t.rawChecksum,
      t.transformationVersion,
    ),
    index("idx_tennis_history_date").on(t.tour, t.matchDate, t.surface),
  ],
);

export const tennisPlayerMatchStats = pgTable(
  "tennis_player_match_stats",
  {
    id: bigserial("id", { mode: "number" }).primaryKey(),
    historicalMatchId: bigint("historical_match_id", { mode: "number" }).notNull().references(() => tennisHistoricalMatches.id),
    playerId: bigint("player_id", { mode: "number" }).notNull().references(() => tennisPlayers.id),
    opponentPlayerId: bigint("opponent_player_id", { mode: "number" }).notNull().references(() => tennisPlayers.id),
    isWinner: boolean("is_winner").notNull(),
    aces: integer("aces"),
    doubleFaults: integer("double_faults"),
    servePoints: integer("serve_points"),
    firstServesIn: integer("first_serves_in"),
    firstServePointsWon: integer("first_serve_points_won"),
    secondServePointsWon: integer("second_serve_points_won"),
    serviceGames: integer("service_games"),
    breakPointsSaved: integer("break_points_saved"),
    breakPointsFaced: integer("break_points_faced"),
    servePointsWonPct: doublePrecision("serve_points_won_pct"),
    returnPointsWonPct: doublePrecision("return_points_won_pct"),
    statsAvailable: boolean("stats_available").notNull().default(false),
    missingReason: text("missing_reason"),
    formulaVersion: text("formula_version"),
    sampleSize: integer("sample_size"),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow(),
  },
  (t) => [unique("tennis_match_stats_match_player_key").on(t.historicalMatchId, t.playerId)],
);

export const tennisExactQuotes = pgTable(
  "tennis_exact_quotes",
  {
    id: bigserial("id", { mode: "number" }).primaryKey(),
    eventId: bigint("event_id", { mode: "number" }).notNull().references(() => tennisEvents.id),
    eventRevisionId: bigint("event_revision_id", { mode: "number" }).notNull().references(() => tennisEventRevisions.id),
    source: text("source").notNull(),
    providerEventId: text("provider_event_id").notNull(),
    bookmakerKey: text("bookmaker_key").notNull(),
    bookmakerName: text("bookmaker_name"),
    region: text("region"),
    market: text("market").notNull(),
    selectionType: text("selection_type").notNull(),
    selectionPlayerId: bigint("selection_player_id", { mode: "number" }).references(() => tennisPlayers.id),
    selectionSide: text("selection_side"),
    lineValue: doublePrecision("line_value"),
    priceAmerican: integer("price_american").notNull(),
    priceDecimal: doublePrecision("price_decimal").notNull(),
    pairedSelectionType: text("paired_selection_type").notNull(),
    pairedPlayerId: bigint("paired_player_id", { mode: "number" }).references(() => tennisPlayers.id),
    pairedSide: text("paired_side"),
    pairedLineValue: doublePrecision("paired_line_value"),
    pairedPriceAmerican: integer("paired_price_american").notNull(),
    pairedPriceDecimal: doublePrecision("paired_price_decimal").notNull(),
    bookmakerUpdatedAt: timestamp("bookmaker_updated_at", { withTimezone: true }).notNull(),
    sourceAvailableAt: timestamp("source_available_at", { withTimezone: true }).notNull(),
    capturedAt: timestamp("captured_at", { withTimezone: true }).notNull(),
    commenceTimeAtCapture: timestamp("commence_time_at_capture", { withTimezone: true }).notNull(),
    isPrestart: boolean("is_prestart").notNull(),
    validationStatus: text("validation_status").notNull().default("valid"),
    rejectionReason: text("rejection_reason"),
    captureKey: text("capture_key").notNull(),
    rawChecksum: text("raw_checksum").notNull(),
    parserVersion: text("parser_version").notNull(),
    rawPayload: jsonb("raw_payload"),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow(),
  },
  (t) => [index("idx_tennis_quotes_lookup").on(t.eventId, t.bookmakerKey, t.market, t.capturedAt)],
);

export const tennisIdentityReviews = pgTable(
  "tennis_identity_reviews",
  {
    id: bigserial("id", { mode: "number" }).primaryKey(),
    provider: text("provider").notNull(),
    tour: text("tour").notNull(),
    rawName: text("raw_name").notNull(),
    normName: text("norm_name").notNull(),
    context: jsonb("context").notNull().default({}),
    candidates: jsonb("candidates").notNull().default([]),
    reason: text("reason").notNull(),
    status: text("status").notNull().default("open"),
    resolutionPlayerId: bigint("resolution_player_id", { mode: "number" }).references(() => tennisPlayers.id),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow(),
    resolvedAt: timestamp("resolved_at", { withTimezone: true }),
  },
  (t) => [index("idx_tennis_identity_reviews_open").on(t.status, t.tour, t.normName)],
);

export const tennisPlayerFeatureSnapshots = pgTable(
  "tennis_player_feature_snapshots",
  {
    id: bigserial("id", { mode: "number" }).primaryKey(),
    playerId: bigint("player_id", { mode: "number" }).notNull().references(() => tennisPlayers.id),
    opponentPlayerId: bigint("opponent_player_id", { mode: "number" }).references(() => tennisPlayers.id),
    historicalMatchId: bigint("historical_match_id", { mode: "number" }).references(() => tennisHistoricalMatches.id),
    eventId: bigint("event_id", { mode: "number" }).references(() => tennisEvents.id),
    cutoffAt: timestamp("cutoff_at", { withTimezone: true }).notNull(),
    statsThroughAt: timestamp("stats_through_at", { withTimezone: true }).notNull(),
    surface: text("surface"),
    overallElo: doublePrecision("overall_elo"),
    surfaceElo: doublePrecision("surface_elo"),
    recentForm: doublePrecision("recent_form"),
    restDays: doublePrecision("rest_days"),
    recentMatchLoad: integer("recent_match_load"),
    servePointsWonPct: doublePrecision("serve_points_won_pct"),
    returnPointsWonPct: doublePrecision("return_points_won_pct"),
    rank: integer("rank"),
    rankPoints: integer("rank_points"),
    sampleSize: integer("sample_size").notNull().default(0),
    featureVersion: text("feature_version").notNull(),
    sourceAvailability: jsonb("source_availability").notNull().default({}),
    missingness: jsonb("missingness").notNull().default({}),
    provenance: jsonb("provenance").notNull().default({}),
    rawChecksum: text("raw_checksum").notNull(),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow(),
  },
  (t) => [index("idx_tennis_feature_snapshots_cutoff").on(t.playerId, t.cutoffAt, t.featureVersion)],
);

export const tennisEloRuns = pgTable(
  "tennis_elo_runs",
  {
    id: bigserial("id", { mode: "number" }).primaryKey(),
    algorithmVersion: text("algorithm_version").notNull(),
    sourceStartDate: date("source_start_date").notNull(),
    sourceEndDate: date("source_end_date").notNull(),
    sourceMatchCount: integer("source_match_count").notNull(),
    sourceChecksum: text("source_checksum").notNull(),
    config: jsonb("config").notNull(),
    status: text("status").notNull().default("running"),
    eligibleMatchCount: integer("eligible_match_count").notNull().default(0),
    excludedMatchCount: integer("excluded_match_count").notNull().default(0),
    eventCount: integer("event_count").notNull().default(0),
    errorMessage: text("error_message"),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow(),
    completedAt: timestamp("completed_at", { withTimezone: true }),
  },
  (t) => [unique("tennis_elo_run_source_key").on(t.algorithmVersion, t.sourceChecksum)],
);

export const tennisEloRatingEvents = pgTable(
  "tennis_elo_rating_events",
  {
    id: bigserial("id", { mode: "number" }).primaryKey(),
    runId: bigint("run_id", { mode: "number" }).notNull().references(() => tennisEloRuns.id),
    historicalMatchId: bigint("historical_match_id", { mode: "number" }).notNull().references(() => tennisHistoricalMatches.id),
    playerId: bigint("player_id", { mode: "number" }).notNull().references(() => tennisPlayers.id),
    opponentPlayerId: bigint("opponent_player_id", { mode: "number" }).notNull().references(() => tennisPlayers.id),
    tour: text("tour").notNull(),
    matchDate: date("match_date").notNull(),
    cutoffAt: timestamp("cutoff_at", { withTimezone: true }).notNull(),
    statsThroughAt: timestamp("stats_through_at", { withTimezone: true }).notNull(),
    surface: text("surface").notNull(),
    surfaceBucket: text("surface_bucket").notNull(),
    isWinner: boolean("is_winner").notNull(),
    eligible: boolean("eligible").notNull(),
    exclusionReason: text("exclusion_reason"),
    overallBefore: doublePrecision("overall_before").notNull(),
    overallDelta: doublePrecision("overall_delta").notNull(),
    overallAfter: doublePrecision("overall_after").notNull(),
    surfaceBefore: doublePrecision("surface_before").notNull(),
    surfaceDelta: doublePrecision("surface_delta").notNull(),
    surfaceAfter: doublePrecision("surface_after").notNull(),
    blendedSurfaceBefore: doublePrecision("blended_surface_before").notNull(),
    expectedOverall: doublePrecision("expected_overall").notNull(),
    expectedSurface: doublePrecision("expected_surface").notNull(),
    expectedBlended: doublePrecision("expected_blended").notNull(),
    overallMatchesBefore: integer("overall_matches_before").notNull(),
    overallMatchesAfter: integer("overall_matches_after").notNull(),
    surfaceMatchesBefore: integer("surface_matches_before").notNull(),
    surfaceMatchesAfter: integer("surface_matches_after").notNull(),
    surfaceReliability: doublePrecision("surface_reliability").notNull(),
    reliabilityLabel: text("reliability_label").notNull(),
    lastEligibleMatchDate: date("last_eligible_match_date"),
    inactivityDays: integer("inactivity_days"),
    sameDayBatch: boolean("same_day_batch").notNull().default(true),
    sameDayMatchCount: integer("same_day_match_count").notNull().default(1),
    orderingStatus: text("ordering_status").notNull(),
    priorRating: doublePrecision("prior_rating").notNull(),
    kFactor: doublePrecision("k_factor").notNull(),
    shrinkageMatches: doublePrecision("shrinkage_matches").notNull(),
    algorithmVersion: text("algorithm_version").notNull(),
    sourceRawChecksum: text("source_raw_checksum").notNull(),
    eventChecksum: text("event_checksum").notNull(),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow(),
  },
  (t) => [
    unique("tennis_elo_event_run_match_player_key").on(t.runId, t.historicalMatchId, t.playerId),
    index("idx_tennis_elo_events_player_date").on(t.playerId, t.matchDate, t.algorithmVersion),
  ],
);

export const tennisEloEvaluationRuns = pgTable(
  "tennis_elo_evaluation_runs",
  {
    id: bigserial("id", { mode: "number" }).primaryKey(),
    eloRunId: bigint("elo_run_id", { mode: "number" }).notNull().references(() => tennisEloRuns.id),
    evaluationVersion: text("evaluation_version").notNull(),
    config: jsonb("config").notNull(),
    sourceChecksum: text("source_checksum").notNull(),
    status: text("status").notNull().default("running"),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow(),
    completedAt: timestamp("completed_at", { withTimezone: true }),
    errorMessage: text("error_message"),
  },
  (t) => [unique("tennis_elo_eval_run_key").on(t.eloRunId, t.evaluationVersion)],
);

export const tennisEloEvaluationMetrics = pgTable(
  "tennis_elo_evaluation_metrics",
  {
    id: bigserial("id", { mode: "number" }).primaryKey(),
    evaluationRunId: bigint("evaluation_run_id", { mode: "number" }).notNull().references(() => tennisEloEvaluationRuns.id),
    tour: text("tour").notNull(),
    period: text("period").notNull(),
    surface: text("surface").notNull(),
    model: text("model").notNull(),
    sampleSize: integer("sample_size").notNull(),
    brier: doublePrecision("brier"),
    logLoss: doublePrecision("log_loss"),
    calibrationError: doublePrecision("calibration_error"),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow(),
  },
  (t) => [unique("tennis_elo_eval_metric_key").on(t.evaluationRunId, t.tour, t.period, t.surface, t.model)],
);

export const tennisEloPromotionGates = pgTable(
  "tennis_elo_promotion_gates",
  {
    id: bigserial("id", { mode: "number" }).primaryKey(),
    evaluationRunId: bigint("evaluation_run_id", { mode: "number" }).notNull().references(() => tennisEloEvaluationRuns.id),
    tour: text("tour").notNull(),
    validationSampleSize: integer("validation_sample_size").notNull(),
    validationLoglossDelta: doublePrecision("validation_logloss_delta"),
    bootstrapCiLow: doublePrecision("bootstrap_ci_low"),
    bootstrapCiHigh: doublePrecision("bootstrap_ci_high"),
    validationEceDelta: doublePrecision("validation_ece_delta"),
    finalTestSampleSize: integer("final_test_sample_size").notNull(),
    finalLoglossDelta: doublePrecision("final_logloss_delta"),
    finalEceDelta: doublePrecision("final_ece_delta"),
    gateStatus: text("gate_status").notNull(),
    reasons: jsonb("reasons").notNull(),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow(),
  },
  (t) => [unique("tennis_elo_promotion_gate_key").on(t.evaluationRunId, t.tour)],
);

export const videoAnalysis = pgTable(
  "video_analysis",
  {
    id: serial("id").primaryKey(),
    videoUrl: text("video_url").notNull(),
    videoId: text("video_id").notNull(),
    title: text("title"),
    channelName: text("channel_name"),
    transcriptText: text("transcript_text").notNull(),
    analysisJson: jsonb("analysis_json").notNull(),
    modelVersion: text("model_version").notNull(),
    createdAt: timestamp("created_at").defaultNow(),
  },
  (t) => [unique("video_analysis_video_id_key").on(t.videoId)]
);

// ── YouTube picks channel tracking ──────────────────────────
// youtube_pick_channels is written from this web app (the "Add Channel"
// action) and read by the Python ingest script to know what to scrape —
// self-provisions via ensureYoutubePickChannelsTable(), also defined in
// db/schema.py so either side can create it first.
// youtube_pick_videos/youtube_picks are owned by the Python pipeline
// (ingest/youtube_picks_videos.py + model/youtube_picks_extraction.py) —
// read-only from the web app, same pattern as mlb_matchups/mlb_team_stats.

export const youtubePickChannels = pgTable("youtube_pick_channels", {
  id: serial("id").primaryKey(),
  channelId: text("channel_id").notNull().unique(),
  channelName: text("channel_name").notNull(),
  handle: text("handle"),
  active: boolean("active").notNull().default(true),
  addedAt: timestamp("added_at").defaultNow(),
});

export const youtubePickVideos = pgTable("youtube_pick_videos", {
  id: serial("id").primaryKey(),
  channelId: text("channel_id").notNull(),
  channelName: text("channel_name").notNull(),
  videoId: text("video_id").notNull(),
  title: text("title").notNull(),
  publishedAt: timestamp("published_at"),
  transcriptText: text("transcript_text"),
  scrapedAt: timestamp("scraped_at").defaultNow(),
});

export const youtubePicks = pgTable("youtube_picks", {
  id: serial("id").primaryKey(),
  videoId: integer("video_id")
    .notNull()
    .references(() => youtubePickVideos.id),
  sport: text("sport").notNull(),
  betType: text("bet_type").notNull(),
  subject: text("subject").notNull(),
  opponent: text("opponent"),
  selection: text("selection").notNull(),
  oddsAmerican: integer("odds_american"),
  gameContext: text("game_context"),
  confidenceLabel: text("confidence_label"),
  quote: text("quote").notNull(),
  modelVersion: text("model_version").notNull(),
  status: text("status").notNull().default("pending"),
  matchupRef: text("matchup_ref"),
  resultDetail: text("result_detail"),
  extractedAt: timestamp("extracted_at").defaultNow(),
  settledAt: timestamp("settled_at"),
});

// ── Type inference ────────────────────────────────────────────

export type Team = typeof teams.$inferSelect;
export type NbaTeamStats = typeof nbaTeamStats.$inferSelect;
export type NbaPlayerStats = typeof nbaPlayerStats.$inferSelect;
export type NbaPlayerGameLog = typeof nbaPlayerGameLogs.$inferSelect;
export type NbaTeamGameLog = typeof nbaTeamGameLogs.$inferSelect;
export type NbaMatchup = typeof nbaMatchups.$inferSelect;
export type MlbTeam = typeof mlbTeams.$inferSelect;
export type MlbParkFactors = typeof mlbParkFactors.$inferSelect;
export type MlbMatchup = typeof mlbMatchups.$inferSelect;
export type MlbBatterStats = typeof mlbBatterStats.$inferSelect;
export type MlbPitcherStats = typeof mlbPitcherStats.$inferSelect;
export type MlbTeamStats = typeof mlbTeamStats.$inferSelect;
export type OddsSignalRun = typeof oddsSignalRuns.$inferSelect;
export type TennisPlayer = typeof tennisPlayers.$inferSelect;
export type TennisPlayerAlias = typeof tennisPlayerAliases.$inferSelect;
export type TennisSourcePartition = typeof tennisSourcePartitions.$inferSelect;
export type TennisEvent = typeof tennisEvents.$inferSelect;
export type TennisEventRevision = typeof tennisEventRevisions.$inferSelect;
export type TennisEventAlias = typeof tennisEventAliases.$inferSelect;
export type TennisHistoricalMatch = typeof tennisHistoricalMatches.$inferSelect;
export type TennisPlayerMatchStat = typeof tennisPlayerMatchStats.$inferSelect;
export type TennisExactQuote = typeof tennisExactQuotes.$inferSelect;
export type TennisIdentityReview = typeof tennisIdentityReviews.$inferSelect;
export type TennisPlayerFeatureSnapshot = typeof tennisPlayerFeatureSnapshots.$inferSelect;
export type TennisEloRun = typeof tennisEloRuns.$inferSelect;
export type TennisEloRatingEvent = typeof tennisEloRatingEvents.$inferSelect;
export type TennisEloEvaluationRun = typeof tennisEloEvaluationRuns.$inferSelect;
export type TennisEloEvaluationMetric = typeof tennisEloEvaluationMetrics.$inferSelect;
export type TennisEloPromotionGate = typeof tennisEloPromotionGates.$inferSelect;
export type VideoAnalysis = typeof videoAnalysis.$inferSelect;
export type YoutubePickChannel = typeof youtubePickChannels.$inferSelect;
export type YoutubePickVideo = typeof youtubePickVideos.$inferSelect;
export type YoutubePick = typeof youtubePicks.$inferSelect;
export type FfPlayer = typeof ffPlayers.$inferSelect;
export type FfPlayerInjuryObservation = typeof ffPlayerInjuryObservations.$inferSelect;
export type FfPlayerInjury = typeof ffPlayerInjuries.$inferSelect;
export type FfInjuryEvent = typeof ffInjuryEvents.$inferSelect;
export type FfRankingSet = typeof ffRankingSets.$inferSelect;
export type FfPlayerRanking = typeof ffPlayerRankings.$inferSelect;
export type FfPlayerIndicator = typeof ffPlayerIndicators.$inferSelect;
export type FfDraftSession = typeof ffDraftSessions.$inferSelect;
export type DkSlate = typeof dkSlates.$inferSelect;
export type DkPlayer = typeof dkPlayers.$inferSelect;
export type DkLineup = typeof dkLineups.$inferSelect;
export type OptimizerJob = typeof optimizerJobs.$inferSelect;
export type OptimizerJobLineup = typeof optimizerJobLineups.$inferSelect;
