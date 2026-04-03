import {
  pgTable,
  serial,
  text,
  integer,
  bigint,
  doublePrecision,
  real,
  boolean,
  date,
  timestamp,
  jsonb,
  unique,
  index,
} from "drizzle-orm/pg-core";

// ── NBA tables ────────────────────────────────────────────────

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
    vegasTotal: doublePrecision("vegas_total"),
    homeWinProb: doublePrecision("vegas_prob_home"),
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
    awaySpId: integer("away_sp_id"),
    vegasTotal: doublePrecision("vegas_total"),
    homeMl: integer("home_ml"),
    awayMl: integer("away_ml"),
    vegasProbHome: doublePrecision("vegas_prob_home"),
    homeImplied: doublePrecision("home_implied"),
    awayImplied: doublePrecision("away_implied"),
    ballpark: text("ballpark"),
    weatherTemp: integer("weather_temp"),
    windSpeed: integer("wind_speed"),
    windDirection: text("wind_direction"),
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
    projOwnPct: real("proj_own_pct"),
    ourProj: real("our_proj"),
    ourLeverage: real("our_leverage"),
    ourOwnPct: real("our_own_pct"),
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
    isOut: boolean("is_out").default(false),
    actualFpts: real("actual_fpts"),
    actualOwnPct: real("actual_own_pct"),
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
export type DkSlate = typeof dkSlates.$inferSelect;
export type DkPlayer = typeof dkPlayers.$inferSelect;
export type DkLineup = typeof dkLineups.$inferSelect;
export type OptimizerJob = typeof optimizerJobs.$inferSelect;
export type OptimizerJobLineup = typeof optimizerJobLineups.$inferSelect;
