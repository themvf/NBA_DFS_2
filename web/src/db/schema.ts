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
export type DkSlate = typeof dkSlates.$inferSelect;
export type DkPlayer = typeof dkPlayers.$inferSelect;
export type DkLineup = typeof dkLineups.$inferSelect;
export type OptimizerJob = typeof optimizerJobs.$inferSelect;
export type OptimizerJobLineup = typeof optimizerJobLineups.$inferSelect;
