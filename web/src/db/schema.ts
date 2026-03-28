import {
  pgTable,
  serial,
  text,
  integer,
  bigint,
  doublePrecision,
  boolean,
  date,
  timestamp,
  unique,
  index,
} from "drizzle-orm/pg-core";

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
    teamId: integer("team_id")
      .notNull()
      .references(() => teams.teamId),
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
    fetchedAt: timestamp("fetched_at").defaultNow(),
  },
  (t) => [unique("nba_player_stats_player_season_key").on(t.playerId, t.season)]
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

export const dkSlates = pgTable("dk_slates", {
  id: serial("id").primaryKey(),
  slateDate: date("slate_date").notNull().unique(),
  gameCount: integer("game_count").default(0),
  dkDraftGroupId: integer("dk_draft_group_id"),
  linestarPeriodId: integer("linestar_period_id"),
  cashLine: doublePrecision("cash_line"),
  createdAt: timestamp("created_at").defaultNow(),
});

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
    teamId: integer("team_id").references(() => teams.teamId),
    matchupId: integer("matchup_id").references(() => nbaMatchups.id),
    eligiblePositions: text("eligible_positions").notNull(),
    salary: integer("salary").notNull(),
    gameInfo: text("game_info"),
    avgFptsDk: doublePrecision("avg_fpts_dk"),
    linestarProj: doublePrecision("linestar_proj"),
    projOwnPct: doublePrecision("proj_own_pct"),
    ourProj: doublePrecision("our_proj"),
    ourLeverage: doublePrecision("our_leverage"),
    isOut: boolean("is_out").default(false),
    actualFpts: doublePrecision("actual_fpts"),
    actualOwnPct: doublePrecision("actual_own_pct"),
  },
  (t) => [unique("dk_players_slate_player_key").on(t.slateId, t.dkPlayerId)]
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

// Type inference
export type Team = typeof teams.$inferSelect;
export type NbaTeamStats = typeof nbaTeamStats.$inferSelect;
export type NbaPlayerStats = typeof nbaPlayerStats.$inferSelect;
export type NbaMatchup = typeof nbaMatchups.$inferSelect;
export type DkSlate = typeof dkSlates.$inferSelect;
export type DkPlayer = typeof dkPlayers.$inferSelect;
export type DkLineup = typeof dkLineups.$inferSelect;
