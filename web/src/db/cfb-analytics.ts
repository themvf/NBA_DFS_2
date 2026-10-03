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
  };
}

const GAME_SELECT = sql`
  SELECT m.id, m.cfbd_game_id AS "cfbdGameId", m.game_date::text AS "gameDate",
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
