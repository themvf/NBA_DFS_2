import "server-only";
import { sql } from "drizzle-orm";
import { db } from "./index";
import { MLB_GAME_SIGNALS, MLB_TERMINAL_BOOKS, normalizeMlbDate, type MlbTerminalBoard, type MlbTerminalGame, type MlbTerminalSignal } from "@/lib/mlb-terminal";

export async function getMlbTerminalBoard(requestedDate?: string): Promise<MlbTerminalBoard> {
  const date = normalizeMlbDate(requestedDate);
  const asOf = new Date().toISOString();
  const auditFrom = new Date(Date.parse(`${date}T12:00:00Z`) - 89 * 86400000).toISOString().slice(0, 10);
  const board: MlbTerminalBoard = { date, asOf, auditFrom, games: [], signals: [], issues: [] };
  const result = await db.execute(sql`
    SELECT m.id, m.game_id AS "gamePk", m.game_date::text AS date,
      m.commence_time::text AS "startsAt", h.abbreviation AS home, a.abbreviation AS away,
      COALESCE(m.game_status, 'Scheduled') AS status, m.home_score AS "homeScore", m.away_score AS "awayScore",
      m.home_sp_name AS "homeStarter", m.away_sp_name AS "awayStarter", m.ballpark AS park,
      COALESCE(trail.history, '[]'::jsonb) AS history
    FROM mlb_matchups m
    JOIN mlb_teams h ON h.team_id=m.home_team_id JOIN mlb_teams a ON a.team_id=m.away_team_id
    LEFT JOIN LATERAL (
      SELECT jsonb_agg(jsonb_build_object('id', x.id, 'capturedAt', x.captured_at::text, 'books', x.books)
        ORDER BY x.captured_at, x.id) AS history
      FROM game_odds_history x
      WHERE x.sport='mlb' AND x.matchup_id=m.id AND x.game_date=m.game_date
        AND x.captured_at < m.commence_time AND x.books IS NOT NULL
        AND x.books ?| ARRAY[${sql.join(MLB_TERMINAL_BOOKS.map((key) => sql`${key}`), sql`, `)}]::text[]
    ) trail ON TRUE
    WHERE m.game_date=${date}::date ORDER BY m.commence_time NULLS LAST, m.id
  `);
  board.games = result.rows.map((r) => ({ ...r, close: null, closeQuality: null, closeBoundary: null }) as MlbTerminalGame);
  for (const game of board.games) for (const capture of game.history) {
    capture.books = Object.fromEntries(Object.entries(capture.books).filter(([key]) => MLB_TERMINAL_BOOKS.includes(key)));
  }
  // Optional close/health tables must not take the canonical board down during a rollout.
  const optional = await Promise.allSettled([
    db.execute(sql`SELECT c.matchup_id, c.quality, c.boundary_source, h.id, h.captured_at::text, h.books
      FROM verified_clv_closes c JOIN game_odds_history h ON h.id=c.history_id
      JOIN mlb_matchups m ON m.id=c.matchup_id WHERE c.sport='mlb' AND m.game_date=${date}::date
      AND h.game_date=m.game_date`),
    db.execute(sql`SELECT a.id, a.matchup_id AS "matchupId", a.game_date::text AS date, a.matchup,
      a.alert_type AS type, a.side, a.created_at::text AS "observedAt", a.outcome,
      COALESCE(a.details_json, '{}'::jsonb) AS details, COALESCE(a.grading_json, '{}'::jsonb) AS grade,
      a.clv_pp AS "clvPp"
      FROM line_alerts a WHERE a.sport='mlb' AND a.game_date BETWEEN ${auditFrom}::date AND ${date}::date
      AND a.alert_type IN (${sql.join(MLB_GAME_SIGNALS.map((type) => sql`${type}`), sql`, `)})
      ORDER BY a.created_at DESC, a.id DESC`),
    db.execute(sql`SELECT checkpoint, status, count(*)::int AS n FROM odds_capture_checkpoints
      WHERE sport='mlb' AND (scheduled_start_at AT TIME ZONE 'America/New_York')::date=${date}::date
        AND status IN ('missed','failed') GROUP BY checkpoint, status`),
  ]);
  if (optional[0].status === "fulfilled") {
    for (const close of optional[0].value.rows) {
      const game = board.games.find((g) => g.id === close.matchup_id);
      if (game) { game.close = { id: Number(close.id), capturedAt: String(close.captured_at), books: close.books as MlbTerminalGame["history"][number]["books"] }; game.closeQuality = String(close.quality); game.closeBoundary = String(close.boundary_source); }
    }
  } else board.issues.push("Verified closing-line data is unavailable.");
  if (optional[1].status === "fulfilled") board.signals = optional[1].value.rows as unknown as MlbTerminalSignal[];
  else board.issues.push("Signal ledger is unavailable; results are not zero.");
  if (optional[2].status === "fulfilled") for (const row of optional[2].value.rows) board.issues.push(`${row.n} ${String(row.checkpoint).replace("t_minus_", "T−")} checkpoints ${row.status}.`);
  else board.issues.push("Checkpoint health is unavailable.");
  return board;
}
