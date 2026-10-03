import { sql } from "drizzle-orm";
import { db } from "@/db";
import { coverageIssues, marketCoverage, type MarketName, type MarketCoverage } from "@/lib/cfb-coverage";

type RawGame = {
  id: number; game_date: string; commence_time: string | Date; start_time_tbd: boolean;
  away_team: string; home_team: string; odds_event_id: string | null;
  latest_at: string | Date | null; latest_books: Record<string, Record<string, unknown>> | null;
  previous_books: Record<string, Record<string, unknown>> | null;
};
type RawCheckpoint = {
  matchup_id: number; checkpoint: string; status: string; target_at: string | Date;
  due_until: string | Date; failure_reason: string | null;
};
const iso = (value: string | Date | null) => value === null ? null : new Date(value).toISOString();

export type CfbCoverageGame = {
  id: number; gameDate: string; kickoff: string; awayTeam: string; homeTeam: string;
  mapped: boolean; startTimeTbd: boolean; capturedAt: string | null;
  nextCheckpoint: { name: string; targetAt: string } | null;
  markets: Record<MarketName, MarketCoverage>; issues: string[];
};

export type CfbCoverageAudit = {
  since: string;
  checkpoints: number;
  markets: Record<MarketName, { usable: number; capturedButThin: number; missed: number }>;
  gaps: Array<{ gameId: number; gameDate: string; game: string; checkpoint: string;
    targetAt: string; market: MarketName; reason: string }>;
};

/** Audits due checkpoints for the last two weeks, per market. A captured
 * request is usable only if the corresponding market has three fresh books. */
export async function getCfbCoverageAudit(): Promise<CfbCoverageAudit> {
  const result = await db.execute(sql`
    SELECT c.matchup_id,c.checkpoint,c.status,c.target_at,c.history_id,
           m.game_date::text AS game_date,m.commence_time,
           a.name AS away_team,h.name AS home_team,
           o.captured_at,o.books
    FROM odds_capture_checkpoints c
    JOIN cfb_matchups m ON m.id=c.matchup_id
    JOIN cfb_teams a ON a.team_id=m.away_team_id
    JOIN cfb_teams h ON h.team_id=m.home_team_id
    LEFT JOIN game_odds_history o ON o.id=c.history_id AND o.sport='cfb'
      AND o.matchup_id=m.id AND o.captured_at<m.commence_time
    WHERE c.sport='cfb'
      AND COALESCE(c.failure_reason,'') <> 'superseded by kickoff reschedule'
      AND c.target_at>=NOW()-INTERVAL '14 days' AND c.target_at<=NOW()
    ORDER BY c.target_at DESC,c.matchup_id,c.checkpoint
  `);
  type AuditRow = { matchup_id: number; checkpoint: string; status: string;
    target_at: string | Date; game_date: string; commence_time: string | Date;
    away_team: string; home_team: string; captured_at: string | Date | null;
    books: Record<string, Record<string, unknown>> | null };
  const markets: CfbCoverageAudit["markets"] = {
    spread: { usable: 0, capturedButThin: 0, missed: 0 },
    total: { usable: 0, capturedButThin: 0, missed: 0 },
    moneyline: { usable: 0, capturedButThin: 0, missed: 0 },
  };
  const gaps: CfbCoverageAudit["gaps"] = [];
  const rows = result.rows as unknown as AuditRow[];
  const now = Date.now();
  for (const row of rows) {
    // An open due window is shown on the live table, not called missed here.
    if (row.status === "pending" || row.status === "attempted") continue;
    const capturedAt = iso(row.captured_at);
    const quality = marketCoverage(row.books, null, capturedAt);
    for (const market of ["spread", "total", "moneyline"] as const) {
      const usable = capturedAt !== null && quality[market].books >= 3
        && quality[market].freshAtCapture >= 3;
      const bucket = usable ? "usable" : capturedAt ? "capturedButThin" : "missed";
      markets[market][bucket]++;
      if (!usable && gaps.length < 100) gaps.push({
        gameId: Number(row.matchup_id), gameDate: row.game_date,
        game: `${row.away_team} at ${row.home_team}`, checkpoint: row.checkpoint,
        targetAt: iso(row.target_at)!, market,
        reason: capturedAt
          ? `${quality[market].books} books, ${quality[market].freshAtCapture} fresh`
          : row.status,
      });
    }
  }
  return { since: new Date(now - 14 * 86_400_000).toISOString(),
    checkpoints: markets.spread.usable + markets.spread.capturedButThin + markets.spread.missed,
    markets, gaps };
}

/** Upcoming coverage is a read-only view of canonical games, accepted history,
 * and the durable checkpoint ledger. It never treats a request as a capture. */
export async function getCfbCoverage(): Promise<{ asOf: string; games: CfbCoverageGame[] }> {
  const result = await db.execute(sql`
    SELECT m.id, m.game_date::text AS game_date, m.commence_time, m.start_time_tbd,
           a.name AS away_team, h.name AS home_team, m.odds_event_id,
           latest.captured_at AS latest_at, latest.books AS latest_books,
           previous.books AS previous_books
    FROM cfb_matchups m
    JOIN cfb_teams a ON a.team_id=m.away_team_id
    JOIN cfb_teams h ON h.team_id=m.home_team_id
    LEFT JOIN LATERAL (
      SELECT captured_at, books FROM game_odds_history
      WHERE sport='cfb' AND matchup_id=m.id AND captured_at<m.commence_time
      ORDER BY captured_at DESC,id DESC LIMIT 1
    ) latest ON TRUE
    LEFT JOIN LATERAL (
      SELECT books FROM game_odds_history
      WHERE sport='cfb' AND matchup_id=m.id AND captured_at<m.commence_time
      ORDER BY captured_at DESC,id DESC OFFSET 1 LIMIT 1
    ) previous ON TRUE
    WHERE m.completed=FALSE AND m.commence_time>NOW()
      AND m.commence_time<=NOW()+INTERVAL '72 hours'
    ORDER BY m.commence_time,m.id
  `);
  const rows = result.rows as unknown as RawGame[];
  const asOf = new Date().toISOString();
  if (!rows.length) return { asOf, games: [] };
  const ids = rows.map((row) => Number(row.id));
  const checkpointsResult = await db.execute(sql`
    SELECT c.matchup_id,c.checkpoint,c.status,c.target_at,c.due_until,c.failure_reason
    FROM odds_capture_checkpoints c JOIN cfb_matchups m ON m.id=c.matchup_id
    WHERE c.sport='cfb' AND c.matchup_id IN (${sql.join(ids.map((id) => sql`${id}`), sql`, `)})
      AND c.scheduled_start_at=m.commence_time
    ORDER BY c.matchup_id,c.target_at
  `);
  const checkpoints = new Map<number, RawCheckpoint[]>();
  for (const item of checkpointsResult.rows as unknown as RawCheckpoint[]) {
    const id = Number(item.matchup_id);
    checkpoints.set(id, [...(checkpoints.get(id) ?? []), item]);
  }
  const games = rows.map((row): CfbCoverageGame => {
    const id = Number(row.id);
    const kickoff = iso(row.commence_time)!;
    const capturedAt = iso(row.latest_at);
    const markets = marketCoverage(row.latest_books, row.previous_books, capturedAt);
    const gameCheckpoints = checkpoints.get(id) ?? [];
    const dueCheckpoint = gameCheckpoints.some((item) =>
      item.status !== "captured" && item.status !== "missed" &&
      iso(item.target_at)! <= asOf && iso(item.due_until)! >= asOf);
    const missedCheckpoint = gameCheckpoints.some((item) => item.status === "missed" &&
      item.failure_reason !== "superseded by kickoff reschedule");
    const next = gameCheckpoints.find((item) => item.status === "pending" && iso(item.target_at)! > asOf);
    const issues = coverageIssues({ mapped: row.odds_event_id !== null, kickoff, asOf,
      capturedAt, markets, dueCheckpoint, missedCheckpoint });
    if (row.start_time_tbd) issues.unshift("Kickoff time unconfirmed");
    return { id, gameDate: row.game_date, kickoff, awayTeam: row.away_team,
      homeTeam: row.home_team, mapped: row.odds_event_id !== null,
      startTimeTbd: row.start_time_tbd, capturedAt,
      nextCheckpoint: next ? { name: next.checkpoint, targetAt: iso(next.target_at)! } : null,
      markets, issues };
  });
  return { asOf, games };
}
