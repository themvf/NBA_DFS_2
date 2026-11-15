import "server-only";
import { sql } from "drizzle-orm";
import { db } from "@/db";
import { isMissingRelation } from "@/lib/database-errors";

type Entry = {
  players: { event_id: string; player_key: string; name: string }[];
  mean: number;
  p75: number;
};

export type P75Card = {
  version: string;
  game_date: string;
  sha256: string;
  frozen_at: string;
  lock_at: string;
  forecast_sha256: string;
  entries: Entry[];
};

type CardGrade = {
  version: string;
  card_sha256: string;
  totals: (number | null)[];
  best: number | null;
  combined: number | null;
};

export async function getNbaP75Card(date: string): Promise<{ card: P75Card; grade: CardGrade | null } | null> {
  try {
    const result = await db.execute(sql`
      SELECT a.manifest->'anchored_p75_card' AS card, g.grade, a.forecast_sha256
      FROM nba_research_archives a
      LEFT JOIN LATERAL (
        SELECT r.manifest->'anchored_p75_grade' AS grade
        FROM nba_research_reviews r
        WHERE r.forecast_sha256 = a.forecast_sha256
          AND r.manifest->'anchored_p75_grade'->>'card_sha256' = a.manifest->'anchored_p75_card'->>'sha256'
        ORDER BY (r.manifest->'anchored_p75_grade'->>'graded_at')::timestamptz DESC, r.created_at DESC
        LIMIT 1
      ) g ON true
      WHERE a.game_date = ${date}::date
        AND a.origin = 'cloud_before_tip'
        AND a.archived_at < a.lock_at
        AND a.manifest->'attachments'->>'percentile-index' = 'indexed'
        AND a.manifest->'anchored_p75_card'->>'version' = 'nba-anchored-p75-card-v1'
      ORDER BY a.archived_at DESC LIMIT 1
    `);
    const row = result.rows[0];
    if (!row) return null;
    const card = row.card as P75Card;
    if (card.game_date !== date || card.forecast_sha256 !== row.forecast_sha256 || card.entries.length !== 5 ||
        !(Date.parse(card.frozen_at) < Date.parse(card.lock_at))) throw new Error("Invalid frozen P75 card");
    const grade = row.grade as CardGrade | null;
    if (grade && (grade.version !== "nba-anchored-p75-grade-v1" || grade.card_sha256 !== card.sha256 ||
        grade.totals.length !== card.entries.length)) throw new Error("Invalid P75 card grade");
    return { card, grade };
  } catch (error) {
    if (isMissingRelation(error)) return null;
    throw error;
  }
}
