/**
 * Recorded Slate Checks (lib/nfl-dfs/slate-check), so a problem is visible on
 * the slate picker before the slate is opened, and there is a history of when
 * each problem appeared.
 *
 * One row per slate signature per distinct result: a check whose items match
 * the latest recorded one only moves `checked_at` forward, so the table grows
 * when something changes, not every half hour.
 */
import { createHash } from "node:crypto";
import { sql } from "drizzle-orm";
import { db } from "@/db";
import type { SlateCheck } from "@/lib/nfl-dfs/slate-check";

export type SlateCheckSource = "schedule" | "page";

export interface RecordedSlateCheck {
  signature: string;
  uploadId: string;
  checkedAt: string;
  source: SlateCheckSource;
  headline: string;
  needs: number;
}

/**
 * What counts as a different result: each problem's full text, but only the
 * id and level of passed checks and notes, whose wording carries timestamps.
 */
export function slateCheckDigest(check: SlateCheck): string {
  return createHash("sha256")
    .update(JSON.stringify({ needs: check.needs, items: check.items.map((i) =>
      i.level === "blocked" || i.level === "attention" ? [i.id, i.level, i.text] : [i.id, i.level]) }))
    .digest("hex");
}

/** Record a check; returns whether it differed from the latest one for this slate. */
export async function recordSlateCheck(uploadId: string, signature: string, check: SlateCheck, source: SlateCheckSource): Promise<boolean> {
  const digest = slateCheckDigest(check);
  const latest = await db.execute(sql`SELECT id, digest FROM nfl_dfs_slate_checks
    WHERE slate_signature=${signature} ORDER BY checked_at DESC, id DESC LIMIT 1`);
  const row = latest.rows[0];
  if (row && String(row.digest) === digest) {
    await db.execute(sql`UPDATE nfl_dfs_slate_checks SET checked_at=NOW(), upload_id=${uploadId}::uuid, source=${source} WHERE id=${Number(row.id)}`);
    return false;
  }
  await db.execute(sql`INSERT INTO nfl_dfs_slate_checks (upload_id, slate_signature, checked_at, source, headline, needs, items, digest)
    VALUES (${uploadId}::uuid, ${signature}, NOW(), ${source}, ${check.headline}, ${check.needs}, ${JSON.stringify(check.items)}::jsonb, ${digest})`);
  return true;
}

/** The latest recorded check per slate signature. */
export async function latestSlateChecks(): Promise<Map<string, RecordedSlateCheck>> {
  const rows = await db.execute(sql`SELECT DISTINCT ON (slate_signature) slate_signature, upload_id, checked_at, source, headline, needs
    FROM nfl_dfs_slate_checks ORDER BY slate_signature, checked_at DESC, id DESC`);
  return new Map(rows.rows.map((r) => [String(r.slate_signature), {
    signature: String(r.slate_signature), uploadId: String(r.upload_id), checkedAt: new Date(String(r.checked_at)).toISOString(),
    source: String(r.source) as SlateCheckSource, headline: String(r.headline), needs: Number(r.needs),
  }]));
}
