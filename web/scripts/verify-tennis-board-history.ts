// Read-only integration check against retained history. Run with DATABASE_URL set:
// node -r ./scripts/server-only-stub.cjs --import tsx scripts/verify-tennis-board-history.ts
import assert from "node:assert/strict";
import { sql } from "drizzle-orm";
import { db } from "../src/db";
import { getLineMovement } from "../src/db/queries";

async function main() {
  // Include past starts and old dates: neither should hide a requested trail.
  const expected = await db.execute(sql`
    SELECT m.id, m.commence_time, COUNT(*)::int AS captures
    FROM tennis_matches m JOIN game_odds_history h
      ON h.matchup_id = m.id AND h.sport = 'tennis'
    WHERE m.commence_time <= NOW()
      AND h.captured_at <= m.commence_time
      AND h.vegas_prob_home IS NOT NULL
      AND NOT (h.books ? 'polymarket')
    GROUP BY m.id ORDER BY m.commence_time DESC LIMIT 60
  `);
  assert.ok(expected.rows.length > 0, "Need retained pre-start tennis observations");
  const ids = expected.rows.map(r => Number(r.id));
  const actual = await getLineMovement("tennis", 0, "sportsbook", ids);
  assert.equal(actual.length, expected.rows.length, "Board must not use the top-40/upcoming shortlist");
  for (const row of expected.rows) {
    const found = actual.find(r => r.matchupId === Number(row.id));
    assert.ok(found);
    assert.equal(found.captures, Number(row.captures));
    assert.equal(found.trail.length, Number(row.captures));
    assert.ok(found.trail.every(p => Date.parse(p.capturedAt) <= new Date(String(row.commence_time)).getTime()));
  }
  assert.deepEqual(await getLineMovement("tennis", 7, "sportsbook", []), []);
  const upcoming = await getLineMovement("tennis", 7, "sportsbook");
  assert.ok(upcoming.every(r => !ids.includes(r.matchupId)), "Default research query remains upcoming-only");
  assert.ok(upcoming.length <= 40);
  console.log(`PASS: ${actual.length} past-start match trails match stored counts; empty/default scopes preserved.`);
}
main().catch(error => { console.error(error); process.exitCode = 1; });
