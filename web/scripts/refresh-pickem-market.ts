/** Repair persisted upcoming probabilities from captured moneylines; no API purchases or alerts.
 * Usage: node --env-file=.env.local -r ./scripts/server-only-stub.cjs --import tsx
 *   ./scripts/refresh-pickem-market.ts 2026 2 [--apply]
 */
import { sql } from "drizzle-orm";
import { db } from "../src/db";
import { getPickemEvidence } from "../src/db/pickem-evidence";
import { getNflPickemSlate } from "../src/db/queries";
import { usablePickemQuote } from "../src/lib/nfl/pickem-evidence";

async function main() {
  const season = Number(process.argv[2]), week = Number(process.argv[3]);
  if (!Number.isInteger(season) || season < 2000 || !Number.isInteger(week) || week < 1 || week > 18)
    throw new Error("Provide season and regular-season week");
  const evidence = await getPickemEvidence(season);
  if (evidence.warnings.length) throw new Error(evidence.warnings.join("; "));
  const slate = await getNflPickemSlate(season, evidence);
  let updated = 0;
  for (const game of slate.games.filter(g => g.week === week)) {
    const quote = evidence.games[game.gameId]?.latest;
    if (!usablePickemQuote(quote, game.kickoff, game.completed, evidence.loadedAt)) continue;
    if (process.argv.includes("--apply")) {
      const result = await db.execute(sql`
        UPDATE nfl_game_win_probs p SET
          p_win = (CASE WHEN p.team_id = g.home_team_id THEN ${quote.pHome}::double precision
                   ELSE 1 - ${quote.pHome}::double precision END) * (1 - COALESCE(p.p_tie, 0)),
          provenance = 'market_ml_novig',
          spread_used = ${quote.homeSpread == null ? null : -quote.homeSpread},
          spread_source = 'odds_api_ml', horizon_weeks = NULL, sigma_h = NULL,
          computed_at = ${quote.capturedAt}::timestamptz
        FROM nfl_season_games g WHERE p.game_id = g.id AND g.id = ${game.gameId}
          AND p.team_id IN (g.home_team_id, g.away_team_id)
          AND NOT g.completed AND g.kickoff > NOW()
          AND (p.computed_at IS NULL OR p.computed_at < ${quote.capturedAt}::timestamptz)
        RETURNING p.game_id`);
      updated += result.rows.length;
    }
    console.log(`${game.awayAbbrev} at ${game.homeAbbrev}: ${(quote.pHome * 100).toFixed(2)}% home; captured ${quote.capturedAt}`);
  }
  console.log(process.argv.includes("--apply") ? `Updated ${updated} stored team probabilities.` : "Dry run; pass --apply to update.");
}
main().catch(error => { console.error(error); process.exitCode = 1; });
