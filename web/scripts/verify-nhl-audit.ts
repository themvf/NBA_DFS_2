/** Read-only live reconciliation. Run with --env-file=.env.local. */
import assert from "node:assert/strict";
import { writeFileSync } from "node:fs";
import { getLineAlerts, getMarketSignalScorecard } from "../src/db/queries";
import { db } from "../src/db";
import { sql } from "drizzle-orm";

async function main(){
  const rows=await getMarketSignalScorecard("nhl");
  const totals=await db.execute(sql`SELECT count(*)::int AS alerts, count(distinct matchup_id)::int AS games,
    count(*) FILTER (WHERE outcome IN ('won','lost'))::int AS settled,
    sum(pnl_units) FILTER (WHERE outcome IN ('won','lost')) AS units
    FROM line_alerts WHERE sport='nhl' AND origin='prospective'`);
  const all=rows.find(r=>r.alertType==="all_moneyline")!,expected=totals.rows[0];
  assert.equal(all.observations,Number(expected.alerts));assert.equal(all.uniqueGames,Number(expected.games));
  assert.equal(all.settled,Number(expected.settled));assert.ok(Math.abs(all.units!-Number(expected.units))<1e-8);
  const alerts=await getLineAlerts("nhl",1000);
  const settled=alerts.filter(a=>a.outcome!=null);
  assert.ok(settled.every(a=>a.details?.close_history_id!=null && typeof a.details?.price_clv_pct==="number"));
  assert.ok(settled.every(a=>["fresh","stale"].includes(String(a.details?.quote_status))));
  assert.equal(all.pricedSettled,all.settled);
  writeFileSync("../artifacts/nhl-audit-v2-live-scorecard.json",JSON.stringify({asOf:new Date().toISOString(),rows,totals:expected},null,2));
  console.log(`Live NHL scorecard reconciled: ${all.observations} alerts, ${all.uniqueGames} games, ${all.settled} settled; original unit totals preserved.`);
}
main().catch(e=>{console.error(e);process.exitCode=1;});
