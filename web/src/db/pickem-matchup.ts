import "server-only";
import { sql } from "drizzle-orm";
import { db } from "@/db";
import { matchupResidual, type MatchupForecast, type ResidualInput } from "@/lib/nfl/pickem-matchup";

/** Optional shared forecasts; only the central current policy can authorize activation. */
export async function getPickemMatchup(season: number, asOf: string): Promise<Record<number, MatchupForecast>> {
  const exists = await db.execute(sql`SELECT to_regclass('nfl_pickem_matchup_forecasts') present`);
  if (!exists.rows[0]?.present) return {};
  const result = await db.execute(sql`SELECT g.id, f.forecast_id, f.payload FROM nfl_season_games g
    JOIN LATERAL (SELECT forecast_id,payload FROM nfl_pickem_matchup_forecasts f
      WHERE f.game_id=g.nflverse_game_id AND f.available_at<=${asOf}::timestamptz
        AND f.available_at<g.kickoff AND f.decision_cutoff<g.kickoff
      ORDER BY f.available_at DESC,f.forecast_id DESC LIMIT 1) f ON TRUE WHERE g.season=${season}`);
  const out: Record<number, MatchupForecast> = {};
  for (const row of result.rows) {
    const payload = row.payload as { input?: ResidualInput } & ResidualInput;
    const input = payload.input ?? payload;
    if (!input.baseline || !Array.isArray(input.features)) continue;
    const forecast = matchupResidual(input); forecast.forecastId = String(row.forecast_id);
    if (forecast.status === "shadow" && forecast.candidate && input.model) {
      const model = input.model, required = [model.definitionId, ...Object.keys(model.coefficients)];
      const qualifications = await db.execute(sql`SELECT q.definition_id, q.policy_version, q.max_age_seconds, d.definition
        FROM nfl_context_qualifications q JOIN nfl_consumer_policy_pointers p
          ON p.consumer_id=q.consumer_id AND p.policy_version=q.policy_version
        JOIN nfl_context_definitions d ON d.definition_id=q.definition_id
        WHERE q.consumer_id='nfl-pickem' AND q.use_case=${model.useCase} AND q.cohort=${model.cohort}
          AND q.usage='predictive' AND q.approved=TRUE
          AND q.registered_at<=${asOf}::timestamptz AND p.activated_at<=${asOf}::timestamptz
          AND q.definition_id IN (${sql.join(required.map(id=>sql`${id}`),sql`, `)})`);
      const allowed = new Set(qualifications.rows.filter(r=> {
        if (String(r.definition_id) === model.definitionId)
          return (r.definition as {artifactId?: string})?.artifactId === model.artifactId;
        const feature = input.features.find(f=>f.definitionId===String(r.definition_id));
        return feature && (r.max_age_seconds == null || Date.parse(input.decisionCutoff)-Date.parse(feature.availableAt) <= Number(r.max_age_seconds)*1000);
      }).map(r=>String(r.definition_id)));
      if (required.every(id=>allowed.has(id))) {
        forecast.status = "qualified"; forecast.reasons = ["Qualified by the active NFL pick'em consumer policy"];
        forecast.qualificationIds = qualifications.rows.map(r=>`${r.definition_id}:${r.policy_version}`);
      }
    }
    out[Number(row.id)] = forecast;
  }
  return out;
}
