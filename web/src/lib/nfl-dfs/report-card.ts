export type ReportVariant = "production" | "shadow_baseline" | "opportunity";
export type Forecast = {
  mean: number | null; median?: number | null; p10: number | null; p90: number | null;
  boom_probability: number | null; history_games: number; captured_at: string;
  forecast_id: string; run_id: string; model_version: string; input_digest: string;
  stat_means: Record<string, number>; source_evidence: Record<string, unknown>;
};
export type ReportRow = {
  player_id: number; name: string; team: string; opponent: string; position: string;
  variant: ReportVariant; game_id: number; season: number; week: number; kickoff: string;
  completed: boolean; forecast: Forecast | null; valid_forecast: boolean; status: string;
  actual: number | null; error: number | null; absolute_error: number | null;
  interval_hit: boolean | null; overdue: boolean; result_id: number | null;
  result_revision_count: number; scoring_version: string | null; result_digest: string | null;
  exclusion_reason: string | null;
  components: { stat: string; projected: number | null; actual: number | null; error: number | null }[];
};
export type WeeklyReport = {
  season: number; week: number; version: string; evaluated_at: string;
  scheduled_games: number; completed_games: number; rejected_non_pregame_snapshots: number;
  checkpoint: string; population: string; missing_policy: string; rows: ReportRow[];
};
export const VARIANT_LABELS: Record<ReportVariant, string> = {
  production: "Production model", shadow_baseline: "Market-free baseline", opportunity: "Opportunity candidate",
};
export function reportSummary(rows: ReportRow[]) {
  const scored = rows.filter(r => r.error !== null);
  const intervals = scored.filter(r => r.interval_hit !== null);
  return { players: rows.length, forecasted: rows.filter(r => r.valid_forecast).length,
    scored: scored.length, unscored: rows.length - scored.length, overdue: rows.filter(r => r.overdue).length,
    mae: scored.length ? scored.reduce((s,r) => s + Math.abs(r.error!), 0) / scored.length : null,
    bias: scored.length ? scored.reduce((s,r) => s + r.error!, 0) / scored.length : null,
    coverage: intervals.length ? intervals.filter(r => r.interval_hit).length / intervals.length : null };
}
