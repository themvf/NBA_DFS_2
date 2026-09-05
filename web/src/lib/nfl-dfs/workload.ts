export type WorkloadComponent = {
  raw_share: number;
  share: number;
  mean: number;
  games: number;
  recorded_games_available: number;
  normalization: string;
  recent: { season: number; week: number; actual: number; share: number }[];
};
export type WorkloadForecast = {
  game_id: number;
  season: number;
  week: number;
  kickoff: string;
  team: string;
  opponent: string;
  as_of: string;
  budgets: Record<
    string,
    {
      mean: number;
      history_mean: number;
      prior: number;
      games: number;
      weight: number;
      allocated_share?: number;
      unallocated_share?: number;
      constraint?: string;
    } | null
  >;
  players: {
    identity: string | null;
    name: string;
    position: string;
    team: string;
    components: Record<string, WorkloadComponent>;
  }[];
};
export type WorkloadReport = {
  version: string;
  season: number;
  week: number;
  as_of_at: string;
  dataset_digest: string;
  production_changed: false;
  forecasts: WorkloadForecast[];
  population: string;
  limits: string[];
  implementation: { model_sha256: string; ingest_sha256: string };
  backtest: {
    status: string;
    rows: number;
    metrics: {
      field: string;
      n: number;
      candidate_mae: number | null;
      baseline_mae: number | null;
      candidate_bias_actual_minus_projected: number | null;
    }[];
  };
};
