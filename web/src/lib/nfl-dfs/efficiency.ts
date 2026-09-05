export type EfficiencyRate = {
  label: string;
  mean: number;
  player_rate: number | null;
  position_prior: number;
  player_opportunities: number;
  prior_equivalent_opportunities: number;
  games: number;
  prior_rows: number;
  numerator: string;
  denominator: string | string[];
};

export type EfficiencyPlayer = {
  identity: string | null;
  name: string;
  position: string;
  status: string;
  history_games: number;
  rates: Record<string, EfficiencyRate>;
  stat_means: Record<string, number>;
  scoring_contributions: Record<string, number>;
  mean_fpts: number;
  p10_fpts: number;
  median_fpts: number;
  p90_fpts: number;
  boom_threshold: number;
  boom_rate: number;
  draws: number;
  seed: number;
  coherence_scope: "within_player_only" | "team_coupled_offense" | "separate_dst_whole_game_resample";
  opponent_context?: {
    opponent: string;
    opponent_allowed_games: number;
    defense_games: number;
    league_prior_equivalent_games: number;
  };
};

export type EfficiencyReport = {
  version: "nfl-dfs-efficiency-v3";
  season: number;
  week: number;
  as_of_at: string;
  dataset_digest: string;
  workload_run_digest: string;
  production_changed: false;
  coherence_scope: "team_coupled_offense_plus_separate_dst";
  forecasts: {
    game_id: number;
    team: string;
    opponent: string;
    kickoff: string;
    players: EfficiencyPlayer[];
    team_coherence: {
      scope: "team_coupled_offense";
      draws: number;
      seed: number;
      max_absolute_mismatch: Record<string, number>;
      mean_unallocated: Record<string, number>;
    };
  }[];
  backtest: {
    status: string;
    rows: number;
    metrics: {
      rate: string;
      label: string;
      n: number;
      candidate_mae: number | null;
      baseline_mae: number | null;
      candidate_bias_actual_minus_projected: number | null;
      unit: string;
    }[];
  };
  limits: string[];
};
