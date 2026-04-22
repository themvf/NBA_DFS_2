export type HomerunFeatureImpactRow = {
  feature: string;
  group: string;
  direction: "higher HR probability" | "lower HR probability" | "neutral";
  treeApDrop: number;
  logisticCoefficient: number;
  medianFill: number;
};

export type HomerunFeatureGroupImpactRow = {
  group: string;
  averagePrecisionDrop: number;
};

export const MLB_HOMERUN_FEATURE_IMPACT = {
  modelVersion: "mlb_homerun_v2",
  featureSource: "pregame_rolling",
  testStart: "2025-07-01",
  testRows: 23559,
  testPositiveRows: 2664,
  groupImpact: [
    { group: "hitter_power", averagePrecisionDrop: 0.03613 },
    { group: "park_environment", averagePrecisionDrop: 0.017804 },
    { group: "lineup_position", averagePrecisionDrop: 0.006769 },
    { group: "pitcher_context", averagePrecisionDrop: 0.000353 },
  ] satisfies HomerunFeatureGroupImpactRow[],
  features: [
    { feature: "hitter_hr_x_order", group: "hitter_power", direction: "higher HR probability", treeApDrop: 0.019889, logisticCoefficient: 0.090517, medianFill: 0.1033 },
    { feature: "hitter_hr_x_park", group: "hitter_power, park_environment", direction: "higher HR probability", treeApDrop: 0.016039, logisticCoefficient: 0.009029, medianFill: 0.1016 },
    { feature: "hitter_pa_pg", group: "base_context", direction: "higher HR probability", treeApDrop: 0.00811, logisticCoefficient: 0.14467, medianFill: 3.8333 },
    { feature: "batting_order", group: "lineup_position", direction: "lower HR probability", treeApDrop: 0.004942, logisticCoefficient: -0.24024, medianFill: 5.0 },
    { feature: "hitter_hr_pg", group: "hitter_power", direction: "higher HR probability", treeApDrop: 0.004369, logisticCoefficient: 0.006468, medianFill: 0.1027 },
    { feature: "hitter_iso", group: "hitter_power", direction: "higher HR probability", treeApDrop: 0.002811, logisticCoefficient: 0.13893, medianFill: 0.154 },
    { feature: "hitter_hr_x_pitcher_hr9", group: "hitter_power, pitcher_context", direction: "higher HR probability", treeApDrop: 0.002283, logisticCoefficient: 0.097999, medianFill: 0.1074 },
    { feature: "hitter_games", group: "base_context", direction: "higher HR probability", treeApDrop: 0.001745, logisticCoefficient: 0.078491, medianFill: 60.0 },
    { feature: "hitter_slg", group: "hitter_power", direction: "lower HR probability", treeApDrop: 0.001281, logisticCoefficient: -0.134066, medianFill: 0.4 },
    { feature: "park_hr_factor", group: "park_environment", direction: "higher HR probability", treeApDrop: 0.001237, logisticCoefficient: 0.104019, medianFill: 1.0 },
    { feature: "pitcher_whip", group: "pitcher_context", direction: "lower HR probability", treeApDrop: 0.001006, logisticCoefficient: -0.056961, medianFill: 1.24 },
    { feature: "pitcher_games", group: "pitcher_context", direction: "lower HR probability", treeApDrop: 0.000814, logisticCoefficient: -0.042577, medianFill: 10.0 },
    { feature: "hitter_iso_x_park", group: "hitter_power, park_environment", direction: "higher HR probability", treeApDrop: 0.000766, logisticCoefficient: 0.036296, medianFill: 0.151 },
    { feature: "park_runs_factor", group: "park_environment", direction: "lower HR probability", treeApDrop: 0.000577, logisticCoefficient: -0.060752, medianFill: 1.0 },
    { feature: "hitter_iso_x_pitcher_hr9", group: "hitter_power, pitcher_context", direction: "higher HR probability", treeApDrop: 0.000511, logisticCoefficient: 0.017667, medianFill: 0.1666 },
    { feature: "pitcher_ip_pg", group: "pitcher_context", direction: "higher HR probability", treeApDrop: 0.000399, logisticCoefficient: 0.044332, medianFill: 8.3294 },
    { feature: "is_home", group: "park_environment", direction: "neutral", treeApDrop: 0.000085, logisticCoefficient: -0.005674, medianFill: 0.0 },
    { feature: "is_order_4", group: "lineup_position", direction: "lower HR probability", treeApDrop: 0.000076, logisticCoefficient: -0.043066, medianFill: 0.0 },
    { feature: "is_order_3", group: "lineup_position", direction: "lower HR probability", treeApDrop: 0.000046, logisticCoefficient: -0.025941, medianFill: 0.0 },
    { feature: "order_pa_factor", group: "lineup_position", direction: "higher HR probability", treeApDrop: 0.000045, logisticCoefficient: 0.157032, medianFill: 1.0 },
    { feature: "is_top3_order", group: "lineup_position", direction: "lower HR probability", treeApDrop: 0.0, logisticCoefficient: -0.135515, medianFill: 0.0 },
    { feature: "is_bottom3_order", group: "lineup_position", direction: "higher HR probability", treeApDrop: 0.0, logisticCoefficient: 0.076297, medianFill: 0.0 },
    { feature: "is_order_2", group: "lineup_position", direction: "lower HR probability", treeApDrop: 0.0, logisticCoefficient: -0.074622, medianFill: 0.0 },
    { feature: "hitter_power_available", group: "hitter_power", direction: "higher HR probability", treeApDrop: 0.0, logisticCoefficient: 0.026748, medianFill: 1.0 },
    { feature: "pitcher_power_allowed_available", group: "pitcher_context", direction: "neutral", treeApDrop: 0.0, logisticCoefficient: 0.006444, medianFill: 1.0 },
    { feature: "hitter_wrc_plus", group: "hitter_power", direction: "neutral", treeApDrop: 0.0, logisticCoefficient: 0.0, medianFill: 0.0 },
    { feature: "hitter_split_wrc_plus", group: "hitter_power", direction: "neutral", treeApDrop: 0.0, logisticCoefficient: 0.0, medianFill: 0.0 },
    { feature: "pitcher_hr_fb_pct", group: "pitcher_context", direction: "neutral", treeApDrop: 0.0, logisticCoefficient: 0.0, medianFill: 0.0 },
    { feature: "pitcher_xfip", group: "pitcher_context", direction: "neutral", treeApDrop: 0.0, logisticCoefficient: 0.0, medianFill: 0.0 },
    { feature: "pitcher_fip", group: "pitcher_context", direction: "neutral", treeApDrop: 0.0, logisticCoefficient: 0.0, medianFill: 0.0 },
    { feature: "has_batting_order", group: "lineup_position", direction: "neutral", treeApDrop: 0.0, logisticCoefficient: 0.0, medianFill: 1.0 },
    { feature: "pitcher_hand_known", group: "pitcher_context", direction: "neutral", treeApDrop: 0.0, logisticCoefficient: 0.0, medianFill: 0.0 },
    { feature: "vs_lhp", group: "pitcher_context", direction: "neutral", treeApDrop: 0.0, logisticCoefficient: 0.0, medianFill: 0.0 },
    { feature: "vs_rhp", group: "pitcher_context", direction: "neutral", treeApDrop: 0.0, logisticCoefficient: 0.0, medianFill: 0.0 },
    { feature: "split_wrc_ratio", group: "hitter_power", direction: "neutral", treeApDrop: 0.0, logisticCoefficient: 0.0, medianFill: 0.0 },
    { feature: "pitcher_xfip_x_park", group: "pitcher_context, park_environment", direction: "neutral", treeApDrop: 0.0, logisticCoefficient: 0.0, medianFill: 0.0 },
    { feature: "is_order_1", group: "lineup_position", direction: "lower HR probability", treeApDrop: -0.00001, logisticCoefficient: -0.103847, medianFill: 0.0 },
    { feature: "pitcher_hr9_x_park", group: "pitcher_context, park_environment", direction: "neutral", treeApDrop: -0.000183, logisticCoefficient: -0.005398, medianFill: 1.0892 },
    { feature: "pitcher_hr_per_9", group: "pitcher_context", direction: "lower HR probability", treeApDrop: -0.000331, logisticCoefficient: -0.035397, medianFill: 1.1131 },
    { feature: "pitcher_era", group: "pitcher_context", direction: "higher HR probability", treeApDrop: -0.000354, logisticCoefficient: 0.030399, medianFill: 3.9 },
    { feature: "pitcher_bb_per_9", group: "pitcher_context", direction: "higher HR probability", treeApDrop: -0.000615, logisticCoefficient: 0.010551, medianFill: 2.81 },
    { feature: "pitcher_k_per_9", group: "pitcher_context", direction: "lower HR probability", treeApDrop: -0.001125, logisticCoefficient: -0.058174, medianFill: 8.34 },
  ] satisfies HomerunFeatureImpactRow[],
} as const;
