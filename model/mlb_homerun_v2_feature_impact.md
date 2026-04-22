# MLB Homerun v2 Feature Impact

Model version: `mlb_homerun_v2`
Feature source: `pregame_rolling`
Test split: `2025-07-01` onward, 23,559 rows, 2,664 HR-positive rows

## Group Impact

| Rank | Group | Avg Precision Drop |
|---:|---|---:|
| 1 | hitter_power | 0.036130 |
| 2 | park_environment | 0.017804 |
| 3 | lineup_position | 0.006769 |
| 4 | pitcher_context | 0.000353 |

## Feature Impact

Tree impact is permutation average-precision drop on the holdout set. Logistic coefficient is standardized; positive means the model associates a higher feature value with higher HR probability.

| Rank | Feature | Group | Direction | Tree AP Drop | Logistic Coef | Median Fill |
|---:|---|---|---|---:|---:|---:|
| 1 | `hitter_hr_x_order` | hitter_power | higher HR probability | 0.019889 | 0.090517 | 0.1033 |
| 2 | `hitter_hr_x_park` | hitter_power, park_environment | higher HR probability | 0.016039 | 0.009029 | 0.1016 |
| 3 | `hitter_pa_pg` | base_context | higher HR probability | 0.008110 | 0.144670 | 3.8333 |
| 4 | `batting_order` | lineup_position | lower HR probability | 0.004942 | -0.240240 | 5.0000 |
| 5 | `hitter_hr_pg` | hitter_power | higher HR probability | 0.004369 | 0.006468 | 0.1027 |
| 6 | `hitter_iso` | hitter_power | higher HR probability | 0.002811 | 0.138930 | 0.1540 |
| 7 | `hitter_hr_x_pitcher_hr9` | hitter_power, pitcher_context | higher HR probability | 0.002283 | 0.097999 | 0.1074 |
| 8 | `hitter_games` | base_context | higher HR probability | 0.001745 | 0.078491 | 60.0000 |
| 9 | `hitter_slg` | hitter_power | lower HR probability | 0.001281 | -0.134066 | 0.4000 |
| 10 | `park_hr_factor` | park_environment | higher HR probability | 0.001237 | 0.104019 | 1.0000 |
| 11 | `pitcher_whip` | pitcher_context | lower HR probability | 0.001006 | -0.056961 | 1.2400 |
| 12 | `pitcher_games` | pitcher_context | lower HR probability | 0.000814 | -0.042577 | 10.0000 |
| 13 | `hitter_iso_x_park` | hitter_power, park_environment | higher HR probability | 0.000766 | 0.036296 | 0.1510 |
| 14 | `park_runs_factor` | park_environment | lower HR probability | 0.000577 | -0.060752 | 1.0000 |
| 15 | `hitter_iso_x_pitcher_hr9` | hitter_power, pitcher_context | higher HR probability | 0.000511 | 0.017667 | 0.1666 |
| 16 | `pitcher_ip_pg` | pitcher_context | higher HR probability | 0.000399 | 0.044332 | 8.3294 |
| 17 | `is_home` | park_environment | neutral | 0.000085 | -0.005674 | 0.0000 |
| 18 | `is_order_4` | lineup_position | lower HR probability | 0.000076 | -0.043066 | 0.0000 |
| 19 | `is_order_3` | lineup_position | lower HR probability | 0.000046 | -0.025941 | 0.0000 |
| 20 | `order_pa_factor` | lineup_position | higher HR probability | 0.000045 | 0.157032 | 1.0000 |
| 21 | `is_top3_order` | lineup_position | lower HR probability | 0.000000 | -0.135515 | 0.0000 |
| 22 | `is_bottom3_order` | lineup_position | higher HR probability | 0.000000 | 0.076297 | 0.0000 |
| 23 | `is_order_2` | lineup_position | lower HR probability | 0.000000 | -0.074622 | 0.0000 |
| 24 | `hitter_power_available` | hitter_power | higher HR probability | 0.000000 | 0.026748 | 1.0000 |
| 25 | `pitcher_power_allowed_available` | pitcher_context | neutral | 0.000000 | 0.006444 | 1.0000 |
| 26 | `hitter_wrc_plus` | hitter_power | neutral | 0.000000 | 0.000000 | 0.0000 |
| 27 | `hitter_split_wrc_plus` | hitter_power | neutral | 0.000000 | 0.000000 | 0.0000 |
| 28 | `pitcher_hr_fb_pct` | pitcher_context | neutral | 0.000000 | 0.000000 | 0.0000 |
| 29 | `pitcher_xfip` | pitcher_context | neutral | 0.000000 | 0.000000 | 0.0000 |
| 30 | `pitcher_fip` | pitcher_context | neutral | 0.000000 | 0.000000 | 0.0000 |
| 31 | `has_batting_order` | lineup_position | neutral | 0.000000 | 0.000000 | 1.0000 |
| 32 | `pitcher_hand_known` | pitcher_context | neutral | 0.000000 | 0.000000 | 0.0000 |
| 33 | `vs_lhp` | pitcher_context | neutral | 0.000000 | 0.000000 | 0.0000 |
| 34 | `vs_rhp` | pitcher_context | neutral | 0.000000 | 0.000000 | 0.0000 |
| 35 | `split_wrc_ratio` | hitter_power | neutral | 0.000000 | 0.000000 | 0.0000 |
| 36 | `pitcher_xfip_x_park` | pitcher_context, park_environment | neutral | 0.000000 | 0.000000 | 0.0000 |
| 37 | `is_order_1` | lineup_position | lower HR probability | -0.000010 | -0.103847 | 0.0000 |
| 38 | `pitcher_hr9_x_park` | pitcher_context, park_environment | neutral | -0.000183 | -0.005398 | 1.0892 |
| 39 | `pitcher_hr_per_9` | pitcher_context | lower HR probability | -0.000331 | -0.035397 | 1.1131 |
| 40 | `pitcher_era` | pitcher_context | higher HR probability | -0.000354 | 0.030399 | 3.9000 |
| 41 | `pitcher_bb_per_9` | pitcher_context | higher HR probability | -0.000615 | 0.010551 | 2.8100 |
| 42 | `pitcher_k_per_9` | pitcher_context | lower HR probability | -0.001125 | -0.058174 | 8.3400 |
