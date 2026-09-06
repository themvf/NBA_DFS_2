# Volume/share versus the historical optimizer algorithm

Run `python -m ingest.nfl_dfs_volume_benchmark --source-root "C:/Docs/_AI Python Projects/NBADFS_v2"` from the isolated worktree. No database writes or provider requests occur. Weekly source hashes and the frozen study prediction hash are verified. The UI report records recipe hashes and the paired prediction digest; compressed paired rows are retained under ignored `artifacts/nfl_volume_share/`.

The comparator is the existing production historical algorithm replay with market inputs disabled, **not archived live projections**. Join keys are season, week, GSIS identity and game ID. Duplicate identities, inconsistent actual scores and baseline history cutoffs reaching the target week fail the run. Candidate forecasts and residuals use earlier weeks. All 3,977 candidate WR forecasts match; 779 additional baseline records are excluded from both sides.

| Season | Games | MAE historical → volume | 80% interval score historical → volume |
|---|---:|---:|---:|
| 2024 | 1,958 | 5.060 → 4.971 | 23.915 → 26.425 |
| 2025 | 2,019 | 4.706 → 4.515 | 21.277 → 24.100 |

Lower is better for both metrics. The candidate fails the mean-and-interval screen. It remains disabled. Overall means hide poor workload calibration: for receivers with at least seven prior weighted targets, 18.3% of 2025 outcomes exceed candidate P90 and 20.5% fall below P10. Low-workload ranges are too broad. A shared positional residual distribution is therefore a concrete next issue to address; this result does not validate any proposed replacement.

The existing Model Lab → volume/share page now displays paired comparisons and tail miss rates by prior workload. Both seasons were previously inspected. Missing/DNP observations are excluded, historical injuries are not modeled, and no contest profitability or lineup-level calibration is established.

The accompanying live availability fix recognizes WAS/WSH, LA/LAR, AZ/ARI and JAC/JAX aliases while preserving team-change rejection, age limits and position checks. A roster IR status now displays IR even when the separate injury field says Questionable. This does not resolve all roster provenance or starter-confirmation limitations.

Next: calibrate WR ranges using strictly prior workload evidence, rerun this frozen comparison, then consider opt-in forecasts and complete-lineup scenario scoring only if supported. Do not promote the new mean while silently attaching unvalidated ranges.
