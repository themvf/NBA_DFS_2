# Today's DFS construction sensitivity comparison

Shadow research only. No production projection, lineup or contest entry was changed.

The current PFR shadow forecasts and the existing optimizer were compared on the same saved salary slate. Independent player samples omit football correlations, so these results do **not** establish realistic stack ceilings, tournament win probabilities or a profitable strategy.

Audited pool: 409 of 671 salary rows. Excluded players retain explicit reasons in the JSON.

| Illustrative entry mode | Old portfolio / old forecasts | Old portfolio / PFR shadow | Reselected portfolio / PFR shadow | Status |
|---|---:|---:|---:|---|
| single_entry (1) | 139.69 | 139.33 | 156.75 | shadow_comparison |
| three_entry (3) | 162.27 | 161.97 | 170.11 | shadow_comparison |
| multi_entry (20) | 182.08 | 181.88 | 186.76 | shadow_comparison |

Values are simulated average **best score among that mode's entries**, using evaluation draws that were not used to select the challenger. They are neither expected tournament winnings nor individual-player projection gains. Compare within a row only: more entries naturally provide more chances.

- Separate illustrative 1-, 3-, and 20-entry comparisons; actual contest size, fees, payouts and field are not supplied.
- Fixed explicit construction settings: $49,000 minimum salary, QB+one pass-catcher, one bring-back, two different players, no randomness, no individual exposure cap beyond one per lineup.
- Forecast models are independently sampled across players. These are construction sensitivity comparisons, not coherent lineup ceilings, calibrated win probabilities or ROI.
- All rows use the audited own-history subset; current-source replay is not a reconstruction of past executable decisions.
- No entry or export is authorized by these shadow results. Pre-lock availability and complete QA remain required.
