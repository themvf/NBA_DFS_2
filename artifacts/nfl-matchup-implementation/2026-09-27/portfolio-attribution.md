# Separate forecast and selection effects

All arms below use the same 29 legal candidate lineups, the same optimizer constraints and independent evaluation draws. This inspected union differs from the earlier 24-new-candidate comparison. Values are simulated mean best portfolio scores, not prize or win estimates.

| Entries | Original / baseline | Original / PFR | Reselection / baseline | Reselection / PFR |
|---|---:|---:|---:|---:|
| 1 | 139.69 | 139.33 | 156.67 | 156.75 |
| 3 | 162.27 | 161.97 | 172.17 | 172.06 |
| 20 | 182.08 | 181.88 | 186.71 | 186.76 |

The large change comes mainly from a different selection objective. The additional PFR forecast effect is small and can be negative. Independent player draws omit game dependence; this comparison does not establish improved accuracy or profitability.
