# NFL DFS ownership — label provenance (nfl-ownership-provenance-v2)

Generated 2026-11-07 21:24 UTC by `python -m research.nfl_ownership_provenance`. Parameters: `{"mass_floor": 0.995, "mass_floor_status": "provisional", "version": "nfl-ownership-provenance-v2"}` (mass floor PROVISIONAL until the scorecard is registered).

Classes per docs/nfl-ownership-scorecard-v1.md section 3. One label per (contest, player, slot): `overall` everywhere, plus `CPT` and `FLEX` in Showdown. Salary-listed players with no ownership row are labelled too (reconciled_zero when the record is complete, else unresolved); contests with no linked salary pool cannot enumerate them. `complete` = DK export, or stat-api with mass >= floor AND full standings fetched.

Persisted to `nfl_ownership_label_provenance`: 13669 new rows this run; 13669 rows under `nfl-ownership-provenance-v2` in total.

## Counts by class per contest

| contest | wk | format | import | entries | users fetched | mass | full standings | complete | slot | observed | reconciled_zero | synthesized_zero | unresolved |
|---|---:|---|---|---:|---:|---:|---|---|---|---:|---:|---:|---:|
| 195390927 | ? | showdown | stat_api | 88,235 | 100 | 596.41/600 | no | no | overall | 34 | 0 | 0 | 1 |
| 195390927 | ? | showdown | stat_api | 88,235 | 100 | 596.41/600 | no | no | CPT | 26 | 0 | 8 | 1 |
| 195390927 | ? | showdown | stat_api | 88,235 | 100 | 596.41/600 | no | no | FLEX | 34 | 0 | 0 | 1 |
| 195526287 | ? | showdown | stat_api | 88,235 | 100 | 598.59/600 | no | yes | overall | 31 | 0 | 0 | 1 |
| 195526287 | ? | showdown | stat_api | 88,235 | 100 | 598.59/600 | no | yes | CPT | 25 | 0 | 6 | 1 |
| 195526287 | ? | showdown | stat_api | 88,235 | 100 | 598.59/600 | no | yes | FLEX | 31 | 0 | 0 | 1 |
| 195677825 | ? | showdown | stat_api | 88,235 | 100 | 599.58/600 | yes | yes | overall | 38 | 0 | 0 | 0 |
| 195677825 | ? | showdown | stat_api | 88,235 | 100 | 599.58/600 | yes | yes | CPT | 23 | 0 | 15 | 0 |
| 195677825 | ? | showdown | stat_api | 88,235 | 100 | 599.58/600 | yes | yes | FLEX | 38 | 0 | 0 | 0 |
| 193028206 | 1 | classic | stat_api | 832,342 | 100 | 896.58/900 | yes | yes | overall | 205 | 514 | 0 | 0 |
| 193391013 | 1 | showdown | stat_api | 126,020 | 100 | 599.41/600 | yes | yes | overall | 33 | 35 | 0 | 0 |
| 193391013 | 1 | showdown | stat_api | 126,020 | 100 | 599.41/600 | yes | yes | CPT | 26 | 35 | 7 | 0 |
| 193391013 | 1 | showdown | stat_api | 126,020 | 100 | 599.41/600 | yes | yes | FLEX | 33 | 35 | 0 | 0 |
| 195648006 | 2 | classic | dk_export | 317,082 | - | 898.97/900 | no | yes | overall | 527 | 143 | 0 | 0 |
| 195648007 | 2 | classic | stat_api | 172,761 | 100 | 896.07/900 | yes | yes | overall | 212 | 458 | 0 | 0 |
| 195786068 | 2 | showdown | stat_api | 88,235 | 100 | 599.29/600 | yes | yes | overall | 31 | 22 | 0 | 0 |
| 195786068 | 2 | showdown | stat_api | 88,235 | 100 | 599.29/600 | yes | yes | CPT | 24 | 22 | 7 | 0 |
| 195786068 | 2 | showdown | stat_api | 88,235 | 100 | 599.29/600 | yes | yes | FLEX | 31 | 22 | 0 | 0 |
| 195786073 | 2 | showdown | dk_export | 47,562 | - | 596.10/600 | no | yes | overall | 52 | 1 | 0 | 0 |
| 195786073 | 2 | showdown | dk_export | 47,562 | - | 596.10/600 | no | yes | CPT | 37 | 16 | 0 | 0 |
| 195786073 | 2 | showdown | dk_export | 47,562 | - | 596.10/600 | no | yes | FLEX | 52 | 1 | 0 | 0 |
| 195905122 | 3 | classic | stat_api | 161,764 | 100 | 895.79/900 | yes | yes | overall | 203 | 468 | 0 | 0 |
| 195943234 | 3 | showdown | stat_api | 88,235 | 100 | 599.36/600 | yes | yes | overall | 32 | 21 | 0 | 0 |
| 195943234 | 3 | showdown | stat_api | 88,235 | 100 | 599.36/600 | yes | yes | CPT | 27 | 21 | 5 | 0 |
| 195943234 | 3 | showdown | stat_api | 88,235 | 100 | 599.36/600 | yes | yes | FLEX | 32 | 21 | 0 | 0 |
| 195943238 | 3 | showdown | dk_export | 83,234 | - | 596.78/600 | yes | yes | overall | 51 | 2 | 0 | 0 |
| 195943238 | 3 | showdown | dk_export | 83,234 | - | 596.78/600 | yes | yes | CPT | 40 | 13 | 0 | 0 |
| 195943238 | 3 | showdown | dk_export | 83,234 | - | 596.78/600 | yes | yes | FLEX | 51 | 2 | 0 | 0 |
| 196036238 | 3 | showdown | stat_api | 88,235 | 100 | 599.83/600 | yes | yes | overall | 32 | 24 | 0 | 0 |
| 196036238 | 3 | showdown | stat_api | 88,235 | 100 | 599.83/600 | yes | yes | CPT | 23 | 24 | 9 | 0 |
| 196036238 | 3 | showdown | stat_api | 88,235 | 100 | 599.83/600 | yes | yes | FLEX | 32 | 24 | 0 | 0 |
| 196128919 | 3 | classic | dk_export | 7,134 | - | 898.88/900 | yes | yes | overall | 287 | 384 | 0 | 0 |
| 196151357 | 4 | classic | stat_api | 161,764 | 100 | 895.44/900 | yes | no | overall | 183 | 0 | 0 | 446 |
| 196187074 | 4 | showdown | stat_api | 88,235 | 100 | 599.92/600 | yes | yes | overall | 35 | 16 | 0 | 0 |
| 196187074 | 4 | showdown | stat_api | 88,235 | 100 | 599.92/600 | yes | yes | CPT | 22 | 16 | 13 | 0 |
| 196187074 | 4 | showdown | stat_api | 88,235 | 100 | 599.92/600 | yes | yes | FLEX | 35 | 16 | 0 | 0 |
| 196204783 | 4 | showdown | dk_export | 22,136 | - | 594.84/600 | yes | yes | overall | 50 | 6 | 0 | 0 |
| 196204783 | 4 | showdown | dk_export | 22,136 | - | 594.84/600 | yes | yes | CPT | 36 | 20 | 0 | 0 |
| 196204783 | 4 | showdown | dk_export | 22,136 | - | 594.84/600 | yes | yes | FLEX | 50 | 6 | 0 | 0 |
| 196280117 | 4 | showdown | dk_export | 8,917 | - | 598.04/600 | yes | yes | overall | 38 | 13 | 0 | 0 |
| 196280117 | 4 | showdown | dk_export | 8,917 | - | 598.04/600 | yes | yes | CPT | 26 | 25 | 0 | 0 |
| 196280117 | 4 | showdown | dk_export | 8,917 | - | 598.04/600 | yes | yes | FLEX | 37 | 14 | 0 | 0 |
| 196285055 | 4 | showdown | dk_export | 35,671 | - | 596.90/600 | no | yes | overall | 46 | 4 | 0 | 0 |
| 196285055 | 4 | showdown | dk_export | 35,671 | - | 596.90/600 | no | yes | CPT | 32 | 18 | 0 | 0 |
| 196285055 | 4 | showdown | dk_export | 35,671 | - | 596.90/600 | no | yes | FLEX | 46 | 4 | 0 | 0 |
| 196285155 | 4 | showdown | stat_api | 88,235 | 100 | 598.68/600 | yes | yes | overall | 29 | 26 | 0 | 1 |
| 196285155 | 4 | showdown | stat_api | 88,235 | 100 | 598.68/600 | yes | yes | CPT | 22 | 26 | 8 | 0 |
| 196285155 | 4 | showdown | stat_api | 88,235 | 100 | 598.68/600 | yes | yes | FLEX | 29 | 26 | 1 | 0 |
| 196285178 | 4 | showdown | dk_export | 8,917 | - | 594.50/600 | yes | yes | overall | 44 | 12 | 0 | 0 |
| 196285178 | 4 | showdown | dk_export | 8,917 | - | 594.50/600 | yes | yes | CPT | 28 | 28 | 0 | 0 |
| 196285178 | 4 | showdown | dk_export | 8,917 | - | 594.50/600 | yes | yes | FLEX | 44 | 12 | 0 | 0 |
| 196403752 | 5 | classic | stat_api | 161,557 | 100 | 897.83/900 | yes | yes | overall | 181 | 406 | 0 | 0 |
| 196438550 | 5 | showdown | stat_api | 88,235 | 100 | 599.84/600 | yes | yes | overall | 33 | 19 | 0 | 0 |
| 196438550 | 5 | showdown | stat_api | 88,235 | 100 | 599.84/600 | yes | yes | CPT | 23 | 19 | 10 | 0 |
| 196438550 | 5 | showdown | stat_api | 88,235 | 100 | 599.84/600 | yes | yes | FLEX | 33 | 19 | 0 | 0 |
| 196480476 | 5 | showdown | stat_api | 500 | 100 | 600.00/600 | yes | yes | overall | 27 | 22 | 0 | 0 |
| 196480476 | 5 | showdown | stat_api | 500 | 100 | 600.00/600 | yes | yes | CPT | 15 | 22 | 12 | 0 |
| 196480476 | 5 | showdown | stat_api | 500 | 100 | 600.00/600 | yes | yes | FLEX | 27 | 22 | 0 | 0 |
| dfsdb-1e8fbcd7-6f69-47ef-8417-d8d3f4acbbed | 5 | showdown | dfsdb | 47,562 | - | 496.48/600 | no | no | overall | 0 | 0 | 0 | 50 |
| dfsdb-1e8fbcd7-6f69-47ef-8417-d8d3f4acbbed | 5 | showdown | dfsdb | 47,562 | - | 496.48/600 | no | no | CPT | 0 | 0 | 0 | 50 |
| dfsdb-1e8fbcd7-6f69-47ef-8417-d8d3f4acbbed | 5 | showdown | dfsdb | 47,562 | - | 496.48/600 | no | no | FLEX | 50 | 0 | 0 | 0 |
| dfsdb-3605c6fb-b8ce-46bb-b829-9396d0982010 | 5 | showdown | dfsdb | 88,235 | - | 499.04/600 | no | no | overall | 0 | 0 | 0 | 50 |
| dfsdb-3605c6fb-b8ce-46bb-b829-9396d0982010 | 5 | showdown | dfsdb | 88,235 | - | 499.04/600 | no | no | CPT | 0 | 0 | 0 | 50 |
| dfsdb-3605c6fb-b8ce-46bb-b829-9396d0982010 | 5 | showdown | dfsdb | 88,235 | - | 499.04/600 | no | no | FLEX | 50 | 0 | 0 | 0 |
| dfsdb-3b42531a-94ed-461b-b420-f417ebb28ec6 | 5 | showdown | dfsdb | 83,234 | - | 497.91/600 | no | no | overall | 0 | 0 | 0 | 50 |
| dfsdb-3b42531a-94ed-461b-b420-f417ebb28ec6 | 5 | showdown | dfsdb | 83,234 | - | 497.91/600 | no | no | CPT | 0 | 0 | 0 | 50 |
| dfsdb-3b42531a-94ed-461b-b420-f417ebb28ec6 | 5 | showdown | dfsdb | 83,234 | - | 497.91/600 | no | no | FLEX | 50 | 0 | 0 | 0 |
| dfsdb-5449aea7-db27-410d-8c54-91df2ad90241 | 5 | showdown | dfsdb | 23,781 | - | 493.54/600 | no | no | overall | 0 | 0 | 0 | 47 |
| dfsdb-5449aea7-db27-410d-8c54-91df2ad90241 | 5 | showdown | dfsdb | 23,781 | - | 493.54/600 | no | no | CPT | 0 | 0 | 0 | 47 |
| dfsdb-5449aea7-db27-410d-8c54-91df2ad90241 | 5 | showdown | dfsdb | 23,781 | - | 493.54/600 | no | no | FLEX | 47 | 0 | 0 | 0 |
| dfsdb-8ac82129-f1d6-4bc9-af29-5a6219b23483 | 5 | showdown | dfsdb | 237,812 | - | 498.42/600 | no | no | overall | 0 | 0 | 0 | 49 |
| dfsdb-8ac82129-f1d6-4bc9-af29-5a6219b23483 | 5 | showdown | dfsdb | 237,812 | - | 498.42/600 | no | no | CPT | 0 | 0 | 0 | 49 |
| dfsdb-8ac82129-f1d6-4bc9-af29-5a6219b23483 | 5 | showdown | dfsdb | 237,812 | - | 498.42/600 | no | no | FLEX | 49 | 0 | 0 | 0 |
| dfsdb-9a830c4f-7351-4575-a75b-1237382766fa | 5 | showdown | dfsdb | 47,562 | - | 497.59/600 | no | no | overall | 0 | 0 | 0 | 47 |
| dfsdb-9a830c4f-7351-4575-a75b-1237382766fa | 5 | showdown | dfsdb | 47,562 | - | 497.59/600 | no | no | CPT | 0 | 0 | 0 | 47 |
| dfsdb-9a830c4f-7351-4575-a75b-1237382766fa | 5 | showdown | dfsdb | 47,562 | - | 497.59/600 | no | no | FLEX | 47 | 0 | 0 | 0 |
| dfsdb-9e8a781e-3c88-498c-992e-5c5d5a665160 | 5 | showdown | dfsdb | 118,906 | - | 498.34/600 | no | no | overall | 0 | 0 | 0 | 50 |
| dfsdb-9e8a781e-3c88-498c-992e-5c5d5a665160 | 5 | showdown | dfsdb | 118,906 | - | 498.34/600 | no | no | CPT | 0 | 0 | 0 | 50 |
| dfsdb-9e8a781e-3c88-498c-992e-5c5d5a665160 | 5 | showdown | dfsdb | 118,906 | - | 498.34/600 | no | no | FLEX | 50 | 0 | 0 | 0 |
| dfsdb-cf22c10c-7c06-4de9-9f35-57355b2da209 | 5 | showdown | dfsdb | 71,343 | - | 498.59/600 | no | no | overall | 0 | 0 | 0 | 47 |
| dfsdb-cf22c10c-7c06-4de9-9f35-57355b2da209 | 5 | showdown | dfsdb | 71,343 | - | 498.59/600 | no | no | CPT | 0 | 0 | 0 | 47 |
| dfsdb-cf22c10c-7c06-4de9-9f35-57355b2da209 | 5 | showdown | dfsdb | 71,343 | - | 498.59/600 | no | no | FLEX | 47 | 0 | 0 | 0 |
| 196653900 | 6 | classic | stat_api | 158,904 | 84 | 894.97/900 | no | no | overall | 162 | 0 | 0 | 364 |
| 196689718 | 6 | showdown | stat_api | 84,384 | 100 | 599.80/600 | yes | yes | overall | 34 | 22 | 0 | 0 |
| 196689718 | 6 | showdown | stat_api | 84,384 | 100 | 599.80/600 | yes | yes | CPT | 25 | 22 | 9 | 0 |
| 196689718 | 6 | showdown | stat_api | 84,384 | 100 | 599.80/600 | yes | yes | FLEX | 34 | 22 | 0 | 0 |
| 196786690 | 6 | showdown | stat_api | 87,465 | 96 | 599.18/600 | no | yes | overall | 32 | 28 | 0 | 0 |
| 196786690 | 6 | showdown | stat_api | 87,465 | 96 | 599.18/600 | no | yes | CPT | 25 | 28 | 7 | 0 |
| 196786690 | 6 | showdown | stat_api | 87,465 | 96 | 599.18/600 | no | yes | FLEX | 32 | 28 | 0 | 0 |
| 196786695 | 6 | showdown | dk_export | 47,562 | - | 596.50/600 | yes | yes | overall | 56 | 4 | 0 | 0 |
| 196786695 | 6 | showdown | dk_export | 47,562 | - | 596.50/600 | yes | yes | CPT | 45 | 15 | 0 | 0 |
| 196786695 | 6 | showdown | dk_export | 47,562 | - | 596.50/600 | yes | yes | FLEX | 55 | 5 | 0 | 0 |
| 196905290 | 7 | classic | stat_api | 147,058 | 88 | 897.76/900 | no | yes | overall | 171 | 347 | 0 | 0 |
| 196947072 | 7 | showdown | stat_api | 73,529 | 91 | 599.49/600 | no | yes | overall | 28 | 26 | 0 | 0 |
| 196947072 | 7 | showdown | stat_api | 73,529 | 91 | 599.49/600 | no | yes | CPT | 23 | 26 | 5 | 0 |
| 196947072 | 7 | showdown | stat_api | 73,529 | 91 | 599.49/600 | no | yes | FLEX | 28 | 26 | 0 | 0 |
| 196947078 | 7 | showdown | dk_export | 47,562 | - | 596.70/600 | yes | yes | overall | 49 | 6 | 0 | 0 |
| 196947078 | 7 | showdown | dk_export | 47,562 | - | 596.70/600 | yes | yes | CPT | 33 | 22 | 0 | 0 |
| 196947078 | 7 | showdown | dk_export | 47,562 | - | 596.70/600 | yes | yes | FLEX | 48 | 7 | 0 | 0 |
| 197055073 | 7 | showdown | stat_api | 73,529 | 99 | 599.66/600 | no | yes | overall | 31 | 24 | 0 | 0 |
| 197055073 | 7 | showdown | stat_api | 73,529 | 99 | 599.66/600 | no | yes | CPT | 22 | 24 | 9 | 0 |
| 197055073 | 7 | showdown | stat_api | 73,529 | 99 | 599.66/600 | no | yes | FLEX | 31 | 24 | 0 | 0 |
| 195745385 | 8 | classic | dk_export | 4,444 | - | 899.95/900 | yes | yes | overall | 174 | 388 | 0 | 0 |
| 197176675 | 8 | classic | dk_export | 147,058 | - | 899.41/900 | yes | yes | overall | 335 | 227 | 0 | 0 |
| 197189585 | 8 | classic | dk_export | 250 | - | 898.40/900 | yes | yes | overall | 131 | 431 | 0 | 0 |
| 197208268 | 8 | showdown | stat_api | 73,529 | 98 | 599.43/600 | no | yes | overall | 33 | 20 | 0 | 0 |
| 197208268 | 8 | showdown | stat_api | 73,529 | 98 | 599.43/600 | no | yes | CPT | 23 | 20 | 10 | 0 |
| 197208268 | 8 | showdown | stat_api | 73,529 | 98 | 599.43/600 | no | yes | FLEX | 33 | 20 | 0 | 0 |
| 197208280 | 8 | showdown | dk_export | 5,945 | - | 597.67/600 | yes | yes | overall | 40 | 13 | 0 | 0 |
| 197208280 | 8 | showdown | dk_export | 5,945 | - | 597.67/600 | yes | yes | CPT | 24 | 29 | 0 | 0 |
| 197208280 | 8 | showdown | dk_export | 5,945 | - | 597.67/600 | yes | yes | FLEX | 39 | 14 | 0 | 0 |
| 197220774 | 8 | classic | dk_export | 198,176 | - | 898.99/900 | yes | yes | overall | 415 | 147 | 0 | 0 |
| 197302803 | 8 | showdown | dk_export | 3,567 | - | 598.83/600 | yes | yes | overall | 39 | 19 | 0 | 0 |
| 197302803 | 8 | showdown | dk_export | 3,567 | - | 598.83/600 | yes | yes | CPT | 26 | 32 | 0 | 0 |
| 197302803 | 8 | showdown | dk_export | 3,567 | - | 598.83/600 | yes | yes | FLEX | 38 | 20 | 0 | 0 |
| 197302895 | 8 | showdown | stat_api | 73,529 | 93 | 599.80/600 | no | yes | overall | 32 | 23 | 0 | 0 |
| 197302895 | 8 | showdown | stat_api | 73,529 | 93 | 599.80/600 | no | yes | CPT | 23 | 23 | 9 | 0 |
| 197302895 | 8 | showdown | stat_api | 73,529 | 93 | 599.80/600 | no | yes | FLEX | 32 | 23 | 0 | 0 |
| 197302907 | 8 | showdown | dk_export | 5,945 | - | 597.56/600 | yes | yes | overall | 40 | 15 | 0 | 0 |
| 197302907 | 8 | showdown | dk_export | 5,945 | - | 597.56/600 | yes | yes | CPT | 24 | 31 | 0 | 0 |
| 197302907 | 8 | showdown | dk_export | 5,945 | - | 597.56/600 | yes | yes | FLEX | 40 | 15 | 0 | 0 |
| 197450244 | 9 | showdown | stat_api | 73,105 | 80 | 599.57/600 | no | yes | overall | 33 | 21 | 0 | 0 |
| 197450244 | 9 | showdown | stat_api | 73,105 | 80 | 599.57/600 | no | yes | CPT | 23 | 21 | 10 | 0 |
| 197450244 | 9 | showdown | stat_api | 73,105 | 80 | 599.57/600 | no | yes | FLEX | 33 | 21 | 0 | 0 |

Totals: overall: observed 4269, reconciled_zero 4357, synthesized_zero 0, unresolved 1203; CPT: observed 771, reconciled_zero 598, synthesized_zero 159, unresolved 392; FLEX: observed 1468, reconciled_zero 449, synthesized_zero 1, unresolved 2

## Bases (why each class)

| basis | class | rows |
|---|---|---:|
| dfsdb_athlete_row | observed | 390 |
| dfsdb_flex_only | unresolved | 390 |
| dfsdb_no_captain_share | unresolved | 390 |
| dk_export_absent | reconciled_zero | 2005 |
| dk_export_row | observed | 3225 |
| dk_export_slot_absent | reconciled_zero | 159 |
| no_player_identity | unresolved | 6 |
| statapi_absent_below_mass_floor | unresolved | 810 |
| statapi_absent_complete | reconciled_zero | 2467 |
| statapi_absent_complete_top100 | reconciled_zero | 773 |
| statapi_captain_only_flex_filled | synthesized_zero | 1 |
| statapi_captain_only_overall_is_lower_bound | unresolved | 1 |
| statapi_cpt_seat | observed | 420 |
| statapi_flex_seat | observed | 578 |
| statapi_no_fetched_captain | synthesized_zero | 159 |
| statapi_overall_minus_cpt | observed | 419 |
| statapi_overall_minus_unseen_captain | observed | 159 |
| statapi_seat | observed | 1317 |

## Notes and verification per contest

- **195390927**: no linked salary pool: salary-listed players without a label cannot be enumerated / 1 ownership row(s) not in any stored lineup: 
- **195526287**: no linked salary pool: salary-listed players without a label cannot be enumerated / 1 ownership row(s) not in any stored lineup: 
- **195677825**: no linked salary pool: salary-listed players without a label cannot be enumerated
- **193391013**: 1 captained player(s) stored with CPT 0 (rounded; kept observed): hassanhaskins
- **dfsdb-1e8fbcd7-6f69-47ef-8417-d8d3f4acbbed**: no linked salary pool: salary-listed players without a label cannot be enumerated
- **dfsdb-3605c6fb-b8ce-46bb-b829-9396d0982010**: no linked salary pool: salary-listed players without a label cannot be enumerated
- **dfsdb-3b42531a-94ed-461b-b420-f417ebb28ec6**: no linked salary pool: salary-listed players without a label cannot be enumerated
- **dfsdb-5449aea7-db27-410d-8c54-91df2ad90241**: no linked salary pool: salary-listed players without a label cannot be enumerated
- **dfsdb-8ac82129-f1d6-4bc9-af29-5a6219b23483**: no linked salary pool: salary-listed players without a label cannot be enumerated
- **dfsdb-9a830c4f-7351-4575-a75b-1237382766fa**: no linked salary pool: salary-listed players without a label cannot be enumerated
- **dfsdb-9e8a781e-3c88-498c-992e-5c5d5a665160**: no linked salary pool: salary-listed players without a label cannot be enumerated
- **dfsdb-cf22c10c-7c06-4de9-9f35-57355b2da209**: no linked salary pool: salary-listed players without a label cannot be enumerated
- **196689718**: 1 captained player(s) stored with CPT 0 (rounded; kept observed): adamprentice
- **196947078**: 1 ownership name(s) not in the linked salary pool: ryanmiller
