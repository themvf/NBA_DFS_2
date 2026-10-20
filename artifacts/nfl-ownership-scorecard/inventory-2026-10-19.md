# NFL DFS ownership — frozen forecast inventory (nfl-ownership-inventory-v1)

Generated 2026-10-20 00:19 UTC by `python -m research.nfl_ownership_inventory --season 2026`. Read-only.

Definitions: canonical horizon = the LAST capture at or before first kickoff minus 20 minutes; early = the FIRST pregame capture inside 24 h of first kickoff; both are marked per source. A capture holds a forecast when at least one player has a numeric `ownPct` and a projection. Pool captures are grouped by `capture_key` (one minute) and cover only the games due, so `games` = games in the capture / games on the slate. `dkStatus` counts players whose record carries the DK status KEY (a null value is still a capture of "no status"). Optimizer runs are OWNER builds only (no `researchStudy`, no `research_*` status, no system slates).

## 1. Distinct slates

| week | format | slates | with canonical forecast (any source) | of which pool capture covers every game | with an imported contest | contest slates without a canonical forecast |
|---:|---|---:|---:|---:|---:|---:|
| 1 | classic | 1 | 0 | 0 | 1 | 1 |
| 1 | showdown | 1 | 0 | 0 | 1 | 1 |
| 2 | classic | 1 | 0 | 0 | 1 | 1 |
| 2 | showdown | 2 | 0 | 0 | 1 | 1 |
| 3 | classic | 1 | 0 | 0 | 1 | 1 |
| 3 | showdown | 2 | 1 | 1 | 2 | 1 |
| 4 | classic | 1 | 1 | 1 | 1 | 0 |
| 4 | showdown | 4 | 4 | 4 | 4 | 0 |
| 5 | classic | 1 | 1 | 1 | 1 | 0 |
| 5 | showdown | 4 | 4 | 4 | 2 | 0 |
| 6 | classic | 1 | 1 | 1 | 0 | 0 |
| 6 | showdown | 4 | 4 | 4 | 1 | 0 |
| all | all | 23 | 16 | 16 | 16 | 6 |

Captures: 1018 rows over 23 distinct slates (pool 899, owner runs 116, ghost freezes 3).

Pool captures relative to the slate's FIRST kickoff: 57 at or before kickoff minus 20 min, 479 inside the final 20 minutes, 363 at or after first kickoff (later games of a multi-game slate). The scheduler's final-window ticks (`poolCaptureDue`: remaining <= 20 min) are by construction after the canonical cutoff (kickoff minus 20 min), so a pool capture can be canonical only when it is the 24-hour capture or an on-upload capture.

## 2. Every imported contest

| contest | name | format | wk | entries | import | slate link | slate first kickoff (UTC) | canonical forecast | source / capture | min before kickoff | ownership version | anchored / players |
|---|---|---|---:|---:|---|---|---|---|---|---:|---|---|
| 193028206 | NFL $3.5M Fantasy Football Millionaire [$1M to 1st] | classic | 1 | 832,342 | stat-api | slate_upload_id | 2026-09-13 17:00 | no_frozen_forecast: the canonical-horizon capture of every source holds no ownPct (an earlier optimizer_run 4875c71f at 09-04 17:18 does) | - | - | - | - |
| 193391013 | NFL Showdown $2.25M Wednesday Kickoff Millionaire [$1M to 1s | showdown | 1 | 126,020 | stat-api | slate_upload_id | 2026-09-10 00:20 | no_frozen_forecast: no capture before kickoff minus 20 min holds ownPct | - | - | - | - |
| 195390927 | NFL $1.5M Thursday Night Showdown [$500K to 1st] (SF vs LAR) | showdown | 1 | 88,235 | stat-api | no_salary_upload (SF@LAR week 1) | - | no slate: no_salary_upload (SF@LAR week 1) | - | - | - | - |
| 195526287 | NFL Showdown $1.5M Monday Night Showdown [$500K to 1st] (DEN | showdown | 1 | 88,235 | stat-api | no_salary_upload (DEN@KC week 1) | - | no slate: no_salary_upload (DEN@KC week 1) | - | - | - | - |
| 195648006 | contest-standings-195648006 | classic | 2 | 317,082 | DK export | slate_upload_id | 2026-09-20 17:00 | no_frozen_forecast: no capture before kickoff minus 20 min holds ownPct | - | - | - | - |
| 195648007 | NFL $3M Fantasy Football Millionaire [$1M to 1st] | classic | 2 | 172,761 | stat-api | slate_upload_id | 2026-09-20 17:00 | no_frozen_forecast: no capture before kickoff minus 20 min holds ownPct | - | - | - | - |
| 195677825 | NFL Showdown $1.5M Thursday Night Showdown [$500K to 1st] (D | showdown | 2 | 88,235 | stat-api | no_salary_upload (DET@BUF week 2) | - | no slate: no_salary_upload (DET@BUF week 2) | - | - | - | - |
| 195786068 | NFL Showdown $1.5M Monday Night Showdown [$500K to 1st] (NYG | showdown | 2 | 88,235 | stat-api | slate_upload_id | 2026-09-22 00:15 | no_frozen_forecast: no capture before kickoff minus 20 min holds ownPct | - | - | - | - |
| 195786073 | contest-standings-195786073 | showdown | 2 | 47,562 | DK export | slate_upload_id | 2026-09-22 00:15 | no_frozen_forecast: no capture before kickoff minus 20 min holds ownPct | - | - | - | - |
| 195905122 | NFL $2.75M Fantasy Football Millionaire [$1M to 1st] | classic | 3 | 161,764 | stat-api | slate_upload_id | 2026-09-27 17:00 | no_frozen_forecast: no capture before kickoff minus 20 min holds ownPct | - | - | - | - |
| 195943234 | NFL Showdown $1.5M Thursday Night Showdown [$500K to 1st] (A | showdown | 3 | 88,235 | stat-api | slate_upload_id | 2026-09-25 00:15 | no_frozen_forecast: no capture before kickoff minus 20 min holds ownPct | - | - | - | - |
| 195943238 | contest-standings-195943238.csv | showdown | 3 | 83,234 | DK export | slate_upload_id | 2026-09-25 00:15 | no_frozen_forecast: no capture before kickoff minus 20 min holds ownPct | - | - | - | - |
| 196036238 | NFL Showdown $1.5M Monday Night Showdown [$500K to 1st] (PHI | showdown | 3 | 88,235 | stat-api | slate_upload_id | 2026-09-29 00:15 | canonical | pool_capture e455e66a:live:2026-09-28 21:01:00+00 | 193 | nfl-ownership-prior-v1 | 0/56 |
| 196128919 | contest-standings-196128919.csv | classic | 3 | 7,134 | DK export | slate_upload_id | 2026-09-27 17:00 | no_frozen_forecast: no capture before kickoff minus 20 min holds ownPct | - | - | - | - |
| 196151357 | NFL $2.75M Fantasy Football Millionaire [$1M to 1st] | classic | 4 | 161,764 | stat-api | slate_upload_id | 2026-10-04 17:00 | canonical | pool_capture 23253360:live:2026-10-04 16:04:00+00 | 55 | nfl-ownership-prior-v2 | 0/629 |
| 196187074 | NFL $1.5M Thursday Night Showdown [$500K to 1st] (PIT @ CLE) | showdown | 4 | 88,235 | stat-api | slate_upload_id | 2026-10-02 00:15 | canonical | pool_capture 8e7cf26c:live:2026-10-01 22:13:00+00 | 121 | nfl-ownership-prior-v2 | 0/51 |
| 196204783 | contest-standings-196204783.csv | showdown | 4 | 22,136 | DK export | slate_upload_id | 2026-10-04 13:30 | canonical | pool_capture 76e25897:live:2026-10-04 08:25:00+00 | 304 | nfl-ownership-prior-v2 | 0/56 |
| 196280117 | contest-standings-196280117.csv | showdown | 4 | 8,917 | DK export | slate_upload_id | 2026-10-02 00:15 | canonical | pool_capture 8e7cf26c:live:2026-10-01 22:13:00+00 | 121 | nfl-ownership-prior-v2 | 0/51 |
| 196285055 | contest-standings-196285055 | showdown | 4 | 35,671 | DK export | slate_upload_id | 2026-10-05 00:20 | canonical | pool_capture 35ec7c6c:live:2026-10-04 22:13:00+00 | 126 | nfl-ownership-prior-v2 | 0/50 |
| 196285155 | NFL Showdown $1.5M Monday Night Showdown [$500K to 1st] (ATL | showdown | 4 | 88,235 | stat-api | slate_upload_id | 2026-10-06 00:15 | canonical | pool_capture 815a5a5d:live:2026-10-05 21:52:00+00 | 142 | nfl-ownership-prior-v3 | 0/56 |
| 196285178 | contest-standings-196285178.csv | showdown | 4 | 8,917 | DK export | slate_upload_id | 2026-10-06 00:15 | canonical | pool_capture 815a5a5d:live:2026-10-05 21:52:00+00 | 142 | nfl-ownership-prior-v3 | 0/56 |
| 196403752 | NFL $2.75M Fantasy Football Millionaire [$1M to 1st, $100K L | classic | 5 | 161,557 | stat-api | slate_upload_id | 2026-10-11 17:00 | canonical | pool_capture f1683510:live:2026-10-11 15:07:00+00 | 112 | nfl-ownership-prior-v3 | 0/587 |
| 196438550 | NFL Showdown $1.5M Thursday Night Showdown [$500K to 1st] (T | showdown | 5 | 88,235 | stat-api | slate_upload_id | 2026-10-09 00:15 | canonical | pool_capture 96614eda:live:2026-10-08 00:15:00+00 | 1439 | nfl-ownership-prior-v3 | 0/52 |
| 196480476 | NFL Showdown $2M MEGA Millionaire [$1M to 1st] (BUF @ LAR) | showdown | 5 | 500 | stat-api | slate_upload_id | 2026-10-13 00:15 | canonical | pool_capture 9f28df85:live:2026-10-12 22:35:00+00 | 99 | nfl-ownership-prior-v3 | 17/49 |
| dfsdb-1e8fbcd7-6f69-47ef-8417-d8d3f4acbbed | NFL Showdown $4K Dime Package [Just $0.10!] (TB @ DAL) | showdown | 5 | 47,562 | dfsdb | inferred_from_contest_name | 2026-10-09 00:15 | canonical | pool_capture 96614eda:live:2026-10-08 00:15:00+00 | 1439 | nfl-ownership-prior-v3 | 0/52 |
| dfsdb-3605c6fb-b8ce-46bb-b829-9396d0982010 | NFL Showdown $1.5M Thursday Night Showdown [$500K to 1st] (T | showdown | 5 | 88,235 | dfsdb | inferred_from_contest_name | 2026-10-09 00:15 | canonical | pool_capture 96614eda:live:2026-10-08 00:15:00+00 | 1439 | nfl-ownership-prior-v3 | 0/52 |
| dfsdb-3b42531a-94ed-461b-b420-f417ebb28ec6 | NFL Showdown $70K First Down [20 Entry Max] (TB @ DAL) | showdown | 5 | 83,234 | dfsdb | inferred_from_contest_name | 2026-10-09 00:15 | canonical | pool_capture 96614eda:live:2026-10-08 00:15:00+00 | 1439 | nfl-ownership-prior-v3 | 0/52 |
| dfsdb-5449aea7-db27-410d-8c54-91df2ad90241 | NFL Showdown $10K mini-MAX [150 Entry Max] (TB @ DAL) | showdown | 5 | 23,781 | dfsdb | inferred_from_contest_name | 2026-10-09 00:15 | canonical | pool_capture 96614eda:live:2026-10-08 00:15:00+00 | 1439 | nfl-ownership-prior-v3 | 0/52 |
| dfsdb-8ac82129-f1d6-4bc9-af29-5a6219b23483 | NFL Showdown $100K mini-MAX [150 Entry Max] (TB @ DAL) | showdown | 5 | 237,812 | dfsdb | inferred_from_contest_name | 2026-10-09 00:15 | canonical | pool_capture 96614eda:live:2026-10-08 00:15:00+00 | 1439 | nfl-ownership-prior-v3 | 0/52 |
| dfsdb-9a830c4f-7351-4575-a75b-1237382766fa | NFL Showdown $10K Quarter Jukebox [Just $0.25!] (TB @ DAL) | showdown | 5 | 47,562 | dfsdb | inferred_from_contest_name | 2026-10-09 00:15 | canonical | pool_capture 96614eda:live:2026-10-08 00:15:00+00 | 1439 | nfl-ownership-prior-v3 | 0/52 |
| dfsdb-9e8a781e-3c88-498c-992e-5c5d5a665160 | NFL Showdown $300K Play-Action [20 Entry Max] (TB @ DAL) | showdown | 5 | 118,906 | dfsdb | inferred_from_contest_name | 2026-10-09 00:15 | canonical | pool_capture 96614eda:live:2026-10-08 00:15:00+00 | 1439 | nfl-ownership-prior-v3 | 0/52 |
| dfsdb-cf22c10c-7c06-4de9-9f35-57355b2da209 | NFL Showdown $300K Flea Flicker [$50K to 1st] (TB @ DAL) | showdown | 5 | 71,343 | dfsdb | inferred_from_contest_name | 2026-10-09 00:15 | canonical | pool_capture 96614eda:live:2026-10-08 00:15:00+00 | 1439 | nfl-ownership-prior-v3 | 0/52 |
| 196689718 | NFL Showdown $1.5M Thursday Night Showdown [$500K to 1st] (S | showdown | 6 | 84,384 | stat-api | slate_upload_id | 2026-10-16 00:15 | canonical | pool_capture 729ccedd:live:2026-10-15 13:54:00+00 | 620 | nfl-ownership-prior-v3 | 15/56 |

## 3. Captures, one row per (slate, source, capture)

Columns `proj`, `avail`, `dkStatus`, `ownPct`, `anchor`, `commit` are player counts out of `players`. `C` = canonical horizon for that source, `E` = early horizon for that source, `*` = canonical/early across all sources.

### showdown NE@SEA — week 1, first kickoff 2026-09-10 00:20 UTC

Uploads 2 (upload); contests 193391013; status **no_frozen_forecast: no capture before kickoff minus 20 min holds ownPct**.

| source | id | origin | at (UTC) | min before kickoff | horizon | games | players | proj | avail | dkStatus | ownPct | ownership version | anchor | commit |
|---|---|---|---|---:|---|---|---:|---:|---:|---:|---:|---|---:|---:|
| optimizer_run | 908cb8cd | partial/gpp | 2026-09-07 23:12 | 2948 | - | all | 42 | 42 | 42 | 42 | 0 | - | 0 | 0 |
| optimizer_run | 3debb2f5 | partial/gpp | 2026-09-07 23:12 | 2948 | - | all | 42 | 42 | 42 | 42 | 0 | - | 0 | 0 |
| optimizer_run | f7f2652e | partial/gpp | 2026-09-07 23:14 | 2946 | CC* | all | 68 | 60 | 68 | 68 | 0 | - | 0 | 0 |

### classic 12 games — week 1, first kickoff 2026-09-13 17:00 UTC

Uploads 4 (upload); contests 193028206; status **no_frozen_forecast: the canonical-horizon capture of every source holds no ownPct (an earlier optimizer_run 4875c71f at 09-04 17:18 does)**.

| source | id | origin | at (UTC) | min before kickoff | horizon | games | players | proj | avail | dkStatus | ownPct | ownership version | anchor | commit |
|---|---|---|---|---:|---|---|---:|---:|---:|---:|---:|---|---:|---:|
| optimizer_run | fad22ec7 | complete/gpp | 2026-09-03 13:12 | 14627 | - | all | 719 | 719 | 0 | 719 | 719 | unversioned | 0 | 0 |
| optimizer_run | f6eb548b | complete/gpp | 2026-09-03 14:21 | 14559 | - | all | 719 | 719 | 0 | 719 | 719 | unversioned | 0 | 0 |
| optimizer_run | a39482c7 | complete/gpp | 2026-09-03 17:39 | 14361 | - | all | 719 | 719 | 0 | 719 | 719 | unversioned | 0 | 0 |
| optimizer_run | 243f4fe1 | complete/gpp | 2026-09-04 12:08 | 13251 | - | all | 719 | 719 | 0 | 719 | 719 | unversioned | 0 | 0 |
| optimizer_run | 4875c71f | complete/gpp | 2026-09-04 17:18 | 12941 | - | all | 719 | 719 | 0 | 719 | 719 | unversioned | 0 | 0 |
| optimizer_run | 4d3a5c7f | complete/gpp | 2026-09-06 14:45 | 10215 | - | all | 719 | 691 | 0 | 719 | 0 | - | 0 | 0 |
| optimizer_run | c6d68205 | complete/gpp | 2026-09-06 22:29 | 9751 | - | all | 680 | 680 | 680 | 680 | 0 | - | 0 | 0 |
| optimizer_run | 2584c2fb | complete/gpp | 2026-09-06 22:32 | 9748 | - | all | 680 | 680 | 680 | 680 | 0 | - | 0 | 0 |
| optimizer_run | 360e4b8e | complete/gpp | 2026-09-06 22:32 | 9748 | - | all | 680 | 680 | 680 | 680 | 0 | - | 0 | 0 |
| optimizer_run | 2a47825c | complete/gpp | 2026-09-06 22:35 | 9744 | - | all | 435 | 435 | 435 | 435 | 0 | - | 0 | 0 |
| optimizer_run | cb3eba82 | complete/gpp | 2026-09-06 22:35 | 9744 | - | all | 435 | 435 | 435 | 435 | 0 | - | 0 | 0 |
| optimizer_run | 491561c6 | complete/gpp | 2026-09-06 22:52 | 9728 | - | all | 435 | 435 | 435 | 435 | 0 | - | 0 | 0 |
| optimizer_run | 293d39e4 | complete/gpp | 2026-09-06 22:52 | 9728 | - | all | 435 | 435 | 435 | 435 | 0 | - | 0 | 0 |
| optimizer_run | 18bbd26a | complete/gpp | 2026-09-06 23:30 | 9689 | - | all | 719 | 691 | 719 | 719 | 0 | - | 0 | 0 |
| optimizer_run | 0d90d762 | complete/gpp | 2026-09-06 23:40 | 9680 | CC* | all | 719 | 691 | 719 | 719 | 0 | - | 0 | 0 |

### classic 13 games — week 2, first kickoff 2026-09-20 17:00 UTC

Uploads 2 (upload); contests 195648006, 195648007; status **no_frozen_forecast: no capture before kickoff minus 20 min holds ownPct**.

| source | id | origin | at (UTC) | min before kickoff | horizon | games | players | proj | avail | dkStatus | ownPct | ownership version | anchor | commit |
|---|---|---|---|---:|---|---|---:|---:|---:|---:|---:|---|---:|---:|
| optimizer_run | c84dbef4 | complete/gpp | 2026-09-19 02:25 | 2315 | - | all | 670 | 641 | 670 | 670 | 0 | - | 0 | 0 |
| optimizer_run | 9b66b833 | complete/gpp | 2026-09-19 02:27 | 2313 | - | all | 670 | 641 | 670 | 670 | 0 | - | 0 | 0 |
| pool_capture | f067c8b9:optimizer:79839671-4397-4e78-9026-227d1f6d5b05 | saved_optimizer | 2026-09-20 16:03 | 56 | CEE* | 13/13 | 670 | 644 | 670 | 0 | 0 | - | 0 | 0 |
| optimizer_run | 79839671 | complete/gpp | 2026-09-20 16:03 | 56 | CEC* | all | 670 | 644 | 670 | 670 | 0 | - | 0 | 0 |
| pool_capture | f067c8b9:live:2026-09-20 17:21:00+00 | live_pool | 2026-09-20 17:21 | -22 | - | 7/13 | 361 | 346 | 361 | 361 | 0 | - | 0 | 0 |
| pool_capture | f067c8b9:live:2026-09-20 17:22:00+00 | live_pool | 2026-09-20 17:22 | -22 | - | 6/13 | 309 | 298 | 309 | 309 | 0 | - | 0 | 0 |
| pool_capture | f067c8b9:live:2026-09-20 19:45:00+00 | live_pool | 2026-09-20 19:45 | -166 | - | 2/13 | 104 | 102 | 104 | 104 | 0 | - | 0 | 0 |
| pool_capture | f067c8b9:live:2026-09-20 19:46:00+00 | live_pool | 2026-09-20 19:46 | -167 | - | 2/13 | 104 | 102 | 104 | 104 | 0 | - | 0 | 0 |
| pool_capture | f067c8b9:live:2026-09-20 19:47:00+00 | live_pool | 2026-09-20 19:47 | -168 | - | 2/13 | 104 | 102 | 104 | 104 | 0 | - | 0 | 0 |
| pool_capture | f067c8b9:live:2026-09-20 19:48:00+00 | live_pool | 2026-09-20 19:48 | -169 | - | 2/13 | 104 | 102 | 104 | 104 | 0 | - | 0 | 0 |
| pool_capture | f067c8b9:live:2026-09-20 19:49:00+00 | live_pool | 2026-09-20 19:49 | -170 | - | 2/13 | 104 | 102 | 104 | 104 | 0 | - | 0 | 0 |
| pool_capture | f067c8b9:live:2026-09-20 19:50:00+00 | live_pool | 2026-09-20 19:50 | -171 | - | 2/13 | 104 | 102 | 104 | 104 | 0 | - | 0 | 0 |
| pool_capture | f067c8b9:live:2026-09-20 19:51:00+00 | live_pool | 2026-09-20 19:51 | -172 | - | 2/13 | 104 | 102 | 104 | 104 | 0 | - | 0 | 0 |
| pool_capture | f067c8b9:live:2026-09-20 19:52:00+00 | live_pool | 2026-09-20 19:52 | -173 | - | 2/13 | 104 | 102 | 104 | 104 | 0 | - | 0 | 0 |
| pool_capture | f067c8b9:live:2026-09-20 19:53:00+00 | live_pool | 2026-09-20 19:53 | -174 | - | 2/13 | 104 | 102 | 104 | 104 | 0 | - | 0 | 0 |
| pool_capture | f067c8b9:live:2026-09-20 19:54:00+00 | live_pool | 2026-09-20 19:54 | -175 | - | 2/13 | 104 | 102 | 104 | 104 | 0 | - | 0 | 0 |
| pool_capture | f067c8b9:live:2026-09-20 19:55:00+00 | live_pool | 2026-09-20 19:55 | -176 | - | 2/13 | 104 | 102 | 104 | 104 | 0 | - | 0 | 0 |
| pool_capture | f067c8b9:live:2026-09-20 19:56:00+00 | live_pool | 2026-09-20 19:56 | -177 | - | 2/13 | 104 | 102 | 104 | 104 | 0 | - | 0 | 0 |
| pool_capture | f067c8b9:live:2026-09-20 19:57:00+00 | live_pool | 2026-09-20 19:57 | -178 | - | 2/13 | 104 | 102 | 104 | 104 | 0 | - | 0 | 0 |
| pool_capture | f067c8b9:live:2026-09-20 19:58:00+00 | live_pool | 2026-09-20 19:58 | -179 | - | 2/13 | 104 | 102 | 104 | 104 | 0 | - | 0 | 0 |
| pool_capture | f067c8b9:live:2026-09-20 19:59:00+00 | live_pool | 2026-09-20 19:59 | -180 | - | 2/13 | 104 | 102 | 104 | 104 | 0 | - | 0 | 0 |
| pool_capture | f067c8b9:live:2026-09-20 20:00:00+00 | live_pool | 2026-09-20 20:00 | -181 | - | 2/13 | 104 | 102 | 104 | 104 | 0 | - | 0 | 0 |
| pool_capture | f067c8b9:live:2026-09-20 20:01:00+00 | live_pool | 2026-09-20 20:01 | -182 | - | 2/13 | 104 | 102 | 104 | 104 | 0 | - | 0 | 0 |
| pool_capture | f067c8b9:live:2026-09-20 20:02:00+00 | live_pool | 2026-09-20 20:02 | -183 | - | 2/13 | 104 | 102 | 104 | 104 | 0 | - | 0 | 0 |
| pool_capture | f067c8b9:live:2026-09-20 20:03:00+00 | live_pool | 2026-09-20 20:03 | -184 | - | 2/13 | 104 | 102 | 104 | 104 | 0 | - | 0 | 0 |
| pool_capture | f067c8b9:live:2026-09-20 20:04:00+00 | live_pool | 2026-09-20 20:04 | -185 | - | 2/13 | 104 | 102 | 104 | 104 | 0 | - | 0 | 0 |
| pool_capture | f067c8b9:live:2026-09-20 20:05:00+00 | live_pool | 2026-09-20 20:05 | -186 | - | 3/13 | 157 | 152 | 157 | 157 | 0 | - | 0 | 0 |
| pool_capture | f067c8b9:live:2026-09-20 20:06:00+00 | live_pool | 2026-09-20 20:06 | -187 | - | 3/13 | 157 | 152 | 157 | 157 | 0 | - | 0 | 0 |
| pool_capture | f067c8b9:live:2026-09-20 20:07:00+00 | live_pool | 2026-09-20 20:07 | -188 | - | 3/13 | 157 | 152 | 157 | 157 | 0 | - | 0 | 0 |
| pool_capture | f067c8b9:live:2026-09-20 20:08:00+00 | live_pool | 2026-09-20 20:08 | -189 | - | 3/13 | 157 | 152 | 157 | 157 | 0 | - | 0 | 0 |
| pool_capture | f067c8b9:live:2026-09-20 20:09:00+00 | live_pool | 2026-09-20 20:09 | -190 | - | 3/13 | 157 | 152 | 157 | 157 | 0 | - | 0 | 0 |
| pool_capture | f067c8b9:live:2026-09-20 20:10:00+00 | live_pool | 2026-09-20 20:10 | -191 | - | 3/13 | 157 | 152 | 157 | 157 | 0 | - | 0 | 0 |
| pool_capture | f067c8b9:live:2026-09-20 20:11:00+00 | live_pool | 2026-09-20 20:11 | -192 | - | 3/13 | 157 | 152 | 157 | 157 | 0 | - | 0 | 0 |
| pool_capture | f067c8b9:live:2026-09-20 20:12:00+00 | live_pool | 2026-09-20 20:12 | -193 | - | 3/13 | 157 | 152 | 157 | 157 | 0 | - | 0 | 0 |
| pool_capture | f067c8b9:live:2026-09-20 20:13:00+00 | live_pool | 2026-09-20 20:13 | -194 | - | 3/13 | 157 | 152 | 157 | 157 | 0 | - | 0 | 0 |
| pool_capture | f067c8b9:live:2026-09-20 20:14:00+00 | live_pool | 2026-09-20 20:14 | -195 | - | 3/13 | 157 | 152 | 157 | 157 | 0 | - | 0 | 0 |
| pool_capture | f067c8b9:live:2026-09-20 20:15:00+00 | live_pool | 2026-09-20 20:15 | -196 | - | 3/13 | 157 | 152 | 157 | 157 | 0 | - | 0 | 0 |
| pool_capture | f067c8b9:live:2026-09-20 20:16:00+00 | live_pool | 2026-09-20 20:16 | -197 | - | 3/13 | 157 | 152 | 157 | 157 | 0 | - | 0 | 0 |
| pool_capture | f067c8b9:live:2026-09-20 20:17:00+00 | live_pool | 2026-09-20 20:17 | -198 | - | 3/13 | 157 | 152 | 157 | 157 | 0 | - | 0 | 0 |
| pool_capture | f067c8b9:live:2026-09-20 20:18:00+00 | live_pool | 2026-09-20 20:18 | -199 | - | 3/13 | 157 | 152 | 157 | 157 | 0 | - | 0 | 0 |
| pool_capture | f067c8b9:live:2026-09-20 20:19:00+00 | live_pool | 2026-09-20 20:19 | -200 | - | 3/13 | 157 | 152 | 157 | 157 | 0 | - | 0 | 0 |
| pool_capture | f067c8b9:live:2026-09-20 20:20:00+00 | live_pool | 2026-09-20 20:20 | -201 | - | 3/13 | 157 | 152 | 157 | 157 | 0 | - | 0 | 0 |
| pool_capture | f067c8b9:live:2026-09-20 20:21:00+00 | live_pool | 2026-09-20 20:21 | -202 | - | 3/13 | 157 | 152 | 157 | 157 | 0 | - | 0 | 0 |
| pool_capture | f067c8b9:live:2026-09-20 20:22:00+00 | live_pool | 2026-09-20 20:22 | -203 | - | 3/13 | 157 | 152 | 157 | 157 | 0 | - | 0 | 0 |
| pool_capture | f067c8b9:live:2026-09-20 20:23:00+00 | live_pool | 2026-09-20 20:23 | -204 | - | 3/13 | 157 | 152 | 157 | 157 | 0 | - | 0 | 0 |
| pool_capture | f067c8b9:live:2026-09-20 20:24:00+00 | live_pool | 2026-09-20 20:24 | -205 | - | 3/13 | 157 | 152 | 157 | 157 | 0 | - | 0 | 0 |
| optimizer_run | 3feb6c83 | complete/gpp | 2026-09-24 01:51 | -4852 | - | all | 670 | 500 | 670 | 670 | 0 | - | 0 | 0 |

### showdown IND@KC — week 2, first kickoff 2026-09-21 00:20 UTC

Uploads 1 (upload); contests none; status **no_frozen_forecast: no capture before kickoff minus 20 min holds ownPct**.

| source | id | origin | at (UTC) | min before kickoff | horizon | games | players | proj | avail | dkStatus | ownPct | ownership version | anchor | commit |
|---|---|---|---|---:|---|---|---:|---:|---:|---:|---:|---|---:|---:|
| pool_capture | 9a95c168:live:2026-09-20 22:39:00+00 | live_pool | 2026-09-20 22:39 | 100 | CEE* | 1/1 | 54 | 52 | 54 | 54 | 0 | - | 0 | 0 |
| optimizer_run | 4007a851 | partial/gpp | 2026-09-20 22:39 | 100 | E | all | 54 | 52 | 54 | 54 | 0 | - | 0 | 0 |
| optimizer_run | f030697f | complete/gpp | 2026-09-20 22:53 | 86 | - | all | 54 | 52 | 54 | 54 | 0 | - | 0 | 0 |
| optimizer_run | 0d53f12b | complete/gpp | 2026-09-20 23:12 | 67 | - | all | 54 | 52 | 54 | 54 | 0 | - | 0 | 0 |
| optimizer_run | 488afe5f | complete/gpp | 2026-09-20 23:13 | 67 | - | all | 54 | 52 | 54 | 54 | 0 | - | 0 | 0 |
| optimizer_run | 4605c820 | complete/gpp | 2026-09-20 23:13 | 67 | CC* | all | 54 | 52 | 54 | 54 | 0 | - | 0 | 0 |
| pool_capture | 9a95c168:live:2026-09-21 00:00:00+00 | live_pool | 2026-09-21 00:00 | 19 | - | 1/1 | 54 | 52 | 54 | 54 | 0 | - | 0 | 0 |
| pool_capture | 9a95c168:live:2026-09-21 00:01:00+00 | live_pool | 2026-09-21 00:01 | 18 | - | 1/1 | 54 | 52 | 54 | 54 | 0 | - | 0 | 0 |
| pool_capture | 9a95c168:live:2026-09-21 00:02:00+00 | live_pool | 2026-09-21 00:02 | 17 | - | 1/1 | 54 | 52 | 54 | 54 | 0 | - | 0 | 0 |
| pool_capture | 9a95c168:live:2026-09-21 00:03:00+00 | live_pool | 2026-09-21 00:03 | 16 | - | 1/1 | 54 | 52 | 54 | 54 | 0 | - | 0 | 0 |
| pool_capture | 9a95c168:live:2026-09-21 00:04:00+00 | live_pool | 2026-09-21 00:04 | 15 | - | 1/1 | 54 | 52 | 54 | 54 | 0 | - | 0 | 0 |
| pool_capture | 9a95c168:live:2026-09-21 00:05:00+00 | live_pool | 2026-09-21 00:05 | 14 | - | 1/1 | 54 | 52 | 54 | 54 | 0 | - | 0 | 0 |
| pool_capture | 9a95c168:live:2026-09-21 00:06:00+00 | live_pool | 2026-09-21 00:06 | 13 | - | 1/1 | 54 | 52 | 54 | 54 | 0 | - | 0 | 0 |
| pool_capture | 9a95c168:live:2026-09-21 00:07:00+00 | live_pool | 2026-09-21 00:07 | 12 | - | 1/1 | 54 | 52 | 54 | 54 | 0 | - | 0 | 0 |
| pool_capture | 9a95c168:live:2026-09-21 00:08:00+00 | live_pool | 2026-09-21 00:08 | 11 | - | 1/1 | 54 | 52 | 54 | 54 | 0 | - | 0 | 0 |
| pool_capture | 9a95c168:live:2026-09-21 00:09:00+00 | live_pool | 2026-09-21 00:09 | 10 | - | 1/1 | 54 | 52 | 54 | 54 | 0 | - | 0 | 0 |
| pool_capture | 9a95c168:live:2026-09-21 00:10:00+00 | live_pool | 2026-09-21 00:10 | 9 | - | 1/1 | 54 | 52 | 54 | 54 | 0 | - | 0 | 0 |
| pool_capture | 9a95c168:live:2026-09-21 00:11:00+00 | live_pool | 2026-09-21 00:11 | 8 | - | 1/1 | 54 | 52 | 54 | 54 | 0 | - | 0 | 0 |
| pool_capture | 9a95c168:live:2026-09-21 00:12:00+00 | live_pool | 2026-09-21 00:12 | 7 | - | 1/1 | 54 | 52 | 54 | 54 | 0 | - | 0 | 0 |
| pool_capture | 9a95c168:live:2026-09-21 00:13:00+00 | live_pool | 2026-09-21 00:13 | 6 | - | 1/1 | 54 | 52 | 54 | 54 | 0 | - | 0 | 0 |
| pool_capture | 9a95c168:live:2026-09-21 00:14:00+00 | live_pool | 2026-09-21 00:14 | 5 | - | 1/1 | 54 | 52 | 54 | 54 | 0 | - | 0 | 0 |
| pool_capture | 9a95c168:live:2026-09-21 00:15:00+00 | live_pool | 2026-09-21 00:15 | 4 | - | 1/1 | 54 | 52 | 54 | 54 | 0 | - | 0 | 0 |
| pool_capture | 9a95c168:live:2026-09-21 00:16:00+00 | live_pool | 2026-09-21 00:16 | 3 | - | 1/1 | 54 | 52 | 54 | 54 | 0 | - | 0 | 0 |
| pool_capture | 9a95c168:live:2026-09-21 00:17:00+00 | live_pool | 2026-09-21 00:17 | 2 | - | 1/1 | 54 | 52 | 54 | 54 | 0 | - | 0 | 0 |
| pool_capture | 9a95c168:live:2026-09-21 00:18:00+00 | live_pool | 2026-09-21 00:18 | 1 | - | 1/1 | 54 | 52 | 54 | 54 | 0 | - | 0 | 0 |
| pool_capture | 9a95c168:live:2026-09-21 00:19:00+00 | live_pool | 2026-09-21 00:19 | 0 | - | 1/1 | 54 | 52 | 54 | 54 | 0 | - | 0 | 0 |

### showdown NYG@LAR — week 2, first kickoff 2026-09-22 00:15 UTC

Uploads 2 (upload); contests 195786068, 195786073; status **no_frozen_forecast: no capture before kickoff minus 20 min holds ownPct**.

| source | id | origin | at (UTC) | min before kickoff | horizon | games | players | proj | avail | dkStatus | ownPct | ownership version | anchor | commit |
|---|---|---|---|---:|---|---|---:|---:|---:|---:|---:|---|---:|---:|
| pool_capture | 889a528f:live:2026-09-21 12:04:00+00 | live_pool | 2026-09-21 12:04 | 730 | EE* | 1/1 | 53 | 52 | 53 | 53 | 0 | - | 0 | 0 |
| optimizer_run | 94f35af3 | complete/gpp | 2026-09-21 12:07 | 728 | E | all | 53 | 52 | 53 | 53 | 0 | - | 0 | 0 |
| optimizer_run | 02dba369 | complete/gpp | 2026-09-21 12:15 | 720 | - | all | 53 | 52 | 53 | 53 | 0 | - | 0 | 0 |
| optimizer_run | 0d2bff3e | complete/gpp | 2026-09-21 12:15 | 719 | - | all | 53 | 52 | 53 | 53 | 0 | - | 0 | 0 |
| optimizer_run | e388ddd6 | complete/gpp | 2026-09-21 12:27 | 708 | - | all | 53 | 52 | 53 | 53 | 0 | - | 0 | 0 |
| optimizer_run | b0b09c3a | complete/gpp | 2026-09-21 16:44 | 450 | - | all | 53 | 52 | 53 | 53 | 0 | - | 0 | 0 |
| pool_capture | 746487fd:live:2026-09-21 20:00:00+00 | live_pool | 2026-09-21 20:00 | 254 | C | 1/1 | 53 | 51 | 53 | 53 | 0 | - | 0 | 0 |
| optimizer_run | 8e90dbc4 | complete/gpp | 2026-09-21 20:00 | 254 | - | all | 53 | 51 | 53 | 53 | 0 | - | 0 | 0 |
| optimizer_run | 8e7cb8ac | complete/gpp | 2026-09-21 20:02 | 253 | CC* | all | 53 | 51 | 53 | 53 | 0 | - | 0 | 0 |
| pool_capture | 746487fd:live:2026-09-21 23:55:00+00 | live_pool | 2026-09-21 23:55 | 19 | - | 1/1 | 53 | 51 | 53 | 53 | 0 | - | 0 | 0 |
| pool_capture | 746487fd:live:2026-09-21 23:56:00+00 | live_pool | 2026-09-21 23:56 | 18 | - | 1/1 | 53 | 51 | 53 | 53 | 0 | - | 0 | 0 |
| pool_capture | 746487fd:live:2026-09-21 23:57:00+00 | live_pool | 2026-09-21 23:57 | 17 | - | 1/1 | 53 | 51 | 53 | 53 | 0 | - | 0 | 0 |
| pool_capture | 746487fd:live:2026-09-21 23:58:00+00 | live_pool | 2026-09-21 23:58 | 16 | - | 1/1 | 53 | 51 | 53 | 53 | 0 | - | 0 | 0 |
| pool_capture | 746487fd:live:2026-09-21 23:59:00+00 | live_pool | 2026-09-21 23:59 | 15 | - | 1/1 | 53 | 51 | 53 | 53 | 0 | - | 0 | 0 |
| pool_capture | 746487fd:live:2026-09-22 00:00:00+00 | live_pool | 2026-09-22 00:00 | 14 | - | 1/1 | 53 | 51 | 53 | 53 | 0 | - | 0 | 0 |
| pool_capture | 746487fd:live:2026-09-22 00:01:00+00 | live_pool | 2026-09-22 00:01 | 13 | - | 1/1 | 53 | 51 | 53 | 53 | 0 | - | 0 | 0 |
| pool_capture | 746487fd:live:2026-09-22 00:02:00+00 | live_pool | 2026-09-22 00:02 | 12 | - | 1/1 | 53 | 51 | 53 | 53 | 0 | - | 0 | 0 |
| pool_capture | 746487fd:live:2026-09-22 00:03:00+00 | live_pool | 2026-09-22 00:03 | 11 | - | 1/1 | 53 | 51 | 53 | 53 | 0 | - | 0 | 0 |
| pool_capture | 746487fd:live:2026-09-22 00:04:00+00 | live_pool | 2026-09-22 00:04 | 10 | - | 1/1 | 53 | 51 | 53 | 53 | 0 | - | 0 | 0 |
| pool_capture | 746487fd:live:2026-09-22 00:05:00+00 | live_pool | 2026-09-22 00:05 | 9 | - | 1/1 | 53 | 51 | 53 | 53 | 0 | - | 0 | 0 |
| pool_capture | 746487fd:live:2026-09-22 00:06:00+00 | live_pool | 2026-09-22 00:06 | 8 | - | 1/1 | 53 | 51 | 53 | 53 | 0 | - | 0 | 0 |
| pool_capture | 746487fd:live:2026-09-22 00:07:00+00 | live_pool | 2026-09-22 00:07 | 7 | - | 1/1 | 53 | 51 | 53 | 53 | 0 | - | 0 | 0 |
| pool_capture | 746487fd:live:2026-09-22 00:08:00+00 | live_pool | 2026-09-22 00:08 | 6 | - | 1/1 | 53 | 51 | 53 | 53 | 0 | - | 0 | 0 |
| pool_capture | 746487fd:live:2026-09-22 00:09:00+00 | live_pool | 2026-09-22 00:09 | 5 | - | 1/1 | 53 | 51 | 53 | 53 | 0 | - | 0 | 0 |
| pool_capture | 746487fd:live:2026-09-22 00:10:00+00 | live_pool | 2026-09-22 00:10 | 4 | - | 1/1 | 53 | 51 | 53 | 53 | 0 | - | 0 | 0 |
| pool_capture | 746487fd:live:2026-09-22 00:11:00+00 | live_pool | 2026-09-22 00:11 | 3 | - | 1/1 | 53 | 51 | 53 | 53 | 0 | - | 0 | 0 |
| pool_capture | 746487fd:live:2026-09-22 00:12:00+00 | live_pool | 2026-09-22 00:12 | 2 | - | 1/1 | 53 | 51 | 53 | 53 | 0 | - | 0 | 0 |
| pool_capture | 746487fd:live:2026-09-22 00:13:00+00 | live_pool | 2026-09-22 00:13 | 1 | - | 1/1 | 53 | 51 | 53 | 53 | 0 | - | 0 | 0 |
| pool_capture | 746487fd:live:2026-09-22 00:14:00+00 | live_pool | 2026-09-22 00:14 | 0 | - | 1/1 | 53 | 51 | 53 | 53 | 0 | - | 0 | 0 |

### showdown ATL@GB — week 3, first kickoff 2026-09-25 00:15 UTC

Uploads 4 (upload); contests 195943234, 195943238; status **no_frozen_forecast: no capture before kickoff minus 20 min holds ownPct**.

| source | id | origin | at (UTC) | min before kickoff | horizon | games | players | proj | avail | dkStatus | ownPct | ownership version | anchor | commit |
|---|---|---|---|---:|---|---|---:|---:|---:|---:|---:|---|---:|---:|
| pool_capture | 843dbfb8:live:2026-09-24 17:51:00+00 | live_pool | 2026-09-24 17:51 | 383 | EE* | 1/1 | 53 | 41 | 53 | 53 | 0 | - | 0 | 0 |
| pool_capture | 81996971:live:2026-09-24 18:05:00+00 | live_pool | 2026-09-24 18:05 | 369 | C | 1/1 | 53 | 41 | 53 | 53 | 0 | - | 0 | 0 |
| optimizer_run | ef8ad5a4 | complete/gpp | 2026-09-24 23:12 | 62 | E | all | 53 | 41 | 53 | 53 | 0 | - | 0 | 0 |
| optimizer_run | 680d3e37 | complete/gpp | 2026-09-24 23:34 | 40 | CC* | all | 53 | 41 | 53 | 53 | 0 | - | 0 | 0 |
| pool_capture | 81996971:live:2026-09-24 23:55:00+00 | live_pool | 2026-09-24 23:55 | 19 | - | 1/1 | 53 | 41 | 53 | 53 | 0 | - | 0 | 0 |
| pool_capture | 81996971:live:2026-09-24 23:56:00+00 | live_pool | 2026-09-24 23:56 | 18 | - | 1/1 | 53 | 41 | 53 | 53 | 0 | - | 0 | 0 |
| pool_capture | 81996971:live:2026-09-24 23:57:00+00 | live_pool | 2026-09-24 23:57 | 17 | - | 1/1 | 53 | 41 | 53 | 53 | 0 | - | 0 | 0 |
| pool_capture | 81996971:live:2026-09-24 23:58:00+00 | live_pool | 2026-09-24 23:58 | 16 | - | 1/1 | 53 | 41 | 53 | 53 | 0 | - | 0 | 0 |
| pool_capture | 81996971:live:2026-09-24 23:59:00+00 | live_pool | 2026-09-24 23:59 | 15 | - | 1/1 | 53 | 41 | 53 | 53 | 0 | - | 0 | 0 |
| pool_capture | 81996971:live:2026-09-25 00:00:00+00 | live_pool | 2026-09-25 00:00 | 14 | - | 1/1 | 53 | 41 | 53 | 53 | 0 | - | 0 | 0 |
| pool_capture | 81996971:live:2026-09-25 00:01:00+00 | live_pool | 2026-09-25 00:01 | 13 | - | 1/1 | 53 | 41 | 53 | 53 | 0 | - | 0 | 0 |
| pool_capture | 81996971:live:2026-09-25 00:02:00+00 | live_pool | 2026-09-25 00:02 | 12 | - | 1/1 | 53 | 41 | 53 | 53 | 0 | - | 0 | 0 |
| pool_capture | 81996971:live:2026-09-25 00:03:00+00 | live_pool | 2026-09-25 00:03 | 11 | - | 1/1 | 53 | 41 | 53 | 53 | 0 | - | 0 | 0 |
| pool_capture | 81996971:live:2026-09-25 00:04:00+00 | live_pool | 2026-09-25 00:04 | 10 | - | 1/1 | 53 | 41 | 53 | 53 | 0 | - | 0 | 0 |
| pool_capture | 81996971:live:2026-09-25 00:05:00+00 | live_pool | 2026-09-25 00:05 | 9 | - | 1/1 | 53 | 41 | 53 | 53 | 0 | - | 0 | 0 |
| pool_capture | 81996971:live:2026-09-25 00:06:00+00 | live_pool | 2026-09-25 00:06 | 8 | - | 1/1 | 53 | 41 | 53 | 53 | 0 | - | 0 | 0 |
| pool_capture | 81996971:live:2026-09-25 00:07:00+00 | live_pool | 2026-09-25 00:07 | 7 | - | 1/1 | 53 | 41 | 53 | 53 | 0 | - | 0 | 0 |
| pool_capture | 81996971:live:2026-09-25 00:08:00+00 | live_pool | 2026-09-25 00:08 | 6 | - | 1/1 | 53 | 41 | 53 | 53 | 0 | - | 0 | 0 |
| pool_capture | 81996971:live:2026-09-25 00:09:00+00 | live_pool | 2026-09-25 00:09 | 5 | - | 1/1 | 53 | 41 | 53 | 53 | 0 | - | 0 | 0 |
| pool_capture | 81996971:live:2026-09-25 00:10:00+00 | live_pool | 2026-09-25 00:10 | 4 | - | 1/1 | 53 | 41 | 53 | 53 | 0 | - | 0 | 0 |
| pool_capture | 81996971:live:2026-09-25 00:11:00+00 | live_pool | 2026-09-25 00:11 | 3 | - | 1/1 | 53 | 41 | 53 | 53 | 0 | - | 0 | 0 |
| pool_capture | 81996971:live:2026-09-25 00:12:00+00 | live_pool | 2026-09-25 00:12 | 2 | - | 1/1 | 53 | 41 | 53 | 53 | 0 | - | 0 | 0 |
| pool_capture | 81996971:live:2026-09-25 00:13:00+00 | live_pool | 2026-09-25 00:13 | 1 | - | 1/1 | 53 | 41 | 53 | 53 | 0 | - | 0 | 0 |
| pool_capture | 81996971:live:2026-09-25 00:14:00+00 | live_pool | 2026-09-25 00:14 | 0 | - | 1/1 | 53 | 41 | 53 | 53 | 0 | - | 0 | 0 |

### classic 13 games — week 3, first kickoff 2026-09-27 17:00 UTC

Uploads 3 (upload); contests 195905122, 196128919; status **no_frozen_forecast: no capture before kickoff minus 20 min holds ownPct**.

| source | id | origin | at (UTC) | min before kickoff | horizon | games | players | proj | avail | dkStatus | ownPct | ownership version | anchor | commit |
|---|---|---|---|---:|---|---|---:|---:|---:|---:|---:|---|---:|---:|
| optimizer_run | 6d7106be | complete/gpp | 2026-09-26 23:00 | 1080 | EE* | all | 671 | 512 | 671 | 671 | 0 | - | 0 | 0 |
| pool_capture | 84d8e0ea:live:2026-09-26 23:00:00+00 | live_pool | 2026-09-26 23:00 | 1079 | E | 13/13 | 671 | 512 | 671 | 671 | 0 | - | 0 | 0 |
| pool_capture | 10a1f03e:live:2026-09-26 23:12:00+00 | live_pool | 2026-09-26 23:12 | 1067 | - | 13/13 | 671 | 518 | 671 | 671 | 0 | - | 0 | 0 |
| pool_capture | d5d97cc7:live:2026-09-27 00:11:00+00 | live_pool | 2026-09-27 00:11 | 1008 | C | 13/13 | 671 | 518 | 671 | 671 | 0 | - | 0 | 0 |
| optimizer_run | 73863820 | complete/gpp | 2026-09-27 11:21 | 338 | - | all | 671 | 512 | 671 | 671 | 0 | - | 0 | 0 |
| optimizer_run | 4585a83d | complete/gpp | 2026-09-27 11:37 | 322 | - | all | 671 | 512 | 671 | 671 | 0 | - | 0 | 0 |
| optimizer_run | 45346d1b | complete/gpp | 2026-09-27 15:37 | 83 | - | all | 671 | 512 | 671 | 671 | 0 | - | 0 | 0 |
| optimizer_run | a896c839 | complete/gpp | 2026-09-27 16:32 | 27 | - | all | 671 | 512 | 671 | 671 | 0 | - | 0 | 0 |
| optimizer_run | e28b1b6c | complete/gpp | 2026-09-27 16:32 | 27 | CC* | all | 671 | 512 | 671 | 671 | 0 | - | 0 | 0 |
| pool_capture | d5d97cc7:live:2026-09-27 16:40:00+00 | live_pool | 2026-09-27 16:40 | 19 | - | 9/13 | 456 | 358 | 456 | 456 | 456 | nfl-ownership-prior-v1 | 0 | 0 |
| pool_capture | d5d97cc7:live:2026-09-27 16:41:00+00 | live_pool | 2026-09-27 16:41 | 18 | - | 9/13 | 456 | 358 | 456 | 456 | 456 | nfl-ownership-prior-v1 | 0 | 0 |
| pool_capture | d5d97cc7:live:2026-09-27 16:42:00+00 | live_pool | 2026-09-27 16:42 | 17 | - | 9/13 | 456 | 358 | 456 | 456 | 456 | nfl-ownership-prior-v1 | 0 | 0 |
| optimizer_run | c0ff6f16 | complete/gpp | 2026-09-27 16:42 | 17 | - | all | 671 | 512 | 671 | 671 | 0 | - | 0 | 0 |
| optimizer_run | 2fb2202b | complete/gpp | 2026-09-27 16:43 | 17 | - | all | 671 | 512 | 671 | 671 | 0 | - | 0 | 0 |
| pool_capture | d5d97cc7:live:2026-09-27 16:43:00+00 | live_pool | 2026-09-27 16:43 | 16 | - | 9/13 | 456 | 358 | 456 | 456 | 456 | nfl-ownership-prior-v1 | 0 | 0 |
| pool_capture | d5d97cc7:live:2026-09-27 16:44:00+00 | live_pool | 2026-09-27 16:44 | 15 | - | 9/13 | 456 | 358 | 456 | 456 | 456 | nfl-ownership-prior-v1 | 0 | 0 |
| pool_capture | d5d97cc7:live:2026-09-27 16:45:00+00 | live_pool | 2026-09-27 16:45 | 14 | - | 9/13 | 456 | 358 | 456 | 456 | 456 | nfl-ownership-prior-v1 | 0 | 0 |
| optimizer_run | 91d51d68 | complete/gpp | 2026-09-27 16:45 | 14 | - | all | 671 | 512 | 671 | 671 | 0 | - | 0 | 0 |
| optimizer_run | 911ddd88 | complete/gpp | 2026-09-27 16:45 | 14 | - | all | 671 | 512 | 671 | 671 | 0 | - | 0 | 0 |
| pool_capture | d5d97cc7:live:2026-09-27 16:46:00+00 | live_pool | 2026-09-27 16:46 | 13 | - | 9/13 | 456 | 358 | 456 | 456 | 456 | nfl-ownership-prior-v1 | 0 | 0 |
| optimizer_run | 767f3715 | complete/gpp | 2026-09-27 16:47 | 13 | - | all | 671 | 512 | 671 | 671 | 0 | - | 0 | 0 |
| pool_capture | d5d97cc7:live:2026-09-27 16:47:00+00 | live_pool | 2026-09-27 16:47 | 12 | - | 9/13 | 456 | 358 | 456 | 456 | 456 | nfl-ownership-prior-v1 | 0 | 0 |
| pool_capture | d5d97cc7:live:2026-09-27 16:48:00+00 | live_pool | 2026-09-27 16:48 | 11 | - | 9/13 | 456 | 358 | 456 | 456 | 456 | nfl-ownership-prior-v1 | 0 | 0 |
| optimizer_run | ea34d346 | complete/gpp | 2026-09-27 16:48 | 11 | - | all | 671 | 512 | 671 | 671 | 0 | - | 0 | 0 |
| pool_capture | d5d97cc7:live:2026-09-27 16:49:00+00 | live_pool | 2026-09-27 16:49 | 10 | - | 9/13 | 456 | 358 | 456 | 456 | 456 | nfl-ownership-prior-v1 | 0 | 0 |
| pool_capture | d5d97cc7:live:2026-09-27 16:50:00+00 | live_pool | 2026-09-27 16:50 | 9 | - | 9/13 | 456 | 358 | 456 | 456 | 456 | nfl-ownership-prior-v1 | 0 | 0 |
| pool_capture | d5d97cc7:live:2026-09-27 16:51:00+00 | live_pool | 2026-09-27 16:51 | 8 | - | 9/13 | 456 | 358 | 456 | 456 | 456 | nfl-ownership-prior-v1 | 0 | 0 |
| pool_capture | d5d97cc7:live:2026-09-27 16:52:00+00 | live_pool | 2026-09-27 16:52 | 7 | - | 9/13 | 456 | 358 | 456 | 456 | 456 | nfl-ownership-prior-v1 | 0 | 0 |
| pool_capture | d5d97cc7:live:2026-09-27 16:54:00+00 | live_pool | 2026-09-27 16:54 | 6 | - | 9/13 | 456 | 358 | 456 | 456 | 456 | nfl-ownership-prior-v1 | 0 | 0 |
| pool_capture | d5d97cc7:live:2026-09-27 16:55:00+00 | live_pool | 2026-09-27 16:55 | 4 | - | 9/13 | 456 | 358 | 456 | 456 | 456 | nfl-ownership-prior-v1 | 0 | 0 |
| pool_capture | d5d97cc7:live:2026-09-27 16:56:00+00 | live_pool | 2026-09-27 16:56 | 3 | - | 9/13 | 456 | 358 | 456 | 456 | 456 | nfl-ownership-prior-v1 | 0 | 0 |
| pool_capture | d5d97cc7:live:2026-09-27 16:57:00+00 | live_pool | 2026-09-27 16:57 | 2 | - | 9/13 | 456 | 358 | 456 | 456 | 456 | nfl-ownership-prior-v1 | 0 | 0 |
| pool_capture | d5d97cc7:live:2026-09-27 16:58:00+00 | live_pool | 2026-09-27 16:58 | 1 | - | 9/13 | 456 | 358 | 456 | 456 | 456 | nfl-ownership-prior-v1 | 0 | 0 |
| pool_capture | d5d97cc7:live:2026-09-27 16:59:00+00 | live_pool | 2026-09-27 16:59 | 0 | - | 9/13 | 456 | 358 | 456 | 456 | 456 | nfl-ownership-prior-v1 | 0 | 0 |
| pool_capture | d5d97cc7:live:2026-09-27 19:45:00+00 | live_pool | 2026-09-27 19:45 | -166 | - | 2/13 | 106 | 80 | 106 | 106 | 106 | nfl-ownership-prior-v1 | 0 | 0 |
| pool_capture | d5d97cc7:live:2026-09-27 19:46:00+00 | live_pool | 2026-09-27 19:46 | -167 | - | 2/13 | 106 | 80 | 106 | 106 | 106 | nfl-ownership-prior-v1 | 0 | 0 |
| pool_capture | d5d97cc7:live:2026-09-27 19:47:00+00 | live_pool | 2026-09-27 19:47 | -168 | - | 2/13 | 106 | 80 | 106 | 106 | 106 | nfl-ownership-prior-v1 | 0 | 0 |
| pool_capture | d5d97cc7:live:2026-09-27 19:48:00+00 | live_pool | 2026-09-27 19:48 | -169 | - | 2/13 | 106 | 80 | 106 | 106 | 106 | nfl-ownership-prior-v1 | 0 | 0 |
| pool_capture | d5d97cc7:live:2026-09-27 19:49:00+00 | live_pool | 2026-09-27 19:49 | -170 | - | 2/13 | 106 | 80 | 106 | 106 | 106 | nfl-ownership-prior-v1 | 0 | 0 |
| pool_capture | d5d97cc7:live:2026-09-27 19:50:00+00 | live_pool | 2026-09-27 19:50 | -171 | - | 2/13 | 106 | 80 | 106 | 106 | 106 | nfl-ownership-prior-v1 | 0 | 0 |
| pool_capture | d5d97cc7:live:2026-09-27 19:51:00+00 | live_pool | 2026-09-27 19:51 | -172 | - | 2/13 | 106 | 80 | 106 | 106 | 106 | nfl-ownership-prior-v1 | 0 | 0 |
| pool_capture | d5d97cc7:live:2026-09-27 19:52:00+00 | live_pool | 2026-09-27 19:52 | -173 | - | 2/13 | 106 | 80 | 106 | 106 | 106 | nfl-ownership-prior-v1 | 0 | 0 |
| pool_capture | d5d97cc7:live:2026-09-27 19:53:00+00 | live_pool | 2026-09-27 19:53 | -174 | - | 2/13 | 106 | 80 | 106 | 106 | 106 | nfl-ownership-prior-v1 | 0 | 0 |
| pool_capture | d5d97cc7:live:2026-09-27 19:54:00+00 | live_pool | 2026-09-27 19:54 | -175 | - | 2/13 | 106 | 80 | 106 | 106 | 106 | nfl-ownership-prior-v1 | 0 | 0 |
| pool_capture | d5d97cc7:live:2026-09-27 19:55:00+00 | live_pool | 2026-09-27 19:55 | -176 | - | 2/13 | 106 | 80 | 106 | 106 | 106 | nfl-ownership-prior-v1 | 0 | 0 |
| pool_capture | d5d97cc7:live:2026-09-27 19:56:00+00 | live_pool | 2026-09-27 19:56 | -177 | - | 2/13 | 106 | 80 | 106 | 106 | 106 | nfl-ownership-prior-v1 | 0 | 0 |
| pool_capture | d5d97cc7:live:2026-09-27 19:57:00+00 | live_pool | 2026-09-27 19:57 | -178 | - | 2/13 | 106 | 80 | 106 | 106 | 106 | nfl-ownership-prior-v1 | 0 | 0 |
| pool_capture | d5d97cc7:live:2026-09-27 19:58:00+00 | live_pool | 2026-09-27 19:58 | -179 | - | 2/13 | 106 | 80 | 106 | 106 | 106 | nfl-ownership-prior-v1 | 0 | 0 |
| pool_capture | d5d97cc7:live:2026-09-27 19:59:00+00 | live_pool | 2026-09-27 19:59 | -180 | - | 2/13 | 106 | 80 | 106 | 106 | 106 | nfl-ownership-prior-v1 | 0 | 0 |
| pool_capture | d5d97cc7:live:2026-09-27 20:00:00+00 | live_pool | 2026-09-27 20:00 | -181 | - | 2/13 | 106 | 80 | 106 | 106 | 106 | nfl-ownership-prior-v1 | 0 | 0 |
| pool_capture | d5d97cc7:live:2026-09-27 20:01:00+00 | live_pool | 2026-09-27 20:01 | -182 | - | 2/13 | 106 | 80 | 106 | 106 | 106 | nfl-ownership-prior-v1 | 0 | 0 |
| pool_capture | d5d97cc7:live:2026-09-27 20:02:00+00 | live_pool | 2026-09-27 20:02 | -183 | - | 2/13 | 106 | 80 | 106 | 106 | 106 | nfl-ownership-prior-v1 | 0 | 0 |
| pool_capture | d5d97cc7:live:2026-09-27 20:03:00+00 | live_pool | 2026-09-27 20:03 | -184 | - | 2/13 | 106 | 80 | 106 | 106 | 106 | nfl-ownership-prior-v1 | 0 | 0 |
| pool_capture | d5d97cc7:live:2026-09-27 20:04:00+00 | live_pool | 2026-09-27 20:04 | -185 | - | 2/13 | 106 | 80 | 106 | 106 | 106 | nfl-ownership-prior-v1 | 0 | 0 |
| pool_capture | d5d97cc7:live:2026-09-27 20:05:00+00 | live_pool | 2026-09-27 20:05 | -186 | - | 2/13 | 109 | 87 | 109 | 109 | 109 | nfl-ownership-prior-v1 | 0 | 0 |
| pool_capture | d5d97cc7:live:2026-09-27 20:06:00+00 | live_pool | 2026-09-27 20:06 | -187 | - | 2/13 | 109 | 87 | 109 | 109 | 109 | nfl-ownership-prior-v1 | 0 | 0 |
| pool_capture | d5d97cc7:live:2026-09-27 20:07:00+00 | live_pool | 2026-09-27 20:07 | -188 | - | 2/13 | 109 | 87 | 109 | 109 | 109 | nfl-ownership-prior-v1 | 0 | 0 |
| pool_capture | d5d97cc7:live:2026-09-27 20:08:00+00 | live_pool | 2026-09-27 20:08 | -189 | - | 2/13 | 109 | 87 | 109 | 109 | 109 | nfl-ownership-prior-v1 | 0 | 0 |
| pool_capture | d5d97cc7:live:2026-09-27 20:09:00+00 | live_pool | 2026-09-27 20:09 | -190 | - | 2/13 | 109 | 87 | 109 | 109 | 109 | nfl-ownership-prior-v1 | 0 | 0 |
| pool_capture | d5d97cc7:live:2026-09-27 20:10:00+00 | live_pool | 2026-09-27 20:10 | -191 | - | 2/13 | 109 | 87 | 109 | 109 | 109 | nfl-ownership-prior-v1 | 0 | 0 |
| pool_capture | d5d97cc7:live:2026-09-27 20:11:00+00 | live_pool | 2026-09-27 20:11 | -192 | - | 2/13 | 109 | 87 | 109 | 109 | 109 | nfl-ownership-prior-v1 | 0 | 0 |
| pool_capture | d5d97cc7:live:2026-09-27 20:12:00+00 | live_pool | 2026-09-27 20:12 | -193 | - | 2/13 | 109 | 87 | 109 | 109 | 109 | nfl-ownership-prior-v1 | 0 | 0 |
| pool_capture | d5d97cc7:live:2026-09-27 20:13:00+00 | live_pool | 2026-09-27 20:13 | -194 | - | 2/13 | 109 | 87 | 109 | 109 | 109 | nfl-ownership-prior-v1 | 0 | 0 |
| pool_capture | d5d97cc7:live:2026-09-27 20:14:00+00 | live_pool | 2026-09-27 20:14 | -195 | - | 2/13 | 109 | 87 | 109 | 109 | 109 | nfl-ownership-prior-v1 | 0 | 0 |
| pool_capture | d5d97cc7:live:2026-09-27 20:15:00+00 | live_pool | 2026-09-27 20:15 | -196 | - | 2/13 | 109 | 87 | 109 | 109 | 109 | nfl-ownership-prior-v1 | 0 | 0 |
| pool_capture | d5d97cc7:live:2026-09-27 20:16:00+00 | live_pool | 2026-09-27 20:16 | -197 | - | 2/13 | 109 | 87 | 109 | 109 | 109 | nfl-ownership-prior-v1 | 0 | 0 |
| pool_capture | d5d97cc7:live:2026-09-27 20:17:00+00 | live_pool | 2026-09-27 20:17 | -198 | - | 2/13 | 109 | 87 | 109 | 109 | 109 | nfl-ownership-prior-v1 | 0 | 0 |
| pool_capture | d5d97cc7:live:2026-09-27 20:18:00+00 | live_pool | 2026-09-27 20:18 | -199 | - | 2/13 | 109 | 87 | 109 | 109 | 109 | nfl-ownership-prior-v1 | 0 | 0 |
| pool_capture | d5d97cc7:live:2026-09-27 20:19:00+00 | live_pool | 2026-09-27 20:19 | -200 | - | 2/13 | 109 | 87 | 109 | 109 | 109 | nfl-ownership-prior-v1 | 0 | 0 |
| pool_capture | d5d97cc7:live:2026-09-27 20:20:00+00 | live_pool | 2026-09-27 20:20 | -201 | - | 2/13 | 109 | 87 | 109 | 109 | 109 | nfl-ownership-prior-v1 | 0 | 0 |
| pool_capture | d5d97cc7:live:2026-09-27 20:21:00+00 | live_pool | 2026-09-27 20:21 | -202 | - | 2/13 | 109 | 87 | 109 | 109 | 109 | nfl-ownership-prior-v1 | 0 | 0 |
| pool_capture | d5d97cc7:live:2026-09-27 20:22:00+00 | live_pool | 2026-09-27 20:22 | -203 | - | 2/13 | 109 | 87 | 109 | 109 | 109 | nfl-ownership-prior-v1 | 0 | 0 |
| pool_capture | d5d97cc7:live:2026-09-27 20:23:00+00 | live_pool | 2026-09-27 20:23 | -204 | - | 2/13 | 109 | 87 | 109 | 109 | 109 | nfl-ownership-prior-v1 | 0 | 0 |
| pool_capture | d5d97cc7:live:2026-09-27 20:24:00+00 | live_pool | 2026-09-27 20:24 | -205 | - | 2/13 | 109 | 87 | 109 | 109 | 109 | nfl-ownership-prior-v1 | 0 | 0 |

### showdown PHI@CHI — week 3, first kickoff 2026-09-29 00:15 UTC

Uploads 1 (upload); contests 196036238; status **canonical**.

| source | id | origin | at (UTC) | min before kickoff | horizon | games | players | proj | avail | dkStatus | ownPct | ownership version | anchor | commit |
|---|---|---|---|---:|---|---|---:|---:|---:|---:|---:|---|---:|---:|
| pool_capture | e455e66a:live:2026-09-28 21:01:00+00 | live_pool | 2026-09-28 21:01 | 193 | CEE* | 1/1 | 56 | 48 | 56 | 56 | 56 | nfl-ownership-prior-v1 | 0 | 0 |
| optimizer_run | 555c84a6 | complete/gpp | 2026-09-28 22:11 | 124 | E | all | 56 | 49 | 56 | 56 | 0 | - | 0 | 0 |
| optimizer_run | 9e630f61 | complete/gpp | 2026-09-28 22:17 | 118 | - | all | 56 | 49 | 56 | 56 | 0 | - | 0 | 0 |
| optimizer_run | eaee2115 | complete/gpp | 2026-09-28 22:24 | 110 | CC* | all | 56 | 49 | 56 | 56 | 0 | - | 0 | 0 |
| pool_capture | e455e66a:live:2026-09-28 23:55:00+00 | live_pool | 2026-09-28 23:55 | 19 | - | 1/1 | 56 | 48 | 56 | 56 | 56 | nfl-ownership-prior-v1 | 0 | 0 |
| pool_capture | e455e66a:live:2026-09-28 23:56:00+00 | live_pool | 2026-09-28 23:56 | 18 | - | 1/1 | 56 | 48 | 56 | 56 | 56 | nfl-ownership-prior-v1 | 0 | 0 |
| pool_capture | e455e66a:live:2026-09-28 23:57:00+00 | live_pool | 2026-09-28 23:57 | 17 | - | 1/1 | 56 | 48 | 56 | 56 | 56 | nfl-ownership-prior-v1 | 0 | 0 |
| pool_capture | e455e66a:live:2026-09-28 23:58:00+00 | live_pool | 2026-09-28 23:58 | 16 | - | 1/1 | 56 | 48 | 56 | 56 | 56 | nfl-ownership-prior-v1 | 0 | 0 |
| pool_capture | e455e66a:live:2026-09-29 00:00:00+00 | live_pool | 2026-09-29 00:00 | 15 | - | 1/1 | 56 | 48 | 56 | 56 | 56 | nfl-ownership-prior-v1 | 0 | 0 |
| pool_capture | e455e66a:live:2026-09-29 00:01:00+00 | live_pool | 2026-09-29 00:01 | 13 | - | 1/1 | 56 | 48 | 56 | 56 | 56 | nfl-ownership-prior-v1 | 0 | 0 |
| pool_capture | e455e66a:live:2026-09-29 00:02:00+00 | live_pool | 2026-09-29 00:02 | 12 | - | 1/1 | 56 | 48 | 56 | 56 | 56 | nfl-ownership-prior-v1 | 0 | 0 |
| pool_capture | e455e66a:live:2026-09-29 00:03:00+00 | live_pool | 2026-09-29 00:03 | 11 | - | 1/1 | 56 | 48 | 56 | 56 | 56 | nfl-ownership-prior-v1 | 0 | 0 |
| pool_capture | e455e66a:live:2026-09-29 00:04:00+00 | live_pool | 2026-09-29 00:04 | 10 | - | 1/1 | 56 | 48 | 56 | 56 | 56 | nfl-ownership-prior-v1 | 0 | 0 |
| pool_capture | e455e66a:live:2026-09-29 00:05:00+00 | live_pool | 2026-09-29 00:05 | 9 | - | 1/1 | 56 | 48 | 56 | 56 | 56 | nfl-ownership-prior-v1 | 0 | 0 |
| pool_capture | e455e66a:live:2026-09-29 00:06:00+00 | live_pool | 2026-09-29 00:06 | 8 | - | 1/1 | 56 | 48 | 56 | 56 | 56 | nfl-ownership-prior-v1 | 0 | 0 |
| pool_capture | e455e66a:live:2026-09-29 00:07:00+00 | live_pool | 2026-09-29 00:07 | 7 | - | 1/1 | 56 | 48 | 56 | 56 | 56 | nfl-ownership-prior-v1 | 0 | 0 |
| pool_capture | e455e66a:live:2026-09-29 00:08:00+00 | live_pool | 2026-09-29 00:08 | 6 | - | 1/1 | 56 | 48 | 56 | 56 | 56 | nfl-ownership-prior-v1 | 0 | 0 |
| pool_capture | e455e66a:live:2026-09-29 00:09:00+00 | live_pool | 2026-09-29 00:09 | 5 | - | 1/1 | 56 | 48 | 56 | 56 | 56 | nfl-ownership-prior-v1 | 0 | 0 |
| pool_capture | e455e66a:live:2026-09-29 00:10:00+00 | live_pool | 2026-09-29 00:10 | 4 | - | 1/1 | 56 | 48 | 56 | 56 | 56 | nfl-ownership-prior-v1 | 0 | 0 |
| pool_capture | e455e66a:live:2026-09-29 00:11:00+00 | live_pool | 2026-09-29 00:11 | 3 | - | 1/1 | 56 | 48 | 56 | 56 | 56 | nfl-ownership-prior-v1 | 0 | 0 |
| pool_capture | e455e66a:live:2026-09-29 00:12:00+00 | live_pool | 2026-09-29 00:12 | 2 | - | 1/1 | 56 | 48 | 56 | 56 | 56 | nfl-ownership-prior-v1 | 0 | 0 |
| pool_capture | e455e66a:live:2026-09-29 00:13:00+00 | live_pool | 2026-09-29 00:13 | 1 | - | 1/1 | 56 | 48 | 56 | 56 | 56 | nfl-ownership-prior-v1 | 0 | 0 |
| pool_capture | e455e66a:live:2026-09-29 00:14:00+00 | live_pool | 2026-09-29 00:14 | 0 | - | 1/1 | 56 | 48 | 56 | 56 | 56 | nfl-ownership-prior-v1 | 0 | 0 |

### showdown PIT@CLE — week 4, first kickoff 2026-10-02 00:15 UTC

Uploads 1 (upload); contests 196187074, 196280117; status **canonical**.

| source | id | origin | at (UTC) | min before kickoff | horizon | games | players | proj | avail | dkStatus | ownPct | ownership version | anchor | commit |
|---|---|---|---|---:|---|---|---:|---:|---:|---:|---:|---|---:|---:|
| pool_capture | 8e7cf26c:live:2026-10-01 22:13:00+00 | live_pool | 2026-10-01 22:13 | 121 | CEE* | 1/1 | 51 | 46 | 51 | 51 | 51 | nfl-ownership-prior-v2 | 0 | 0 |
| optimizer_run | c79c62d2 | complete/gpp | 2026-10-01 22:13 | 121 | CEC* | all | 51 | 46 | 51 | 51 | 51 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 8e7cf26c:live:2026-10-01 23:55:00+00 | live_pool | 2026-10-01 23:55 | 19 | - | 1/1 | 51 | 46 | 51 | 51 | 51 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 8e7cf26c:live:2026-10-01 23:56:00+00 | live_pool | 2026-10-01 23:56 | 18 | - | 1/1 | 51 | 46 | 51 | 51 | 51 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 8e7cf26c:live:2026-10-01 23:57:00+00 | live_pool | 2026-10-01 23:57 | 17 | - | 1/1 | 51 | 46 | 51 | 51 | 51 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 8e7cf26c:live:2026-10-01 23:58:00+00 | live_pool | 2026-10-01 23:58 | 16 | - | 1/1 | 51 | 46 | 51 | 51 | 51 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 8e7cf26c:live:2026-10-01 23:59:00+00 | live_pool | 2026-10-01 23:59 | 15 | - | 1/1 | 51 | 46 | 51 | 51 | 51 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 8e7cf26c:live:2026-10-02 00:00:00+00 | live_pool | 2026-10-02 00:00 | 14 | - | 1/1 | 51 | 46 | 51 | 51 | 51 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 8e7cf26c:live:2026-10-02 00:01:00+00 | live_pool | 2026-10-02 00:01 | 13 | - | 1/1 | 51 | 46 | 51 | 51 | 51 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 8e7cf26c:live:2026-10-02 00:02:00+00 | live_pool | 2026-10-02 00:02 | 12 | - | 1/1 | 51 | 46 | 51 | 51 | 51 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 8e7cf26c:live:2026-10-02 00:03:00+00 | live_pool | 2026-10-02 00:03 | 11 | - | 1/1 | 51 | 46 | 51 | 51 | 51 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 8e7cf26c:live:2026-10-02 00:04:00+00 | live_pool | 2026-10-02 00:04 | 10 | - | 1/1 | 51 | 46 | 51 | 51 | 51 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 8e7cf26c:live:2026-10-02 00:05:00+00 | live_pool | 2026-10-02 00:05 | 9 | - | 1/1 | 51 | 46 | 51 | 51 | 51 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 8e7cf26c:live:2026-10-02 00:06:00+00 | live_pool | 2026-10-02 00:06 | 8 | - | 1/1 | 51 | 46 | 51 | 51 | 51 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 8e7cf26c:live:2026-10-02 00:07:00+00 | live_pool | 2026-10-02 00:07 | 7 | - | 1/1 | 51 | 46 | 51 | 51 | 51 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 8e7cf26c:live:2026-10-02 00:08:00+00 | live_pool | 2026-10-02 00:08 | 6 | - | 1/1 | 51 | 46 | 51 | 51 | 51 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 8e7cf26c:live:2026-10-02 00:09:00+00 | live_pool | 2026-10-02 00:09 | 5 | - | 1/1 | 51 | 46 | 51 | 51 | 51 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 8e7cf26c:live:2026-10-02 00:10:00+00 | live_pool | 2026-10-02 00:10 | 4 | - | 1/1 | 51 | 46 | 51 | 51 | 51 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 8e7cf26c:live:2026-10-02 00:11:00+00 | live_pool | 2026-10-02 00:11 | 3 | - | 1/1 | 51 | 46 | 51 | 51 | 51 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 8e7cf26c:live:2026-10-02 00:12:00+00 | live_pool | 2026-10-02 00:12 | 2 | - | 1/1 | 51 | 46 | 51 | 51 | 51 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 8e7cf26c:live:2026-10-02 00:13:00+00 | live_pool | 2026-10-02 00:13 | 1 | - | 1/1 | 51 | 46 | 51 | 51 | 51 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 8e7cf26c:live:2026-10-02 00:14:00+00 | live_pool | 2026-10-02 00:14 | 0 | - | 1/1 | 51 | 46 | 51 | 51 | 51 | nfl-ownership-prior-v2 | 0 | 0 |

### showdown IND@WAS — week 4, first kickoff 2026-10-04 13:30 UTC

Uploads 4 (upload); contests 196204783; status **canonical**.

| source | id | origin | at (UTC) | min before kickoff | horizon | games | players | proj | avail | dkStatus | ownPct | ownership version | anchor | commit |
|---|---|---|---|---:|---|---|---:|---:|---:|---:|---:|---|---:|---:|
| pool_capture | a81b0ba8:live:2026-10-03 21:11:00+00 | live_pool | 2026-10-03 21:11 | 978 | EE* | 1/1 | 56 | 47 | 56 | 56 | 56 | nfl-ownership-prior-v2 | 0 | 0 |
| optimizer_run | f55a596a | complete/gpp | 2026-10-03 21:12 | 977 | E | all | 56 | 47 | 56 | 56 | 56 | nfl-ownership-prior-v2 | 0 | 0 |
| optimizer_run | 9623a52a | complete/gpp | 2026-10-03 21:13 | 976 | - | all | 56 | 47 | 56 | 56 | 56 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | d0c2d4f8:live:2026-10-03 21:13:00+00 | live_pool | 2026-10-03 21:13 | 976 | - | 1/1 | 56 | 47 | 56 | 56 | 56 | nfl-ownership-prior-v2 | 0 | 0 |
| optimizer_run | bd9b6233 | complete/gpp | 2026-10-04 08:24 | 306 | - | all | 56 | 47 | 56 | 56 | 56 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 76e25897:live:2026-10-04 08:25:00+00 | live_pool | 2026-10-04 08:25 | 304 | C | 1/1 | 56 | 47 | 56 | 56 | 56 | nfl-ownership-prior-v2 | 0 | 0 |
| optimizer_run | 349b3b8d | complete/gpp | 2026-10-04 08:25 | 304 | CC* | all | 56 | 47 | 56 | 56 | 56 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 76e25897:live:2026-10-04 13:10:00+00 | live_pool | 2026-10-04 13:10 | 19 | - | 1/1 | 56 | 47 | 56 | 56 | 56 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 76e25897:live:2026-10-04 13:11:00+00 | live_pool | 2026-10-04 13:11 | 18 | - | 1/1 | 56 | 47 | 56 | 56 | 56 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 76e25897:live:2026-10-04 13:12:00+00 | live_pool | 2026-10-04 13:12 | 17 | - | 1/1 | 56 | 47 | 56 | 56 | 56 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 76e25897:live:2026-10-04 13:13:00+00 | live_pool | 2026-10-04 13:13 | 16 | - | 1/1 | 56 | 47 | 56 | 56 | 56 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 76e25897:live:2026-10-04 13:14:00+00 | live_pool | 2026-10-04 13:14 | 15 | - | 1/1 | 56 | 47 | 56 | 56 | 56 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 76e25897:live:2026-10-04 13:15:00+00 | live_pool | 2026-10-04 13:15 | 14 | - | 1/1 | 56 | 47 | 56 | 56 | 56 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 76e25897:live:2026-10-04 13:16:00+00 | live_pool | 2026-10-04 13:16 | 13 | - | 1/1 | 56 | 47 | 56 | 56 | 56 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 76e25897:live:2026-10-04 13:17:00+00 | live_pool | 2026-10-04 13:17 | 12 | - | 1/1 | 56 | 47 | 56 | 56 | 56 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 76e25897:live:2026-10-04 13:18:00+00 | live_pool | 2026-10-04 13:18 | 11 | - | 1/1 | 56 | 47 | 56 | 56 | 56 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 76e25897:live:2026-10-04 13:19:00+00 | live_pool | 2026-10-04 13:19 | 10 | - | 1/1 | 56 | 47 | 56 | 56 | 56 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 76e25897:live:2026-10-04 13:20:00+00 | live_pool | 2026-10-04 13:20 | 9 | - | 1/1 | 56 | 47 | 56 | 56 | 56 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 76e25897:live:2026-10-04 13:21:00+00 | live_pool | 2026-10-04 13:21 | 8 | - | 1/1 | 56 | 47 | 56 | 56 | 56 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 76e25897:live:2026-10-04 13:22:00+00 | live_pool | 2026-10-04 13:22 | 7 | - | 1/1 | 56 | 47 | 56 | 56 | 56 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 76e25897:live:2026-10-04 13:23:00+00 | live_pool | 2026-10-04 13:23 | 6 | - | 1/1 | 56 | 47 | 56 | 56 | 56 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 76e25897:live:2026-10-04 13:24:00+00 | live_pool | 2026-10-04 13:24 | 5 | - | 1/1 | 56 | 47 | 56 | 56 | 56 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 76e25897:live:2026-10-04 13:25:00+00 | live_pool | 2026-10-04 13:25 | 4 | - | 1/1 | 56 | 47 | 56 | 56 | 56 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 76e25897:live:2026-10-04 13:26:00+00 | live_pool | 2026-10-04 13:26 | 3 | - | 1/1 | 56 | 47 | 56 | 56 | 56 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 76e25897:live:2026-10-04 13:34:00+00 | live_pool | 2026-10-04 13:34 | -5 | - | 1/1 | 56 | 47 | 56 | 56 | 56 | nfl-ownership-prior-v2 | 0 | 0 |

### classic 12 games — week 4, first kickoff 2026-10-04 17:00 UTC

Uploads 7 (upload); contests 196151357; status **canonical**.

| source | id | origin | at (UTC) | min before kickoff | horizon | games | players | proj | avail | dkStatus | ownPct | ownership version | anchor | commit |
|---|---|---|---|---:|---|---|---:|---:|---:|---:|---:|---|---:|---:|
| optimizer_run | fbb9a050 | failed/gpp | 2026-10-02 22:29 | 2551 | - | all | 629 | 504 | 629 | 629 | 629 | nfl-ownership-prior-v2 | 0 | 0 |
| optimizer_run | c9efae89 | failed/gpp | 2026-10-02 22:30 | 2550 | - | all | 629 | 504 | 629 | 629 | 629 | nfl-ownership-prior-v2 | 0 | 0 |
| optimizer_run | 7ffab760 | complete/gpp | 2026-10-02 22:30 | 2549 | - | all | 629 | 504 | 629 | 629 | 629 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 0759c396:live:2026-10-03 17:00:00+00 | live_pool | 2026-10-03 17:00 | 1439 | EE* | 8/12 | 411 | 334 | 411 | 411 | 411 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | e1c5ef9d:live:2026-10-03 17:00:00+00 | live_pool | 2026-10-03 17:00 | 1439 | - | 8/12 | 411 | 334 | 411 | 411 | 411 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 03ecf5d7:live:2026-10-03 17:12:00+00 | live_pool | 2026-10-03 17:12 | 1427 | - | 8/12 | 411 | 334 | 411 | 411 | 411 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 03ecf5d7:live:2026-10-03 20:05:00+00 | live_pool | 2026-10-03 20:05 | 1254 | - | 1/12 | 52 | 39 | 52 | 52 | 52 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | e1c5ef9d:live:2026-10-03 20:05:00+00 | live_pool | 2026-10-03 20:05 | 1254 | - | 1/12 | 52 | 39 | 52 | 52 | 52 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 03ecf5d7:live:2026-10-03 20:25:00+00 | live_pool | 2026-10-03 20:25 | 1234 | - | 3/12 | 166 | 131 | 166 | 166 | 166 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | e1c5ef9d:live:2026-10-03 20:25:00+00 | live_pool | 2026-10-03 20:25 | 1234 | - | 3/12 | 166 | 131 | 166 | 166 | 166 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | b86f8038:live:2026-10-03 21:06:00+00 | live_pool | 2026-10-03 21:06 | 1193 | - | 12/12 | 629 | 504 | 629 | 629 | 629 | nfl-ownership-prior-v2 | 0 | 0 |
| optimizer_run | e7ebd42a | failed/gpp | 2026-10-03 21:09 | 1190 | E | all | 629 | 504 | 629 | 629 | 629 | nfl-ownership-prior-v2 | 0 | 0 |
| optimizer_run | 10c74f57 | complete/gpp | 2026-10-03 21:10 | 1190 | - | all | 629 | 504 | 629 | 629 | 629 | nfl-ownership-prior-v2 | 0 | 0 |
| optimizer_run | d36eb30c | complete/gpp | 2026-10-03 21:19 | 1180 | - | all | 629 | 504 | 629 | 629 | 629 | nfl-ownership-prior-v2 | 0 | 0 |
| optimizer_run | 46f8b9f9 | complete/cash | 2026-10-03 21:21 | 1179 | - | all | 629 | 504 | 629 | 629 | 629 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 0070e900:live:2026-10-04 15:59:00+00 | live_pool | 2026-10-04 15:59 | 60 | - | 12/12 | 629 | 510 | 629 | 629 | 629 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 23253360:live:2026-10-04 16:04:00+00 | live_pool | 2026-10-04 16:04 | 55 | C | 12/12 | 629 | 512 | 629 | 629 | 629 | nfl-ownership-prior-v2 | 0 | 0 |
| optimizer_run | 25c49d60 | complete/gpp | 2026-10-04 16:07 | 52 | CC* | all | 629 | 512 | 629 | 629 | 629 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 23253360:live:2026-10-04 16:43:00+00 | live_pool | 2026-10-04 16:43 | 17 | - | 8/12 | 411 | 343 | 411 | 411 | 411 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 03ecf5d7:live:2026-10-04 16:43:00+00 | live_pool | 2026-10-04 16:43 | 17 | - | 8/12 | 411 | 342 | 411 | 411 | 411 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | e1c5ef9d:live:2026-10-04 16:43:00+00 | live_pool | 2026-10-04 16:43 | 16 | - | 8/12 | 411 | 342 | 411 | 411 | 411 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 23253360:live:2026-10-04 16:44:00+00 | live_pool | 2026-10-04 16:44 | 15 | - | 8/12 | 411 | 343 | 411 | 411 | 411 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 03ecf5d7:live:2026-10-04 16:44:00+00 | live_pool | 2026-10-04 16:44 | 15 | - | 8/12 | 411 | 342 | 411 | 411 | 411 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | e1c5ef9d:live:2026-10-04 16:44:00+00 | live_pool | 2026-10-04 16:44 | 15 | - | 8/12 | 411 | 342 | 411 | 411 | 411 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 23253360:live:2026-10-04 16:45:00+00 | live_pool | 2026-10-04 16:45 | 14 | - | 8/12 | 411 | 343 | 411 | 411 | 411 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 03ecf5d7:live:2026-10-04 16:45:00+00 | live_pool | 2026-10-04 16:45 | 14 | - | 8/12 | 411 | 342 | 411 | 411 | 411 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | e1c5ef9d:live:2026-10-04 16:45:00+00 | live_pool | 2026-10-04 16:45 | 14 | - | 8/12 | 411 | 342 | 411 | 411 | 411 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 23253360:live:2026-10-04 16:46:00+00 | live_pool | 2026-10-04 16:46 | 13 | - | 8/12 | 411 | 343 | 411 | 411 | 411 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 03ecf5d7:live:2026-10-04 16:46:00+00 | live_pool | 2026-10-04 16:46 | 13 | - | 8/12 | 411 | 342 | 411 | 411 | 411 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | e1c5ef9d:live:2026-10-04 16:46:00+00 | live_pool | 2026-10-04 16:46 | 13 | - | 8/12 | 411 | 342 | 411 | 411 | 411 | nfl-ownership-prior-v2 | 0 | 0 |
| optimizer_run | af17aac3 | complete/gpp | 2026-10-04 16:47 | 12 | - | all | 629 | 513 | 629 | 629 | 629 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 23253360:live:2026-10-04 16:47:00+00 | live_pool | 2026-10-04 16:47 | 12 | - | 8/12 | 411 | 343 | 411 | 411 | 411 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 03ecf5d7:live:2026-10-04 16:47:00+00 | live_pool | 2026-10-04 16:47 | 12 | - | 8/12 | 411 | 342 | 411 | 411 | 411 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | e1c5ef9d:live:2026-10-04 16:47:00+00 | live_pool | 2026-10-04 16:47 | 12 | - | 8/12 | 411 | 342 | 411 | 411 | 411 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 23253360:live:2026-10-04 16:48:00+00 | live_pool | 2026-10-04 16:48 | 11 | - | 8/12 | 411 | 343 | 411 | 411 | 411 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 03ecf5d7:live:2026-10-04 16:48:00+00 | live_pool | 2026-10-04 16:48 | 11 | - | 8/12 | 411 | 342 | 411 | 411 | 411 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | e1c5ef9d:live:2026-10-04 16:48:00+00 | live_pool | 2026-10-04 16:48 | 11 | - | 8/12 | 411 | 342 | 411 | 411 | 411 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 23253360:live:2026-10-04 16:49:00+00 | live_pool | 2026-10-04 16:49 | 10 | - | 8/12 | 411 | 343 | 411 | 411 | 411 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 03ecf5d7:live:2026-10-04 16:49:00+00 | live_pool | 2026-10-04 16:49 | 10 | - | 8/12 | 411 | 342 | 411 | 411 | 411 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | e1c5ef9d:live:2026-10-04 16:49:00+00 | live_pool | 2026-10-04 16:49 | 10 | - | 8/12 | 411 | 342 | 411 | 411 | 411 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 23253360:live:2026-10-04 16:50:00+00 | live_pool | 2026-10-04 16:50 | 9 | - | 8/12 | 411 | 343 | 411 | 411 | 411 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 03ecf5d7:live:2026-10-04 16:50:00+00 | live_pool | 2026-10-04 16:50 | 9 | - | 8/12 | 411 | 342 | 411 | 411 | 411 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | e1c5ef9d:live:2026-10-04 16:50:00+00 | live_pool | 2026-10-04 16:50 | 9 | - | 8/12 | 411 | 342 | 411 | 411 | 411 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 23253360:live:2026-10-04 16:51:00+00 | live_pool | 2026-10-04 16:51 | 8 | - | 8/12 | 411 | 343 | 411 | 411 | 411 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 03ecf5d7:live:2026-10-04 16:51:00+00 | live_pool | 2026-10-04 16:51 | 8 | - | 8/12 | 411 | 342 | 411 | 411 | 411 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | e1c5ef9d:live:2026-10-04 16:51:00+00 | live_pool | 2026-10-04 16:51 | 8 | - | 7/12 | 360 | 299 | 360 | 360 | 360 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | e1c5ef9d:live:2026-10-04 16:52:00+00 | live_pool | 2026-10-04 16:52 | 8 | - | 8/12 | 411 | 342 | 411 | 411 | 411 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 23253360:live:2026-10-04 16:52:00+00 | live_pool | 2026-10-04 16:52 | 7 | - | 8/12 | 411 | 343 | 411 | 411 | 411 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 03ecf5d7:live:2026-10-04 16:52:00+00 | live_pool | 2026-10-04 16:52 | 7 | - | 8/12 | 411 | 342 | 411 | 411 | 411 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 23253360:live:2026-10-04 16:53:00+00 | live_pool | 2026-10-04 16:53 | 6 | - | 8/12 | 411 | 343 | 411 | 411 | 411 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 03ecf5d7:live:2026-10-04 16:53:00+00 | live_pool | 2026-10-04 16:53 | 6 | - | 8/12 | 411 | 342 | 411 | 411 | 411 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | e1c5ef9d:live:2026-10-04 16:53:00+00 | live_pool | 2026-10-04 16:53 | 6 | - | 8/12 | 411 | 342 | 411 | 411 | 411 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 23253360:live:2026-10-04 16:54:00+00 | live_pool | 2026-10-04 16:54 | 5 | - | 8/12 | 411 | 343 | 411 | 411 | 411 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 03ecf5d7:live:2026-10-04 16:54:00+00 | live_pool | 2026-10-04 16:54 | 5 | - | 8/12 | 411 | 342 | 411 | 411 | 411 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | e1c5ef9d:live:2026-10-04 16:54:00+00 | live_pool | 2026-10-04 16:54 | 5 | - | 8/12 | 411 | 342 | 411 | 411 | 411 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 23253360:live:2026-10-04 16:55:00+00 | live_pool | 2026-10-04 16:55 | 4 | - | 8/12 | 411 | 343 | 411 | 411 | 411 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 03ecf5d7:live:2026-10-04 16:55:00+00 | live_pool | 2026-10-04 16:55 | 4 | - | 8/12 | 411 | 342 | 411 | 411 | 411 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | e1c5ef9d:live:2026-10-04 16:55:00+00 | live_pool | 2026-10-04 16:55 | 4 | - | 8/12 | 411 | 342 | 411 | 411 | 411 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 23253360:live:2026-10-04 16:56:00+00 | live_pool | 2026-10-04 16:56 | 3 | - | 8/12 | 411 | 343 | 411 | 411 | 411 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 03ecf5d7:live:2026-10-04 16:56:00+00 | live_pool | 2026-10-04 16:56 | 3 | - | 8/12 | 411 | 342 | 411 | 411 | 411 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | e1c5ef9d:live:2026-10-04 16:56:00+00 | live_pool | 2026-10-04 16:56 | 3 | - | 8/12 | 411 | 342 | 411 | 411 | 411 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 23253360:live:2026-10-04 16:57:00+00 | live_pool | 2026-10-04 16:57 | 2 | - | 8/12 | 411 | 343 | 411 | 411 | 411 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 03ecf5d7:live:2026-10-04 16:57:00+00 | live_pool | 2026-10-04 16:57 | 2 | - | 8/12 | 411 | 342 | 411 | 411 | 411 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | e1c5ef9d:live:2026-10-04 16:57:00+00 | live_pool | 2026-10-04 16:57 | 2 | - | 8/12 | 411 | 342 | 411 | 411 | 411 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 23253360:live:2026-10-04 16:58:00+00 | live_pool | 2026-10-04 16:58 | 1 | - | 8/12 | 411 | 343 | 411 | 411 | 411 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 03ecf5d7:live:2026-10-04 16:58:00+00 | live_pool | 2026-10-04 16:58 | 1 | - | 8/12 | 411 | 342 | 411 | 411 | 411 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | e1c5ef9d:live:2026-10-04 16:58:00+00 | live_pool | 2026-10-04 16:58 | 1 | - | 8/12 | 411 | 342 | 411 | 411 | 411 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 23253360:live:2026-10-04 16:59:00+00 | live_pool | 2026-10-04 16:59 | 0 | - | 8/12 | 411 | 343 | 411 | 411 | 411 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 03ecf5d7:live:2026-10-04 16:59:00+00 | live_pool | 2026-10-04 16:59 | 0 | - | 8/12 | 411 | 342 | 411 | 411 | 411 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | e1c5ef9d:live:2026-10-04 16:59:00+00 | live_pool | 2026-10-04 16:59 | 0 | - | 8/12 | 411 | 342 | 411 | 411 | 411 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 23253360:live:2026-10-04 19:45:00+00 | live_pool | 2026-10-04 19:45 | -166 | - | 1/12 | 52 | 39 | 52 | 52 | 52 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 03ecf5d7:live:2026-10-04 19:45:00+00 | live_pool | 2026-10-04 19:45 | -166 | - | 1/12 | 52 | 39 | 52 | 52 | 52 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | e1c5ef9d:live:2026-10-04 19:45:00+00 | live_pool | 2026-10-04 19:45 | -166 | - | 1/12 | 52 | 39 | 52 | 52 | 52 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 23253360:live:2026-10-04 19:46:00+00 | live_pool | 2026-10-04 19:46 | -167 | - | 1/12 | 52 | 39 | 52 | 52 | 52 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 03ecf5d7:live:2026-10-04 19:46:00+00 | live_pool | 2026-10-04 19:46 | -167 | - | 1/12 | 52 | 39 | 52 | 52 | 52 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | e1c5ef9d:live:2026-10-04 19:46:00+00 | live_pool | 2026-10-04 19:46 | -167 | - | 1/12 | 52 | 39 | 52 | 52 | 52 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 23253360:live:2026-10-04 19:47:00+00 | live_pool | 2026-10-04 19:47 | -168 | - | 1/12 | 52 | 39 | 52 | 52 | 52 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 03ecf5d7:live:2026-10-04 19:47:00+00 | live_pool | 2026-10-04 19:47 | -168 | - | 1/12 | 52 | 39 | 52 | 52 | 52 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | e1c5ef9d:live:2026-10-04 19:47:00+00 | live_pool | 2026-10-04 19:47 | -168 | - | 1/12 | 52 | 39 | 52 | 52 | 52 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 23253360:live:2026-10-04 19:48:00+00 | live_pool | 2026-10-04 19:48 | -169 | - | 1/12 | 52 | 39 | 52 | 52 | 52 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 03ecf5d7:live:2026-10-04 19:48:00+00 | live_pool | 2026-10-04 19:48 | -169 | - | 1/12 | 52 | 39 | 52 | 52 | 52 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | e1c5ef9d:live:2026-10-04 19:48:00+00 | live_pool | 2026-10-04 19:48 | -169 | - | 1/12 | 52 | 39 | 52 | 52 | 52 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 23253360:live:2026-10-04 19:49:00+00 | live_pool | 2026-10-04 19:49 | -170 | - | 1/12 | 52 | 39 | 52 | 52 | 52 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 03ecf5d7:live:2026-10-04 19:49:00+00 | live_pool | 2026-10-04 19:49 | -170 | - | 1/12 | 52 | 39 | 52 | 52 | 52 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | e1c5ef9d:live:2026-10-04 19:49:00+00 | live_pool | 2026-10-04 19:49 | -170 | - | 1/12 | 52 | 39 | 52 | 52 | 52 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 23253360:live:2026-10-04 19:50:00+00 | live_pool | 2026-10-04 19:50 | -171 | - | 1/12 | 52 | 39 | 52 | 52 | 52 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 03ecf5d7:live:2026-10-04 19:50:00+00 | live_pool | 2026-10-04 19:50 | -171 | - | 1/12 | 52 | 39 | 52 | 52 | 52 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | e1c5ef9d:live:2026-10-04 19:50:00+00 | live_pool | 2026-10-04 19:50 | -171 | - | 1/12 | 52 | 39 | 52 | 52 | 52 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 23253360:live:2026-10-04 19:51:00+00 | live_pool | 2026-10-04 19:51 | -172 | - | 1/12 | 52 | 39 | 52 | 52 | 52 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 03ecf5d7:live:2026-10-04 19:51:00+00 | live_pool | 2026-10-04 19:51 | -172 | - | 1/12 | 52 | 39 | 52 | 52 | 52 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | e1c5ef9d:live:2026-10-04 19:51:00+00 | live_pool | 2026-10-04 19:51 | -172 | - | 1/12 | 52 | 39 | 52 | 52 | 52 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 23253360:live:2026-10-04 19:52:00+00 | live_pool | 2026-10-04 19:52 | -173 | - | 1/12 | 52 | 39 | 52 | 52 | 52 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 03ecf5d7:live:2026-10-04 19:52:00+00 | live_pool | 2026-10-04 19:52 | -173 | - | 1/12 | 52 | 39 | 52 | 52 | 52 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | e1c5ef9d:live:2026-10-04 19:52:00+00 | live_pool | 2026-10-04 19:52 | -173 | - | 1/12 | 52 | 39 | 52 | 52 | 52 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 23253360:live:2026-10-04 19:53:00+00 | live_pool | 2026-10-04 19:53 | -174 | - | 1/12 | 52 | 39 | 52 | 52 | 52 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 03ecf5d7:live:2026-10-04 19:53:00+00 | live_pool | 2026-10-04 19:53 | -174 | - | 1/12 | 52 | 39 | 52 | 52 | 52 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | e1c5ef9d:live:2026-10-04 19:53:00+00 | live_pool | 2026-10-04 19:53 | -174 | - | 1/12 | 52 | 39 | 52 | 52 | 52 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 23253360:live:2026-10-04 19:54:00+00 | live_pool | 2026-10-04 19:54 | -175 | - | 1/12 | 52 | 39 | 52 | 52 | 52 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 03ecf5d7:live:2026-10-04 19:54:00+00 | live_pool | 2026-10-04 19:54 | -175 | - | 1/12 | 52 | 39 | 52 | 52 | 52 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | e1c5ef9d:live:2026-10-04 19:54:00+00 | live_pool | 2026-10-04 19:54 | -175 | - | 1/12 | 52 | 39 | 52 | 52 | 52 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 23253360:live:2026-10-04 19:55:00+00 | live_pool | 2026-10-04 19:55 | -176 | - | 1/12 | 52 | 39 | 52 | 52 | 52 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 03ecf5d7:live:2026-10-04 19:55:00+00 | live_pool | 2026-10-04 19:55 | -176 | - | 1/12 | 52 | 39 | 52 | 52 | 52 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | e1c5ef9d:live:2026-10-04 19:55:00+00 | live_pool | 2026-10-04 19:55 | -176 | - | 1/12 | 52 | 39 | 52 | 52 | 52 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 23253360:live:2026-10-04 19:56:00+00 | live_pool | 2026-10-04 19:56 | -177 | - | 1/12 | 52 | 39 | 52 | 52 | 52 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 03ecf5d7:live:2026-10-04 19:56:00+00 | live_pool | 2026-10-04 19:56 | -177 | - | 1/12 | 52 | 39 | 52 | 52 | 52 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | e1c5ef9d:live:2026-10-04 19:56:00+00 | live_pool | 2026-10-04 19:56 | -177 | - | 1/12 | 52 | 39 | 52 | 52 | 52 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 23253360:live:2026-10-04 19:57:00+00 | live_pool | 2026-10-04 19:57 | -178 | - | 1/12 | 52 | 39 | 52 | 52 | 52 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 03ecf5d7:live:2026-10-04 19:57:00+00 | live_pool | 2026-10-04 19:57 | -178 | - | 1/12 | 52 | 39 | 52 | 52 | 52 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | e1c5ef9d:live:2026-10-04 19:57:00+00 | live_pool | 2026-10-04 19:57 | -178 | - | 1/12 | 52 | 39 | 52 | 52 | 52 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 23253360:live:2026-10-04 19:58:00+00 | live_pool | 2026-10-04 19:58 | -179 | - | 1/12 | 52 | 39 | 52 | 52 | 52 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 03ecf5d7:live:2026-10-04 19:58:00+00 | live_pool | 2026-10-04 19:58 | -179 | - | 1/12 | 52 | 39 | 52 | 52 | 52 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | e1c5ef9d:live:2026-10-04 19:58:00+00 | live_pool | 2026-10-04 19:58 | -179 | - | 1/12 | 52 | 39 | 52 | 52 | 52 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 23253360:live:2026-10-04 19:59:00+00 | live_pool | 2026-10-04 19:59 | -180 | - | 1/12 | 52 | 39 | 52 | 52 | 52 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 03ecf5d7:live:2026-10-04 19:59:00+00 | live_pool | 2026-10-04 19:59 | -180 | - | 1/12 | 52 | 39 | 52 | 52 | 52 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | e1c5ef9d:live:2026-10-04 19:59:00+00 | live_pool | 2026-10-04 19:59 | -180 | - | 1/12 | 52 | 39 | 52 | 52 | 52 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 23253360:live:2026-10-04 20:00:00+00 | live_pool | 2026-10-04 20:00 | -181 | - | 1/12 | 52 | 39 | 52 | 52 | 52 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 03ecf5d7:live:2026-10-04 20:00:00+00 | live_pool | 2026-10-04 20:00 | -181 | - | 1/12 | 52 | 39 | 52 | 52 | 52 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | e1c5ef9d:live:2026-10-04 20:00:00+00 | live_pool | 2026-10-04 20:00 | -181 | - | 1/12 | 52 | 39 | 52 | 52 | 52 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 23253360:live:2026-10-04 20:01:00+00 | live_pool | 2026-10-04 20:01 | -182 | - | 1/12 | 52 | 39 | 52 | 52 | 52 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 03ecf5d7:live:2026-10-04 20:01:00+00 | live_pool | 2026-10-04 20:01 | -182 | - | 1/12 | 52 | 39 | 52 | 52 | 52 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | e1c5ef9d:live:2026-10-04 20:01:00+00 | live_pool | 2026-10-04 20:01 | -182 | - | 1/12 | 52 | 39 | 52 | 52 | 52 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 23253360:live:2026-10-04 20:02:00+00 | live_pool | 2026-10-04 20:02 | -183 | - | 1/12 | 52 | 39 | 52 | 52 | 52 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 03ecf5d7:live:2026-10-04 20:02:00+00 | live_pool | 2026-10-04 20:02 | -183 | - | 1/12 | 52 | 39 | 52 | 52 | 52 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | e1c5ef9d:live:2026-10-04 20:02:00+00 | live_pool | 2026-10-04 20:02 | -183 | - | 1/12 | 52 | 39 | 52 | 52 | 52 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 23253360:live:2026-10-04 20:03:00+00 | live_pool | 2026-10-04 20:03 | -184 | - | 1/12 | 52 | 39 | 52 | 52 | 52 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 03ecf5d7:live:2026-10-04 20:03:00+00 | live_pool | 2026-10-04 20:03 | -184 | - | 1/12 | 52 | 39 | 52 | 52 | 52 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | e1c5ef9d:live:2026-10-04 20:03:00+00 | live_pool | 2026-10-04 20:03 | -184 | - | 1/12 | 52 | 39 | 52 | 52 | 52 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 23253360:live:2026-10-04 20:04:00+00 | live_pool | 2026-10-04 20:04 | -185 | - | 1/12 | 52 | 39 | 52 | 52 | 52 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 03ecf5d7:live:2026-10-04 20:04:00+00 | live_pool | 2026-10-04 20:04 | -185 | - | 1/12 | 52 | 39 | 52 | 52 | 52 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | e1c5ef9d:live:2026-10-04 20:04:00+00 | live_pool | 2026-10-04 20:04 | -185 | - | 1/12 | 52 | 39 | 52 | 52 | 52 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 23253360:live:2026-10-04 20:05:00+00 | live_pool | 2026-10-04 20:05 | -186 | - | 3/12 | 166 | 131 | 166 | 166 | 166 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 03ecf5d7:live:2026-10-04 20:05:00+00 | live_pool | 2026-10-04 20:05 | -186 | - | 3/12 | 166 | 131 | 166 | 166 | 166 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | e1c5ef9d:live:2026-10-04 20:05:00+00 | live_pool | 2026-10-04 20:05 | -186 | - | 3/12 | 166 | 131 | 166 | 166 | 166 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 23253360:live:2026-10-04 20:06:00+00 | live_pool | 2026-10-04 20:06 | -187 | - | 3/12 | 166 | 131 | 166 | 166 | 166 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 03ecf5d7:live:2026-10-04 20:06:00+00 | live_pool | 2026-10-04 20:06 | -187 | - | 3/12 | 166 | 131 | 166 | 166 | 166 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | e1c5ef9d:live:2026-10-04 20:06:00+00 | live_pool | 2026-10-04 20:06 | -187 | - | 3/12 | 166 | 131 | 166 | 166 | 166 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 23253360:live:2026-10-04 20:07:00+00 | live_pool | 2026-10-04 20:07 | -188 | - | 3/12 | 166 | 131 | 166 | 166 | 166 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 03ecf5d7:live:2026-10-04 20:07:00+00 | live_pool | 2026-10-04 20:07 | -188 | - | 3/12 | 166 | 131 | 166 | 166 | 166 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | e1c5ef9d:live:2026-10-04 20:07:00+00 | live_pool | 2026-10-04 20:07 | -188 | - | 3/12 | 166 | 131 | 166 | 166 | 166 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 23253360:live:2026-10-04 20:08:00+00 | live_pool | 2026-10-04 20:08 | -189 | - | 3/12 | 166 | 131 | 166 | 166 | 166 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 03ecf5d7:live:2026-10-04 20:08:00+00 | live_pool | 2026-10-04 20:08 | -189 | - | 3/12 | 166 | 131 | 166 | 166 | 166 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | e1c5ef9d:live:2026-10-04 20:08:00+00 | live_pool | 2026-10-04 20:08 | -189 | - | 3/12 | 166 | 131 | 166 | 166 | 166 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 23253360:live:2026-10-04 20:09:00+00 | live_pool | 2026-10-04 20:09 | -190 | - | 3/12 | 166 | 131 | 166 | 166 | 166 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 03ecf5d7:live:2026-10-04 20:09:00+00 | live_pool | 2026-10-04 20:09 | -190 | - | 3/12 | 166 | 131 | 166 | 166 | 166 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | e1c5ef9d:live:2026-10-04 20:09:00+00 | live_pool | 2026-10-04 20:09 | -190 | - | 3/12 | 166 | 131 | 166 | 166 | 166 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 23253360:live:2026-10-04 20:10:00+00 | live_pool | 2026-10-04 20:10 | -191 | - | 3/12 | 166 | 131 | 166 | 166 | 166 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 03ecf5d7:live:2026-10-04 20:10:00+00 | live_pool | 2026-10-04 20:10 | -191 | - | 3/12 | 166 | 131 | 166 | 166 | 166 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | e1c5ef9d:live:2026-10-04 20:10:00+00 | live_pool | 2026-10-04 20:10 | -191 | - | 3/12 | 166 | 131 | 166 | 166 | 166 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 23253360:live:2026-10-04 20:11:00+00 | live_pool | 2026-10-04 20:11 | -192 | - | 3/12 | 166 | 131 | 166 | 166 | 166 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 03ecf5d7:live:2026-10-04 20:11:00+00 | live_pool | 2026-10-04 20:11 | -192 | - | 3/12 | 166 | 131 | 166 | 166 | 166 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | e1c5ef9d:live:2026-10-04 20:11:00+00 | live_pool | 2026-10-04 20:11 | -192 | - | 3/12 | 166 | 131 | 166 | 166 | 166 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 23253360:live:2026-10-04 20:12:00+00 | live_pool | 2026-10-04 20:12 | -193 | - | 3/12 | 166 | 131 | 166 | 166 | 166 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 03ecf5d7:live:2026-10-04 20:12:00+00 | live_pool | 2026-10-04 20:12 | -193 | - | 3/12 | 166 | 131 | 166 | 166 | 166 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | e1c5ef9d:live:2026-10-04 20:12:00+00 | live_pool | 2026-10-04 20:12 | -193 | - | 3/12 | 166 | 131 | 166 | 166 | 166 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 23253360:live:2026-10-04 20:13:00+00 | live_pool | 2026-10-04 20:13 | -194 | - | 3/12 | 166 | 131 | 166 | 166 | 166 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 03ecf5d7:live:2026-10-04 20:13:00+00 | live_pool | 2026-10-04 20:13 | -194 | - | 3/12 | 166 | 131 | 166 | 166 | 166 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | e1c5ef9d:live:2026-10-04 20:13:00+00 | live_pool | 2026-10-04 20:13 | -194 | - | 3/12 | 166 | 131 | 166 | 166 | 166 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 23253360:live:2026-10-04 20:14:00+00 | live_pool | 2026-10-04 20:14 | -195 | - | 3/12 | 166 | 131 | 166 | 166 | 166 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 03ecf5d7:live:2026-10-04 20:14:00+00 | live_pool | 2026-10-04 20:14 | -195 | - | 3/12 | 166 | 131 | 166 | 166 | 166 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | e1c5ef9d:live:2026-10-04 20:14:00+00 | live_pool | 2026-10-04 20:14 | -195 | - | 3/12 | 166 | 131 | 166 | 166 | 166 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 23253360:live:2026-10-04 20:15:00+00 | live_pool | 2026-10-04 20:15 | -196 | - | 3/12 | 166 | 131 | 166 | 166 | 166 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 03ecf5d7:live:2026-10-04 20:15:00+00 | live_pool | 2026-10-04 20:15 | -196 | - | 3/12 | 166 | 131 | 166 | 166 | 166 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | e1c5ef9d:live:2026-10-04 20:15:00+00 | live_pool | 2026-10-04 20:15 | -196 | - | 3/12 | 166 | 131 | 166 | 166 | 166 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 23253360:live:2026-10-04 20:16:00+00 | live_pool | 2026-10-04 20:16 | -197 | - | 3/12 | 166 | 131 | 166 | 166 | 166 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 03ecf5d7:live:2026-10-04 20:16:00+00 | live_pool | 2026-10-04 20:16 | -197 | - | 3/12 | 166 | 131 | 166 | 166 | 166 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | e1c5ef9d:live:2026-10-04 20:16:00+00 | live_pool | 2026-10-04 20:16 | -197 | - | 3/12 | 166 | 131 | 166 | 166 | 166 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 23253360:live:2026-10-04 20:17:00+00 | live_pool | 2026-10-04 20:17 | -198 | - | 3/12 | 166 | 131 | 166 | 166 | 166 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 03ecf5d7:live:2026-10-04 20:17:00+00 | live_pool | 2026-10-04 20:17 | -198 | - | 3/12 | 166 | 131 | 166 | 166 | 166 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | e1c5ef9d:live:2026-10-04 20:17:00+00 | live_pool | 2026-10-04 20:17 | -198 | - | 3/12 | 166 | 131 | 166 | 166 | 166 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 23253360:live:2026-10-04 20:18:00+00 | live_pool | 2026-10-04 20:18 | -199 | - | 3/12 | 166 | 131 | 166 | 166 | 166 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 03ecf5d7:live:2026-10-04 20:18:00+00 | live_pool | 2026-10-04 20:18 | -199 | - | 3/12 | 166 | 131 | 166 | 166 | 166 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | e1c5ef9d:live:2026-10-04 20:18:00+00 | live_pool | 2026-10-04 20:18 | -199 | - | 3/12 | 166 | 131 | 166 | 166 | 166 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 23253360:live:2026-10-04 20:19:00+00 | live_pool | 2026-10-04 20:19 | -200 | - | 3/12 | 166 | 131 | 166 | 166 | 166 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 03ecf5d7:live:2026-10-04 20:19:00+00 | live_pool | 2026-10-04 20:19 | -200 | - | 3/12 | 166 | 131 | 166 | 166 | 166 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | e1c5ef9d:live:2026-10-04 20:19:00+00 | live_pool | 2026-10-04 20:19 | -200 | - | 3/12 | 166 | 131 | 166 | 166 | 166 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 23253360:live:2026-10-04 20:20:00+00 | live_pool | 2026-10-04 20:20 | -201 | - | 3/12 | 166 | 131 | 166 | 166 | 166 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 03ecf5d7:live:2026-10-04 20:20:00+00 | live_pool | 2026-10-04 20:20 | -201 | - | 3/12 | 166 | 131 | 166 | 166 | 166 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | e1c5ef9d:live:2026-10-04 20:20:00+00 | live_pool | 2026-10-04 20:20 | -201 | - | 3/12 | 166 | 131 | 166 | 166 | 166 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 23253360:live:2026-10-04 20:21:00+00 | live_pool | 2026-10-04 20:21 | -202 | - | 3/12 | 166 | 131 | 166 | 166 | 166 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 03ecf5d7:live:2026-10-04 20:21:00+00 | live_pool | 2026-10-04 20:21 | -202 | - | 3/12 | 166 | 131 | 166 | 166 | 166 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | e1c5ef9d:live:2026-10-04 20:21:00+00 | live_pool | 2026-10-04 20:21 | -202 | - | 3/12 | 166 | 131 | 166 | 166 | 166 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 23253360:live:2026-10-04 20:22:00+00 | live_pool | 2026-10-04 20:22 | -203 | - | 3/12 | 166 | 131 | 166 | 166 | 166 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 03ecf5d7:live:2026-10-04 20:22:00+00 | live_pool | 2026-10-04 20:22 | -203 | - | 3/12 | 166 | 131 | 166 | 166 | 166 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | e1c5ef9d:live:2026-10-04 20:22:00+00 | live_pool | 2026-10-04 20:22 | -203 | - | 3/12 | 166 | 131 | 166 | 166 | 166 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 23253360:live:2026-10-04 20:23:00+00 | live_pool | 2026-10-04 20:23 | -204 | - | 3/12 | 166 | 131 | 166 | 166 | 166 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 03ecf5d7:live:2026-10-04 20:23:00+00 | live_pool | 2026-10-04 20:23 | -204 | - | 3/12 | 166 | 131 | 166 | 166 | 166 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | e1c5ef9d:live:2026-10-04 20:23:00+00 | live_pool | 2026-10-04 20:23 | -204 | - | 3/12 | 166 | 131 | 166 | 166 | 166 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 23253360:live:2026-10-04 20:24:00+00 | live_pool | 2026-10-04 20:24 | -205 | - | 3/12 | 166 | 131 | 166 | 166 | 166 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 03ecf5d7:live:2026-10-04 20:24:00+00 | live_pool | 2026-10-04 20:24 | -205 | - | 3/12 | 166 | 131 | 166 | 166 | 166 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | e1c5ef9d:live:2026-10-04 20:24:00+00 | live_pool | 2026-10-04 20:24 | -205 | - | 3/12 | 166 | 131 | 166 | 166 | 166 | nfl-ownership-prior-v2 | 0 | 0 |

### showdown DET@CAR — week 4, first kickoff 2026-10-05 00:20 UTC

Uploads 2 (upload); contests 196285055; status **canonical**.

| source | id | origin | at (UTC) | min before kickoff | horizon | games | players | proj | avail | dkStatus | ownPct | ownership version | anchor | commit |
|---|---|---|---|---:|---|---|---:|---:|---:|---:|---:|---|---:|---:|
| pool_capture | b9c1b090:live:2026-10-04 21:56:00+00 | live_pool | 2026-10-04 21:56 | 143 | EE* | 1/1 | 50 | 41 | 50 | 50 | 50 | nfl-ownership-prior-v2 | 0 | 0 |
| optimizer_run | a766624d | complete/gpp | 2026-10-04 21:58 | 141 | E | all | 50 | 41 | 50 | 50 | 50 | linestar, nfl-ownership-prior-v2 | 0 | 0 |
| optimizer_run | 6323aafc | complete/gpp | 2026-10-04 22:00 | 139 | - | all | 50 | 41 | 50 | 50 | 50 | linestar, nfl-ownership-prior-v2 | 0 | 0 |
| optimizer_run | ef2b4e15 | complete/gpp | 2026-10-04 22:02 | 137 | - | all | 50 | 41 | 50 | 50 | 50 | linestar, nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 35ec7c6c:live:2026-10-04 22:13:00+00 | live_pool | 2026-10-04 22:13 | 126 | C | 1/1 | 50 | 41 | 50 | 50 | 50 | nfl-ownership-prior-v2 | 0 | 0 |
| optimizer_run | 102dce40 | complete/gpp | 2026-10-04 22:18 | 122 | CC* | all | 50 | 41 | 50 | 50 | 50 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 35ec7c6c:live:2026-10-05 00:00:00+00 | live_pool | 2026-10-05 00:00 | 19 | - | 1/1 | 50 | 41 | 50 | 50 | 50 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 35ec7c6c:live:2026-10-05 00:01:00+00 | live_pool | 2026-10-05 00:01 | 18 | - | 1/1 | 50 | 41 | 50 | 50 | 50 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 35ec7c6c:live:2026-10-05 00:02:00+00 | live_pool | 2026-10-05 00:02 | 17 | - | 1/1 | 50 | 41 | 50 | 50 | 50 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 35ec7c6c:live:2026-10-05 00:03:00+00 | live_pool | 2026-10-05 00:03 | 16 | - | 1/1 | 50 | 41 | 50 | 50 | 50 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 35ec7c6c:live:2026-10-05 00:04:00+00 | live_pool | 2026-10-05 00:04 | 15 | - | 1/1 | 50 | 41 | 50 | 50 | 50 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 35ec7c6c:live:2026-10-05 00:05:00+00 | live_pool | 2026-10-05 00:05 | 14 | - | 1/1 | 50 | 41 | 50 | 50 | 50 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 35ec7c6c:live:2026-10-05 00:06:00+00 | live_pool | 2026-10-05 00:06 | 13 | - | 1/1 | 50 | 41 | 50 | 50 | 50 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 35ec7c6c:live:2026-10-05 00:07:00+00 | live_pool | 2026-10-05 00:07 | 12 | - | 1/1 | 50 | 41 | 50 | 50 | 50 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 35ec7c6c:live:2026-10-05 00:08:00+00 | live_pool | 2026-10-05 00:08 | 11 | - | 1/1 | 50 | 41 | 50 | 50 | 50 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 35ec7c6c:live:2026-10-05 00:09:00+00 | live_pool | 2026-10-05 00:09 | 10 | - | 1/1 | 50 | 41 | 50 | 50 | 50 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 35ec7c6c:live:2026-10-05 00:10:00+00 | live_pool | 2026-10-05 00:10 | 9 | - | 1/1 | 50 | 41 | 50 | 50 | 50 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 35ec7c6c:live:2026-10-05 00:11:00+00 | live_pool | 2026-10-05 00:11 | 8 | - | 1/1 | 50 | 41 | 50 | 50 | 50 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 35ec7c6c:live:2026-10-05 00:12:00+00 | live_pool | 2026-10-05 00:12 | 7 | - | 1/1 | 50 | 41 | 50 | 50 | 50 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 35ec7c6c:live:2026-10-05 00:13:00+00 | live_pool | 2026-10-05 00:13 | 6 | - | 1/1 | 50 | 41 | 50 | 50 | 50 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 35ec7c6c:live:2026-10-05 00:14:00+00 | live_pool | 2026-10-05 00:14 | 5 | - | 1/1 | 50 | 41 | 50 | 50 | 50 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 35ec7c6c:live:2026-10-05 00:15:00+00 | live_pool | 2026-10-05 00:15 | 4 | - | 1/1 | 50 | 41 | 50 | 50 | 50 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 35ec7c6c:live:2026-10-05 00:16:00+00 | live_pool | 2026-10-05 00:16 | 3 | - | 1/1 | 50 | 41 | 50 | 50 | 50 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 35ec7c6c:live:2026-10-05 00:17:00+00 | live_pool | 2026-10-05 00:17 | 2 | - | 1/1 | 50 | 41 | 50 | 50 | 50 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 35ec7c6c:live:2026-10-05 00:18:00+00 | live_pool | 2026-10-05 00:18 | 1 | - | 1/1 | 50 | 41 | 50 | 50 | 50 | nfl-ownership-prior-v2 | 0 | 0 |
| pool_capture | 35ec7c6c:live:2026-10-05 00:19:00+00 | live_pool | 2026-10-05 00:19 | 0 | - | 1/1 | 50 | 41 | 50 | 50 | 50 | nfl-ownership-prior-v2 | 0 | 0 |

### showdown ATL@NO — week 4, first kickoff 2026-10-06 00:15 UTC

Uploads 2 (upload); contests 196285155, 196285178; status **canonical**.

| source | id | origin | at (UTC) | min before kickoff | horizon | games | players | proj | avail | dkStatus | ownPct | ownership version | anchor | commit |
|---|---|---|---|---:|---|---|---:|---:|---:|---:|---:|---|---:|---:|
| pool_capture | 2bd233db:live:2026-10-05 11:14:00+00 | live_pool | 2026-10-05 11:14 | 780 | EE* | 1/1 | 56 | 48 | 56 | 56 | 56 | nfl-ownership-prior-v3 | 0 | 0 |
| optimizer_run | cf4f6182 | complete/gpp | 2026-10-05 11:14 | 780 | E | all | 56 | 48 | 56 | 56 | 56 | nfl-ownership-prior-v3 | 0 | 0 |
| optimizer_run | 8410f051 | complete/gpp | 2026-10-05 11:15 | 779 | - | all | 56 | 48 | 56 | 56 | 56 | nfl-ownership-prior-v3 | 0 | 0 |
| optimizer_run | bbdb5b94 | complete/gpp | 2026-10-05 21:52 | 142 | - | all | 56 | 48 | 56 | 56 | 56 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 815a5a5d:live:2026-10-05 21:52:00+00 | live_pool | 2026-10-05 21:52 | 142 | C | 1/1 | 56 | 48 | 56 | 56 | 56 | nfl-ownership-prior-v3 | 0 | 0 |
| optimizer_run | 17cb1d86 | complete/gpp | 2026-10-05 21:55 | 140 | CC* | all | 56 | 48 | 56 | 56 | 56 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 815a5a5d:live:2026-10-05 23:55:00+00 | live_pool | 2026-10-05 23:55 | 19 | - | 1/1 | 56 | 48 | 56 | 56 | 56 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 815a5a5d:live:2026-10-05 23:56:00+00 | live_pool | 2026-10-05 23:56 | 18 | - | 1/1 | 56 | 48 | 56 | 56 | 56 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 815a5a5d:live:2026-10-05 23:57:00+00 | live_pool | 2026-10-05 23:57 | 17 | - | 1/1 | 56 | 48 | 56 | 56 | 56 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 815a5a5d:live:2026-10-05 23:58:00+00 | live_pool | 2026-10-05 23:58 | 16 | - | 1/1 | 56 | 48 | 56 | 56 | 56 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 815a5a5d:live:2026-10-05 23:59:00+00 | live_pool | 2026-10-05 23:59 | 15 | - | 1/1 | 56 | 48 | 56 | 56 | 56 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 815a5a5d:live:2026-10-06 00:00:00+00 | live_pool | 2026-10-06 00:00 | 14 | - | 1/1 | 56 | 48 | 56 | 56 | 56 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 815a5a5d:live:2026-10-06 00:01:00+00 | live_pool | 2026-10-06 00:01 | 13 | - | 1/1 | 56 | 48 | 56 | 56 | 56 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 815a5a5d:live:2026-10-06 00:02:00+00 | live_pool | 2026-10-06 00:02 | 12 | - | 1/1 | 56 | 48 | 56 | 56 | 56 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 815a5a5d:live:2026-10-06 00:03:00+00 | live_pool | 2026-10-06 00:03 | 11 | - | 1/1 | 56 | 48 | 56 | 56 | 56 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 815a5a5d:live:2026-10-06 00:04:00+00 | live_pool | 2026-10-06 00:04 | 10 | - | 1/1 | 56 | 48 | 56 | 56 | 56 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 815a5a5d:live:2026-10-06 00:05:00+00 | live_pool | 2026-10-06 00:05 | 9 | - | 1/1 | 56 | 48 | 56 | 56 | 56 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 815a5a5d:live:2026-10-06 00:06:00+00 | live_pool | 2026-10-06 00:06 | 8 | - | 1/1 | 56 | 48 | 56 | 56 | 56 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 815a5a5d:live:2026-10-06 00:07:00+00 | live_pool | 2026-10-06 00:07 | 7 | - | 1/1 | 56 | 48 | 56 | 56 | 56 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 815a5a5d:live:2026-10-06 00:08:00+00 | live_pool | 2026-10-06 00:08 | 6 | - | 1/1 | 56 | 48 | 56 | 56 | 56 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 815a5a5d:live:2026-10-06 00:09:00+00 | live_pool | 2026-10-06 00:09 | 5 | - | 1/1 | 56 | 48 | 56 | 56 | 56 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 815a5a5d:live:2026-10-06 00:10:00+00 | live_pool | 2026-10-06 00:10 | 4 | - | 1/1 | 56 | 48 | 56 | 56 | 56 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 815a5a5d:live:2026-10-06 00:11:00+00 | live_pool | 2026-10-06 00:11 | 3 | - | 1/1 | 56 | 48 | 56 | 56 | 56 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 815a5a5d:live:2026-10-06 00:12:00+00 | live_pool | 2026-10-06 00:12 | 2 | - | 1/1 | 56 | 48 | 56 | 56 | 56 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 815a5a5d:live:2026-10-06 00:13:00+00 | live_pool | 2026-10-06 00:13 | 1 | - | 1/1 | 56 | 48 | 56 | 56 | 56 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 815a5a5d:live:2026-10-06 00:14:00+00 | live_pool | 2026-10-06 00:14 | 0 | - | 1/1 | 56 | 48 | 56 | 56 | 56 | nfl-ownership-prior-v3 | 0 | 0 |

### showdown TB@DAL — week 5, first kickoff 2026-10-09 00:15 UTC

Uploads 1 (upload); contests 196438550, dfsdb-1e8fbcd7-6f69-47ef-8417-d8d3f4acbbed, dfsdb-3605c6fb-b8ce-46bb-b829-9396d0982010, dfsdb-3b42531a-94ed-461b-b420-f417ebb28ec6, dfsdb-5449aea7-db27-410d-8c54-91df2ad90241, dfsdb-8ac82129-f1d6-4bc9-af29-5a6219b23483, dfsdb-9a830c4f-7351-4575-a75b-1237382766fa, dfsdb-9e8a781e-3c88-498c-992e-5c5d5a665160, dfsdb-cf22c10c-7c06-4de9-9f35-57355b2da209; status **canonical**.

| source | id | origin | at (UTC) | min before kickoff | horizon | games | players | proj | avail | dkStatus | ownPct | ownership version | anchor | commit |
|---|---|---|---|---:|---|---|---:|---:|---:|---:|---:|---|---:|---:|
| pool_capture | 96614eda:live:2026-10-08 00:15:00+00 | live_pool | 2026-10-08 00:15 | 1439 | CEC*E* | 1/1 | 52 | 44 | 52 | 52 | 52 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 96614eda:live:2026-10-08 23:55:00+00 | live_pool | 2026-10-08 23:55 | 19 | - | 1/1 | 52 | 44 | 52 | 52 | 52 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 96614eda:live:2026-10-08 23:56:00+00 | live_pool | 2026-10-08 23:56 | 18 | - | 1/1 | 52 | 44 | 52 | 52 | 52 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 96614eda:live:2026-10-08 23:57:00+00 | live_pool | 2026-10-08 23:57 | 17 | - | 1/1 | 52 | 44 | 52 | 52 | 52 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 96614eda:live:2026-10-08 23:58:00+00 | live_pool | 2026-10-08 23:58 | 16 | - | 1/1 | 52 | 44 | 52 | 52 | 52 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 96614eda:live:2026-10-08 23:59:00+00 | live_pool | 2026-10-08 23:59 | 15 | - | 1/1 | 52 | 44 | 52 | 52 | 52 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 96614eda:live:2026-10-09 00:00:00+00 | live_pool | 2026-10-09 00:00 | 14 | - | 1/1 | 52 | 44 | 52 | 52 | 52 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 96614eda:live:2026-10-09 00:01:00+00 | live_pool | 2026-10-09 00:01 | 13 | - | 1/1 | 52 | 44 | 52 | 52 | 52 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 96614eda:live:2026-10-09 00:02:00+00 | live_pool | 2026-10-09 00:02 | 12 | - | 1/1 | 52 | 44 | 52 | 52 | 52 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 96614eda:live:2026-10-09 00:03:00+00 | live_pool | 2026-10-09 00:03 | 11 | - | 1/1 | 52 | 44 | 52 | 52 | 52 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 96614eda:live:2026-10-09 00:04:00+00 | live_pool | 2026-10-09 00:04 | 10 | - | 1/1 | 52 | 44 | 52 | 52 | 52 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 96614eda:live:2026-10-09 00:05:00+00 | live_pool | 2026-10-09 00:05 | 9 | - | 1/1 | 52 | 44 | 52 | 52 | 52 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 96614eda:live:2026-10-09 00:06:00+00 | live_pool | 2026-10-09 00:06 | 8 | - | 1/1 | 52 | 44 | 52 | 52 | 52 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 96614eda:live:2026-10-09 00:07:00+00 | live_pool | 2026-10-09 00:07 | 7 | - | 1/1 | 52 | 44 | 52 | 52 | 52 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 96614eda:live:2026-10-09 00:08:00+00 | live_pool | 2026-10-09 00:08 | 6 | - | 1/1 | 52 | 44 | 52 | 52 | 52 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 96614eda:live:2026-10-09 00:09:00+00 | live_pool | 2026-10-09 00:09 | 5 | - | 1/1 | 52 | 44 | 52 | 52 | 52 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 96614eda:live:2026-10-09 00:10:00+00 | live_pool | 2026-10-09 00:10 | 4 | - | 1/1 | 52 | 44 | 52 | 52 | 52 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 96614eda:live:2026-10-09 00:11:00+00 | live_pool | 2026-10-09 00:11 | 3 | - | 1/1 | 52 | 44 | 52 | 52 | 52 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 96614eda:live:2026-10-09 00:12:00+00 | live_pool | 2026-10-09 00:12 | 2 | - | 1/1 | 52 | 44 | 52 | 52 | 52 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 96614eda:live:2026-10-09 00:13:00+00 | live_pool | 2026-10-09 00:13 | 1 | - | 1/1 | 52 | 44 | 52 | 52 | 52 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 96614eda:live:2026-10-09 00:14:00+00 | live_pool | 2026-10-09 00:14 | 0 | - | 1/1 | 52 | 44 | 52 | 52 | 52 | nfl-ownership-prior-v3 | 0 | 0 |

### showdown PHI@JAX — week 5, first kickoff 2026-10-11 13:30 UTC

Uploads 2 (upload); contests none; status **canonical**.

| source | id | origin | at (UTC) | min before kickoff | horizon | games | players | proj | avail | dkStatus | ownPct | ownership version | anchor | commit |
|---|---|---|---|---:|---|---|---:|---:|---:|---:|---:|---|---:|---:|
| pool_capture | 35fe0876:live:2026-10-10 13:30:00+00 | live_pool | 2026-10-10 13:30 | 1439 | EE* | 1/1 | 56 | 47 | 56 | 56 | 56 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 50dbda76:live:2026-10-10 23:20:00+00 | live_pool | 2026-10-10 23:20 | 849 | C | 1/1 | 56 | 47 | 56 | 56 | 56 | nfl-ownership-prior-v3 | 0 | 0 |
| optimizer_run | f56e7cc3 | complete/gpp | 2026-10-10 23:21 | 848 | E | all | 56 | 47 | 56 | 56 | 56 | nfl-ownership-prior-v3 | 0 | 0 |
| optimizer_run | cb4b2dca | complete/gpp | 2026-10-10 23:31 | 839 | - | all | 56 | 47 | 56 | 56 | 56 | nfl-ownership-prior-v3 | 0 | 0 |
| optimizer_run | 6b3a39ba | complete/gpp | 2026-10-11 00:19 | 791 | CC* | all | 56 | 47 | 56 | 56 | 56 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 50dbda76:live:2026-10-11 13:10:00+00 | live_pool | 2026-10-11 13:10 | 19 | - | 1/1 | 56 | 48 | 56 | 56 | 56 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 50dbda76:live:2026-10-11 13:11:00+00 | live_pool | 2026-10-11 13:11 | 18 | - | 1/1 | 56 | 48 | 56 | 56 | 56 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 50dbda76:live:2026-10-11 13:12:00+00 | live_pool | 2026-10-11 13:12 | 17 | - | 1/1 | 56 | 48 | 56 | 56 | 56 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 50dbda76:live:2026-10-11 13:13:00+00 | live_pool | 2026-10-11 13:13 | 16 | - | 1/1 | 56 | 48 | 56 | 56 | 56 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 50dbda76:live:2026-10-11 13:14:00+00 | live_pool | 2026-10-11 13:14 | 15 | - | 1/1 | 56 | 48 | 56 | 56 | 56 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 50dbda76:live:2026-10-11 13:15:00+00 | live_pool | 2026-10-11 13:15 | 14 | - | 1/1 | 56 | 48 | 56 | 56 | 56 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 50dbda76:live:2026-10-11 13:16:00+00 | live_pool | 2026-10-11 13:16 | 13 | - | 1/1 | 56 | 48 | 56 | 56 | 56 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 50dbda76:live:2026-10-11 13:17:00+00 | live_pool | 2026-10-11 13:17 | 12 | - | 1/1 | 56 | 48 | 56 | 56 | 56 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 50dbda76:live:2026-10-11 13:18:00+00 | live_pool | 2026-10-11 13:18 | 11 | - | 1/1 | 56 | 48 | 56 | 56 | 56 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 50dbda76:live:2026-10-11 13:19:00+00 | live_pool | 2026-10-11 13:19 | 10 | - | 1/1 | 56 | 48 | 56 | 56 | 56 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 50dbda76:live:2026-10-11 13:20:00+00 | live_pool | 2026-10-11 13:20 | 9 | - | 1/1 | 56 | 48 | 56 | 56 | 56 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 50dbda76:live:2026-10-11 13:21:00+00 | live_pool | 2026-10-11 13:21 | 8 | - | 1/1 | 56 | 48 | 56 | 56 | 56 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 50dbda76:live:2026-10-11 13:22:00+00 | live_pool | 2026-10-11 13:22 | 7 | - | 1/1 | 56 | 48 | 56 | 56 | 56 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 50dbda76:live:2026-10-11 13:23:00+00 | live_pool | 2026-10-11 13:23 | 6 | - | 1/1 | 56 | 48 | 56 | 56 | 56 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 50dbda76:live:2026-10-11 13:24:00+00 | live_pool | 2026-10-11 13:24 | 5 | - | 1/1 | 56 | 48 | 56 | 56 | 56 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 50dbda76:live:2026-10-11 13:25:00+00 | live_pool | 2026-10-11 13:25 | 4 | - | 1/1 | 56 | 48 | 56 | 56 | 56 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 50dbda76:live:2026-10-11 13:26:00+00 | live_pool | 2026-10-11 13:26 | 3 | - | 1/1 | 56 | 48 | 56 | 56 | 56 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 50dbda76:live:2026-10-11 13:27:00+00 | live_pool | 2026-10-11 13:27 | 2 | - | 1/1 | 56 | 48 | 56 | 56 | 56 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 50dbda76:live:2026-10-11 13:28:00+00 | live_pool | 2026-10-11 13:28 | 1 | - | 1/1 | 56 | 48 | 56 | 56 | 56 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 50dbda76:live:2026-10-11 13:29:00+00 | live_pool | 2026-10-11 13:29 | 0 | - | 1/1 | 56 | 48 | 56 | 56 | 56 | nfl-ownership-prior-v3 | 0 | 0 |

### classic 11 games — week 5, first kickoff 2026-10-11 17:00 UTC

Uploads 6 (upload); contests 196403752; status **canonical**.

| source | id | origin | at (UTC) | min before kickoff | horizon | games | players | proj | avail | dkStatus | ownPct | ownership version | anchor | commit |
|---|---|---|---|---:|---|---|---:|---:|---:|---:|---:|---|---:|---:|
| pool_capture | 43dc5b64:live:2026-10-10 17:00:00+00 | live_pool | 2026-10-10 17:00 | 1439 | EE* | 8/11 | 424 | 352 | 424 | 424 | 424 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | f7b6ffd5:live:2026-10-10 17:05:00+00 | live_pool | 2026-10-10 17:05 | 1434 | - | 8/11 | 424 | 354 | 424 | 424 | 424 | nfl-ownership-prior-v3 | 0 | 0 |
| optimizer_run | 00cc9229 | complete/gpp | 2026-10-10 17:08 | 1432 | E | all | 587 | 481 | 587 | 587 | 587 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 43dc5b64:live:2026-10-10 20:05:00+00 | live_pool | 2026-10-10 20:05 | 1254 | - | 1/11 | 53 | 40 | 53 | 53 | 53 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | f7b6ffd5:live:2026-10-10 20:06:00+00 | live_pool | 2026-10-10 20:06 | 1254 | - | 1/11 | 53 | 40 | 53 | 53 | 53 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 43dc5b64:live:2026-10-10 20:25:00+00 | live_pool | 2026-10-10 20:25 | 1234 | - | 2/11 | 110 | 87 | 110 | 110 | 110 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | f7b6ffd5:live:2026-10-10 20:26:00+00 | live_pool | 2026-10-10 20:26 | 1234 | - | 2/11 | 110 | 87 | 110 | 110 | 110 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 404c8ff5:live:2026-10-10 22:52:00+00 | live_pool | 2026-10-10 22:52 | 1087 | - | 11/11 | 587 | 475 | 587 | 587 | 587 | nfl-ownership-prior-v3 | 0 | 0 |
| optimizer_run | 0b216409 | complete/gpp | 2026-10-10 22:57 | 1082 | - | all | 587 | 475 | 587 | 587 | 587 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | a80bf273:live:2026-10-11 14:31:00+00 | live_pool | 2026-10-11 14:31 | 148 | - | 11/11 | 587 | 480 | 587 | 587 | 587 | nfl-ownership-prior-v3 | 0 | 0 |
| optimizer_run | 6ab06c5b | complete/gpp | 2026-10-11 14:37 | 143 | - | all | 587 | 480 | 587 | 587 | 587 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | f1683510:live:2026-10-11 15:07:00+00 | live_pool | 2026-10-11 15:07 | 112 | C | 11/11 | 587 | 480 | 587 | 587 | 587 | nfl-ownership-prior-v3 | 0 | 15 |
| optimizer_run | 4c50a8d6 | complete/gpp | 2026-10-11 15:09 | 110 | - | all | 587 | 480 | 587 | 587 | 587 | nfl-ownership-prior-v3 | 0 | 15 |
| optimizer_run | 9fa63c82 | complete/gpp | 2026-10-11 15:37 | 82 | CC* | all | 587 | 480 | 587 | 587 | 587 | nfl-ownership-prior-v3 | 0 | 15 |
| pool_capture | 43dc5b64:live:2026-10-11 16:40:00+00 | live_pool | 2026-10-11 16:40 | 19 | - | 8/11 | 424 | 357 | 424 | 424 | 424 | nfl-ownership-prior-v3 | 0 | 15 |
| pool_capture | f1683510:live:2026-10-11 16:40:00+00 | live_pool | 2026-10-11 16:40 | 19 | - | 8/11 | 424 | 357 | 424 | 424 | 424 | nfl-ownership-prior-v3 | 0 | 15 |
| pool_capture | 43dc5b64:live:2026-10-11 16:41:00+00 | live_pool | 2026-10-11 16:41 | 18 | - | 8/11 | 424 | 357 | 424 | 424 | 424 | nfl-ownership-prior-v3 | 0 | 15 |
| pool_capture | f1683510:live:2026-10-11 16:41:00+00 | live_pool | 2026-10-11 16:41 | 18 | - | 8/11 | 424 | 357 | 424 | 424 | 424 | nfl-ownership-prior-v3 | 0 | 15 |
| pool_capture | 43dc5b64:live:2026-10-11 16:42:00+00 | live_pool | 2026-10-11 16:42 | 17 | - | 8/11 | 424 | 357 | 424 | 424 | 424 | nfl-ownership-prior-v3 | 0 | 15 |
| pool_capture | f1683510:live:2026-10-11 16:42:00+00 | live_pool | 2026-10-11 16:42 | 17 | - | 8/11 | 424 | 357 | 424 | 424 | 424 | nfl-ownership-prior-v3 | 0 | 15 |
| pool_capture | 43dc5b64:live:2026-10-11 16:43:00+00 | live_pool | 2026-10-11 16:43 | 16 | - | 8/11 | 424 | 357 | 424 | 424 | 424 | nfl-ownership-prior-v3 | 0 | 15 |
| pool_capture | f1683510:live:2026-10-11 16:43:00+00 | live_pool | 2026-10-11 16:43 | 16 | - | 8/11 | 424 | 357 | 424 | 424 | 424 | nfl-ownership-prior-v3 | 0 | 15 |
| pool_capture | 43dc5b64:live:2026-10-11 16:44:00+00 | live_pool | 2026-10-11 16:44 | 15 | - | 8/11 | 424 | 357 | 424 | 424 | 424 | nfl-ownership-prior-v3 | 0 | 15 |
| pool_capture | f1683510:live:2026-10-11 16:44:00+00 | live_pool | 2026-10-11 16:44 | 15 | - | 8/11 | 424 | 357 | 424 | 424 | 424 | nfl-ownership-prior-v3 | 0 | 15 |
| pool_capture | 43dc5b64:live:2026-10-11 16:45:00+00 | live_pool | 2026-10-11 16:45 | 14 | - | 8/11 | 424 | 357 | 424 | 424 | 424 | nfl-ownership-prior-v3 | 0 | 15 |
| pool_capture | f1683510:live:2026-10-11 16:45:00+00 | live_pool | 2026-10-11 16:45 | 14 | - | 8/11 | 424 | 357 | 424 | 424 | 424 | nfl-ownership-prior-v3 | 0 | 15 |
| pool_capture | 43dc5b64:live:2026-10-11 16:46:00+00 | live_pool | 2026-10-11 16:46 | 13 | - | 8/11 | 424 | 357 | 424 | 424 | 424 | nfl-ownership-prior-v3 | 0 | 15 |
| pool_capture | f1683510:live:2026-10-11 16:46:00+00 | live_pool | 2026-10-11 16:46 | 13 | - | 8/11 | 424 | 357 | 424 | 424 | 424 | nfl-ownership-prior-v3 | 0 | 15 |
| pool_capture | 43dc5b64:live:2026-10-11 16:47:00+00 | live_pool | 2026-10-11 16:47 | 12 | - | 8/11 | 424 | 357 | 424 | 424 | 424 | nfl-ownership-prior-v3 | 0 | 15 |
| pool_capture | f1683510:live:2026-10-11 16:47:00+00 | live_pool | 2026-10-11 16:47 | 12 | - | 8/11 | 424 | 357 | 424 | 424 | 424 | nfl-ownership-prior-v3 | 0 | 15 |
| pool_capture | 43dc5b64:live:2026-10-11 16:48:00+00 | live_pool | 2026-10-11 16:48 | 11 | - | 8/11 | 424 | 357 | 424 | 424 | 424 | nfl-ownership-prior-v3 | 0 | 15 |
| pool_capture | f1683510:live:2026-10-11 16:48:00+00 | live_pool | 2026-10-11 16:48 | 11 | - | 8/11 | 424 | 357 | 424 | 424 | 424 | nfl-ownership-prior-v3 | 0 | 15 |
| pool_capture | 43dc5b64:live:2026-10-11 16:49:00+00 | live_pool | 2026-10-11 16:49 | 10 | - | 8/11 | 424 | 357 | 424 | 424 | 424 | nfl-ownership-prior-v3 | 0 | 15 |
| pool_capture | f1683510:live:2026-10-11 16:49:00+00 | live_pool | 2026-10-11 16:49 | 10 | - | 8/11 | 424 | 357 | 424 | 424 | 424 | nfl-ownership-prior-v3 | 0 | 15 |
| pool_capture | 43dc5b64:live:2026-10-11 16:50:00+00 | live_pool | 2026-10-11 16:50 | 9 | - | 8/11 | 424 | 357 | 424 | 424 | 424 | nfl-ownership-prior-v3 | 0 | 15 |
| pool_capture | f1683510:live:2026-10-11 16:50:00+00 | live_pool | 2026-10-11 16:50 | 9 | - | 8/11 | 424 | 357 | 424 | 424 | 424 | nfl-ownership-prior-v3 | 0 | 15 |
| pool_capture | 43dc5b64:live:2026-10-11 16:51:00+00 | live_pool | 2026-10-11 16:51 | 8 | - | 8/11 | 424 | 357 | 424 | 424 | 424 | nfl-ownership-prior-v3 | 0 | 15 |
| pool_capture | f1683510:live:2026-10-11 16:51:00+00 | live_pool | 2026-10-11 16:51 | 8 | - | 8/11 | 424 | 357 | 424 | 424 | 424 | nfl-ownership-prior-v3 | 0 | 15 |
| pool_capture | 43dc5b64:live:2026-10-11 16:52:00+00 | live_pool | 2026-10-11 16:52 | 7 | - | 8/11 | 424 | 357 | 424 | 424 | 424 | nfl-ownership-prior-v3 | 0 | 15 |
| pool_capture | f1683510:live:2026-10-11 16:52:00+00 | live_pool | 2026-10-11 16:52 | 7 | - | 8/11 | 424 | 357 | 424 | 424 | 424 | nfl-ownership-prior-v3 | 0 | 15 |
| pool_capture | 43dc5b64:live:2026-10-11 16:53:00+00 | live_pool | 2026-10-11 16:53 | 6 | - | 8/11 | 424 | 357 | 424 | 424 | 424 | nfl-ownership-prior-v3 | 0 | 15 |
| pool_capture | f1683510:live:2026-10-11 16:53:00+00 | live_pool | 2026-10-11 16:53 | 6 | - | 8/11 | 424 | 357 | 424 | 424 | 424 | nfl-ownership-prior-v3 | 0 | 15 |
| pool_capture | 43dc5b64:live:2026-10-11 16:54:00+00 | live_pool | 2026-10-11 16:54 | 5 | - | 8/11 | 424 | 357 | 424 | 424 | 424 | nfl-ownership-prior-v3 | 0 | 15 |
| pool_capture | f1683510:live:2026-10-11 16:54:00+00 | live_pool | 2026-10-11 16:54 | 5 | - | 8/11 | 424 | 357 | 424 | 424 | 424 | nfl-ownership-prior-v3 | 0 | 15 |
| pool_capture | 43dc5b64:live:2026-10-11 16:55:00+00 | live_pool | 2026-10-11 16:55 | 4 | - | 8/11 | 424 | 357 | 424 | 424 | 424 | nfl-ownership-prior-v3 | 0 | 15 |
| pool_capture | f1683510:live:2026-10-11 16:55:00+00 | live_pool | 2026-10-11 16:55 | 4 | - | 8/11 | 424 | 357 | 424 | 424 | 424 | nfl-ownership-prior-v3 | 0 | 15 |
| pool_capture | 43dc5b64:live:2026-10-11 16:56:00+00 | live_pool | 2026-10-11 16:56 | 3 | - | 8/11 | 424 | 357 | 424 | 424 | 424 | nfl-ownership-prior-v3 | 0 | 15 |
| pool_capture | f1683510:live:2026-10-11 16:56:00+00 | live_pool | 2026-10-11 16:56 | 3 | - | 8/11 | 424 | 357 | 424 | 424 | 424 | nfl-ownership-prior-v3 | 0 | 15 |
| pool_capture | 43dc5b64:live:2026-10-11 16:57:00+00 | live_pool | 2026-10-11 16:57 | 2 | - | 8/11 | 424 | 357 | 424 | 424 | 424 | nfl-ownership-prior-v3 | 0 | 15 |
| pool_capture | f1683510:live:2026-10-11 16:57:00+00 | live_pool | 2026-10-11 16:57 | 2 | - | 8/11 | 424 | 357 | 424 | 424 | 424 | nfl-ownership-prior-v3 | 0 | 15 |
| pool_capture | 43dc5b64:live:2026-10-11 16:58:00+00 | live_pool | 2026-10-11 16:58 | 1 | - | 8/11 | 424 | 357 | 424 | 424 | 424 | nfl-ownership-prior-v3 | 0 | 15 |
| pool_capture | f1683510:live:2026-10-11 16:58:00+00 | live_pool | 2026-10-11 16:58 | 1 | - | 8/11 | 424 | 357 | 424 | 424 | 424 | nfl-ownership-prior-v3 | 0 | 15 |
| pool_capture | 43dc5b64:live:2026-10-11 16:59:00+00 | live_pool | 2026-10-11 16:59 | 0 | - | 8/11 | 424 | 357 | 424 | 424 | 424 | nfl-ownership-prior-v3 | 0 | 15 |
| pool_capture | f1683510:live:2026-10-11 16:59:00+00 | live_pool | 2026-10-11 16:59 | 0 | - | 8/11 | 424 | 357 | 424 | 424 | 424 | nfl-ownership-prior-v3 | 0 | 15 |
| pool_capture | 43dc5b64:live:2026-10-11 19:45:00+00 | live_pool | 2026-10-11 19:45 | -166 | - | 1/11 | 53 | 40 | 53 | 53 | 53 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | f1683510:live:2026-10-11 19:45:00+00 | live_pool | 2026-10-11 19:45 | -166 | - | 1/11 | 53 | 40 | 53 | 53 | 53 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 43dc5b64:live:2026-10-11 19:46:00+00 | live_pool | 2026-10-11 19:46 | -167 | - | 1/11 | 53 | 40 | 53 | 53 | 53 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | f1683510:live:2026-10-11 19:46:00+00 | live_pool | 2026-10-11 19:46 | -167 | - | 1/11 | 53 | 40 | 53 | 53 | 53 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 43dc5b64:live:2026-10-11 19:47:00+00 | live_pool | 2026-10-11 19:47 | -168 | - | 1/11 | 53 | 40 | 53 | 53 | 53 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | f1683510:live:2026-10-11 19:47:00+00 | live_pool | 2026-10-11 19:47 | -168 | - | 1/11 | 53 | 40 | 53 | 53 | 53 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 43dc5b64:live:2026-10-11 19:48:00+00 | live_pool | 2026-10-11 19:48 | -169 | - | 1/11 | 53 | 40 | 53 | 53 | 53 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | f1683510:live:2026-10-11 19:48:00+00 | live_pool | 2026-10-11 19:48 | -169 | - | 1/11 | 53 | 40 | 53 | 53 | 53 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 43dc5b64:live:2026-10-11 19:49:00+00 | live_pool | 2026-10-11 19:49 | -170 | - | 1/11 | 53 | 40 | 53 | 53 | 53 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | f1683510:live:2026-10-11 19:49:00+00 | live_pool | 2026-10-11 19:49 | -170 | - | 1/11 | 53 | 40 | 53 | 53 | 53 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 43dc5b64:live:2026-10-11 19:50:00+00 | live_pool | 2026-10-11 19:50 | -171 | - | 1/11 | 53 | 40 | 53 | 53 | 53 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | f1683510:live:2026-10-11 19:50:00+00 | live_pool | 2026-10-11 19:50 | -171 | - | 1/11 | 53 | 40 | 53 | 53 | 53 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 43dc5b64:live:2026-10-11 19:51:00+00 | live_pool | 2026-10-11 19:51 | -172 | - | 1/11 | 53 | 40 | 53 | 53 | 53 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | f1683510:live:2026-10-11 19:51:00+00 | live_pool | 2026-10-11 19:51 | -172 | - | 1/11 | 53 | 40 | 53 | 53 | 53 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 43dc5b64:live:2026-10-11 19:52:00+00 | live_pool | 2026-10-11 19:52 | -173 | - | 1/11 | 53 | 40 | 53 | 53 | 53 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | f1683510:live:2026-10-11 19:52:00+00 | live_pool | 2026-10-11 19:52 | -173 | - | 1/11 | 53 | 40 | 53 | 53 | 53 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 43dc5b64:live:2026-10-11 19:53:00+00 | live_pool | 2026-10-11 19:53 | -174 | - | 1/11 | 53 | 40 | 53 | 53 | 53 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | f1683510:live:2026-10-11 19:53:00+00 | live_pool | 2026-10-11 19:53 | -174 | - | 1/11 | 53 | 40 | 53 | 53 | 53 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 43dc5b64:live:2026-10-11 19:54:00+00 | live_pool | 2026-10-11 19:54 | -175 | - | 1/11 | 53 | 40 | 53 | 53 | 53 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | f1683510:live:2026-10-11 19:54:00+00 | live_pool | 2026-10-11 19:54 | -175 | - | 1/11 | 53 | 40 | 53 | 53 | 53 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 43dc5b64:live:2026-10-11 19:55:00+00 | live_pool | 2026-10-11 19:55 | -176 | - | 1/11 | 53 | 40 | 53 | 53 | 53 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | f1683510:live:2026-10-11 19:55:00+00 | live_pool | 2026-10-11 19:55 | -176 | - | 1/11 | 53 | 40 | 53 | 53 | 53 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 43dc5b64:live:2026-10-11 19:56:00+00 | live_pool | 2026-10-11 19:56 | -177 | - | 1/11 | 53 | 40 | 53 | 53 | 53 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | f1683510:live:2026-10-11 19:56:00+00 | live_pool | 2026-10-11 19:56 | -177 | - | 1/11 | 53 | 40 | 53 | 53 | 53 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 43dc5b64:live:2026-10-11 19:57:00+00 | live_pool | 2026-10-11 19:57 | -178 | - | 1/11 | 53 | 40 | 53 | 53 | 53 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | f1683510:live:2026-10-11 19:57:00+00 | live_pool | 2026-10-11 19:57 | -178 | - | 1/11 | 53 | 40 | 53 | 53 | 53 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 43dc5b64:live:2026-10-11 19:58:00+00 | live_pool | 2026-10-11 19:58 | -179 | - | 1/11 | 53 | 40 | 53 | 53 | 53 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | f1683510:live:2026-10-11 19:58:00+00 | live_pool | 2026-10-11 19:58 | -179 | - | 1/11 | 53 | 40 | 53 | 53 | 53 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 43dc5b64:live:2026-10-11 19:59:00+00 | live_pool | 2026-10-11 19:59 | -180 | - | 1/11 | 53 | 40 | 53 | 53 | 53 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | f1683510:live:2026-10-11 19:59:00+00 | live_pool | 2026-10-11 19:59 | -180 | - | 1/11 | 53 | 40 | 53 | 53 | 53 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 43dc5b64:live:2026-10-11 20:00:00+00 | live_pool | 2026-10-11 20:00 | -181 | - | 1/11 | 53 | 40 | 53 | 53 | 53 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | f1683510:live:2026-10-11 20:00:00+00 | live_pool | 2026-10-11 20:00 | -181 | - | 1/11 | 53 | 40 | 53 | 53 | 53 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 43dc5b64:live:2026-10-11 20:01:00+00 | live_pool | 2026-10-11 20:01 | -182 | - | 1/11 | 53 | 40 | 53 | 53 | 53 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | f1683510:live:2026-10-11 20:01:00+00 | live_pool | 2026-10-11 20:01 | -182 | - | 1/11 | 53 | 40 | 53 | 53 | 53 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 43dc5b64:live:2026-10-11 20:02:00+00 | live_pool | 2026-10-11 20:02 | -183 | - | 1/11 | 53 | 40 | 53 | 53 | 53 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | f1683510:live:2026-10-11 20:02:00+00 | live_pool | 2026-10-11 20:02 | -183 | - | 1/11 | 53 | 40 | 53 | 53 | 53 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 43dc5b64:live:2026-10-11 20:03:00+00 | live_pool | 2026-10-11 20:03 | -184 | - | 1/11 | 53 | 40 | 53 | 53 | 53 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | f1683510:live:2026-10-11 20:03:00+00 | live_pool | 2026-10-11 20:03 | -184 | - | 1/11 | 53 | 40 | 53 | 53 | 53 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 43dc5b64:live:2026-10-11 20:04:00+00 | live_pool | 2026-10-11 20:04 | -185 | - | 1/11 | 53 | 40 | 53 | 53 | 53 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | f1683510:live:2026-10-11 20:04:00+00 | live_pool | 2026-10-11 20:04 | -185 | - | 1/11 | 53 | 40 | 53 | 53 | 53 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 43dc5b64:live:2026-10-11 20:05:00+00 | live_pool | 2026-10-11 20:05 | -186 | - | 2/11 | 110 | 87 | 110 | 110 | 110 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | f1683510:live:2026-10-11 20:05:00+00 | live_pool | 2026-10-11 20:05 | -186 | - | 2/11 | 110 | 87 | 110 | 110 | 110 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 43dc5b64:live:2026-10-11 20:06:00+00 | live_pool | 2026-10-11 20:06 | -187 | - | 2/11 | 110 | 87 | 110 | 110 | 110 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | f1683510:live:2026-10-11 20:06:00+00 | live_pool | 2026-10-11 20:06 | -187 | - | 2/11 | 110 | 87 | 110 | 110 | 110 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 43dc5b64:live:2026-10-11 20:07:00+00 | live_pool | 2026-10-11 20:07 | -188 | - | 2/11 | 110 | 87 | 110 | 110 | 110 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | f1683510:live:2026-10-11 20:07:00+00 | live_pool | 2026-10-11 20:07 | -188 | - | 2/11 | 110 | 87 | 110 | 110 | 110 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 43dc5b64:live:2026-10-11 20:08:00+00 | live_pool | 2026-10-11 20:08 | -189 | - | 2/11 | 110 | 87 | 110 | 110 | 110 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | f1683510:live:2026-10-11 20:08:00+00 | live_pool | 2026-10-11 20:08 | -189 | - | 2/11 | 110 | 87 | 110 | 110 | 110 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 43dc5b64:live:2026-10-11 20:09:00+00 | live_pool | 2026-10-11 20:09 | -190 | - | 2/11 | 110 | 87 | 110 | 110 | 110 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | f1683510:live:2026-10-11 20:09:00+00 | live_pool | 2026-10-11 20:09 | -190 | - | 2/11 | 110 | 87 | 110 | 110 | 110 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 43dc5b64:live:2026-10-11 20:10:00+00 | live_pool | 2026-10-11 20:10 | -191 | - | 2/11 | 110 | 87 | 110 | 110 | 110 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | f1683510:live:2026-10-11 20:10:00+00 | live_pool | 2026-10-11 20:10 | -191 | - | 2/11 | 110 | 87 | 110 | 110 | 110 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 43dc5b64:live:2026-10-11 20:11:00+00 | live_pool | 2026-10-11 20:11 | -192 | - | 2/11 | 110 | 87 | 110 | 110 | 110 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | f1683510:live:2026-10-11 20:11:00+00 | live_pool | 2026-10-11 20:11 | -192 | - | 2/11 | 110 | 87 | 110 | 110 | 110 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 43dc5b64:live:2026-10-11 20:12:00+00 | live_pool | 2026-10-11 20:12 | -193 | - | 2/11 | 110 | 87 | 110 | 110 | 110 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | f1683510:live:2026-10-11 20:12:00+00 | live_pool | 2026-10-11 20:12 | -193 | - | 2/11 | 110 | 87 | 110 | 110 | 110 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 43dc5b64:live:2026-10-11 20:13:00+00 | live_pool | 2026-10-11 20:13 | -194 | - | 2/11 | 110 | 87 | 110 | 110 | 110 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | f1683510:live:2026-10-11 20:13:00+00 | live_pool | 2026-10-11 20:13 | -194 | - | 2/11 | 110 | 87 | 110 | 110 | 110 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 43dc5b64:live:2026-10-11 20:14:00+00 | live_pool | 2026-10-11 20:14 | -195 | - | 2/11 | 110 | 87 | 110 | 110 | 110 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | f1683510:live:2026-10-11 20:14:00+00 | live_pool | 2026-10-11 20:14 | -195 | - | 2/11 | 110 | 87 | 110 | 110 | 110 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 43dc5b64:live:2026-10-11 20:15:00+00 | live_pool | 2026-10-11 20:15 | -196 | - | 2/11 | 110 | 87 | 110 | 110 | 110 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | f1683510:live:2026-10-11 20:15:00+00 | live_pool | 2026-10-11 20:15 | -196 | - | 2/11 | 110 | 87 | 110 | 110 | 110 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 43dc5b64:live:2026-10-11 20:16:00+00 | live_pool | 2026-10-11 20:16 | -197 | - | 2/11 | 110 | 87 | 110 | 110 | 110 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | f1683510:live:2026-10-11 20:16:00+00 | live_pool | 2026-10-11 20:16 | -197 | - | 2/11 | 110 | 87 | 110 | 110 | 110 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 43dc5b64:live:2026-10-11 20:17:00+00 | live_pool | 2026-10-11 20:17 | -198 | - | 2/11 | 110 | 87 | 110 | 110 | 110 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | f1683510:live:2026-10-11 20:17:00+00 | live_pool | 2026-10-11 20:17 | -198 | - | 2/11 | 110 | 87 | 110 | 110 | 110 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 43dc5b64:live:2026-10-11 20:18:00+00 | live_pool | 2026-10-11 20:18 | -199 | - | 2/11 | 110 | 87 | 110 | 110 | 110 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | f1683510:live:2026-10-11 20:18:00+00 | live_pool | 2026-10-11 20:18 | -199 | - | 2/11 | 110 | 87 | 110 | 110 | 110 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 43dc5b64:live:2026-10-11 20:19:00+00 | live_pool | 2026-10-11 20:19 | -200 | - | 2/11 | 110 | 87 | 110 | 110 | 110 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | f1683510:live:2026-10-11 20:19:00+00 | live_pool | 2026-10-11 20:19 | -200 | - | 2/11 | 110 | 87 | 110 | 110 | 110 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 43dc5b64:live:2026-10-11 20:20:00+00 | live_pool | 2026-10-11 20:20 | -201 | - | 2/11 | 110 | 87 | 110 | 110 | 110 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | f1683510:live:2026-10-11 20:20:00+00 | live_pool | 2026-10-11 20:20 | -201 | - | 2/11 | 110 | 87 | 110 | 110 | 110 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 43dc5b64:live:2026-10-11 20:21:00+00 | live_pool | 2026-10-11 20:21 | -202 | - | 2/11 | 110 | 87 | 110 | 110 | 110 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | f1683510:live:2026-10-11 20:21:00+00 | live_pool | 2026-10-11 20:21 | -202 | - | 2/11 | 110 | 87 | 110 | 110 | 110 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 43dc5b64:live:2026-10-11 20:22:00+00 | live_pool | 2026-10-11 20:22 | -203 | - | 2/11 | 110 | 87 | 110 | 110 | 110 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | f1683510:live:2026-10-11 20:22:00+00 | live_pool | 2026-10-11 20:22 | -203 | - | 2/11 | 110 | 87 | 110 | 110 | 110 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 43dc5b64:live:2026-10-11 20:23:00+00 | live_pool | 2026-10-11 20:23 | -204 | - | 2/11 | 110 | 87 | 110 | 110 | 110 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | f1683510:live:2026-10-11 20:23:00+00 | live_pool | 2026-10-11 20:23 | -204 | - | 2/11 | 110 | 87 | 110 | 110 | 110 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 43dc5b64:live:2026-10-11 20:24:00+00 | live_pool | 2026-10-11 20:24 | -205 | - | 2/11 | 110 | 87 | 110 | 110 | 110 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | f1683510:live:2026-10-11 20:24:00+00 | live_pool | 2026-10-11 20:24 | -205 | - | 2/11 | 110 | 87 | 110 | 110 | 110 | nfl-ownership-prior-v3 | 0 | 0 |

### showdown BAL@ATL — week 5, first kickoff 2026-10-12 00:20 UTC

Uploads 1 (upload); contests none; status **canonical**.

| source | id | origin | at (UTC) | min before kickoff | horizon | games | players | proj | avail | dkStatus | ownPct | ownership version | anchor | commit |
|---|---|---|---|---:|---|---|---:|---:|---:|---:|---:|---|---:|---:|
| pool_capture | d91b0b98:live:2026-10-11 22:37:00+00 | live_pool | 2026-10-11 22:37 | 102 | CEE* | 1/1 | 52 | 41 | 52 | 52 | 52 | nfl-ownership-prior-v3 | 0 | 0 |
| optimizer_run | 79f91be8 | partial/gpp | 2026-10-11 22:37 | 102 | E | all | 52 | 41 | 52 | 52 | 52 | nfl-ownership-prior-v3 | 0 | 0 |
| optimizer_run | 56d3de4a | partial/gpp | 2026-10-11 22:40 | 99 | - | all | 52 | 41 | 52 | 52 | 52 | nfl-ownership-prior-v3 | 0 | 0 |
| optimizer_run | 234314f8 | partial/gpp | 2026-10-11 22:41 | 98 | - | all | 52 | 41 | 52 | 52 | 52 | nfl-ownership-prior-v3 | 0 | 0 |
| optimizer_run | a0a61465 | partial/gpp | 2026-10-11 22:42 | 98 | - | all | 52 | 41 | 52 | 52 | 52 | nfl-ownership-prior-v3 | 0 | 0 |
| optimizer_run | f596f0ee | partial/gpp | 2026-10-11 22:43 | 96 | CC* | all | 52 | 41 | 52 | 52 | 52 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | d91b0b98:live:2026-10-12 00:00:00+00 | live_pool | 2026-10-12 00:00 | 19 | - | 1/1 | 52 | 42 | 52 | 52 | 52 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | d91b0b98:live:2026-10-12 00:01:00+00 | live_pool | 2026-10-12 00:01 | 18 | - | 1/1 | 52 | 42 | 52 | 52 | 52 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | d91b0b98:live:2026-10-12 00:02:00+00 | live_pool | 2026-10-12 00:02 | 17 | - | 1/1 | 52 | 42 | 52 | 52 | 52 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | d91b0b98:live:2026-10-12 00:03:00+00 | live_pool | 2026-10-12 00:03 | 16 | - | 1/1 | 52 | 42 | 52 | 52 | 52 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | d91b0b98:live:2026-10-12 00:04:00+00 | live_pool | 2026-10-12 00:04 | 15 | - | 1/1 | 52 | 42 | 52 | 52 | 52 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | d91b0b98:live:2026-10-12 00:05:00+00 | live_pool | 2026-10-12 00:05 | 14 | - | 1/1 | 52 | 42 | 52 | 52 | 52 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | d91b0b98:live:2026-10-12 00:06:00+00 | live_pool | 2026-10-12 00:06 | 13 | - | 1/1 | 52 | 42 | 52 | 52 | 52 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | d91b0b98:live:2026-10-12 00:07:00+00 | live_pool | 2026-10-12 00:07 | 12 | - | 1/1 | 52 | 42 | 52 | 52 | 52 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | d91b0b98:live:2026-10-12 00:08:00+00 | live_pool | 2026-10-12 00:08 | 11 | - | 1/1 | 52 | 42 | 52 | 52 | 52 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | d91b0b98:live:2026-10-12 00:09:00+00 | live_pool | 2026-10-12 00:09 | 10 | - | 1/1 | 52 | 42 | 52 | 52 | 52 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | d91b0b98:live:2026-10-12 00:10:00+00 | live_pool | 2026-10-12 00:10 | 9 | - | 1/1 | 52 | 42 | 52 | 52 | 52 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | d91b0b98:live:2026-10-12 00:11:00+00 | live_pool | 2026-10-12 00:11 | 8 | - | 1/1 | 52 | 42 | 52 | 52 | 52 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | d91b0b98:live:2026-10-12 00:12:00+00 | live_pool | 2026-10-12 00:12 | 7 | - | 1/1 | 52 | 42 | 52 | 52 | 52 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | d91b0b98:live:2026-10-12 00:13:00+00 | live_pool | 2026-10-12 00:13 | 6 | - | 1/1 | 52 | 42 | 52 | 52 | 52 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | d91b0b98:live:2026-10-12 00:14:00+00 | live_pool | 2026-10-12 00:14 | 5 | - | 1/1 | 52 | 42 | 52 | 52 | 52 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | d91b0b98:live:2026-10-12 00:15:00+00 | live_pool | 2026-10-12 00:15 | 4 | - | 1/1 | 52 | 42 | 52 | 52 | 52 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | d91b0b98:live:2026-10-12 00:16:00+00 | live_pool | 2026-10-12 00:16 | 3 | - | 1/1 | 52 | 42 | 52 | 52 | 52 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | d91b0b98:live:2026-10-12 00:17:00+00 | live_pool | 2026-10-12 00:17 | 2 | - | 1/1 | 52 | 42 | 52 | 52 | 52 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | d91b0b98:live:2026-10-12 00:18:00+00 | live_pool | 2026-10-12 00:18 | 1 | - | 1/1 | 52 | 42 | 52 | 52 | 52 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | d91b0b98:live:2026-10-12 00:19:00+00 | live_pool | 2026-10-12 00:19 | 0 | - | 1/1 | 52 | 42 | 52 | 52 | 52 | nfl-ownership-prior-v3 | 0 | 0 |

### showdown BUF@LAR — week 5, first kickoff 2026-10-13 00:15 UTC

Uploads 1 (upload); contests 196480476; status **canonical**.

| source | id | origin | at (UTC) | min before kickoff | horizon | games | players | proj | avail | dkStatus | ownPct | ownership version | anchor | commit |
|---|---|---|---|---:|---|---|---:|---:|---:|---:|---:|---|---:|---:|
| pool_capture | 9f28df85:live:2026-10-12 22:35:00+00 | live_pool | 2026-10-12 22:35 | 99 | CEE* | 1/1 | 49 | 39 | 49 | 49 | 49 | nfl-ownership-prior-v3 | 17 | 0 |
| optimizer_run | 36be5e10 | complete/gpp | 2026-10-12 22:36 | 98 | E | all | 49 | 39 | 49 | 49 | 49 | nfl-ownership-prior-v3 | 17 | 0 |
| optimizer_run | 065314fa | partial/gpp | 2026-10-12 23:03 | 71 | - | all | 49 | 39 | 49 | 49 | 49 | nfl-ownership-prior-v3 | 17 | 0 |
| optimizer_run | 25f97cdb | complete/gpp | 2026-10-12 23:13 | 62 | CC* | all | 49 | 41 | 49 | 49 | 49 | nfl-ownership-prior-v3 | 18 | 0 |
| pool_capture | 9f28df85:live:2026-10-12 23:55:00+00 | live_pool | 2026-10-12 23:55 | 19 | - | 1/1 | 49 | 41 | 49 | 49 | 49 | nfl-ownership-prior-v3 | 18 | 0 |
| pool_capture | 9f28df85:live:2026-10-12 23:56:00+00 | live_pool | 2026-10-12 23:56 | 18 | - | 1/1 | 49 | 41 | 49 | 49 | 49 | nfl-ownership-prior-v3 | 18 | 0 |
| pool_capture | 9f28df85:live:2026-10-12 23:57:00+00 | live_pool | 2026-10-12 23:57 | 17 | - | 1/1 | 49 | 41 | 49 | 49 | 49 | nfl-ownership-prior-v3 | 18 | 0 |
| pool_capture | 9f28df85:live:2026-10-12 23:58:00+00 | live_pool | 2026-10-12 23:58 | 16 | - | 1/1 | 49 | 41 | 49 | 49 | 49 | nfl-ownership-prior-v3 | 18 | 0 |
| pool_capture | 9f28df85:live:2026-10-12 23:59:00+00 | live_pool | 2026-10-12 23:59 | 15 | - | 1/1 | 49 | 41 | 49 | 49 | 49 | nfl-ownership-prior-v3 | 18 | 0 |
| pool_capture | 9f28df85:live:2026-10-13 00:00:00+00 | live_pool | 2026-10-13 00:00 | 14 | - | 1/1 | 49 | 41 | 49 | 49 | 49 | nfl-ownership-prior-v3 | 18 | 0 |
| pool_capture | 9f28df85:live:2026-10-13 00:01:00+00 | live_pool | 2026-10-13 00:01 | 13 | - | 1/1 | 49 | 41 | 49 | 49 | 49 | nfl-ownership-prior-v3 | 18 | 0 |
| pool_capture | 9f28df85:live:2026-10-13 00:02:00+00 | live_pool | 2026-10-13 00:02 | 12 | - | 1/1 | 49 | 41 | 49 | 49 | 49 | nfl-ownership-prior-v3 | 18 | 0 |
| pool_capture | 9f28df85:live:2026-10-13 00:03:00+00 | live_pool | 2026-10-13 00:03 | 11 | - | 1/1 | 49 | 41 | 49 | 49 | 49 | nfl-ownership-prior-v3 | 18 | 0 |
| pool_capture | 9f28df85:live:2026-10-13 00:04:00+00 | live_pool | 2026-10-13 00:04 | 10 | - | 1/1 | 49 | 41 | 49 | 49 | 49 | nfl-ownership-prior-v3 | 18 | 0 |
| pool_capture | 9f28df85:live:2026-10-13 00:05:00+00 | live_pool | 2026-10-13 00:05 | 9 | - | 1/1 | 49 | 41 | 49 | 49 | 49 | nfl-ownership-prior-v3 | 18 | 0 |
| pool_capture | 9f28df85:live:2026-10-13 00:06:00+00 | live_pool | 2026-10-13 00:06 | 8 | - | 1/1 | 49 | 41 | 49 | 49 | 49 | nfl-ownership-prior-v3 | 18 | 0 |
| pool_capture | 9f28df85:live:2026-10-13 00:07:00+00 | live_pool | 2026-10-13 00:07 | 7 | - | 1/1 | 49 | 41 | 49 | 49 | 49 | nfl-ownership-prior-v3 | 18 | 0 |
| pool_capture | 9f28df85:live:2026-10-13 00:08:00+00 | live_pool | 2026-10-13 00:08 | 6 | - | 1/1 | 49 | 41 | 49 | 49 | 49 | nfl-ownership-prior-v3 | 18 | 0 |
| pool_capture | 9f28df85:live:2026-10-13 00:09:00+00 | live_pool | 2026-10-13 00:09 | 5 | - | 1/1 | 49 | 41 | 49 | 49 | 49 | nfl-ownership-prior-v3 | 18 | 0 |
| pool_capture | 9f28df85:live:2026-10-13 00:10:00+00 | live_pool | 2026-10-13 00:10 | 4 | - | 1/1 | 49 | 41 | 49 | 49 | 49 | nfl-ownership-prior-v3 | 18 | 0 |
| pool_capture | 9f28df85:live:2026-10-13 00:11:00+00 | live_pool | 2026-10-13 00:11 | 3 | - | 1/1 | 49 | 41 | 49 | 49 | 49 | nfl-ownership-prior-v3 | 18 | 0 |
| pool_capture | 9f28df85:live:2026-10-13 00:12:00+00 | live_pool | 2026-10-13 00:12 | 2 | - | 1/1 | 49 | 41 | 49 | 49 | 49 | nfl-ownership-prior-v3 | 18 | 0 |
| pool_capture | 9f28df85:live:2026-10-13 00:13:00+00 | live_pool | 2026-10-13 00:13 | 1 | - | 1/1 | 49 | 41 | 49 | 49 | 49 | nfl-ownership-prior-v3 | 18 | 0 |
| pool_capture | 9f28df85:live:2026-10-13 00:14:00+00 | live_pool | 2026-10-13 00:14 | 0 | - | 1/1 | 49 | 41 | 49 | 49 | 49 | nfl-ownership-prior-v3 | 18 | 0 |

### showdown SEA@DEN — week 6, first kickoff 2026-10-16 00:15 UTC

Uploads 3 (upload); contests 196689718; status **canonical**.

| source | id | origin | at (UTC) | min before kickoff | horizon | games | players | proj | avail | dkStatus | ownPct | ownership version | anchor | commit |
|---|---|---|---|---:|---|---|---:|---:|---:|---:|---:|---|---:|---:|
| pool_capture | 1a8b89b7:live:2026-10-15 13:29:00+00 | live_pool | 2026-10-15 13:29 | 645 | EE* | 1/1 | 56 | 43 | 56 | 56 | 56 | nfl-ownership-prior-v3 | 15 | 0 |
| pool_capture | 5d1447c0:live:2026-10-15 13:33:00+00 | live_pool | 2026-10-15 13:33 | 641 | - | 1/1 | 56 | 43 | 56 | 56 | 56 | nfl-ownership-prior-v3 | 15 | 0 |
| optimizer_run | 12b329c3 | partial/gpp | 2026-10-15 13:54 | 621 | E | all | 56 | 43 | 56 | 56 | 56 | nfl-ownership-prior-v3 | 15 | 0 |
| pool_capture | 729ccedd:live:2026-10-15 13:54:00+00 | live_pool | 2026-10-15 13:54 | 620 | C | 1/1 | 56 | 43 | 56 | 56 | 56 | nfl-ownership-prior-v3 | 15 | 0 |
| optimizer_run | bb2cadc3 | partial/gpp | 2026-10-15 22:04 | 130 | - | all | 56 | 43 | 56 | 56 | 56 | nfl-ownership-prior-v3 | 15 | 0 |
| optimizer_run | 36c70002 | partial/gpp | 2026-10-15 22:12 | 122 | CC* | all | 56 | 43 | 56 | 56 | 56 | nfl-ownership-prior-v3 | 15 | 0 |
| pool_capture | 729ccedd:live:2026-10-15 23:55:00+00 | live_pool | 2026-10-15 23:55 | 19 | - | 1/1 | 56 | 46 | 56 | 56 | 56 | nfl-ownership-prior-v3 | 17 | 0 |
| pool_capture | 729ccedd:live:2026-10-15 23:56:00+00 | live_pool | 2026-10-15 23:56 | 18 | - | 1/1 | 56 | 46 | 56 | 56 | 56 | nfl-ownership-prior-v3 | 17 | 0 |
| pool_capture | 729ccedd:live:2026-10-15 23:57:00+00 | live_pool | 2026-10-15 23:57 | 17 | - | 1/1 | 56 | 46 | 56 | 56 | 56 | nfl-ownership-prior-v3 | 17 | 0 |
| pool_capture | 729ccedd:live:2026-10-15 23:58:00+00 | live_pool | 2026-10-15 23:58 | 16 | - | 1/1 | 56 | 46 | 56 | 56 | 56 | nfl-ownership-prior-v3 | 17 | 0 |
| pool_capture | 729ccedd:live:2026-10-15 23:59:00+00 | live_pool | 2026-10-15 23:59 | 15 | - | 1/1 | 56 | 46 | 56 | 56 | 56 | nfl-ownership-prior-v3 | 17 | 0 |
| pool_capture | 729ccedd:live:2026-10-16 00:00:00+00 | live_pool | 2026-10-16 00:00 | 14 | - | 1/1 | 56 | 46 | 56 | 56 | 56 | nfl-ownership-prior-v3 | 17 | 0 |
| pool_capture | 729ccedd:live:2026-10-16 00:01:00+00 | live_pool | 2026-10-16 00:01 | 13 | - | 1/1 | 56 | 46 | 56 | 56 | 56 | nfl-ownership-prior-v3 | 17 | 0 |
| pool_capture | 729ccedd:live:2026-10-16 00:02:00+00 | live_pool | 2026-10-16 00:02 | 12 | - | 1/1 | 56 | 46 | 56 | 56 | 56 | nfl-ownership-prior-v3 | 17 | 0 |
| pool_capture | 729ccedd:live:2026-10-16 00:03:00+00 | live_pool | 2026-10-16 00:03 | 11 | - | 1/1 | 56 | 46 | 56 | 56 | 56 | nfl-ownership-prior-v3 | 17 | 0 |
| pool_capture | 729ccedd:live:2026-10-16 00:04:00+00 | live_pool | 2026-10-16 00:04 | 10 | - | 1/1 | 56 | 46 | 56 | 56 | 56 | nfl-ownership-prior-v3 | 17 | 0 |
| pool_capture | 729ccedd:live:2026-10-16 00:05:00+00 | live_pool | 2026-10-16 00:05 | 9 | - | 1/1 | 56 | 46 | 56 | 56 | 56 | nfl-ownership-prior-v3 | 17 | 0 |
| pool_capture | 729ccedd:live:2026-10-16 00:06:00+00 | live_pool | 2026-10-16 00:06 | 8 | - | 1/1 | 56 | 46 | 56 | 56 | 56 | nfl-ownership-prior-v3 | 17 | 0 |
| pool_capture | 729ccedd:live:2026-10-16 00:07:00+00 | live_pool | 2026-10-16 00:07 | 7 | - | 1/1 | 56 | 46 | 56 | 56 | 56 | nfl-ownership-prior-v3 | 17 | 0 |
| pool_capture | 729ccedd:live:2026-10-16 00:08:00+00 | live_pool | 2026-10-16 00:08 | 6 | - | 1/1 | 56 | 46 | 56 | 56 | 56 | nfl-ownership-prior-v3 | 17 | 0 |
| pool_capture | 729ccedd:live:2026-10-16 00:09:00+00 | live_pool | 2026-10-16 00:09 | 5 | - | 1/1 | 56 | 46 | 56 | 56 | 56 | nfl-ownership-prior-v3 | 17 | 0 |
| pool_capture | 729ccedd:live:2026-10-16 00:10:00+00 | live_pool | 2026-10-16 00:10 | 4 | - | 1/1 | 56 | 46 | 56 | 56 | 56 | nfl-ownership-prior-v3 | 17 | 0 |
| pool_capture | 729ccedd:live:2026-10-16 00:11:00+00 | live_pool | 2026-10-16 00:11 | 3 | - | 1/1 | 56 | 46 | 56 | 56 | 56 | nfl-ownership-prior-v3 | 17 | 0 |
| pool_capture | 729ccedd:live:2026-10-16 00:12:00+00 | live_pool | 2026-10-16 00:12 | 2 | - | 1/1 | 56 | 46 | 56 | 56 | 56 | nfl-ownership-prior-v3 | 17 | 0 |
| pool_capture | 729ccedd:live:2026-10-16 00:13:00+00 | live_pool | 2026-10-16 00:13 | 1 | - | 1/1 | 56 | 46 | 56 | 56 | 56 | nfl-ownership-prior-v3 | 17 | 0 |
| pool_capture | 729ccedd:live:2026-10-16 00:14:00+00 | live_pool | 2026-10-16 00:14 | 0 | - | 1/1 | 56 | 46 | 56 | 56 | 56 | nfl-ownership-prior-v3 | 17 | 0 |

### showdown HOU@JAX — week 6, first kickoff 2026-10-18 13:30 UTC

Uploads 2 (upload); contests none; status **canonical**.

| source | id | origin | at (UTC) | min before kickoff | horizon | games | players | proj | avail | dkStatus | ownPct | ownership version | anchor | commit |
|---|---|---|---|---:|---|---|---:|---:|---:|---:|---:|---|---:|---:|
| pool_capture | ab604f29:live:2026-10-17 20:42:00+00 | live_pool | 2026-10-17 20:42 | 1007 | EE* | 1/1 | 55 | 44 | 55 | 55 | 55 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 6e234b71:live:2026-10-17 20:46:00+00 | live_pool | 2026-10-17 20:46 | 1003 | C | 1/1 | 55 | 44 | 55 | 55 | 55 | nfl-ownership-prior-v3 | 0 | 0 |
| optimizer_run | a0ae1e0d | complete/gpp | 2026-10-17 20:56 | 993 | E | all | 55 | 44 | 55 | 55 | 55 | nfl-ownership-prior-v3 | 0 | 0 |
| optimizer_run | 63e1c1bc | partial/gpp | 2026-10-17 20:57 | 993 | - | all | 55 | 44 | 55 | 55 | 55 | nfl-ownership-prior-v3 | 0 | 0 |
| optimizer_run | 905c94fe | partial/gpp | 2026-10-17 21:16 | 973 | - | all | 55 | 44 | 55 | 55 | 55 | nfl-ownership-prior-v3 | 16 | 0 |
| optimizer_run | b2f140c0 | partial/gpp | 2026-10-17 21:21 | 968 | - | all | 55 | 44 | 55 | 55 | 55 | nfl-ownership-prior-v3 | 16 | 0 |
| optimizer_run | aca8c411 | partial/gpp | 2026-10-17 22:46 | 883 | - | all | 55 | 44 | 55 | 55 | 55 | nfl-ownership-prior-v3 | 16 | 0 |
| optimizer_run | 91c25327 | partial/gpp | 2026-10-17 22:56 | 874 | - | all | 55 | 44 | 55 | 55 | 55 | nfl-ownership-prior-v3 | 16 | 0 |
| optimizer_run | 080e5df1 | partial/gpp | 2026-10-17 23:44 | 825 | CC* | all | 55 | 44 | 55 | 55 | 55 | nfl-ownership-prior-v3 | 16 | 0 |
| pool_capture | 6e234b71:live:2026-10-18 13:10:00+00 | live_pool | 2026-10-18 13:10 | 19 | - | 1/1 | 55 | 46 | 55 | 55 | 55 | nfl-ownership-prior-v3 | 17 | 0 |
| pool_capture | 6e234b71:live:2026-10-18 13:11:00+00 | live_pool | 2026-10-18 13:11 | 18 | - | 1/1 | 55 | 46 | 55 | 55 | 55 | nfl-ownership-prior-v3 | 17 | 0 |
| pool_capture | 6e234b71:live:2026-10-18 13:12:00+00 | live_pool | 2026-10-18 13:12 | 17 | - | 1/1 | 55 | 46 | 55 | 55 | 55 | nfl-ownership-prior-v3 | 17 | 0 |
| pool_capture | 6e234b71:live:2026-10-18 13:13:00+00 | live_pool | 2026-10-18 13:13 | 16 | - | 1/1 | 55 | 46 | 55 | 55 | 55 | nfl-ownership-prior-v3 | 17 | 0 |
| pool_capture | 6e234b71:live:2026-10-18 13:14:00+00 | live_pool | 2026-10-18 13:14 | 15 | - | 1/1 | 55 | 46 | 55 | 55 | 55 | nfl-ownership-prior-v3 | 17 | 0 |
| pool_capture | 6e234b71:live:2026-10-18 13:15:00+00 | live_pool | 2026-10-18 13:15 | 14 | - | 1/1 | 55 | 46 | 55 | 55 | 55 | nfl-ownership-prior-v3 | 17 | 0 |
| pool_capture | 6e234b71:live:2026-10-18 13:16:00+00 | live_pool | 2026-10-18 13:16 | 13 | - | 1/1 | 55 | 46 | 55 | 55 | 55 | nfl-ownership-prior-v3 | 17 | 0 |
| pool_capture | 6e234b71:live:2026-10-18 13:17:00+00 | live_pool | 2026-10-18 13:17 | 12 | - | 1/1 | 55 | 46 | 55 | 55 | 55 | nfl-ownership-prior-v3 | 17 | 0 |
| pool_capture | 6e234b71:live:2026-10-18 13:18:00+00 | live_pool | 2026-10-18 13:18 | 11 | - | 1/1 | 55 | 46 | 55 | 55 | 55 | nfl-ownership-prior-v3 | 17 | 0 |
| pool_capture | 6e234b71:live:2026-10-18 13:19:00+00 | live_pool | 2026-10-18 13:19 | 10 | - | 1/1 | 55 | 46 | 55 | 55 | 55 | nfl-ownership-prior-v3 | 17 | 0 |
| pool_capture | 6e234b71:live:2026-10-18 13:20:00+00 | live_pool | 2026-10-18 13:20 | 9 | - | 1/1 | 55 | 46 | 55 | 55 | 55 | nfl-ownership-prior-v3 | 17 | 0 |
| pool_capture | 6e234b71:live:2026-10-18 13:21:00+00 | live_pool | 2026-10-18 13:21 | 8 | - | 1/1 | 55 | 46 | 55 | 55 | 55 | nfl-ownership-prior-v3 | 17 | 0 |
| pool_capture | 6e234b71:live:2026-10-18 13:22:00+00 | live_pool | 2026-10-18 13:22 | 7 | - | 1/1 | 55 | 46 | 55 | 55 | 55 | nfl-ownership-prior-v3 | 17 | 0 |
| pool_capture | 6e234b71:live:2026-10-18 13:23:00+00 | live_pool | 2026-10-18 13:23 | 6 | - | 1/1 | 55 | 46 | 55 | 55 | 55 | nfl-ownership-prior-v3 | 17 | 0 |
| pool_capture | 6e234b71:live:2026-10-18 13:24:00+00 | live_pool | 2026-10-18 13:24 | 5 | - | 1/1 | 55 | 46 | 55 | 55 | 55 | nfl-ownership-prior-v3 | 17 | 0 |
| pool_capture | 6e234b71:live:2026-10-18 13:25:00+00 | live_pool | 2026-10-18 13:25 | 4 | - | 1/1 | 55 | 46 | 55 | 55 | 55 | nfl-ownership-prior-v3 | 17 | 0 |
| pool_capture | 6e234b71:live:2026-10-18 13:26:00+00 | live_pool | 2026-10-18 13:26 | 3 | - | 1/1 | 55 | 46 | 55 | 55 | 55 | nfl-ownership-prior-v3 | 17 | 0 |
| pool_capture | 6e234b71:live:2026-10-18 13:27:00+00 | live_pool | 2026-10-18 13:27 | 2 | - | 1/1 | 55 | 46 | 55 | 55 | 55 | nfl-ownership-prior-v3 | 17 | 0 |
| pool_capture | 6e234b71:live:2026-10-18 13:28:00+00 | live_pool | 2026-10-18 13:28 | 1 | - | 1/1 | 55 | 46 | 55 | 55 | 55 | nfl-ownership-prior-v3 | 17 | 0 |
| pool_capture | 6e234b71:live:2026-10-18 13:29:00+00 | live_pool | 2026-10-18 13:29 | 0 | - | 1/1 | 55 | 46 | 55 | 55 | 55 | nfl-ownership-prior-v3 | 17 | 0 |

### classic 10 games — week 6, first kickoff 2026-10-18 17:00 UTC

Uploads 5 (system, upload); contests none; status **canonical**.

| source | id | origin | at (UTC) | min before kickoff | horizon | games | players | proj | avail | dkStatus | ownPct | ownership version | anchor | commit |
|---|---|---|---|---:|---|---|---:|---:|---:|---:|---:|---|---:|---:|
| pool_capture | 62838aaa:live:2026-10-17 17:00:00+00 | live_pool | 2026-10-17 17:00 | 1439 | EE* | 7/10 | 366 | 313 | 366 | 366 | 366 | nfl-ownership-prior-v3 | 0 | 7 |
| pool_capture | 62838aaa:live:2026-10-17 20:05:00+00 | live_pool | 2026-10-17 20:05 | 1254 | - | 1/10 | 51 | 42 | 51 | 51 | 51 | nfl-ownership-prior-v3 | 0 | 5 |
| pool_capture | 62838aaa:live:2026-10-17 20:25:00+00 | live_pool | 2026-10-17 20:25 | 1234 | - | 2/10 | 108 | 87 | 108 | 108 | 108 | nfl-ownership-prior-v3 | 0 | 0 |
| pool_capture | 95194297:live:2026-10-17 20:41:00+00 | live_pool | 2026-10-17 20:41 | 1218 | - | 10/10 | 525 | 442 | 525 | 525 | 525 | nfl-ownership-prior-v3 | 0 | 12 |
| pool_capture | de2bd3c0:live:2026-10-18 00:43:00+00 | live_pool | 2026-10-18 00:43 | 976 | - | 10/10 | 525 | 442 | 525 | 525 | 525 | nfl-ownership-prior-v3 | 127 | 6 |
| optimizer_run | 1b635a48 | complete/gpp | 2026-10-18 00:47 | 973 | E | all | 525 | 442 | 525 | 525 | 525 | nfl-ownership-prior-v3 | 127 | 6 |
| optimizer_run | aa794ed4 | complete/gpp | 2026-10-18 00:56 | 963 | - | all | 525 | 442 | 525 | 525 | 525 | nfl-ownership-prior-v3 | 127 | 6 |
| optimizer_run | 0d78dd8f | complete/gpp | 2026-10-18 01:36 | 923 | - | all | 525 | 442 | 525 | 525 | 525 | nfl-ownership-prior-v3 | 127 | 6 |
| pool_capture | 5db2c428:live:2026-10-18 13:12:00+00 | live_pool | 2026-10-18 13:12 | 227 | C | 10/10 | 526 | 440 | 526 | 526 | 526 | nfl-ownership-prior-v3 | 128 | 6 |
| optimizer_run | d356193c | complete/gpp | 2026-10-18 13:13 | 227 | - | all | 526 | 440 | 526 | 526 | 526 | nfl-ownership-prior-v3 | 128 | 6 |
| optimizer_run | b24bfed1 | partial/gpp | 2026-10-18 13:25 | 214 | - | all | 526 | 440 | 526 | 526 | 526 | nfl-ownership-prior-v3 | 128 | 6 |
| optimizer_run | e320de49 | partial/gpp | 2026-10-18 13:52 | 187 | - | all | 526 | 440 | 526 | 526 | 526 | nfl-ownership-prior-v3 | 128 | 6 |
| optimizer_run | 50ecff72 | complete/gpp | 2026-10-18 13:59 | 180 | - | all | 526 | 440 | 526 | 526 | 526 | nfl-ownership-prior-v3 | 128 | 6 |
| optimizer_run | cca74cc6 | complete/gpp | 2026-10-18 14:02 | 177 | C | all | 526 | 440 | 526 | 526 | 526 | nfl-ownership-prior-v3 | 128 | 6 |
| ghost_freeze | a15a81f5 | system | 2026-10-18 15:15 | 104 | CEC* | all | 526 | 440 | 526 | 526 | 526 | nfl-ownership-prior-v3 | 128 | 6 |
| pool_capture | 5db2c428:live:2026-10-18 16:40:00+00 | live_pool | 2026-10-18 16:40 | 19 | - | 7/10 | 367 | 315 | 367 | 367 | 367 | nfl-ownership-prior-v3 | 98 | 2 |
| pool_capture | de2bd3c0:live:2026-10-18 16:40:00+00 | live_pool | 2026-10-18 16:40 | 19 | - | 7/10 | 366 | 317 | 366 | 366 | 366 | nfl-ownership-prior-v3 | 97 | 2 |
| pool_capture | 5db2c428:live:2026-10-18 16:41:00+00 | live_pool | 2026-10-18 16:41 | 18 | - | 7/10 | 367 | 315 | 367 | 367 | 367 | nfl-ownership-prior-v3 | 98 | 2 |
| pool_capture | de2bd3c0:live:2026-10-18 16:41:00+00 | live_pool | 2026-10-18 16:41 | 18 | - | 7/10 | 366 | 317 | 366 | 366 | 366 | nfl-ownership-prior-v3 | 97 | 2 |
| pool_capture | 5db2c428:live:2026-10-18 16:42:00+00 | live_pool | 2026-10-18 16:42 | 17 | - | 7/10 | 367 | 315 | 367 | 367 | 367 | nfl-ownership-prior-v3 | 98 | 2 |
| pool_capture | de2bd3c0:live:2026-10-18 16:42:00+00 | live_pool | 2026-10-18 16:42 | 17 | - | 7/10 | 366 | 317 | 366 | 366 | 366 | nfl-ownership-prior-v3 | 97 | 2 |
| pool_capture | 5db2c428:live:2026-10-18 16:43:00+00 | live_pool | 2026-10-18 16:43 | 16 | - | 7/10 | 367 | 315 | 367 | 367 | 367 | nfl-ownership-prior-v3 | 98 | 2 |
| pool_capture | de2bd3c0:live:2026-10-18 16:43:00+00 | live_pool | 2026-10-18 16:43 | 16 | - | 7/10 | 366 | 317 | 366 | 366 | 366 | nfl-ownership-prior-v3 | 97 | 2 |
| pool_capture | 5db2c428:live:2026-10-18 16:44:00+00 | live_pool | 2026-10-18 16:44 | 15 | - | 7/10 | 367 | 315 | 367 | 367 | 367 | nfl-ownership-prior-v3 | 98 | 2 |
| pool_capture | de2bd3c0:live:2026-10-18 16:44:00+00 | live_pool | 2026-10-18 16:44 | 15 | - | 7/10 | 366 | 317 | 366 | 366 | 366 | nfl-ownership-prior-v3 | 97 | 2 |
| pool_capture | 5db2c428:live:2026-10-18 16:45:00+00 | live_pool | 2026-10-18 16:45 | 14 | - | 7/10 | 367 | 315 | 367 | 367 | 367 | nfl-ownership-prior-v3 | 98 | 2 |
| pool_capture | de2bd3c0:live:2026-10-18 16:45:00+00 | live_pool | 2026-10-18 16:45 | 14 | - | 7/10 | 366 | 317 | 366 | 366 | 366 | nfl-ownership-prior-v3 | 97 | 2 |
| pool_capture | 5db2c428:live:2026-10-18 16:46:00+00 | live_pool | 2026-10-18 16:46 | 13 | - | 7/10 | 367 | 315 | 367 | 367 | 367 | nfl-ownership-prior-v3 | 98 | 2 |
| pool_capture | de2bd3c0:live:2026-10-18 16:46:00+00 | live_pool | 2026-10-18 16:46 | 13 | - | 7/10 | 366 | 317 | 366 | 366 | 366 | nfl-ownership-prior-v3 | 97 | 2 |
| pool_capture | 5db2c428:live:2026-10-18 16:47:00+00 | live_pool | 2026-10-18 16:47 | 12 | - | 7/10 | 367 | 315 | 367 | 367 | 367 | nfl-ownership-prior-v3 | 98 | 2 |
| pool_capture | de2bd3c0:live:2026-10-18 16:47:00+00 | live_pool | 2026-10-18 16:47 | 12 | - | 7/10 | 366 | 317 | 366 | 366 | 366 | nfl-ownership-prior-v3 | 97 | 2 |
| pool_capture | 5db2c428:live:2026-10-18 16:48:00+00 | live_pool | 2026-10-18 16:48 | 11 | - | 7/10 | 367 | 315 | 367 | 367 | 367 | nfl-ownership-prior-v3 | 98 | 2 |
| pool_capture | de2bd3c0:live:2026-10-18 16:48:00+00 | live_pool | 2026-10-18 16:48 | 11 | - | 7/10 | 366 | 317 | 366 | 366 | 366 | nfl-ownership-prior-v3 | 97 | 2 |
| pool_capture | 5db2c428:live:2026-10-18 16:49:00+00 | live_pool | 2026-10-18 16:49 | 10 | - | 7/10 | 367 | 315 | 367 | 367 | 367 | nfl-ownership-prior-v3 | 98 | 2 |
| pool_capture | de2bd3c0:live:2026-10-18 16:49:00+00 | live_pool | 2026-10-18 16:49 | 10 | - | 7/10 | 366 | 317 | 366 | 366 | 366 | nfl-ownership-prior-v3 | 97 | 2 |
| pool_capture | 5db2c428:live:2026-10-18 16:50:00+00 | live_pool | 2026-10-18 16:50 | 9 | - | 7/10 | 367 | 315 | 367 | 367 | 367 | nfl-ownership-prior-v3 | 98 | 2 |
| pool_capture | de2bd3c0:live:2026-10-18 16:53:00+00 | live_pool | 2026-10-18 16:53 | 6 | - | 7/10 | 366 | 317 | 366 | 366 | 366 | nfl-ownership-prior-v3 | 97 | 2 |
| pool_capture | 5db2c428:live:2026-10-18 16:53:00+00 | live_pool | 2026-10-18 16:53 | 6 | - | 7/10 | 367 | 315 | 367 | 367 | 367 | nfl-ownership-prior-v3 | 98 | 2 |
| pool_capture | de2bd3c0:live:2026-10-18 16:54:00+00 | live_pool | 2026-10-18 16:54 | 6 | - | 7/10 | 366 | 317 | 366 | 366 | 366 | nfl-ownership-prior-v3 | 97 | 2 |
| pool_capture | 5db2c428:live:2026-10-18 16:54:00+00 | live_pool | 2026-10-18 16:54 | 5 | - | 7/10 | 367 | 315 | 367 | 367 | 367 | nfl-ownership-prior-v3 | 98 | 2 |
| pool_capture | 5db2c428:live:2026-10-18 16:55:00+00 | live_pool | 2026-10-18 16:55 | 4 | - | 7/10 | 367 | 315 | 367 | 367 | 367 | nfl-ownership-prior-v3 | 98 | 2 |
| pool_capture | de2bd3c0:live:2026-10-18 16:55:00+00 | live_pool | 2026-10-18 16:55 | 4 | - | 7/10 | 366 | 317 | 366 | 366 | 366 | nfl-ownership-prior-v3 | 97 | 2 |
| pool_capture | 5db2c428:live:2026-10-18 16:56:00+00 | live_pool | 2026-10-18 16:56 | 3 | - | 7/10 | 367 | 315 | 367 | 367 | 367 | nfl-ownership-prior-v3 | 98 | 2 |
| pool_capture | de2bd3c0:live:2026-10-18 16:56:00+00 | live_pool | 2026-10-18 16:56 | 3 | - | 7/10 | 366 | 317 | 366 | 366 | 366 | nfl-ownership-prior-v3 | 97 | 2 |
| pool_capture | 5db2c428:live:2026-10-18 16:57:00+00 | live_pool | 2026-10-18 16:57 | 2 | - | 7/10 | 367 | 315 | 367 | 367 | 367 | nfl-ownership-prior-v3 | 98 | 2 |
| pool_capture | de2bd3c0:live:2026-10-18 16:57:00+00 | live_pool | 2026-10-18 16:57 | 2 | - | 7/10 | 366 | 317 | 366 | 366 | 366 | nfl-ownership-prior-v3 | 97 | 2 |
| pool_capture | 5db2c428:live:2026-10-18 16:58:00+00 | live_pool | 2026-10-18 16:58 | 1 | - | 7/10 | 367 | 315 | 367 | 367 | 367 | nfl-ownership-prior-v3 | 98 | 2 |
| pool_capture | de2bd3c0:live:2026-10-18 16:58:00+00 | live_pool | 2026-10-18 16:58 | 1 | - | 7/10 | 366 | 317 | 366 | 366 | 366 | nfl-ownership-prior-v3 | 97 | 2 |
| pool_capture | 5db2c428:live:2026-10-18 16:59:00+00 | live_pool | 2026-10-18 16:59 | 0 | - | 7/10 | 367 | 315 | 367 | 367 | 367 | nfl-ownership-prior-v3 | 98 | 2 |
| pool_capture | de2bd3c0:live:2026-10-18 16:59:00+00 | live_pool | 2026-10-18 16:59 | 0 | - | 7/10 | 366 | 317 | 366 | 366 | 366 | nfl-ownership-prior-v3 | 97 | 2 |
| pool_capture | 5db2c428:live:2026-10-18 19:45:00+00 | live_pool | 2026-10-18 19:45 | -166 | - | 1/10 | 51 | 42 | 51 | 51 | 51 | nfl-ownership-prior-v3 | 15 | 1 |
| pool_capture | de2bd3c0:live:2026-10-18 19:45:00+00 | live_pool | 2026-10-18 19:45 | -166 | - | 1/10 | 51 | 42 | 51 | 51 | 51 | nfl-ownership-prior-v3 | 15 | 1 |
| pool_capture | 5db2c428:live:2026-10-18 19:46:00+00 | live_pool | 2026-10-18 19:46 | -167 | - | 1/10 | 51 | 42 | 51 | 51 | 51 | nfl-ownership-prior-v3 | 15 | 1 |
| pool_capture | de2bd3c0:live:2026-10-18 19:46:00+00 | live_pool | 2026-10-18 19:46 | -167 | - | 1/10 | 51 | 42 | 51 | 51 | 51 | nfl-ownership-prior-v3 | 15 | 1 |
| pool_capture | 5db2c428:live:2026-10-18 19:47:00+00 | live_pool | 2026-10-18 19:47 | -168 | - | 1/10 | 51 | 42 | 51 | 51 | 51 | nfl-ownership-prior-v3 | 15 | 1 |
| pool_capture | de2bd3c0:live:2026-10-18 19:47:00+00 | live_pool | 2026-10-18 19:47 | -168 | - | 1/10 | 51 | 42 | 51 | 51 | 51 | nfl-ownership-prior-v3 | 15 | 1 |
| pool_capture | 5db2c428:live:2026-10-18 19:48:00+00 | live_pool | 2026-10-18 19:48 | -169 | - | 1/10 | 51 | 42 | 51 | 51 | 51 | nfl-ownership-prior-v3 | 15 | 1 |
| pool_capture | de2bd3c0:live:2026-10-18 19:48:00+00 | live_pool | 2026-10-18 19:48 | -169 | - | 1/10 | 51 | 42 | 51 | 51 | 51 | nfl-ownership-prior-v3 | 15 | 1 |
| pool_capture | 5db2c428:live:2026-10-18 19:49:00+00 | live_pool | 2026-10-18 19:49 | -170 | - | 1/10 | 51 | 42 | 51 | 51 | 51 | nfl-ownership-prior-v3 | 15 | 1 |
| pool_capture | de2bd3c0:live:2026-10-18 19:49:00+00 | live_pool | 2026-10-18 19:49 | -170 | - | 1/10 | 51 | 42 | 51 | 51 | 51 | nfl-ownership-prior-v3 | 15 | 1 |
| pool_capture | 5db2c428:live:2026-10-18 19:50:00+00 | live_pool | 2026-10-18 19:50 | -171 | - | 1/10 | 51 | 42 | 51 | 51 | 51 | nfl-ownership-prior-v3 | 15 | 1 |
| pool_capture | de2bd3c0:live:2026-10-18 19:50:00+00 | live_pool | 2026-10-18 19:50 | -171 | - | 1/10 | 51 | 42 | 51 | 51 | 51 | nfl-ownership-prior-v3 | 15 | 1 |
| pool_capture | 5db2c428:live:2026-10-18 19:51:00+00 | live_pool | 2026-10-18 19:51 | -172 | - | 1/10 | 51 | 42 | 51 | 51 | 51 | nfl-ownership-prior-v3 | 15 | 1 |
| pool_capture | de2bd3c0:live:2026-10-18 19:51:00+00 | live_pool | 2026-10-18 19:51 | -172 | - | 1/10 | 51 | 42 | 51 | 51 | 51 | nfl-ownership-prior-v3 | 15 | 1 |
| pool_capture | 5db2c428:live:2026-10-18 19:52:00+00 | live_pool | 2026-10-18 19:52 | -173 | - | 1/10 | 51 | 42 | 51 | 51 | 51 | nfl-ownership-prior-v3 | 15 | 1 |
| pool_capture | de2bd3c0:live:2026-10-18 19:52:00+00 | live_pool | 2026-10-18 19:52 | -173 | - | 1/10 | 51 | 42 | 51 | 51 | 51 | nfl-ownership-prior-v3 | 15 | 1 |
| pool_capture | 5db2c428:live:2026-10-18 19:53:00+00 | live_pool | 2026-10-18 19:53 | -174 | - | 1/10 | 51 | 42 | 51 | 51 | 51 | nfl-ownership-prior-v3 | 15 | 1 |
| pool_capture | de2bd3c0:live:2026-10-18 19:53:00+00 | live_pool | 2026-10-18 19:53 | -174 | - | 1/10 | 51 | 42 | 51 | 51 | 51 | nfl-ownership-prior-v3 | 15 | 1 |
| pool_capture | 5db2c428:live:2026-10-18 19:54:00+00 | live_pool | 2026-10-18 19:54 | -175 | - | 1/10 | 51 | 42 | 51 | 51 | 51 | nfl-ownership-prior-v3 | 15 | 1 |
| pool_capture | de2bd3c0:live:2026-10-18 19:54:00+00 | live_pool | 2026-10-18 19:54 | -175 | - | 1/10 | 51 | 42 | 51 | 51 | 51 | nfl-ownership-prior-v3 | 15 | 1 |
| pool_capture | 5db2c428:live:2026-10-18 19:55:00+00 | live_pool | 2026-10-18 19:55 | -176 | - | 1/10 | 51 | 42 | 51 | 51 | 51 | nfl-ownership-prior-v3 | 15 | 1 |
| pool_capture | de2bd3c0:live:2026-10-18 19:55:00+00 | live_pool | 2026-10-18 19:55 | -176 | - | 1/10 | 51 | 42 | 51 | 51 | 51 | nfl-ownership-prior-v3 | 15 | 1 |
| pool_capture | 5db2c428:live:2026-10-18 19:56:00+00 | live_pool | 2026-10-18 19:56 | -177 | - | 1/10 | 51 | 42 | 51 | 51 | 51 | nfl-ownership-prior-v3 | 15 | 1 |
| pool_capture | de2bd3c0:live:2026-10-18 19:56:00+00 | live_pool | 2026-10-18 19:56 | -177 | - | 1/10 | 51 | 42 | 51 | 51 | 51 | nfl-ownership-prior-v3 | 15 | 1 |
| pool_capture | 5db2c428:live:2026-10-18 19:57:00+00 | live_pool | 2026-10-18 19:57 | -178 | - | 1/10 | 51 | 42 | 51 | 51 | 51 | nfl-ownership-prior-v3 | 15 | 1 |
| pool_capture | de2bd3c0:live:2026-10-18 19:57:00+00 | live_pool | 2026-10-18 19:57 | -178 | - | 1/10 | 51 | 42 | 51 | 51 | 51 | nfl-ownership-prior-v3 | 15 | 1 |
| pool_capture | 5db2c428:live:2026-10-18 19:58:00+00 | live_pool | 2026-10-18 19:58 | -179 | - | 1/10 | 51 | 42 | 51 | 51 | 51 | nfl-ownership-prior-v3 | 15 | 1 |
| pool_capture | de2bd3c0:live:2026-10-18 19:58:00+00 | live_pool | 2026-10-18 19:58 | -179 | - | 1/10 | 51 | 42 | 51 | 51 | 51 | nfl-ownership-prior-v3 | 15 | 1 |
| pool_capture | 5db2c428:live:2026-10-18 19:59:00+00 | live_pool | 2026-10-18 19:59 | -180 | - | 1/10 | 51 | 42 | 51 | 51 | 51 | nfl-ownership-prior-v3 | 15 | 1 |
| pool_capture | de2bd3c0:live:2026-10-18 19:59:00+00 | live_pool | 2026-10-18 19:59 | -180 | - | 1/10 | 51 | 42 | 51 | 51 | 51 | nfl-ownership-prior-v3 | 15 | 1 |
| pool_capture | 5db2c428:live:2026-10-18 20:00:00+00 | live_pool | 2026-10-18 20:00 | -181 | - | 1/10 | 51 | 42 | 51 | 51 | 51 | nfl-ownership-prior-v3 | 15 | 1 |
| pool_capture | de2bd3c0:live:2026-10-18 20:00:00+00 | live_pool | 2026-10-18 20:00 | -181 | - | 1/10 | 51 | 42 | 51 | 51 | 51 | nfl-ownership-prior-v3 | 15 | 1 |
| pool_capture | 5db2c428:live:2026-10-18 20:01:00+00 | live_pool | 2026-10-18 20:01 | -182 | - | 1/10 | 51 | 42 | 51 | 51 | 51 | nfl-ownership-prior-v3 | 15 | 1 |
| pool_capture | de2bd3c0:live:2026-10-18 20:01:00+00 | live_pool | 2026-10-18 20:01 | -182 | - | 1/10 | 51 | 42 | 51 | 51 | 51 | nfl-ownership-prior-v3 | 15 | 1 |
| pool_capture | 5db2c428:live:2026-10-18 20:02:00+00 | live_pool | 2026-10-18 20:02 | -183 | - | 1/10 | 51 | 42 | 51 | 51 | 51 | nfl-ownership-prior-v3 | 15 | 1 |
| pool_capture | de2bd3c0:live:2026-10-18 20:02:00+00 | live_pool | 2026-10-18 20:02 | -183 | - | 1/10 | 51 | 42 | 51 | 51 | 51 | nfl-ownership-prior-v3 | 15 | 1 |
| pool_capture | 5db2c428:live:2026-10-18 20:03:00+00 | live_pool | 2026-10-18 20:03 | -184 | - | 1/10 | 51 | 42 | 51 | 51 | 51 | nfl-ownership-prior-v3 | 15 | 1 |
| pool_capture | de2bd3c0:live:2026-10-18 20:03:00+00 | live_pool | 2026-10-18 20:03 | -184 | - | 1/10 | 51 | 42 | 51 | 51 | 51 | nfl-ownership-prior-v3 | 15 | 1 |
| pool_capture | 5db2c428:live:2026-10-18 20:04:00+00 | live_pool | 2026-10-18 20:04 | -185 | - | 1/10 | 51 | 42 | 51 | 51 | 51 | nfl-ownership-prior-v3 | 15 | 1 |
| pool_capture | de2bd3c0:live:2026-10-18 20:04:00+00 | live_pool | 2026-10-18 20:04 | -185 | - | 1/10 | 51 | 42 | 51 | 51 | 51 | nfl-ownership-prior-v3 | 15 | 1 |
| pool_capture | 5db2c428:live:2026-10-18 20:05:00+00 | live_pool | 2026-10-18 20:05 | -186 | - | 2/10 | 108 | 87 | 108 | 108 | 108 | nfl-ownership-prior-v3 | 30 | 0 |
| pool_capture | de2bd3c0:live:2026-10-18 20:05:00+00 | live_pool | 2026-10-18 20:05 | -186 | - | 2/10 | 108 | 87 | 108 | 108 | 108 | nfl-ownership-prior-v3 | 30 | 0 |
| pool_capture | 5db2c428:live:2026-10-18 20:06:00+00 | live_pool | 2026-10-18 20:06 | -187 | - | 2/10 | 108 | 87 | 108 | 108 | 108 | nfl-ownership-prior-v3 | 30 | 0 |
| pool_capture | de2bd3c0:live:2026-10-18 20:06:00+00 | live_pool | 2026-10-18 20:06 | -187 | - | 2/10 | 108 | 87 | 108 | 108 | 108 | nfl-ownership-prior-v3 | 30 | 0 |
| pool_capture | 5db2c428:live:2026-10-18 20:07:00+00 | live_pool | 2026-10-18 20:07 | -188 | - | 2/10 | 108 | 87 | 108 | 108 | 108 | nfl-ownership-prior-v3 | 30 | 0 |
| pool_capture | de2bd3c0:live:2026-10-18 20:07:00+00 | live_pool | 2026-10-18 20:07 | -188 | - | 2/10 | 108 | 87 | 108 | 108 | 108 | nfl-ownership-prior-v3 | 30 | 0 |
| pool_capture | 5db2c428:live:2026-10-18 20:08:00+00 | live_pool | 2026-10-18 20:08 | -189 | - | 2/10 | 108 | 87 | 108 | 108 | 108 | nfl-ownership-prior-v3 | 30 | 0 |
| pool_capture | de2bd3c0:live:2026-10-18 20:08:00+00 | live_pool | 2026-10-18 20:08 | -189 | - | 2/10 | 108 | 87 | 108 | 108 | 108 | nfl-ownership-prior-v3 | 30 | 0 |
| pool_capture | 5db2c428:live:2026-10-18 20:09:00+00 | live_pool | 2026-10-18 20:09 | -190 | - | 2/10 | 108 | 87 | 108 | 108 | 108 | nfl-ownership-prior-v3 | 30 | 0 |
| pool_capture | de2bd3c0:live:2026-10-18 20:09:00+00 | live_pool | 2026-10-18 20:09 | -190 | - | 2/10 | 108 | 87 | 108 | 108 | 108 | nfl-ownership-prior-v3 | 30 | 0 |
| pool_capture | 5db2c428:live:2026-10-18 20:10:00+00 | live_pool | 2026-10-18 20:10 | -191 | - | 2/10 | 108 | 87 | 108 | 108 | 108 | nfl-ownership-prior-v3 | 30 | 0 |
| pool_capture | de2bd3c0:live:2026-10-18 20:10:00+00 | live_pool | 2026-10-18 20:10 | -191 | - | 2/10 | 108 | 87 | 108 | 108 | 108 | nfl-ownership-prior-v3 | 30 | 0 |
| pool_capture | 5db2c428:live:2026-10-18 20:11:00+00 | live_pool | 2026-10-18 20:11 | -192 | - | 2/10 | 108 | 87 | 108 | 108 | 108 | nfl-ownership-prior-v3 | 30 | 0 |
| pool_capture | de2bd3c0:live:2026-10-18 20:11:00+00 | live_pool | 2026-10-18 20:11 | -192 | - | 2/10 | 108 | 87 | 108 | 108 | 108 | nfl-ownership-prior-v3 | 30 | 0 |
| pool_capture | 5db2c428:live:2026-10-18 20:12:00+00 | live_pool | 2026-10-18 20:12 | -193 | - | 2/10 | 108 | 87 | 108 | 108 | 108 | nfl-ownership-prior-v3 | 30 | 0 |
| pool_capture | de2bd3c0:live:2026-10-18 20:12:00+00 | live_pool | 2026-10-18 20:12 | -193 | - | 2/10 | 108 | 87 | 108 | 108 | 108 | nfl-ownership-prior-v3 | 30 | 0 |
| pool_capture | 5db2c428:live:2026-10-18 20:13:00+00 | live_pool | 2026-10-18 20:13 | -194 | - | 2/10 | 108 | 87 | 108 | 108 | 108 | nfl-ownership-prior-v3 | 30 | 0 |
| pool_capture | de2bd3c0:live:2026-10-18 20:13:00+00 | live_pool | 2026-10-18 20:13 | -194 | - | 2/10 | 108 | 87 | 108 | 108 | 108 | nfl-ownership-prior-v3 | 30 | 0 |
| pool_capture | 5db2c428:live:2026-10-18 20:14:00+00 | live_pool | 2026-10-18 20:14 | -195 | - | 2/10 | 108 | 87 | 108 | 108 | 108 | nfl-ownership-prior-v3 | 30 | 0 |
| pool_capture | de2bd3c0:live:2026-10-18 20:14:00+00 | live_pool | 2026-10-18 20:14 | -195 | - | 2/10 | 108 | 87 | 108 | 108 | 108 | nfl-ownership-prior-v3 | 30 | 0 |
| pool_capture | 5db2c428:live:2026-10-18 20:15:00+00 | live_pool | 2026-10-18 20:15 | -196 | - | 2/10 | 108 | 87 | 108 | 108 | 108 | nfl-ownership-prior-v3 | 30 | 0 |
| pool_capture | de2bd3c0:live:2026-10-18 20:15:00+00 | live_pool | 2026-10-18 20:15 | -196 | - | 2/10 | 108 | 87 | 108 | 108 | 108 | nfl-ownership-prior-v3 | 30 | 0 |
| pool_capture | 5db2c428:live:2026-10-18 20:16:00+00 | live_pool | 2026-10-18 20:16 | -197 | - | 2/10 | 108 | 87 | 108 | 108 | 108 | nfl-ownership-prior-v3 | 30 | 0 |
| pool_capture | de2bd3c0:live:2026-10-18 20:16:00+00 | live_pool | 2026-10-18 20:16 | -197 | - | 2/10 | 108 | 87 | 108 | 108 | 108 | nfl-ownership-prior-v3 | 30 | 0 |
| pool_capture | 5db2c428:live:2026-10-18 20:17:00+00 | live_pool | 2026-10-18 20:17 | -198 | - | 2/10 | 108 | 87 | 108 | 108 | 108 | nfl-ownership-prior-v3 | 30 | 0 |
| pool_capture | de2bd3c0:live:2026-10-18 20:17:00+00 | live_pool | 2026-10-18 20:17 | -198 | - | 2/10 | 108 | 87 | 108 | 108 | 108 | nfl-ownership-prior-v3 | 30 | 0 |
| pool_capture | 5db2c428:live:2026-10-18 20:18:00+00 | live_pool | 2026-10-18 20:18 | -199 | - | 2/10 | 108 | 87 | 108 | 108 | 108 | nfl-ownership-prior-v3 | 30 | 0 |
| pool_capture | de2bd3c0:live:2026-10-18 20:18:00+00 | live_pool | 2026-10-18 20:18 | -199 | - | 2/10 | 108 | 87 | 108 | 108 | 108 | nfl-ownership-prior-v3 | 30 | 0 |
| pool_capture | 5db2c428:live:2026-10-18 20:19:00+00 | live_pool | 2026-10-18 20:19 | -200 | - | 2/10 | 108 | 87 | 108 | 108 | 108 | nfl-ownership-prior-v3 | 30 | 0 |
| pool_capture | de2bd3c0:live:2026-10-18 20:19:00+00 | live_pool | 2026-10-18 20:19 | -200 | - | 2/10 | 108 | 87 | 108 | 108 | 108 | nfl-ownership-prior-v3 | 30 | 0 |
| pool_capture | 5db2c428:live:2026-10-18 20:20:00+00 | live_pool | 2026-10-18 20:20 | -201 | - | 2/10 | 108 | 87 | 108 | 108 | 108 | nfl-ownership-prior-v3 | 30 | 0 |
| pool_capture | de2bd3c0:live:2026-10-18 20:20:00+00 | live_pool | 2026-10-18 20:20 | -201 | - | 2/10 | 108 | 87 | 108 | 108 | 108 | nfl-ownership-prior-v3 | 30 | 0 |
| pool_capture | 5db2c428:live:2026-10-18 20:21:00+00 | live_pool | 2026-10-18 20:21 | -202 | - | 2/10 | 108 | 87 | 108 | 108 | 108 | nfl-ownership-prior-v3 | 30 | 0 |
| pool_capture | de2bd3c0:live:2026-10-18 20:21:00+00 | live_pool | 2026-10-18 20:21 | -202 | - | 2/10 | 108 | 87 | 108 | 108 | 108 | nfl-ownership-prior-v3 | 30 | 0 |
| pool_capture | 5db2c428:live:2026-10-18 20:22:00+00 | live_pool | 2026-10-18 20:22 | -203 | - | 2/10 | 108 | 87 | 108 | 108 | 108 | nfl-ownership-prior-v3 | 30 | 0 |
| pool_capture | de2bd3c0:live:2026-10-18 20:22:00+00 | live_pool | 2026-10-18 20:22 | -203 | - | 2/10 | 108 | 87 | 108 | 108 | 108 | nfl-ownership-prior-v3 | 30 | 0 |
| pool_capture | 5db2c428:live:2026-10-18 20:23:00+00 | live_pool | 2026-10-18 20:23 | -204 | - | 2/10 | 108 | 87 | 108 | 108 | 108 | nfl-ownership-prior-v3 | 30 | 0 |
| pool_capture | de2bd3c0:live:2026-10-18 20:23:00+00 | live_pool | 2026-10-18 20:23 | -204 | - | 2/10 | 108 | 87 | 108 | 108 | 108 | nfl-ownership-prior-v3 | 30 | 0 |
| pool_capture | 5db2c428:live:2026-10-18 20:24:00+00 | live_pool | 2026-10-18 20:24 | -205 | - | 2/10 | 108 | 87 | 108 | 108 | 108 | nfl-ownership-prior-v3 | 30 | 0 |
| pool_capture | de2bd3c0:live:2026-10-18 20:24:00+00 | live_pool | 2026-10-18 20:24 | -205 | - | 2/10 | 108 | 87 | 108 | 108 | 108 | nfl-ownership-prior-v3 | 30 | 0 |

### showdown DAL@GB — week 6, first kickoff 2026-10-19 00:20 UTC

Uploads 2 (system, upload); contests none; status **canonical**.

| source | id | origin | at (UTC) | min before kickoff | horizon | games | players | proj | avail | dkStatus | ownPct | ownership version | anchor | commit |
|---|---|---|---|---:|---|---|---:|---:|---:|---:|---:|---|---:|---:|
| pool_capture | c4ffcc6d:live:2026-10-18 19:00:00+00 | live_pool | 2026-10-18 19:00 | 319 | CEE* | 1/1 | 55 | 46 | 55 | 55 | 55 | nfl-ownership-prior-v3 | 15 | 0 |
| optimizer_run | 9295023e | partial/gpp | 2026-10-18 19:01 | 319 | E | all | 55 | 46 | 55 | 55 | 55 | nfl-ownership-prior-v3 | 15 | 0 |
| optimizer_run | 55d5d160 | partial/gpp | 2026-10-18 19:08 | 311 | - | all | 55 | 46 | 55 | 55 | 55 | nfl-ownership-prior-v3 | 15 | 0 |
| optimizer_run | fbdaac30 | partial/gpp | 2026-10-18 19:22 | 297 | - | all | 55 | 46 | 55 | 55 | 55 | nfl-ownership-prior-v3 | 15 | 0 |
| optimizer_run | 5aad1020 | partial/gpp | 2026-10-18 20:15 | 244 | - | all | 55 | 46 | 55 | 55 | 55 | nfl-ownership-prior-v3 | 15 | 0 |
| optimizer_run | ffe49069 | partial/gpp | 2026-10-18 20:18 | 242 | C | all | 55 | 46 | 55 | 55 | 55 | nfl-ownership-prior-v3 | 15 | 0 |
| ghost_freeze | 34a43a44 | system | 2026-10-18 22:35 | 104 | CEC* | all | 55 | 46 | 55 | 55 | 55 | nfl-ownership-prior-v3 | 15 | 0 |
| pool_capture | c4ffcc6d:live:2026-10-19 00:00:00+00 | live_pool | 2026-10-19 00:00 | 19 | - | 1/1 | 55 | 47 | 55 | 55 | 55 | nfl-ownership-prior-v3 | 17 | 0 |
| pool_capture | c4ffcc6d:live:2026-10-19 00:01:00+00 | live_pool | 2026-10-19 00:01 | 18 | - | 1/1 | 55 | 47 | 55 | 55 | 55 | nfl-ownership-prior-v3 | 17 | 0 |
| pool_capture | c4ffcc6d:live:2026-10-19 00:02:00+00 | live_pool | 2026-10-19 00:02 | 17 | - | 1/1 | 55 | 47 | 55 | 55 | 55 | nfl-ownership-prior-v3 | 17 | 0 |
| pool_capture | c4ffcc6d:live:2026-10-19 00:03:00+00 | live_pool | 2026-10-19 00:03 | 16 | - | 1/1 | 55 | 47 | 55 | 55 | 55 | nfl-ownership-prior-v3 | 17 | 0 |
| pool_capture | c4ffcc6d:live:2026-10-19 00:04:00+00 | live_pool | 2026-10-19 00:04 | 15 | - | 1/1 | 55 | 47 | 55 | 55 | 55 | nfl-ownership-prior-v3 | 17 | 0 |
| pool_capture | c4ffcc6d:live:2026-10-19 00:05:00+00 | live_pool | 2026-10-19 00:05 | 14 | - | 1/1 | 55 | 47 | 55 | 55 | 55 | nfl-ownership-prior-v3 | 17 | 0 |
| pool_capture | c4ffcc6d:live:2026-10-19 00:06:00+00 | live_pool | 2026-10-19 00:06 | 13 | - | 1/1 | 55 | 47 | 55 | 55 | 55 | nfl-ownership-prior-v3 | 17 | 0 |
| pool_capture | c4ffcc6d:live:2026-10-19 00:07:00+00 | live_pool | 2026-10-19 00:07 | 12 | - | 1/1 | 55 | 47 | 55 | 55 | 55 | nfl-ownership-prior-v3 | 17 | 0 |
| pool_capture | c4ffcc6d:live:2026-10-19 00:08:00+00 | live_pool | 2026-10-19 00:08 | 11 | - | 1/1 | 55 | 47 | 55 | 55 | 55 | nfl-ownership-prior-v3 | 17 | 0 |
| pool_capture | c4ffcc6d:live:2026-10-19 00:09:00+00 | live_pool | 2026-10-19 00:09 | 10 | - | 1/1 | 55 | 47 | 55 | 55 | 55 | nfl-ownership-prior-v3 | 17 | 0 |
| pool_capture | c4ffcc6d:live:2026-10-19 00:10:00+00 | live_pool | 2026-10-19 00:10 | 9 | - | 1/1 | 55 | 47 | 55 | 55 | 55 | nfl-ownership-prior-v3 | 17 | 0 |
| pool_capture | c4ffcc6d:live:2026-10-19 00:11:00+00 | live_pool | 2026-10-19 00:11 | 8 | - | 1/1 | 55 | 47 | 55 | 55 | 55 | nfl-ownership-prior-v3 | 17 | 0 |
| pool_capture | c4ffcc6d:live:2026-10-19 00:12:00+00 | live_pool | 2026-10-19 00:12 | 7 | - | 1/1 | 55 | 47 | 55 | 55 | 55 | nfl-ownership-prior-v3 | 17 | 0 |
| pool_capture | c4ffcc6d:live:2026-10-19 00:13:00+00 | live_pool | 2026-10-19 00:13 | 6 | - | 1/1 | 55 | 47 | 55 | 55 | 55 | nfl-ownership-prior-v3 | 17 | 0 |
| pool_capture | c4ffcc6d:live:2026-10-19 00:14:00+00 | live_pool | 2026-10-19 00:14 | 5 | - | 1/1 | 55 | 47 | 55 | 55 | 55 | nfl-ownership-prior-v3 | 17 | 0 |
| pool_capture | c4ffcc6d:live:2026-10-19 00:15:00+00 | live_pool | 2026-10-19 00:15 | 4 | - | 1/1 | 55 | 47 | 55 | 55 | 55 | nfl-ownership-prior-v3 | 17 | 0 |
| pool_capture | c4ffcc6d:live:2026-10-19 00:16:00+00 | live_pool | 2026-10-19 00:16 | 3 | - | 1/1 | 55 | 47 | 55 | 55 | 55 | nfl-ownership-prior-v3 | 17 | 0 |
| pool_capture | c4ffcc6d:live:2026-10-19 00:17:00+00 | live_pool | 2026-10-19 00:17 | 2 | - | 1/1 | 55 | 47 | 55 | 55 | 55 | nfl-ownership-prior-v3 | 17 | 0 |
| pool_capture | c4ffcc6d:live:2026-10-19 00:18:00+00 | live_pool | 2026-10-19 00:18 | 1 | - | 1/1 | 55 | 47 | 55 | 55 | 55 | nfl-ownership-prior-v3 | 17 | 0 |
| pool_capture | c4ffcc6d:live:2026-10-19 00:19:00+00 | live_pool | 2026-10-19 00:19 | 0 | - | 1/1 | 55 | 47 | 55 | 55 | 55 | nfl-ownership-prior-v3 | 17 | 0 |

### showdown WAS@SF — week 6, first kickoff 2026-10-20 00:15 UTC

Uploads 4 (system, upload); contests none; status **canonical**.

| source | id | origin | at (UTC) | min before kickoff | horizon | games | players | proj | avail | dkStatus | ownPct | ownership version | anchor | commit |
|---|---|---|---|---:|---|---|---:|---:|---:|---:|---:|---|---:|---:|
| pool_capture | b2ce597f:live:2026-10-19 21:57:00+00 | live_pool | 2026-10-19 21:57 | 137 | EE* | 1/1 | 60 | 50 | 60 | 60 | 60 | nfl-ownership-prior-v3 | 10 | 1 |
| optimizer_run | d01363e5 | partial/gpp | 2026-10-19 21:57 | 137 | E | all | 60 | 50 | 60 | 60 | 60 | nfl-ownership-prior-v3 | 10 | 1 |
| ghost_freeze | cdb25c08 | system | 2026-10-19 22:30 | 104 | CE | all | 60 | 52 | 60 | 60 | 60 | nfl-ownership-prior-v3 | 10 | 2 |
| pool_capture | 37c29c58:live:2026-10-19 22:43:00+00 | live_pool | 2026-10-19 22:43 | 91 | - | 1/1 | 60 | 52 | 60 | 60 | 60 | nfl-ownership-prior-v3 | 10 | 2 |
| optimizer_run | 4aa1427e | partial/gpp | 2026-10-19 22:58 | 77 | - | all | 60 | 52 | 60 | 60 | 60 | nfl-ownership-prior-v3 | 10 | 2 |
| pool_capture | dafd66f0:live:2026-10-19 23:03:00+00 | live_pool | 2026-10-19 23:03 | 71 | C | 1/1 | 60 | 52 | 60 | 60 | 60 | nfl-ownership-prior-v3 | 10 | 2 |
| optimizer_run | cc15f1a2 | partial/gpp | 2026-10-19 23:04 | 70 | - | all | 60 | 52 | 60 | 60 | 60 | nfl-ownership-prior-v3 | 10 | 2 |
| optimizer_run | 21bf2279 | partial/gpp | 2026-10-19 23:13 | 62 | CC* | all | 60 | 52 | 60 | 60 | 60 | nfl-ownership-prior-v3 | 13 | 1 |
| pool_capture | dafd66f0:live:2026-10-19 23:55:00+00 | live_pool | 2026-10-19 23:55 | 19 | - | 1/1 | 60 | 52 | 60 | 60 | 60 | nfl-ownership-prior-v3 | 13 | 1 |
| pool_capture | 37c29c58:live:2026-10-19 23:55:00+00 | live_pool | 2026-10-19 23:55 | 19 | - | 1/1 | 60 | 52 | 60 | 60 | 60 | nfl-ownership-prior-v3 | 13 | 1 |
| pool_capture | dafd66f0:live:2026-10-19 23:57:00+00 | live_pool | 2026-10-19 23:57 | 18 | - | 1/1 | 60 | 52 | 60 | 60 | 60 | nfl-ownership-prior-v3 | 13 | 1 |
| pool_capture | 37c29c58:live:2026-10-19 23:57:00+00 | live_pool | 2026-10-19 23:57 | 18 | - | 1/1 | 60 | 52 | 60 | 60 | 60 | nfl-ownership-prior-v3 | 13 | 1 |
| pool_capture | dafd66f0:live:2026-10-19 23:58:00+00 | live_pool | 2026-10-19 23:58 | 17 | - | 1/1 | 60 | 52 | 60 | 60 | 60 | nfl-ownership-prior-v3 | 13 | 1 |
| pool_capture | 37c29c58:live:2026-10-19 23:58:00+00 | live_pool | 2026-10-19 23:58 | 17 | - | 1/1 | 60 | 52 | 60 | 60 | 60 | nfl-ownership-prior-v3 | 13 | 1 |
| pool_capture | dafd66f0:live:2026-10-19 23:59:00+00 | live_pool | 2026-10-19 23:59 | 15 | - | 1/1 | 60 | 52 | 60 | 60 | 60 | nfl-ownership-prior-v3 | 13 | 1 |
| pool_capture | 37c29c58:live:2026-10-19 23:59:00+00 | live_pool | 2026-10-19 23:59 | 15 | - | 1/1 | 60 | 52 | 60 | 60 | 60 | nfl-ownership-prior-v3 | 13 | 1 |
| pool_capture | dafd66f0:live:2026-10-20 00:00:00+00 | live_pool | 2026-10-20 00:00 | 14 | - | 1/1 | 60 | 52 | 60 | 60 | 60 | nfl-ownership-prior-v3 | 13 | 1 |
| pool_capture | 37c29c58:live:2026-10-20 00:00:00+00 | live_pool | 2026-10-20 00:00 | 14 | - | 1/1 | 60 | 52 | 60 | 60 | 60 | nfl-ownership-prior-v3 | 13 | 1 |
| pool_capture | dafd66f0:live:2026-10-20 00:01:00+00 | live_pool | 2026-10-20 00:01 | 13 | - | 1/1 | 60 | 52 | 60 | 60 | 60 | nfl-ownership-prior-v3 | 13 | 1 |
| pool_capture | 37c29c58:live:2026-10-20 00:01:00+00 | live_pool | 2026-10-20 00:01 | 13 | - | 1/1 | 60 | 52 | 60 | 60 | 60 | nfl-ownership-prior-v3 | 13 | 1 |
| pool_capture | dafd66f0:live:2026-10-20 00:02:00+00 | live_pool | 2026-10-20 00:02 | 12 | - | 1/1 | 60 | 52 | 60 | 60 | 60 | nfl-ownership-prior-v3 | 13 | 1 |
| pool_capture | 37c29c58:live:2026-10-20 00:02:00+00 | live_pool | 2026-10-20 00:02 | 12 | - | 1/1 | 60 | 52 | 60 | 60 | 60 | nfl-ownership-prior-v3 | 13 | 1 |
| pool_capture | dafd66f0:live:2026-10-20 00:03:00+00 | live_pool | 2026-10-20 00:03 | 11 | - | 1/1 | 60 | 52 | 60 | 60 | 60 | nfl-ownership-prior-v3 | 13 | 1 |
| pool_capture | 37c29c58:live:2026-10-20 00:03:00+00 | live_pool | 2026-10-20 00:03 | 11 | - | 1/1 | 60 | 52 | 60 | 60 | 60 | nfl-ownership-prior-v3 | 13 | 1 |
| pool_capture | dafd66f0:live:2026-10-20 00:04:00+00 | live_pool | 2026-10-20 00:04 | 10 | - | 1/1 | 60 | 52 | 60 | 60 | 60 | nfl-ownership-prior-v3 | 13 | 1 |
| pool_capture | 37c29c58:live:2026-10-20 00:04:00+00 | live_pool | 2026-10-20 00:04 | 10 | - | 1/1 | 60 | 52 | 60 | 60 | 60 | nfl-ownership-prior-v3 | 13 | 1 |
| pool_capture | dafd66f0:live:2026-10-20 00:05:00+00 | live_pool | 2026-10-20 00:05 | 9 | - | 1/1 | 60 | 52 | 60 | 60 | 60 | nfl-ownership-prior-v4 | 13 | 1 |
| pool_capture | 37c29c58:live:2026-10-20 00:05:00+00 | live_pool | 2026-10-20 00:05 | 9 | - | 1/1 | 60 | 52 | 60 | 60 | 60 | nfl-ownership-prior-v4 | 13 | 1 |
| pool_capture | dafd66f0:live:2026-10-20 00:06:00+00 | live_pool | 2026-10-20 00:06 | 8 | - | 1/1 | 60 | 52 | 60 | 60 | 60 | nfl-ownership-prior-v4 | 13 | 1 |
| pool_capture | 37c29c58:live:2026-10-20 00:06:00+00 | live_pool | 2026-10-20 00:06 | 8 | - | 1/1 | 60 | 52 | 60 | 60 | 60 | nfl-ownership-prior-v4 | 13 | 1 |
| pool_capture | dafd66f0:live:2026-10-20 00:07:00+00 | live_pool | 2026-10-20 00:07 | 7 | - | 1/1 | 60 | 52 | 60 | 60 | 60 | nfl-ownership-prior-v4 | 13 | 1 |
| pool_capture | 37c29c58:live:2026-10-20 00:07:00+00 | live_pool | 2026-10-20 00:07 | 7 | - | 1/1 | 60 | 52 | 60 | 60 | 60 | nfl-ownership-prior-v4 | 13 | 1 |
| pool_capture | dafd66f0:live:2026-10-20 00:08:00+00 | live_pool | 2026-10-20 00:08 | 6 | - | 1/1 | 60 | 52 | 60 | 60 | 60 | nfl-ownership-prior-v4 | 13 | 1 |
| pool_capture | 37c29c58:live:2026-10-20 00:08:00+00 | live_pool | 2026-10-20 00:08 | 6 | - | 1/1 | 60 | 52 | 60 | 60 | 60 | nfl-ownership-prior-v4 | 13 | 1 |
| pool_capture | dafd66f0:live:2026-10-20 00:09:00+00 | live_pool | 2026-10-20 00:09 | 5 | - | 1/1 | 60 | 52 | 60 | 60 | 60 | nfl-ownership-prior-v4 | 13 | 1 |
| pool_capture | 37c29c58:live:2026-10-20 00:09:00+00 | live_pool | 2026-10-20 00:09 | 5 | - | 1/1 | 60 | 52 | 60 | 60 | 60 | nfl-ownership-prior-v4 | 13 | 1 |
| pool_capture | dafd66f0:live:2026-10-20 00:10:00+00 | live_pool | 2026-10-20 00:10 | 4 | - | 1/1 | 60 | 52 | 60 | 60 | 60 | nfl-ownership-prior-v4 | 13 | 1 |
| pool_capture | 37c29c58:live:2026-10-20 00:10:00+00 | live_pool | 2026-10-20 00:10 | 4 | - | 1/1 | 60 | 52 | 60 | 60 | 60 | nfl-ownership-prior-v4 | 13 | 1 |
| pool_capture | dafd66f0:live:2026-10-20 00:11:00+00 | live_pool | 2026-10-20 00:11 | 3 | - | 1/1 | 60 | 52 | 60 | 60 | 60 | nfl-ownership-prior-v4 | 13 | 1 |
| pool_capture | 37c29c58:live:2026-10-20 00:11:00+00 | live_pool | 2026-10-20 00:11 | 3 | - | 1/1 | 60 | 52 | 60 | 60 | 60 | nfl-ownership-prior-v4 | 13 | 1 |
| pool_capture | dafd66f0:live:2026-10-20 00:12:00+00 | live_pool | 2026-10-20 00:12 | 2 | - | 1/1 | 60 | 52 | 60 | 60 | 60 | nfl-ownership-prior-v4 | 13 | 1 |
| pool_capture | 37c29c58:live:2026-10-20 00:12:00+00 | live_pool | 2026-10-20 00:12 | 2 | - | 1/1 | 60 | 52 | 60 | 60 | 60 | nfl-ownership-prior-v4 | 13 | 1 |
| pool_capture | dafd66f0:live:2026-10-20 00:13:00+00 | live_pool | 2026-10-20 00:13 | 1 | - | 1/1 | 60 | 52 | 60 | 60 | 60 | nfl-ownership-prior-v4 | 13 | 1 |
| pool_capture | 37c29c58:live:2026-10-20 00:13:00+00 | live_pool | 2026-10-20 00:13 | 1 | - | 1/1 | 60 | 52 | 60 | 60 | 60 | nfl-ownership-prior-v4 | 13 | 1 |
| pool_capture | dafd66f0:live:2026-10-20 00:14:00+00 | live_pool | 2026-10-20 00:14 | 0 | - | 1/1 | 60 | 52 | 60 | 60 | 60 | nfl-ownership-prior-v4 | 13 | 1 |
| pool_capture | 37c29c58:live:2026-10-20 00:14:00+00 | live_pool | 2026-10-20 00:14 | 0 | - | 1/1 | 60 | 52 | 60 | 60 | 60 | nfl-ownership-prior-v4 | 13 | 1 |

