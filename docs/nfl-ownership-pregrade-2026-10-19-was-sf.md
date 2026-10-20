# Ownership pre-grade record: WAS@SF Showdown, 2026-10-19 (week 6 MNF)

Written after lock (00:15 UTC 10-20) and BEFORE the flagship contest is
imported (Tuesday post-week job, 14:07 UTC). Nothing here may be edited after
the import; corrections append a dated note below. Grading follows
`docs/nfl-ownership-scorecard-v1.md` (registered 2026-10-19). One slate is
descriptive: no verdict, and Showdown has no registered gate.

## Which forecast is frozen

- **The canonical frozen ownership forecast is v3, not v4.** Production ran
  commit `1963fcd7` (prior v3) until the 23:55 UTC capture, `93866b16`
  (still v3) until 00:04, and `b76d66ee` (v4, PR #477) only from 00:05 UTC,
  ten minutes before kickoff. The scorecard's canonical horizon (last capture
  at or before T-20 = 23:55 UTC) therefore holds v3.
- **Pool captures store no ownership and no DraftKings average.** Their
  player rows carry projection, salary, status and range, so an ownership
  forecast cannot be read from them, and the prior cannot be recomputed from
  them exactly (it uses `dkAvg`). Task 1's "holds ownPct" must be read with
  this in mind for Showdown slates.
- **Graded forecast source:** the owner's final pregame build,
  `nfl_dfs_optimizer_runs` `21bf2279-bb8d-4222-84bc-5456489d22b0`
  (created 23:13 UTC, T-62; upload `dafd66f0`; input digest
  `9b72eef2d91bba16b7ce07bbc787d796bd189813d938df7dfa293a83f11c37f8`).
  Its stored per-player ownership is `nfl-ownership-prior-v3`.
- **v4 is graded as an exact recomputation** from that same frozen input with
  favorite SF / underdog WAS (`gameScriptKnown: true`, 27 punt-tier players
  taking 60 flex points). Recomputing v3 the same way reproduces the stored
  v3 numbers to 0.000 on every player, which is what makes the v3/v4
  comparison exact.

## Things the grade must account for

1. **Three uploads.** `b2ce597f` (21:57 UTC) had Austin Ekeler and Brandin
   Cooks unmatched (no projection) because they were missing from the
   identity registry; 9 players were registered by hand at ~22:05 UTC.
   `37c29c58` (22:42) and `dafd66f0` (23:03, different file digest) have
   them. Only `dafd66f0`'s run is graded.
2. **Status at capture:** Mike Evans and Stefon Diggs were Q in the frozen
   forecast and played. The prior's status term applied to both.
3. **Market anchor:** applied to the starters with full DK line sets
   (stored per player as `marketAnchor`); Ekeler, Cooks and Kaelon Black were
   on the model. Report anchored/unanchored counts per the scorecard.
4. **Captain labels from stat-api:** a captain zero for a player no fetched
   user captained is `synthesized_zero` and excluded; only observed captain
   shares are graded.
5. **The MNF flagship must import in full.** Sunday night's Showdown was not
   served free this week. If the Monday flagship is preview-only, the slate
   is `no_label`, not a zero.

## Frozen predictions (captain / flex %, from run 21bf2279's input)

Named in advance so the read-back cannot be chosen after the result. The
"outside view" column is the owner-supplied expert coverage collected
10-17 to 10-19 (directional, not percentages).

| player | pos | salary | status at capture | v3 | v4 | outside view |
|---|---|---|---|---|---|---|
| Brock Purdy | QB | $10,800 | | 31.7 / 63.3 | 33.4 / 61.6 | popular, incl. captain |
| Jayden Daniels | QB | $10,000 | | 22.7 / 72.3 | 24.0 / 71.0 | |
| Christian McCaffrey | RB | $11,800 | | 19.1 / 75.9 | 20.2 / 74.8 | prominent premium |
| George Kittle | TE | $8,200 | | 10.2 / 69.9 | 7.5 / 63.6 | most-recommended captain |
| Mike Evans | WR | $9,600 | Q | 5.1 / 43.9 | 5.3 / 40.0 | depends on status |
| Rachaad White | RB | $6,600 | | 2.2 / 25.5 | 2.4 / 23.2 | disputed vs Ekeler |
| Austin Ekeler | RB | $3,600 | | 0.2 / 5.4 | 0.2 / 4.9 | disputed (one site 24% vs White 13%) |
| Terry McLaurin | WR | $8,000 | | 2.5 / 27.1 | 2.6 / 24.6 | known captain pivot |
| Stefon Diggs | WR | $8,400 | Q | 1.1 / 16.0 | 1.2 / 14.6 | |
| Brandin Cooks | WR | $4,000 | | 0.3 / 6.3 | 0.3 / 5.7 | value chalk |
| Kyle Juszczyk | RB | $1,400 | | 0.1 / 3.1 | 0.1 / 5.8 | projects 0.0; expected v4 over-call |
| Drew Stevens | K | $5,000 | | 1.0 / 15.3 | 0.3 / 8.4 | |
| Eddy Pineiro | K | $5,200 | | 0.8 / 12.3 | 0.2 / 6.7 | |
| 49ers DST | DST | $4,400 | | 0.8 / 12.5 | 0.3 / 14.8 | |
| Commanders DST | DST | $3,400 | | 0.2 / 5.0 | 0.0 / 3.2 | |
| Chig Okonkwo | TE | $4,600 | | 0.4 / 8.2 | 0.3 / 7.5 | cheap alternative |
| Jordan Watkins | WR | $2,000 | | 0.1 / 3.3 | 0.1 / 5.9 | named for large GPPs |
| Jacob Cowing | WR | $2,200 | | 0.0 / 1.2 | 0.0 / 3.9 | named for large GPPs |

## Read-back questions, fixed now

- Kittle captain: is the actual nearer v3 (10.2) or v4 (7.5)? The TE captain
  x0.7 is a v4 design choice read off weeks 2-5.
- Kickers: v4 halves both. Is actual flex nearer 15/12 or 8/7?
- Punt tier: do Watkins, Cowing and Juszczyk land nearer v3 or v4?
- Cooks and Ekeler: both versions say under 7% flex; the outside view says
  higher. A large miss here is a "value hype" signal neither version models.
- Full-pool MAE, bias and tied-rank Spearman for flex and observed captain,
  v3 beside v4, plus allocation by position.

## Result (appended 2026-10-20, after import)

Graded on the owner's DraftKings export of contest 196786695 (47,562 entries,
below the 50,000 target, so `below_target`: descriptive only; complete
export, missing players are reconciled zeros). Pool 45 active players, 44
labelled. The Tuesday flagship import is graded separately.

| slot | version | MAE | bias | Spearman (tied) | top-10 overlap |
|---|---|---|---|---|---|
| flex | v3 | 5.11 | +0.08 | 0.895 | 7 |
| flex | v4 | 5.05 | +0.08 | 0.860 | 7 |
| captain | v3 | 1.11 | +0.01 | 0.916 | 9 |
| captain | v4 | 1.27 | +0.01 | 0.898 | 8 |

Read-back answers: Kittle captain 17.3% (v3 10.2 nearer; v4's TE cut went the
wrong way). Kickers flex 16.3 / 23.4 (v3 nearer; v4's x0.6 went the wrong
way). Punt tier: Juszczyk 11.9 and Cowing 10.7 (v4 nearer), Watkins 4.4
(both close). Cooks 19.3 and Ekeler 19.4 flex against 5-6% in both
versions: the largest misses, matching the outside view's value-chalk call.
Allocation: QB captain 33.9% actual vs 54-57% predicted; WR and TE captain
roughly double what both versions gave. One slate; no constant changes.
