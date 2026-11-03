# Showdown ownership v2 — proposal (2026-11-02)

Status: PROPOSAL, nothing registered, nothing shipped. Written after the
PHI @ WAS review (week 8 Sunday night, DraftKings $1 contest, 3,560 entries)
and sized on the 18 imported full-field Showdowns (weeks 1-8) with the same
leave-one-slate-out harness `nfl-ownership-fit-showdown-v1` was chosen with.
Every number below is therefore in-sample at the level of "which fix to try":
the variants were picked by looking at the results. The registered test in
section 5 is what would make any of them a finding.

## 1. The three misses, restated as model defects

| Miss on PHI @ WAS | What the model did | Defect |
|---|---|---|
| Daniels captain 21% projected vs 38% actual (flex 74 vs 55; his total was right, 95 vs 93) | Captain is allocated first by its own ridge; flex takes the rest up to the 95 total cap | No input says "this quarterback is the only star on the slate" |
| Lemon 25 vs 43, McKee 30 vs 48, Commanders DST 11 vs 25, White 31 vs 42; Cooper, Burks, Mitchell, Blue 8-9 points too high | Those players' projections carried the news (market anchor, absence commit) but the model was never fitted on such inputs; the leftover flex budget spread onto depth players | Train/serve mismatch (section 2), plus no stand-in-QB input for the opposing defense |
| Top RB/WR pinned at the 95 total cap in 7 of 18 slates against actual totals of 76-84 (flex about 13 too high) | The flex ridge, fitted on sqrt(ownership), extrapolates past the top of the field: uncapped scores 69-90 for Bijan, Taylor, Gibbs, Smith-Njigba against actual flex 46-60 | Unbounded top end; the cap is the only thing holding it |

A fact worth carrying: across the 18 slates the chalkiest player's total
ownership (captain + flex) was 67-93, mean about 80. The 95 cap was never
the field's number; it is the model's.

## 2. Fix 0, found while sizing the others: fit on the inputs the model is served

`research/nfl_showdown_ownership_fit.py` builds its training frame from
`nfl_dfs_slate_players.our_proj`, the projection stored when the salary file
was uploaded: before the market anchor, before the absence commit. The live
build applies the fitted model to the workspace projection, after both. On
PHI @ WAS the two differed for 24 of 32 active players: McKee 7.8 stored vs
12.5 live, Lemon 7.7 vs 11.5, Barkley 10.2 vs 12.7.

The pregame pool capture (`nfl_dfs_pool_captures`, one minute before
kickoff) records the projection the build saw. 15 of the 18 slates have one.
Refitting on those projections, leave-one-slate-out, and comparing the same
week 8 players:

| | flex MAE | flex Spearman | captain MAE | McKee | Lemon | White |
|---|---:|---:|---:|---:|---:|---:|
| fitted on stored upload projections (v1) | 5.31 | 0.779 | 1.06 | 8 | 19 | 23 |
| fitted on pool-capture projections | 4.66 | 0.812 | 1.03 | 37 | 27 | 33 |
| actual flex | | | | 48 | 43 | 42 |

(The two rows are 18 and 15 slates, so the pooled MAEs are not a paired
comparison; the three player columns are the same slate.) The market anchor
already carries most of the news the field reads. The model was simply
never shown it. This is the largest single improvement available and costs
one loader change: `load_slate` reads the projection from the latest
pregame pool capture when one exists, else the stored upload row, and the
training frame records which. It also makes every other fix measurable on
the right inputs; the tables below report both input sets for that reason.

## 3. The fixes, sized

Variants on the current architecture (two ridges, captain allocated first),
leave-one-slate-out. "top-2 flex MAE" is the error on each slate's two
most-owned players; "chalk captain MAE" the captain error on the most-owned.

**On pool-capture projections (15 slates, 573 rows):**

| variant | flex MAE | captain MAE | top-2 flex MAE | chalk captain MAE | slates beating current on flex | DST flex MAE | bias, flex 30%+ |
|---|---:|---:|---:|---:|---:|---:|---:|
| current (A0) | 4.66 | 1.03 | 8.53 | 5.20 | | 5.93 | −0.12 |
| A1 total cap 95 → 85 | 4.60 | 1.03 | 6.78 | 5.20 | 8 / 15 | 6.01 | −0.43 |
| A2 + role change, log((proj+1)/(DK avg+1)) | 4.62 | 1.01 | 8.59 | 5.27 | 9 | 5.82 | −0.19 |
| A3 + opponent's starting QB out (DST rows) | 4.61 | 1.02 | 8.64 | 5.14 | 11 | 5.06 | −0.18 |
| A4 + lead: points gap to the 2nd player, and gap × QB | 4.61 | 0.98 | 7.08 | 3.87 | 7 | 5.97 | −0.23 |
| A1 + A3 + A4 | 4.52 | 0.98 | 5.62 | 3.90 | 12 | 5.11 | −0.68 |
| all four | 4.49 | 0.96 | 5.60 | 4.03 | 11 | 5.05 | −0.76 |

**On stored upload projections (18 slates, 700 rows), for the record:**

| variant | flex MAE | captain MAE | top-2 flex MAE | chalk captain MAE | slates beating current | DST flex MAE |
|---|---:|---:|---:|---:|---:|---:|
| current | 5.31 | 1.06 | 10.49 | 4.40 | | 5.11 |
| A1 cap 85 | 5.21 | 1.06 | 7.70 | 4.40 | 12 / 18 | 5.17 |
| A2 role change | 5.25 | 1.06 | 9.88 | 4.42 | 15 | 5.16 |
| A3 opp QB out | 5.29 | 1.05 | 10.56 | 4.36 | 14 | 4.71 |
| A4 lead | 5.34 | 1.06 | 9.83 | 4.64 | 5 | 5.14 |
| all four | 5.17 | 1.06 | 6.68 | 4.65 | 16 | 4.78 |

PHI @ WAS under the combined variant on pool-capture inputs: Daniels 45 flex
/ 40 captain (actual 55 / 38), McKee 39 (48), White 36 (42), Lemon 30 (43),
Commanders 8 (25), Cooper 19 (12).

### Miss 3, the pinned non-QB chalk: A1 (cap 85)

Direct and cheap. Top-2 flex MAE drops by about a quarter on both input
sets, the captain side is untouched, and it is the only variant that helps
on a majority of slates in both sets. Its cost is a slightly more negative
bias on 30%+ flex players (the field's chalk is still a little under-
projected on average). It is a band-aid over the real defect, the
unbounded sqrt target: a bounded target (logit of flex/90) was tried and was
worse (flex MAE 6.21), so the honest v2 ships the cap and leaves the
top-end shape for a later registration.

### Miss 1, the lone-QB captain split: A4 (lead features)

Two features for both ridges: the chalkiest player's projected-points gap
to the second player (zero for everyone else) and that gap times "is a
quarterback". On pool-capture inputs it is the only variant that moves the
captain side: captain MAE 1.03 → 0.98, chalk captain MAE 5.20 → 3.87,
Daniels 55 / 40 against 55 / 38. On stored inputs it was slightly worse
(4.40 → 4.64), and Daniels is the slate that motivated it, so this is the
least settled of the four. It should go in only under the forward test.

### Miss 2, news and matchup chalk: Fix 0, then A3, then A2

Fix 0 does most of the work (McKee 8 → 37, Lemon 19 → 27, White 23 → 33).
`nfl-ownership-news-v1` (closed 2026-10-24) found that raising a whole room
when a starter is out does not work; this is different: the market anchor
names the specific beneficiary, and the model learns how much the field
follows. The remaining gap (Lemon 27 vs 43, McKee 37 vs 48) is the field
reading published projections we do not have, and no frozen input closes it.

A3 is one flag: the DST row whose opponent's highest-DK-average quarterback
is OUT (derivable from the slate rows alone). Four training rows exist
(Eagles wk3 22.4%, Colts wk4 11.3%, Cowboys wk5 17.6%, Cowboys wk7 14.6%,
against a typical DST flex near 8%). It lowers DST flex MAE on both input
sets and improves 11-14 slates, but ridge shrinkage on four rows leaves the
Commanders at 7-8 against 24.5. It earns its place as a feature that gets
better as rows accrue, not as a fix for this week.

A2, the role-change ratio (our projection against DraftKings' season
average), is small and positive on stored inputs (15 of 18 slates), neutral
on pool-capture inputs. Include it; expect little.

The over-projected depth players (Cooper, Burks) are mostly where the budget
goes when chalk is under-projected; on pool-capture inputs Blue falls 9 → 4,
Cooper stays 17-19. The `punt` feature favours sub-$3,000 players
regardless of role; a depth-chart input would be the next thing to try,
separately.

## 4. What v2 would be

`nfl-ownership-fit-showdown-v2`:

1. Training frame from the latest pregame pool capture's projections where
   one exists, else the stored upload row (Fix 0), with the source recorded
   per slate.
2. `TOTAL_MAX` 95 → 85 (A1). `FLEX_MAX` 90 and `CAPTAIN_MAX` 50 unchanged.
3. Four new features: `opp_qb_out` (A3), `role_change` (A2), `lead_gap`
   and `lead_gap_qb` (A4). Alpha 3.0, sqrt targets, captain-first
   allocation unchanged.
4. The web port, fixture and tests regenerated; v1 kept callable as the
   shadow, as v5 is today.

Not in v2: a bounded flex target (tried, worse), a joint total-then-split
allocation (tried, no better: top-2 flex MAE 10.49 → 10.17, captain worse),
any depth-chart or published-ownership input (none frozen).

## 5. The test (to be frozen before the first counted slate)

Population: every full-field Showdown (recorded mass ≥ 590 of 600) whose
first kickoff is after v2's `fitted_at`, graded on the pregame pool capture's
projections. Floors: at least 8 slates and the first look no earlier than
2026-12-14 (one look). v2 KEEPS if, pooled over those slates, (a) flex MAE
and captain MAE both beat v1's recomputed on the same rows, (b) top-2 flex
MAE beats v1's, and (c) the most-owned player's total is not over-projected
by more than 5 points on average. Otherwise v1 stays and the next variant
is a new registration. No constant in section 4 changes between the freeze
and the verdict; a refit before it restarts the count.

Expected size if it holds, from section 3: flex MAE about 4.5 against 4.7,
captain about 1.0, and the chalk player's flex within about 6 points instead
of 8-10. The field is a $1-contest sample for the SNF slates and a
flagship sample for most others; both are counted, the contest named per row.

## 6. Order of work

1. Fix 0 in the loader and runner (a morning): refit v1 on aligned inputs
   as `v1-aligned`, record the leave-one-slate-out numbers.
2. Section 4 features and cap, fixture, tests, port (a day).
3. Freeze, register section 5 in `docs/nfl-ownership-model.md`, ship v2
   live with v1 as the shadow (the Classic pattern).
4. Grade weekly from the Monday field import; verdict at the floor.

Scratch measurements: `own_fix_sizes3.py`, `own_trainserve.py`,
`own_news_avail.py`, `own_chalk_cpt.py` in this session's scratchpad; they
read the database only.
