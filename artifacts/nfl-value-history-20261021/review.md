# NFL value review: Weeks 1–6, 2026

Reviewed every distinct completed Classic and Showdown slate saved in this project: **6 Classic + 17 Showdown = 23 slates**. Selected 22 pre-lock portfolios containing **789 lineups**. Week 5 TB/DAL has an immutable baseline but no saved portfolio. Repeated salary uploads were consolidated; 99 system-generated experiment runs were excluded from portfolio selection. This does not cover hypothetical DraftKings slates for which the project has no saved salary pool.

The strongest process reviews are **Likely’s Week 5 salary-rule exclusion; Tucker and Douglas’s repeated Classic misses; and the low-exposure Showdown cases of Brian Robinson, Roman Wilson, and Ryan Flournoy**. Burks/Noel are role-gate investigations, not proven cases for overriding a safety check.

All dollar screens use **base/FLEX salary strictly below $5,000**. Captain costs and points receive the 1.5 multiplier once; the detailed Showdown pages show Captain/FLEX exposure separately and identify which Captain purchases also cost below $5,000. A player who costs $4,400 at FLEX costs $6,600 at Captain and is not a sub-$5,000 Captain.

These are saved generated exposures, not verified submitted entries. Actual above mean is an observed outcome, not proof a projection or allocation was wrong. No reoptimized portfolios, calibrated exposure targets, payout/ROI claims, or predictive-validation claims are made.

## Priority findings

| Case | Salary | Frozen projection → actual | Saved exposure | Our own. | Field own. | Why review |
|---|---:|---:|---:|---:|---:|---|
| W5 Classic: Isaiah Likely | $4,300 | 8.87 → 32.40 | 0/40 (0.0%) | 3.5% | 11.9% | Excluded by TE salary ceiling despite 27 targets in three games |
| W2 Classic: Tre Tucker | $4,500 | 7.60 → 25.90 | 0/20 (0.0%) | not stored | 0.7% | Repeated low exposure |
| W6 Classic: Tre Tucker | $4,700 | 10.31 → 26.70 | 2/65 (3.1%) | 4.7% | 12.6% | Repeated low exposure; W6 frozen air-volume/YAC signals |
| W5 Classic: DeMario Douglas | $3,500 | 4.88 → 22.50 | 0/40 (0.0%) | 0.4% | 1.2% | Repeated omissions |
| W6 Classic: DeMario Douglas | $4,200 | 5.47 → 20.60 | 0/65 (0.0%) | 1.0% | 1.4% | Repeated omissions; W6 deep-target evidence |
| W4 Classic: Emanuel Wilson | $4,700 | 4.96 → 27.00 | 0/107 (0.0%) | 0.2% | 5.6% | Frozen inside-five opportunity signal |
| W4 ATL/NO Showdown: Brian Robinson Jr. | $3,800 | 5.92 → 25.70 | 2/40 (5.0%) | 0.4% / 10.0% | 1.2% / 23.1% | Also beat projection with zero exposure in W3 Showdown |
| W4 PIT/CLE Showdown: Roman Wilson | $4,000 | 6.31 → 16.40 | 1/20 (5.0%) | 3.6% / 23.9% | 2.5% / 20.1% | Eligible cheap receiving alternative |
| W6 DAL/GB Showdown: Ryan Flournoy | $4,400 | 8.11 → 17.00 | 1/20 (5.0%) | 0.7% / 8.3% | no contest imported | Frozen close-range target signal |
| W6 HOU/JAX Showdown: Jaylin Noel | $3,000 | 4.63 → 23.70 | 0/50 (0.0%) | 0.8% / 9.1% | no contest imported | Excluded by role gate; historical role still unresolved |

Ownership: "Our own." is the frozen pre-lock forecast stored with the selected build (Showdown captain / flex); "Field own." is the slate's largest imported contest. A player absent from a complete DraftKings export is 0.0% (none drafted); absent from a stat-api import is unresolved, not zero (docs/nfl-ownership-scorecard-v1.md section 3).

The frozen Week 5 TE rule differs from the Week 6 rule. The audit explains each saved decision using its own eligibility ledger. Earlier snapshots often lack later opportunity chips; missing chips do not demonstrate an absence of opportunity.

## Classic, every saved week

| Week / full review | Saved lineups | Largest observed misses (salary; forecast → actual; entries; our → field ownership) |
|---|---:|---|
| [Week 1](slates/week-01-classic.md) | 1 | Jalen Coker ($4,600; 10.4 → 36.8; 0/1; own not stored → 7.5%); Dalton Kincaid ($4,600; 5.3 → 21.0; 0/1; own not stored → 1.8%); Matthew Golden ($4,700; 1.8 → 15.5; 0/1; own not stored → 1.7%) |
| [Week 2](slates/week-02-classic.md) | 20 | Panthers ($2,700; 5.0 → 26.0; 0/20; own not stored → 4.7%); Tre Tucker ($4,500; 7.6 → 25.9; 0/20; own not stored → 0.7%); Rashod Bateman ($4,400; 5.3 → 21.8; 0/20; own not stored → 7.9%) |
| [Week 3](slates/week-03-classic.md) | 20 | Kenyon Sadiq ($3,500; 7.3 → 26.5; 0/20; own not stored → 6.0%); Geno Smith ($4,900; 11.9 → 30.0; 0/20; own not stored → 7.5%); Noah Fant ($3,200; 4.6 → 19.3; 0/20; own not stored → 0.1%) |
| [Week 4](slates/week-04-classic.md) | 107 | Tyquan Thornton ($3,400; 5.5 → 31.1; 0/107; own 0.6% → 0.7%); Emanuel Wilson ($4,700; 5.0 → 27.0; 0/107; own 0.2% → 5.6%); Kyle Monangai ($4,900; 9.7 → 31.0; 0/107; own 3.6% → 1.6%) |
| [Week 5](slates/week-05-classic.md) | 40 | Isaiah Likely ($4,300; 8.9 → 32.4; 0/40; own 3.5% → 11.9%); Jameis Winston ($4,800; 11.4 → 29.7; 0/40; own 1.2% → 3.9%); DeMario Douglas ($3,500; 4.9 → 22.5; 0/40; own 0.4% → 1.2%) |
| [Week 6](slates/week-06-classic.md) | 65 | Titans ($2,300; 2.6 → 21.0; 0/65; own 1.0% → 2.4%); Tommy Tremble ($2,700; 3.8 → 20.6; 0/65; own 0.2% → 0.1%); Tre Tucker ($4,700; 10.3 → 26.7; 2/65; own 4.7% → 12.6%) |

## Showdown, every saved slate

| Week / full review | Game | Saved lineups | Larger-hit outcomes, exposure, our → field ownership (CPT / FLEX) |
|---|---|---:|---|
| [Week 1](slates/week-01-showdown-NE-at-SEA.md) | NE@SEA | 18 | Brady Russell ($200; 0.1 → 4.0; 0/18; own not stored → 0.0% / 0.1%); Seahawks ($4,400; 9.2 → 13.0; 5/18; own not stored → 3.4% / 18.0%) |
| [Week 2](slates/week-02-showdown-IND-at-KC.md) | IND@KC | 20 | Tyquan Thornton ($3,800; 4.8 → 12.6; 4/20; own not stored → no contest imported) |
| [Week 2](slates/week-02-showdown-NYG-at-LAR.md) | NYG@LAR | 40 | Terrance Ferguson ($1,800; 5.3 → 17.4; 6/40; own not stored → 3.3% / 18.1%) |
| [Week 3](slates/week-03-showdown-ATL-at-GB.md) | ATL@GB | 20 | Brian Robinson Jr. ($4,400; 4.4 → 11.0; 0/20; own not stored → 0.3% / 7.1%) |
| [Week 3](slates/week-03-showdown-PHI-at-CHI.md) | PHI@CHI | 20 | Kalif Raymond ($3,800; 7.6 → 21.0; 11/20; own not stored → 1.2% / 21.7%); Bears ($4,000; 6.3 → 12.0; 1/20; own not stored → 0.9% / 10.3%) |
| [Week 4](slates/week-04-showdown-PIT-at-CLE.md) | PIT@CLE | 20 | Roman Wilson ($4,000; 6.3 → 16.4; 1/20; own 3.6% / 23.9% → 2.5% / 20.1%); Darnell Washington ($3,600; 5.0 → 11.7; 0/20; own 0.5% / 6.6% → 0.9% / 11.2%) |
| [Week 4](slates/week-04-showdown-IND-at-WSH.md) | IND@WSH | 20 | Treylon Burks ($2,000; 3.7 → 11.7; 0/20; own 0.5% / 4.6% → 0.5% / 14.3%) |
| [Week 4](slates/week-04-showdown-DET-at-CAR.md) | DET@CAR | 31 | Isaac TeSlaa ($4,200; 6.8 → 13.5; 11/31; own 0.1% / 4.3% → 0.5% / 9.8%) |
| [Week 4](slates/week-04-showdown-ATL-at-NO.md) | ATL@NO | 40 | Brian Robinson Jr. ($3,800; 5.9 → 25.7; 2/40; own 0.4% / 10.0% → 1.2% / 23.1%); Jahan Dotson ($3,000; 3.0 → 13.4; 0/40; own 0.0% / 1.4% → 0.7% / 15.6%) |
| [Week 5](slates/week-05-showdown-TB-at-DAL.md) | TB@DAL | None saved | Ryan Flournoy ($3,800; 8.2 → 12.0; exposure unavailable; own not stored → 2.6% / 26.3%); Tez Johnson ($2,200; 3.8 → 7.4; exposure unavailable; own not stored → 0.3% / 10.4%) |
| [Week 5](slates/week-05-showdown-PHI-at-JAX.md) | PHI@JAX | 50 | Darius Cooper ($4,200; 5.2 → 15.1; 9/50; own 0.9% / 10.8% → no contest imported) |
| [Week 5](slates/week-05-showdown-BAL-at-ATL.md) | BAL@ATL | 100 | Kyle Pitts Sr. ($4,800; 8.2 → 15.2; 35/100; own 0.3% / 8.7% → no contest imported) |
| [Week 5](slates/week-05-showdown-BUF-at-LAR.md) | BUF@LAR | 26 | Blake Corum ($3,800; 5.9 → 16.2; 5/26; own 0.2% / 4.8% → 0.8% / 18.4%); Tyler Higbee ($4,400; 7.5 → 13.3; 10/26; own 0.6% / 9.7% → 2.2% / 15.6%); Ty Johnson ($3,200; 5.4 → 11.2; 3/26; own 0.2% / 5.0% → 0.2% / 12.2%) |
| [Week 6](slates/week-06-showdown-SEA-at-DEN.md) | SEA@DEN | 35 | Rashid Shaheed ($3,600; 6.2 → 12.8; 3/35; own 0.7% / 11.2% → 2.2% / 19.1%) |
| [Week 6](slates/week-06-showdown-HOU-at-JAX.md) | HOU@JAX | 50 | Jaylin Noel ($3,000; 4.6 → 23.7; 0/50; own 0.8% / 9.1% → no contest imported); Foster Moreau ($1,600; 3.5 → 14.7; 2/50; own 0.2% / 2.6% → no contest imported); Jawhar Jordan ($2,400; 6.3 → 12.0; 14/50; own 0.2% / 2.9% → no contest imported) |
| [Week 6](slates/week-06-showdown-DAL-at-GB.md) | DAL@GB | 20 | Ryan Flournoy ($4,400; 8.1 → 17.0; 1/20; own 0.7% / 8.3% → no contest imported) |
| [Week 6](slates/week-06-showdown-WSH-at-SF.md) | WSH@SF | 26 | Chig Okonkwo ($4,600; 6.9 → 13.9; 13/26; own 0.4% / 8.2% → 1.3% / 19.5%); Treylon Burks ($3,200; 5.2 → 10.6; 8/26; own 0.4% / 7.7% → 0.4% / 9.4%) |

## Base rates: the denominator behind the highlights (added 2026-10-22)

The cases above are the tail. Across every supported, non-DST player below $5,000 with an observed result, the table shows how many cleared their projection by ten points, by the reason the saved build gave for each one. A rule that cost value would show a higher hit rate among the players it excluded.

| Group | Players | Mean actual − projection | Beat projection by 10+ |
|---|---:|---:|---:|
| All cheap supported players | 1388 | -0.46 | 4.7% |
| eligible | 586 | +0.25 | 7.2% |
| no saved ledger | 472 | -1.54 | 3.2% |
| WINNERS_SHAPE | 145 | +0.31 | 4.8% |
| ROLE_UNRESOLVED | 96 | -0.05 | 1.0% |
| ABSOLUTE_SALARY_BLOCK | 86 | -1.19 | 0.0% |
| KICKER_EXCLUDED | 2 | +4.98 | 0.0% |
| ROLE_UNKNOWN | 1 | -3.62 | 0.0% |

Low-owned hits are what matter in a tournament. Among players who beat projection by ten or more with an observed or reconciled field share:

- 60 such hits; 40 were under 5% field ownership.

## What we already captured

Examples include Week 3 Kalif Raymond (11/20), Week 4 Isaac TeSlaa (11/31), Week 5 Pitts (35/100), and Week 6 Okonkwo (13/26, at cap). In Week 6 Classic, Bernard appeared 21/65 and Likely/Loveland each 26/65. Outperforming projection does not by itself make these players underexposed.

## Evidence limits and verification

- All 23 slates passed canonical game completion and both-team skill-result coverage. All 789 selected lineups passed seat count, unique-player, salary-cap, salary-sum, projection-sum, and frozen-player/slot projection checks. Captain scoring and salaries were checked separately.
- There are 517 supported cheap-player/slate observations above projection; 162 meet the larger-hit reporting screen. Repeated players and overlapping Classic/Showdown games are correlated. Missing result rows remain unknown, so these counts are not a complete calibration scorecard or a hit rate.
- Two DST outcomes disagree by one point between imported contest evidence and the canonical v4 ledger: Week 1 Buccaneers (contest 9, ledger 8) and Week 5 Seahawks (contest 9, ledger 8). They are explicitly flagged; both values remain available.
- Historical dual-provider depth and official inactive evidence is incomplete. For 261 player/slate decision boundaries checked, 219 source injury observations matched the proper week and pre-build cutoff. This is not 219 independently validated roles or 219 unique players. No current capture is presented as historical pre-lock evidence.
- Week 1 Classic has only one lineup in the latest saved build, created roughly a week before lock. Week 5 TB/DAL has no saved build. Neither supports a confident claim about your actual allocation. Other selected portfolios vary from 18 to 107 lineups.
- Ownership (added 2026-10-22): every case carries our frozen forecast and the field share from the slate's largest imported contest with its label class. Slates without an imported contest (IND/KC, PHI/JAX, BAL/ATL, HOU/JAX, DAL/GB) show no field share; builds saved before ownership was stored show "not stored". `ownership.json` holds the per-player values.
- The JSON retains every listed cheap player, supported and unsupported forecasts, source IDs, original eligibility reasons, opportunity signals, actual components, and Captain/FLEX exposures. Original forecasts are preserved; no model or production change was made.

## Reproduce or inspect

[Complete player-level assessment](assessment.json) · [Slate and run inventory](inventory.json) · [Historical injury boundaries](injury-boundaries.json) · [Verification results](verification.json)

Offline verification passed: 1,343 non-DST component rescores, 641 Showdown Captain/base-row checks, all 219 retained injury observation boundaries, and exact reproduction of the 63 supported outperformers from the earlier Week 6 Sunday review. The two DST contest/ledger differences remain unresolved.

Frozen source evidence: `inputs.json.gz`. Scripts in the parent artifacts folder: `capture_nfl_value_history_20261021.py`, `capture_nfl_value_injuries_20261021.py`, `analyze_nfl_value_history_20261021.py`, and `render_nfl_value_history_20261021.py`. Captures use SELECT-only database transactions. The analysis and report generation run offline from retained evidence.
