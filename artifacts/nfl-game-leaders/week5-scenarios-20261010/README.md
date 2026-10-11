# Week 5 (2026) game-leader review: role scenarios vs the published batch

Built 2026-10-10 after comparing the published PHI@JAX forecast with the DraftKings board. Every forecast here is `nfl-game-leaders-v3`, `exploratory_not_calibrated`, `market_inputs_used: false`, run against the same frozen capture as the published batch (`final-repaired-capture-20261008.json.gz`, sha aeaffa5a…). Nothing in this directory is calibrated and nothing here should be read as a lean.

## What was wrong

The published batch had `role_scenarios: null` for every game. By the model's own stated limit, an OUT player's workload then stays in unresolved newcomer slots (shown as OTHER:<team>), so the model cannot name a leader on the side with absences. Share of leader probability left unassigned, per game:

| game | rush | rec yds | receptions | total | out players with usage |
|---|---:|---:|---:|---:|---|
| PHI_JAX | 21% | 17% | 17% | 30% | Saquon Barkley (RB: 9.0 car, 1.8 tgt /g); Marquise Brown (WR: 2.0 car, 0.0 tgt /g); DeVonta Smith (WR: 0.0 car, 9.0 tgt /g); Tank Bigsby (RB: 7.0 car, 1.0 tgt /g) |
| CHI_GB | 19% | 2% | 1% | 6% | Kyle Monangai (RB: 15.0 car, 1.2 tgt /g); Chris Brooks (RB: 3.8 car, 1.2 tgt /g); Jayden Reed (WR: 0.0 car, 4.0 tgt /g) |
| CIN_MIA | 6% | 4% | 2% | 4% | Andrei Iosivas (WR: 0.0 car, 2.0 tgt /g); De'Von Achane (RB: 11.3 car, 4.0 tgt /g); Caleb Douglas (WR: 0.0 car, 5.0 tgt /g) |
| CLE_NYJ | 14% | 7% | 4% | 12% | Breece Hall (RB: 17.0 car, 3.3 tgt /g); Adonai Mitchell (WR: 0.0 car, 7.5 tgt /g); Arian Smith (WR: 0.0 car, 1.0 tgt /g); Omar Cooper Jr. (WR: 0.0 car, 1.0 tgt /g) |
| HOU_TEN | 0% | 1% | 0% | 0% |  |
| IND_PIT | 0% | 5% | 2% | 1% | Ashton Dulin (WR: 0.0 car, 3.5 tgt /g); Alec Pierce (WR: 0.0 car, 3.5 tgt /g); Michael Pittman (WR: 0.0 car, 4.0 tgt /g) |
| LV_NE | 1% | 10% | 5% | 4% | Jalen Nailor (WR: 0.0 car, 3.7 tgt /g); Jack Bech (WR: 0.0 car, 3.0 tgt /g); Mack Hollins (WR: 0.0 car, 5.0 tgt /g); A.J. Brown (WR: 0.0 car, 4.0 tgt /g); Reggie Gilliam (RB: 0.0 car, 2.0 tgt /g) |
| MIN_NO | 5% | 2% | 1% | 2% | Josh Oliver (TE: 0.0 car, 2.5 tgt /g); Jordan Mason (RB: 15.0 car, 0.0 tgt /g); Travis Etienne (RB: 10.0 car, 4.3 tgt /g) |
| NYG_WAS | 3% | 9% | 7% | 6% | Jaxson Dart (QB: 11.0 car, 0.0 tgt /g); Braxton Berrios (WR: 0.0 car, 1.0 tgt /g); Marcus Mariota (QB: 2.3 car, 0.0 tgt /g); Jaylin Lane (WR: 0.0 car, 1.0 tgt /g); Stefon Diggs (WR: 0.0 car, 7.8 tgt /g) |
| DEN_LAC | 1% | 13% | 10% | 7% | Pat Bryant (WR: 0.2 car, 3.0 tgt /g); Jonah Coleman (RB: 10.0 car, 3.0 tgt /g); David Njoku (TE: 0.0 car, 4.0 tgt /g); Quentin Johnston (WR: 0.5 car, 4.8 tgt /g); Brenen Thompson (WR: 0.0 car, 3.0 tgt /g) |
| DET_ARI | 0% | 0% | 0% | 0% | Hunter Long (TE: 0.0 car, 1.0 tgt /g) |
| SF_SEA | 4% | 1% | 1% | 1% | Kyle Juszczyk (RB: 0.0 car, 2.0 tgt /g); Demarcus Robinson (WR: 0.0 car, 3.0 tgt /g); Jadarian Price (RB: 9.3 car, 2.3 tgt /g) |
| BAL_ATL | 0% | 1% | 0% | 0% | Lamar Jackson (QB: 4.8 car, 0.0 tgt /g); Durham Smythe (TE: 0.0 car, 1.0 tgt /g); Ja'Kobi Lane (WR: 0.0 car, 3.0 tgt /g) |
| BUF_LA | 0% | 2% | 1% | 1% | Ronnie Rivers (RB: 5.0 car, 1.0 tgt /g); Terrance Ferguson (TE: 0.0 car, 4.0 tgt /g) |

## PHI@JAX, hand-built scenario vs DraftKings (de-vigged)

`request-PHI_JAX-scenario.json` / `forecast-PHI_JAX-scenario.json`. Carries: vacated Barkley/Bigsby/Brown share split 55/35/10 Shipley/Pierce/Blue (FantasyPros RB1-3). Targets: per-game rates in games played, Goedert restored as healthy TE1.


**rushing_yards**

| player | DK | DK fair | published | scenario |
|---|---:|---:|---:|---:|
| Bhayshul Tuten | -140 | 52.0% | 60.9% | 53.9% |
| Will Shipley | +450 | 16.2% | 1.8% | 26.3% |
| Dameon Pierce | +600 | 12.7% | 0.0% | 5.9% |
| Chris Rodriguez Jr. | +950 | 8.5% | 8.3% | 7.4% |
| Jalen Hurts | +1000 | 8.1% | 5.3% | 3.7% |
| Trevor Lawrence | +3500 | 2.5% | 1.3% | 0.8% |
| OTHER:PHI (unassigned) | | | 21.4% | 0.5% |

**receiving_yards**

| player | DK | DK fair | published | scenario |
|---|---:|---:|---:|---:|
| Parker Washington | +175 | 31.3% | 21.7% | 19.8% |
| Jakobi Meyers | +500 | 14.3% | 8.4% | 8.4% |
| Dontayvion Wicks | +500 | 14.3% | 17.1% | 17.0% |
| Dallas Goedert | +650 | 11.5% | 1.2% | 8.5% |
| Brenton Strange | +1000 | 7.8% | 11.5% | 11.3% |
| Makai Lemon | +1000 | 7.8% | 8.2% | 8.1% |
| Brian Thomas Jr. | +1300 | 6.1% | 5.2% | 4.5% |
| Darius Cooper | +1600 | 5.1% | 5.1% | 15.2% |
| Josh Cameron | +5000 | 1.7% | 1.1% | 1.2% |
| OTHER:PHI (unassigned) | | | 17.2% | 0.5% |

**receptions**

| player | DK | DK fair | published | scenario |
|---|---:|---:|---:|---:|
| Parker Washington | +240 | 26.3% | 19.6% | 17.3% |
| Dallas Goedert | +316 | 21.5% | 0.7% | 8.8% |
| Jakobi Meyers | +594 | 12.9% | 9.2% | 9.5% |
| Makai Lemon | +610 | 12.6% | 7.9% | 7.0% |
| Dontayvion Wicks | +630 | 12.3% | 12.8% | 13.2% |
| Brenton Strange | +1040 | 7.9% | 20.5% | 18.0% |
| Darius Cooper | +2500 | 3.4% | 3.1% | 12.9% |
| Brian Thomas Jr. | +2800 | 3.1% | 2.7% | 2.6% |
| OTHER:PHI (unassigned) | | | 16.9% | 0.1% |

Remaining gaps to DK after the scenario are assumptions, not findings: Cooper's 5.0 targets a game comes from two games played; Pierce has no PHI history so his yards come from peer priors; Strange vs Washington at receptions is a near tie (tie probability 22%).

## Rule-based scenarios, all 14 games

`build_scenarios.py` writes `request-<game>-rule.json` for every game with one rule set (docstring in the script); `compare.py` prints the table. Residual share after the rule is under 1.2% in every market of every game. Leader changes and the largest moves:

| game | market | residual | leader (published → rule) | largest moves |
|---|---|---|---|---|
| BAL_ATL | rushing_yards | 0% → 0.1% | Bijan Robinson 60% → Bijan Robinson 53% | Bijan Robinson -7%, Derrick Henry +7% |
| BAL_ATL | receiving_yards | 1% → 0.5% | Zay Flowers 34% → Zay Flowers 38% | Drake London -6%, Zay Flowers +4% |
| BAL_ATL | receptions | 0% → 0.2% | Mark Andrews 27% → Zay Flowers 33% | Zay Flowers +7%, Mark Andrews -6% |
| BAL_ATL | total_yards | 0% → 0.1% | Bijan Robinson 51% → Bijan Robinson 47% | Bijan Robinson -5%, Derrick Henry +3% |
| BUF_LA | rushing_yards | 0% → 0.1% | James Cook 53% → James Cook 50% | James Cook -3%, Blake Corum +3% |
| BUF_LA | receiving_yards | 2% → 0.4% | Davante Adams 31% → Puka Nacua 39% | Puka Nacua +25%, Davante Adams -9% |
| BUF_LA | receptions | 1% → 0.2% | Kyren Williams 24% → Puka Nacua 36% | Puka Nacua +27%, Kyren Williams -12% |
| BUF_LA | total_yards | 1% → 0.1% | Kyren Williams 40% → Kyren Williams 30% | Puka Nacua +16%, Kyren Williams -10% |
| CHI_GB | rushing_yards | 19% → 0.5% | D'Andre Swift 64% → D'Andre Swift 69% | D'Andre Swift +5%, Roschon Johnson +4% |
| CHI_GB | receiving_yards | 2% → 0.6% | Luther Burden III 24% → Luther Burden III 22% | Christian Watson +3%, Luther Burden III -2% |
| CHI_GB | receptions | 1% → 0.3% | Luther Burden III 28% → Luther Burden III 26% | Kalif Raymond +4%, Colston Loveland -4% |
| CHI_GB | total_yards | 6% → 0.3% | D'Andre Swift 29% → D'Andre Swift 37% | D'Andre Swift +8%, Luther Burden III -4% |
| CIN_MIA | rushing_yards | 6% → 0.4% | Chase Brown 64% → Chase Brown 53% | Ollie Gordon II +19%, Chase Brown -11% |
| CIN_MIA | receiving_yards | 4% → 0.8% | Tee Higgins 39% → Tee Higgins 27% | Tee Higgins -12%, Mitchell Tinsley +8% |
| CIN_MIA | receptions | 2% → 0.3% | Tee Higgins 31% → Malik Washington 23% | Tee Higgins -10%, Chase Brown -6% |
| CIN_MIA | total_yards | 4% → 0.3% | Chase Brown 30% → Chase Brown 30% | Tee Higgins -10%, Ollie Gordon II +10% |
| CLE_NYJ | rushing_yards | 14% → 0.6% | Quinshon Judkins 48% → Braelon Allen 44% | Braelon Allen +25%, Quinshon Judkins -12% |
| CLE_NYJ | receiving_yards | 7% → 0.9% | Garrett Wilson 32% → Garrett Wilson 37% | Garrett Wilson +4%, KC Concepcion -3% |
| CLE_NYJ | receptions | 4% → 0.2% | Garrett Wilson 32% → Garrett Wilson 37% | Garrett Wilson +5%, KC Concepcion -4% |
| CLE_NYJ | total_yards | 12% → 0.3% | Quinshon Judkins 28% → Quinshon Judkins 23% | Braelon Allen +13%, Quinshon Judkins -5% |
| DEN_LAC | rushing_yards | 1% → 0.5% | J.K. Dobbins 41% → Omarion Hampton 42% | J.K. Dobbins -7%, RJ Harvey +7% |
| DEN_LAC | receiving_yards | 13% → 1.1% | Jaylen Waddle 22% → Jaylen Waddle 19% | Alec Ingold +4%, Charlie Kolar +4% |
| DEN_LAC | receptions | 10% → 0.3% | RJ Harvey 25% → RJ Harvey 27% | Alec Ingold +10%, Charlie Kolar +4% |
| DEN_LAC | total_yards | 7% → 0.4% | Omarion Hampton 19% → Omarion Hampton 20% | RJ Harvey +11%, J.K. Dobbins -5% |
| DET_ARI | rushing_yards | 0% → 0.1% | Jahmyr Gibbs 67% → Jahmyr Gibbs 70% | Jeremiyah Love -4%, Jahmyr Gibbs +3% |
| DET_ARI | receiving_yards | 0% → 0.2% | Michael Wilson 29% → Amon-Ra St. Brown 24% | Michael Wilson -6%, Kendrick Bourne +3% |
| DET_ARI | receptions | 0% → 0.0% | Trey McBride 38% → Trey McBride 38% | Michael Wilson -5%, Amon-Ra St. Brown +3% |
| DET_ARI | total_yards | 0% → 0.1% | Jahmyr Gibbs 47% → Jahmyr Gibbs 49% | Michael Wilson -4%, Jahmyr Gibbs +2% |
| HOU_TEN | rushing_yards | 0% → 0.7% | Tony Pollard 49% → Tony Pollard 44% | Tony Pollard -5%, David Montgomery +2% |
| HOU_TEN | receiving_yards | 1% → 0.6% | Carnell Tate 38% → Carnell Tate 32% | Nico Collins +16%, Carnell Tate -6% |
| HOU_TEN | receptions | 0% → 0.3% | Carnell Tate 38% → Carnell Tate 32% | Nico Collins +14%, Carnell Tate -5% |
| HOU_TEN | total_yards | 0% → 0.3% | Carnell Tate 27% → Carnell Tate 23% | Nico Collins +14%, Carnell Tate -5% |
| IND_PIT | rushing_yards | 0% → 0.2% | Jonathan Taylor 57% → Jonathan Taylor 71% | Jaylen Warren -17%, Jonathan Taylor +14% |
| IND_PIT | receiving_yards | 5% → 0.8% | DK Metcalf 24% → DK Metcalf 23% | Keenan Allen +5%, Germie Bernard +2% |
| IND_PIT | receptions | 2% → 0.2% | Tyler Warren 21% → Tyler Warren 19% | Keenan Allen +7%, Rico Dowdle +4% |
| IND_PIT | total_yards | 1% → 0.1% | Jonathan Taylor 38% → Jonathan Taylor 44% | Jaylen Warren -15%, Jonathan Taylor +5% |
| LV_NE | rushing_yards | 1% → 0.5% | Ashton Jeanty 42% → Ashton Jeanty 42% | TreVeyon Henderson +6%, Rhamondre Stevenson -4% |
| LV_NE | receiving_yards | 10% → 0.5% | Brock Bowers 18% → Brock Bowers 40% | Brock Bowers +22%, Tre Tucker -5% |
| LV_NE | receptions | 5% → 0.1% | Brock Bowers 23% → Brock Bowers 53% | Brock Bowers +29%, Michael Mayer -9% |
| LV_NE | total_yards | 4% → 0.2% | Ashton Jeanty 35% → Ashton Jeanty 28% | Brock Bowers +16%, Ashton Jeanty -7% |
| MIN_NO | rushing_yards | 5% → 0.4% | Aaron Jones 84% → Aaron Jones 73% | Aaron Jones -10%, Alvin Kamara +10% |
| MIN_NO | receiving_yards | 2% → 0.6% | Chris Olave 41% → Chris Olave 37% | Justin Jefferson +9%, T.J. Hockenson -4% |
| MIN_NO | receptions | 1% → 0.2% | Chris Olave 32% → Chris Olave 30% | Justin Jefferson +7%, T.J. Hockenson -6% |
| MIN_NO | total_yards | 2% → 0.4% | Aaron Jones 41% → Aaron Jones 34% | Aaron Jones -7%, Justin Jefferson +6% |
| NYG_WAS | rushing_yards | 3% → 0.8% | Cam Skattebo 48% → Cam Skattebo 43% | Jacory Croskey-Merritt -12%, Jayden Daniels +8% |
| NYG_WAS | receiving_yards | 9% → 0.8% | Isaiah Likely 34% → Isaiah Likely 27% | Terry McLaurin +13%, Isaiah Likely -8% |
| NYG_WAS | receptions | 7% → 0.2% | Isaiah Likely 54% → Isaiah Likely 41% | Isaiah Likely -12%, Terry McLaurin +10% |
| NYG_WAS | total_yards | 6% → 0.6% | Cam Skattebo 23% → Isaiah Likely 19% | Terry McLaurin +10%, Jacory Croskey-Merritt -6% |
| PHI_JAX | rushing_yards | 21% → 0.8% | Bhayshul Tuten 61% → Bhayshul Tuten 46% | Will Shipley +29%, Bhayshul Tuten -15% |
| PHI_JAX | receiving_yards | 17% → 0.7% | Parker Washington 22% → Parker Washington 19% | Darius Cooper +12%, Dallas Goedert +7% |
| PHI_JAX | receptions | 17% → 0.2% | Brenton Strange 21% → Parker Washington 17% | Darius Cooper +12%, Dallas Goedert +9% |
| PHI_JAX | total_yards | 30% → 0.7% | Bhayshul Tuten 21% → Bhayshul Tuten 16% | Will Shipley +11%, Darius Cooper +9% |
| SF_SEA | rushing_yards | 4% → 0.6% | Emanuel Wilson 45% → Emanuel Wilson 56% | Emanuel Wilson +11%, Christian McCaffrey -10% |
| SF_SEA | receiving_yards | 1% → 0.5% | Jaxon Smith-Njigba 58% → Jaxon Smith-Njigba 60% | Jaxon Smith-Njigba +2%, George Kittle -2% |
| SF_SEA | receptions | 1% → 0.1% | Jaxon Smith-Njigba 55% → Jaxon Smith-Njigba 58% | Jaxon Smith-Njigba +3%, George Kittle -2% |
| SF_SEA | total_yards | 1% → 0.3% | Jaxon Smith-Njigba 45% → Jaxon Smith-Njigba 47% | Christian McCaffrey -4%, Emanuel Wilson +3% |

## What the rule gets right and where it needs a reviewer

- It removes the unassigned mass everywhere, and where a lead back is out it hands the carries to the depth chart (NYJ Braelon Allen for Hall, CHI Swift for Monangai, MIA Ollie Gordon for Achane, SEA Wilson for Price, DEN Harvey/Dobbins for Coleman). Those are depth-chart claims, not fitted roles.
- "Missed weeks are absences" is why Brock Bowers (two games, 12 targets a game), Nico Collins, Puka Nacua and Goedert jump: the published model's recency weighting reads their missed weeks as low usage. This is the one change that looks like a model defect rather than a scenario choice, and it should be fixed in the model, not patched per week.
- The same rule inflates small samples: Darius Cooper (2 games), Rico Dowdle (2 games), Mitchell Tinsley, Alec Ingold. A per-game rate from two games is treated like a full-time role. A reviewer should cut those back or the rule should shrink rates by games played.
- Departed players still in the recent box scores (Odell Beckham Jr. on NYG, Kaytron Allen and Brandon Coleman on WSH, Xavier Smith on LAR, Bradley Pinion, Jake Brendel) must be named in the override at zero share; the model refuses the request otherwise. The builder does this and lists them in `scenario-notes.json`.
- A QB who is out (Lamar Jackson, Jaxson Dart, Mariota) has his carries vacated but not reassigned; the backup keeps his own per-game rate. Not handled.

## Status

Built and run; not validated against results. The week-5 grade (`research.nfl_game_leaders grade`) can score the published, hand-built and rule-based forecasts side by side once the games are final, which is the only honest test of whether the scenarios help.
