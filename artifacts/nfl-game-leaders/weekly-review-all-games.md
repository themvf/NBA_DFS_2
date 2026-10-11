# 2026 weekly game-leader retrospective review

All 64 games in Weeks 1–4 are covered. First choices are graded against complete nflverse player stats whose carries, targets, receptions, rushing yards and receiving yards sum to the separately published team feed. Original forecasts were retained; eight previously excluded target games were reconstructed without retuning the model. Their PBP discrepancies remain excluded from training.

Cells show **model / recent-average baseline** first-choice rates. Tied winners divide credit equally. These are reconstructed historical tests with corrected records and prior-usage rosters, not evidence of archived pregame availability or calibrated betting odds.

| Week | Games | Rushing yards | Receptions | Receiving yards |
|---|---:|---:|---:|---:|
| 1 | 16 | 37.5% / 43.8% | 21.9% / 21.9% | 31.2% / 31.2% |
| 2 | 16 | 25.0% / 43.8% | 30.2% / 30.2% | 25.0% / 25.0% |
| 3 | 16 | 56.2% / 56.2% | 6.2% / 15.6% | 12.5% / 12.5% |
| 4 | 16 | 56.2% / 56.2% | 12.5% / 21.9% | 31.2% / 31.2% |
| Overall | 64 | 43.8% / 50.0% | 17.7% / 22.4% | 25.0% / 25.0% |

The model trails the baseline for rushing and receptions, and ties it for receiving yards. Receptions remain the largest weakness; neither model demonstrates reliable game-leader accuracy.

## Game-by-game results

Each cell shows model pick → actual leader (winning total). A check marks a correct or tied-first selection; ties remain fractional in the weekly rates.

### Week 1

| Game | Rushing yards | Receptions | Receiving yards |
|---|---|---|---|
| NE at SEA | ✗ TreVeyon Henderson → Jadarian Price (52) | ✓ Jaxon Smith-Njigba → Jaxon Smith-Njigba (8) | ✓ Jaxon Smith-Njigba → Jaxon Smith-Njigba (122) |
| SF at LAR | ✗ Kyren Williams → Christian McCaffrey (68) | ✗ Puka Nacua → Mike Evans, Deebo Samuel Sr. (6) | ✓ Puka Nacua → Puka Nacua (74) |
| ATL at PIT | ✓ Bijan Robinson → Bijan Robinson (83) | ✗ Kyle Pitts → Bijan Robinson (8) | ✗ Kyle Pitts → Bijan Robinson (90) |
| BAL at IND | ✗ Jonathan Taylor → Derrick Henry (144) | ✗ Zay Flowers → Keenan Allen (6) | ✓ Zay Flowers → Zay Flowers (150) |
| BUF at HOU | ✗ James Cook → David Montgomery (60) | ✗ Dalton Schultz → Nico Collins (7) | ✗ Nico Collins → Dalton Kincaid (130) |
| CHI at CAR | ✓ D'Andre Swift → D'Andre Swift (124) | ✗ Colston Loveland → Kalif Raymond, Jalen Coker (8) | ✗ Tetairoa McMillan → Jalen Coker (138) |
| CLE at JAX | ✗ Travis Etienne → Bhayshul Tuten (66) | ✗ Jakobi Meyers → Parker Washington (5) | ✓ Parker Washington → Parker Washington (83) |
| NO at DET | ✓ Jahmyr Gibbs → Jahmyr Gibbs (156) | ✓ Amon-Ra St. Brown → Amon-Ra St. Brown, Chris Olave (10) | ✗ Amon-Ra St. Brown → Chris Olave (182) |
| NYJ at TEN | ✗ Tony Pollard → Breece Hall (102) | ✗ Chig Okonkwo → Garrett Wilson (6) | ✗ Adonai Mitchell → Garrett Wilson (79) |
| TB at CIN | ✓ Chase Brown → Chase Brown (56) | ✗ Ja'Marr Chase → Bucky Irving (7) | ✗ Ja'Marr Chase → Mike Gesicki (78) |
| ARI at LAC | ✗ Omarion Hampton → Tyler Allgeier (61) | ✓ Trey McBride → Trey McBride (9) | ✓ Trey McBride → Trey McBride (95) |
| GB at MIN | ✗ Aaron Jones → Jordan Mason (59) | ✓ Justin Jefferson → Justin Jefferson (8) | ✗ Justin Jefferson → Christian Watson (147) |
| MIA at LV | ✓ Ashton Jeanty → Ashton Jeanty (102) | ✗ Tre Tucker → Michael Mayer, Ashton Jeanty (6) | ✗ Tre Tucker → Caleb Douglas (94) |
| WSH at PHI | ✓ Saquon Barkley → Saquon Barkley (83) | ✗ A.J. Brown → Stefon Diggs, Dallas Goedert, Antonio Williams (4) | ✗ A.J. Brown → Dallas Goedert (77) |
| DAL at NYG | ✗ Tyrone Tracy Jr. → Cam Skattebo (81) | ✗ Wan'Dale Robinson → Isaiah Likely (8) | ✗ Wan'Dale Robinson → Isaiah Likely (78) |
| DEN at KC | ✗ RJ Harvey → Kenneth Walker III (173) | ✗ Travis Kelce → Evan Engram, Pat Bryant, RJ Harvey (4) | ✗ Courtland Sutton → Travis Kelce (71) |

### Week 2

| Game | Rushing yards | Receptions | Receiving yards |
|---|---|---|---|
| DET at BUF | ✗ Jahmyr Gibbs → James Cook (135) | ✓ Amon-Ra St. Brown → Amon-Ra St. Brown (9) | ✓ Amon-Ra St. Brown → Amon-Ra St. Brown (142) |
| CAR at ATL | ✓ Bijan Robinson → Bijan Robinson (72) | ✗ Bijan Robinson → Jalen Coker (8) | ✗ Bijan Robinson → Tetairoa McMillan (101) |
| CIN at HOU | ✓ Chase Brown → Chase Brown (80) | ✗ Mike Gesicki → Dalton Schultz (12) | ✗ Nico Collins → Dalton Schultz (140) |
| CLE at TB | ✗ Quinshon Judkins → Bucky Irving (89) | ✗ Bucky Irving → KC Concepcion (6) | ✗ Emeka Egbuka → Denzel Boston (95) |
| GB at NYJ | ✗ Breece Hall → Kaleb Johnson (32) | ✗ Matthew Golden → Isaiah Williams, Adonai Mitchell (6) | ✗ Matthew Golden → Breece Hall, Adonai Mitchell (63) |
| MIN at CHI | ✗ D'Andre Swift → Aaron Jones (105) | ✓ Kalif Raymond → Kalif Raymond, D'Andre Swift (5) | ✓ Justin Jefferson → Justin Jefferson (55) |
| NO at BAL | ✓ Derrick Henry → Derrick Henry (68) | ✓ Chris Olave → Chris Olave (8) | ✗ Chris Olave → Rashod Bateman (88) |
| PHI at TEN | ✗ Saquon Barkley → Tony Pollard (64) | ✓ DeVonta Smith → DeVonta Smith (10) | ✓ DeVonta Smith → DeVonta Smith (117) |
| PIT at NE | ✗ Rhamondre Stevenson → TreVeyon Henderson (76) | ✗ DK Metcalf → Roman Wilson (5) | ✗ DK Metcalf → Romeo Doubs (96) |
| JAX at DEN | ✗ J.K. Dobbins → Bhayshul Tuten (65) | ✗ Parker Washington → Jaylen Waddle (8) | ✗ Parker Washington → Jaylen Waddle (138) |
| LV at LAC | ✗ Ashton Jeanty → Omarion Hampton (94) | ✗ Michael Mayer → Tre Tucker (5) | ✗ Ladd McConkey → Tre Tucker (119) |
| MIA at SF | ✓ De'Von Achane → De'Von Achane (74) | ✓ Christian McCaffrey → Christian McCaffrey, George Kittle, Malik Washington (4) | ✗ Caleb Douglas → George Kittle (80) |
| SEA at ARI | ✗ Tyler Allgeier → Emanuel Wilson (92) | ✓ Jaxon Smith-Njigba → Jaxon Smith-Njigba (9) | ✓ Jaxon Smith-Njigba → Jaxon Smith-Njigba (155) |
| WSH at DAL | ✗ Javonte Williams → Jayden Daniels (69) | ✗ Stefon Diggs → CeeDee Lamb (8) | ✗ Stefon Diggs → CeeDee Lamb (153) |
| IND at KC | ✗ Jonathan Taylor → Kenneth Walker III (117) | ✗ Kenneth Walker III → Travis Kelce (9) | ✗ Alec Pierce → Travis Kelce (101) |
| NYG at LAR | ✗ Cam Skattebo → Kyren Williams (85) | ✗ Isaiah Likely → Davante Adams (8) | ✗ Puka Nacua → Davante Adams (195) |

### Week 3

| Game | Rushing yards | Receptions | Receiving yards |
|---|---|---|---|
| ATL at GB | ✓ Bijan Robinson → Bijan Robinson (194) | ✗ Bijan Robinson → Drake London (9) | ✗ Christian Watson → Drake London (194) |
| CAR at CLE | ✓ Chuba Hubbard → Chuba Hubbard (82) | ✗ Jalen Coker → Harold Fannin Jr. (7) | ✗ Jalen Coker → Brycen Tremayne (83) |
| CIN at PIT | ✗ Chase Brown → Jaylen Warren (127) | ✗ DK Metcalf → Ja'Marr Chase (9) | ✗ DK Metcalf → Ja'Marr Chase (98) |
| HOU at IND | ✓ Jonathan Taylor → Jonathan Taylor (68) | ✗ Dalton Schultz → Tyler Warren (9) | ✗ Dalton Schultz → Josh Downs (77) |
| KC at MIA | ✓ Kenneth Walker III → Kenneth Walker III (70) | ✗ Kenneth Walker III → Rashee Rice (7) | ✗ Travis Kelce → Rashee Rice (88) |
| LAC at BUF | ✓ James Cook → James Cook (154) | ✗ Dalton Kincaid → DJ Moore, Tre Harris (6) | ✗ Dalton Kincaid → Tre Harris (76) |
| NE at JAX | ✓ Bhayshul Tuten → Bhayshul Tuten (73) | ✗ Parker Washington → Jakobi Meyers (7) | ✗ Parker Washington → Mack Hollins (87) |
| NYJ at DET | ✓ Jahmyr Gibbs → Jahmyr Gibbs (99) | ✗ Amon-Ra St. Brown → Garrett Wilson (10) | ✗ Amon-Ra St. Brown → Garrett Wilson (107) |
| SEA at WSH | ✗ Emanuel Wilson → Jacory Croskey-Merritt (36) | ✓ Jaxon Smith-Njigba → Jaxon Smith-Njigba (10) | ✓ Jaxon Smith-Njigba → Jaxon Smith-Njigba (128) |
| TEN at NYG | ✗ Cam Skattebo → Tony Pollard (74) | ✗ Isaiah Likely → Wan'Dale Robinson (7) | ✗ Isaiah Likely → Carnell Tate (58) |
| ARI at SF | ✗ Kaelon Black → Jeremiyah Love (90) | ✗ Trey McBride → Michael Wilson (11) | ✗ Trey McBride → Michael Wilson (89) |
| MIN at TB | ✓ Aaron Jones → Aaron Jones (58) | ✗ Justin Jefferson → Aaron Jones, Jordan Addison, Emeka Egbuka (5) | ✗ Justin Jefferson → Jordan Addison (90) |
| BAL at DAL | ✗ Derrick Henry → Javonte Williams (98) | ✗ Mark Andrews → CeeDee Lamb, George Pickens (7) | ✓ CeeDee Lamb → CeeDee Lamb (112) |
| LV at NO | ✗ Ashton Jeanty → Travis Etienne (57) | ✗ Chris Olave → Brock Bowers (10) | ✗ Chris Olave → Brock Bowers (116) |
| LAR at DEN | ✗ Blake Corum → Kyren Williams (88) | ✗ Jaylen Waddle → Tyler Higbee (8) | ✗ Jaylen Waddle → Davante Adams (137) |
| PHI at CHI | ✓ D'Andre Swift → D'Andre Swift (84) | ✗ DeVonta Smith → Luther Burden III (7) | ✗ DeVonta Smith → Kalif Raymond (90) |

### Week 4

| Game | Rushing yards | Receptions | Receiving yards |
|---|---|---|---|
| PIT at CLE | ✓ Jaylen Warren → Jaylen Warren (93) | ✗ Harold Fannin Jr. → Quinshon Judkins (6) | ✓ DK Metcalf → DK Metcalf (115) |
| IND at WSH | ✓ Jonathan Taylor → Jonathan Taylor (95) | ✓ Tyler Warren → Stefon Diggs, Laquon Treadwell, Tyler Warren (5) | ✗ Josh Downs → Dyami Brown (65) |
| ARI at NYG | ✗ Cam Skattebo → Jeremiyah Love (63) | ✓ Trey McBride → Trey McBride, Isaiah Likely, Michael Wilson (7) | ✗ Trey McBride → Malik Nabers (112) |
| DAL at HOU | ✓ Javonte Williams → Javonte Williams (62) | ✗ Dalton Schultz → CeeDee Lamb (17) | ✓ CeeDee Lamb → CeeDee Lamb (189) |
| GB at TB | ✓ Bucky Irving → Bucky Irving (61) | ✗ Christian Watson → Chris Godwin Jr., Tucker Kraft (6) | ✓ Christian Watson → Christian Watson (47) |
| JAX at CIN | ✗ Chase Brown → Bhayshul Tuten (73) | ✗ Parker Washington → Tee Higgins, Chase Brown (11) | ✗ Parker Washington → Tee Higgins (157) |
| LAR at PHI | ✓ Kyren Williams → Kyren Williams (80) | ✗ DeVonta Smith → Kyren Williams (10) | ✗ DeVonta Smith → Puka Nacua (125) |
| NE at BUF | ✓ James Cook → James Cook (75) | ✗ Dalton Kincaid → Khalil Shakir (7) | ✗ Dalton Kincaid → Keon Coleman (116) |
| NYJ at CHI | ✗ D'Andre Swift → Kyle Monangai (146) | ✗ Garrett Wilson → Rome Odunze, Colston Loveland (6) | ✗ Garrett Wilson → Rome Odunze (94) |
| TEN at BAL | ✓ Derrick Henry → Derrick Henry (73) | ✗ Mark Andrews → Carnell Tate (9) | ✗ Mark Andrews → Carnell Tate (145) |
| MIA at MIN | ✗ Aaron Jones → Ollie Gordon II (100) | ✗ Malik Washington → T.J. Hockenson (13) | ✗ Malik Washington → T.J. Hockenson (119) |
| DEN at SF | ✗ J.K. Dobbins → Christian McCaffrey (52) | ✗ Christian McCaffrey → RJ Harvey (10) | ✗ Jaylen Waddle → Mike Evans (76) |
| KC at LV | ✓ Kenneth Walker III → Kenneth Walker III (177) | ✗ Rashee Rice → Michael Mayer (8) | ✗ Rashee Rice → Tyquan Thornton (111) |
| LAC at SEA | ✗ Omarion Hampton → Emanuel Wilson (81) | ✓ Jaxon Smith-Njigba → Keaton Mitchell, Jaxon Smith-Njigba, Oronde Gadsden II (5) | ✓ Jaxon Smith-Njigba → Jaxon Smith-Njigba (76) |
| DET at CAR | ✗ Jahmyr Gibbs → Chuba Hubbard (122) | ✗ Amon-Ra St. Brown → Tetairoa McMillan (14) | ✗ Amon-Ra St. Brown → Tetairoa McMillan (192) |
| ATL at NO | ✓ Bijan Robinson → Bijan Robinson (145) | ✓ Chris Olave → Chris Olave (8) | ✓ Chris Olave → Chris Olave (116) |

## Audit files

- `weekly-review-all-games.json`: all forecasts, grades, weekly summary, exclusions and provenance.
- `weekly-review-team-source.json`: separately captured full team-stat source, URL, time and hash.
- `weekly-review-20261008.py`: reproducible review procedure (exclusive output creation).
