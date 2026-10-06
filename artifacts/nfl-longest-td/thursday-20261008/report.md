# Thursday forward test: Tampa Bay at Dallas

Kickoff: Thursday, October 8, 2026, 8:15 p.m. ET.
Frozen decision: 2026-10-06T13:49:36.653563+00:00 (October 6, 9:49 a.m. ET).

**Status: exploratory pregame test. Thursday outcome is pending. No sportsbook odds used.**

## Results

| Player | Longest TD share | Any rushing/receiving TD | TD at least 40 yards | Share across chosen assumptions |
|---|---:|---:|---:|---:|
| CeeDee Lamb | 14.3% | 45.0% | 7.7% | 14.0%–15.5% |
| Bucky Irving | 10.3% | 48.6% | 3.8% | 6.6%–10.3% |
| George Pickens | 10.1% | 28.5% | 4.9% | 8.6%–10.1% |
| Javonte Williams | 9.7% | 55.6% | 4.3% | 9.1%–10.6% |
| Emeka Egbuka | 9.1% | 31.1% | 3.5% | 5.4%–10.3% |
| Chris Godwin Jr. | 7.4% | 23.1% | 3.6% | 5.9%–11.9% |
| Ryan Flournoy | 6.9% | 24.9% | 3.7% | 6.9%–8.4% |
| Ted Hurst III | 5.7% | 18.6% | 3.4% | 3.1%–7.0% |
| Cade Otton | 5.2% | 24.2% | 0.9% | 4.3%–6.6% |
| Kenny Gainwell | 3.4% | 21.9% | 1.5% | 2.7%–3.9% |
| Tez Johnson | 3.4% | 12.7% | 1.4% | 3.4%–3.9% |
| Jake Ferguson | 2.8% | 16.0% | 0.7% | 2.8%–3.6% |
| Tyler Goodson | 2.0% | 10.0% | 0.8% | 1.7%–2.0% |
| Dak Prescott | 1.9% | 14.9% | 0.3% | 1.0%–1.9% |
| KaVontae Turpin | 1.6% | 6.4% | 0.4% | 1.4%–1.7% |
| Jalon Daniels | 1.2% | 10.1% | 0.4% | 1.0%–3.1% |
| Kameron Johnson | 1.0% | 3.3% | 0.3% | 1.0%–3.2% |
| Jonathan Mingo | 1.0% | 3.5% | 0.7% | 1.0%–1.4% |
| Luke Schoonmaker | 0.6% | 2.3% | 0.3% | 0.3%–0.7% |
| Brevyn Spann-Ford | 0.6% | 4.5% | 0.4% | 0.4%–0.6% |
| Hunter Luepke | 0.5% | 2.9% | 0.2% | 0.3%–0.5% |
| Payne Durham | 0.5% | 1.6% | 0.1% | 0.0%–0.5% |
| Sean Tucker | 0.3% | 3.3% | 0.1% | 0.3%–1.2% |
| Josh Williams | 0.0% | 0.0% | 0.0% | 0.0%–0.0% |

Longest share divides credit for tied longest TDs. It is not the chance of scoring any TD. A quarterback receives credit only for a rushing/receiving score, not for throwing a TD. Assumption ranges are not confidence intervals.
No modeled scrimmage TD: 0.4%.

## Interpretation

CeeDee Lamb ranks first in all five runs (standard, two smoothing alternatives, TB Week4-only stress, Ko Kieft eligible). His standard share is only about 14%, so most simulated games are won by someone else. Javonte Williams has the highest chance of scoring any TD among modeled players; short scoring opportunities do not make him the longest-TD favorite.

Tampa Bay is sensitive: Godwin moves from 7.4% to 11.9% in the Week4-only stress; Irving moves from 10.3% to 6.6%, and Egbuka from 9.1% to 5.4%. This jointly changes usage, team tendencies, and player scoring evidence. It does not prove a quarterback effect.

## Evidence and gaps

Inputs: uploaded slate 96614eda-2f85-4726-a61c-8b682145065c, canonical game 2026_05_TB_DAL, 2023–2026 PBP snapshot (880 games, including 64 current-season games through Week4), GSIS identities, both depth sources, and Week5 injury observations. Team role shares use observed current-season carries/targets; depth ranks are not projected touches.

Sleeper snapshot fetched October6 07:36 UTC; latest player rows updated12:07 UTC. FantasyPros depth captured13:43 UTC; provider update time unavailable. FantasyPros injury observations13:38 UTC; Sleeper injury observations12:07 UTC. Source freshness and disagreements are retained in the evidence files.

Official team evidence confirms Daniels as the starter, despite FantasyPros showing Mayfield questionable while DK/Sleeper show out. Daniels has only one start. Ko Kieft has disputed availability and no modeled current role; an unchanged inclusion scenario demonstrates this missing role forecast, not that he has no real scoring chance. Goodson and Tez depth ranks disagree; we retain those conflicts and use observed usage.

Bowles said Tucker should get more opportunities without specifying equal shares. The baseline has only three current-season Tucker carries and does not automatically forecast that increase. Final game-day availability and defensive injury impacts are not incorporated as measured coefficients.

Unsupported eligible names: Israel Abanikanda, Michael Trigg, James Mitchell, Anthony Smith, Jordan Hudson, Eric Rivers Jr., Dean Patterson IV, Bauer Sharp, Garrett Greene, CJ Dippre. These are unresolved, not verified zero-probability players. Known roles are renormalized; there is no quantified reserve for all unknown workloads.

## Verification

Nine model unit tests passed. Each of five runs used2,000 simulations. Probability accounting, nested event probabilities, kickoff/label cutoffs, target exclusion, frozen source/request digests, unavailable exclusions, and snap limits passed. Simulation size does not establish accuracy. The prior eight-game development backtest is too small to establish calibration.

## Postgame grading

Preserve forecast.json as the primary prediction and grade it with the existing evaluate function once verified PBP exists. Do not choose the best-performing sensitivity after the result. Compare the actual longest scrimmage score, fractional tie credit, Brier score, and log loss; distinguish role/data failures from outcome variance.

## Official sources

- [Daniels starting confirmation](https://www.buccaneers.com/news/jalon-daniels-will-remain-bucs-starter-baker-mayfield-absence)
- [Monday injury report](https://www.buccaneers.com/news/buccaneers-cowboys-injury-report-oct-5-week-5-2026)
- [Backfield workload discussion](https://www.buccaneers.com/news/todd-bowles-bucs-need-to-split-backfield-reps-three-ways)
