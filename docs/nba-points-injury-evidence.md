# Injury and market evidence

This adds free official NBA injury report capture and time-aware joins alongside the frozen points trial. It collects evidence for future injury-adjustment evaluation; it does not fit injury coefficients, change forecasts, infer participation probabilities or request sportsbook quotes.

## Trust contract

- Keep publication, actual observation, and ingestion timestamps distinct. The URL timestamp must match the PDF's printed publication header. A report published before lock but first downloaded after tip remains retrospective evidence.
- Save original PDF bytes, SHA-256, source URL, parsed report rows, page provenance, team-submission coverage, and an observation digest. Files are exclusive creations. Parse errors retain raw PDF evidence and an error record rather than creating a partial valid observation.
- Parse physical table coordinates, status anchors, repeated/cross-page game/team context and wrapped reasons. Unknown layout, team/matchup disagreement and duplicate player identities fail closed. The known layout is the official landscape report; changed layouts require verification before use.
- Injury joins require the same slate date, canonical player, matchup and reported team within the frozen game. No fuzzy match or historical-team assumption. For players not listed, status remains unknown. Missing/unsubmitted team coverage is explicit. An Out list covers both teams in the game, grouped by the report's team; do not label these all as a player's teammates.
- For each quote use only reports actually observed strictly before that quote. Also save status as of forecast creation and the common contest lock. A later status must not be attributed to an earlier line. Final outcome fields are diagnostic only.
- Preserve every relevant status and quote observation. Latest eligible publication wins; older reports retrieved later cannot roll status backwards. Later-published reports that omit a player do not silently carry forward an earlier status or imply Available.
- Do not interpret line movement as an injury's causal scoring effect. Opponent news, rotation, other information and market noise can move lines. No status is translated automatically into minutes, usage or points.

## Capture

```powershell
python -m research.nba_points_injury_evidence --date 2026-10-27 --archive-root "C:\Docs\_AI Python Projects\NBADFS_v2\artifacts\nba-injury-evidence"
```

By default, select the latest already-published report for that date from the official season index. If the index is blocked or has no usable date, try at most four already-published quarter-hour PDF archive URLs. Discovery method and attempted response codes remain in the saved record. These are free HTTPS requests only to the official NBA index and report. A bounded lookup failure remains a capture failure, not evidence that nobody is injured. `--report-url URL` selects a specific official archive. `--pdf FILE --report-url URL` parses an already-downloaded report but records observation now; it cannot backdate the download.

`--pdf FILE --capture-receipt RECEIPT.json` imports an existing internal capture receipt with its original source SHA-256, URL and captured_at. These must agree exactly; imported provenance and ingestion time remain visible. Use only a genuinely preserved capture receipt, never a newly invented timestamp. Otherwise, historical downloads have a current observation time. The collector refuses arbitrary old observation times without a matching receipt.

## Daily review

For a future freeze, add `--availability-output NEW_FILE.json` to capture. It exports only that slate date's unique canonical names, with explicit source/date/publication/observation evidence. Pass the new file to the existing `research.nba_ghost_runner freeze --availability NEW_FILE.json` option. Its existing reported-Out handling excludes those players from frozen trio eligibility and suppresses their watch flag; conditional point estimates and fitted coefficients remain unchanged. Questionable is not converted into an arbitrary scoring discount. Unlisted/ambiguous players remain unverified. No existing prospective snapshot may be replaced.

```powershell
python -m research.nba_points_daily --date 2026-10-26 --injury-archive "C:\Docs\_AI Python Projects\NBADFS_v2\artifacts\nba-injury-evidence" --output-dir artifacts/daily/new-review --apply
```

The existing v2 append-only grading path now stores injury/market evidence in an additional field. Core forecast and grading calculations remain unchanged. Daily artifacts include `injury-observations.json`, `injury-market-evidence.json`, `injury-market-review.md`, and raw injury PDFs in `injury-sources/`. An empty archive yields explicit unknown status throughout, never presumed availability. The readable report links this audit; compact inline chart data contains only status-coverage counts.

The audit includes morning, contest-lock and game-specific quote statuses; publication versus observation timelines; same-game absences by reported team; source quote IDs and line movement; final points, minutes and FGA alongside frozen opportunity forecasts. Subsequent adjustment models can be evaluated using these saved inputs; a model without an actual pregame adjusted prediction cannot be credited with hypothetical improvement.

The local daily automation should capture free injury evidence **before** a new forecast freeze, and pass the shared archive path to every daily review. Additional manual/authorized monitoring captures can use the same command near lock. A single morning run does not establish final game-day availability or continuous injury monitoring. Paid near-tip odds collection and website deployment remain outside this feature.
