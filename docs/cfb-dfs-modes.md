# CFB DraftKings Classic and Showdown

Use `/cfb/dfs` and select the mode before uploading salaries. Existing slates default to Classic through additive schema changes.

Acceptance: Classic retains eight slots, a $50,000 cap and two games. Showdown requires six distinct athletes from one game and both teams, exactly one Captain, uploaded Captain IDs/salaries, and 1.5x Captain points. Pairing rejects ambiguous/missing rows, duplicate IDs and incorrect salary multipliers. Locks, exclusions, exposure and uniqueness count athletes across both roles; changing only Captain does not satisfy minimum unique athletes. Entry exports preserve FLEX/UTIL template headers; plain Showdown exports use CPT and five UTIL columns. New runs freeze base projections; manual estimates are stored in settings.

Kickers are accepted for Showdown, but require a manual estimate or explicit exclusion because the baseline lacks kicking coverage. Known unavailable players cannot be restored through overrides. Showdown postgame grading is explicitly unavailable; Classic grading remains unchanged.

Verification (2026-10-31): TypeScript, ESLint and five suites pass: cfb-dfs, cfb-showdown, cfb-entry-export, cfb-results, cfb-live-status. Showdown tests compare the optimizer against exhaustive enumeration and cover pairing, legality, controls and Captain exports, and parse and optimize a real DraftKings CFB Showdown file (`web/scripts/fixtures/cfb-dk-showdown-salaries-2026-10-31-osu-usc.csv`, draft group 155789, 82 paired athletes, 6 kickers). In that file DraftKings writes the non-Captain slot as `UTIL`, every Captain salary is exactly 1.5x, and the live lobby lists Showdown as contest type 95 ("Showdown Captain Mode"). Classic uses a real salary fixture.

End to end on a local server against the production database, the same file: Classic mode refused it ("no S-FLEX slot"); Showdown mode uploaded and projected it (OSU 28.8 / USC 22.3 implied); status refresh matched draft group 155789; Generate refused until each of the 6 kickers had a manual estimate or was marked Out; 20 lineups were built and saved with the OUT quarterback absent and Captain at 1.5x salary; a reload restored Showdown mode and the run (the top player in exactly 14 of 20, the 70% cap across both roles); the plain export wrote `CPT,UTIL,...` with the Captain draftable ID first.

Limits: DraftKings has not accepted one of these uploads yet; the plain CSV and the entries-file layout are inferred from the salary file and the Classic entries format. Projections remain an unvalidated baseline. Salary files do not reliably identify sport; supply CFB files. Showdown results grading is not built. No entries submitted.

Rules reference: https://support.draftkings.com/dk/en-us/game-style-showdowns-overview?id=kb_article_view&sysparm_article=KB0010694
