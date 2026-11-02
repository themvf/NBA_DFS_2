# NBA evidence timing and pre-lock lineup safety

The primary cloud dispatcher freezes at 11:37 AM America/New_York across DST.
The free official NBA report is captured and published before every new cloud
freeze. Failed capture leaves an explicit warning and unverified availability.
Late GitHub cron only fills a missed morning; originals are never replaced.
The existing 11:35 local follow-up publishes evidence, waits for the canonical
cloud original, then supplies a fallback only if that original remains absent.

Two bounded free official injury refreshes run at 6:37 and 6:52 PM Eastern.
They publish observations only: no odds calls, model changes, or refreezing.
Earlier afternoon locks remain closed; later observations never become earlier
decision-time evidence. These fixed checks are not continuous monitoring.

`research.nba_points_injury_evidence --apply` appends a verified observation to
`nba_injury_observations`. Original PDFs and parser receipts remain in the
collector archive (cloud artifacts or the existing durable local archive).
The table retains publication, true observation time, raw-source hash, official
URL and parsed rows. Latest eligible publication wins; a player absent from
that report becomes unverified, never automatically Active.

The leaders page shows the frozen forecast, latest stored market capture and
actual injury check separately. Per-player statuses require an exact canonical
player AND game/team join. Out, Questionable, Doubtful and Probable are visible;
an explicit Available status is a report observation, not a minutes guarantee.
No report entry or failed/ambiguous match stays unverified.

New discretionary saves use `nba-boom-exposure-v2-availability`. The highest
eligible frozen lines form its baseline; currently observed Out players are
removed from that new selection pool. Other statuses receive warnings without
point discounts. Frozen source predictions and the registered `nba-boom-trios-v1`
pool stay unchanged. Both original and eligible boards, the observation digest,
statuses and all actual selections are saved. The API rejects changed evidence,
recomputes selections, and checks the latest observation and first-tip deadline
in the database insert. Old saves remain untouched and grade separately.

## Independent workload challenger

`research/artifacts/nba_role_opportunity_v2.json` is the new immutable registration.
It pins `research/nba_points_role_challenger.py`, the original v3 model, a fixed
50/50 recent-intensity adaptation, source/time gates, grading and review rules.
It changes no original model, minutes, shooting rates, or probabilities.
Enrollment starts November 3; today's earlier v3 original cannot enter the test.
Successful future v3 records attach a separate `role-challenger.json` and
`role-index` marker. Failure preserves the original forecast and experiments.

The October 31 draft required official NBA roster evidence. Automated retrieval
of the official roster page and bounded official feed candidates returned HTTP
403 on November 2. Before registration, the evidence policy was explicitly
revised to the existing public ESPN roster collector, with source provider,
athlete IDs, unique joins and completion-time observations retained. The new
`nba-rosters-v2-completed-observation` timestamps a full capture after retrieval;
old run-start timestamps are not eligible evidence for this new test.
Roster membership establishes team identity only, not participation or usage.

After each genuine original source review, grade a present sidecar with:

```
python -m research.nba_points_role_challenger grade --record ORIGINAL/role-challenger.json --diagnostics REVIEW/diagnostics.json --output ROOT/role-grades/NEW.json
python -m research.nba_points_role_challenger report --research-root ROOT --output NEW_REPORT.json
```

Grades preserve low-minute played outcomes and exclude DNP/missing points.
The report takes the latest grade once per indexed original and fixed protocol,
model and source identity. No data means pending with zero prospective counts.
Formal date/player-clustered review requires 30 evaluation-ready dates AND 100
resolved primary conflicts, with a 90-day information limit. Confirmation is
separate, and neither a passing gate nor simulation count promotes the model.

Verification includes injury/game identity, unknown/ambiguous handling, unchanged
Questionable points, Out eligibility, DST schedules, adaptation arithmetic,
registration/cutoff poisoning, duplicate histories, and DNP/missing grading.
Original v1/v2/v3 trial artifacts are preserved.

Morning reviews hydrate cloud observations into the existing local archive with `python -m research.nba_injury_store --hydrate-date YYYY-MM-DD --archive-root PATH`. Downloaded PDF bytes must match the originally stored source hash; the genuine database capture receipt retains its original observation time. Failed hydration stays unresolved. This never treats a newly downloaded, unverified report as pregame evidence.

Registration lineage: v2 supersedes the zero-recording v1 registration during the pre-enrollment implementation audit. V1 remains unchanged for audit. Existing formula functions remain available unchanged in `research/nba_points_role_formula.py`; the prospective wrapper has independent record and protocol identities.
