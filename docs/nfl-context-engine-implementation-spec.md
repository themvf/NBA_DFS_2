# NFL context engine implementation specification

Status: first descriptive vertical slice published for `2026_02_NYG_LA`.
Predictive and decision use remain unqualified.

The first opportunity and market-attribution studies are also complete. Neutral
snap interval did not clear the DFS materiality gate, while a broader PBP
descriptor bundle improved held-out reconstruction of opening spreads. Neither
result has betting or optimizer authority.

## Objective

Preserve NFL source observations as immutable evidence, derive revisioned
football facts, publish versioned context objects, and require every consumer
to resolve those objects through a trusted policy. Vercel displays the same
saved objects used by non-UI calculations; it does not recompute them.

## Implemented foundation

- `nfl_evidence_observations` stores raw payloads with distinct event,
  publication, observation, and ingestion timestamps plus an idempotency key.
- `nfl_fact_releases`, `nfl_play_fact_revisions`, and
  `nfl_play_penalty_events` separate football status from record lifecycle.
- `model/nfl_play_facts.py` creates one revision candidate per source row,
  retains raw descriptions, recovers wiped actions, preserves multiple penalty
  occurrences, and attaches independent second-event tags.
- `nfl_context_definitions` and `nfl_context_snapshots` retain definition,
  numerator, denominator, value, coverage, evidence, and exact as-of time.
- `nfl_context_qualifications` is the trusted
  definition × consumer × use-case × cohort registry.
- `nfl_consumer_policy_pointers` selects the active centrally managed policy;
  callers cannot activate their own policy version.
- `nfl_consumer_snapshot_manifests` freezes resolved inputs, eligibility,
  fallback decisions, model artifacts, configurations, and scenarios.
- `model/nfl_context_engine.py` implements explicit pinned and current-eligible
  read semantics. It rejects caller-created permissions.
- `web/src/db/nfl-context.ts` is the thin Vercel reader. It performs no football
  calculation and exposes the resolved manifest with every returned value.
- `docs/nfl-context-consumers.json` is the initial consumer inventory.
- `ingest/nfl_prop_odds.py` begins prospective capture. The free event listing
  and cost estimate are the default; `--apply` is required for paid calls.
- `ingest/nfl_context_publish.py` publishes evidence, facts, context, and
  descriptive policy registrations. It is dry-run unless `--apply` is present.

## Timestamp meanings

These fields are not aliases:

- `event_occurred_at`: when the football event or target market event occurs.
- `source_published_at`: when the provider says the observation was published.
- `system_observed_at`: when this system first received the payload.
- `ingested_at`: when the database transaction persisted it.

An absent provider publication time remains null. It must not be replaced with
the system observation time.

## Football status and revision status

A play fact uses independent axes:

- Snap execution: `executed`, `no_snap`, or `unknown`.
- Official action validity: `counted`, `voided`, `administrative`, or `unknown`.
- Penalty event adjudication: `accepted`, `declined`, `offsetting`, or `unknown`.
- Fact revision lifecycle: `current`, `superseded`, or `withdrawn`.

There may be several penalty events for one play revision. A declined penalty
can coexist with a counted action. A superseded fact revision may describe an
action that counted; supersession does not describe football.

The legacy archetype table remains readable during migration. It is not the
new revision ledger, and consumers must not infer revisions from its
`labelled_at` timestamp.

## First context definition

Definition ID: `neutral_offensive_snap_interval_seconds@v1`.

Question: how many game-clock seconds elapsed between adjacent eligible
offensive snaps in neutral pre-play states?

Rules:

1. Sort the complete source sequence by game and play ID.
2. Form intervals between adjacent source records before applying eligibility.
3. Both endpoints must be pass/run snaps by the subject team.
4. Exclude kneels, spikes, two-point attempts, no-plays, and administrative
   records.
5. Both pre-play states must be in quarters 1–3 with score differential from
   -7 through +7, inclusive.
6. Endpoints must share game, possession team, drive, and quarter.
7. Retain game-clock deltas from 0 through 60 seconds.
8. Weight each valid interval equally. The numerator is summed seconds; the
   denominator is valid interval count; the value is their mean.
9. This measures game-clock consumption, including play time. It is not actual
   wall-clock time between snaps.
10. Minimum provisional coverage is 20 intervals across at least two games.
    Failure produces a measured value with `minimumCoverageMet=false`, not a
    fabricated zero.

The feasibility gate must report missing clock rate, invalid delta rate,
eligible intervals per team-game, games meeting coverage, and sensitivity to
the 60-second cap. If coverage is unreliable, the first qualified context will
instead be eligible offensive plays per possession.

## Reader contracts

### Pinned read

Input includes a context snapshot ID and consumer identity. The reader returns
that exact snapshot or fails. It never substitutes a correction.

### Current-eligible read

Input includes definition, subject, target, requested as-of time, consumer,
use case, cohort, and usage. The reader:

1. Resolves a centrally registered qualification.
2. Chooses the newest compatible snapshot available by the requested as-of.
3. Applies staleness rules.
4. Rejects an unapproved dependency.
5. May resolve a separately qualified fallback.
6. Returns a complete manifest so the result can later be replayed exactly.

“Fail closed” rejects the dependency. It does not require the entire consumer
to terminate if that consumer has an independently approved fallback.

## Consumer qualification

Qualification is not a universal maturity ladder. It is explicit for:

> context definition × consumer × use case × applicable cohort × usage

Descriptive approval does not imply predictive approval. Predictive approval
does not imply optimizer or betting-decision approval. Shadow runs may not be
read by decision consumers unless a later policy version grants that exact use.

## Prospective prop capture

The raw provider response is stored before normalized rows. Paired Over/Under
and one-sided markets retain their original outcome shapes because their valid
uses differ. Anonymous outcomes are not guessed onto players.

Dry-run quota estimate:

```powershell
python -m ingest.nfl_prop_odds --event-limit 5
```

Paid capture and persistence:

```powershell
python -m ingest.nfl_prop_odds --event-limit 5 --max-credits 35 --apply
```

Applied capture now also requires an explicit hard ceiling, for example
`--max-credits 35`. The command aborts before the first paid event request when
the estimate exceeds that ceiling. The manual `Capture NFL prop evidence`
workflow defaults the ceiling to zero, so it cannot spend credits accidentally.

Activation in a scheduled workflow requires an explicit budget and cadence.
Historical acquisition remains separate from prospective capture.

## First vertical slice

1. Run the pace feasibility report against a frozen season release.
2. Register the definition and descriptive qualifications for the explorer and
   postgame evaluator.
3. Publish a saved context snapshot and manifest.
4. Expose the same serialized object to the PBP explorer.
5. Make the postgame evaluator perform a pinned read of that snapshot.
6. Demonstrate that a corrected fact release creates a new current snapshot
   while the saved evaluation still reads the old one.
7. Only after retrospective testing, register a shadow predictive
   qualification for an applicable cohort.
8. Evaluate opportunity distributions directly. If fantasy points are also
   shown, feed opportunities into a pinned existing efficiency/scoring model
   and label the result as hybrid.

### Published descriptive release

The 2025 frozen regular-season source produced 46,452 play facts, including
1,627 semi-merged rows, 4,105 merged rows, and 4,219 separately stored penalty
occurrences. The release is
`1b8493a8854518c21824d8c7064ad5f60a064fc92b839cd7397293f40640f379`.

For target `2026_02_NYG_LA`:

- NYG: 31.942 game-clock seconds over 398 eligible intervals; snapshot
  `23041fc2f47f5e7ddba5ee089fbf4627f21c1b5c2f7f27ec6e9699c45fd2b2fd`.
- LA: 31.546 game-clock seconds over 432 eligible intervals; snapshot
  `44b8adb6de447961650eb633e057c45f7abd63b807235ee8a98ebaef4421fa24`.

`npm run verify:nfl-context -- 2026_02_NYG_LA` verifies current resolution,
exact pinned replay, and rejection of an optimizer decision read. The PBP page
shows these stored objects and their evidence contracts without recomputing the
measure in TypeScript.

## Retrospective qualification results

### DFS opportunity

`nfl-context-volume-study-v1` used frozen 2020–2025 regular-season PBP, strictly
prior four-game features, expanding season holdouts, and a game-clustered
bootstrap. Adding team and opponent neutral snap interval to recent play volume
reduced team-play MAE from 6.527 to 6.499 over 1,790 held-out team-games. The
0.028-play gain was consistent but failed the predeclared 0.25-play materiality
gate. Run `34e7dd83d93473f27028f2878eea7737ee0af226400f9b8aa39a438b97abdbf9`
is therefore `not_qualified`; no DFS shadow, production, or optimizer policy was
registered.

### Market explanation

`nfl-market-context-attribution-v1` joined strictly pregame rolling descriptors
to 826 historical consensus opening spreads. Compared with recent scoring
margin and scoring environment, the PBP bundle improved held-out spread MAE by
0.087 points in 2024 and 0.233 in 2025. Recent scoring margin remained the
dominant association; success-rate difference and pass-rate difference were the
largest PBP associations. Run
`6674e98788e63973b0cb40d8b7fd11a3f317c3a87bcc4aa00686097b51285940`
is explanatory only. It can describe factors consistent with the number but
cannot establish sportsbook intent, causation, or a tradable edge.

The PBP page reads both immutable reports and presents separate Vegas, DFS, and
prop-readiness cards. It never upgrades a research result into a recommendation.

## Executable acceptance cases

`tests/test_nfl_context_engine.py` proves:

- Pinned replay never substitutes a newer correction.
- Current reads resolve the latest compatible snapshot and return its ID.
- A caller cannot grant itself decision permission.
- Qualifications are consumer/use-case/cohort/usage specific.
- Unknown and zero remain distinct.
- Estimates require estimation metadata.
- Interval construction does not bridge an excluded play, non-neutral state,
  possession, drive, or quarter.
- Context carries its denominator, coverage, evidence, and release identity.

`tests/test_nfl_prop_odds.py` proves paired and one-sided prop shapes remain
distinguishable and that ambiguous player attribution is rejected.

Run:

```powershell
python -m pytest tests/test_nfl_context_engine.py tests/test_nfl_prop_odds.py tests/test_nfl_dfs_team_context.py -q
```
