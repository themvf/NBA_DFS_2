# NFL DFS expert coverage as an ownership signal — `nfl-expert-coverage-study-v1` (registered 2026-10-21)

Status: REGISTERED 2026-10-21, before any capture for a 2026 week-7 slate (DB
numbering in `nfl_season_games`: week 7 = NE@CHI Thursday 2026-10-23 00:15 UTC
through DAL@PHI Monday 2026-10-27). This document is committed before the
collector (`ingest/nfl_expert_coverage.py`) writes its first row. Nothing below
changes `nfl-ownership-prior-v5`; a coverage term, if this study passes, would
be `v6` with its own registration.

## 0. Why

v5 misses "value chalk": cheap players the field piles onto. WAS@SF
(2026-10-20 00:15 UTC, contest 196786695): Brandin Cooks 19.3% and Austin
Ekeler 19.4% flex against 5-6% predicted
(`docs/nfl-ownership-pregrade-2026-10-19-was-sf.md`). The task-4 regrade found
97 such misses with no signal in our inputs. Expert DFS articles published
before lock named Cooks as a value play. The hypothesis is that the field reads
the same articles, so how widely and in what role a player is covered carries
information about field ownership that v5 (projection, salary, DK average,
status) does not.

## 1. Hypothesis

**H1.** Coverage features computed from expert articles captured before the
frozen cutoff reduce the error of v5's ownership forecast on held-out slates.

Mechanism, stated before data: the field over-owns players that several outlets
present as values (cheap, newly in a role). So the coefficient on value
coverage should be POSITIVE (more coverage, actual above v5). A fit that only
improves through a negative coverage coefficient is not support for H1.

## 2. Collection (frozen for v1)

Collector `ingest/nfl_expert_coverage.py`, tag `nfl-expert-coverage-v1`;
extractor `model/nfl_expert_extraction.py`, `nfl-expert-extract-deepseek-v1`.

**Sources (v1 set).** Verified live 2026-10-21 (robots evidence stored in
`nfl_expert_robots_checks` on every run):

| source | discovery | status |
|---|---|---|
| DraftKings Network | `news-sitemap.xml` + newest `post-sitemap` | allowed (only `/search-results` disallowed for `*`) |
| RotoBaller | `google-news-sitemap.xml` + newest `sitemap_post_1.xml.gz` | allowed; `/premium/` URLs skipped as paywalled |
| RotoWire | `news-sitemap.php` | allowed for `*` (named scrapers blocked; we are not one) |
| Footballguys | `/articles?category=dfs` listing | robots.txt 404 = no restrictions (RFC 9309); logged-out page only |
| DailyAmbush | `sitemap.rss` | allowed; members-only parts are images and are not captured |
| Fantasy Life | `news-sitemap.xml` | allowed (`/articles/` open; `/analysis/`, `/api/` closed) |
| Lineups.com | — | DROPPED: every page incl. the sitemap answers a Cloudflare challenge (403); never bypassed |
| DraftWithDoug | — | DROPPED: article body is not in the HTML (title and deck only); the body loads client-side from a data API and robots disallows `/api/` |

Adding a source later is allowed for COLLECTION but the primary analysis below
uses the v1 source set only; a wider set is a new feature version.

**Rules.** Descriptive User-Agent, at most one request per second, robots.txt
fetched and evaluated every run, never log in, never bypass a paywall or
challenge; a page whose JSON-LD says `isAccessibleForFree: false` or whose URL
is a known premium path is not stored. Raw article text is append-only; a
re-fetch whose text changed appends a revision, never rewrites.

**Schedule.** Per kickoff group (games sharing a kickoff time), one pass in
each window before kickoff: T-48h, T-24h, T-6h, T-90m (the Vercel dispatch
tick inside 47.75-48h, 23.75-24h, 5.75-6h, 75-90min). Dispatched by
`/api/cron/dispatch` (job `nfl-expert-coverage`), GitHub schedule as fallback.

**Timing.** `published_at` is parsed from the page (JSON-LD `datePublished`,
then `article:published_time`, then `<time datetime>`, then the site's printed
date) and stored with the method and its precision (`second` or `day`).
`captured_at` is our fetch time. An article REVISION is eligible for slate S iff
`captured_at <= first_kickoff(S) - 20 minutes` (the scorecard v3 canonical
cutoff, "T-20"). Capture time binds because it is the only time we can prove;
published_at must not be after captured_at (checked, flagged). Anything
captured after S's first kickoff is `retrospective` and can never enter this
study; between T-20 and kickoff it is `late` and excluded from the frozen
features (reported).

## 3. Records extracted (LLM extraction only)

Per article revision: the slate(s) it covers (games, format Showdown/Classic)
and, per player: name, team, role in {captain, core, value, contrarian, fade,
mention}, slot if stated, published ownership % if printed (with the source's
slot/contest definition when stated, else null), and a verbatim quote. A record
whose quote is not a substring of the stored text is discarded and counted.
Sample lineups: players and slots when printed; a lineup with any player name
absent from the text is discarded and counted. The LLM never estimates
ownership or judges edge.

Players are matched to the slate's DraftKings pool (name within the stated
team, then unique surname within the team). Unmatched and ambiguous mentions
stay recorded with their flag, are excluded from features, and their rate is
reported per slate; a slate with more than 15% unmatched mentions is flagged.

## 4. Features (frozen)

Per slate S, player p, using the latest eligible revision of each article,
counting DISTINCT SOURCES (one outlet writing three articles counts once):

| feature | definition |
|---|---|
| `n_outlets` | sources mentioning p in any role except `fade` |
| `n_value` | sources giving p the role `value` |
| `n_captain` | sources giving p the role `captain` (Showdown captain model only) |
| `pub_own` | mean published ownership % across sources that print one for p's slot (or unslotted on Classic / Showdown flex); null when none |

Secondary, reported never gated: `n_core`, `n_contrarian`, `n_fade`,
`n_lineups` (sample lineups including p).

## 5. Primary analysis

Graded contests and labels exactly as `docs/nfl-ownership-scorecard-v1.md`
under scorecard v3: reference contest per slate, population 1 (full pool,
`observed` + `reconciled_zero` labels only), canonical horizon v5 capture.
Residual `r = actual - v5`, in percentage points, separately for:

1. Classic overall,
2. Showdown flex,
3. Showdown captain.

**Walk-forward by slate.** Order a format's graded slates by first kickoff. For
slate k with at least 2 earlier graded slates of the same format, fit two OLS
models on the earlier slates only (labels available before slate k's cutoff):

- B (baseline): `r ~ 1` (an intercept: v5 plus its own bias correction)
- C (coverage): Classic and Showdown flex `r ~ 1 + n_outlets + n_value`;
  Showdown captain `r ~ 1 + n_outlets + n_captain`.

Predict `max(0, v5 + fit)` on slate k; score MAE on population 1.

**Metric.** Per held-out slate `d_k = MAE_B - MAE_C` (positive = coverage
helps). Pooled mean of `d_k` with a slate-clustered bootstrap (10,000
resamples, seed 20261021).

**Minimum sample before any verdict.** 8 Showdown and 4 Classic held-out slates
WITH coverage (at least one eligible v1-source article for the slate). Slates
without coverage are still scored (zero features is information) but do not
count toward the minimum. One look, at the first grade on which a format's
minimum is met; if it is not met by the end of the 2026 regular season, that
format is INCONCLUSIVE.

**Pass, per test.** All of:

- the lower bound of the bootstrap interval of mean `d_k` is above zero, at
  98.33% two-sided (Bonferroni across the three tests);
- the mean per-slate change in Spearman (averaged ties) is no worse than -0.01;
- the coverage coefficient(s) in the fit through the last graded slate are
  positive (`n_value` for Classic and flex, `n_captain` for captain).

A pass licenses drafting `v6` with a coverage term and its own registration; it
does not change v5. A failure is recorded as written; no re-slicing, feature
redefinition, source-set change or extraction re-prompt rescues it (each is a
new, separately registered version).

## 6. Secondary (descriptive, never a gate)

- **Published ownership as external forecasts.** Every printed percentage is
  graded per site under scorecard v3 (source tag `expert:<source>`), on the
  players that site printed (selected population, stated as such), by slot when
  stated. MAE, bias, Spearman, n.
- **Named misses.** Per slate, players with actual >= 10% and v5 < 7% (the
  value-chalk pattern): how many had `n_value >= 1` before cutoff.
- Coverage counts per source and slate, eligible vs late vs retrospective,
  extraction discards, unmatched rates.

## 7. Provenance and what this cannot claim

Every feature row traces to article revision ids, capture times and the
extraction model version. Articles describe what writers recommend, not what
the field did; a passing result says coverage carries ownership information,
not that it is causal, and not that it would survive a change in which outlets
the field reads. WAS@SF and every other already-played slate are
`retrospective` dry runs of the pipeline and are excluded.
