# Reliable NBA research operations

This operational release preserves the original v2/v3 models, three registered
experiments, workload protocol, math protocol and top-12 selection policy.

Acceptance criteria:

1. The existing 11:37 AM Eastern cloud freeze also runs the pinned v3 recorder
   and its original sidecars. No paid capture, training or late enrollment.
2. Original bytes and index identities survive an ephemeral runner. A database
   advisory lock and append-only per-date/model archive prevent duplicate
   originals. Repeated runs restore the original rather than forecast again.
3. The exact pinned model ships compressed; its content digest is verified
   before use. Restoration never changes forecast or sidecar JSON bytes.
4. The 11:52 AM and 12:22 PM Eastern checks alert on absent originals or failed
   attachments while preserving cutoff and uniqueness rules. Alerts use one
   deduplicated GitHub issue per failure category, following the existing
   failure-sweep convention.
5. Morning cloud reviews grade original archives, hydrate genuine injury
   evidence, retain raw ESPN summaries and full-slate readiness evidence, and
   publish only the already authorized independent math grades and pool audits.
   Latest grades count once. Missing selected outcomes remain unresolved.
6. Boom exposure remains manual and defaults to the market baseline. No model
   receives automatic promotion or a changed mean, ceiling or probability.
7. A cumulative pool diagnostic separates restriction loss from selection loss,
   counts latest audits once per original, preserves provisional coverage and
   fractional ties, and never creates postgame selections. It is descriptive;
   any expanded-pool forecasting challenger requires a new preregistration.

The local 11:35 check-in observes/restores cloud originals. It must not run a
competing v3 recording. The morning check-in can read the durable cloud reviews.
GitHub artifact uploads provide an additional raw-evidence copy; the database
archive is canonical and does not depend on artifact retention.

## Runtime and evidence transport

The exact original v3 model is packaged as
`research/artifacts/nba_ghost_v3_model_v1.json.gz`; decompression must verify
`651441d2dcfbe40aa1bd2c320ef8327056e02bc2c06257995c1cb3c40254bec4`.
No fitting, coefficient changes or new forecasting probabilities are added.

`python -m research.nba_research_cloud restore --root ROOT` restores genuine
originals and observed grade history onto a new runner. Optional `--env-file`
reads an existing local credential file into memory without copying it. Cloud
jobs use the existing `DATABASE_URL` secret. The session-lock worker uses the
same Neon endpoint without `-pooler`, or an existing `DATABASE_URL_UNPOOLED`;
transaction pooling cannot provide session advisory-lock guarantees. The normal
website database connection is unchanged.

`import-local --date DATE --root ORIGINAL_RESEARCH_ROOT` is an operator-only
transport of an already indexed genuine original and its existing evidence.
It creates no forecast, registration or outcome grade. Cloud recording requires
the database clock to precede the verified first tip. If a worker stopped after
forecasting began but before durable archival, retries fail closed and alert;
operators must inspect the preserved workflow artifact instead of generating a
replacement. Missing or failed sidecars are never attached by a later retry.

## Archive cache (2026-11-10)

Every `record` and `review` rebuilt the runner's research folder by downloading
every archive from the database: about 180 MB a run by 2026-11-10 (11 originals,
17 reviews, 8 v2 reviews), growing by roughly 14 MB a day, 13 such runs in two
days. `--archive-cache DIR` keeps the packed archives between runs. Manifests are
still read for every row; an archive's bytes are read from the cache only when
the copy's own sha256 equals the manifest's `archive_sha256`, and from the
database otherwise (the download then replaces the copy). `unpack` and `restore`
are unchanged and still check every file, so the folder a run rebuilds is the same
with a warm cache, a cold cache or none. Both workflows restore the cache with
`actions/cache/restore` (prefix `nba-research-archives-`) and save it again only
when it gained an archive; a `check` restores nothing and skips the cache. Each
run's result records `archive_transport` (`cache`, `database`, `cache_enabled`),
so a cache that stopped working shows as every archive coming from the database.
The cache folder sits outside `artifacts/nba-research/`, so it is not uploaded
with the run's evidence.

Original artifacts, their original path-bearing indexes, and relocated indexes
are retained separately. Every file is hash-checked; conflicting local files,
path escapes, altered models and duplicate per-date identities are rejected.
The original three experiment histories and workload/math grades keep their
actual observation timestamps when transported. Registered report readers still
select the latest grade once per indexed original and protocol/model.

The cumulative pool report is `python -m research.nba_trio_pool_report --root
ROOT --output NEW.json`. Its complete and provisional cohorts never pool. A
missing audit for the latest exact math grade remains pending rather than
falling back to a stale audit. Trio Lab exposes the same separate denominators.
The Boom builder starts at zero requested extra inclusion; selecting a rising
volume player alone does not silently raise exposure.
