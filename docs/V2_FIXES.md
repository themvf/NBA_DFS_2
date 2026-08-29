# Fantasy Football Roster-Aware V2

**Status:** Approved architecture; shadow challenger; not active
**Execution ledger:** [`V2_EXECUTION_STATUS.json`](V2_EXECUTION_STATUS.json)
**Detailed product and acceptance specification:**
[`fantasy-football-roster-aware-v2.md`](fantasy-football-roster-aware-v2.md)
**Source and provenance contract:**
[`fantasy-football-v2-source-contracts.md`](fantasy-football-v2-source-contracts.md)

The detailed specification linked above is incorporated into this V2 source of
truth in full. It defines the shared redraft and DraftKings Best Ball pipeline,
data and source contracts, team-opportunity model, player allocation,
efficiency and touchdown treatment, weekly distributions, Mike Evans
changed-team fixture, validation requirements, promotion policy, and product
delivery behavior.

The broader product contracts remain:

- [`fantasy-football-draft-spec.md`](fantasy-football-draft-spec.md)
- [`nfl-best-ball-model-improvement-spec.md`](nfl-best-ball-model-improvement-spec.md)

Where the detailed roster-aware specification and a broader document differ,
the roster-aware specification governs the V2 football projection pipeline;
the Best Ball specification continues to govern DraftKings contest scoring and
roster-simulation behavior.

## Dependency order

1. Foundations and immutable context
2. Chronological validation framework
3. Team opportunity forecast
4. Player role allocation
5. Efficiency and touchdown share
6. Weekly outcomes and roster simulation
7. Validation and promotion decision
8. Product delivery and activation

No later wave may advance until its ledger dependencies are complete or the
user explicitly records a waiver. Existing partial Best Ball shadow code is
repository evidence, not proof that a V2 acceptance target is complete.

## Activation rule

V2 must not change displayed projections, ranking order, deterministic advisor
inputs, or live draft recommendations until the chronological evidence supports
an explicit `PROMOTE` decision and the activation target passes. A `REVISE`,
`RETAIN`, inconclusive, or failed result keeps V2 in shadow mode.
