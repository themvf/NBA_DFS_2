# Implementation Plan: Fantasy Auto-Draft Simulator

## Overview
Implement the scoped persisted MVP and validate the complete user/CPU turn lifecycle.

## Tasks
- [x] 1. Specify requirements, design, consistency rules, and exclusions.
- [x] 2. Add pure deterministic CPU selection engine.
  - [x] 2.1 Add seeded variation and ranking fallbacks.
  - [x] 2.2 Add starter/FLEX need, specialist timing, caps, and feasibility.
  - [x] 2.3 Add full seeded snake-draft tests.
- [x] 3. Persist simulator configuration during draft creation.
  - [x] 3.1 Add Manual vs CPU setup mode.
  - [x] 3.2 Store simulator version and seed.
- [x] 4. Implement simulator server actions.
  - [x] 4.1 Enforce controlled/computer turn ownership.
  - [x] 4.2 Atomically persist consecutive CPU picks.
  - [x] 4.3 Group undo around the latest user decision.
- [x] 5. Update draft-room UX.
  - [x] 5.1 Resume CPU turns automatically.
  - [x] 5.2 Show progress and disable user controls off-turn.
  - [x] 5.3 Preserve manual-room behavior.
- [x] 6. Complete validation.
  - [x] 6.1 Run CPU engine tests and TypeScript checks.
  - [x] 6.2 Run existing suites and production build.
  - [x] 6.3 Smoke-test persisted simulator lifecycle.

## Task Dependency Graph
```json
{
  "waves": [
    { "wave": 1, "tasks": ["1"] },
    { "wave": 2, "tasks": ["2"] },
    { "wave": 3, "tasks": ["3", "4"] },
    { "wave": 4, "tasks": ["5"] },
    { "wave": 5, "tasks": ["6"] }
  ]
}
```

## Notes
Auction, keeper, dynasty, trades, multiple humans, third-party synchronization, and empirical CPU calibration remain out of scope.