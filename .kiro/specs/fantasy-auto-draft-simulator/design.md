# Design Document: Fantasy Auto-Draft Simulator

## Overview
Reuse the existing persisted Draft Lab. Simulator metadata lives in `recommendation_config.simulator`; a pure seeded policy chooses players; one server action atomically records every CPU pick between user turns.

## Architecture
The browser observes the persisted current turn. On a CPU turn it invokes one server action, which loads the immutable ranking snapshot, applies the pure CPU policy for consecutive opponent slots, and commits the batch through one revision-guarded SQL statement. On a controlled turn, the existing manual action records the user selection and the refreshed room resumes CPU advancement.

## Components and Interfaces
- `auto-draft.ts`: exports `AUTO_DRAFT_VERSION`, typed inputs/results, and `selectComputerPick()`.
- `DraftSetupForm`: selects `simulator` or `manual` mode.
- `createFantasyDraft()`: stores `{enabled, version, seed}`.
- `advanceComputerDraft()`: computes and commits one CPU batch.
- `recordFantasyPick()`: enforces controlled-team ownership in simulator mode.
- `undoFantasyPick()`: reverses a simulator decision group.
- `DraftRoomClient`: triggers CPU advancement and disables off-turn controls.

## Data Models
No migration is required. Existing session/team/slot/event tables are reused.

```ts
recommendationConfig.simulator = {
  enabled: boolean;
  version: string;
  seed: string;
};
```

Each CPU event payload stores version, seed, team slot, policy score, reasons, adjusted ADP, and ranking source.

## Correctness Properties
### Property 1: Active-player uniqueness
**Validates: Requirements 2.4**

A player appears in at most one active pick.

### Property 2: Consecutive CPU ownership
**Validates: Requirements 2.1, 2.2**

A CPU batch contains consecutive uncontrolled slots beginning at `current_pick`.

### Property 3: Atomic advancement
**Validates: Requirements 2.4**

A successful batch advances revision once and inserts every requested pick.

### Property 4: Seed determinism
**Validates: Requirements 1.2, 3.1**

The same state and seed produce the same CPU choice.

### Property 5: Roster feasibility
**Validates: Requirements 3.2, 3.3**

Completed CPU rosters can fill all configured dedicated and FLEX slots.

### Property 6: Backward compatibility
**Validates: Requirements 4.4**

Manual rooms preserve existing behavior.

## Error Handling
Revision mismatches, stale boards, duplicate players, illegal turns, and empty legal pools return explicit errors without partial inserts. The UI keeps the board intact and allows refresh/retry.

## Testing Strategy
- Pure tests: determinism, variation, ADP fallback, roster needs, specialists, full-draft uniqueness, and feasibility.
- Existing Fantasy Football and redraft suites.
- TypeScript and production build.
- Live database smoke: create, CPU advance, user pick, second advance, grouped undo, and cleanup.