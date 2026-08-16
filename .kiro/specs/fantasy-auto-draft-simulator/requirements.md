# Requirements Document

## Introduction
Add a persisted Fantasy Football snake-draft simulator where one user chooses a draft slot and all other teams are computer-controlled. Existing manual draft rooms remain supported.

## Glossary
- **Controlled Team**: The single team drafted by the user.
- **CPU Team**: Any computer-controlled opponent.
- **CPU Batch**: Consecutive CPU selections between controlled-team turns.
- **Seed**: Stored value that makes CPU decisions reproducible.

## Requirements
### Requirement 1: Simulator Setup
**User Story:** As a user, I want to choose my slot and opponent mode so that I can run a mock draft.

#### Acceptance Criteria
1. WHEN creating a draft, THE SYSTEM SHALL offer Mock vs CPU and Manual Room modes.
2. WHEN CPU mode is selected, THE SYSTEM SHALL store a simulator version and seed.
3. THE SYSTEM SHALL preserve team count, scoring, roster, rounds, strategy, and ranking-snapshot validation.

### Requirement 2: Turn Orchestration
**User Story:** As a user, I want opponents to draft automatically so that I only make my team’s selections.

#### Acceptance Criteria
1. WHEN a CPU team is on the clock, THE SYSTEM SHALL advance CPU picks until the controlled team is on the clock or the draft completes.
2. WHEN the controlled team is on the clock, THE SYSTEM SHALL reject CPU picks.
3. WHEN a CPU team is on the clock, THE SYSTEM SHALL reject manual picks in simulator mode.
4. THE SYSTEM SHALL prevent duplicate players, non-consecutive batches, and partial CPU persistence.

### Requirement 3: Credible CPU Decisions
**User Story:** As a user, I want plausible opponent selections so that the mock draft is useful.

#### Acceptance Criteria
1. THE CPU SHALL consider roster-adjusted ADP, ranking fallbacks, starter/FLEX needs, and seeded variation.
2. THE CPU SHALL suppress early K/DST picks unless feasibility requires them.
3. THE CPU SHALL complete each roster with every configured required position fillable.
4. THE CPU SHALL persist its model version, score, and reasons with every selection.

### Requirement 4: Recovery and UX
**User Story:** As a user, I want clear progress and safe undo behavior.

#### Acceptance Criteria
1. WHEN a simulator opens on a CPU turn, THE UI SHALL resume advancement automatically and announce progress.
2. WHEN undo is requested, THE SYSTEM SHALL reverse the latest user pick and every later CPU pick.
3. WHEN the draft completes, THE SYSTEM SHALL preserve the full board and completed status.
4. Existing manual sessions SHALL retain single-pick undo and manual opponent entry.