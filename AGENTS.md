# Repository agent guide

For NFL play-by-play archetypes, team-season comparisons, or the proposed Team Identity page, read `docs/nfl-team-identity-source-map.md` before changing queries, metrics, or UI. It records the canonical game and team joins, market timing rules, source coverage, and drilldown contract. Keep that map current when adding a source or changing a join.

Existing work in this checkout may be in progress. Inspect the working tree before editing and preserve unrelated changes.

## NFL roster and availability evidence

For NFL player-specific roster, depth, replacement-role, injury, availability, or projection research, inspect **both** Sleeper and FantasyPros evidence for the same team and decision time. Use `python -m research.nfl_dual_depth_audit --season YEAR --team ABBR --output PATH` for a timestamped Sleeper/FantasyPros depth comparison. Also inspect the week-matched FantasyPros injury observation and an official inactive list when one exists. A FantasyPros depth page is a separate published source; its injury API is not a depth feed. When one provider lacks the field in question, record that missing coverage rather than substituting an unrelated field.

Keep source ranks, alignment, capture times, identity matches, and disagreements visible. If either source is unavailable or stale, say so and leave the role unresolved. Do not silently choose one provider, convert a depth rank into projected snaps or targets, or treat an active roster listing as game-day confirmation. After kickoff, new captures are retrospective and must not be described as pre-lock evidence.
