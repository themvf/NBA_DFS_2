import assert from "node:assert/strict";

import {
  ANALYST_NOTE_STYLE,
  ANALYST_VERDICTS,
  MAX_NOTE_LENGTH,
  analystNoteTooltip,
  isAnalystVerdict,
  normalizeAnalystName,
  validateNoteInput,
  type PlayerNote,
} from "../src/lib/fantasy-football/analyst-notes";

// Suffix stripping mirrors ingest/ff_fantasypros.py::normalize_name, so the seed
// and admin lookups resolve a note keyed on the suffixed name.
assert.equal(normalizeAnalystName("Kenneth Walker III"), "kennethwalker");
assert.equal(normalizeAnalystName("Michael Pittman Jr."), "michaelpittman");
assert.equal(normalizeAnalystName("Ja'Marr Chase"), "jamarrchase");
assert.equal(normalizeAnalystName("Amon-Ra St. Brown"), "amonrastbrown");
assert.equal(normalizeAnalystName(""), "");

// Every verdict has presentation, and the guard accepts exactly those four --
// it is what stops a hand-crafted server-action call writing a verdict the CHECK
// constraint would then reject at the database.
for (const verdict of ANALYST_VERDICTS) {
  assert.ok(ANALYST_NOTE_STYLE[verdict], `${verdict} has no style`);
  assert.ok(isAnalystVerdict(verdict));
}
assert.equal(isAnalystVerdict("bullish"), false);
assert.equal(isAnalystVerdict(""), false);
assert.equal(isAnalystVerdict(null), false);
assert.equal(isAnalystVerdict(7), false);

// Input validation: empty notes and over-long fields are refused before they
// reach the database.
assert.ok(validateNoteInput("", "Target"));
assert.ok(validateNoteInput("   ", "Target"));
assert.equal(validateNoteInput("A real note.", "Target"), null);
assert.ok(validateNoteInput("x".repeat(MAX_NOTE_LENGTH + 1), "Target"));
assert.ok(validateNoteInput("A real note.", "x".repeat(41)));

const seeded: PlayerNote = {
  playerId: 1,
  category: "draft-board",
  verdict: "fade",
  verdictLabel: "Fade at this price",
  note: "Rice averaged 18.5 PPR points in his eight games last season.",
  updatedAt: "2026-08-29T12:00:00Z",
  listRank: 15,
  sourceTeam: "KC",
  sourceAdp: 15,
};

// A seeded note keeps its provenance in the tooltip heading.
const seededTooltip = analystNoteTooltip(seeded);
assert.match(seededTooltip, /Fade at this price/);
assert.match(seededTooltip, /#15 on the analyst board/);
assert.match(seededTooltip, /KC/);
assert.match(seededTooltip, /listed ADP 15/);
assert.match(seededTooltip, /Rice averaged 18\.5 PPR points/);
assert.match(seededTooltip, /does not change our projection, rank, or ADP/);

// A hand-written note has no list rank, team, or ADP -- the heading must not
// render an empty "#null" provenance run for it.
const handWritten: PlayerNote = {
  playerId: 2,
  category: "ppr-consistency",
  verdict: "target",
  verdictLabel: "Target",
  note: "Cheap points in a good offense.",
  updatedAt: null,
  listRank: null,
  sourceTeam: null,
  sourceAdp: null,
};
const handTooltip = analystNoteTooltip(handWritten);
assert.match(handTooltip, /Target/);
assert.doesNotMatch(handTooltip, /#/);
assert.doesNotMatch(handTooltip, /null/);
assert.doesNotMatch(handTooltip, /undefined/);
assert.doesNotMatch(handTooltip, /last edited/);
assert.match(handTooltip, /does not change our projection, rank, or ADP/);

console.log("analyst notes: all assertions passed");
