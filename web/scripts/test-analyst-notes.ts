import assert from "node:assert/strict";

import {
  ANALYST_NOTES,
  analystNoteTooltip,
  getAnalystNote,
  normalizeAnalystName,
} from "../src/lib/fantasy-football/analyst-notes";

// The supplied list is exactly 1-100, contiguous, no gaps or repeats.
assert.equal(ANALYST_NOTES.length, 100);
assert.deepEqual(
  ANALYST_NOTES.map((note) => note.listRank),
  Array.from({ length: 100 }, (_, index) => index + 1),
);

// Every note carries the fields the tooltip renders.
for (const note of ANALYST_NOTES) {
  assert.ok(note.name.trim(), `#${note.listRank} missing name`);
  assert.ok(note.note.trim().length > 40, `#${note.listRank} note looks truncated`);
  assert.ok(note.verdictLabel.trim(), `#${note.listRank} missing verdict label`);
  assert.ok(Number.isFinite(note.adp), `#${note.listRank} missing adp`);
}

// Suffix stripping mirrors ingest/ff_fantasypros.py::normalize_name, so a note
// keyed on the suffixed name still resolves the board's unsuffixed row.
assert.equal(normalizeAnalystName("Kenneth Walker III"), "kennethwalker");
assert.equal(normalizeAnalystName("Michael Pittman Jr."), "michaelpittman");
assert.equal(normalizeAnalystName("Ja'Marr Chase"), "jamarrchase");
assert.equal(normalizeAnalystName("Amon-Ra St. Brown"), "amonrastbrown");

// Lookup is name-driven, so a suffix or punctuation difference between the note
// and the roster feed still matches.
assert.equal(getAnalystNote("Kenneth Walker", "RB")?.listRank, 23);
assert.equal(getAnalystNote("Kenneth Walker III", "RB")?.listRank, 23);
assert.equal(getAnalystNote("Travis Etienne", "RB")?.listRank, 38);
assert.equal(getAnalystNote("Jahmyr Gibbs", "RB")?.listRank, 1);

// Team is deliberately NOT part of the key: several notes describe 2026 moves
// the roster feed may not agree with, and a team mismatch must not hide a note.
assert.equal(getAnalystNote("Kenneth Walker III", "RB")?.team, "KC");

// Position is only a tiebreaker, so a position the note doesn't list still
// resolves as long as the name is unique -- which every entry currently is.
assert.equal(getAnalystNote("Jahmyr Gibbs", "WR")?.listRank, 1);
assert.equal(getAnalystNote("Jahmyr Gibbs")?.listRank, 1);

// No two notes collide after normalization, which is what makes the above safe.
const keys = ANALYST_NOTES.map((note) => normalizeAnalystName(note.name));
assert.equal(new Set(keys).size, keys.length, "two notes normalize to the same name");

// Players outside the list render nothing rather than a wrong note.
assert.equal(getAnalystNote("Some Undrafted Guy", "WR"), null);
assert.equal(getAnalystNote(""), null);

// The tooltip carries the verdict, the source rank/ADP, the note itself, and
// the disclaimer that this never moves our numbers.
const tooltip = analystNoteTooltip(ANALYST_NOTES[14]);
assert.match(tooltip, /Fade at this price/);
assert.match(tooltip, /#15 on the analyst board \(WR KC, listed ADP 15\)/);
assert.match(tooltip, /Rice averaged 18\.5 PPR points/);
assert.match(tooltip, /does not change our projection, rank, or ADP/);

console.log("analyst notes: all assertions passed");
