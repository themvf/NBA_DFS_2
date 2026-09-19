import assert from "node:assert/strict";
import {
  OUT_PROJECTION_STATUS, availabilityNote, zeroOutProjection,
  type ZeroableProjection,
} from "../src/lib/nfl-dfs/out-projection";
import { buildValueIndex } from "../src/lib/nfl-dfs/salary-value";

const projection = (over: Partial<ZeroableProjection> = {}): ZeroableProjection => ({
  projectionStatus: "historical", ourProj: 17.4377, floorFpts: 6.2,
  ceilingFpts: 31.8, boomRate: 0.22, ...over,
});

// ── Zeroing an absent player ───────────────────────────────────────────
{
  // Nico Collins as the live slate actually had him: DK OUT, model row
  // untouched at 17.44 with status "historical".
  const collins = projection();
  const out = zeroOutProjection(collins, true);
  assert.equal(out.ourProj, 0);
  assert.equal(out.floorFpts, 0);
  assert.equal(out.ceilingFpts, 0);
  assert.equal(out.boomRate, 0);
  assert.equal(out.projectionStatus, OUT_PROJECTION_STATUS);
  assert.equal(OUT_PROJECTION_STATUS, "out", "matches the Python zero_out() status string");

  // Zero, not shrunk, and not null: "we project nothing" is a real number,
  // distinct from "we have no projection".
  assert.notEqual(out.ourProj, null);
}

// ── A playing player is returned untouched ─────────────────────────────
{
  const playing = projection();
  const same = zeroOutProjection(playing, false);
  assert.equal(same, playing, "same reference, so callers can apply it unconditionally");
  assert.equal(same.ourProj, 17.4377);
}

// ── The input is not mutated ───────────────────────────────────────────
{
  const original = projection();
  zeroOutProjection(original, true);
  assert.equal(original.ourProj, 17.4377, "the caller's row is left alone");
  assert.equal(original.projectionStatus, "historical");
}

// ── Extra fields survive ───────────────────────────────────────────────
{
  const row = { ...projection(), name: "Nico Collins", salary: 7000, avgFptsDk: 14.2 };
  const out = zeroOutProjection(row, true);
  assert.equal(out.name, "Nico Collins");
  assert.equal(out.salary, 7000);
  // Other people's numbers are NOT zeroed: doing so would misreport what
  // DK / FantasyPros / LineStar actually published.
  assert.equal(out.avgFptsDk, 14.2);
}

// ── A zeroed row reads as "no value", not as terrible value ────────────
{
  const pool = Array.from({ length: 20 }, (_, i) => ({
    position: "WR", salary: 5000, ourProj: 8 + i,
    projectionStatus: "historical", isOut: false,
  }));
  const zeroed = { position: "WR", salary: 7000, ourProj: 0, projectionStatus: "out", isOut: true };
  const verdict = buildValueIndex([...pool, zeroed]).assess(zeroed);
  assert.equal(verdict.tier, "unknown", "an out player gets no value tier");
  assert.equal(verdict.multiple, 0, "0 points for $7,000 is 0.00x, which is true");
}

// ── FIXED COPY: `rule: "zeroed"` no longer claims a handoff ────────────
{
  // The stamp lands on the ABSENT player and carries no transfer information.
  // The old copy read "projection zeroed, opportunity handed to the backup"
  // here, which for a receiver asserted something that never happened.
  const note = availabilityNote({ rule: "zeroed", status: "OUT" }, { isOut: true });
  assert.match(note!, /zeroed/);
  assert.doesNotMatch(note!, /handed to the backup/i, "no unsupported handoff claim");
  assert.doesNotMatch(note!, /backup|inherit|teammate/i, "says nothing about redistribution at all");
}

// ── A real transfer IS described, on the player who received it ────────
{
  const note = availabilityNote(
    { rule: "inherits", applied: true, from_player: "Joe Burrow", multiplier: 2.5, capped: false },
    { isOut: false },
  );
  assert.match(note!, /Inherits Joe Burrow's opportunity/);
  assert.match(note!, /×2\.50/);
  assert.match(note!, /keeps his own efficiency/, "the volume-not-production rule is stated");
  assert.doesNotMatch(note!, /capped/);

  const capped = availabilityNote(
    { rule: "inherits", applied: true, from_player: "Joe Burrow", multiplier: 4, capped: true },
    { isOut: false },
  );
  assert.match(capped!, /capped/, "a capped multiplier is disclosed, not hidden");
}

// ── A transfer that did NOT apply says so, with the reason ─────────────
{
  const note = availabilityNote(
    { rule: "inherits", applied: false, from_player: "Joe Burrow", reason: "no usable opportunity history for the transfer" },
    { isOut: false },
  );
  assert.match(note!, /no transfer was applied/);
  assert.match(note!, /no usable opportunity history/);
  assert.doesNotMatch(note!, /Inherits/, "an unapplied transfer is not described as inherited");
}

// ── The real production case: DK says out, our feed never saw it ───────
{
  const note = availabilityNote(null, { isOut: true, dkStatus: "OUT" });
  assert.match(note!, /DraftKings lists this player as OUT/);
  assert.match(note!, /zeroed/);
  assert.match(note!, /no teammate inherited his opportunity/,
    "states the absence of redistribution rather than implying it happened");

  // Status is normalised, and missing status still reads sensibly.
  assert.match(availabilityNote(null, { isOut: true, dkStatus: " ir " })!, /as IR/);
  assert.match(availabilityNote(undefined, { isOut: true })!, /as OUT/);
}

// ── A playing player with no note gets no sentence ─────────────────────
{
  assert.equal(availabilityNote(null, { isOut: false }), null);
  assert.equal(availabilityNote(undefined, { isOut: false, dkStatus: null }), null);
  assert.equal(availabilityNote({}, { isOut: false }), null, "an empty note is not a claim");
}

// ── Model evidence outranks the DK fallback ────────────────────────────
{
  // When our own feed did see him, quote our feed rather than the DK column.
  const note = availabilityNote({ rule: "zeroed", status: "IR" }, { isOut: true, dkStatus: "OUT" });
  assert.match(note!, /availability feed/);
  assert.doesNotMatch(note!, /DraftKings/);
}

console.log("nfl out projection: all assertions passed");
