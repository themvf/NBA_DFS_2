import assert from "node:assert/strict";
import {
  appearancesByName, classifyRemoval, findAppearance, silenceShare,
  teamOffensivePlays, type ParticipantRow,
} from "../src/lib/nfl-dfs/removal";

const row = (playId: number, role: string, playerName: string,
             over: Partial<ParticipantRow> = {}): ParticipantRow =>
  ({ playId, role, playerName, team: "CHI", side: "offense", ...over });

const drive = (plays: number[], passer = "QB1", receiver = "WR1") =>
  plays.flatMap(p => [row(p, "passer", passer), row(p, "receiver", receiver)]);

const range = (a: number, b: number) => Array.from({ length: b - a }, (_, i) => a + i);

// ── Denominators ──────────────────────────────────────────────────────
{
  // One snap yields a passer row AND a receiver row. Counting rows would
  // double the denominator and halve every share derived from it.
  assert.equal(teamOffensivePlays(drive(range(1, 11))).get("CHI")!.length, 10);
  const withDefense = [...drive(range(1, 6)), row(99, "rusher", "X", { side: "defense" })];
  assert.deepEqual(teamOffensivePlays(withDefense).get("CHI"), [1, 2, 3, 4, 5]);
  // Non-ball roles are excluded so this matches model/nfl_dfs_removal.py's
  // definition exactly. If these two ever diverge, the CI probe and this
  // panel can propose different verdicts about the same player.
  const withBlocker = [...drive(range(1, 6)), row(50, "blocker", "T1")];
  assert.deepEqual(teamOffensivePlays(withBlocker).get("CHI"), [1, 2, 3, 4, 5]);
}

// ── Quarterback removal ───────────────────────────────────────────────
{
  const rows = [...drive(range(1, 41)).filter(r => !(r.role === "passer" && r.playId > 20)),
                ...range(21, 41).map(p => row(p, "passer", "QB2"))];
  const proposal = classifyRemoval({
    position: "QB",
    appearance: findAppearance(appearancesByName(rows), "QB1"),
    teamPlays: teamOffensivePlays(rows).get("CHI")!,
  });
  assert.equal(proposal.verdict, "LIKELY_REMOVED");
  assert.equal(proposal.confidence, "high");
}
{
  // A backup who only ever appeared early never carried a starter's snap
  // share, so his silence is ordinary. Calling it a removal inverts the story.
  const rows = [...range(1, 6).map(p => row(p, "passer", "QB2")),
                ...range(6, 41).map(p => row(p, "passer", "QB1")),
                ...range(1, 41).map(p => row(p, "receiver", "WR1"))];
  assert.equal(classifyRemoval({
    position: "QB", appearance: findAppearance(appearancesByName(rows), "QB2"),
    teamPlays: teamOffensivePlays(rows).get("CHI")!,
  }).verdict, "UNKNOWN");
}

// ── The Loveland case ─────────────────────────────────────────────────
{
  const rows = drive(range(1, 41));
  const proposal = classifyRemoval({
    position: "TE", appearance: findAppearance(appearancesByName(rows), "WR1"),
    teamPlays: teamOffensivePlays(rows).get("CHI")!, receptions: 0,
  });
  assert.equal(proposal.verdict, "OPPORTUNITY_NO_CONVERSION");
  assert.equal(proposal.evidence.targets, 40);
}

// ── A receiver's silence is never a removal ───────────────────────────
{
  // Participation rows record touches, not snaps: a receiver can run a full
  // second half of routes and produce no row at all.
  const rows = drive(range(1, 41)).filter(r => !(r.role === "receiver" && r.playId > 10));
  const proposal = classifyRemoval({
    position: "WR", appearance: findAppearance(appearancesByName(rows), "WR1"),
    teamPlays: teamOffensivePlays(rows).get("CHI")!,
  });
  assert.equal(proposal.verdict, "UNKNOWN");
  assert.match(proposal.reason, /touches, not snaps/);
}

// ── Absence and unknowns are distinct from zero ───────────────────────
{
  const rows = drive(range(1, 41));
  assert.equal(classifyRemoval({
    position: "WR", appearance: null, teamPlays: teamOffensivePlays(rows).get("CHI")!,
  }).verdict, "NO_OPPORTUNITY");
  // No denominator means no share -- never 0, which reads as "played to the whistle".
  assert.equal(silenceShare(findAppearance(appearancesByName(rows), "WR1")!, []), null);
}

// ── Name matching across the two sources' spellings ───────────────────
{
  // The report card says "Colston Loveland"; play-by-play says "C.Loveland".
  // A missed join looks exactly like a player who never touched the ball,
  // which is the one distinction this feature exists to make.
  const index = appearancesByName(range(1, 6).map(p => row(p, "receiver", "C.Loveland")));
  assert.ok(findAppearance(index, "Colston Loveland"), "initial-and-surname match");
  assert.ok(findAppearance(index, "C.Loveland"), "exact match");
  assert.equal(findAppearance(index, "Rome Odunze"), null, "no false match");
}

console.log("nfl removal: all assertions passed");
