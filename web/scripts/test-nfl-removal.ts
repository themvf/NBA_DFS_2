import assert from "node:assert/strict";
import {
  appearances, classifyRemoval, findAppearance, injuriesFor, injuryEvents,
  nameKeys, NEEDS_ATTENTION, teamOffensivePlays,
  type ParticipantRow, type PlayRow,
} from "../src/lib/nfl-dfs/removal";

const GAME = "2026_01_TB_ATL";
const range = (a: number, b: number) => Array.from({ length: b - a }, (_, i) => a + i);

const part = (playId: number, role: string, playerName: string,
              over: Partial<ParticipantRow> = {}): ParticipantRow =>
  ({ gameId: GAME, playId, role, playerName, team: "TB", side: "offense", ...over });

const drive = (plays: number[], passer = "B.Mayfield", receiver = "M.Evans") =>
  plays.flatMap(p => [part(p, "passer", passer), part(p, "receiver", receiver)]);

/** Plays 1-15 in Q1, 16-30 in Q2, and so on. */
const quarters = (plays: number[], game = GAME) =>
  new Map<string, number | null>(plays.map(p => [`${game}:${p}`, Math.min(4, Math.floor((p - 1) / 15) + 1)]));

const injuryPlay = (playId: number, name: string, game = GAME): PlayRow => ({
  gameId: game, playId, quarter: 2, clock: "11:42",
  description: `(11:42) Pass short right to ${name}. ${name} was injured during the play. `
    + "His return is Questionable.",
});

const tbPlays = (rows: ParticipantRow[]) => teamOffensivePlays(rows).get(`${GAME}:TB`)!;

// ── Reading the injury out of the play text ───────────────────────────
{
  const events = injuryEvents([injuryPlay(20, "M.Evans")]);
  assert.equal(events.length, 1, "one injury per play text");
  // The description names the receiver, then names him again as injured. A
  // greedy match across the sentence boundary yields "M.Evans. M.Evans",
  // which matches no player and silently drops the injury.
  assert.equal(events[0].playerName, "M.Evans");
  assert.equal(events[0].quarter, 2);
  assert.equal(events[0].clock, "11:42");

  for (const [text, expected] of [
    ["J.Smith-Schuster was injured during the play.", "J.Smith-Schuster"],
    ["Marquise Brown was injured during the play.", "Marquise Brown"],
    ["O.Beckham Jr. was injured during the play.", "O.Beckham Jr."],
  ] as const) {
    assert.equal(injuryEvents([{ ...injuryPlay(1, "X"), description: text }])[0]?.playerName,
      expected, text);
  }
  assert.deepEqual(injuryEvents([{ ...injuryPlay(1, "X"), description: "Pass deep left for 30 yards." }]), []);
  // A null description must not be scanned at all.
  assert.deepEqual(injuryEvents([{ ...injuryPlay(1, "X"), description: null }]), []);
}

// ── Denominators ──────────────────────────────────────────────────────
{
  // One snap yields a passer row AND a receiver row. Counting rows would
  // double the denominator and halve every share derived from it.
  assert.equal(tbPlays(drive(range(1, 11))).length, 10);
  const noisy = [...drive(range(1, 6)),
                 part(99, "rusher", "X", { side: "defense" }),
                 part(50, "blocker", "T1")];
  assert.deepEqual(tbPlays(noisy), [1, 2, 3, 4, 5]);
}

// ── The five availability verdicts ────────────────────────────────────
{
  // Named injured, never seen again: observed, not inferred.
  const rows = drive(range(1, 61)).filter(r => !(r.playerName === "M.Evans" && r.playId > 20));
  const index = appearances(rows, quarters(range(1, 61)));
  const p = classifyRemoval({
    position: "WR", appearance: findAppearance(index, GAME, "Mike Evans"),
    teamPlays: tbPlays(rows), injuries: injuriesFor(injuryEvents([injuryPlay(20, "M.Evans")]), GAME, "Mike Evans"),
  });
  assert.equal(p.verdict, "INJURED_OUT");
  assert.equal(p.confidence, "high");
}
{
  // A man who tweaked something and came back is, for this purpose, a man who
  // played. We remove the unavailable, not the uncomfortable.
  const rows = drive(range(1, 61));
  const index = appearances(rows, quarters(range(1, 61)));
  assert.equal(classifyRemoval({
    position: "WR", appearance: findAppearance(index, GAME, "Mike Evans"),
    teamPlays: tbPlays(rows), injuries: injuriesFor(injuryEvents([injuryPlay(20, "M.Evans")]), GAME, "Mike Evans"),
  }).verdict, "INJURED_RETURNED");
}
{
  // Presence is the strong test, and unlike absence it works at receiver.
  const rows = drive(range(1, 61));
  const p = classifyRemoval({
    position: "WR", appearance: findAppearance(appearances(rows, quarters(range(1, 61))), GAME, "Mike Evans"),
    teamPlays: tbPlays(rows),
  });
  assert.equal(p.verdict, "PLAYED_LATE");
  assert.equal(p.confidence, "high");
}
{
  // Early exit with no injury note stays ambiguous -- and at receiver it must
  // never reach a confident verdict, because rows record touches not snaps.
  const rows = drive(range(1, 61)).filter(r => !(r.playerName === "M.Evans" && r.playId > 20));
  const p = classifyRemoval({
    position: "WR", appearance: findAppearance(appearances(rows, quarters(range(1, 61))), GAME, "Mike Evans"),
    teamPlays: tbPlays(rows),
  });
  assert.equal(p.verdict, "LAST_SEEN_EARLY");
  assert.equal(p.confidence, "low");
  assert.match(p.reason, /touches rather than snaps/);
}
{
  // The same absence carries more weight at quarterback.
  const rows = [...drive(range(1, 61)).filter(r => !(r.playerName === "B.Mayfield" && r.playId > 20)),
                ...range(21, 61).map(p => part(p, "passer", "K.Trask"))];
  const p = classifyRemoval({
    position: "QB", appearance: findAppearance(appearances(rows, quarters(range(1, 61))), GAME, "Baker Mayfield"),
    teamPlays: tbPlays(rows),
  });
  assert.equal(p.verdict, "LAST_SEEN_EARLY");
  assert.equal(p.confidence, "medium");
}
{
  // A last touch near the whistle is the game ending, not an exit.
  const rows = drive(range(1, 61)).filter(r => !(r.playerName === "M.Evans" && r.playId > 55));
  const index = appearances(rows, new Map());  // no quarters: the plays-after rule decides
  const p = classifyRemoval({
    position: "WR", appearance: findAppearance(index, GAME, "Mike Evans"), teamPlays: tbPlays(rows),
  });
  assert.equal(p.verdict, "PLAYED_LATE");
  assert.equal(p.confidence, "medium");
}
{
  const rows = drive(range(1, 61));
  assert.equal(classifyRemoval({ position: "WR", appearance: null, teamPlays: tbPlays(rows) }).verdict,
    "NO_OPPORTUNITY");
}

// ── Only the actionable verdicts draw the eye ─────────────────────────
{
  assert.ok(NEEDS_ATTENTION.has("INJURED_OUT") && NEEDS_ATTENTION.has("LAST_SEEN_EARLY"));
  for (const v of ["PLAYED_LATE", "INJURED_RETURNED", "NO_OPPORTUNITY"] as const) {
    assert.ok(!NEEDS_ATTENTION.has(v), `${v} must not mark the delta cell`);
  }
}

// ── Identity ──────────────────────────────────────────────────────────
{
  // The report card says "Colston Loveland"; play-by-play says "C.Loveland".
  // A missed join looks exactly like a player who never touched the ball.
  const index = appearances(range(1, 6).map(p => part(p, "receiver", "C.Loveland")), new Map());
  assert.ok(findAppearance(index, GAME, "Colston Loveland"), "initial-and-surname match");
  assert.ok(findAppearance(index, GAME, "C.Loveland"), "exact match");
  assert.equal(findAppearance(index, GAME, "Rome Odunze"), null, "no false match");
  assert.ok(nameKeys("Colston Loveland").includes("cloveland"));
}
{
  // Two games in a week can carry the same surname. A cross-game merge would
  // invent a late appearance that never happened -- precisely the error that
  // would clear an injured player.
  const rows = [...range(1, 6).map(p => part(p, "receiver", "M.Evans", { gameId: "A" })),
                ...range(40, 61).map(p => part(p, "receiver", "M.Evans", { gameId: "B" }))];
  const index = appearances(rows, new Map());
  assert.equal(findAppearance(index, "A", "M.Evans")!.lastPlay, 5);
  assert.equal(findAppearance(index, "B", "M.Evans")!.lastPlay, 60);
  // And an injury in another game is never attached to this player.
  const events = injuryEvents([injuryPlay(20, "M.Evans", "B")]);
  assert.deepEqual(injuriesFor(events, "A", "M.Evans"), []);
  assert.equal(injuriesFor(events, "B", "Mike Evans").length, 1);
}

console.log("nfl removal: all assertions passed");
