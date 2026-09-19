import assert from "node:assert/strict";
import {
  ALL_VERDICTS, ATTENTION_VERDICTS, buildAvailability, countByVerdict, sortAvailability,
} from "../src/lib/nfl-dfs/availability-board";
import type { ParticipantRow, PlayRow, Proposal, Verdict } from "../src/lib/nfl-dfs/removal";
import type { ReportRow } from "../src/lib/nfl-dfs/report-card";

const GAME = "2026_01_TB_ATL";
const range = (a: number, b: number) => Array.from({ length: b - a }, (_, i) => a + i);

let nextId = 1;
const row = (over: Partial<ReportRow> & { name: string; position: string }): ReportRow => ({
  player_id: nextId++, team: "TB", opponent: "ATL", game_id: 1,
  actual: null, error: null, ...over,
} as ReportRow);

const part = (playId: number, role: string, playerName: string,
              over: Partial<ParticipantRow> = {}): ParticipantRow =>
  ({ gameId: GAME, playId, role, playerName, team: "TB", side: "offense", ...over });

const drive = (plays: number[], passer = "B.Mayfield", receiver = "M.Evans") =>
  plays.flatMap(p => [part(p, "passer", passer), part(p, "receiver", receiver)]);

const context = (plays: number[]): PlayRow[] => plays.map(p => ({
  gameId: GAME, playId: p, quarter: Math.min(4, Math.floor((p - 1) / 15) + 1),
  clock: null, description: null,
}));

const injuryAt = (playId: number, name: string): PlayRow => ({
  gameId: GAME, playId, quarter: 2, clock: "11:42",
  description: `${name} was injured during the play. His return is Questionable.`,
});

// ── Missing play-by-play is not a finding ─────────────────────────────
{
  // Null, not an empty list: "we have no play-by-play" and "nobody left the
  // game" are different statements, and only one of them is a finding.
  const rows = [row({ name: "Mike Evans", position: "WR" })];
  assert.equal(buildAvailability(rows, null, context(range(1, 10))), null);
  assert.equal(buildAvailability(rows, drive(range(1, 10)), null), null);
}

// ── End to end, from report rows to verdicts ──────────────────────────
{
  const plays = range(1, 61);
  const participants = [
    ...drive(plays).filter(r => !(r.playerName === "M.Evans" && r.playId > 20)),
    ...plays.map(p => part(p, "rusher", "R.White")),
  ];
  const rows = [
    row({ name: "Mike Evans", position: "WR", actual: 2, error: -14 }),
    row({ name: "Rachaad White", position: "RB", actual: 9, error: -3 }),
    row({ name: "Baker Mayfield", position: "QB", actual: 18, error: -2 }),
    row({ name: "Ghost Player", position: "TE", actual: 0, error: -8 }),
  ];
  const entries = buildAvailability(rows, participants, [...context(plays), injuryAt(20, "M.Evans")])!;
  const verdict = (name: string) => entries.find(e => e.row.name === name)!.proposal.verdict;

  assert.equal(verdict("Mike Evans"), "INJURED_OUT", "named injured, never seen again");
  assert.equal(verdict("Rachaad White"), "PLAYED_LATE", "carrying it in Q4");
  assert.equal(verdict("Baker Mayfield"), "PLAYED_LATE");
  // A player absent from play-by-play must never read as removed.
  assert.equal(verdict("Ghost Player"), "NO_OPPORTUNITY");

  // Counts describe the whole week, so the headline cannot drift with filters.
  const counts = countByVerdict(entries);
  assert.equal(counts.INJURED_OUT, 1);
  assert.equal(counts.PLAYED_LATE, 2);
  assert.equal(counts.NO_OPPORTUNITY, 1);
  assert.equal(Object.values(counts).reduce((a, b) => a + b, 0), rows.length,
    "every row lands in exactly one verdict");
}

// ── Ordering puts the short list first ────────────────────────────────
{
  // Only the verdict drives ordering, so the rest of the proposal is filler.
  const stub = (verdict: Verdict): Proposal => ({
    verdict, version: "test", confidence: "high", reason: "",
    evidence: { targets: 0, carries: 0, dropbacks: 0, plays: 0, lastPlay: null,
      lastQuarter: null, teamPlays: 0, playsAfter: null, injuries: [] },
  });
  const entries = [
    { row: row({ name: "Cleared Big Miss", position: "WR", actual: 1, error: -20 }), proposal: stub("PLAYED_LATE") },
    { row: row({ name: "Small Injury", position: "WR", actual: 8, error: -2 }), proposal: stub("INJURED_OUT") },
    { row: row({ name: "Big Injury", position: "WR", actual: 1, error: -15 }), proposal: stub("INJURED_OUT") },
    { row: row({ name: "Unscored", position: "WR" }), proposal: stub("INJURED_OUT") },
    { row: row({ name: "Ambiguous", position: "WR", actual: 4, error: -9 }), proposal: stub("LAST_SEEN_EARLY") },
  ];

  const order = sortAvailability(entries).map(e => e.row.name);
  // Needs-a-human first, worst miss first inside a verdict, and a cleared
  // 20-point miss still sorts below an ambiguous 9-point one.
  assert.deepEqual(order,
    ["Big Injury", "Small Injury", "Unscored", "Ambiguous", "Cleared Big Miss"]);
}

// ── The default filter is the actionable one ──────────────────────────
{
  assert.deepEqual([...ATTENTION_VERDICTS], ["INJURED_OUT", "LAST_SEEN_EARLY"]);
  assert.equal(new Set(ALL_VERDICTS).size, 5, "every verdict is selectable");
  for (const v of ATTENTION_VERDICTS) assert.ok(ALL_VERDICTS.includes(v));
}

console.log("nfl availability board: all assertions passed");
