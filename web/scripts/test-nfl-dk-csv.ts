import assert from "node:assert/strict";
import {
  canonicalNflTeam,
  dkIdForSlot,
  NflDkCsvError,
  parseNflDkSalaryCsv,
  playablePlayers,
  salaryForSlot,
  SHOWDOWN_CAPTAIN_MULTIPLIER,
} from "../src/lib/nfl-dfs/dk-salary-csv";

// Column layout taken verbatim from a real DK Week 1 export, including the
// trailing `Status` column and the UTF-8 BOM DK writes before `Position`.
const HEADER =
  "﻿Position,Name + ID,Name,ID,Roster Position,Salary,Game Info,TeamAbbrev,AvgPointsPerGame,Status";

function classicCsv(rows: string[]): string {
  return [HEADER, ...rows].join("\n");
}

// ── Classic ────────────────────────────────────────────────────────────

{
  const csv = classicCsv([
    `QB,Patrick Mahomes (12345),Patrick Mahomes,12345,QB,7800,KC@BUF 09/07/2026 08:20PM ET,KC,21.4`,
    `RB,James Cook (12346),James Cook,12346,RB/FLEX,6900,KC@BUF 09/07/2026 08:20PM ET,BUF,17.2`,
    `WR,CeeDee Lamb (12347),CeeDee Lamb,12347,WR/FLEX,8200,DAL@PHI 09/07/2026 04:25PM ET,DAL,19.8`,
    `DST,Chiefs (12348),Chiefs,12348,DST,3400,KC@BUF 09/07/2026 08:20PM ET,KC,7.9`,
  ]);
  const slate = parseNflDkSalaryCsv(csv);

  assert.equal(slate.format, "classic");
  assert.equal(slate.players.length, 4);
  assert.deepEqual(slate.games, ["DAL@PHI", "KC@BUF"]);
  assert.deepEqual(slate.teams, ["BUF", "DAL", "KC"]);

  const mahomes = slate.players.find((p) => p.name === "Patrick Mahomes")!;
  assert.equal(mahomes.position, "QB");
  assert.equal(mahomes.salary, 7800);
  assert.equal(mahomes.avgFptsDk, 21.4);
  assert.equal(mahomes.gameKey, "KC@BUF");
  assert.equal(mahomes.opponent, "BUF");
  assert.equal(mahomes.homeAway, "away", "KC is the away side of KC@BUF");
  assert.equal(mahomes.captain, null, "Classic rows carry no captain purchase");

  const cook = slate.players.find((p) => p.name === "James Cook")!;
  assert.deepEqual(cook.rosterPositions, ["RB", "FLEX"]);
  assert.equal(cook.homeAway, "home");
  assert.equal(cook.opponent, "KC");

  // Position comes from `Position`, never from `Roster Position` -- the two
  // diverge entirely on Showdown and would mislabel every player there.
  assert.equal(slate.players.find((p) => p.name === "Chiefs")!.position, "DST");
}

// A Classic pool confined to one game cannot produce a DK-legal lineup,
// because DK requires 2+ games. Say so rather than let the optimizer
// return an unexplained empty result.
{
  const slate = parseNflDkSalaryCsv(
    classicCsv([
      `QB,A B (1),A B,1,QB,7000,KC@BUF 09/07/2026 08:20PM ET,KC,20`,
      `RB,C D (2),C D,2,RB/FLEX,6000,KC@BUF 09/07/2026 08:20PM ET,BUF,15`,
    ]),
  );
  assert.equal(slate.games.length, 1);
  assert.ok(
    slate.warnings.some((w) => w.includes("at least 2 different games")),
    "single-game Classic pool must warn",
  );
}

// ── Showdown: the duplicate-row trap ───────────────────────────────────

{
  const csv = classicCsv([
    `QB,Patrick Mahomes (900),Patrick Mahomes,900,CPT,17400,KC@BUF 09/07/2026 08:20PM ET,KC,21.4`,
    `QB,Patrick Mahomes (901),Patrick Mahomes,901,FLEX,11600,KC@BUF 09/07/2026 08:20PM ET,KC,21.4`,
    `WR,Khalil Shakir (902),Khalil Shakir,902,CPT,9600,KC@BUF 09/07/2026 08:20PM ET,BUF,12.1`,
    `WR,Khalil Shakir (903),Khalil Shakir,903,FLEX,6400,KC@BUF 09/07/2026 08:20PM ET,BUF,12.1`,
  ]);
  const slate = parseNflDkSalaryCsv(csv);

  assert.equal(slate.format, "showdown", "a CPT roster position identifies a Showdown file");
  assert.equal(slate.players.length, 2, "CPT and FLEX rows collapse into one player each");
  assert.deepEqual(slate.teams, ["BUF", "KC"]);
  assert.equal(slate.games.length, 1);

  const mahomes = slate.players.find((p) => p.name === "Patrick Mahomes")!;
  // Canonical identity is the FLEX row; the CPT purchase hangs off it.
  assert.equal(mahomes.dkPlayerId, 901);
  assert.equal(mahomes.salary, 11600);
  assert.equal(mahomes.captain?.dkPlayerId, 900);
  assert.equal(mahomes.captain?.salary, 17400);

  // Slot pricing/identity must use DK's own CPT row, not a derived value:
  // DK rounds, so a computed 1.5x can disagree with what DK charges.
  assert.equal(salaryForSlot(mahomes, "FLEX"), 11600);
  assert.equal(salaryForSlot(mahomes, "CPT"), 17400);
  assert.equal(dkIdForSlot(mahomes, "FLEX"), 901);
  assert.equal(dkIdForSlot(mahomes, "CPT"), 900);

  // Position survives the collapse even though Roster Position said CPT/FLEX.
  assert.equal(mahomes.position, "QB");
  assert.equal(slate.players.find((p) => p.name === "Khalil Shakir")!.position, "WR");
}

// A FLEX row with no CPT partner is usable, but only in FLEX -- warn.
{
  const slate = parseNflDkSalaryCsv(
    classicCsv([
      `QB,A B (1),A B,1,CPT,15000,KC@BUF 09/07/2026 08:20PM ET,KC,20`,
      `QB,A B (2),A B,2,FLEX,10000,KC@BUF 09/07/2026 08:20PM ET,KC,20`,
      `WR,C D (3),C D,3,FLEX,5000,KC@BUF 09/07/2026 08:20PM ET,BUF,10`,
    ]),
  );
  const cd = slate.players.find((p) => p.name === "C D")!;
  assert.equal(cd.captain, null);
  assert.equal(salaryForSlot(cd, "CPT"), Math.round(5000 * SHOWDOWN_CAPTAIN_MULTIPLIER));
  assert.ok(slate.warnings.some((w) => w.includes("no CPT row")));
}

// A CPT row with no FLEX partner is still a real player -- keep him rather
// than dropping him silently, and back out the FLEX-equivalent salary.
{
  const slate = parseNflDkSalaryCsv(
    classicCsv([
      `QB,A B (1),A B,1,CPT,15000,KC@BUF 09/07/2026 08:20PM ET,KC,20`,
      `WR,C D (3),C D,3,FLEX,5000,KC@BUF 09/07/2026 08:20PM ET,BUF,10`,
      `WR,E F (4),E F,4,CPT,9000,KC@BUF 09/07/2026 08:20PM ET,BUF,12`,
    ]),
  );
  const ab = slate.players.find((p) => p.name === "A B")!;
  assert.equal(ab.captain?.dkPlayerId, 1);
  assert.equal(ab.salary, 10000, "15000 CPT implies a 10000 FLEX-equivalent");
  assert.ok(slate.warnings.some((w) => w.includes("no FLEX row")));
}

// ── Quoting, which the NBA/MLB parser does not handle ──────────────────

{
  // A quoted Game Info containing a comma would shift every later column
  // under a naive `line.split(",")`.
  const slate = parseNflDkSalaryCsv(
    classicCsv([
      `DST,Football Team (7),"Washington, DC",7,DST,3000,"KC@BUF 09/07/2026, 08:20PM ET",WAS,8.0`,
      `QB,A B (8),A B,8,QB,7000,DAL@PHI 09/07/2026 04:25PM ET,DAL,20`,
    ]),
  );
  const dst = slate.players.find((p) => p.dkPlayerId === 7)!;
  assert.equal(dst.name, "Washington, DC", "quoted comma inside Name is preserved");
  assert.equal(dst.salary, 3000, "columns after a quoted comma stay aligned");
  assert.equal(dst.teamAbbrev, "WAS");
  assert.equal(dst.position, "DST");
}

// ── Team abbreviation normalization ────────────────────────────────────

{
  assert.equal(canonicalNflTeam("JAC"), "JAX");
  assert.equal(canonicalNflTeam("AZ"), "ARI");
  assert.equal(canonicalNflTeam("kc"), "KC");

  const slate = parseNflDkSalaryCsv(
    classicCsv([
      `QB,A B (1),A B,1,QB,7000,JAC@AZ 09/07/2026 04:25PM ET,JAC,20`,
      `RB,C D (2),C D,2,RB/FLEX,6000,DAL@PHI 09/07/2026 04:25PM ET,DAL,15`,
    ]),
  );
  const ab = slate.players.find((p) => p.dkPlayerId === 1)!;
  assert.equal(ab.teamAbbrev, "JAX");
  assert.equal(ab.gameKey, "JAX@ARI", "game key is normalized on both sides");
  assert.equal(ab.opponent, "ARI");
}

// ── Row-level problems warn rather than throw ──────────────────────────

{
  const slate = parseNflDkSalaryCsv(
    classicCsv([
      `QB,A B (1),A B,1,QB,7000,KC@BUF 09/07/2026 08:20PM ET,KC,20`,
      `RB,C D (2),C D,2,RB/FLEX,0,DAL@PHI 09/07/2026 04:25PM ET,DAL,15`,
      `WR,E F (1),E F,1,WR/FLEX,5000,DAL@PHI 09/07/2026 04:25PM ET,PHI,11`,
      `XX,G H (4),G H,4,XX,5000,DAL@PHI 09/07/2026 04:25PM ET,PHI,11`,
    ]),
  );
  assert.equal(slate.players.length, 1, "zero-salary, duplicate-ID and unknown-position rows are dropped");
  assert.ok(slate.warnings.some((w) => w.includes("zero salary")));
  assert.ok(slate.warnings.some((w) => w.includes("Duplicate DK ID")));
  assert.ok(slate.warnings.some((w) => w.includes("Unrecognized position")));
}

// A blank AvgPointsPerGame is unknown; an explicit 0.0 is a real DK value
// for a player with no history. They must not collapse to the same thing.
{
  const slate = parseNflDkSalaryCsv(
    classicCsv([
      `QB,A B (1),A B,1,QB,7000,KC@BUF 09/07/2026 08:20PM ET,KC,`,
      `RB,C D (2),C D,2,RB/FLEX,6000,DAL@PHI 09/07/2026 04:25PM ET,DAL,0.0`,
    ]),
  );
  assert.equal(slate.players.find((p) => p.dkPlayerId === 1)!.avgFptsDk, null);
  assert.equal(slate.players.find((p) => p.dkPlayerId === 2)!.avgFptsDk, 0);
}

// ── Header location ────────────────────────────────────────────────────

{
  // DK exports sometimes carry a preamble above the real header.
  const slate = parseNflDkSalaryCsv(
    [
      "NFL $500K Play-Action [$100K to 1st]",
      "",
      HEADER,
      `QB,A B (1),A B,1,QB,7000,KC@BUF 09/07/2026 08:20PM ET,KC,20`,
      `RB,C D (2),C D,2,RB/FLEX,6000,DAL@PHI 09/07/2026 04:25PM ET,DAL,15`,
    ].join("\n"),
  );
  assert.equal(slate.players.length, 2, "header is located rather than assumed to be line 0");
}

// ── DK injury status ───────────────────────────────────────────────────
//
// Real DK NFL exports carry a `Status` column (`OUT`, `IR`, `Q`, blank).
// The NBA/MLB parser assumes it does not exist and defers to LineStar; for
// NFL that dependency is unnecessary, and an IR player left in the pool
// silently wastes a roster slot.
{
  const slate = parseNflDkSalaryCsv(
    classicCsv([
      `QB,Healthy QB (1),Healthy QB,1,QB,7000,KC@BUF 09/07/2026 08:20PM ET,KC,20,`,
      `RB,Hurt RB (2),Hurt RB,2,RB/FLEX,6000,KC@BUF 09/07/2026 08:20PM ET,KC,15,OUT`,
      `WR,Shelved WR (3),Shelved WR,3,WR/FLEX,5000,DAL@PHI 09/07/2026 04:25PM ET,DAL,11,IR`,
      `TE,Maybe TE (4),Maybe TE,4,TE/FLEX,4000,DAL@PHI 09/07/2026 04:25PM ET,PHI,9,Q`,
    ]),
  );

  const byId = new Map(slate.players.map((p) => [p.dkPlayerId, p]));
  assert.equal(byId.get(1)!.status, null, "a blank Status is null, not an empty string");
  assert.equal(byId.get(1)!.isOut, false);
  assert.equal(byId.get(2)!.status, "OUT");
  assert.equal(byId.get(2)!.isOut, true);
  assert.equal(byId.get(3)!.status, "IR");
  assert.equal(byId.get(3)!.isOut, true, "IR is unrosterable, same as OUT");

  // Questionable is risk, not absence. Whether to accept it is a
  // contest-type decision the optimizer makes, so the parser keeps the
  // player and merely reports the flag.
  assert.equal(byId.get(4)!.status, "Q");
  assert.equal(byId.get(4)!.isOut, false, "Q must stay in the pool");

  assert.deepEqual(
    playablePlayers(slate.players).map((p) => p.dkPlayerId).sort(),
    [1, 4],
  );
}

// A file with no Status column at all must not crash or mark everyone out.
{
  const slate = parseNflDkSalaryCsv(
    [
      "Position,Name + ID,Name,ID,Roster Position,Salary,Game Info,TeamAbbrev,AvgPointsPerGame",
      `QB,A B (1),A B,1,QB,7000,KC@BUF 09/07/2026 08:20PM ET,KC,20`,
      `RB,C D (2),C D,2,RB/FLEX,6000,DAL@PHI 09/07/2026 04:25PM ET,DAL,15`,
    ].join("\n"),
  );
  assert.equal(slate.players.every((p) => p.status === null && !p.isOut), true);
  assert.equal(playablePlayers(slate.players).length, 2);
}

// Status survives the Showdown CPT/FLEX collapse.
{
  const slate = parseNflDkSalaryCsv(
    classicCsv([
      `RB,Zach Charbonnet (1),Zach Charbonnet,1,CPT,12300,NE@SEA 09/09/2026 08:20PM ET,SEA,11,OUT`,
      `RB,Zach Charbonnet (2),Zach Charbonnet,2,FLEX,8200,NE@SEA 09/09/2026 08:20PM ET,SEA,11,OUT`,
      `QB,Drake Maye (3),Drake Maye,3,CPT,15000,NE@SEA 09/09/2026 08:20PM ET,NE,20.9,`,
      `QB,Drake Maye (4),Drake Maye,4,FLEX,10000,NE@SEA 09/09/2026 08:20PM ET,NE,20.9,`,
    ]),
  );
  assert.equal(slate.players.length, 2);
  const charbonnet = slate.players.find((p) => p.name === "Zach Charbonnet")!;
  assert.equal(charbonnet.isOut, true, "an OUT player must not become playable via his CPT row");
  assert.equal(charbonnet.captain?.dkPlayerId, 1);
  assert.deepEqual(playablePlayers(slate.players).map((p) => p.name), ["Drake Maye"]);
}

assert.throws(() => parseNflDkSalaryCsv("not,a,dk,file\n1,2,3,4"), NflDkCsvError);
assert.throws(() => parseNflDkSalaryCsv(classicCsv([])), NflDkCsvError);

console.log("nfl dk csv: all assertions passed");
