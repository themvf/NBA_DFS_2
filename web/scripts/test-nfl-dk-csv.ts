import assert from "node:assert/strict";
import {
  canonicalNflTeam,
  dkIdForSlot,
  NflDkCsvError,
  parseNflDkSalaryCsv,
  salaryForSlot,
  SHOWDOWN_CAPTAIN_MULTIPLIER,
} from "../src/lib/nfl-dfs/dk-salary-csv";

const HEADER = "Position,Name + ID,Name,ID,Roster Position,Salary,Game Info,TeamAbbrev,AvgPointsPerGame";

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

assert.throws(() => parseNflDkSalaryCsv("not,a,dk,file\n1,2,3,4"), NflDkCsvError);
assert.throws(() => parseNflDkSalaryCsv(classicCsv([])), NflDkCsvError);

console.log("nfl dk csv: all assertions passed");
