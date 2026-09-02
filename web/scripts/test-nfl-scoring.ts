import assert from "node:assert/strict";
import {
  applyCaptainMultiplier,
  dstPointsAllowedPoints,
  scoreNflDst,
  scoreNflKicker,
  scoreNflOffense,
  scoreNflSlot,
  scoreNflStatLine,
  SHOWDOWN_CAPTAIN_MULTIPLIER,
} from "../src/lib/nfl-dfs/scoring";

const close = (actual: number, expected: number, msg: string) =>
  assert.ok(Math.abs(actual - expected) < 1e-9, `${msg}: got ${actual}, expected ${expected}`);

// ── Offense ────────────────────────────────────────────────────────────

{
  // 300 pass yds (12) + 3 pass TD (12) - 1 INT (1) + 300-yard bonus (3)
  close(
    scoreNflOffense({ passYds: 300, passTds: 3, interceptions: 1 }),
    12 + 12 - 1 + 3,
    "QB with the 300-yard bonus",
  );

  // 100 rush yds (10) + 1 rush TD (6) + 5 rec (5) + 40 rec yds (4)
  //   + 100-rush-yard bonus (3). No receiving bonus at 40 yards.
  close(
    scoreNflOffense({ rushYds: 100, rushTds: 1, receptions: 5, recYds: 40 }),
    10 + 6 + 5 + 4 + 3,
    "RB with the rushing bonus only",
  );

  // Full PPR: every reception is a point.
  close(scoreNflOffense({ receptions: 9, recYds: 95 }), 9 + 9.5, "9 catches for 95");

  // Negative events.
  close(
    scoreNflOffense({ rushYds: 20, fumblesLost: 1, interceptions: 2 }),
    2 - 1 - 2,
    "fumble lost and interceptions are negative",
  );

  // Every +6 category and the 2pt conversion.
  close(
    scoreNflOffense({ returnTds: 1, offensiveFumbleRecoveryTds: 1, twoPointConversions: 1 }),
    6 + 6 + 2,
    "return TD, offensive fumble recovery TD, 2pt conversion",
  );
}

// Bonuses are thresholds, inclusive at the stated number.
{
  close(scoreNflOffense({ passYds: 299 }), 299 * 0.04, "299 pass yards earns no bonus");
  close(scoreNflOffense({ passYds: 300 }), 300 * 0.04 + 3, "300 pass yards earns the bonus");
  close(scoreNflOffense({ rushYds: 99 }), 9.9, "99 rush yards earns no bonus");
  close(scoreNflOffense({ rushYds: 100 }), 10 + 3, "100 rush yards earns the bonus");
  close(scoreNflOffense({ recYds: 99 }), 9.9, "99 receiving yards earns no bonus");
  close(scoreNflOffense({ recYds: 100 }), 10 + 3, "100 receiving yards earns the bonus");

  // Both rushing and receiving bonuses can land in the same line.
  close(scoreNflOffense({ rushYds: 100, recYds: 100 }), 10 + 10 + 3 + 3, "a player can earn both yardage bonuses");
}

// The reason the projection model simulates instead of scoring a mean:
// a step function makes E[score(X)] and score(E[X]) different numbers.
{
  const meanYds = 280;
  const scoreOfTheMean = scoreNflOffense({ passYds: meanYds });
  // Two equally likely outcomes with the same mean, one clearing 300.
  const meanOfTheScores =
    (scoreNflOffense({ passYds: 200 }) + scoreNflOffense({ passYds: 360 })) / 2;
  close(scoreOfTheMean, 11.2, "scoring the mean misses the bonus entirely");
  close(meanOfTheScores, 11.2 + 1.5, "averaging the scores captures half a bonus");
  assert.notEqual(
    scoreOfTheMean,
    meanOfTheScores,
    "score(E[X]) must not equal E[score(X)] across a bonus threshold",
  );
}

// ── Kicker ─────────────────────────────────────────────────────────────

{
  close(
    scoreNflKicker({ extraPointsMade: 2, fgMade0to39: 1, fgMade40to49: 1, fgMade50Plus: 1 }),
    2 + 3 + 4 + 5,
    "kicker distance tiers",
  );
  close(scoreNflKicker({}), 0, "an empty kicker line scores zero");
}

// ── DST ────────────────────────────────────────────────────────────────

{
  // Every points-allowed boundary, both edges of each tier.
  const tiers: Array<[number, number]> = [
    [0, 10],
    [1, 7], [6, 7],
    [7, 4], [13, 4],
    [14, 1], [20, 1],
    [21, 0], [27, 0],
    [28, -1], [34, -1],
    [35, -4], [100, -4],
  ];
  for (const [pa, expected] of tiers) {
    close(dstPointsAllowedPoints(pa), expected, `points allowed ${pa}`);
  }

  // 3 sacks (3) + 2 INT (4) + 1 fumble rec (2) + 1 safety (2)
  //   + 1 blocked kick (2) + 1 TD (6) + 10 points allowed (4)
  close(
    scoreNflDst({
      sacks: 3,
      dstInterceptions: 2,
      fumbleRecoveries: 1,
      safeties: 1,
      blockedKicks: 1,
      dstTds: 1,
      twoPointReturns: 0,
      pointsAllowed: 10,
    }),
    3 + 4 + 2 + 2 + 2 + 6 + 4,
    "full DST line",
  );

  // An explicit shutout with no other production scores the +10 tier.
  close(scoreNflDst({ pointsAllowed: 0 }), 10, "an explicit shutout scores +10");

  // An OMITTED pointsAllowed must contribute nothing, not a shutout.
  // Defaulting the field to 0 would hand the single most valuable
  // outcome in the DST table to any line that forgot to set it, which
  // would inflate every partial DST projection in the same direction.
  close(scoreNflDst({}), 0, "an empty DST line scores zero, not a shutout");
  close(
    scoreNflDst({ sacks: 3 }),
    3,
    "a partial DST line scores only what it states",
  );
  assert.notEqual(
    scoreNflDst({}),
    scoreNflDst({ pointsAllowed: 0 }),
    "omitted and zero points allowed are different claims",
  );
}

// ── Position gating ────────────────────────────────────────────────────

{
  // DK: "Kickers are only eligible for extra points and field goals made.
  // Non-kickers are not eligible for these scoring categories."
  const kickingLine = { fgMade50Plus: 2, extraPointsMade: 3 };
  close(scoreNflStatLine("K", kickingLine), 10 + 3, "a kicker scores his kicking");
  close(scoreNflStatLine("WR", kickingLine), 0, "a non-kicker scores nothing for kicking");

  const rushingLine = { rushYds: 100, rushTds: 1 };
  close(scoreNflStatLine("RB", rushingLine), 10 + 6 + 3, "an RB scores his rushing");
  close(scoreNflStatLine("K", rushingLine), 0, "a kicker scores nothing for rushing");

  // A DST is scored on the defensive schedule, never the offensive one.
  close(scoreNflStatLine("DST", { sacks: 4, pointsAllowed: 17 }), 4 + 1, "DST uses the defensive schedule");
  close(scoreNflStatLine("QB", { sacks: 4, pointsAllowed: 17 }), 0, "a QB earns nothing from DST stats");
}

// ── Showdown captain ───────────────────────────────────────────────────

{
  const line = { passYds: 300, passTds: 3, interceptions: 1 };
  const base = scoreNflOffense(line);
  close(base, 26, "base QB line");
  close(applyCaptainMultiplier(base), 39, "captain multiplies the total by 1.5");
  assert.equal(SHOWDOWN_CAPTAIN_MULTIPLIER, 1.5);

  close(scoreNflSlot("QB", line, "CPT"), 39, "CPT slot applies the multiplier");
  close(scoreNflSlot("QB", line, "FLEX"), 26, "FLEX slot does not");
  close(scoreNflSlot("QB", line, "CLASSIC"), 26, "Classic slots do not");

  // DK multiplies "each statistic" by 1.5. Because every term including
  // the bonuses is linear in its statistic, multiplying the total is
  // equivalent -- assert that rather than assume it.
  const perStat =
    300 * 0.04 * 1.5 + 3 * 4 * 1.5 + 1 * -1 * 1.5 + 3 * 1.5;
  close(applyCaptainMultiplier(base), perStat, "total-multiply equals per-statistic multiply");

  // A DST or kicker may be captained in Showdown.
  close(scoreNflSlot("DST", { pointsAllowed: 0 }, "CPT"), 15, "a DST can be captain");
  close(scoreNflSlot("K", { fgMade50Plus: 1 }, "CPT"), 7.5, "a kicker can be captain");
}

console.log("nfl scoring: all assertions passed");
