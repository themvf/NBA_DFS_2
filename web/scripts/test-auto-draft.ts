import assert from "node:assert/strict";
import { buildSnakeSlots } from "../src/lib/fantasy-football/draft-engine";
import { calculateRosterSize, ROSTER_PRESETS, type RosterConfig } from "../src/lib/fantasy-football/league-config";
import {
  AUTO_DRAFT_POSITIONS,
  AUTO_DRAFT_VERSION,
  selectComputerPick,
  type AutoDraftPlayer,
} from "../src/lib/fantasy-football/auto-draft";

const ROSTER: RosterConfig = { QB: 1, RB: 2, WR: 2, TE: 1, FLEX: 1, K: 1, DST: 1, BN: 6 };
let nextId = 1;

function player(position: string, rank: number, overrides: Partial<AutoDraftPlayer> = {}): AutoDraftPlayer {
  const playerId = nextId++;
  return {
    playerId,
    name: `${position} ${playerId}`,
    position,
    adp: rank,
    ecr: rank,
    ourRank: rank,
    projectedPoints: 300 - rank,
    ...overrides,
  };
}

function context(availablePlayers: AutoDraftPlayer[], roster: AutoDraftPlayer[] = [], overrides = {}) {
  return {
    availablePlayers,
    roster,
    rosterConfig: ROSTER,
    teamCount: 4,
    teamSlot: 2,
    overallPick: roster.length * 4 + 2,
    seed: "league-seed",
    ...overrides,
  };
}

assert.equal(AUTO_DRAFT_VERSION, "cpu-auto-draft-v1");
// Same seed/team/pick is exactly deterministic, while seeds can break a close tie differently.
const tied = [
  player("RB", 10, { adp: null, ecr: 10 }),
  player("WR", 10, { adp: null, ecr: 10 }),
  player("TE", 10, { adp: null, ecr: 10 }),
];
const deterministicA = selectComputerPick(context(tied));
const deterministicB = selectComputerPick(context(tied));
assert.deepEqual(deterministicA, deterministicB);
const variedIds = new Set(Array.from({ length: 40 }, (_, seed) =>
  selectComputerPick(context(tied, [], { seed }))?.player.playerId,
));
assert.ok(variedIds.size > 1, "different seeds should be able to vary otherwise-equal picks");

// Roster-adjusted ADP is primary, with ECR, our rank, then projection fallbacks.
assert.equal(selectComputerPick(context([player("RB", 20)]))?.rankingSource, "adjusted-adp");
assert.equal(selectComputerPick(context([player("RB", 20, { adp: null, ecr: 7 })]))?.rankingSource, "ecr");
assert.equal(selectComputerPick(context([player("RB", 20, { adp: null, ecr: null, ourRank: 8 })]))?.rankingSource, "our-rank");
assert.equal(selectComputerPick(context([player("RB", 20, { adp: null, ecr: null, ourRank: null })]))?.rankingSource, "projection");

// K/DST cannot jump a normal early-round candidate, even with implausibly strong ranks.
const early = selectComputerPick(context([
  player("K", 1), player("DST", 2), player("RB", 45),
], [], { overallPick: 1 }));
assert.equal(early?.player.position, "RB");
// Suppression is not illegality: a specialist-only legal pool still yields a pick.
assert.equal(selectComputerPick(context([player("K", 1)], [], { overallPick: 1 }))?.player.position, "K");

// With exactly two roster picks and only K/DST requirements missing, feasibility forces a specialist.
const almostFull = [
  ...Array.from({ length: 3 }, () => player("QB", 100)),
  ...Array.from({ length: 4 }, () => player("RB", 100)),
  ...Array.from({ length: 4 }, () => player("WR", 100)),
  ...Array.from({ length: 2 }, () => player("TE", 100)),
];
const forced = selectComputerPick(context([player("RB", 1), player("K", 180), player("DST", 181)], almostFull));
assert.ok(forced && ["K", "DST"].includes(forced.player.position));
assert.ok(forced.reasons.includes("required-slot feasibility"));
// Direct starter need and FLEX fit outweigh modest best-player value gaps.
const qbRoster = [player("QB", 1)];
const needPick = selectComputerPick(context([player("QB", 5), player("RB", 25)], qbRoster));
assert.equal(needPick?.player.position, "RB");
assert.ok(needPick?.reasons.includes("RB starter need"));

// Unsupported positions are never returned, and null is reserved for an empty/legal-free pool.
const supported = player("WR", 50);
assert.equal(selectComputerPick(context([player("P", 1), supported]))?.player.playerId, supported.playerId);
assert.equal(selectComputerPick(context([])), null);
assert.equal(selectComputerPick(context([player("P", 1)])), null);
const cappedK = player("K", 10);
assert.equal(selectComputerPick(context([player("K", 1)], [cappedK])), null);

function requiredSlotsFilled(roster: AutoDraftPlayer[], config: RosterConfig): boolean {
  const counts = Object.fromEntries(AUTO_DRAFT_POSITIONS.map((position) => [position, 0])) as Record<string, number>;
  for (const drafted of roster) counts[drafted.position] = (counts[drafted.position] ?? 0) + 1;
  if (counts.QB < config.QB || counts.RB < config.RB || counts.WR < config.WR
      || counts.TE < config.TE || counts.K < config.K || counts.DST < config.DST) return false;
  const flexBodies = Math.max(0, counts.RB - config.RB)
    + Math.max(0, counts.WR - config.WR)
    + Math.max(0, counts.TE - config.TE);
  return flexBodies >= config.FLEX;
}

function playersAt(position: string, count: number, rankStart: number): AutoDraftPlayer[] {
  return Array.from({ length: count }, (_, index) => player(position, rankStart + index * 4));
}

// Full seeded multi-team snake: every overall pick is unique and every CPU roster is feasible.
const teamCount = 4;
const rounds = calculateRosterSize(ROSTER);
const pool = [
  ...playersAt("QB", 12, 28),
  ...playersAt("RB", 24, 1),
  ...playersAt("WR", 24, 2),
  ...playersAt("TE", 12, 35),
  ...playersAt("K", 4, 170),
  ...playersAt("DST", 4, 174),
  player("P", 1),
];
const rosters = new Map(Array.from({ length: teamCount }, (_, index) => [index + 1, [] as AutoDraftPlayer[]]));
const draftedIds = new Set<number | string>();
for (const slot of buildSnakeSlots(teamCount, rounds)) {
  const roster = rosters.get(slot.teamSlot)!;
  const result = selectComputerPick({
    availablePlayers: pool,
    roster,
    rosterConfig: ROSTER,
    teamCount,
    teamSlot: slot.teamSlot,
    overallPick: slot.overallPick,
    seed: "full-snake-2026",
    draftedPlayerIds: [...draftedIds],
  });
  assert.ok(result, `team ${slot.teamSlot} must have a legal pick at ${slot.overallPick}`);
  assert.ok(!draftedIds.has(result!.player.playerId), "a player may only be selected once");
  assert.ok(AUTO_DRAFT_POSITIONS.includes(result!.player.position as never));
  draftedIds.add(result!.player.playerId);
  roster.push(result!.player);
}
assert.equal(draftedIds.size, teamCount * rounds);
for (const [teamSlot, roster] of rosters) {
  assert.equal(roster.length, rounds);
  assert.ok(requiredSlotsFilled(roster, ROSTER), `team ${teamSlot} must fill every configured starter/FLEX slot`);
  assert.ok(roster.filter((drafted) => drafted.position === "K").length <= ROSTER.K);
  assert.ok(roster.filter((drafted) => drafted.position === "DST").length <= ROSTER.DST);
}

// Every setup exposed by the UI must complete for every supported league size.
for (const [presetKey, preset] of Object.entries(ROSTER_PRESETS)) {
  const config: RosterConfig = { ...preset.config };
  for (let scenarioTeams = 8; scenarioTeams <= 14; scenarioTeams += 1) {
    const scenarioRounds = calculateRosterSize(config);
    const scenarioPool = [
      ...playersAt("QB", scenarioTeams * 3, 28),
      ...playersAt("RB", scenarioTeams * 9, 1),
      ...playersAt("WR", scenarioTeams * 10, 2),
      ...playersAt("TE", scenarioTeams * 4, 35),
      ...playersAt("K", scenarioTeams, 220),
      ...playersAt("DST", scenarioTeams, 230),
    ];
    const scenarioRosters = new Map(
      Array.from({ length: scenarioTeams }, (_, index) => [index + 1, [] as AutoDraftPlayer[]]),
    );
    const scenarioDrafted = new Set<number | string>();
    for (const slot of buildSnakeSlots(scenarioTeams, scenarioRounds)) {
      const roster = scenarioRosters.get(slot.teamSlot)!;
      const result = selectComputerPick({
        availablePlayers: scenarioPool,
        roster,
        rosterConfig: config,
        teamCount: scenarioTeams,
        teamSlot: slot.teamSlot,
        overallPick: slot.overallPick,
        seed: `${presetKey}-${scenarioTeams}`,
        draftedPlayerIds: [...scenarioDrafted],
      });
      assert.ok(result, `${presetKey}/${scenarioTeams} must select at pick ${slot.overallPick}`);
      assert.ok(!scenarioDrafted.has(result!.player.playerId));
      scenarioDrafted.add(result!.player.playerId);
      roster.push(result!.player);
    }
    assert.equal(scenarioDrafted.size, scenarioTeams * scenarioRounds);
    for (const roster of scenarioRosters.values()) {
      assert.ok(requiredSlotsFilled(roster, config), `${presetKey}/${scenarioTeams} must fill required slots`);
      assert.equal(roster.filter((drafted) => drafted.position === "K").length, config.K);
      assert.equal(roster.filter((drafted) => drafted.position === "DST").length, config.DST);
    }
  }
}

console.log("auto-draft tests passed");