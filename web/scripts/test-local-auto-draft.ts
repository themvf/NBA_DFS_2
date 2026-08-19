import assert from "node:assert/strict";
import { buildSnakeSlots } from "../src/lib/fantasy-football/draft-engine";
import type { AutoDraftPlayer } from "../src/lib/fantasy-football/auto-draft";
import { computeCpuDraftBatch, localAutoDraftSeed } from "../src/lib/fantasy-football/local-auto-draft";
import {
  BEST_BALL_AUTO_DRAFT_ROSTER_CONFIG,
  BEST_BALL_ROUNDS,
  BEST_BALL_TARGETS,
  BEST_BALL_TEAM_COUNT,
} from "../src/lib/fantasy-football/best-ball";
import {
  REDRAFT_AUTO_DRAFT_ROSTER_CONFIG,
  REDRAFT_FLEX_SLOTS,
  REDRAFT_ROUNDS,
  REDRAFT_STARTER_SLOTS,
  REDRAFT_TEAM_COUNT,
} from "../src/lib/fantasy-football/redraft";

let nextId = 1;
function player(position: string, rank: number): AutoDraftPlayer {
  const playerId = nextId++;
  return { playerId, name: `${position} ${playerId}`, position, adp: rank, ecr: rank, ourRank: rank, projectedPoints: 500 - rank };
}
function playersAt(position: string, count: number, rankStart: number): AutoDraftPlayer[] {
  return Array.from({ length: count }, (_, index) => player(position, rankStart + index * 4));
}

// No real team ever has this slot, so a batch computed with this as the
// "userSlot" never stops early -- a convenient way to drive an all-CPU draft
// in a test without simulating a human turn in between.
const NO_USER = -1;

function requiredSlotsFilled(
  roster: AutoDraftPlayer[],
  config: { QB: number; RB: number; WR: number; TE: number; K: number; DST: number; FLEX: number },
): boolean {
  const counts: Record<string, number> = { QB: 0, RB: 0, WR: 0, TE: 0, K: 0, DST: 0 };
  for (const drafted of roster) counts[drafted.position] = (counts[drafted.position] ?? 0) + 1;
  if (counts.QB < config.QB || counts.RB < config.RB || counts.WR < config.WR
    || counts.TE < config.TE || counts.K < config.K || counts.DST < config.DST) return false;
  const flexBodies = Math.max(0, counts.RB - config.RB) + Math.max(0, counts.WR - config.WR) + Math.max(0, counts.TE - config.TE);
  return flexBodies >= config.FLEX;
}

function rostersFromBatch(slots: ReturnType<typeof buildSnakeSlots>, players: AutoDraftPlayer[], playerIds: number[]) {
  const byId = new Map(players.map((candidate) => [candidate.playerId, candidate]));
  const rosters = new Map<number, AutoDraftPlayer[]>();
  playerIds.forEach((id, index) => {
    const slot = slots[index];
    const drafted = byId.get(id);
    if (!slot || !drafted) return;
    const roster = rosters.get(slot.teamSlot) ?? [];
    roster.push(drafted);
    rosters.set(slot.teamSlot, roster);
  });
  return rosters;
}

// --- Redraft: full all-CPU draft (10 teams x 15 rounds) -------------------
{
  const slots = buildSnakeSlots(REDRAFT_TEAM_COUNT, REDRAFT_ROUNDS);
  const pool = [
    ...playersAt("QB", REDRAFT_TEAM_COUNT * 3, 28),
    ...playersAt("RB", REDRAFT_TEAM_COUNT * 6, 1),
    ...playersAt("WR", REDRAFT_TEAM_COUNT * 6, 2),
    ...playersAt("TE", REDRAFT_TEAM_COUNT * 3, 35),
    ...playersAt("K", REDRAFT_TEAM_COUNT, 220),
    ...playersAt("DST", REDRAFT_TEAM_COUNT, 230),
  ];
  const batch = computeCpuDraftBatch({
    slots, players: pool, playerIds: [], userSlot: NO_USER, teamCount: REDRAFT_TEAM_COUNT,
    rosterConfig: REDRAFT_AUTO_DRAFT_ROSTER_CONFIG, seed: "redraft-full",
  });
  assert.equal(batch.length, REDRAFT_TEAM_COUNT * REDRAFT_ROUNDS, "redraft: every slot should fill");
  assert.equal(new Set(batch).size, batch.length, "redraft: no player drafted twice");
  const rosters = rostersFromBatch(slots, pool, batch);
  for (const [teamSlot, roster] of rosters) {
    assert.equal(roster.length, REDRAFT_ROUNDS, `redraft team ${teamSlot} roster size`);
    assert.ok(
      requiredSlotsFilled(roster, { ...REDRAFT_STARTER_SLOTS, FLEX: REDRAFT_FLEX_SLOTS }),
      `redraft team ${teamSlot} must fill every starter/FLEX slot`,
    );
  }

  // Determinism: identical inputs produce an identical batch.
  const batchAgain = computeCpuDraftBatch({
    slots, players: pool, playerIds: [], userSlot: NO_USER, teamCount: REDRAFT_TEAM_COUNT,
    rosterConfig: REDRAFT_AUTO_DRAFT_ROSTER_CONFIG, seed: "redraft-full",
  });
  assert.deepEqual(batch, batchAgain);
}

// --- Redraft: batch stops at the user's turn and resumes correctly --------
{
  nextId = 1;
  const slots = buildSnakeSlots(REDRAFT_TEAM_COUNT, REDRAFT_ROUNDS);
  const pool = [
    ...playersAt("QB", REDRAFT_TEAM_COUNT * 3, 28),
    ...playersAt("RB", REDRAFT_TEAM_COUNT * 6, 1),
    ...playersAt("WR", REDRAFT_TEAM_COUNT * 6, 2),
    ...playersAt("TE", REDRAFT_TEAM_COUNT * 3, 35),
    ...playersAt("K", REDRAFT_TEAM_COUNT, 220),
    ...playersAt("DST", REDRAFT_TEAM_COUNT, 230),
  ];
  const userSlot = 4;
  let playerIds: number[] = [];
  const seed = localAutoDraftSeed("test-room", 7, userSlot);
  const draftedByUser: number[] = [];
  while (playerIds.length < slots.length) {
    const cpuBatch = computeCpuDraftBatch({
      slots, players: pool, playerIds, userSlot, teamCount: REDRAFT_TEAM_COUNT,
      rosterConfig: REDRAFT_AUTO_DRAFT_ROSTER_CONFIG, seed,
    });
    playerIds = [...playerIds, ...cpuBatch];
    if (playerIds.length >= slots.length) break;
    const onClock = slots[playerIds.length];
    assert.equal(onClock.teamSlot, userSlot, "batch must stop exactly on the user's turn");
    // Simulate a manual user pick: take the best-available player by rank.
    const drafted = new Set(playerIds);
    const pick = pool.filter((candidate) => !drafted.has(candidate.playerId as number))
      .sort((left, right) => (left.ourRank ?? 999) - (right.ourRank ?? 999))[0];
    const pickedPlayerId = pick.playerId as number;
    playerIds = [...playerIds, pickedPlayerId];
    draftedByUser.push(pickedPlayerId);
  }
  assert.equal(playerIds.length, slots.length);
  assert.equal(new Set(playerIds).size, playerIds.length, "no duplicate picks across a resumed draft");
  assert.equal(draftedByUser.length, REDRAFT_ROUNDS, "the user should have made exactly one pick per round");
  const rosters = rostersFromBatch(slots, pool, playerIds);
  assert.deepEqual(
    new Set(rosters.get(userSlot)!.map((entry) => entry.playerId)),
    new Set(draftedByUser),
    "the user's own roster must be exactly the players the user drafted",
  );
}

// --- Best Ball: full all-CPU draft (12 teams x 20 rounds, no K/DST/FLEX) ---
{
  nextId = 1;
  const slots = buildSnakeSlots(BEST_BALL_TEAM_COUNT, BEST_BALL_ROUNDS);
  const pool = [
    ...playersAt("QB", BEST_BALL_TEAM_COUNT * BEST_BALL_TARGETS.QB, 28),
    ...playersAt("RB", BEST_BALL_TEAM_COUNT * BEST_BALL_TARGETS.RB, 1),
    ...playersAt("WR", BEST_BALL_TEAM_COUNT * BEST_BALL_TARGETS.WR, 2),
    ...playersAt("TE", BEST_BALL_TEAM_COUNT * BEST_BALL_TARGETS.TE, 35),
  ];
  const batch = computeCpuDraftBatch({
    slots, players: pool, playerIds: [], userSlot: NO_USER, teamCount: BEST_BALL_TEAM_COUNT,
    rosterConfig: BEST_BALL_AUTO_DRAFT_ROSTER_CONFIG, seed: "best-ball-full",
  });
  assert.equal(batch.length, BEST_BALL_TEAM_COUNT * BEST_BALL_ROUNDS, "best ball: every slot should fill");
  assert.equal(new Set(batch).size, batch.length, "best ball: no player drafted twice");
  const rosters = rostersFromBatch(slots, pool, batch);
  for (const [teamSlot, roster] of rosters) {
    assert.equal(roster.length, 20, `best ball team ${teamSlot} roster size`);
    const counts: Record<string, number> = { QB: 0, RB: 0, WR: 0, TE: 0 };
    for (const drafted of roster) counts[drafted.position] += 1;
    // Zero slack in this config (targets sum exactly to 20) means every CPU
    // roster must land on the exact target composition, not just the real
    // DK minimums -- see the comment on BEST_BALL_AUTO_DRAFT_ROSTER_CONFIG.
    assert.deepEqual(counts, BEST_BALL_TARGETS, `best ball team ${teamSlot} should exactly match the target composition`);
  }
}

console.log("local-auto-draft tests passed");
