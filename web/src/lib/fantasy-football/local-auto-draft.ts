/**
 * CPU opponents for the local, browser-only mock draft rooms (Best Ball and
 * Redraft Mock). These rooms never touch the database -- unlike the
 * persisted "Draft Lab" (`/fantasy-football/draft/[draftId]`,
 * `web/src/lib/fantasy-football/auto-draft.ts` + `actions.ts`), which is its
 * own CPU-vs-user flow backed by `fantasy_draft_*` tables.
 *
 * This module reuses the same pure decision engine (`selectComputerPick`)
 * so both flows draft with identical logic; it just adapts it to a
 * client-side batch over a `DraftSlot[]` + `playerIds[]` array instead of
 * database rows.
 */
import type { DraftSlot } from "./draft-engine";
import type { RosterConfig } from "./league-config";
import {
  selectComputerPick,
  type AutoDraftContext,
  type AutoDraftPlayer,
} from "./auto-draft";
import type { FantasyRankingRow } from "@/db/queries-fantasy-football";

export function mapRankingsToAutoDraftPlayers(rankings: readonly FantasyRankingRow[]): AutoDraftPlayer[] {
  return rankings.map((player) => ({
    playerId: player.playerId,
    name: player.name,
    position: player.position,
    adp: player.adp,
    ecr: player.ecr,
    ourRank: player.ourRank,
    projectedPoints: player.ourProjectedPoints,
  }));
}

/**
 * A stable, deterministic seed for a local mock draft room. There's no
 * persisted seed field to store (these rooms only persist
 * `{userSlot, playerIds, cpuEnabled}`), so the seed is derived from the
 * values that already identify "this draft" -- same inputs always produce
 * the same CPU picks, matching the persisted simulator's determinism
 * property without adding storage.
 */
export function localAutoDraftSeed(roomKey: string, rankingSetId: number, userSlot: number): string {
  return `${roomKey}:${rankingSetId}:${userSlot}`;
}

/**
 * Computes every consecutive CPU pick starting right after `playerIds`,
 * stopping the instant the user's team is on the clock or the draft
 * completes. Mirrors the persisted simulator's "atomic advancement" /
 * "consecutive CPU ownership" properties: the whole batch is one pure,
 * side-effect-free computation the caller applies in a single state update,
 * so there's never a moment where the board reflects a half-applied batch.
 *
 * Returns an empty array (a no-op) if it's already the user's turn, the
 * draft is complete, or a required position genuinely cannot be filled from
 * the remaining pool (e.g. the board ran out of legal players -- shouldn't
 * happen with these rooms' 260-player boards, but the pure engine can
 * return `null` and this stops cleanly rather than looping forever).
 */
export function computeCpuDraftBatch(params: {
  slots: readonly DraftSlot[];
  players: readonly AutoDraftPlayer[];
  playerIds: readonly number[];
  userSlot: number;
  teamCount: number;
  rosterConfig: RosterConfig;
  seed: string;
  positionCaps?: AutoDraftContext["positionCaps"];
}): number[] {
  const { slots, players, playerIds, userSlot, teamCount, rosterConfig, seed, positionCaps } = params;
  const playerById = new Map(players.map((candidate) => [candidate.playerId, candidate]));
  const drafted = new Set<number | string>(playerIds);
  const rosterBySlot = new Map<number, AutoDraftPlayer[]>();
  playerIds.forEach((playerId, index) => {
    const slot = slots[index];
    const drafted_ = playerById.get(playerId);
    if (!slot || !drafted_) return;
    const roster = rosterBySlot.get(slot.teamSlot) ?? [];
    roster.push(drafted_);
    rosterBySlot.set(slot.teamSlot, roster);
  });

  const batch: number[] = [];
  let index = playerIds.length;
  while (index < slots.length) {
    const slot = slots[index];
    if (slot.teamSlot === userSlot) break;
    const roster = rosterBySlot.get(slot.teamSlot) ?? [];
    const result = selectComputerPick({
      availablePlayers: players,
      roster,
      rosterConfig,
      teamCount,
      teamSlot: slot.teamSlot,
      overallPick: slot.overallPick,
      seed,
      draftedPlayerIds: [...drafted],
      positionCaps,
    });
    if (!result) break;
    const playerId = result.player.playerId as number;
    drafted.add(playerId);
    roster.push(result.player);
    rosterBySlot.set(slot.teamSlot, roster);
    batch.push(playerId);
    index += 1;
  }
  return batch;
}
