import { buildSnakeSlots } from "./draft-engine";
import type { RosterConfig } from "./league-config";

// Yahoo standard redraft league, verified 2026-08-07 against Yahoo's own live
// express-settings default page (same discipline as the DST scoring constants
// in ingest/ff_independent.py -- read from the source, not assumed):
//   10 teams | QB 1, WR 2, RB 2, TE 1, W/R/T 1, K 1, DEF 1 | BN 6 | IR 2
// IR spots are not drafted, so the draft is 9 starters + 6 bench = 15 rounds.
export const REDRAFT_POSITIONS = ["QB", "RB", "WR", "TE", "K", "DST"] as const;
export type RedraftPosition = typeof REDRAFT_POSITIONS[number];

export const REDRAFT_TEAM_COUNT = 10;
export const REDRAFT_ROUNDS = 15;
export const REDRAFT_ROSTER_SIZE = REDRAFT_ROUNDS;

// Our data uses "DST"; Yahoo's UI calls the slot "DEF". Display Yahoo's term so
// the board matches what the user sees on draft day, without renaming the
// underlying position code that every query and projection already uses.
export const REDRAFT_POSITION_LABEL: Record<RedraftPosition, string> = {
  QB: "QB", RB: "RB", WR: "WR", TE: "TE", K: "K", DST: "DEF",
};

export const REDRAFT_FLEX_POSITIONS = ["RB", "WR", "TE"] as const;

/** Dedicated (non-flex) starting slots. */
export const REDRAFT_STARTER_SLOTS: Record<RedraftPosition, number> = {
  QB: 1, RB: 2, WR: 2, TE: 1, K: 1, DST: 1,
};
export const REDRAFT_FLEX_SLOTS = 1;
export const REDRAFT_STARTER_COUNT =
  Object.values(REDRAFT_STARTER_SLOTS).reduce((total, count) => total + count, 0) + REDRAFT_FLEX_SLOTS;
export const REDRAFT_BENCH_SLOTS = REDRAFT_ROSTER_SIZE - REDRAFT_STARTER_COUNT;

export type RedraftRosterPlayer = {
  playerId: number;
  position: string;
  team: string | null;
};

export type RedraftGate = {
  code: string;
  label: string;
  pass: boolean;
};

export type RedraftRosterStatus = {
  size: number;
  counts: Record<RedraftPosition, number>;
  /** Starters this roster can actually field, by slot. */
  filledStarterSlots: number;
  canFieldLegalLineup: boolean;
  gates: RedraftGate[];
};

export type RedraftDraftState = {
  userSlot: number;
  playerIds: number[];
  /** CPU opponents draft the other teams via `local-auto-draft.ts`. Off by default -- existing self-play behavior is unchanged unless a user opts in. */
  cpuEnabled: boolean;
};

export function parseRedraftState(value: string): RedraftDraftState {
  try {
    const parsed = JSON.parse(value) as Partial<RedraftDraftState>;
    const slot = Number(parsed.userSlot);
    const userSlot = Number.isInteger(slot) && slot >= 1 && slot <= REDRAFT_TEAM_COUNT ? slot : 1;
    const playerIds = Array.isArray(parsed.playerIds)
      ? parsed.playerIds
        .filter((id): id is number => Number.isInteger(id) && id > 0)
        .slice(0, REDRAFT_TEAM_COUNT * REDRAFT_ROUNDS)
      : [];
    const cpuEnabled = typeof parsed.cpuEnabled === "boolean" ? parsed.cpuEnabled : false;
    return { userSlot, playerIds: [...new Set(playerIds)], cpuEnabled };
  } catch {
    return { userSlot: 1, playerIds: [], cpuEnabled: false };
  }
}

/**
 * Direct mapping of this room's own starter/flex/bench constants into the
 * shared CPU engine's `RosterConfig` shape -- derived, not duplicated, so it
 * can't silently drift from `REDRAFT_STARTER_SLOTS`/`REDRAFT_FLEX_SLOTS`/
 * `REDRAFT_BENCH_SLOTS` if this league's roster ever changes.
 */
export const REDRAFT_AUTO_DRAFT_ROSTER_CONFIG: RosterConfig = {
  QB: REDRAFT_STARTER_SLOTS.QB,
  RB: REDRAFT_STARTER_SLOTS.RB,
  WR: REDRAFT_STARTER_SLOTS.WR,
  TE: REDRAFT_STARTER_SLOTS.TE,
  FLEX: REDRAFT_FLEX_SLOTS,
  K: REDRAFT_STARTER_SLOTS.K,
  DST: REDRAFT_STARTER_SLOTS.DST,
  BN: REDRAFT_BENCH_SLOTS,
};

/**
 * How many starting slots this roster can fill, filling dedicated slots first
 * and then spending whatever RB/WR/TE remain on the single flex.
 */
export function countFilledStarterSlots(counts: Record<RedraftPosition, number>): number {
  let filled = 0;
  let flexEligibleSpare = 0;
  for (const position of REDRAFT_POSITIONS) {
    const dedicated = Math.min(counts[position], REDRAFT_STARTER_SLOTS[position]);
    filled += dedicated;
    if ((REDRAFT_FLEX_POSITIONS as readonly string[]).includes(position)) {
      flexEligibleSpare += counts[position] - dedicated;
    }
  }
  return filled + Math.min(flexEligibleSpare, REDRAFT_FLEX_SLOTS);
}

export function getRedraftRosterStatus(players: RedraftRosterPlayer[]): RedraftRosterStatus {
  const counts: Record<RedraftPosition, number> = { QB: 0, RB: 0, WR: 0, TE: 0, K: 0, DST: 0 };
  for (const player of players) {
    if ((REDRAFT_POSITIONS as readonly string[]).includes(player.position)) {
      counts[player.position as RedraftPosition] += 1;
    }
  }
  const filledStarterSlots = countFilledStarterSlots(counts);
  const canFieldLegalLineup = filledStarterSlots === REDRAFT_STARTER_COUNT;
  // Advisory, not blocking. Yahoo does not stop you drafting an unbalanced
  // roster, so neither do we -- these gates tell you whether you can field a
  // legal Week 1 lineup, they don't prevent the pick.
  const gates: RedraftGate[] = [
    { code: "ROSTER_SIZE", label: `${players.length}/${REDRAFT_ROSTER_SIZE} drafted`, pass: players.length === REDRAFT_ROSTER_SIZE },
    { code: "LINEUP", label: `Can field lineup (${filledStarterSlots}/${REDRAFT_STARTER_COUNT})`, pass: canFieldLegalLineup },
    ...REDRAFT_POSITIONS.map((position) => ({
      code: `MIN_${position}`,
      label: `${REDRAFT_POSITION_LABEL[position]} ${counts[position]}/${REDRAFT_STARTER_SLOTS[position]}`,
      pass: counts[position] >= REDRAFT_STARTER_SLOTS[position],
    })),
  ];
  return { size: players.length, counts, filledStarterSlots, canFieldLegalLineup, gates };
}

/**
 * Yahoo lets you draft any eligible player until the roster is full, so the
 * only hard blocks are a full roster and a duplicate pick. Roster balance is
 * surfaced through gates instead of being enforced.
 */
export function canAddRedraftPlayer(players: RedraftRosterPlayer[], candidate: RedraftRosterPlayer): boolean {
  if (players.length >= REDRAFT_ROSTER_SIZE) return false;
  if (players.some((player) => player.playerId === candidate.playerId)) return false;
  return (REDRAFT_POSITIONS as readonly string[]).includes(candidate.position);
}

export const REDRAFT_SLOTS = buildSnakeSlots(REDRAFT_TEAM_COUNT, REDRAFT_ROUNDS);
