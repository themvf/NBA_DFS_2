// Who leads each game in points: the chance each quoted player scores the most in his game,
// from the books' points lines and ladders (the contest simulation, nba-points-sim-recal-v1).
//
// Measured on 123 games (Oct 20 - Nov 5, 2026): the biggest line led his game 43% of the
// time; the uncorrected simulation said 35%, the corrected one 39%. Books that post a
// "Most Points" market price the favourite higher still, so treat these chances as a little
// low on the favourite and a little high on the longshots. Not graded nightly yet.

import { RECAL_VERSION, rng, seedOf, simulatePoints, type ContestPlayer } from "./nba-contest-optimizer";

export const LEADER_SIMS = 20000;
export const LEADER_VERSION = RECAL_VERSION;

export type LeaderChance = {
  key: string;
  player: string;
  team: string | null;
  line: number;
  books: number;
  chance: number;
};

export type GameLeaders = {
  eventId: string;
  game: string;
  commenceTime: string;
  players: LeaderChance[];   // most likely first
};

/** Share of simulated games each player scores the most (ties split), in pool order. */
export function leaderChances(pool: ContestPlayer[], sims: number, seed: number): number[] {
  const n = pool.length;
  if (n === 0) return [];
  const pts = simulatePoints(pool, sims, rng(seed), true);
  const share = new Array<number>(n).fill(0);
  for (let s = 0; s < sims; s++) {
    let top = -1, count = 0;
    for (let i = 0; i < n; i++) {
      const v = pts[s * n + i];
      if (v > top) { top = v; count = 1; } else if (v === top) count++;
    }
    for (let i = 0; i < n; i++) if (pts[s * n + i] === top) share[i] += 1 / count;
  }
  return share.map((v) => v / sims);
}

/** Every game's quoted players with their chance to lead it. */
export function gameLeaders(
  players: ContestPlayer[],
  games: { eventId: string; game: string; commenceTime: string }[],
  sims: number = LEADER_SIMS,
): GameLeaders[] {
  return games.map((g) => {
    const pool = players.filter((p) => p.eventId === g.eventId).sort((a, b) => b.line - a.line || (a.key < b.key ? -1 : 1));
    const chances = leaderChances(pool, sims, seedOf(`${g.eventId}|${pool.map((p) => `${p.key}:${p.line}`).join("|")}`));
    return {
      eventId: g.eventId, game: g.game, commenceTime: g.commenceTime,
      players: pool.map((p, i) => ({ key: p.key, player: p.player, team: p.team, line: p.line, books: p.books, chance: chances[i] }))
        .sort((a, b) => b.chance - a.chance),
    };
  });
}

/** The American price at which a bet on this chance breaks even, before any book margin. */
export function fairAmerican(chance: number): string {
  if (!(chance > 0 && chance < 1)) return "-";
  if (chance >= 0.5) return `-${Math.round((chance / (1 - chance)) * 100)}`;
  return `+${Math.round(((1 - chance) / chance) * 100)}`;
}
