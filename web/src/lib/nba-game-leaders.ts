// Who leads each game in points: the chance each quoted player scores the most in his game,
// from the books' points lines and ladders (the contest simulation, nba-points-sim-recal-v1).
//
// Measured on 123 games (Oct 20 - Nov 5, 2026): the biggest line led his game 43% of the
// time; the uncorrected simulation said 35%, the corrected one 39%. Books that post a
// "Most Points" market price the favourite higher still, so treat these chances as a little
// low on the favourite and a little high on the longshots. Not graded nightly yet.
//
// Per team (2026-11-06): the same draws give each player's chance to outscore his own
// teammates, and the graded season gives how often each team's biggest line actually did
// (54% of 246 team-games, Oct 20 - Nov 5; no team differed from the rest more than chance).

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
  /** Chance he outscores every quoted teammate; null when his team is unknown. */
  teamChance: number | null;
};

/** One completed team-game: the team's biggest line and his share of leading the team. */
export type TeamLeaderGame = { team: string; player: string; playerKey: string; credit: number };

export type TeamRecord = {
  games: number;
  led: number;
  /** By the player who held the team's biggest line. */
  players: Record<string, { player: string; games: number; led: number }>;
};

export type GameLeaders = {
  eventId: string;
  game: string;
  commenceTime: string;
  players: LeaderChance[];   // most likely first
};

/**
 * Share of simulated games each player scores the most in his game, and among his own
 * quoted teammates (ties split), in pool order. A player with no team gets a null team share.
 */
export function leaderChances(pool: ContestPlayer[], sims: number, seed: number): { game: number[]; team: (number | null)[] } {
  const n = pool.length;
  if (n === 0) return { game: [], team: [] };
  const pts = simulatePoints(pool, sims, rng(seed), true);
  const teams = [...new Set(pool.map((p) => p.team).filter((t): t is string => t != null))];
  const teamOf = pool.map((p) => (p.team == null ? -1 : teams.indexOf(p.team)));
  const share = new Array<number>(n).fill(0);
  const teamShare = new Array<number>(n).fill(0);
  const tTop = new Array<number>(teams.length);
  const tCount = new Array<number>(teams.length);
  for (let s = 0; s < sims; s++) {
    let top = -1, count = 0;
    tTop.fill(-1); tCount.fill(0);
    for (let i = 0; i < n; i++) {
      const v = pts[s * n + i];
      if (v > top) { top = v; count = 1; } else if (v === top) count++;
      const t = teamOf[i];
      if (t >= 0) { if (v > tTop[t]) { tTop[t] = v; tCount[t] = 1; } else if (v === tTop[t]) tCount[t]++; }
    }
    for (let i = 0; i < n; i++) {
      const v = pts[s * n + i];
      if (v === top) share[i] += 1 / count;
      const t = teamOf[i];
      if (t >= 0 && v === tTop[t]) teamShare[i] += 1 / tCount[t];
    }
  }
  return {
    game: share.map((v) => v / sims),
    team: teamShare.map((v, i) => (teamOf[i] < 0 ? null : v / sims)),
  };
}

/** This season's record of each team's biggest line, keyed by the caller's team key. */
export function teamRecords(rows: TeamLeaderGame[], keyOf: (team: string) => string): Record<string, TeamRecord> {
  const out: Record<string, TeamRecord> = {};
  for (const r of rows) {
    const rec = (out[keyOf(r.team)] ??= { games: 0, led: 0, players: {} });
    rec.games += 1;
    rec.led += r.credit;
    const p = (rec.players[r.playerKey] ??= { player: r.player, games: 0, led: 0 });
    p.games += 1;
    p.led += r.credit;
  }
  return out;
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
      players: pool.map((p, i) => ({
        key: p.key, player: p.player, team: p.team, line: p.line, books: p.books,
        chance: chances.game[i], teamChance: chances.team[i],
      }))
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
