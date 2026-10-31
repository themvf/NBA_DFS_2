/**
 * Boom lineup builder (`nba-boom-exposure-v1`): pick a rising-shot-volume player
 * and require him in N of five unique three-player lineups, without letting any
 * lineup fall more than a chosen number of projected points below the best
 * trio. Lineup 1 is always the baseline trio of the frozen boom rules
 * (`nba-boom-trios-v1`): the three highest market lines. Projections are never
 * changed; the boom score (shot attempts per game, free throws x 0.44, last 3
 * games minus last 20) only decides who can be featured.
 *
 * Started by the Codex session on 2026-10-31 (ranked by its ceiling-watch score
 * on blended points); finished on the same definitions as the frozen rules so
 * the page, the saved records and the tracked trios agree.
 */
import { BOOM_MIN, boomPool, boomScore, shots, type BoomCandidate } from "./nba-boom-trios";

export const BOOM_POLICY = "nba-boom-exposure-v1";
export const LINEUPS = 5;

export type BoomPlayer = {
  id: string; name: string; points: number; pointRank: number;
  boom: number | null; boomRank: number | null; shotsLast3: number | null; shotsLast20: number | null; availability: string;
};
export type BoomBoard = {
  date: string; snapshot: string; model: string | null; frozenAt: string; lockAt: string;
  players: BoomPlayer[]; excluded: number;
};
export type BoardSnapshot = {
  version?: string | null; mode?: string | null; game_date?: string | null; sha256?: string | null; model_sha256?: string | null;
  created_at?: string | null; lock_at?: string | null; candidates?: BoomCandidate[] | null;
};

const finite = (x: unknown): x is number => typeof x === "number" && Number.isFinite(x);
const playerId = (c: Pick<BoomCandidate, "event_id" | "player_key">) => `${c.event_id}:${c.player_key}`;

/** A prospective ghost v2 snapshot frozen before its lock, or null. Players in the frozen trio pool, ranked by line. */
export function readBoomBoard(input: unknown, date: string): BoomBoard | null {
  const s = input as BoardSnapshot | null;
  if (!s || s.version !== "nba-points-ghost-v2" || s.mode !== "prospective" || s.game_date !== date || !s.sha256 ||
      !s.created_at || !s.lock_at || !Array.isArray(s.candidates) ||
      !Number.isFinite(Date.parse(s.created_at)) || !Number.isFinite(Date.parse(s.lock_at)) ||
      Date.parse(s.created_at) >= Date.parse(s.lock_at)) return null;
  const ids = new Set<string>();
  for (const c of s.candidates) {
    if (!c.event_id || !c.player_key || !c.player || ids.has(playerId(c))) return null;
    ids.add(playerId(c));
  }
  const pool = boomPool(s.candidates);
  const players: BoomPlayer[] = pool.map((c, i) => {
    const sh = shots(c);
    return { id: playerId(c), name: c.player, points: c.predictions.market_line as number, pointRank: i + 1,
      boom: boomScore(c), boomRank: null, shotsLast3: sh ? sh[0] : null, shotsLast20: sh ? sh[1] : null,
      availability: c.availability?.status ?? "unverified" };
  });
  const candidates = players.filter((p) => p.boom !== null && p.boom >= BOOM_MIN)
    .sort((a, b) => (b.boom as number) - (a.boom as number) || b.points - a.points || (a.id < b.id ? -1 : 1));
  candidates.forEach((p, i) => { p.boomRank = i + 1; });
  return { date, snapshot: s.sha256, model: s.model_sha256 ?? null, frozenAt: s.created_at, lockAt: s.lock_at,
    players, excluded: s.candidates.length - players.length };
}

export type Trio = { key: string; ids: string[]; total: number };
export type BoomLineupResult = { baseline: Trio[]; adjusted: Trio[]; requested: number; achieved: number; feasible: boolean; reason: string | null };

/**
 * Five unique trios. `baseline` = the five highest-projected trios (lineup 1 is
 * the frozen baseline trio). `adjusted` keeps lineup 1, then includes the
 * featured player in at least `requested` lineups using the highest-projected
 * trios that contain him, then fills by projection. Every lineup stays within
 * `maxDrop` of lineup 1; a request that cannot fit is infeasible, never relaxed.
 */
export function buildBoomLineups(board: BoomBoard, selected: string | null, requested: number, maxDrop: number): BoomLineupResult {
  if (!Number.isInteger(requested) || requested < 0 || requested > LINEUPS || !finite(maxDrop) || maxDrop < 0 || maxDrop > 20) throw new Error("Invalid lineup controls");
  const players = board.players;
  const featured = players.find((p) => p.id === selected);
  if (selected && (!featured || featured.boomRank === null)) throw new Error("Player has no frozen boom signal");
  if (players.length < 3) return { baseline: [], adjusted: [], requested, achieved: 0, feasible: false, reason: "Fewer than three eligible players." };
  const best = players[0].points + players[1].points + players[2].points;
  // A trio containing player i totals at most line1 + line2 + line_i, so anyone more
  // than maxDrop below the third line can never fit; skipping them keeps this small.
  const usable = players.filter((p, i) => i < 3 || players[2].points - p.points <= maxDrop + 1e-9);
  const rank = new Map(players.map((p) => [p.id, p.pointRank]));
  const all: Trio[] = [];
  for (let i = 0; i < usable.length - 2; i += 1) for (let j = i + 1; j < usable.length - 1; j += 1) for (let k = j + 1; k < usable.length; k += 1) {
    const ps = [usable[i], usable[j], usable[k]], total = ps[0].points + ps[1].points + ps[2].points;
    if (best - total > maxDrop + 1e-9) continue;
    const ids = ps.map((p) => p.id);
    all.push({ key: ids.join("|"), ids, total });
  }
  const rankSum = (t: Trio) => t.ids.reduce((s, id) => s + (rank.get(id) ?? 0), 0);
  all.sort((a, b) => b.total - a.total || rankSum(a) - rankSum(b) || (a.key < b.key ? -1 : 1));
  const baseline = all.slice(0, LINEUPS), adjusted = [all[0]];
  if (selected) for (const trio of all) {
    if (adjusted.filter((t) => t.ids.includes(selected)).length >= requested || adjusted.length === LINEUPS) break;
    if (trio.ids.includes(selected) && !adjusted.some((t) => t.key === trio.key)) adjusted.push(trio);
  }
  for (const trio of all) {
    if (adjusted.length === LINEUPS) break;
    if (!adjusted.some((t) => t.key === trio.key)) adjusted.push(trio);
  }
  const achieved = selected ? adjusted.filter((t) => t.ids.includes(selected)).length : 0;
  const feasible = adjusted.length === LINEUPS && (!selected || achieved >= requested);
  return { baseline, adjusted, requested, achieved, feasible,
    reason: feasible ? null : "Requested inclusion is not possible with five unique trios, the fixed first lineup and this points limit." };
}
