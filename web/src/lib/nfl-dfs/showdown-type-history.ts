/**
 * Which Showdown lineup types have won so far (`nfl-showdown-type-history-v1`).
 *
 * For every past Showdown slate with an imported contest: the type of the
 * contest's winning lineup, how its top-100 lineups split, and the type of the
 * best lineup possible from the points DraftKings paid. "Type" is the team
 * split (and whether the bigger side was the team our projections favored),
 * the captain's position and projection rank, quarterbacks, kicker and
 * defense. Descriptive: a running count of what happened, not a forecast.
 */
import { actualBestShowdown, type PredictionPlayer } from "./slate-shape-prediction";

export const SHOWDOWN_TYPE_HISTORY_VERSION = "nfl-showdown-type-history-v1";

export interface HistoryPlayer {
  normalizedName: string;
  name: string;
  team: string;
  position: string;
  salary: number;
  captainSalary: number | null;
  ourProj: number | null;
}

export interface HistoryEntry { rank: number; points: number | null; players: { normalizedName: string; slot: string }[] }

export interface HistorySlate {
  key: string;
  label: string;
  season: number | null;
  week: number | null;
  contestName: string;
  entryCount: number;
  winningScore: number | null;
  players: HistoryPlayer[];
  /** DraftKings points by normalized name. */
  fpts: ReadonlyMap<string, number>;
  /** Top entries, best first (empty when the contest kept none). */
  top: HistoryEntry[];
}

export interface LineupType {
  splitKind: string;
  /** "Favored 4-2", "Underdog 4-2", "Even 3-3". */
  side: string;
  captain: string;
  captainPosition: string;
  /** "Top projected", "2nd-3rd", "4th-6th", "7th or lower" among the slate's players. */
  captainRank: string;
  /** Depth on his own team by DraftKings salary: "WR1", "WR2", "WR3+", "RB1", "QB1"... */
  captainRole: string;
  /** "Favored" or "Underdog" (the team our projections gave more points), or "Unknown". */
  captainSide: string;
  quarterbacks: string;
  kicker: boolean;
  dst: boolean;
}

export interface SlateTypeRow {
  key: string;
  label: string;
  week: number | null;
  contestName: string;
  entryCount: number;
  favored: string | null;
  winner: (LineupType & { score: number | null }) | null;
  best: (LineupType & { score: number }) | null;
  /** Split kind shares among the stored top lineups (up to 100). */
  topShares: Record<string, number> | null;
  topCount: number;
  /** Players in stored lineups not found on the salary slate. */
  unmatched: number;
}

export interface TallyRow { key: string; winners: number; best: number; topShare: number | null }

export interface TypeTally {
  version: string;
  slates: SlateTypeRow[];
  withWinner: number;
  withBest: number;
  withTop: number;
  splitKinds: TallyRow[];
  sides: TallyRow[];
  captainPositions: TallyRow[];
  captainRanks: TallyRow[];
  captainRoles: TallyRow[];
  captainSides: TallyRow[];
  /** Role and side together, e.g. "WR1 · Favored". */
  captainRoleSides: TallyRow[];
  quarterbacks: TallyRow[];
  kicker: TallyRow[];
  dst: TallyRow[];
}

const ROLE_ORDER = ["QB1", "QB2+", "RB1", "RB2+", "WR1", "WR2", "WR3+", "TE1", "TE2+", "K", "DST"];
const RANK_LABELS = ["Top projected", "2nd-3rd", "4th-6th", "7th or lower"];
const rankBucket = (rank: number) => (rank <= 1 ? RANK_LABELS[0] : rank <= 3 ? RANK_LABELS[1] : rank <= 6 ? RANK_LABELS[2] : RANK_LABELS[3]);

function favoredTeam(players: readonly HistoryPlayer[]): string | null {
  const totals = new Map<string, number>();
  for (const p of players) if ((p.ourProj ?? 0) > 0) totals.set(p.team, (totals.get(p.team) ?? 0) + (p.ourProj ?? 0));
  const sorted = [...totals].sort((a, b) => b[1] - a[1]);
  return sorted.length === 2 && sorted[0][1] > sorted[1][1] ? sorted[0][0] : null;
}

/**
 * A player's depth on his own team at his position, ranked by DraftKings
 * salary (a stand-in for the depth chart; projection breaks ties): WR1, WR2,
 * WR3+; a K or DST is just "K"/"DST".
 */
export function roleOf(player: HistoryPlayer, slatePlayers: readonly HistoryPlayer[]): string {
  if (player.position === "K" || player.position === "DST") return player.position;
  const mates = slatePlayers.filter((p) => p.team === player.team && p.position === player.position)
    .sort((a, b) => b.salary - a.salary || (b.ourProj ?? 0) - (a.ourProj ?? 0) || a.name.localeCompare(b.name));
  const depth = mates.findIndex((p) => p.normalizedName === player.normalizedName) + 1;
  if (depth <= 0) return `${player.position}?`;
  const cap = player.position === "WR" ? 3 : 2;
  return depth >= cap ? `${player.position}${cap}+` : `${player.position}${depth}`;
}

/** Type of one lineup given its captain and flex players. */
export function typeOf(captain: HistoryPlayer, flex: readonly HistoryPlayer[], slatePlayers: readonly HistoryPlayer[], favored: string | null): LineupType {
  const all = [captain, ...flex];
  const teams = new Map<string, number>();
  for (const p of all) teams.set(p.team, (teams.get(p.team) ?? 0) + 1);
  const sides = [...teams].sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]));
  const big = sides[0][1], small = sides[1]?.[1] ?? 0, splitKind = `${big}-${small}`;
  const side = big === small ? `Even ${splitKind}` : favored == null ? `Unknown side ${splitKind}` : `${sides[0][0] === favored ? "Favored" : "Underdog"} ${splitKind}`;
  const ranked = [...slatePlayers].filter((p) => (p.ourProj ?? 0) > 0).sort((a, b) => (b.ourProj ?? 0) - (a.ourProj ?? 0));
  const rank = ranked.findIndex((p) => p.normalizedName === captain.normalizedName) + 1;
  const qbs = all.filter((p) => p.position === "QB").length;
  return { splitKind, side, captain: captain.name, captainPosition: captain.position,
    captainRank: rank > 0 ? rankBucket(rank) : RANK_LABELS[3],
    captainRole: roleOf(captain, slatePlayers),
    captainSide: favored == null ? "Unknown" : captain.team === favored ? "Favored" : "Underdog",
    quarterbacks: qbs >= 2 ? "Both QBs" : qbs === 1 ? "One QB" : "No QB",
    kicker: all.some((p) => p.position === "K"), dst: all.some((p) => p.position === "DST") };
}

export function tallyShowdownTypes(slates: readonly HistorySlate[]): TypeTally {
  const rows: SlateTypeRow[] = slates.map((slate) => {
    const byName = new Map(slate.players.map((p) => [p.normalizedName, p]));
    const favored = favoredTeam(slate.players);
    let unmatched = 0;
    const resolve = (entry: HistoryEntry) => {
      const cpt = entry.players.find((p) => p.slot === "CPT");
      const captain = cpt ? byName.get(cpt.normalizedName) : undefined;
      const flex = entry.players.filter((p) => p.slot !== "CPT").map((p) => byName.get(p.normalizedName));
      if (!captain || flex.some((p) => !p) || flex.length !== 5) { unmatched += entry.players.filter((p) => !byName.has(p.normalizedName)).length; return null; }
      return typeOf(captain, flex as HistoryPlayer[], slate.players, favored);
    };
    const winnerEntry = slate.top[0];
    const winnerType = winnerEntry && winnerEntry.rank === 1 ? resolve(winnerEntry) : null;
    const topTypes = slate.top.slice(0, 100).map(resolve).filter((t): t is LineupType => t != null);
    const topShares = topTypes.length ? Object.fromEntries(["3-3", "4-2", "5-1"].map((k) => [k, (100 * topTypes.filter((t) => t.splitKind === k).length) / topTypes.length])) : null;
    // Best lineup the real points allowed (players nobody drafted count as 0).
    const eligible = slate.players.filter((p) => p.captainSalary != null && p.salary > 0);
    const pool: PredictionPlayer[] = eligible.map((p, i) => ({
      dkPlayerId: i + 1, name: p.name, team: p.team, opponent: null, gameKey: null, position: p.position, salary: p.salary,
      captainSalary: p.captainSalary, ourProj: p.ourProj, floorFpts: null, ceilingFpts: null, isOut: false }));
    const poolByIndex = new Map(eligible.map((p, i) => [i + 1, p]));
    const actual = new Map(pool.map((p) => [p.dkPlayerId, slate.fpts.get(poolByIndex.get(p.dkPlayerId)!.normalizedName) ?? 0]));
    const bestLineup = slate.fpts.size ? actualBestShowdown(pool, actual) : null;
    const best = bestLineup ? { ...typeOf(poolByIndex.get(bestLineup.captain)!, bestLineup.flex.map((id) => poolByIndex.get(id)!), slate.players, favored), score: bestLineup.score } : null;
    return { key: slate.key, label: slate.label, week: slate.week, contestName: slate.contestName, entryCount: slate.entryCount, favored,
      winner: winnerType ? { ...winnerType, score: winnerEntry.points } : null, best, topShares, topCount: topTypes.length, unmatched };
  });
  const tally = (pick: (t: LineupType) => string, order?: string[]): TallyRow[] => {
    const keys = new Set<string>();
    rows.forEach((r) => { if (r.winner) keys.add(pick(r.winner)); if (r.best) keys.add(pick(r.best)); });
    if (order) order.forEach((k) => keys.add(k));
    return [...keys].map((key) => ({ key,
      winners: rows.filter((r) => r.winner && pick(r.winner) === key).length,
      best: rows.filter((r) => r.best && pick(r.best) === key).length,
      topShare: null as number | null }))
      .sort((a, b) => order ? (order.indexOf(a.key) + 1 || 99) - (order.indexOf(b.key) + 1 || 99) : b.winners - a.winners || b.best - a.best || a.key.localeCompare(b.key));
  };
  const withTop = rows.filter((r) => r.topShares);
  const splitKinds = tally((t) => t.splitKind, ["3-3", "4-2", "5-1"]).map((row) => ({ ...row,
    topShare: withTop.length ? withTop.reduce((s, r) => s + (r.topShares![row.key] ?? 0), 0) / withTop.length : null }));
  return {
    version: SHOWDOWN_TYPE_HISTORY_VERSION, slates: rows,
    withWinner: rows.filter((r) => r.winner).length, withBest: rows.filter((r) => r.best).length, withTop: withTop.length,
    splitKinds,
    sides: tally((t) => t.side, ["Favored 4-2", "Underdog 4-2", "Favored 5-1", "Underdog 5-1", "Even 3-3"]),
    captainPositions: tally((t) => t.captainPosition, ["QB", "RB", "WR", "TE", "K", "DST"]),
    captainRanks: tally((t) => t.captainRank, RANK_LABELS),
    captainRoles: tally((t) => t.captainRole, ROLE_ORDER),
    captainSides: tally((t) => t.captainSide, ["Favored", "Underdog"]),
    captainRoleSides: tally((t) => `${t.captainRole} · ${t.captainSide}`,
      ROLE_ORDER.flatMap((r) => [`${r} · Favored`, `${r} · Underdog`])).filter((r) => r.winners || r.best),
    quarterbacks: tally((t) => t.quarterbacks, ["Both QBs", "One QB", "No QB"]),
    kicker: tally((t) => (t.kicker ? "With a kicker" : "No kicker"), ["With a kicker", "No kicker"]),
    dst: tally((t) => (t.dst ? "With a defense" : "No defense"), ["With a defense", "No defense"]),
  };
}
