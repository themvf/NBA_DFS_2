/**
 * Expected RB / WR / TE stats for a week (`nfl-position-stats-v1`), per player,
 * per team position group and across the league.
 *
 * Why the base is the game-leaders simulation and not the DFS projection rows:
 * the projection run projects every rostered player as if he plays, so a
 * team's rows do not add up (2026 week 8: Seattle's WRs summed to 399
 * receiving yards). The game-leaders simulation draws a team budget of carries
 * and targets and splits it among the players, so a team's players add up.
 *
 * Two steps, per team:
 *  1. Each player's own estimate. Rushing yards, receptions and receiving
 *     yards are the simulation's mean, unless the books post a full set of
 *     lines for him (the market anchor's rule: RB rush yds + receptions +
 *     anytime TD, WR/TE receptions + receiving yds + anytime TD); then each
 *     stat with its own line is the calibrated line mean (market-anchor.ts).
 *     A partial set is not used, because partial sets graded no better than
 *     the model.
 *  2. The team total. Lines priced one player at a time do not add up: on
 *     2026 week 8 GB the lined receivers plus the simulation's backups came to
 *     321 receiving yards against Jordan Love's calibrated passing line of
 *     263. So each stat keeps a team budget and the players split it in
 *     proportion to their own estimates. Receiving yards: the quarterback's
 *     calibrated passing-yards line times the simulation's RB/WR/TE share of
 *     the team's receiving yards, when a line is posted; otherwise the
 *     simulation's RB/WR/TE total. Receptions and rushing yards: the
 *     simulation's RB/WR/TE total. A player ruled out since the simulation ran
 *     is removed and his share goes to his teammates pro rata.
 * Touchdowns (rushing + receiving) are not budgeted: the calibrated anytime-TD
 * line when posted, otherwise the DFS model's TD mean scaled by the share of
 * his model yardage the simulation gives him (capped at 1.5x, a stated rule).
 * Quarterbacks and the simulation's unnamed residual rows are not counted.
 * Nothing here is validated as a forecast.
 */
import { CALIBRATION, anytimeTdRate, marketMean, statLine, type PropQuote } from "@/lib/nfl-dfs/market-anchor";

export const POSITION_STATS_VERSION = "nfl-position-stats-v1";
export const TD_SCALE_CAP = 1.5;
export const GROUPS = ["RB", "WR", "TE"] as const;
export type Group = (typeof GROUPS)[number];
export type Source = "vegas" | "sim" | "model" | "none";
type Volume = "rushYds" | "rec" | "recYds";
const VOLUME: Volume[] = ["rushYds", "rec", "recYds"];

export type Stats = { rushYds: number; rec: number; recYds: number; tds: number };
export type PlayerInput = {
  identity: string; name: string; team: string; position: string | null;
  sim: { rushing_yards: number | null; receptions: number | null; receiving_yards: number | null };
  model: { rushing_yards?: number | null; receiving_yards?: number | null; rushing_tds?: number | null; receiving_tds?: number | null } | null;
  lines: Record<string, PropQuote> | null;
  ruledOut: boolean;
};
export type PlayerRow = PlayerInput & {
  group: Group;
  /** After the team budget; zero for a ruled-out player. */
  stats: Stats;
  /** The player's own estimate before the team budget. */
  own: Stats;
  sources: { rushYds: Source; rec: Source; recYds: Source; tds: Source };
};
export type GameInput = {
  gameId: string; kickoff: string; home: string; away: string;
  homeSpread: number | null; total: number | null; players: PlayerInput[];
  /** The simulation's receiving yards per team over every row, quarterbacks and residual included. */
  simTeamReceivingYards: Record<string, number>;
};
export type TeamRow = {
  team: string; opponent: string; gameId: string; kickoff: string; home: boolean;
  impliedPoints: number | null; groups: Record<Group, Stats>; total: Stats;
  /** Where the receiving-yards budget came from; the quarterback's name when it was his line. */
  receivingBudget: { source: "vegas" | "sim"; quarterback: string | null; passingYards: number | null };
  vegasPlayers: number; players: number; ruledOut: string[];
};
export type WeekResult = {
  players: PlayerRow[]; teams: TeamRow[];
  league: Record<Group | "ALL", Stats>; teamCount: number;
  skipped: { noPosition: number; ruledOut: number };
};

const zero = (): Stats => ({ rushYds: 0, rec: 0, recYds: 0, tds: 0 });
const add = (a: Stats, b: Stats) => { a.rushYds += b.rushYds; a.rec += b.rec; a.recYds += b.recYds; a.tds += b.tds; };
const num = (v: number | null | undefined) => (v != null && Number.isFinite(Number(v)) ? Number(v) : null);

/** The calibrated mean of one two-way market, or null when it is not fully priced. */
export function vegasMean(market: string, quote: PropQuote | undefined): number | null {
  const m = marketMean(market, quote);
  return m == null ? null : m * (CALIBRATION[market] ?? 1);
}

const simStats = (p: PlayerInput) => ({ rushYds: num(p.sim.rushing_yards) ?? 0, rec: num(p.sim.receptions) ?? 0, recYds: num(p.sim.receiving_yards) ?? 0 });

/** One player's own expected stats (before the team budget) and where each came from. */
export function expectedStats(p: PlayerInput, group: Group): { stats: Stats; sources: PlayerRow["sources"] } {
  const lines = p.lines ?? {};
  const sim = simStats(p);
  const stats: Stats = { ...sim, tds: 0 };
  const sources: PlayerRow["sources"] = { rushYds: "sim", rec: "sim", recYds: "sim", tds: "none" };
  if (statLine(group, lines)) {
    const pairs: [Volume, string][] = [["rushYds", "player_rush_yds"], ["rec", "player_receptions"], ["recYds", "player_reception_yds"]];
    for (const [stat, market] of pairs) {
      const v = vegasMean(market, lines[market]);
      if (v != null) { stats[stat] = Math.max(0, v); sources[stat] = "vegas"; }
    }
  }
  const rate = anytimeTdRate(lines.player_anytime_td?.yes);
  if (rate != null) {
    stats.tds = rate * CALIBRATION.player_anytime_td;
    sources.tds = "vegas";
  } else if (p.model && (p.model.rushing_tds != null || p.model.receiving_tds != null)) {
    const modelTds = (num(p.model.rushing_tds) ?? 0) + (num(p.model.receiving_tds) ?? 0);
    const modelYds = (num(p.model.rushing_yards) ?? 0) + (num(p.model.receiving_yards) ?? 0);
    const scale = modelYds > 1 ? Math.min(TD_SCALE_CAP, (sim.rushYds + sim.recYds) / modelYds) : 0;
    stats.tds = modelTds * scale;
    sources.tds = "model";
  }
  return { stats, sources };
}

const isGroup = (p: string | null): p is Group => p != null && (GROUPS as readonly string[]).includes(p);

export function impliedPoints(total: number | null, homeSpread: number | null, home: boolean): number | null {
  if (total == null || homeSpread == null) return null;
  const teamSpread = home ? homeSpread : -homeSpread;
  return (total - teamSpread) / 2;
}

/** The team's quarterback passing-yards line (calibrated), the largest when two are posted. */
function quarterbackLine(players: PlayerInput[]): { name: string; yards: number } | null {
  let best: { name: string; yards: number } | null = null;
  for (const p of players) {
    if (p.position !== "QB" || p.ruledOut) continue;
    const yards = vegasMean("player_pass_yds", p.lines?.player_pass_yds);
    if (yards != null && (!best || yards > best.yards)) best = { name: p.name, yards };
  }
  return best;
}

/** Players, team position groups and league totals for one week's games. */
export function buildWeek(games: GameInput[]): WeekResult {
  const players: PlayerRow[] = [];
  const teams: TeamRow[] = [];
  const league = { RB: zero(), WR: zero(), TE: zero(), ALL: zero() };
  const skipped = { noPosition: 0, ruledOut: 0 };
  for (const g of games) {
    for (const side of [g.home, g.away]) {
      const home = side === g.home;
      const roster = g.players.filter((p) => p.team === side);
      const rows: PlayerRow[] = [];
      for (const p of roster) {
        if (!isGroup(p.position)) { if (p.position == null) skipped.noPosition += 1; continue; }
        const { stats, sources } = expectedStats(p, p.position);
        rows.push({ ...p, group: p.position, own: stats, stats: { ...stats }, sources });
      }
      // Budgets come from every RB/WR/TE the simulation drew, ruled out or not.
      const budget = { rushYds: 0, rec: 0, recYds: 0 };
      for (const r of rows) { const s = simStats(r); for (const k of VOLUME) budget[k] += s[k]; }
      const qb = quarterbackLine(roster);
      const allRec = g.simTeamReceivingYards[side] ?? 0;
      const receivingBudget: TeamRow["receivingBudget"] = { source: "sim", quarterback: null, passingYards: null };
      if (qb && allRec > 0) {
        budget.recYds = qb.yards * (budget.recYds / allRec);
        Object.assign(receivingBudget, { source: "vegas", quarterback: qb.name, passingYards: qb.yards });
      }
      const active = rows.filter((r) => !r.ruledOut);
      for (const k of VOLUME) {
        const weight = active.reduce((sum, r) => sum + r.own[k], 0);
        for (const r of rows) r.stats[k] = !r.ruledOut && weight > 0 ? budget[k] * r.own[k] / weight : 0;
      }
      const team: TeamRow = { team: side, opponent: home ? g.away : g.home, gameId: g.gameId, kickoff: g.kickoff, home,
        impliedPoints: impliedPoints(g.total, g.homeSpread, home), groups: { RB: zero(), WR: zero(), TE: zero() }, total: zero(),
        receivingBudget, vegasPlayers: 0, players: 0, ruledOut: [] };
      for (const r of rows) {
        players.push(r);
        if (r.ruledOut) { r.stats.tds = 0; team.ruledOut.push(r.name); skipped.ruledOut += 1; continue; }
        add(team.groups[r.group], r.stats);
        add(team.total, r.stats);
        add(league[r.group], r.stats);
        add(league.ALL, r.stats);
        team.players += 1;
        if (Object.values(r.sources).includes("vegas")) team.vegasPlayers += 1;
      }
      teams.push(team);
    }
    for (const p of g.players) if (p.team !== g.home && p.team !== g.away && isGroup(p.position)) skipped.noPosition += 1;
  }
  return { players, teams, league, teamCount: teams.length, skipped };
}

/** Name key for matching prop rows to simulation rows: letters only, suffixes dropped. */
export function nameKey(name: string): string {
  return String(name ?? "").toLowerCase().normalize("NFKD").replace(/[̀-ͯ]/g, "")
    .replace(/\b(jr|sr|ii|iii|iv|v)\b\.?/g, "").replace(/[^a-z]/g, "");
}
