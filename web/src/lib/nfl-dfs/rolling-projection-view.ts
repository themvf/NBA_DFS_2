import { nflIdentityName, nflIdentityPosition, nflIdentityTeam } from "./identity";
import type { QbMatchupContext } from "./qb-matchup-context";
import { BOOM_THRESHOLDS } from "./replacement-upside";

export const STAT_KEYS = [
  "passing_yards", "passing_tds", "passing_interceptions",
  "carries", "rushing_yards", "rushing_tds",
  "receptions", "receiving_yards", "receiving_tds",
  "fg_made", "fg_att", "fg_made_50_59", "fg_made_60_", "pat_made",
] as const;
export type StatKey = typeof STAT_KEYS[number];
export type StatMeans = Record<StatKey, number | null>;

export type RollingRun = { runId: string; season: number; week: number; modelVersion: string; asOf: string; playerCount: number };
export type RollingPlayer = {
  id: number; name: string; team: string; opponent: string; position: string;
  status: string; projected: number | null; floor: number | null; median: number | null;
  ceiling: number | null; boomRate: number | null;
  historyGames: number; salary: number | null; kickoff: string;
  stats: StatMeans;
  teamContext: QbMatchupContext | null;
};
export type RollingGame = {
  gameId: string; kickoff: string; home: string; away: string;
  homeImplied: number | null; awayImplied: number | null;
  homeSpread: number | null; total: number | null;
};
export type RollingProjectionBoard = {
  options: RollingRun[]; run: RollingRun | null; players: RollingPlayer[]; games: RollingGame[];
  matchupAsOf: string | null; omittedPlayers: number; contextError: string | null;
  salaryAsOf: string | null;
};

export const POSITIONS = ["QB", "RB", "WR", "TE", "K", "DST"] as const;
export type PositionFilter = "ALL" | "FLEX" | typeof POSITIONS[number];
export const POSITION_FILTERS: PositionFilter[] = ["ALL", "QB", "RB", "WR", "TE", "FLEX", "K", "DST"];
const FLEX = new Set(["RB", "WR", "TE"]);

export const SORT_KEYS = ["projected", "ceiling", "boom", "floor", "salary", "value", "name"] as const;
export type SortKey = typeof SORT_KEYS[number];
export const SORT_LABELS: Record<SortKey, string> = {
  projected: "Projection", ceiling: "Ceiling", boom: "Boom chance", floor: "Floor",
  salary: "Salary", value: "Points per $1K", name: "Name",
};

export type ExplorerFilters = {
  position: PositionFilter; team: string | null; query: string;
  includeOut: boolean; includeEstimates: boolean;
};
export type ExplorerState = ExplorerFilters & { sort: SortKey; view: "cards" | "table" };

/** Too few games for a projection of his own: the position average, which the slate report cards measured running high. */
export const isPositionEstimate = (player: RollingPlayer) => player.status === "position_prior";
export const isOut = (player: RollingPlayer) => player.status === "out";

export function matchesPosition(position: string, filter: PositionFilter): boolean {
  if (filter === "ALL") return true;
  if (filter === "FLEX") return FLEX.has(position);
  return position === filter;
}

const normalize = (text: string) => text.normalize("NFKD").replace(/[̀-ͯ]/g, "").toLowerCase().trim();

function matchesQuery(player: RollingPlayer, query: string): boolean {
  const needle = normalize(query);
  return !needle || normalize(player.name).includes(needle) || player.team.toLowerCase() === needle;
}

/** Everything except the position filter, so position counts answer "how many would I see". */
function passesNonPosition(player: RollingPlayer, filters: ExplorerFilters): boolean {
  return (filters.includeOut || !isOut(player))
    && (filters.includeEstimates || !isPositionEstimate(player))
    && (!filters.team || player.team === filters.team)
    && matchesQuery(player, filters.query);
}

export function filterPlayers(players: readonly RollingPlayer[], filters: ExplorerFilters): RollingPlayer[] {
  return players.filter(player => passesNonPosition(player, filters) && matchesPosition(player.position, filters.position));
}

export function positionCounts(players: readonly RollingPlayer[], filters: ExplorerFilters): Record<PositionFilter, number> {
  const counts = Object.fromEntries(POSITION_FILTERS.map(filter => [filter, 0])) as Record<PositionFilter, number>;
  for (const player of players) {
    if (!passesNonPosition(player, filters)) continue;
    for (const filter of POSITION_FILTERS) if (matchesPosition(player.position, filter)) counts[filter] += 1;
  }
  return counts;
}

/** Players the default view leaves out, so the toggle can say exactly what it adds. */
export function hiddenCounts(players: readonly RollingPlayer[], filters: ExplorerFilters): { out: number; estimates: number } {
  const scoped = players.filter(player => (!filters.team || player.team === filters.team)
    && matchesPosition(player.position, filters.position) && matchesQuery(player, filters.query));
  return { out: scoped.filter(isOut).length, estimates: scoped.filter(isPositionEstimate).length };
}

const sortValue = (player: RollingPlayer, key: Exclude<SortKey, "name">) => ({
  projected: player.projected, ceiling: player.ceiling, boom: player.boomRate, floor: player.floor,
  salary: player.salary, value: pointsPerThousand(player),
})[key];

export function sortPlayers(players: readonly RollingPlayer[], key: SortKey): RollingPlayer[] {
  return [...players].sort((a, b) => {
    if (key === "name") return a.name.localeCompare(b.name);
    const difference = (sortValue(b, key) ?? -Infinity) - (sortValue(a, key) ?? -Infinity);
    return Number.isNaN(difference) || difference === 0 ? a.name.localeCompare(b.name) : difference;
  });
}

/** One upper bound for every range bar on screen, so bars compare across players. */
export function rangeMax(players: readonly RollingPlayer[]): number {
  const top = Math.max(0, ...players.map(player => player.ceiling ?? player.projected ?? 0));
  return Math.max(10, Math.ceil(top / 5) * 5);
}

export type StatItem = { label: string; value: number; digits: number };

type StatLineSpec = { label: string; keys: StatKey[]; digits: number };

const RECEIVER_LINE: StatLineSpec[] = [
  { label: "rec", keys: ["receptions"], digits: 1 },
  { label: "rec yds", keys: ["receiving_yards"], digits: 0 },
  { label: "TD", keys: ["receiving_tds", "rushing_tds"], digits: 1 },
];

const STAT_LINES: Record<string, StatLineSpec[]> = {
  QB: [
    { label: "pass yds", keys: ["passing_yards"], digits: 0 },
    { label: "pass TD", keys: ["passing_tds"], digits: 1 },
    { label: "INT", keys: ["passing_interceptions"], digits: 1 },
    { label: "rush yds", keys: ["rushing_yards"], digits: 0 },
  ],
  RB: [
    { label: "carries", keys: ["carries"], digits: 1 },
    { label: "rush yds", keys: ["rushing_yards"], digits: 0 },
    { label: "rec", keys: ["receptions"], digits: 1 },
    { label: "TD", keys: ["rushing_tds", "receiving_tds"], digits: 1 },
  ],
  WR: RECEIVER_LINE,
  TE: RECEIVER_LINE,
  K: [
    { label: "FG", keys: ["fg_made"], digits: 1 },
    { label: "FG att", keys: ["fg_att"], digits: 1 },
    { label: "50+", keys: ["fg_made_50_59", "fg_made_60_"], digits: 1 },
    { label: "XP", keys: ["pat_made"], digits: 1 },
  ],
};

/** The projection's mean stat line. A stat the run did not store is left out, never shown as zero. */
export function statLine(player: RollingPlayer): StatItem[] {
  return (STAT_LINES[player.position] ?? []).flatMap(({ label, keys, digits }) => {
    const values = keys.map(key => player.stats[key]);
    if (values[0] == null) return [];
    return [{ label, value: values.reduce<number>((sum, value) => sum + (value ?? 0), 0), digits }];
  });
}

/** DK points per $1,000 of Classic salary, the usual DFS value measure. */
export function pointsPerThousand(player: Pick<RollingPlayer, "projected" | "salary">): number | null {
  return player.projected == null || !player.salary ? null : player.projected / (player.salary / 1000);
}

/** Defenses pair by team, as the slate identity match does, because DraftKings names them by nickname. */
export const salaryKey = (name: string, team: string, position: string) =>
  nflIdentityPosition(position) === "DST" ? `dst|${nflIdentityTeam(team)}` : `${nflIdentityName(name)}|${nflIdentityTeam(team)}`;

/**
 * One Classic salary per player from every Classic draft group captured for the week.
 * DraftKings prices a player the same in all of a week's Classic groups; a key that
 * carries two different salaries (or two players) is dropped rather than guessed.
 */
export function classicSalaryIndex(rows: readonly { name: string; team: string; position: string; salary: number }[]): Map<string, number> {
  const seen = new Map<string, number | null>();
  for (const row of rows) {
    const key = salaryKey(row.name, row.team, row.position);
    const previous = seen.get(key);
    seen.set(key, previous === undefined || previous === row.salary ? row.salary : null);
  }
  return new Map([...seen].filter((entry): entry is [string, number] => entry[1] != null));
}

export function boomThreshold(position: string): number | null {
  return BOOM_THRESHOLDS[position] ?? null;
}

export type TeamEnvironment = { implied: number | null; opponentImplied: number | null; spread: number | null; total: number | null };

export function environmentByTeam(games: readonly RollingGame[]): Map<string, TeamEnvironment> {
  const map = new Map<string, TeamEnvironment>();
  for (const game of games) {
    map.set(game.home, { implied: game.homeImplied, opponentImplied: game.awayImplied, spread: game.homeSpread, total: game.total });
    map.set(game.away, { implied: game.awayImplied, opponentImplied: game.homeImplied,
      spread: game.homeSpread == null ? null : -game.homeSpread, total: game.total });
  }
  return map;
}

/** A defense scores off the other team's offense, so its relevant total is the opponent's. */
export function relevantImplied(position: string, environment: TeamEnvironment | undefined): { value: number | null; label: string } {
  return position === "DST"
    ? { value: environment?.opponentImplied ?? null, label: "Opp implied" }
    : { value: environment?.implied ?? null, label: "Team implied" };
}

export function parseFilters(params: Record<string, string | undefined>, teams: readonly string[]): ExplorerState {
  const position = POSITION_FILTERS.find(filter => filter === params.position) ?? "ALL";
  const sort = SORT_KEYS.find(key => key === params.sort) ?? "projected";
  return {
    position, team: teams.includes(params.team ?? "") ? params.team! : null, query: params.q ?? "",
    includeOut: params.out === "1", includeEstimates: params.est === "1", sort,
    view: params.view === "table" ? "table" : "cards",
  };
}

export function filterSearch(state: ExplorerState, week: string | null): string {
  const params = new URLSearchParams();
  if (week) params.set("week", week);
  if (state.position !== "ALL") params.set("position", state.position);
  if (state.team) params.set("team", state.team);
  if (state.query.trim()) params.set("q", state.query.trim());
  if (state.sort !== "projected") params.set("sort", state.sort);
  if (state.view !== "cards") params.set("view", state.view);
  if (state.includeOut) params.set("out", "1");
  if (state.includeEstimates) params.set("est", "1");
  const text = params.toString();
  return text ? `?${text}` : "";
}

export const formatNumber = (value: number | null, digits = 1) => value == null ? "—" : value.toFixed(digits);
export const formatPercent = (value: number | null) => value == null ? "—" : `${Math.round(value * 100)}%`;
export const formatSigned = (value: number | null, digits = 1) =>
  value == null ? "—" : `${value > 0 ? "+" : ""}${value.toFixed(digits)}`;
const easternFormat = new Intl.DateTimeFormat("en-US", {
  timeZone: "America/New_York", weekday: "short", hour: "numeric", minute: "2-digit",
});
export const formatKickoff = (value: string) => `${easternFormat.format(new Date(value)).replace(",", "")} ET`;
export const formatSalary = (value: number | null) => value == null ? "—" : `$${value.toLocaleString("en-US")}`;
export const formatSalaryShort = (value: number | null) => value == null ? "—" : `$${(value / 1000).toFixed(1)}K`;
export const formatValue = (value: number | null) => value == null ? "—" : `${value.toFixed(1)}x`;
