/**
 * Showdown field-ownership model (`nfl-ownership-field-showdown-v1`), our own
 * Showdown ownership once the books' player lines are in (owner's decision
 * 2026-11-13). A port of model/nfl_showdown_field_model.py; the numbers are
 * fitted by research/nfl_showdown_field_model.py and written to
 * src/data/nfl-showdown-field-model-v1.json. The shared fixture
 * (tests/fixtures/nfl_showdown_field_model_fixture.json) pins both languages.
 *
 * It models how the field builds whole lineups. Every legal lineup among the
 * slate's top 24 players (the 80,000 most plausible, at most $6,000 unspent)
 * gets a value: what the crowd expects its six seats to score, a charge for
 * unspent salary and a bonus for a pass catcher captained with his own
 * quarterback. The field picks lineups with probability proportional to
 * exp(value / tau), and captain and flex ownership are what falls out. So the
 * two always sum to 100% and 500%, and one player's shares cannot pass 100%
 * together. A player outside the top 24 gets 0%.
 *
 * The crowd's points are our projection as the ownership step sees it, with
 * three stand-ins where ours and the crowd's differ most: a kicker is read off
 * his team's Vegas points, a defense off the opponent's, and a player without a
 * full set of sportsbook lines is scaled down (0.70 with a touchdown line only,
 * 0.34 with none). Our projections themselves are not changed.
 *
 * Tested by predicting each of eight 2026 slates (weeks 7-10) from the other
 * seven, against the real field of each night's largest contest: captain off
 * by 0.89 points a player and flex by 3.36, where the per-player fitted model
 * it replaces was off by 1.18 and 4.85 on the same players and stat-api's
 * projected ownership by 0.81 and 2.31. Eight slates of one season: an
 * estimate, and it stays labelled uncalibrated.
 *
 * Only for a slate whose lines are in (`showdownHasLines`). On four slates
 * frozen before any line was captured, none of them in the fit, it missed flex
 * by 4.59 and captain by 1.04: no better than the model it replaces (4.12 and
 * 0.82, on slates that model was fitted on), so that model stays live until the
 * lines arrive.
 *
 * What it does not show: better lineups. Six stored slates rebuilt the way the
 * page builds (Winners shape v14, leverage on, 10 seeds, paid out of 30 real
 * contests) scored +0.9 points a lineup and +1.4 points of top-10% finishes
 * with this model against the fitted one, with the same top-1% rate and
 * return, and every interval included zero. The real field's ownership did no
 * better. So this is a more accurate estimate, not a demonstrated edge.
 */
import model from "@/data/nfl-showdown-field-model-v1.json";

export const NFL_OWNERSHIP_FIELD_SHOWDOWN_VERSION = "nfl-ownership-field-showdown-v1";
export const NFL_OWNERSHIP_FIELD_SHOWDOWN_FITTED_AT: string = model.fitted_at;
const POSITIONS = ["QB", "RB", "WR", "TE", "K", "DST"] as const;
export const FIELD_TOP_PLAYERS = 24;
export const FIELD_KEEP_LINEUPS = 80_000;
const SALARY_CAP = 50_000;
const MAX_UNSPENT = 6_000;
const CAPTAIN_MULTIPLIER = 1.5;
const TRIM_UNSPENT_PER_1K = 0.5;
const STACK_CAPTAINS: readonly string[] = ["WR", "TE"];
const DEFAULT_IMPLIED_TOTAL = 21;
const MIN_CROWD_POINTS = 0.1;
/** Players with a full set of sportsbook lines a slate needs before a missing set counts against anyone. */
export const FIELD_MIN_FULL_ANCHORS = 6;

export interface ShowdownFieldPlayer {
  id: number;
  position: string;
  team: string | null;
  salary: number;
  captainSalary: number | null;
  isOut: boolean;
  /** Our projection as the ownership step sees it; null or <= 0 means not ownable. */
  pts: number | null;
  /** The market anchor's mode: a full set of lines, a touchdown line only, or none. */
  anchor: "full" | "td_only" | null;
  /** Vegas points for the player's team and its opponent (read for kickers and defenses). */
  teamImplied: number | null;
  opponentImplied: number | null;
}

export interface ShowdownFieldResult {
  /** Captain and flex percent by player id; 0 for anyone outside the kept players. */
  shares: Map<number, { captain: number; flex: number }>;
  /** Whether the books' lines were in (see `showdownHasLines`). */
  lined: boolean;
  keptPlayers: number;
  lineups: number;
}

const index = (position: string) => (POSITIONS as readonly string[]).indexOf(position);

function eligible(p: ShowdownFieldPlayer): boolean {
  return !p.isOut && index(p.position) >= 0 && (p.pts ?? 0) > 0 && p.salary > 0 && (p.captainSalary ?? 0) > 0;
}

/** Whether the books' player lines are in on this slate. */
export function showdownHasLines(players: readonly ShowdownFieldPlayer[]): boolean {
  return players.filter((p) => eligible(p) && p.anchor === "full").length >= FIELD_MIN_FULL_ANCHORS;
}

/** What the crowd expects the player to score. */
export function crowdPoints(p: ShowdownFieldPlayer, lined: boolean): number {
  let value: number;
  if (p.position === "K") value = model.kicker.intercept + model.kicker.slope * (p.teamImplied ?? DEFAULT_IMPLIED_TOTAL);
  else if (p.position === "DST") value = model.defense.intercept + model.defense.slope * (p.opponentImplied ?? DEFAULT_IMPLIED_TOTAL);
  else if (!lined || p.anchor === "full") value = p.pts as number;
  else value = (p.pts as number) * (p.anchor === "td_only" ? model.fringe.td_only : model.fringe.none);
  return Math.max(value, MIN_CROWD_POINTS);
}

function choose5(n: number): number {
  return n < 5 ? 0 : (n * (n - 1) * (n - 2) * (n - 3) * (n - 4)) / 120;
}

let last: { key: string; value: ShowdownFieldResult | null } | null = null;

/**
 * Captain and flex ownership for a Showdown pool, or null when the pool cannot
 * form a lineup (fewer than six ownable players, or one team). Pure; the last
 * pool's answer is kept, since one slate is read many times in a row.
 */
export function projectShowdownField(players: readonly ShowdownFieldPlayer[]): ShowdownFieldResult | null {
  const key = JSON.stringify(players.map((p) => [p.id, p.position, p.team, p.salary, p.captainSalary, p.isOut, p.pts, p.anchor, p.teamImplied, p.opponentImplied]));
  if (last?.key === key) return last.value;
  const value = compute(players);
  last = { key, value };
  return value;
}

function compute(players: readonly ShowdownFieldPlayer[]): ShowdownFieldResult | null {
  const lined = showdownHasLines(players);
  // Top players by crowd points; ties keep pool order (Array.prototype.sort is stable).
  const top = players.filter(eligible).map((p) => ({ p, pts: crowdPoints(p, lined) })).sort((a, b) => b.pts - a.pts).slice(0, FIELD_TOP_PLAYERS);
  const n = top.length;
  if (n < 6) return null;
  const pts = top.map((t) => t.pts);
  const salary = top.map((t) => t.p.salary), captainSalary = top.map((t) => t.p.captainSalary as number);
  const position = top.map((t) => index(t.p.position));
  const teamA = top.map((t) => (t.p.team === top[0].p.team ? 1 : 0));
  // Each team's quarterback: its highest crowd-points QB among the kept players.
  const quarterback = new Map<string | null, number>();
  top.forEach((t, k) => { if (t.p.position === "QB" && !quarterback.has(t.p.team)) quarterback.set(t.p.team, k); });

  const capacity = n * choose5(n - 1);
  const cap = new Uint8Array(capacity), flex = new Uint8Array(capacity * 5), stack = new Uint8Array(capacity);
  const left = new Float64Array(capacity), trim = new Float64Array(capacity);
  let m = 0;
  for (let c = 0; c < n; c += 1) {
    const others: number[] = [];
    for (let k = 0; k < n; k += 1) if (k !== c) others.push(k);
    const q = STACK_CAPTAINS.includes(top[c].p.position) ? quarterback.get(top[c].p.team) : undefined;
    const size = others.length;
    for (let a = 0; a < size - 4; a += 1) for (let b = a + 1; b < size - 3; b += 1) for (let d = b + 1; d < size - 2; d += 1)
      for (let e = d + 1; e < size - 1; e += 1) for (let f = e + 1; f < size; f += 1) {
        const i0 = others[a], i1 = others[b], i2 = others[d], i3 = others[e], i4 = others[f];
        const spent = salary[i0] + salary[i1] + salary[i2] + salary[i3] + salary[i4] + captainSalary[c];
        if (spent > SALARY_CAP || SALARY_CAP - spent > MAX_UNSPENT) continue;
        const side = teamA[i0] + teamA[i1] + teamA[i2] + teamA[i3] + teamA[i4] + teamA[c];
        if (side < 1 || side > 5) continue;
        const unspent = (SALARY_CAP - spent) / 1000;
        cap[m] = c; left[m] = unspent;
        const at = m * 5;
        flex[at] = i0; flex[at + 1] = i1; flex[at + 2] = i2; flex[at + 3] = i3; flex[at + 4] = i4;
        stack[m] = q !== undefined && (i0 === q || i1 === q || i2 === q || i3 === q || i4 === q) ? 1 : 0;
        trim[m] = pts[i0] + pts[i1] + pts[i2] + pts[i3] + pts[i4] + CAPTAIN_MULTIPLIER * pts[c] - TRIM_UNSPENT_PER_1K * unspent;
        m += 1;
      }
  }
  if (!m) return null;

  // Keep the most plausible lineups; a tie at the cut goes to the earlier lineup, as in the Python.
  let threshold = -Infinity, tiesKept = Infinity;
  if (m > FIELD_KEEP_LINEUPS) {
    const sorted = trim.slice(0, m).sort();
    threshold = sorted[m - FIELD_KEEP_LINEUPS];
    let above = 0;
    for (let k = 0; k < m; k += 1) if (trim[k] > threshold) above += 1;
    tiesKept = FIELD_KEEP_LINEUPS - above;
  }
  const flexOffsets = model.flex_offsets as Record<string, number>, captainOffsets = model.captain_offsets as Record<string, number>;
  const flexValue = pts.map((v, k) => v + flexOffsets[POSITIONS[position[k]]]);
  const captainValue = pts.map((v, k) => CAPTAIN_MULTIPLIER * v + captainOffsets[POSITIONS[position[k]]]);
  const kept: number[] = [];
  const utility: number[] = [];
  let best = -Infinity;
  for (let k = 0; k < m; k += 1) {
    if (trim[k] < threshold) continue;
    if (trim[k] === threshold) { if (tiesKept <= 0) continue; tiesKept -= 1; }
    const at = k * 5;
    const u = flexValue[flex[at]] + flexValue[flex[at + 1]] + flexValue[flex[at + 2]] + flexValue[flex[at + 3]] + flexValue[flex[at + 4]]
      + captainValue[cap[k]] + model.unused_salary * left[k] + model.qb_stack * stack[k];
    kept.push(k); utility.push(u);
    if (u > best) best = u;
  }
  const captain = new Array<number>(n).fill(0), flexShare = new Array<number>(n).fill(0);
  let total = 0;
  for (let j = 0; j < kept.length; j += 1) { utility[j] = Math.exp((utility[j] - best) / model.tau); total += utility[j]; }
  for (let j = 0; j < kept.length; j += 1) {
    const w = (utility[j] / total) * 100, k = kept[j], at = k * 5;
    captain[cap[k]] += w;
    flexShare[flex[at]] += w; flexShare[flex[at + 1]] += w; flexShare[flex[at + 2]] += w; flexShare[flex[at + 3]] += w; flexShare[flex[at + 4]] += w;
  }
  const shares = new Map(players.map((p) => [p.id, { captain: 0, flex: 0 }]));
  top.forEach((t, k) => shares.set(t.p.id, { captain: captain[k], flex: flexShare[k] }));
  return { shares, lined, keptPlayers: n, lineups: kept.length };
}
