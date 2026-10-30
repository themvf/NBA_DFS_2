/**
 * Relative upside: how a player's good day compares with a normal starter's at
 * his position on this slate, and whether his range is steady or spiky.
 *
 * Display only (`nfl-upside-display-v1`). Nothing here feeds the optimizer, the
 * ownership prior or a projection: it reads the same projection and P90 the
 * optimizer scores and restates them against a slate baseline. Pure, so the
 * baselines can be tested without a page.
 *
 * Why percentiles and not a standard deviation: the historical model resamples
 * real games, so ranges are lumpy and right-skewed (a tight end can carry a
 * 12-point mean, a 10-point median and a 37-point P90). The P90 is already the
 * number the GPP objective credits; an SD would summarize the same spread less
 * faithfully.
 */

export const RELATIVE_UPSIDE_VERSION = "nfl-upside-display-v1";

/**
 * Starters per team on the slate. A stated convention for "a normal starter",
 * not fitted: one QB, a lead back plus half a second, two and a half receivers,
 * one tight end, one defense, one kicker. Rounded half up against the slate's
 * team count (12 teams: 12 QBs, 18 RBs, 30 WRs).
 */
export const STARTERS_PER_TEAM: Readonly<Record<string, number>> = {
  QB: 1, RB: 1.5, WR: 2.5, TE: 1, DST: 1, K: 1,
};

/** Below this many eligible starters a position gets no baseline. */
export const MIN_STARTERS = 2;

/** P90 over projection. Under 1.6 reads steady, over 2.2 reads spiky. */
export const SHAPE_STEADY_BELOW = 1.6;
export const SHAPE_SPIKY_ABOVE = 2.2;

/** A ratio over a near-zero projection is noise, so none is shown below this. */
export const MIN_SHAPE_PROJECTION = 1;

export type UpsideShape = "steady" | "normal" | "spiky";

export type UpsidePlayer = {
  dkPlayerId: number;
  position: string;
  team: string;
  ourProj: number | null;
  ceilingFpts: number | null;
  isOut?: boolean;
  projectionStatus?: string | null;
};

export type StarterBaseline = {
  position: string;
  teams: number;
  /** How many players the convention asked for, and how many it found. */
  wanted: number;
  starters: number;
  avgProjection: number;
  avgP90: number;
};

export type RelativeUpside = {
  version: typeof RELATIVE_UPSIDE_VERSION;
  /** Player P90 minus the average starter's P90 at his position. */
  vsStarterP90: number | null;
  /** Player projection minus the average starter's projection. */
  vsStarterProjection: number | null;
  baseline: StarterBaseline | null;
  shapeRatio: number | null;
  shape: UpsideShape | null;
};

const finite = (value: number | null | undefined): value is number =>
  value !== null && value !== undefined && Number.isFinite(value);

const round1 = (value: number) => Math.round(value * 10) / 10;

/** A starter candidate must be playing and carry evidence of his own, not the position prior. */
function eligibleStarter(player: UpsidePlayer): boolean {
  if (player.isOut || player.projectionStatus === "out") return false;
  if (player.projectionStatus === "position_prior") return false;
  return finite(player.ourProj) && finite(player.ceilingFpts);
}

export function starterBaselines(players: readonly UpsidePlayer[]): Map<string, StarterBaseline> {
  const teams = new Set(players.map((p) => p.team).filter(Boolean)).size;
  const byPosition = new Map<string, UpsidePlayer[]>();
  for (const player of players) {
    if (!(player.position in STARTERS_PER_TEAM) || !eligibleStarter(player)) continue;
    const list = byPosition.get(player.position) ?? [];
    list.push(player);
    byPosition.set(player.position, list);
  }
  const out = new Map<string, StarterBaseline>();
  for (const [position, list] of byPosition) {
    const wanted = Math.floor(teams * STARTERS_PER_TEAM[position] + 0.5);
    const starters = [...list]
      .sort((a, b) => (b.ourProj as number) - (a.ourProj as number) || a.dkPlayerId - b.dkPlayerId)
      .slice(0, wanted);
    if (starters.length < MIN_STARTERS) continue;
    const mean = (values: number[]) => values.reduce((s, v) => s + v, 0) / values.length;
    out.set(position, {
      position, teams, wanted, starters: starters.length,
      avgProjection: mean(starters.map((p) => p.ourProj as number)),
      avgP90: mean(starters.map((p) => p.ceilingFpts as number)),
    });
  }
  return out;
}

export function upsideShape(ratio: number | null): UpsideShape | null {
  if (!finite(ratio)) return null;
  if (ratio < SHAPE_STEADY_BELOW) return "steady";
  if (ratio > SHAPE_SPIKY_ABOVE) return "spiky";
  return "normal";
}

export function relativeUpside(
  player: UpsidePlayer,
  baselines: ReadonlyMap<string, StarterBaseline>,
): RelativeUpside {
  const baseline = baselines.get(player.position) ?? null;
  const out = player.isOut || player.projectionStatus === "out";
  const p90 = !out && finite(player.ceilingFpts) ? player.ceilingFpts : null;
  const proj = !out && finite(player.ourProj) ? player.ourProj : null;
  const shapeRatio = p90 !== null && proj !== null && proj >= MIN_SHAPE_PROJECTION ? p90 / proj : null;
  return {
    version: RELATIVE_UPSIDE_VERSION,
    vsStarterP90: baseline && p90 !== null ? round1(p90 - baseline.avgP90) : null,
    vsStarterProjection: baseline && proj !== null ? round1(proj - baseline.avgProjection) : null,
    baseline,
    shapeRatio: shapeRatio === null ? null : Math.round(shapeRatio * 100) / 100,
    shape: upsideShape(shapeRatio),
  };
}

/**
 * The optimizer's default GPP ceiling cap (`DEFAULT_MAX_CEILING_MULTIPLE` in
 * `nfl-optimizer.ts`, which is server-only, so the drawer cannot import it).
 * The test reads the optimizer source and fails if the two drift.
 */
export const GPP_CEILING_CAP_MULTIPLE = 2.5;

/** Points of P90 the GPP objective does not credit because of its ceiling cap. */
export function uncreditedCeiling(player: UpsidePlayer, maxCeilingMultiple: number): number | null {
  if (!finite(player.ceilingFpts) || !finite(player.ourProj) || player.ourProj <= 0) return null;
  const over = player.ceilingFpts - player.ourProj * maxCeilingMultiple;
  return over > 0 ? round1(over) : 0;
}

// ── Week-to-week spread from results (read in the explanation drawer) ──────

/** League-wide starters for the spread baseline: 32 teams on the same convention. */
export const LEAGUE_TEAMS = 32;
export const MIN_SPREAD_GAMES = 3;

export type WeekResult = { playerId: number; week: number; points: number };

export type WeeklySpread = {
  games: number;
  mean: number | null;
  sd: number | null;
  weeks: { week: number; points: number }[];
  /** Mean of each league starter's own week-to-week SD, and their mean points. */
  starterSd: number | null;
  starterMean: number | null;
  starters: number;
};

function sampleSd(values: number[]): number | null {
  if (values.length < 2) return null;
  const mean = values.reduce((s, v) => s + v, 0) / values.length;
  const variance = values.reduce((s, v) => s + (v - mean) ** 2, 0) / (values.length - 1);
  return Math.sqrt(variance);
}

/**
 * `rows` are one position's scored weeks for the season so far, one row per
 * player-week. A player with no row in a week did not score that week and is
 * not counted as a zero. Starters are the top players by mean points among
 * those with at least `MIN_SPREAD_GAMES` games.
 */
export function weeklySpread(playerId: number, position: string, rows: readonly WeekResult[]): WeeklySpread {
  const byPlayer = new Map<number, { week: number; points: number }[]>();
  for (const row of rows) {
    const list = byPlayer.get(row.playerId) ?? [];
    list.push({ week: row.week, points: row.points });
    byPlayer.set(row.playerId, list);
  }
  const own = (byPlayer.get(playerId) ?? []).sort((a, b) => a.week - b.week);
  const ownPoints = own.map((w) => w.points);
  const perTeam = STARTERS_PER_TEAM[position] ?? 1;
  const wanted = Math.floor(LEAGUE_TEAMS * perTeam + 0.5);
  const qualified = [...byPlayer.entries()]
    .filter(([, weeks]) => weeks.length >= MIN_SPREAD_GAMES)
    .map(([id, weeks]) => {
      const pts = weeks.map((w) => w.points);
      return { id, mean: pts.reduce((s, v) => s + v, 0) / pts.length, sd: sampleSd(pts) as number };
    })
    .sort((a, b) => b.mean - a.mean || a.id - b.id)
    .slice(0, wanted);
  const avg = (values: number[]) => (values.length ? values.reduce((s, v) => s + v, 0) / values.length : null);
  return {
    games: own.length,
    mean: own.length ? round1(ownPoints.reduce((s, v) => s + v, 0) / own.length) : null,
    sd: (() => { const sd = sampleSd(ownPoints); return sd === null ? null : round1(sd); })(),
    weeks: own,
    starterSd: (() => { const v = avg(qualified.map((q) => q.sd)); return v === null ? null : round1(v); })(),
    starterMean: (() => { const v = avg(qualified.map((q) => q.mean)); return v === null ? null : round1(v); })(),
    starters: qualified.length,
  };
}
