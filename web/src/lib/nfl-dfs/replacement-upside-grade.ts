/**
 * Weekly grade of `nfl-replacement-upside-v1` against what DraftKings paid.
 *
 * Pre-registered in docs/nfl-replacement-upside-grading.md (grade version
 * `nfl-replacement-upside-grade-v1`) before any week-4 2026 game was played.
 * Every constant below is frozen by that registration; changing one is a new
 * grade version, never an edit.
 *
 * Evidence is the append-only pool capture (`nfl_dfs_pool_captures`): the
 * whole slate as the workspace showed it, saved every minute of the last 20
 * minutes before each kickoff. Each player-game is graded on the LAST pregame
 * capture that contains him, so nothing is rebuilt after the game from data
 * that could since have changed.
 *
 * Until every floor is met the report is BLINDED: counts and pipeline health
 * only, no outcome metric of any kind. The first run past the floors is the
 * one look; the caller freezes its verdict.
 *
 * Pure: no database access.
 */
import { benchmarkTeam } from './competitor-benchmark';
import { nflRandom } from './random';
import { BOOM_THRESHOLDS, REPLACEMENT_UPSIDE_VERSION, type ReplacementUpside } from './replacement-upside';

export const UPSIDE_GRADE_VERSION = 'nfl-replacement-upside-grade-v1';

export const UPSIDE_GRADE_SPEC = {
  featureVersion: REPLACEMENT_UPSIDE_VERSION,
  /**
   * 2026 weeks 1-3 are excluded: some of their outcomes were looked at while
   * the feature was designed. If the floors are not met in 2026, accrual
   * continues through the 2027 regular season under the same rules.
   */
  windows: [{ season: 2026, firstWeek: 4 }, { season: 2027, firstWeek: 1 }],
  tau: 0.9,
  floors: { events: 60, flagged: 130, weeks: 8 },
  bootstrap: { draws: 10_000, seed: 20260928, level: 0.95 },
  widening: { min: 1, max: 2.5, step: 0.01 },
  /** Control backups must project at least this many DK points. */
  controlMinMean: 1,
  /** Boom probabilities are clipped to [clip, 1 - clip] before log loss. */
  boomClip: 0.001,
} as const;

type Room = 'RB' | 'TE' | 'WR';
const roomOf = (position: string): Room | null =>
  position === 'RB' || position === 'FB' ? 'RB' : position === 'TE' ? 'TE' : position === 'WR' ? 'WR' : null;

export interface GradeFeature {
  version: string;
  flagged?: number;
  skipped?: { team: string; name: string; reason: string }[];
  error?: string;
}

export interface GradeCapturePlayer {
  dkPlayerId: number;
  playerId: number | null;
  name: string;
  team: string;
  position: string;
  isOut: boolean;
  /** Baseline as the workspace displayed it. */
  projection: number | null;
  ceiling: number | null;
  boom: number | null;
  upside: ReplacementUpside | null;
  unchanged: { from: string; reason: string } | null;
}

export interface GradeCapture {
  digest: string;
  uploadId: string;
  uploadCreatedAt: string;
  observedAt: string;
  capturedAt: string;
  origin: string;
  codeRevision: string | null;
  game: { id: number; season: number; week: number; kickoff: string };
  /** `payload.context.replacementUpside`; null when the capture predates it. */
  feature: GradeFeature | null;
  players: GradeCapturePlayer[];
}

export interface GradeGame { id: number; season: number; week: number; kickoff: string; completed: boolean }
export interface GradeResult {
  id: string; playerId: number; gameId: number; team: string; position: string;
  actual: number | null; status: string; computedAt: string;
}

const finite = (v: unknown): v is number => typeof v === 'number' && Number.isFinite(v);
const num = (v: unknown): number | null => (finite(v) ? v : null);
const ms = (s: string) => Date.parse(s);

/** Map one stored pool capture (digest already verified by the caller) to grade input. */
export function captureFromRow(row: {
  digest: string; uploadId: string; uploadCreatedAt: string; observedAt: string; capturedAt: string; payload: unknown;
}): GradeCapture {
  const payload = row.payload as {
    origin: string; codeRevision?: string; game: GradeCapture['game'];
    context?: { replacementUpside?: GradeFeature | null } | null;
    players: { dkPlayerId: number; playerId: number | null; name: string; team: string; position: string; isOut: boolean;
      projection: number | null; ceiling: number | null; boom: number | null; evidence?: unknown }[];
  };
  const feature = payload.context?.replacementUpside;
  return {
    digest: row.digest, uploadId: row.uploadId, uploadCreatedAt: row.uploadCreatedAt,
    observedAt: row.observedAt, capturedAt: row.capturedAt, origin: payload.origin,
    codeRevision: payload.codeRevision ?? null,
    game: { id: Number(payload.game.id), season: Number(payload.game.season), week: Number(payload.game.week), kickoff: payload.game.kickoff },
    feature: feature && typeof feature.version === 'string' ? feature : null,
    players: payload.players.map((p) => {
      const evidence = (p.evidence ?? {}) as {
        replacementUpside?: ReplacementUpside | null; replacementUpsideUnchanged?: { from: string; reason: string } | null;
      };
      return {
        dkPlayerId: p.dkPlayerId, playerId: p.playerId ?? null, name: p.name, team: p.team, position: p.position,
        isOut: p.isOut === true, projection: num(p.projection), ceiling: num(p.ceiling), boom: num(p.boom),
        upside: evidence.replacementUpside ?? null, unchanged: evidence.replacementUpsideUnchanged ?? null,
      };
    }),
  };
}

export function inWindow(season: number, week: number): boolean {
  return UPSIDE_GRADE_SPEC.windows.some((w) => w.season === season && week >= w.firstWeek);
}

/** Preferred capture: latest pregame look, then the newest upload, then digest (outcome-blind). */
function preferred(a: GradeCapture, b: GradeCapture): boolean {
  if (ms(a.observedAt) !== ms(b.observedAt)) return ms(a.observedAt) > ms(b.observedAt);
  if (ms(a.uploadCreatedAt) !== ms(b.uploadCreatedAt)) return ms(a.uploadCreatedAt) > ms(b.uploadCreatedAt);
  return a.digest < b.digest;
}

/** One observation per player-game: the last live pregame capture that contains him, across all uploads. */
export function selectPlayerGames(captures: readonly GradeCapture[]) {
  const excluded = { notLiveCaptures: 0, outOfWindowCaptures: 0, notPregameCaptures: 0, unlinkedPlayers: 0 };
  const unlinked = new Set<string>();
  const chosen = new Map<string, { capture: GradeCapture; player: GradeCapturePlayer }>();
  for (const c of captures) {
    if (c.origin !== 'live_pool') { excluded.notLiveCaptures += 1; continue; }
    if (!inWindow(c.game.season, c.game.week)) { excluded.outOfWindowCaptures += 1; continue; }
    const times = [c.observedAt, c.capturedAt, c.game.kickoff, c.uploadCreatedAt].map(ms);
    if (!times.every(Number.isFinite) || !(ms(c.observedAt) < ms(c.game.kickoff)) || ms(c.observedAt) > ms(c.capturedAt)) {
      excluded.notPregameCaptures += 1;
      continue;
    }
    for (const p of c.players) {
      if (p.playerId == null) { unlinked.add(`${c.game.id}:${p.dkPlayerId}`); continue; }
      const key = `${c.game.id}:${p.playerId}`;
      const prev = chosen.get(key);
      if (!prev || preferred(c, prev.capture)) chosen.set(key, { capture: c, player: p });
    }
  }
  excluded.unlinkedPlayers = unlinked.size;
  const selected = [...chosen.values()].sort((a, b) => a.capture.game.id - b.capture.game.id || a.player.playerId! - b.player.playerId!);
  return { selected, excluded };
}

/**
 * Control backups in one capture: RB/WR/TE players in a team-room with no one
 * ruled out and no upside marker, below the room's top projection, projecting
 * at least `controlMinMean`, with a positive ceiling.
 */
function controlIds(capture: GradeCapture): Set<number> {
  const rooms = new Map<string, GradeCapturePlayer[]>();
  for (const p of capture.players) {
    const room = roomOf(p.position);
    if (!room) continue;
    const key = `${benchmarkTeam(p.team)}:${room}`;
    rooms.set(key, [...(rooms.get(key) ?? []), p]);
  }
  const ids = new Set<number>();
  for (const members of rooms.values()) {
    if (members.some((p) => p.isOut || p.upside || p.unchanged)) continue;
    const ranked = [...members].sort((a, b) => (b.projection ?? -Infinity) - (a.projection ?? -Infinity) || a.dkPlayerId - b.dkPlayerId);
    for (const p of ranked.slice(1)) {
      if (finite(p.projection) && p.projection >= UPSIDE_GRADE_SPEC.controlMinMean && finite(p.ceiling) && p.ceiling > 0) ids.add(p.dkPlayerId);
    }
  }
  return ids;
}

export type ActualStatus = 'scored' | 'pending_result' | 'awaiting_source' | 'game_missing' | 'schedule_changed' | 'result_identity_conflict';

/** Latest exact result per game and player, as of `now`. */
function exactResults(results: readonly GradeResult[], now: string) {
  const byGame = new Map<number, Map<number, GradeResult>>();
  for (const r of results) {
    if (r.status !== 'exact' || !finite(r.actual) || !(ms(r.computedAt) <= ms(now))) continue;
    const game = byGame.get(r.gameId) ?? new Map<number, GradeResult>();
    const prev = game.get(r.playerId);
    if (!prev || ms(r.computedAt) > ms(prev.computedAt) || (ms(r.computedAt) === ms(prev.computedAt) && Number(r.id) > Number(prev.id))) {
      game.set(r.playerId, r);
    }
    byGame.set(r.gameId, game);
  }
  return byGame;
}

/**
 * DraftKings' convention, identical to `nfl-dfs-slate-report-v1`: a listed
 * player in a completed game that has at least one exact result, with no row
 * of his own, scored 0. A backup who never touched the ball is exactly the
 * "didn't get the job" outcome, so he must not drop out as missing.
 */
export function resolveActual(capture: GradeCapture, player: GradeCapturePlayer,
  games: ReadonlyMap<number, GradeGame>, exact: ReadonlyMap<number, ReadonlyMap<number, GradeResult>>):
  { status: ActualStatus; actual: number | null } {
  const game = games.get(capture.game.id);
  if (!game) return { status: 'game_missing', actual: null };
  if (ms(game.kickoff) !== ms(capture.game.kickoff)) return { status: 'schedule_changed', actual: null };
  if (!game.completed) return { status: 'pending_result', actual: null };
  const rows = exact.get(game.id);
  if (!rows || rows.size === 0) return { status: 'awaiting_source', actual: null };
  const hit = rows.get(player.playerId!);
  if (hit && benchmarkTeam(hit.team) !== benchmarkTeam(player.team)) return { status: 'result_identity_conflict', actual: null };
  return { status: 'scored', actual: hit ? hit.actual! : 0 };
}

export const pinball = (q: number, y: number, tau: number = UPSIDE_GRADE_SPEC.tau) => (y >= q ? tau * (y - q) : (1 - tau) * (q - y));

const logLoss = (p: number, hit: boolean) => {
  const c = UPSIDE_GRADE_SPEC.boomClip;
  const q = Math.min(1 - c, Math.max(c, p));
  return hit ? -Math.log(q) : -Math.log(1 - q);
};

export interface FlaggedRow {
  season: number; week: number; gameId: number; event: string; team: string; room: Room; role: 'lead' | 'other'; pi: number;
  playerId: number; name: string; position: string; from: string; actual: number;
  baseMean: number; mixMean: number; baseP90: number; mixP90: number; baseBoom: number | null; mixBoom: number | null;
  boomLine: number | null; captureDigest: string; codeRevision: string | null;
}
export interface ControlRow { season: number; week: number; gameId: number; playerId: number; position: string; actual: number; baseP90: number }
export interface UnchangedRow { season: number; week: number; gameId: number; playerId: number; name: string; from: string; actual: number; baseP90: number }

/** Multiplicative widening k on control ceilings that minimises P90 pinball (ties to the smaller k). */
export function fitWidening(controls: readonly ControlRow[]): number {
  const { min, max, step } = UPSIDE_GRADE_SPEC.widening;
  let best: number = min, bestLoss = Infinity;
  for (let i = 0; min + i * step <= max + 1e-9; i += 1) {
    const k = Math.round((min + i * step) * 100) / 100;
    const loss = controls.reduce((s, c) => s + pinball(k * c.baseP90, c.actual), 0) / Math.max(1, controls.length);
    if (loss < bestLoss - 1e-12) { best = k; bestLoss = loss; }
  }
  return best;
}

export interface Interval { n: number; mean: number | null; lo: number | null; hi: number | null }

/**
 * Cluster bootstrap over absence events: every draw resamples whole events
 * (all backups behind one ruled-out starter move together) and takes the
 * row-weighted mean of each metric, using the same resampled events for all.
 */
export function clusterBootstrap(rows: readonly { cluster: string; values: readonly (number | null)[] }[], metrics: number,
  draws: number = UPSIDE_GRADE_SPEC.bootstrap.draws, seed: number = UPSIDE_GRADE_SPEC.bootstrap.seed): Interval[] {
  const clusters = new Map<string, { sum: number[]; count: number[] }>();
  for (const r of rows) {
    const c = clusters.get(r.cluster) ?? { sum: Array(metrics).fill(0), count: Array(metrics).fill(0) };
    r.values.forEach((v, m) => { if (finite(v)) { c.sum[m] += v; c.count[m] += 1; } });
    clusters.set(r.cluster, c);
  }
  const list = [...clusters.keys()].sort().map((k) => clusters.get(k)!);
  const point = Array.from({ length: metrics }, (_, m) => {
    const n = list.reduce((s, c) => s + c.count[m], 0);
    return { n, mean: n ? list.reduce((s, c) => s + c.sum[m], 0) / n : null };
  });
  if (!list.length) return point.map((p) => ({ ...p, lo: null, hi: null }));
  const random = nflRandom(seed);
  const samples: number[][] = Array.from({ length: metrics }, () => []);
  for (let b = 0; b < draws; b += 1) {
    const sum = Array(metrics).fill(0), count = Array(metrics).fill(0);
    for (let i = 0; i < list.length; i += 1) {
      const c = list[Math.floor(random() * list.length)];
      for (let m = 0; m < metrics; m += 1) { sum[m] += c.sum[m]; count[m] += c.count[m]; }
    }
    for (let m = 0; m < metrics; m += 1) if (count[m]) samples[m].push(sum[m] / count[m]);
  }
  const tail = (1 - UPSIDE_GRADE_SPEC.bootstrap.level) / 2;
  return point.map((p, m) => {
    const s = samples[m].sort((a, b) => a - b);
    const at = (q: number) => s[Math.min(s.length - 1, Math.max(0, Math.round(q * (s.length - 1))))];
    return { ...p, lo: s.length ? at(tail) : null, hi: s.length ? at(1 - tail) : null };
  });
}

export type UpsideVerdict = 'PROMOTE' | 'PROMOTE_CEILING_ONLY' | 'NOT_PROMOTED_GENERIC' | 'NOT_PROMOTED' | 'RETIRE';

/**
 * The registered decision table.
 *   G1 ceiling beats baseline:           95% CI of (mixture - baseline) pinball entirely below 0.
 *   G2 ceiling beats a generic widening: 95% CI of (mixture - k x baseline) pinball entirely below 0.
 *   G3 boom not worse:                   point estimate of (mixture - baseline) boom log loss <= 0.
 */
export function upsideVerdict(g1: Interval, g2: Interval, g3: Interval): UpsideVerdict {
  if (g1.hi != null && g1.hi < 0) {
    if (g2.hi != null && g2.hi < 0) return g3.mean != null && g3.mean <= 0 ? 'PROMOTE' : 'PROMOTE_CEILING_ONLY';
    return 'NOT_PROMOTED_GENERIC';
  }
  if (g1.lo != null && g1.lo > 0) return 'RETIRE';
  return 'NOT_PROMOTED';
}

export const VERDICT_MEANING: Record<UpsideVerdict, string> = {
  PROMOTE: 'The GPP objective may read the if-job ceiling and boom rate for flagged players (a separate, versioned optimizer change). Cash mode, the projection column and ownership keep the baseline.',
  PROMOTE_CEILING_ONLY: 'The GPP objective may read the if-job ceiling; boom rate stays on the baseline.',
  NOT_PROMOTED_GENERIC: 'The mixture beat the baseline but not a generic ceiling widening: the gain is general under-coverage, not the job mechanism. Display stays; widening all ceilings needs its own registration.',
  NOT_PROMOTED: 'No demonstrated improvement. Display stays, labelled unvalidated; any change to pi, thresholds or trigger is a new version and a new registration.',
  RETIRE: 'The mixture ceiling is confirmed worse than the baseline. Remove the second range from the display.',
};

const mean = (xs: readonly number[]) => (xs.length ? xs.reduce((a, b) => a + b, 0) / xs.length : null);
const rate = (xs: readonly boolean[]) => mean(xs.map(Number));

export interface GradeInput { captures: readonly GradeCapture[]; games: readonly GradeGame[]; results: readonly GradeResult[]; now: string }

export function gradeReplacementUpside(input: GradeInput) {
  const spec = UPSIDE_GRADE_SPEC;
  const games = new Map(input.games.map((g) => [g.id, g]));
  const exact = exactResults(input.results, input.now);
  const { selected, excluded } = selectPlayerGames(input.captures);
  const controlCache = new Map<string, Set<number>>();
  const flagged: FlaggedRow[] = [], controls: ControlRow[] = [], unchanged: UnchangedRow[] = [];
  const flaggedStatus: Record<string, number> = {};
  const note = (status: string) => { flaggedStatus[status] = (flaggedStatus[status] ?? 0) + 1; };
  let featureUnavailable = 0;
  const featureGames = new Set<number>(), skipped = new Map<string, { team: string; name: string; reason: string }>();
  const revisions = new Set<string>();

  for (const { capture, player } of selected) {
    const feature = capture.feature;
    if (!feature || feature.version !== spec.featureVersion || feature.error) { featureUnavailable += 1; continue; }
    featureGames.add(capture.game.id);
    for (const s of feature.skipped ?? []) skipped.set(`${s.team}:${s.name}`, s);
    const upside = player.upside?.version === spec.featureVersion ? player.upside : null;
    const { status, actual } = resolveActual(capture, player, games, exact);
    if (upside) {
      if (![upside.baseline.p90, upside.ifStarterRole.p90, upside.baseline.mean, upside.ifStarterRole.mean].every(finite)) { note('invalid_distribution'); continue; }
      note(status);
      if (status !== 'scored') continue;
      if (capture.codeRevision) revisions.add(capture.codeRevision);
      const boomLine = BOOM_THRESHOLDS[player.position] ?? null;
      flagged.push({
        season: capture.game.season, week: capture.game.week, gameId: capture.game.id,
        event: `${capture.game.id}:${benchmarkTeam(player.team)}:${upside.room}`, team: benchmarkTeam(player.team),
        room: upside.room, role: upside.role, pi: upside.pi, playerId: player.playerId!, name: player.name,
        position: player.position, from: upside.from.name, actual: actual!,
        baseMean: upside.baseline.mean, mixMean: upside.ifStarterRole.mean,
        baseP90: upside.baseline.p90, mixP90: upside.ifStarterRole.p90,
        baseBoom: num(upside.baseline.boom), mixBoom: num(upside.ifStarterRole.boom), boomLine,
        captureDigest: capture.digest, codeRevision: capture.codeRevision,
      });
      continue;
    }
    if (status !== 'scored') continue;
    if (player.unchanged && finite(player.ceiling)) {
      unchanged.push({ season: capture.game.season, week: capture.game.week, gameId: capture.game.id, playerId: player.playerId!,
        name: player.name, from: player.unchanged.from, actual: actual!, baseP90: player.ceiling });
      continue;
    }
    const ids = controlCache.get(capture.digest) ?? controlIds(capture);
    controlCache.set(capture.digest, ids);
    if (ids.has(player.dkPlayerId)) {
      controls.push({ season: capture.game.season, week: capture.game.week, gameId: capture.game.id, playerId: player.playerId!,
        position: player.position, actual: actual!, baseP90: player.ceiling! });
    }
  }

  const events = new Set(flagged.map((r) => r.event));
  const weeks = new Set(flagged.map((r) => `${r.season}:${r.week}`));
  const floors = {
    events: { required: spec.floors.events, have: events.size },
    flagged: { required: spec.floors.flagged, have: flagged.length },
    weeks: { required: spec.floors.weeks, have: weeks.size },
  };
  const floorsMet = Object.values(floors).every((f) => f.have >= f.required);
  const count = <T>(rows: readonly T[], key: (r: T) => string) =>
    rows.reduce<Record<string, number>>((acc, r) => { acc[key(r)] = (acc[key(r)] ?? 0) + 1; return acc; }, {});
  const completedInWindow = input.games.filter((g) => g.completed && inWindow(g.season, g.week));
  const base = {
    version: UPSIDE_GRADE_VERSION, featureVersion: spec.featureVersion, evaluatedAt: input.now, spec,
    floors, floorsMet,
    // Accrual and health only: sample sizes, never outcomes.
    accrual: {
      flaggedScored: flagged.length, events: events.size, weeks: [...weeks].sort(),
      byRole: count(flagged, (r) => `${r.room} ${r.role}`), byWeek: count(flagged, (r) => `${r.season}:${r.week}`),
      flaggedStatuses: flaggedStatus, controlsScored: controls.length, unchangedScored: unchanged.length,
    },
    health: {
      completedGamesInWindow: completedInWindow.length,
      gamesWithFeatureCapture: completedInWindow.filter((g) => featureGames.has(g.id)).length,
      playerGamesWithoutFeature: featureUnavailable,
      skippedStarters: [...skipped.values()],
      codeRevisions: [...revisions].sort(),
    },
    exclusions: excluded,
  };
  if (!floorsMet) {
    return { ...base, revealed: false as const,
      note: 'Blinded: every floor must be met before any outcome metric is computed. Counts and health only.' };
  }

  const k = fitWidening(controls);
  const g3Row = (r: FlaggedRow) => r.boomLine != null && r.baseBoom != null && r.mixBoom != null
    ? logLoss(r.mixBoom, r.actual >= r.boomLine) - logLoss(r.baseBoom, r.actual >= r.boomLine) : null;
  const [g1, g2, g3] = clusterBootstrap(flagged.map((r) => ({ cluster: r.event, values: [
    pinball(r.mixP90, r.actual) - pinball(r.baseP90, r.actual),
    pinball(r.mixP90, r.actual) - pinball(k * r.baseP90, r.actual),
    g3Row(r),
  ] })), 3);
  const verdict = upsideVerdict(g1, g2, g3);
  const roles = [...new Set(flagged.map((r) => `${r.room} ${r.role}`))].sort();
  return {
    ...base, revealed: true as const,
    widening: { k, controls: controls.length },
    metrics: {
      g1CeilingVsBaseline: g1, g2CeilingVsWidened: g2, g3BoomLogLoss: g3,
      passed: { g1: g1.hi != null && g1.hi < 0, g2: g2.hi != null && g2.hi < 0, g3: g3.mean != null && g3.mean <= 0 },
    },
    verdict, meaning: VERDICT_MEANING[verdict],
    // Descriptive only: none of these can change the verdict.
    descriptive: {
      exceedance: {
        flaggedBaseline: rate(flagged.map((r) => r.actual > r.baseP90)),
        flaggedWidened: rate(flagged.map((r) => r.actual > k * r.baseP90)),
        flaggedMixture: rate(flagged.map((r) => r.actual > r.mixP90)),
        controlBaseline: rate(controls.map((c) => c.actual > c.baseP90)),
        controlWidened: rate(controls.map((c) => c.actual > k * c.baseP90)),
        unchangedLeadWrBaseline: rate(unchanged.map((u) => u.actual > u.baseP90)),
      },
      meanSquaredError: {
        baseline: mean(flagged.map((r) => (r.actual - r.baseMean) ** 2)),
        mixture: mean(flagged.map((r) => (r.actual - r.mixMean) ** 2)),
      },
      byRole: Object.fromEntries(roles.map((role) => {
        const rows = flagged.filter((r) => `${r.room} ${r.role}` === role);
        return [role, {
          n: rows.length,
          meanPinballDelta: mean(rows.map((r) => pinball(r.mixP90, r.actual) - pinball(r.baseP90, r.actual))),
          exceedBaseline: rate(rows.map((r) => r.actual > r.baseP90)),
          exceedMixture: rate(rows.map((r) => r.actual > r.mixP90)),
        }];
      })),
    },
    rows: { flagged, unchanged },
  };
}

export type UpsideGradeReport = ReturnType<typeof gradeReplacementUpside>;
