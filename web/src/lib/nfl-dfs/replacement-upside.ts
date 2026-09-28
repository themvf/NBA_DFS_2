/**
 * Replacement upside: what a backup's game looks like if he gets the job.
 *
 * When a starter sits, the man behind him does not have a slightly higher
 * average. He has two possible games: he takes the starter's role and plays
 * like the starter, or he doesn't and plays like himself. Averaging those into
 * one number (the withheld redistribution, ~+2 points) describes a game that
 * rarely happens. Here each flagged backup gets a MIXTURE instead:
 *
 *   with chance (1 - pi): his own projected distribution (the baseline)
 *   with chance pi:       the absent starter's projected distribution
 *
 * The mean moves a little; the ceiling and boom rate move a lot, which is
 * where a GPP is won.
 *
 * DISPLAY ONLY. The optimizer, the ownership prior and the projection column
 * keep the baseline. Both numbers are shown side by side until the mixture is
 * tuned and graded on 2026 slates.
 *
 * Evidence (docs/nfl-replacement-upside.md): in the first game a starter
 * misses, the lead remaining player beat his own 90th-percentile score in
 * 42-46% of games (RB/TE) instead of ~15%, and got the starter's workload in
 * 39-65% of them. pi per role was fitted on 2020-25 by 90th-percentile
 * pinball loss and rechecked on 2014-18 (both already-seen seasons, so these
 * are stated priors, not a validated model). Roles whose fit did not hold up
 * on 2014-18 get pi = 0: the top remaining WR (his own games already carry
 * that ceiling) and a TE when a receiver sits.
 */
import { fitQuantileShape, quantile, type QuantileShape } from './captain-simulation';

export const REPLACEMENT_UPSIDE_VERSION = 'nfl-replacement-upside-v1';

export const WINDOW_GAMES = 8;
export const HALF_LIFE = 4;
export const MIN_ACTIVE_GAMES = 2;

type Room = 'RB' | 'TE' | 'WR';
type Unit = 'carries' | 'targets';

/** A starter: this volume a game over the team's last 8 games. Same bars as the blind studies. */
export const STARTER_THRESHOLDS: Record<Room, { unit: Unit; min: number }> = {
  RB: { unit: 'carries', min: 10 },
  TE: { unit: 'targets', min: 4 },
  WR: { unit: 'targets', min: 7 },
};

/** Chance the backup gets the starter's job, by role. Stated priors (see header). */
export const ROLE_PI = {
  RB: { lead: 0.5, other: 0.2 },
  TE: { lead: 0.4, other: 0.2 },
  WR: { lead: 0, other: 0.2 },
} as const;

/** Mirrors BOOM_THRESHOLDS in model/nfl_dfs_historical.py. */
export const BOOM_THRESHOLDS: Record<string, number> = { QB: 30, RB: 25, WR: 25, TE: 20 };

const ROOM_POSITIONS: Record<Room, ReadonlySet<string>> = {
  RB: new Set(['RB', 'FB']),
  TE: new Set(['TE']),
  WR: new Set(['WR']),
};

export interface UpsideDistribution {
  mean: number;
  p10: number;
  median: number;
  p90: number;
  /** P(score >= the position's boom line). */
  boom: number | null;
}

export interface UpsidePlayer {
  key: number;
  name: string;
  position: string;
  team: string;
  out: boolean;
  /** For a ruled-out starter this must be his stored (pre-zeroing) projection. */
  dist: UpsideDistribution | null;
}

/** One team's last completed games, most recent first, and each player's usage in them. */
export interface TeamUsageWindow {
  team: string;
  games: { season: number; week: number }[];
  /** Aligned to `games`; null = no stat row that game (treated as not active). */
  usage: Record<number, ({ targets: number; carries: number } | null)[]>;
}

export interface ReplacementUpside {
  version: string;
  key: number;
  role: 'lead' | 'other';
  room: Room;
  pi: number;
  from: { key: number; name: string; position: string; volume: number; unit: Unit };
  baseline: UpsideDistribution;
  ifStarterRole: UpsideDistribution;
  note: string;
}

export interface ReplacementUpsideReport {
  version: string;
  upside: ReplacementUpside[];
  /** Starters who sat but produced no upside row, and why. Never silent. */
  skipped: { team: string; name: string; reason: string }[];
  /** Players behind a flagged starter whose role gets no adjustment (pi = 0), and why. */
  unchanged: { key: number; from: string; reason: string }[];
}

const UNCHANGED_REASON: Partial<Record<Room, Partial<Record<'lead' | 'other', string>>>> = {
  WR: { lead: 'Top remaining receiver: his own recent games already show this ceiling; the extra upside did not hold up when rechecked on 2014-18.' },
};

export function volumeBaseline(usage: readonly ({ targets: number; carries: number } | null)[], unit: Unit): { base: number; games: number } | null {
  let w = 0, total = 0, games = 0;
  usage.slice(0, WINDOW_GAMES).forEach((row, k) => {
    if (!row) return;
    const weight = 0.5 ** (k / HALF_LIFE);
    w += weight; total += weight * (Number.isFinite(row[unit]) ? row[unit] : 0); games += 1;
  });
  return games >= MIN_ACTIVE_GAMES && w > 0 ? { base: total / w, games } : null;
}

function validDist(d: UpsideDistribution | null): d is UpsideDistribution {
  // A zero projection is not a distribution: a starter the model already
  // knew was out carries 0/0/0, and mixing it in would drag the backup DOWN.
  return !!d && [d.mean, d.p10, d.median, d.p90].every(Number.isFinite) && d.p90 >= d.p10 && d.mean > 0 && d.p90 > 0;
}

function cdf(shape: QuantileShape, x: number): number {
  if (x <= shape.q0) return 0;
  if (x >= shape.q1) return 1;
  let a = 0, b = 1;
  for (let i = 0; i < 60; i += 1) { const m = (a + b) / 2; if (quantile(shape, m) < x) a = m; else b = m; }
  return (a + b) / 2;
}

/**
 * Mixture of two projected distributions: (1 - pi) * A + pi * B. Each side's
 * stored boom rate is used when present (it is measured at the recipient's
 * line by the caller's contract); otherwise it is read off the fitted curve.
 */
export function mixDistributions(a: UpsideDistribution, b: UpsideDistribution, pi: number, boomLine: number | null): UpsideDistribution {
  if (!(pi >= 0 && pi <= 1)) throw new Error('pi must lie in [0, 1]');
  const sa = fitQuantileShape(a.mean, a.p10, a.median, a.p90);
  const sb = fitQuantileShape(b.mean, b.p10, b.median, b.p90);
  const at = (x: number) => (1 - pi) * cdf(sa, x) + pi * cdf(sb, x);
  const q = (p: number) => {
    let lo = Math.min(sa.q0, sb.q0), hi = Math.max(sa.q1, sb.q1);
    for (let i = 0; i < 80; i += 1) { const m = (lo + hi) / 2; if (at(m) < p) lo = m; else hi = m; }
    return (lo + hi) / 2;
  };
  const tail = (d: UpsideDistribution, s: QuantileShape) =>
    d.boom != null && Number.isFinite(d.boom) ? d.boom : boomLine == null ? null : 1 - cdf(s, boomLine);
  const boomA = tail(a, sa), boomB = tail(b, sb);
  return {
    mean: (1 - pi) * a.mean + pi * b.mean,
    p10: q(0.1), median: q(0.5), p90: q(0.9),
    boom: boomA == null || boomB == null ? null : (1 - pi) * boomA + pi * boomB,
  };
}

const round = (x: number) => Math.round(x * 100) / 100;
const roundDist = (d: UpsideDistribution): UpsideDistribution =>
  ({ mean: round(d.mean), p10: round(d.p10), median: round(d.median), p90: round(d.p90), boom: d.boom == null ? null : Math.round(d.boom * 1e4) / 1e4 });

export function computeReplacementUpside(players: readonly UpsidePlayer[], windows: readonly TeamUsageWindow[]): ReplacementUpsideReport {
  const byTeam = new Map(windows.map((w) => [w.team, w]));
  const upside = new Map<number, ReplacementUpside>();
  const skipped: ReplacementUpsideReport['skipped'] = [];
  const unchanged: ReplacementUpsideReport['unchanged'] = [];
  const teams = [...new Set(players.map((p) => p.team))].sort();
  for (const team of teams) {
    const window = byTeam.get(team);
    const roster = players.filter((p) => p.team === team);
    for (const room of ['RB', 'TE', 'WR'] as Room[]) {
      const { unit, min } = STARTER_THRESHOLDS[room];
      const positions = ROOM_POSITIONS[room];
      const starters = roster.filter((p) => p.out && positions.has(p.position)).flatMap((p) => {
        const usage = window?.usage[p.key];
        const base = usage ? volumeBaseline(usage, unit) : null;
        if (!base || base.base < min) return [];
        // First game of the absence only: once he has missed a game, the
        // backup's own recent games already carry the new role.
        if (!usage?.[0]) return [];
        return [{ p, base: base.base }];
      }).sort((x, y) => y.base - x.base || x.p.key - y.p.key);
      if (!starters.length) continue;
      const star = starters[0];
      if (!validDist(star.p.dist)) {
        skipped.push({ team, name: star.p.name, reason: 'No stored projection range for the ruled-out starter.' });
        continue;
      }
      const recipients = roster.filter((p) => !p.out && positions.has(p.position)).flatMap((p) => {
        const base = window?.usage[p.key] ? volumeBaseline(window.usage[p.key], unit) : null;
        return base && base.base > 0 ? [{ p, base: base.base }] : [];
      }).sort((x, y) => y.base - x.base || x.p.key - y.p.key);
      if (!recipients.length) {
        skipped.push({ team, name: star.p.name, reason: `No active ${room} with recent ${unit} on this slate.` });
        continue;
      }
      recipients.forEach(({ p }, i) => {
        const role = i === 0 ? 'lead' : 'other';
        const pi = ROLE_PI[room][role];
        if (pi <= 0) {
          if (!upside.has(p.key) && !unchanged.some((u) => u.key === p.key)) {
            unchanged.push({ key: p.key, from: star.p.name, reason: UNCHANGED_REASON[room]?.[role] ?? 'No adjustment for this role.' });
          }
          return;
        }
        if (!validDist(p.dist) || upside.has(p.key)) return;
        const boomLine = BOOM_THRESHOLDS[p.position] ?? null;
        // A stored boom rate is measured at the starter's own position line;
        // reuse it only when that is the recipient's line too.
        const starterDist = star.p.position === p.position ? star.p.dist! : { ...star.p.dist!, boom: null };
        const mixed = mixDistributions(p.dist, starterDist, pi, boomLine);
        upside.set(p.key, {
          version: REPLACEMENT_UPSIDE_VERSION, key: p.key, role, room, pi,
          from: { key: star.p.key, name: star.p.name, position: star.p.position, volume: round(star.base), unit },
          baseline: roundDist(p.dist), ifStarterRole: roundDist(mixed),
          note: `${star.p.name} is out (${star.base.toFixed(1)} ${unit}/game). ${Math.round(pi * 100)}% chance ${p.name} plays like him, ${Math.round((1 - pi) * 100)}% he plays like himself.`,
        });
      });
    }
  }
  return { version: REPLACEMENT_UPSIDE_VERSION, upside: [...upside.values()].sort((a, b) => a.key - b.key), skipped,
    unchanged: unchanged.filter((u) => !upside.has(u.key)).sort((a, b) => a.key - b.key) };
}
