/**
 * Absence commit (`nfl-absence-commit-v1`, 2026-10-11): the optimizer builds
 * from a starter-aware projection when a starter is out, instead of the
 * backup's own average.
 *
 * Owner's decision (2026-10-11): commit to a projection and learn from the
 * result, rather than keep a number we know is wrong while a study runs. The
 * old number is kept on every changed row (`absenceCommit.before`) and the
 * immutable projection run still holds it, so each week can be graded
 * adjusted-versus-old. Change a constant here and bump the version.
 *
 * Two rules, in this order:
 *
 * 1. own_role_games — the starter is out AND already missed the team's last
 *    game, and this SEASON this player has games without him and at least
 *    one game with him (so the change is this absence, not a new season or a
 *    starter who has been out all along). His projected volume moves toward
 *    what he got without him:
 *
 *      w      = n / (n + 1)                    n = his games without the starter
 *      target = w * volume_without + (1 - w) * projected_volume
 *      scale  = clamp(target / projected_volume, 0.5, 3)
 *
 *    and his whole projected range is scaled by `scale` (same efficiency,
 *    more or fewer chances). The anchor is the projection's OWN volume (its
 *    stat means), in the same units as the games, so nothing is double
 *    counted. Volume, not points: volume was predicted well in every absence
 *    study; points per chance were the noisy part. A scale below 1 is
 *    allowed: a committee back who did not get the work is projected lower.
 *
 * 2. first_game — the starter played the last game and is out now, so the
 *    backup has no games without him. The "if he gets the job" mixture from
 *    `replacement-upside.ts` (pi by role) becomes his projection, but only
 *    when it raises it: taking a lower-projected player's job is never a
 *    reason to project lower. Rule 1 wins when both apply.
 *
 * Volume units: RB = carries + receptions, WR/TE = receptions; projected
 * volume uses the same keys from the projection's stat means. (Two drafts
 * before release used converted targets and last-season "with" games; both
 * blamed unrelated usage changes on the absence and were dropped.)
 */
import {
  BOOM_THRESHOLDS, STARTER_THRESHOLDS, mixDistributions, volumeBaseline,
  type ReplacementUpside, type TeamUsageWindow, type UpsideDistribution,
} from './replacement-upside';

export const ABSENCE_COMMIT_VERSION = 'nfl-absence-commit-v1';
export const SCALE_MIN = 0.5;
export const SCALE_MAX = 3;

type Room = 'RB' | 'WR' | 'TE';
const ROOM_OF: Record<string, Room | undefined> = { RB: 'RB', FB: 'RB', WR: 'WR', TE: 'TE' };

export interface CommitPlayer {
  key: number;
  name: string;
  position: string;
  team: string;
  out: boolean;
  dist: UpsideDistribution | null;
  statMeans: Record<string, number> | null;
  upside: ReplacementUpside | null;
}

export interface AbsenceCommit {
  version: string;
  key: number;
  rule: 'own_role_games' | 'first_game';
  from: string[];
  before: UpsideDistribution;
  after: UpsideDistribution;
  /** own_role_games only. */
  games?: { season: number; week: number; volume: number }[];
  /** The projection's own volume (stat means), same units as `games`. */
  projectedVolume?: number;
  weight?: number;
  scale?: number;
  note: string;
}

export interface AbsenceCommitReport {
  version: string;
  commits: AbsenceCommit[];
  /** Out starters this rule could not act on, and why. Never silent. */
  skipped: { team: string; name: string; reason: string }[];
}

const r2 = (x: number) => Math.round(x * 100) / 100;
const roundDist = (d: UpsideDistribution): UpsideDistribution =>
  ({ mean: r2(d.mean), p10: r2(d.p10), median: r2(d.median), p90: r2(d.p90), boom: d.boom == null ? null : Math.round(d.boom * 1e4) / 1e4 });

function valid(d: UpsideDistribution | null): d is UpsideDistribution {
  return !!d && [d.mean, d.p10, d.median, d.p90].every(Number.isFinite) && d.p90 >= d.p10 && d.mean > 0 && d.p90 > 0;
}

export function gameVolume(room: Room, row: { targets: number; carries: number; receptions?: number }): number {
  const receptions = row.receptions ?? 0;
  return room === 'RB' ? row.carries + receptions : receptions;
}

export function projectedVolume(room: Room, statMeans: Record<string, number> | null): number | null {
  if (!statMeans) return null;
  const receptions = Number(statMeans.receptions) || 0;
  const value = room === 'RB' ? (Number(statMeans.carries) || 0) + receptions : receptions;
  return Number.isFinite(value) ? value : null;
}

/** Scale a whole range; the boom rate is re-read off the fitted curve at the player's line. */
export function scaleDistribution(d: UpsideDistribution, scale: number, boomLine: number | null): UpsideDistribution {
  const scaled = { mean: d.mean * scale, p10: d.p10 * scale, median: d.median * scale, p90: d.p90 * scale, boom: null };
  // pi = 0 returns the same distribution with its quantiles and boom read off one fitted curve.
  return mixDistributions(scaled, scaled, 0, boomLine);
}

export function computeAbsenceCommits(players: readonly CommitPlayer[], windows: readonly TeamUsageWindow[], season: number): AbsenceCommitReport {
  const byTeam = new Map(windows.map((w) => [w.team, w]));
  const commits = new Map<number, AbsenceCommit>();
  const skipped: AbsenceCommitReport['skipped'] = [];
  for (const team of [...new Set(players.map((p) => p.team))].sort()) {
    const window = byTeam.get(team);
    if (!window) continue;
    const roster = players.filter((p) => p.team === team);
    for (const room of ['RB', 'WR', 'TE'] as Room[]) {
      const { unit, min } = STARTER_THRESHOLDS[room];
      // Starters out today who already missed the team's last game.
      const absent = roster.filter((p) => p.out && ROOM_OF[p.position] === room).filter((p) => {
        const usage = window.usage[p.key];
        const base = usage ? volumeBaseline(usage, unit) : null;
        return !!base && base.base >= min && !usage?.[0];
      });
      if (!absent.length) continue;
      for (const p of roster) {
        if (p.out || ROOM_OF[p.position] !== room || commits.has(p.key)) continue;
        const usage = window.usage[p.key];
        if (!usage) continue;
        // This season only. "Without": at least one of today's absent starters did not play it. "With": all did.
        const games: { season: number; week: number; volume: number }[] = [];
        let withGames = 0;
        window.games.forEach((game, i) => {
          const line = usage[i];
          if (!line || game.season !== season) return;
          if (absent.some((s) => !window.usage[s.key]?.[i])) games.push({ ...game, volume: gameVolume(room, line) });
          else withGames += 1;
        });
        if (!games.length || !withGames) continue; // no contrast this season: nothing to attribute to the absence
        const projected = projectedVolume(room, p.statMeans);
        if (!valid(p.dist)) { skipped.push({ team, name: p.name, reason: 'No usable projected range to scale.' }); continue; }
        if (projected == null || projected < 0.5) { skipped.push({ team, name: p.name, reason: 'Projected volume under 0.5; nothing to scale from.' }); continue; }
        const n = games.length;
        const roleVolume = games.reduce((sum, g) => sum + g.volume, 0) / n;
        const weight = n / (n + 1);
        const target = weight * roleVolume + (1 - weight) * projected;
        const scale = Math.min(SCALE_MAX, Math.max(SCALE_MIN, target / projected));
        const after = scaleDistribution(p.dist, scale, BOOM_THRESHOLDS[p.position] ?? null);
        const label = room === 'RB' ? 'touches' : 'catches';
        commits.set(p.key, {
          version: ABSENCE_COMMIT_VERSION, key: p.key, rule: 'own_role_games', from: absent.map((s) => s.name),
          before: roundDist(p.dist), after: roundDist(after), games, projectedVolume: r2(projected), weight: r2(weight), scale: r2(scale),
          note: `${absent.map((s) => s.name).join(', ')} out. In ${n} game${n === 1 ? '' : 's'} without him, ${p.name} averaged ${roleVolume.toFixed(1)} ${label} against ${projected.toFixed(1)} projected; range scaled ×${scale.toFixed(2)}.`,
        });
      }
    }
  }
  // First game of an absence: the replacement-upside mixture becomes the projection.
  for (const p of players) {
    if (p.out || commits.has(p.key) || !p.upside || !(p.upside.pi > 0) || !valid(p.dist)) continue;
    // Never lower a projection for taking a lower-projected player's job.
    if (!(p.upside.ifStarterRole.mean > p.dist.mean)) continue;
    commits.set(p.key, {
      version: ABSENCE_COMMIT_VERSION, key: p.key, rule: 'first_game', from: [p.upside.from.name],
      before: roundDist(p.dist), after: p.upside.ifStarterRole,
      note: `${p.upside.from.name} out for the first time. Projection is the job mixture: ${Math.round(p.upside.pi * 100)}% he plays like the starter.`,
    });
  }
  return { version: ABSENCE_COMMIT_VERSION, commits: [...commits.values()].sort((a, b) => a.key - b.key), skipped };
}

