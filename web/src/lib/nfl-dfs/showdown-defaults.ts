/**
 * Showdown form defaults and pre-build notices (`nfl-showdown-defaults-v1`, 2026-11-11).
 *
 * Page layer only: nothing here changes a projection, an ownership number or
 * the optimizer. Two things came out of the money-graded Showdown replay
 * (16 primetime slates paid from five real contests each):
 *
 * 1. Max exposure. The Winners shape's ownership floor (v14) needs the
 *    popular players in most lineups, so the form's 50-60% cap ran them out
 *    and builds stopped short (94% of requested lineups); at 90% every lineup
 *    built and the return was the same (0.58 per $1 both ways). A new Showdown
 *    slate therefore starts at 90%. A value the user typed, or one carried by
 *    a saved draft or run, is never changed: a low one gets a notice with a
 *    one-click fix instead.
 * 2. Inactives. The saved snapshots predated the inactive list, and players
 *    later ruled out (several in 40-60% of rebuilt lineups) cost about $0.10
 *    per $1, as much as the three v14 settings gained. Inactives are posted
 *    about 90 minutes before kickoff, so before then the page says so and
 *    names the questionable players still carrying a projection.
 */
export const SHOWDOWN_DEFAULTS_VERSION = 'nfl-showdown-defaults-v1';

/** The form's starting Max exposure everywhere else. */
export const DEFAULT_MAX_EXPOSURE = 0.6;
/** The starting Max exposure for a Showdown GPP build under the Winners shape. */
export const SHOWDOWN_WINNERS_MAX_EXPOSURE = 0.9;
/** At or below this cap a Winners Showdown build stopped short in the replay. */
export const LOW_EXPOSURE_NOTICE_AT = 0.6;

type FormatModeShape = { format: 'classic' | 'showdown'; mode: 'cash' | 'gpp'; buildShape?: string };

/** The Max exposure a form starts from when nobody has chosen one. */
export function defaultMaxExposure(s: FormatModeShape): number {
  return s.format === 'showdown' && s.mode === 'gpp' && s.buildShape === 'winners' ? SHOWDOWN_WINNERS_MAX_EXPOSURE : DEFAULT_MAX_EXPOSURE;
}

/** Whether to warn that this Max exposure will probably stop a Winners Showdown build short. */
export function lowExposureForOwnershipFloor(s: FormatModeShape & { maxExposure: number }): boolean {
  return s.format === 'showdown' && s.mode === 'gpp' && s.buildShape === 'winners' && s.maxExposure <= LOW_EXPOSURE_NOTICE_AT + 1e-9;
}

/** Minutes before kickoff at which teams post inactives. */
export const INACTIVES_POSTED_MINUTES = 90;
/** A questionable player is named when he still carries at least this projection. */
export const INACTIVE_RISK_MIN_POINTS = 5;
const QUESTIONABLE = new Set(['Q', 'QUESTIONABLE', 'GTD']);

export type InactivesNoticePlayer = { dkPlayerId: number; name: string; position: string; team: string; isOut: boolean;
  ourProj: number | null; dkStatus?: string | null; availabilityStatus?: string | null };
export type InactivesNotice = {
  /** When inactives are due (kickoff minus 90 minutes), epoch ms. */
  postedAt: number;
  minutesUntilPosted: number;
  /** Questionable players still carrying a projection, largest first. */
  questionable: { dkPlayerId: number; name: string; position: string; team: string; points: number }[];
};

/**
 * The wait-for-inactives notice for a Showdown slate, or null when it does not
 * apply: not a Showdown, kickoff unknown, or inactives already due. After that
 * time the page's own status handling takes over (Update data re-reads
 * DraftKings' statuses), so the notice stops rather than guess.
 */
export function inactivesNotice(format: 'classic' | 'showdown', firstKickoff: string | null | undefined, now: number,
  players: readonly InactivesNoticePlayer[]): InactivesNotice | null {
  if (format !== 'showdown' || !firstKickoff) return null;
  const kickoff = Date.parse(firstKickoff);
  if (!Number.isFinite(kickoff)) return null;
  const postedAt = kickoff - INACTIVES_POSTED_MINUTES * 60_000;
  if (now >= postedAt) return null;
  const tag = (value: string | null | undefined) => (value ?? '').trim().toUpperCase();
  const questionable = players
    .filter((p) => !p.isOut && (p.ourProj ?? 0) >= INACTIVE_RISK_MIN_POINTS && (QUESTIONABLE.has(tag(p.dkStatus)) || QUESTIONABLE.has(tag(p.availabilityStatus))))
    .map((p) => ({ dkPlayerId: p.dkPlayerId, name: p.name, position: p.position, team: p.team, points: p.ourProj as number }))
    .sort((a, b) => b.points - a.points || a.dkPlayerId - b.dkPlayerId);
  return { postedAt, minutesUntilPosted: Math.ceil((postedAt - now) / 60_000), questionable };
}
