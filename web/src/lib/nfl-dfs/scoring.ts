/**
 * DraftKings NFL fantasy scoring.
 *
 * VERIFIED against DK's official NFL Classic and NFL Showdown Captain
 * Mode rules pages (2026-09-01). The two formats share one scoring
 * system; Showdown adds kickers and multiplies the captain by 1.5.
 *
 * Single source of truth for the formula -- the projection model, the
 * optimizer's objective, and any results/backtest path must all score
 * through here rather than re-deriving it. See docs/nfl-dfs-spec.md
 * section 1.
 */

import { SHOWDOWN_CAPTAIN_MULTIPLIER, type NflPosition } from "./dk-salary-csv";

export { SHOWDOWN_CAPTAIN_MULTIPLIER };

// ── Offense ───────────────────────────────────────────────────────────

export const PASS_YARD_PTS = 0.04;
export const PASS_TD_PTS = 4;
export const INTERCEPTION_PTS = -1;
export const RUSH_YARD_PTS = 0.1;
export const RUSH_TD_PTS = 6;
export const REC_YARD_PTS = 0.1;
export const REC_TD_PTS = 6;
export const RECEPTION_PTS = 1;
export const RETURN_TD_PTS = 6;
export const FUMBLE_LOST_PTS = -1;
export const TWO_POINT_CONVERSION_PTS = 2;
export const OFFENSIVE_FUMBLE_RECOVERY_TD_PTS = 6;

/** Yardage bonuses are threshold events. See `NflStatLine` for why that matters. */
export const PASS_YARD_BONUS_THRESHOLD = 300;
export const RUSH_YARD_BONUS_THRESHOLD = 100;
export const REC_YARD_BONUS_THRESHOLD = 100;
export const YARDAGE_BONUS_PTS = 3;

// ── Kicker (Showdown only -- Classic has no kicker slot) ───────────────

export const EXTRA_POINT_PTS = 1;
export const FG_0_39_PTS = 3;
export const FG_40_49_PTS = 4;
export const FG_50_PLUS_PTS = 5;

// ── Defense / Special Teams ───────────────────────────────────────────

export const SACK_PTS = 1;
export const DST_INTERCEPTION_PTS = 2;
export const FUMBLE_RECOVERY_PTS = 2;
export const SAFETY_PTS = 2;
export const BLOCKED_KICK_PTS = 2;
export const DST_TD_PTS = 6;
export const TWO_POINT_RETURN_PTS = 2;

/**
 * Points-allowed tiers, highest bracket first. `max` is inclusive.
 *
 * NOTE: DK's "Points Allowed" is NOT the opponent's final score -- it
 * counts only points surrendered while the DST is on the field, so a
 * pick-six thrown by the DST's own offense is excluded. Special-teams
 * and blocked-kick return TDs, extra points and field goals are all
 * included. Anything deriving PA from a team total must account for
 * that gap; see docs/nfl-dfs-spec.md section 1.
 */
export const POINTS_ALLOWED_TIERS: ReadonlyArray<{ max: number; points: number }> = [
  { max: 0, points: 10 },
  { max: 6, points: 7 },
  { max: 13, points: 4 },
  { max: 20, points: 1 },
  { max: 27, points: 0 },
  { max: 34, points: -1 },
  { max: Infinity, points: -4 },
];

export type NflOffenseStats = {
  passYds: number;
  passTds: number;
  interceptions: number;
  rushYds: number;
  rushTds: number;
  recYds: number;
  recTds: number;
  receptions: number;
  fumblesLost: number;
  twoPointConversions: number;
  /** Punt / kickoff / FG return for TD. */
  returnTds: number;
  offensiveFumbleRecoveryTds: number;
};

export type NflKickerStats = {
  extraPointsMade: number;
  fgMade0to39: number;
  fgMade40to49: number;
  fgMade50Plus: number;
};

export type NflDstStats = {
  sacks: number;
  dstInterceptions: number;
  fumbleRecoveries: number;
  safeties: number;
  blockedKicks: number;
  /**
   * Every +6 DST score combined: interception return, fumble recovery
   * return, blocked punt / FG return, and punt / kickoff / FG return TDs.
   * DK prices them all identically, so splitting them buys nothing.
   */
  dstTds: number;
  twoPointReturns: number;
  pointsAllowed: number;
};

/**
 * A partial stat line. Every field is optional and defaults to 0, so a
 * projection can emit only the stats it actually models.
 *
 * **Do not score a mean stat line and call the result an expected
 * value.** The three yardage bonuses and the points-allowed tiers are
 * step functions, so E[score(stats)] != score(E[stats]): a 280-yard mean
 * carries real 300-bonus probability that scoring the mean throws away
 * entirely. Draw from the distribution and average the scores. This is
 * the same mean-versus-distribution error recorded for MLB totals in
 * CLAUDE.md.
 */
export type NflStatLine = Partial<NflOffenseStats & NflKickerStats & NflDstStats>;

const n = (value: number | undefined): number => value ?? 0;

/** Fantasy points from DK's points-allowed tiers. */
export function dstPointsAllowedPoints(pointsAllowed: number): number {
  // Tiers are defined on whole points; a fractional value here almost
  // always means someone scored an average instead of a draw.
  const pa = Math.max(0, Math.floor(pointsAllowed));
  for (const tier of POINTS_ALLOWED_TIERS) {
    if (pa <= tier.max) return tier.points;
  }
  return POINTS_ALLOWED_TIERS[POINTS_ALLOWED_TIERS.length - 1].points;
}

export function scoreNflOffense(stats: NflStatLine): number {
  const passYds = n(stats.passYds);
  const rushYds = n(stats.rushYds);
  const recYds = n(stats.recYds);

  return (
    passYds * PASS_YARD_PTS +
    n(stats.passTds) * PASS_TD_PTS +
    n(stats.interceptions) * INTERCEPTION_PTS +
    rushYds * RUSH_YARD_PTS +
    n(stats.rushTds) * RUSH_TD_PTS +
    recYds * REC_YARD_PTS +
    n(stats.recTds) * REC_TD_PTS +
    n(stats.receptions) * RECEPTION_PTS +
    n(stats.returnTds) * RETURN_TD_PTS +
    n(stats.fumblesLost) * FUMBLE_LOST_PTS +
    n(stats.twoPointConversions) * TWO_POINT_CONVERSION_PTS +
    n(stats.offensiveFumbleRecoveryTds) * OFFENSIVE_FUMBLE_RECOVERY_TD_PTS +
    (passYds >= PASS_YARD_BONUS_THRESHOLD ? YARDAGE_BONUS_PTS : 0) +
    (rushYds >= RUSH_YARD_BONUS_THRESHOLD ? YARDAGE_BONUS_PTS : 0) +
    (recYds >= REC_YARD_BONUS_THRESHOLD ? YARDAGE_BONUS_PTS : 0)
  );
}

export function scoreNflKicker(stats: NflStatLine): number {
  return (
    n(stats.extraPointsMade) * EXTRA_POINT_PTS +
    n(stats.fgMade0to39) * FG_0_39_PTS +
    n(stats.fgMade40to49) * FG_40_49_PTS +
    n(stats.fgMade50Plus) * FG_50_PLUS_PTS
  );
}

export function scoreNflDst(stats: NflStatLine): number {
  return (
    n(stats.sacks) * SACK_PTS +
    n(stats.dstInterceptions) * DST_INTERCEPTION_PTS +
    n(stats.fumbleRecoveries) * FUMBLE_RECOVERY_PTS +
    n(stats.safeties) * SAFETY_PTS +
    n(stats.blockedKicks) * BLOCKED_KICK_PTS +
    n(stats.dstTds) * DST_TD_PTS +
    n(stats.twoPointReturns) * TWO_POINT_RETURN_PTS +
    // An ABSENT pointsAllowed contributes nothing. Defaulting it to 0
    // would mean "shutout" and silently award the +10 top tier -- the
    // single most valuable outcome in the DST table -- to any line that
    // simply forgot the field. Every other stat here contributes 0 when
    // omitted; points allowed must behave the same way. A real shutout
    // is `pointsAllowed: 0`, which is explicit and does score +10.
    (stats.pointsAllowed === undefined ? 0 : dstPointsAllowedPoints(stats.pointsAllowed))
  );
}

/**
 * Score one player's line, gated on position.
 *
 * The gate is DK's rule, not a convenience: *"Kickers are only eligible
 * for extra points and field goals made. Non-kickers are not eligible
 * for these scoring categories."* So a position player credited with a
 * field goal scores nothing for it, and a kicker scores only his kicking.
 */
export function scoreNflStatLine(position: NflPosition, stats: NflStatLine): number {
  if (position === "DST") return scoreNflDst(stats);
  if (position === "K") return scoreNflKicker(stats);
  return scoreNflOffense(stats);
}

/**
 * Showdown captain scoring. DK multiplies "each statistic" by 1.5, and
 * since every term including the bonuses is linear in its statistic,
 * that is equivalent to multiplying the total.
 */
export function applyCaptainMultiplier(points: number): number {
  return points * SHOWDOWN_CAPTAIN_MULTIPLIER;
}

/** Score a player in a specific Showdown/Classic roster slot. */
export function scoreNflSlot(
  position: NflPosition,
  stats: NflStatLine,
  slot: "CPT" | "FLEX" | "CLASSIC",
): number {
  const base = scoreNflStatLine(position, stats);
  return slot === "CPT" ? applyCaptainMultiplier(base) : base;
}
