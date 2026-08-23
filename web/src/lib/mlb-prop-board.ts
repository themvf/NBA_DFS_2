/**
 * Pure decision layer for the MLB prop board (`/vegas/mlb-props-v2`).
 *
 * Deliberately framework-free so the rules that decide WHAT A USER SEES are
 * testable without mounting React, and so a component can never quietly grow a
 * second copy of them — the same reason `alert-audit-policy.ts` exists.
 *
 * Three classifications live here, and only here:
 *
 *   1. ARM — which measurement arm an alert belongs to. `dk_prop_value` is the
 *      live arm (the only prop trigger with measured positive same-book
 *      same-line CLV). `prop_line_gap` is the CONTROL arm, demoted 2026-08-15
 *      on n=439 settled at -0.13% CLV, 95% CI [-0.25%, -0.04%] — entirely
 *      below zero. It keeps scanning because it costs nothing and cancels
 *      systematic error the live arm shares, and it must NEVER be presented as
 *      a play. The old shared panel separated the two by chip colour alone;
 *      here they are structurally different sections.
 *
 *   2. LIFECYCLE — whether the alert is still bettable. The shared panel
 *      selected `commence_time` and never rendered it, so an alert on a game
 *      that started four hours ago looked identical to one on a 7:05 first
 *      pitch. That is the single biggest reason the old page could not answer
 *      "what should I look at".
 *
 *   3. EXECUTION — where you would actually place it. `dk_prop_value` v3
 *      SELECTS at DraftKings and EXECUTES at the best same-line price across
 *      six books; the payload carries `exec_book`/`exec_odds`/`exec_gain_pct`.
 *      The old chip printed `dk_odds`, which is the selection price and
 *      deliberately not where the bet goes.
 *
 * What this file does NOT do: it never computes a CLV point estimate, a
 * confidence interval, or a verdict. Those come from
 * `model/mlb_prop_program.py`, whose metric is frozen at registration; a
 * second implementation across the TS/Python boundary is exactly the drift
 * hazard already documented for `DETECTOR_REGISTRY`.
 */

export const LIVE_ARM_ALERT_TYPE = "dk_prop_value";
export const CONTROL_ARM_ALERT_TYPE = "prop_line_gap";
export const PROP_ALERT_TYPES = [LIVE_ARM_ALERT_TYPE, CONTROL_ARM_ALERT_TYPE];

/** Live-arm detector generation currently enrolled in `mlb-prop-program-v1`. */
export const ENROLLED_DETECTOR_VERSION = "prop-value-v3-dk-trigger-best-exec";

/** Selection threshold on the live arm (`_PROP_VALUE_MIN_EV` in the scanner). */
export const LIVE_ARM_MIN_EV_PCT = 3.0;

/**
 * What to plan against, in CLV percentage points — NOT the headline.
 *
 * v1's n=73 measured +1.29% [+0.43%, +2.32%], but the estimate degrades
 * gracefully under trimming: drop the top 3 and it is +0.87%, top 5 +0.62%,
 * top 10 +0.27% [+0.08%, +0.54%] — still excluding zero at n=63. A real,
 * small, broadly distributed effect. Quoting 1.29% on the board would set an
 * expectation the trimmed estimate does not support.
 */
export const PLANNING_CLV_RANGE_PCT: readonly [number, number] = [0.3, 0.6];

export type PropArm = "live" | "control";

/** Bettable-ness, in the order the board sorts them. */
export type PropLifecycle = "live" | "started" | "settled" | "unknown";

export type PropMarketKey =
  | "pitcher_strikeouts"
  | "batter_total_bases"
  | "pitcher_outs"
  | "pitcher_hits_allowed"
  | "pitcher_earned_runs";

/** The four markets in the pre-registered pooled cell (the census set). */
export const ANCHORED_MARKETS: readonly string[] = [
  "pitcher_strikeouts",
  "batter_total_bases",
  "pitcher_outs",
  "pitcher_hits_allowed",
];

const MARKET_LABELS: Record<string, string> = {
  pitcher_strikeouts: "Strikeouts",
  batter_total_bases: "Total Bases",
  pitcher_outs: "Outs Recorded",
  pitcher_hits_allowed: "Hits Allowed",
  pitcher_earned_runs: "Earned Runs",
  total_games: "Total Games",
};

export function marketLabel(key: string | null | undefined): string {
  if (!key) return "—";
  return MARKET_LABELS[key] ?? key;
}

/**
 * Odds-API book keys → the name a user would recognise on the app icon.
 * `williamhill_us` is Caesars; printing the raw key would send someone to the
 * wrong sportsbook, which is the one failure mode a "where to bet" field has.
 */
const BOOK_LABELS: Record<string, string> = {
  draftkings: "DraftKings",
  fanduel: "FanDuel",
  betmgm: "BetMGM",
  williamhill_us: "Caesars",
  betrivers: "BetRivers",
  fanatics: "Fanatics",
  espnbet: "ESPN BET",
  hardrockbet: "Hard Rock Bet",
  pinnacle: "Pinnacle",
};

export function bookLabel(key: string | null | undefined): string {
  if (!key) return "—";
  return BOOK_LABELS[key] ?? key;
}

export function formatAmerican(odds: number | null | undefined): string {
  if (odds == null || !Number.isFinite(odds)) return "—";
  return odds > 0 ? `+${Math.round(odds)}` : `${Math.round(odds)}`;
}

/** A prop alert reduced to the fields a bettor acts on. */
export type PropPlay = {
  /** Stable within a render; alerts are unique per (matchup, type, side). */
  key: string;
  arm: PropArm;
  lifecycle: PropLifecycle;

  matchup: string;
  player: string;
  market: string;
  marketLabel: string;
  /** "Over" | "Under" as written by the scanner. */
  side: string;
  /** The line the bet is graded at. On the control arm this is DK's line. */
  line: number | null;
  /** Control arm only: Pinnacle's DIFFERENT line. Never comparable to `line`. */
  referenceLine: number | null;

  /** Best same-line price found after selection — where the bet goes. */
  execBook: string | null;
  execOdds: number | null;
  /** % better than the selection price at DraftKings. Pure line-shopping gain. */
  execGainPct: number | null;
  /** DraftKings' price: the SELECTION price, and the one CLV is graded on. */
  dkOdds: number | null;
  /** EV vs Pinnacle's vig-free fair value at the same line, in %. */
  evPct: number | null;
  booksQualifying: number | null;
  detectorVersion: string | null;
  /** True when this row is in the enrolled cohort for `mlb-prop-program-v1`. */
  enrolled: boolean;
  anchoredMarket: boolean;

  createdAt: string;
  commenceTime: string | null;
  /** Null when `commenceTime` is unknown. Negative once first pitch has passed. */
  minutesToFirstPitch: number | null;

  clvPp: number | null;
  outcome: string | null;
};

type RawAlert = {
  matchupId: number;
  createdAt: string;
  matchup: string;
  commenceTime: string | null;
  alertType: string;
  side: string;
  alertProb: number | null;
  sharpProb: number | null;
  details: Record<string, unknown> | null;
  clvPp: number | null;
  outcome: string | null;
};

const num = (v: unknown): number | null => {
  if (v == null) return null;
  const n = typeof v === "number" ? v : Number(v);
  return Number.isFinite(n) ? n : null;
};

const str = (v: unknown): string | null =>
  typeof v === "string" && v.length > 0 ? v : null;

/**
 * Minutes until first pitch, or null when the alert carries no commence time.
 *
 * Null is NOT folded into "started". A missing commence time means we cannot
 * tell whether the game has begun, and the board says so rather than guessing
 * — the same fail-closed rule the game-line decision layer applies.
 */
export function minutesUntil(
  commenceTime: string | null,
  now: Date,
): number | null {
  if (!commenceTime) return null;
  const t = Date.parse(commenceTime);
  if (!Number.isFinite(t)) return null;
  return Math.round((t - now.getTime()) / 60000);
}

export function classifyLifecycle(
  outcome: string | null,
  minutesToFirstPitch: number | null,
): PropLifecycle {
  if (outcome != null) return "settled";
  if (minutesToFirstPitch == null) return "unknown";
  return minutesToFirstPitch > 0 ? "live" : "started";
}

export function toPropPlay(a: RawAlert, now: Date): PropPlay {
  const d = a.details ?? {};
  const arm: PropArm = a.alertType === CONTROL_ARM_ALERT_TYPE ? "control" : "live";
  const minutesToFirstPitch = minutesUntil(a.commenceTime, now);
  const market = str(d.market) ?? "";
  const detectorVersion = str(d.detector_version);
  return {
    key: `${a.matchupId}:${a.alertType}:${a.side}`,
    arm,
    lifecycle: classifyLifecycle(a.outcome, minutesToFirstPitch),
    matchup: a.matchup,
    player: str(d.player) ?? a.side,
    market,
    marketLabel: marketLabel(market),
    side: str(d.bet) ?? "",
    line: num(d.line),
    referenceLine: num(d.pin_line),
    execBook: str(d.exec_book),
    execOdds: num(d.exec_odds),
    execGainPct: num(d.exec_gain_pct),
    dkOdds: num(d.dk_odds),
    evPct: num(d.ev_pct),
    booksQualifying: num(d.books_qualifying),
    detectorVersion,
    enrolled: arm === "live" && detectorVersion === ENROLLED_DETECTOR_VERSION,
    anchoredMarket: ANCHORED_MARKETS.includes(market),
    createdAt: a.createdAt,
    commenceTime: a.commenceTime,
    minutesToFirstPitch,
    clvPp: a.clvPp,
    outcome: a.outcome,
  };
}

/**
 * The board: only the live arm, only games that have not started.
 *
 * Soonest first pitch first, because that is the only ordering under which the
 * thing about to become un-bettable is the thing you read first. EV breaks
 * ties, never leads — sorting a research board by claimed edge invites reading
 * the top row as a ranked recommendation, which the evidence does not support.
 */
export function liveBoard(plays: PropPlay[]): PropPlay[] {
  return plays
    .filter((p) => p.arm === "live" && p.lifecycle === "live")
    .sort((a, b) => {
      const am = a.minutesToFirstPitch ?? Number.MAX_SAFE_INTEGER;
      const bm = b.minutesToFirstPitch ?? Number.MAX_SAFE_INTEGER;
      if (am !== bm) return am - bm;
      return (b.evPct ?? 0) - (a.evPct ?? 0);
    });
}

/** Live-arm alerts whose game already started, or which have graded. */
export function liveArmHistory(plays: PropPlay[]): PropPlay[] {
  return plays
    .filter((p) => p.arm === "live" && p.lifecycle !== "live")
    .sort((a, b) => b.createdAt.localeCompare(a.createdAt));
}

export function controlArm(plays: PropPlay[]): PropPlay[] {
  return plays
    .filter((p) => p.arm === "control")
    .sort((a, b) => b.createdAt.localeCompare(a.createdAt));
}

export type BoardFilters = {
  /** Market key, or "all". */
  market: string;
  /** Only rows whose EV clears this, in %. */
  minEvPct: number;
};

export function applyFilters(plays: PropPlay[], f: BoardFilters): PropPlay[] {
  return plays.filter((p) => {
    if (f.market !== "all" && p.market !== f.market) return false;
    if (f.minEvPct > 0 && (p.evPct ?? 0) < f.minEvPct) return false;
    return true;
  });
}

export function formatCountdown(minutes: number | null): string {
  if (minutes == null) return "start time unknown";
  if (minutes <= 0) return "started";
  if (minutes < 60) return `${minutes}m to first pitch`;
  const h = Math.floor(minutes / 60);
  const m = minutes % 60;
  if (h < 24) return `${h}h ${m}m to first pitch`;
  return `${Math.floor(h / 24)}d ${h % 24}h to first pitch`;
}

/**
 * How urgent a live row is. Purely a legibility aid for the countdown — it
 * carries no view on whether the bet is good, and must never be styled with
 * the palette used for validation state.
 */
export function urgency(minutes: number | null): "soon" | "today" | "later" {
  if (minutes == null) return "later";
  if (minutes <= 60) return "soon";
  if (minutes <= 60 * 8) return "today";
  return "later";
}
