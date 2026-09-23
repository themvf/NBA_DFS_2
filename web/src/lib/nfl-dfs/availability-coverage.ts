/**
 * How much of this slate's availability do we actually know?
 *
 * ## The incident this exists for
 *
 * On the 2026 week-2 Sunday classic slate, every one of the 670 players came
 * back with availability status UNKNOWN and not one row marked fresh. The
 * workspace said so -- in a per-player table, inside a `<details>`, inside the
 * "Research & audit" tab, which is a different tab from the Generate button.
 * Twenty lineups were built without that ever being seen, and 44% of the
 * players we projected as active went on to record no stat line at all.
 *
 * The slate status strip did show "Our model 644/670", which reads as
 * reassuring and answers a different question: how many players we have a
 * PROJECTION for, not how many we know are PLAYING. A projection for a player
 * who is inactive is worse than no projection, because the optimizer will
 * happily roster him.
 *
 * So coverage belongs next to that number, always visible, on the same screen
 * as the button.
 *
 * ## What counts
 *
 * Two different things, kept separate because they fail separately:
 *
 *   resolved -- we have SOME status for him ("ACTIVE", "QUESTIONABLE", ...).
 *               UNKNOWN is not a status; it is the absence of one.
 *   fresh    -- that evidence is recent enough to be worth acting on.
 *
 * A slate can be fully resolved and entirely stale (last week's roster), or
 * fresh and unresolved (we fetched, and the provider had nothing). Reporting
 * one number would hide whichever failed.
 *
 * Players we have already ruled out are excluded from the denominator: their
 * availability is not in question, and counting them would make a slate look
 * better covered the more players it was missing.
 *
 * ## What this is not
 *
 * Not a projection of who will play, and not a reason to trust a slate that
 * passes. Full coverage means the feed answered, not that it was right --
 * Puka Nacua was freshly and correctly tagged QUESTIONABLE on the Monday
 * slate and still took 14 of 40 captain slots. Coverage is a floor under the
 * decision, not a verdict on it.
 */

export const AVAILABILITY_COVERAGE_VERSION = "nfl-availability-coverage-v1";

/** Below this share resolved, the slate cannot support a considered decision. */
export const RESOLVED_BLIND_THRESHOLD = 0.25;
/** Below this share resolved, it is thin enough to say so prominently. */
export const RESOLVED_THIN_THRESHOLD = 0.75;
/** Below this share of resolved players carrying fresh evidence, say so. */
export const FRESH_THIN_THRESHOLD = 0.5;

export type CoverageState = "blind" | "thin" | "adequate";

export interface CoveragePlayer {
  isOut?: boolean;
  availability?: { status?: string | null; fresh?: boolean | null } | null;
}

export interface AvailabilityCoverage {
  version: string;
  /** Players whose availability is an open question (excludes those already ruled out). */
  considered: number;
  /** ...of which we have any status at all. */
  resolved: number;
  /** ...of which the evidence is fresh. */
  fresh: number;
  ruledOut: number;
  resolvedShare: number;
  freshShare: number;
  state: CoverageState;
  /** One sentence, written for the person about to press Generate. */
  headline: string;
  /** Short label for the status strip. */
  metric: string;
}

const UNRESOLVED = new Set(["", "UNKNOWN", "NONE", "N/A"]);

function isResolved(player: CoveragePlayer): boolean {
  const status = (player.availability?.status ?? "").trim().toUpperCase();
  return status !== "" && !UNRESOLVED.has(status);
}

export function availabilityCoverage(players: readonly CoveragePlayer[]): AvailabilityCoverage {
  const open = players.filter((p) => !p.isOut);
  const considered = open.length;
  const resolved = open.filter(isResolved).length;
  // Freshness is only meaningful for a player we have a status for: "fresh
  // evidence of nothing" is not coverage.
  const fresh = open.filter((p) => isResolved(p) && p.availability?.fresh === true).length;
  const resolvedShare = considered ? resolved / considered : 0;
  const freshShare = resolved ? fresh / resolved : 0;

  let state: CoverageState = "adequate";
  if (considered === 0 || resolvedShare < RESOLVED_BLIND_THRESHOLD) state = "blind";
  else if (resolvedShare < RESOLVED_THIN_THRESHOLD || freshShare < FRESH_THIN_THRESHOLD) state = "thin";

  const headline =
    considered === 0
      ? "No players on this slate are available to assess."
      : state === "blind"
        ? `We do not know who is playing. ${resolved} of ${considered} players have any availability status at all` +
          `${resolved === 0 ? "" : `, and ${fresh} of those are fresh`}. ` +
          "Lineups built now will roster inactive players, and the optimizer cannot tell."
        : state === "thin"
          ? `Availability is thin: ${resolved} of ${considered} players have a status` +
            `, ${fresh} of them fresh. Check the roles panel before generating.`
          : `${resolved} of ${considered} players have an availability status, ${fresh} of them fresh.`;

  return {
    version: AVAILABILITY_COVERAGE_VERSION,
    considered,
    resolved,
    fresh,
    ruledOut: players.length - considered,
    resolvedShare: Number(resolvedShare.toFixed(4)),
    freshShare: Number(freshShare.toFixed(4)),
    state,
    headline,
    metric: `${resolved}/${considered}${fresh === resolved ? "" : ` · ${fresh} fresh`}`,
  };
}
