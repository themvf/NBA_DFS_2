/**
 * Suggested Showdown captain ranges -- a starting point the user edits, never a
 * setting applied behind their back.
 *
 * The rule, in full, so the numbers in the CPT boxes can always be explained:
 *
 *   who     the CHALK_CAPTAIN_COUNT chalkiest captain-eligible players -- the
 *           same set the chalk-captain lineup model restricts captains to --
 *           ranked by projected ownership when any is supplied, else by our
 *           projection. Skipped: OUT, Questionable or Doubtful (they may not
 *           take the 1.5x slot), no captain row, no positive projection.
 *   share   proportional to projection squared. Chalk captaining concentrates
 *           on the favourite faster than projection alone would suggest; the
 *           square is a stated prior, not a fitted one.
 *   range   share +/- CAPTAIN_BAND_PCT points, rounded to 5, so the optimizer
 *           still has room to follow the ceiling inside the plan.
 *
 * On the Thursday ATL@GB slate this gives Bijan 35-50, Watson 20-40 and
 * Love 20-35 -- close to the ranges chosen by hand before this existed.
 *
 * Nothing here is validated. It encodes "take the captain where the field is,
 * spread across the obvious names", which the week-2 field audit supports only
 * as far as winners being chalkier than us. The field audit grades it after.
 */
import { CHALK_CAPTAIN_COUNT } from "./archetypes";
import type { CaptainTarget } from "./generation-settings";

export const CAPTAIN_SHARE_POWER = 2;
export const CAPTAIN_BAND_PCT = 8;
export const CAPTAIN_RECOMMENDATION_VERSION = "nfl-captain-ranges-v1";

export interface CaptainCandidate {
  dkPlayerId: number;
  name: string;
  position: string;
  ourProj: number | null;
  isOut: boolean;
  captainDkPlayerId: number | null;
  availabilityStatus?: string | null;
  dkStatus?: string | null;
  linestarOwnPct?: number | null;
}

export interface CaptainRecommendationRow {
  dkPlayerId: number;
  name: string;
  sharePct: number;
  min: number;
  max: number;
}

export interface CaptainRecommendation {
  version: string;
  basis: "projected ownership" | "projection";
  rows: CaptainRecommendationRow[];
  targets: Record<string, CaptainTarget>;
}

const DOUBT = new Set(["Q", "D", "QUESTIONABLE", "DOUBTFUL", "GTD"]);
const roundTo5 = (value: number) => Math.round(value / 5) * 5;

export function recommendCaptainRanges(
  players: readonly CaptainCandidate[],
  opts: { count?: number; power?: number; band?: number } = {},
): CaptainRecommendation {
  const count = opts.count ?? CHALK_CAPTAIN_COUNT;
  const power = opts.power ?? CAPTAIN_SHARE_POWER;
  const band = opts.band ?? CAPTAIN_BAND_PCT;

  const eligible = players.filter((p) =>
    !p.isOut
    && p.captainDkPlayerId != null
    && p.position !== "K" && p.position !== "DST"
    && (p.ourProj ?? 0) > 0
    && !DOUBT.has((p.availabilityStatus ?? "").trim().toUpperCase())
    && !DOUBT.has((p.dkStatus ?? "").trim().toUpperCase()));
  const anyOwnership = eligible.some((p) => p.linestarOwnPct != null);
  const chosen = [...eligible]
    .sort((a, b) =>
      (anyOwnership ? (b.linestarOwnPct ?? -1) - (a.linestarOwnPct ?? -1) : 0)
      || (b.ourProj ?? 0) - (a.ourProj ?? 0)
      || a.dkPlayerId - b.dkPlayerId)
    .slice(0, count);

  const weights = chosen.map((p) => Math.pow(p.ourProj ?? 0, power));
  const total = weights.reduce((a, b) => a + b, 0);
  const rows = chosen.map((p, i) => {
    const sharePct = total > 0 ? (100 * weights[i]) / total : 100 / chosen.length;
    return {
      dkPlayerId: p.dkPlayerId,
      name: p.name,
      sharePct: Math.round(sharePct * 10) / 10,
      min: Math.max(0, roundTo5(sharePct - band)),
      max: Math.min(100, roundTo5(sharePct + band)),
    };
  });
  // The optimizer rejects a plan whose captain minimums exceed 100%, and the
  // chalk model needs the listed captains to be able to fill every lineup.
  // Rounding to 5 can nudge either sum; widen from the least-favoured end.
  while (rows.reduce((a, r) => a + r.min, 0) > 100) {
    const r = [...rows].reverse().find((row) => row.min > 0)!;
    r.min -= 5;
  }
  while (rows.length && rows.reduce((a, r) => a + r.max, 0) < 100) {
    const r = rows.find((row) => row.max < 100)!;
    r.max = Math.min(100, r.max + 5);
  }
  return {
    version: CAPTAIN_RECOMMENDATION_VERSION,
    basis: anyOwnership ? "projected ownership" : "projection",
    rows,
    targets: Object.fromEntries(rows.map((r) => [String(r.dkPlayerId), { min: r.min, max: r.max }])),
  };
}
