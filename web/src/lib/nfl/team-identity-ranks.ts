import type { NflIdentityTotals } from "@/db/nfl-team-identity";

/**
 * League ranks and a style tag per team, from season-to-date totals
 * (getNflIdentityTotals). Context for the pick'em live board, not a pick
 * signal: the pick'em studies found EPA separates winners best but is what the
 * line already prices, and week-to-week form predicted nothing.
 */

/** Fewer scrimmage plays than this (about two games) gets no rank, never a zero. */
export const MIN_IDENTITY_PLAYS = 100;

/**
 * Pass rate over expected (nflverse pass_oe, percentage points) cut points:
 * the 2025 regular-season team terciles, -3.9 and -0.9, rounded. The league
 * mean sits below zero, so "balanced" is centred on about -2.4, not 0.
 */
export const RUN_HEAVY_MAX_PASS_OE = -4;
export const PASS_HEAVY_MIN_PASS_OE = -1;

export type TeamStyle = "pass-heavy" | "balanced" | "run-heavy";

export type TeamIdentity = {
  team: string;
  games: number;
  /** False when the sample is too small to rank; every rank is then null. */
  enough: boolean;
  offenseEpa: number | null;
  defenseEpaAllowed: number | null;
  /** 1 = best offense (highest EPA/play). */
  offenseRank: number | null;
  /** 1 = best defense (lowest EPA/play allowed). */
  defenseRank: number | null;
  passOe: number | null;
  style: TeamStyle | null;
  explosiveRate: number | null;
  touchdownDriveRate: number | null;
  threeAndOutRate: number | null;
  /** Teams that were ranked, the denominator for "#5 of N". */
  rankedTeams: number;
};

const rate = (numerator: number, denominator: number): number | null => denominator > 0 ? numerator / denominator : null;

export function styleOf(passOe: number | null): TeamStyle | null {
  if (passOe == null) return null;
  if (passOe <= RUN_HEAVY_MAX_PASS_OE) return "run-heavy";
  if (passOe >= PASS_HEAVY_MIN_PASS_OE) return "pass-heavy";
  return "balanced";
}

/** Competition ranking (ties share the better rank), best first. */
function ranks(values: Array<[string, number]>, higherIsBetter: boolean): Map<string, number> {
  const sorted = [...values].sort((a, b) => higherIsBetter ? b[1] - a[1] : a[1] - b[1]);
  const out = new Map<string, number>();
  sorted.forEach(([team, value], i) => {
    const prev = sorted[i - 1];
    out.set(team, prev && prev[1] === value ? out.get(prev[0])! : i + 1);
  });
  return out;
}

export function rankTeamIdentities(totals: NflIdentityTotals[]): Record<string, TeamIdentity> {
  const base = totals.map(t => {
    const enough = t.offensePlays >= MIN_IDENTITY_PLAYS && t.defensePlays >= MIN_IDENTITY_PLAYS;
    return {
      t, enough,
      offenseEpa: rate(t.offenseEpaSum, t.offenseEpaCount),
      defenseEpaAllowed: rate(t.defenseEpaSum, t.defenseEpaCount),
    };
  });
  const ranked = base.filter(b => b.enough && b.offenseEpa != null && b.defenseEpaAllowed != null);
  const offense = ranks(ranked.map(b => [b.t.team, b.offenseEpa!]), true);
  const defense = ranks(ranked.map(b => [b.t.team, b.defenseEpaAllowed!]), false);
  const out: Record<string, TeamIdentity> = {};
  for (const b of base) {
    const passOe = rate(b.t.passOeSum, b.t.passOeCount);
    out[b.t.team] = {
      team: b.t.team,
      games: b.t.games,
      enough: offense.has(b.t.team),
      offenseEpa: b.offenseEpa,
      defenseEpaAllowed: b.defenseEpaAllowed,
      offenseRank: offense.get(b.t.team) ?? null,
      defenseRank: defense.get(b.t.team) ?? null,
      passOe,
      style: b.enough ? styleOf(passOe) : null,
      explosiveRate: rate(b.t.offenseExplosives, b.t.offensePlays),
      touchdownDriveRate: rate(b.t.touchdownDrives, b.t.drives),
      threeAndOutRate: rate(b.t.threeAndOutDrives, b.t.drives),
      rankedTeams: ranked.length,
    };
  }
  return out;
}
