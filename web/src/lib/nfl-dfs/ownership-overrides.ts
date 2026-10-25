/**
 * Manual ownership (`nfl-ownership-manual-v1`): the owner types a field
 * ownership for a player and the build reads it in place of the prior or the
 * bounded prior. Saved in the run's settings (`ownershipOverrides`) and in
 * each snapshot player's `ownership.source`, so Monday's grading can separate
 * hand edits from the model's numbers.
 *
 * Pure. On a Showdown slate the typed number is the player's TOTAL (captain
 * plus flex), and the stored captain/flex split is scaled to it, within the
 * 95% one-slot-per-lineup cap the prior enforces.
 */

export const NFL_OWNERSHIP_MANUAL_SOURCE = "manual";

export type OwnershipOverrides = Record<string, number>;

type Owned = { dkPlayerId: number; ownPct?: number | null; ownSource?: string | null; captainOwnPct?: number | null; flexOwnPct?: number | null };

/** Keep only well-formed entries: a finite percent in [0, 100] keyed by a DK id. */
export function sanitizeOwnershipOverrides(raw: unknown): OwnershipOverrides {
  if (!raw || typeof raw !== "object") return {};
  const out: OwnershipOverrides = {};
  for (const [id, value] of Object.entries(raw as Record<string, unknown>)) {
    if (!/^\d+$/.test(id)) continue;
    const n = typeof value === "number" ? value : typeof value === "string" && value.trim() !== "" ? Number(value) : NaN;
    if (Number.isFinite(n)) out[id] = Math.max(0, Math.min(100, Math.round(n * 10) / 10));
  }
  return out;
}

export function applyOwnershipOverrides<P extends Owned>(players: P[], overrides: OwnershipOverrides | undefined, format: "classic" | "showdown"): { players: P[]; applied: number } {
  const clean = sanitizeOwnershipOverrides(overrides);
  if (!Object.keys(clean).length) return { players, applied: 0 };
  let applied = 0;
  const out = players.map((p) => {
    const typed = clean[String(p.dkPlayerId)];
    if (typed == null) return p;
    applied += 1;
    if (format !== "showdown") return { ...p, ownPct: typed, ownSource: NFL_OWNERSHIP_MANUAL_SOURCE };
    const captain = p.captainOwnPct ?? 0, flex = p.flexOwnPct ?? 0, current = captain + flex;
    const total = Math.min(typed, 95);
    const share = current > 0 ? captain / current : 0.15;
    return { ...p, ownPct: total, ownSource: NFL_OWNERSHIP_MANUAL_SOURCE,
      captainOwnPct: Math.round(total * share * 10) / 10, flexOwnPct: Math.round(total * (1 - share) * 10) / 10 };
  });
  return { players: out, applied };
}
