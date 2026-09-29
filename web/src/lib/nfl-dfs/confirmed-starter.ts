/**
 * A starting quarterback confirmed by the person building the slate.
 *
 * The QB promotion in `opportunity-redistribution` fires only while the
 * ruled-out quarterback is still QB1 on the Sleeper depth chart. Sleeper moves
 * an injured starter down as soon as he is ruled out, which blocks the
 * promotion exactly when it is needed, and the reordered chart can name the
 * wrong replacement. PHI@CHI, 2026-09-28: Caleb Williams OUT, chart
 * Keenum 1 / Bagent 2 / Williams 3, actual starter Bagent, who stayed at a
 * backup's 3.5 points.
 *
 * This rewrites only the named team's quarterback roles so the existing,
 * tested promotion does the arithmetic. It never invents a donor: the donor is
 * the ruled-out quarterback with the most observed attempts. With no
 * ruled-out quarterback it changes nothing, because there is no workload to
 * move.
 */
import { hasObservedOpportunity, type RedistributionRow } from "./opportunity-redistribution";
import type { Availability } from "./availability";

/** Statuses that mean a quarterback is not playing, as opposed to listed behind someone. */
export const INJURED_STATUSES: ReadonlySet<string> = new Set(["OUT", "IR", "PUP", "NFI", "SUSPENDED", "INACTIVE"]);

/**
 * The confirmation also decides who may be rostered. The depth chart blocks
 * every QB listed below QB1, so without this the confirmed starter would be
 * blocked as a backup while the chart's QB1 stayed eligible. A ruled-out
 * player (injury status or DraftKings OUT) is never cleared.
 */
export function confirmStarterAvailability(availability: Availability, player: { dkPlayerId: number; team: string; position: string; platformOut: boolean },
  starters: ConfirmedStartingQbs, starterName: (team: string) => string | undefined): Availability {
  const starter = starters[player.team];
  if (player.position !== "QB" || starter == null) return availability;
  const injured = player.platformOut || INJURED_STATUSES.has(availability.status) || availability.blockedReason?.startsWith("Unavailable") === true;
  if (injured) return availability;
  // The role string stays the canonical "Expected starter · QB1": other
  // checks (the workload source) compare it exactly, and a decorated label
  // silently failed them. The override is recorded in `source`, `warnings`
  // and `chartRole` instead, so the page can show what the chart said.
  const chartRole = availability.chartRole ?? availability.role;
  if (player.dkPlayerId === starter) return { ...availability, role: "Expected starter · QB1", chartRole, blockedReason: null,
    source: "Starting QB confirmed in the build form", confirmedStarter: true,
    warnings: [...(availability.warnings ?? []), `Starter confirmed in the build form; the depth chart listed him as ${chartRole}.`] };
  return { ...availability, role: "Backup · starter confirmed", chartRole,
    blockedReason: availability.blockedReason ?? `${starterName(player.team) ?? "Another quarterback"} is the confirmed starter; starter workload not supported` };
}

/** Team code -> DraftKings player id of the confirmed starter. */
export type ConfirmedStartingQbs = Record<string, number>;

export type ConfirmedStarterReport = {
  applied: { team: string; starter: string; donor: string }[];
  rejected: { team: string; reason: string }[];
};

/** Keep only well-formed entries; the browser is not trusted to send them. */
export function sanitizeConfirmedStartingQbs(value: unknown): ConfirmedStartingQbs {
  if (!value || typeof value !== "object" || Array.isArray(value)) return {};
  return Object.fromEntries(Object.entries(value as Record<string, unknown>)
    .filter(([team, id]) => /^[A-Z]{2,4}$/.test(team) && Number.isSafeInteger(id) && (id as number) > 0)) as ConfirmedStartingQbs;
}

const attempts = (row: RedistributionRow) => {
  const value = Number(row.statMeans.attempts);
  return Number.isFinite(value) ? value : 0;
};

/**
 * `injured` names the ruled-out players. Backups blocked by the confirmation
 * are also `isOut`, but they are not playing by choice, not injury, and their
 * work is not the starter's to hand on.
 */
export function applyConfirmedStartingQbs(rows: readonly RedistributionRow[], starters: ConfirmedStartingQbs,
  injured: (row: RedistributionRow) => boolean = (row) => row.isOut):
  { rows: RedistributionRow[]; report: ConfirmedStarterReport } {
  const report: ConfirmedStarterReport = { applied: [], rejected: [] };
  const roles = new Map<number, Pick<RedistributionRow, "depthOrder" | "canDonate">>();
  for (const [team, starterKey] of Object.entries(starters)) {
    const qbs = rows.filter((row) => row.team === team && row.position === "QB");
    const starter = qbs.find((row) => row.key === starterKey && !row.isOut);
    if (!starter) { report.rejected.push({ team, reason: "The confirmed starter is not an available quarterback on this team." }); continue; }
    const donor = qbs.filter((row) => row.isOut && injured(row) && hasObservedOpportunity(row) && attempts(row) > 0)
      .sort((a, b) => attempts(b) - attempts(a))[0];
    if (!donor) { report.rejected.push({ team, reason: "Backups are blocked. No ruled-out quarterback has a workload to hand on, so the starter keeps his own projection." }); continue; }
    for (const qb of qbs) {
      roles.set(qb.key, qb.key === donor.key ? { depthOrder: 1, canDonate: true }
        : qb.key === starter.key ? { depthOrder: 2, canDonate: false }
        : { depthOrder: null, canDonate: false });
    }
    report.applied.push({ team, starter: starter.name, donor: donor.name });
  }
  return { rows: rows.map((row) => roles.has(row.key) ? { ...row, ...roles.get(row.key) } : row), report };
}
