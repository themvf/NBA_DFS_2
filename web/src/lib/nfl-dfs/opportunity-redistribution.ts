/**
 * When a player is ruled out, his opportunity goes to his teammates.
 *
 * Pure: no React, no database, no clock. One rule, applied at the slate
 * layer, so the pool table and the projection drawer cannot disagree about
 * who inherited what.
 *
 * ## Why this lives here and not only in Python
 *
 * `model/nfl_dfs_availability.py` already models this, but it cannot fire in
 * production for two independent reasons, both measured:
 *
 *  1. Its status feed is our own FantasyPros injury observations, which on a
 *     real 13-game slate carried **zero** OUT rows while DraftKings flagged
 *     77. The player we actually need to handle -- a ruled-out Nico Collins
 *     -- is known out only through DK's `Status` column, which reaches us at
 *     the slate layer, not the projection run.
 *  2. `apply()` defaults to `positions=("QB",)`, so no receiver, back or
 *     tight end has ever received a transfer regardless of the feed.
 *
 * So the redistribution has to happen where the OUT flag is known. This is a
 * read-time slate decision, exactly like `zeroOutProjection`: the immutable
 * `nfl_dfs_player_projections` row is never rewritten.
 *
 * ## The model
 *
 * Opportunity moves; efficiency does not. A backup handed 8 targets is not
 * the starter handed 8 targets -- he catches them at his own rate, for his
 * own yards. So each recipient's *volume* stats scale and his *rates* are
 * left alone, which is the same mechanic as the Python `transfer_opportunity`.
 *
 * Three independent pools, because a team's passing, rushing and receiving
 * work are separate budgets that redistribute to different people:
 *
 * | pool     | unit         | who inherits                                  |
 * |----------|--------------|-----------------------------------------------|
 * | `pass`   | `attempts`   | the single highest-volume available QB        |
 * | `rush`   | `carries`    | available RBs, proportional to their own carries |
 * | `target` | `receptions` | available WR **and** TE **and** pass-catching RB, proportional to their own receptions |
 *
 * The target pool spans the whole pass-catching group on purpose. A WR's
 * targets do not stay inside the WR room -- the tight end and the back catch
 * some of them -- and restricting recipients to the absent man's own position
 * would systematically understate the TE/RB bump that follows a receiver
 * absence.
 *
 * The pass pool is the one winner-take-all case, and that is football rather
 * than an inconsistency: only one quarterback plays.
 *
 * A player with no history in a pool inherits nothing from it. That falls out
 * of proportional allocation (a zero share receives zero) and is deliberate:
 * scaling a player who has never caught a pass produces a number backed by
 * nothing.
 *
 * ## What this deliberately does NOT do
 *
 * **It does not re-score a mean stat line.** DK's three yardage bonuses are
 * step functions, so `E[score(mean)] != E[score]` -- scoring a 96-yard mean
 * throws away real 100-yard bonus probability, and scoring a 104-yard mean
 * invents a bonus that is only ~50% likely. `scoring.ts` documents this and
 * CLAUDE.md records the same mean-versus-distribution error for MLB totals.
 * So the projection is adjusted by the *marginal* points of the inherited
 * volume, scored linearly, and added to the existing simulated projection:
 *
 *     newProjection = oldProjection + linear(scaledStats) - linear(ownStats)
 *
 * The distribution-aware work that produced `oldProjection` survives intact.
 * The cost is that the ceiling gain is understated, because a bigger workload
 * genuinely does raise bonus probability. That is the conservative direction
 * and it is flagged rather than hidden.
 *
 * **It does not re-simulate the interval.** Floor and ceiling scale by the
 * same ratio as the projection -- crude, honest, and reported as such.
 *
 * **It does not touch `boomRate`.** Boom is a distributional quantity and a
 * proportional scale would be meaningless for it.
 *
 * ## Status
 *
 * Shipped live and NOT yet validated. This is a stated model, not a measured
 * effect: the 50/50-style split questions, the cap, and the choice of
 * receptions as the target unit are all reasoned defaults awaiting a
 * backtest. Every applied transfer is reported with its inputs so the
 * decision can be graded after the week.
 */

import { scoreNflOffenseLinear } from "./scoring";

export const VERSION = "nfl-dfs-redistribution-v1";

/**
 * A backup with a handful of mop-up snaps has a noisy efficiency estimate.
 * Scaling it far enough turns that noise into a projection, so the
 * multiplier is capped and the cap is reported rather than hidden.
 * Matches `MAX_TRANSFER_MULTIPLIER` in the Python module.
 */
export const MAX_MULTIPLIER = 4.0;

export type PoolName = "pass" | "rush" | "target";

type PoolSpec = {
  /** The stat_means key standing for "how much work". */
  readonly unit: string;
  /** Positions whose absence contributes to this pool. */
  readonly donors: ReadonlySet<string>;
  /** Positions eligible to inherit from it. */
  readonly recipients: ReadonlySet<string>;
  /** Volume stats that grow with this pool's unit. */
  readonly scales: readonly string[];
  /** Winner-take-all (only one QB plays) rather than proportional. */
  readonly single: boolean;
  readonly label: string;
};

export const POOLS: Readonly<Record<PoolName, PoolSpec>> = {
  pass: {
    unit: "attempts",
    donors: new Set(["QB"]),
    recipients: new Set(["QB"]),
    // Interceptions are in here on purpose: more attempts means more chances
    // to throw one. A transfer that moved only the upside would be a cheat.
    scales: ["passing_yards", "passing_tds", "passing_interceptions", "passing_2pt_conversions"],
    single: true,
    label: "pass attempts",
  },
  rush: {
    unit: "carries",
    donors: new Set(["QB", "RB", "WR", "TE"]),
    recipients: new Set(["RB"]),
    scales: ["rushing_yards", "rushing_tds", "rushing_2pt_conversions"],
    single: false,
    label: "carries",
  },
  target: {
    // `targets` is never persisted -- the coupled simulation does not emit it
    // and only `receptions` survives into stat_means on both projection
    // paths. Receptions understate opportunity for a low-catch-rate player,
    // which is a known limitation of the unit, not of the allocation.
    unit: "receptions",
    donors: new Set(["RB", "WR", "TE"]),
    recipients: new Set(["RB", "WR", "TE"]),
    scales: ["receptions", "receiving_yards", "receiving_tds", "receiving_2pt_conversions"],
    single: false,
    label: "receptions",
  },
};

/** Scaled by total touches rather than by any single pool. */
const TOUCH_SCALED = "fumbles_lost_total";

export type RedistributionRow = {
  /** Stable identifier; `dkPlayerId` at the slate layer. */
  key: number;
  name: string;
  position: string;
  team: string;
  isOut: boolean;
  /**
   * `projection_status` from the immutable row. When it already reads `out`,
   * the Python pipeline zeroed this player upstream and his opportunity is
   * gone from `statMeans` -- so the pool is empty and nothing double-pays.
   * Carried only so the report can say which of the two happened.
   */
  projectionStatus?: string;
  statMeans: Record<string, number>;
  ourProj: number | null;
  floorFpts: number | null;
  ceilingFpts: number | null;
};

export type InheritedFrom = {
  pool: PoolName;
  /** Unit label, e.g. "receptions". */
  unit: string;
  from: string[];
  /** Opportunity units inherited, after the cap. */
  gained: number;
  /** This recipient's own opportunity before the transfer. */
  own: number;
  multiplier: number;
  cappedFrom: number | null;
};

export type RedistributionResult = {
  key: number;
  name: string;
  ourProj: number;
  floorFpts: number | null;
  ceilingFpts: number | null;
  statMeans: Record<string, number>;
  inherited: InheritedFrom[];
  pointsBefore: number;
  pointsAfter: number;
};

export type UnresolvedPool = {
  team: string;
  pool: PoolName;
  pooled: number;
  from: string[];
  reason: string;
};

export type RedistributionReport = {
  version: string;
  applied: RedistributionResult[];
  unresolved: UnresolvedPool[];
  /** OUT players who contributed no opportunity to any pool, and why. */
  donorsWithoutOpportunity: { team: string; name: string; position: string; reason: string }[];
};

const num = (value: unknown): number => {
  const parsed = typeof value === "number" ? value : Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
};

/**
 * Map Python's snake_case stat_means onto the camelCase stat line that
 * `scoring.ts` scores. Keys absent from a line contribute nothing, which is
 * the documented behaviour of every offensive term.
 */
const SCORING_KEY: Readonly<Record<string, string>> = {
  passing_yards: "passYds",
  passing_tds: "passTds",
  passing_interceptions: "interceptions",
  rushing_yards: "rushYds",
  rushing_tds: "rushTds",
  receiving_yards: "recYds",
  receiving_tds: "recTds",
  receptions: "receptions",
  fumbles_lost_total: "fumblesLost",
  special_teams_tds: "returnTds",
  fumble_recovery_tds: "offensiveFumbleRecoveryTds",
};

function linearPoints(stats: Record<string, number>): number {
  const line: Record<string, number> = {};
  let twoPoint = 0;
  for (const [key, value] of Object.entries(stats)) {
    if (key.endsWith("_2pt_conversions")) { twoPoint += num(value); continue; }
    const mapped = SCORING_KEY[key];
    if (mapped) line[mapped] = num(value);
  }
  if (twoPoint > 0) line.twoPointConversions = twoPoint;
  return scoreNflOffenseLinear(line);
}

/**
 * Hand every ruled-out player's opportunity to his available teammates.
 *
 * Returns only the rows that actually changed, plus a report of every pool
 * that could not be placed and why. A caller applies the results by `key`;
 * rows absent from `applied` are untouched.
 */
export function redistributeOutOpportunity(rows: readonly RedistributionRow[]): RedistributionReport {
  const report: RedistributionReport = {
    version: VERSION, applied: [], unresolved: [], donorsWithoutOpportunity: [],
  };

  const byTeam = new Map<string, RedistributionRow[]>();
  for (const row of rows) {
    const list = byTeam.get(row.team);
    if (list) list.push(row); else byTeam.set(row.team, [row]);
  }

  for (const [team, roster] of byTeam) {
    const absent = roster.filter(r => r.isOut);
    if (absent.length === 0) continue;
    const available = roster.filter(r => !r.isOut);

    // pool -> recipient key -> opportunity units inherited
    const gains = new Map<PoolName, Map<number, number>>();
    const sources = new Map<PoolName, string[]>();
    const contributed = new Set<number>();

    for (const [name, spec] of Object.entries(POOLS) as [PoolName, PoolSpec][]) {
      const donors = absent.filter(r => spec.donors.has(r.position) && num(r.statMeans[spec.unit]) > 0);
      const pooled = donors.reduce((sum, r) => sum + num(r.statMeans[spec.unit]), 0);
      if (pooled <= 0) continue;
      for (const donor of donors) contributed.add(donor.key);
      const donorNames = donors.map(d => d.name);

      const eligible = available.filter(r =>
        spec.recipients.has(r.position) && num(r.statMeans[spec.unit]) > 0);
      if (eligible.length === 0) {
        report.unresolved.push({
          team, pool: name, pooled: round(pooled), from: donorNames,
          reason: `no available ${[...spec.recipients].join("/")} on this team has any ${spec.label} history to scale`,
        });
        continue;
      }

      const allocation = new Map<number, number>();
      if (spec.single) {
        // Only one quarterback plays, so the load does not spread.
        const winner = eligible.reduce((best, r) =>
          num(r.statMeans[spec.unit]) > num(best.statMeans[spec.unit]) ? r : best);
        allocation.set(winner.key, pooled);
      } else {
        const total = eligible.reduce((sum, r) => sum + num(r.statMeans[spec.unit]), 0);
        for (const r of eligible) {
          allocation.set(r.key, pooled * num(r.statMeans[spec.unit]) / total);
        }
      }
      gains.set(name, allocation);
      sources.set(name, donorNames);
    }

    for (const donor of absent) {
      if (contributed.has(donor.key)) continue;
      report.donorsWithoutOpportunity.push({
        team, name: donor.name, position: donor.position,
        reason: donor.projectionStatus === "out"
          ? "the projection pipeline had already ruled him out and moved his opportunity upstream"
          : "his projection carries no opportunity history to hand on",
      });
    }

    // Apply every pool a recipient gained from, in one pass per player, so a
    // back who inherits both carries and receptions is scaled once per pool
    // rather than once overall.
    for (const player of available) {
      const inherited: InheritedFrom[] = [];
      const stats: Record<string, number> = {};
      for (const [key, value] of Object.entries(player.statMeans)) stats[key] = num(value);
      const before = { ...stats };

      let touchOwn = 0;
      let touchAfter = 0;

      for (const [name, spec] of Object.entries(POOLS) as [PoolName, PoolSpec][]) {
        const own = num(player.statMeans[spec.unit]);
        if (name !== "pass") { touchOwn += own; touchAfter += own; }
        const gained = gains.get(name)?.get(player.key) ?? 0;
        if (gained <= 0 || own <= 0) continue;

        const raw = (own + gained) / own;
        const multiplier = Math.min(raw, MAX_MULTIPLIER);
        const applied = own * (multiplier - 1);
        if (name !== "pass") touchAfter += applied;

        for (const stat of spec.scales) {
          if (stat in stats) stats[stat] *= multiplier;
        }
        // The unit itself is a volume stat whenever it is also scored
        // (receptions). `attempts` and `carries` are carried but never
        // scored, so they are updated for honesty in the audit trail.
        stats[spec.unit] = own * multiplier;

        inherited.push({
          pool: name, unit: spec.label, from: sources.get(name) ?? [],
          gained: round(applied), own: round(own),
          multiplier: round(multiplier), cappedFrom: raw > multiplier ? round(raw) : null,
        });
      }

      if (inherited.length === 0) continue;

      if (TOUCH_SCALED in stats && touchOwn > 0) {
        stats[TOUCH_SCALED] = num(before[TOUCH_SCALED]) * (touchAfter / touchOwn);
      }

      // Marginal points only -- never a re-score of the mean line. See the
      // module header for why the bonus thresholds forbid the latter.
      const delta = linearPoints(stats) - linearPoints(before);
      const oldProj = num(player.ourProj);
      const newProj = oldProj + delta;
      const ratio = oldProj > 0 ? newProj / oldProj : 1;

      report.applied.push({
        key: player.key,
        name: player.name,
        ourProj: round(newProj),
        floorFpts: player.floorFpts === null ? null : round(player.floorFpts * ratio),
        ceilingFpts: player.ceilingFpts === null ? null : round(player.ceilingFpts * ratio),
        statMeans: Object.fromEntries(Object.entries(stats).map(([k, v]) => [k, round(v)])),
        inherited,
        pointsBefore: round(oldProj),
        pointsAfter: round(newProj),
      });
    }
  }

  return report;
}

function round(value: number): number {
  return Math.round(value * 10000) / 10000;
}

/**
 * Invert the report: for each ruled-out donor, who actually received his
 * work and in what units.
 *
 * The absent player's own row cannot answer this -- it is stamped before
 * anyone is paid -- which is exactly the bug that had the UI asserting a
 * handoff that never happened. This reads the payments themselves.
 */
export function paidByDonor(report: RedistributionReport): Map<string, { paidTo: string[]; units: string[] }> {
  const byDonor = new Map<string, { paidTo: string[]; units: string[] }>();
  for (const recipient of report.applied) {
    for (const item of recipient.inherited) {
      for (const donor of item.from) {
        const entry = byDonor.get(donor) ?? { paidTo: [], units: [] };
        if (!entry.paidTo.includes(recipient.name)) entry.paidTo.push(recipient.name);
        if (!entry.units.includes(item.unit)) entry.units.push(item.unit);
        byDonor.set(donor, entry);
      }
    }
  }
  return byDonor;
}

/** One sentence a player row can show for what he picked up. */
export function inheritanceNote(inherited: readonly InheritedFrom[]): string {
  if (inherited.length === 0) return "";
  const parts = inherited.map(item => {
    const capped = item.cappedFrom === null ? ""
      : ` (capped from ${item.cappedFrom.toFixed(2)}x)`;
    return `+${item.gained.toFixed(1)} ${item.unit} from ${item.from.join(", ")}` +
      ` -- ${item.multiplier.toFixed(2)}x his own ${item.own.toFixed(1)}${capped}`;
  });
  return `Inherits opportunity from a ruled-out teammate: ${parts.join("; ")}. ` +
    `His own efficiency is unchanged; the interval is scaled proportionally, not re-simulated.`;
}
