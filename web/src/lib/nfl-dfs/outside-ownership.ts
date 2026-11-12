/**
 * Outside ownership (`nfl-ownership-outside-v1`): a pull of stat-api's
 * projected field ownership for one slate, stored per slate and read in place
 * of our own ownership model for the players it covers. Owner's decision
 * 2026-11-12, as a trial: on 20 past Showdown flagships stat-api's stored
 * pre-game ownership was closer to real ownership than our fitted model
 * (flex miss 2.09 vs 3.59 points per player, closer on all 20), and our model
 * stayed in a 15-29% band for the quarterbacks' share of captains against a
 * real 10-39%. Not shown to improve payouts; that is untested.
 *
 * Pure. The pull itself is `statapi-ownership-client.ts`; the stored row and
 * the page wiring are in the NFL DFS actions.
 *
 * Rules:
 * - stat-api's number is used for every active player it gives more than 0%.
 * - An active player it leaves at 0% (or leaves out) keeps OUR number when
 *   ours is at least `KEEP_OURS_MIN_PCT`: on its first slate stat-api had two
 *   players it projected to play at 0%. The stat-api players are then scaled
 *   down in that slot so the slate's total stays where stat-api had it.
 * - A player we have ruled out gets nothing; what stat-api had on him is
 *   reported, never handed to anyone else.
 * - Showdown keeps captain and flex separate, and a player's two shares are
 *   held to 95% together (one slot per lineup), the cap our own model uses.
 */

export const NFL_OWNERSHIP_OUTSIDE_VERSION = "nfl-ownership-outside-v1";
export const NFL_OWNERSHIP_STATAPI_SOURCE = "stat-api";
/** Our total ownership at or above which a player stat-api has at 0% keeps our number. */
export const KEEP_OURS_MIN_PCT = 1;
/** Captain plus flex for one player (one slot per lineup). */
const SHOWDOWN_TOTAL_CAP = 95;
/** Share of the slate's DraftKings ids a stat-api slate must contain to be the same slate. */
export const MIN_SLATE_OVERLAP = 0.8;

export type OutsideOwnershipRow = { dkPlayerId: number; captainPct: number | null; flexPct: number | null; totalPct: number };

export type OutsideOwnershipMeta = {
  slateId: number | null; slateName: string | null; contestId: number | null; contestName: string | null;
  model: string | null; asOf: string | null;
};

type Owned = {
  dkPlayerId: number; name: string; isOut: boolean;
  ownPct?: number | null; ownSource?: string | null; captainOwnPct?: number | null; flexOwnPct?: number | null;
  modelCaptainOwnPct?: number | null; modelFlexOwnPct?: number | null;
};

/** Our own model's split, carried on every player the function returns. */
type ModelSplit = { modelCaptainOwnPct: number | null; modelFlexOwnPct: number | null };

export type OutsideOwnershipReport = {
  /** Active players now on stat-api's number. */
  applied: number;
  /** Active players stat-api had at 0% (or not at all) who keep our number. */
  keptOurs: { name: string; ourPct: number }[];
  /** Players we have ruled out that stat-api still gave ownership. */
  ruledOutWithOwnership: { name: string; theirPct: number }[];
  /** Slot totals after the rules above, in percent. */
  captainTotal: number | null;
  flexTotal: number;
};

const round1 = (n: number) => Math.round(n * 10) / 10;
const num = (v: unknown): number | null => {
  const n = typeof v === "number" ? v : typeof v === "string" && v.trim() !== "" ? Number(v) : NaN;
  return Number.isFinite(n) ? n : null;
};

/**
 * Read stat-api's two payloads for one slate into rows keyed by DraftKings id.
 * The ownership payload is keyed by stat-api's own player id; the projections
 * payload carries the DraftKings export id for the same player. On a Showdown
 * slate each player has a CPT row and a flex row; the flex row's export id is
 * the one our slate uses.
 */
export function parseStatApiOwnership(ownership: unknown, projections: unknown, format: "classic" | "showdown"): { rows: OutsideOwnershipRow[]; meta: OutsideOwnershipMeta; problems: string[] } {
  const problems: string[] = [];
  const own = (ownership ?? {}) as Record<string, unknown>;
  const proj = (projections ?? {}) as Record<string, unknown>;
  const slate = (own.slate ?? {}) as Record<string, unknown>;
  const contest = (own.contest ?? {}) as Record<string, unknown>;
  const contests = Array.isArray(own.contests) ? own.contests as Record<string, unknown>[] : [];
  const contestId = num(contest.id);
  const meta: OutsideOwnershipMeta = {
    slateId: num(slate.id), slateName: typeof slate.name === "string" ? slate.name : null, contestId,
    contestName: (contests.find((c) => num(c.id) === contestId)?.name as string | undefined) ?? null,
    model: typeof own.model === "string" ? own.model : null, asOf: typeof own.as_of === "string" ? own.as_of : null,
  };
  const dkIdByPlayer = new Map<number, number>();
  for (const entry of Array.isArray(proj.projections) ? proj.projections as Record<string, unknown>[] : []) {
    const player = (entry.player ?? {}) as Record<string, unknown>;
    if (player.position === "CPT") continue;
    const playerId = num(player.player_id), dkId = num(player.operator_player_export_id);
    if (playerId != null && dkId != null) dkIdByPlayer.set(playerId, dkId);
  }
  const players = Array.isArray(contest.players) ? contest.players as Record<string, unknown>[] : [];
  if (!players.length) problems.push("stat-api returned no ownership for this slate's main contest.");
  const rows: OutsideOwnershipRow[] = [];
  let unmapped = 0;
  for (const p of players) {
    const playerId = num(p.player_id);
    const dkId = playerId == null ? undefined : dkIdByPlayer.get(playerId);
    if (dkId == null) { unmapped += 1; continue; }
    const captain = format === "showdown" ? num(p.captain_pct) : null;
    const flex = num(p.flex_pct);
    const total = num(p.total_pct) ?? (captain ?? 0) + (flex ?? 0);
    if (format === "showdown" && (captain == null || flex == null)) { problems.push(`stat-api has no captain and flex split for player ${playerId}.`); continue; }
    if ([captain, flex, total].some((v) => v != null && (v < 0 || v > 100))) { problems.push(`stat-api ownership for player ${playerId} is outside 0-100%.`); continue; }
    rows.push({ dkPlayerId: dkId, captainPct: captain, flexPct: format === "showdown" ? flex : null, totalPct: total });
  }
  if (unmapped) problems.push(`${unmapped} stat-api player(s) had no DraftKings id and were left out.`);
  return { rows, meta, problems };
}

/** Share of our slate's DraftKings ids that appear in a stat-api projections payload. */
export function slateOverlap(dkPlayerIds: readonly number[], projections: unknown): number {
  const ours = new Set(dkPlayerIds);
  if (!ours.size) return 0;
  const theirs = new Set<number>();
  for (const entry of Array.isArray((projections as Record<string, unknown> | null)?.projections) ? (projections as { projections: Record<string, unknown>[] }).projections : []) {
    const id = num(((entry.player ?? {}) as Record<string, unknown>).operator_player_export_id);
    if (id != null) theirs.add(id);
  }
  let hit = 0;
  for (const id of ours) if (theirs.has(id)) hit += 1;
  return hit / ours.size;
}

/**
 * Put the stored rows on the pool. Every player's own-model split is kept in
 * `modelCaptainOwnPct` / `modelFlexOwnPct` (and `ourOwnPct`, set upstream), so
 * a saved build records both and the two can be graded against the field.
 */
export function applyOutsideOwnership<P extends Owned>(players: P[], rows: readonly OutsideOwnershipRow[], format: "classic" | "showdown"): { players: (P & ModelSplit)[]; report: OutsideOwnershipReport } {
  const byId = new Map(rows.map((r) => [r.dkPlayerId, r]));
  const showdown = format === "showdown";
  const keptOurs: OutsideOwnershipReport["keptOurs"] = [];
  const ruledOutWithOwnership: OutsideOwnershipReport["ruledOutWithOwnership"] = [];
  const useTheirs = new Set<number>();
  let theirCaptain = 0, theirFlex = 0, keptCaptain = 0, keptFlex = 0;
  for (const p of players) {
    const row = byId.get(p.dkPlayerId);
    if (p.isOut) { if (row && row.totalPct > 0) ruledOutWithOwnership.push({ name: p.name, theirPct: round1(row.totalPct) }); continue; }
    if (row && row.totalPct > 0) {
      useTheirs.add(p.dkPlayerId);
      theirCaptain += row.captainPct ?? 0; theirFlex += showdown ? row.flexPct ?? 0 : row.totalPct;
    } else if ((p.ownPct ?? 0) >= KEEP_OURS_MIN_PCT) {
      keptOurs.push({ name: p.name, ourPct: round1(p.ownPct as number) });
      keptCaptain += showdown ? p.captainOwnPct ?? 0 : 0; keptFlex += showdown ? p.flexOwnPct ?? 0 : p.ownPct as number;
    }
  }
  // Make room for the players who keep our number: scale stat-api's down, never up.
  const captainScale = theirCaptain > 0 ? Math.max(0, theirCaptain - keptCaptain) / theirCaptain : 1;
  const flexScale = theirFlex > 0 ? Math.max(0, theirFlex - keptFlex) / theirFlex : 1;
  let captainTotal = 0, flexTotal = 0;
  const out = players.map((p) => {
    const base = { ...p, modelCaptainOwnPct: p.captainOwnPct ?? null, modelFlexOwnPct: p.flexOwnPct ?? null };
    if (p.isOut) return base;
    const row = byId.get(p.dkPlayerId);
    if (!useTheirs.has(p.dkPlayerId) || !row) {
      // Stat-api's 0% stands for a player we also have under the keep-ours line.
      if (row && (p.ownPct ?? 0) < KEEP_OURS_MIN_PCT) return { ...base, ownPct: 0, captainOwnPct: showdown ? 0 : null, flexOwnPct: showdown ? 0 : null, ownSource: NFL_OWNERSHIP_STATAPI_SOURCE };
      captainTotal += showdown ? p.captainOwnPct ?? 0 : 0; flexTotal += showdown ? p.flexOwnPct ?? 0 : p.ownPct ?? 0;
      return base;
    }
    if (!showdown) {
      const total = round1(row.totalPct * flexScale);
      flexTotal += total;
      return { ...base, ownPct: total, captainOwnPct: null, flexOwnPct: null, ownSource: NFL_OWNERSHIP_STATAPI_SOURCE };
    }
    const captain = round1((row.captainPct ?? 0) * captainScale);
    const flex = round1(Math.min((row.flexPct ?? 0) * flexScale, SHOWDOWN_TOTAL_CAP - captain));
    captainTotal += captain; flexTotal += flex;
    return { ...base, ownPct: round1(captain + flex), captainOwnPct: captain, flexOwnPct: flex, ownSource: NFL_OWNERSHIP_STATAPI_SOURCE };
  });
  return { players: out, report: { applied: useTheirs.size, keptOurs, ruledOutWithOwnership,
    captainTotal: showdown ? round1(captainTotal) : null, flexTotal: round1(flexTotal) } };
}
