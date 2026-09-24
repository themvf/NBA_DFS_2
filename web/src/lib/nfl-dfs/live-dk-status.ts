/**
 * DraftKings' live status, laid over a saved slate.
 *
 * ## The problem this solves
 *
 * A salary file is a photograph. Upload Thursday's slate on Wednesday, finalise
 * lineups Thursday evening, and the `Status` column you are reading is a day
 * old -- which is most of the window in which a player gets ruled out. Week 2
 * made the cost concrete: Brock Bowers was Doubtful from early in the week and
 * Puka Nacua was a late scratch, and the workspace could not see either change
 * because it only ever held one snapshot.
 *
 * `ingest/nfl_dfs_dk_pool.py` polls DraftKings' own player pool and appends an
 * observation. This joins the two, at READ time, and prefers whichever is
 * newer. It never writes `nfl_dfs_slate_players`: that row is the record of
 * what the workspace showed when a lineup was built, and rewriting it would
 * silently restate the input to lineups already exported.
 *
 * ## Why the join is by name
 *
 * The slate stores DraftKings' *draftable* id; the live pool carries
 * DraftKings' *player* id and no draftable id at all. The endpoint that bridges
 * them returns 403. So the rules in `model/nfl_dfs_dk_pool_match.py` apply here
 * too, and this file is their TypeScript half: a repeated name is dropped
 * rather than guessed, and a pool whose salaries disagree with the slate is a
 * different slate and is not applied at all.
 */

export const LIVE_DK_STATUS_VERSION = "nfl-dfs-live-dk-status-v1";

/** Below this share of matched players agreeing on salary, refuse the whole pool. */
export const SALARY_AGREEMENT_FLOOR = 0.95;
/** ...and a handful of matched players is not evidence either way. */
export const MIN_MATCHED_FOR_SALARY_CHECK = 10;

export interface LivePoolPlayer {
  normalizedName: string;
  name: string;
  team: string | null;
  salary: number | null;
  status: string | null;
  isDisabled: boolean;
}

export interface LivePool {
  draftGroupId: number;
  format: string;
  teams: string[];
  capturedAt: Date;
  players: LivePoolPlayer[];
}

export interface SlateRowForLive {
  normalizedName: string;
  salary: number;
  dkStatus: string | null;
}

export interface LiveStatusChange {
  name: string;
  team: string | null;
  from: string | null;
  to: string | null;
}

export interface LiveStatusOverlay {
  version: string;
  applied: boolean;
  reason: string;
  draftGroupId: number | null;
  capturedAt: string | null;
  /** normalized name -> DraftKings' current tag (null = no tag). Only unambiguous matches. */
  statuses: Map<string, string | null>;
  matched: number;
  salaryAgreement: number | null;
  ambiguousNames: string[];
  changes: LiveStatusChange[];
}

export const EMPTY_LIVE_OVERLAY: LiveStatusOverlay = {
  version: LIVE_DK_STATUS_VERSION,
  applied: false,
  reason: "No live DraftKings pool has been captured for this slate.",
  draftGroupId: null,
  capturedAt: null,
  statuses: new Map(),
  matched: 0,
  salaryAgreement: null,
  ambiguousNames: [],
  changes: [],
};

function indexUnique<T extends { normalizedName: string }>(rows: readonly T[]) {
  const index = new Map<string, T>();
  const duplicates = new Set<string>();
  for (const row of rows) {
    if (!row.normalizedName) continue;
    if (index.has(row.normalizedName)) duplicates.add(row.normalizedName);
    index.set(row.normalizedName, row);
  }
  // Two players sharing a name is exactly the case where a tiebreak is worst.
  for (const key of duplicates) index.delete(key);
  return { index, duplicates: [...duplicates].sort() };
}

/**
 * Resolve one live pool against one saved slate, or refuse and say why.
 *
 * `uploadedAt` is when the salary file was read. An observation captured before
 * it tells us nothing the file did not already say.
 */
export function buildLiveStatusOverlay(
  slateRows: readonly SlateRowForLive[],
  slateFormat: string,
  slateTeams: readonly string[],
  pool: LivePool | null,
  uploadedAt: Date,
): LiveStatusOverlay {
  if (!pool) return EMPTY_LIVE_OVERLAY;
  const refuse = (reason: string, extra: Partial<LiveStatusOverlay> = {}): LiveStatusOverlay =>
    ({ ...EMPTY_LIVE_OVERLAY, reason, draftGroupId: pool.draftGroupId, ...extra });

  if (pool.format !== slateFormat) return refuse(`Live pool is ${pool.format}; this slate is ${slateFormat}.`);
  if ([...pool.teams].sort().join(",") !== [...slateTeams].sort().join(","))
    return refuse("Live pool covers a different set of teams than this slate.");
  if (pool.capturedAt.getTime() <= uploadedAt.getTime())
    return refuse("The live pool is no newer than the uploaded salary file.");

  const slate = indexUnique(slateRows);
  const live = indexUnique(pool.players);
  const shared = [...slate.index.keys()].filter((key) => live.index.has(key)).sort();
  if (!shared.length) return refuse("No player on this slate could be matched to the live pool.");

  const agree = shared.filter((key) => slate.index.get(key)!.salary === live.index.get(key)!.salary).length;
  const agreement = agree / shared.length;
  if (shared.length >= MIN_MATCHED_FOR_SALARY_CHECK && agreement < SALARY_AGREEMENT_FLOOR)
    return refuse(
      `Salaries disagree on ${shared.length - agree} of ${shared.length} matched players — `
      + "this is a different slate, so none of it was applied.",
      { matched: shared.length, salaryAgreement: agreement },
    );

  const statuses = new Map<string, string | null>();
  const changes: LiveStatusChange[] = [];
  for (const key of shared) {
    const livePlayer = live.index.get(key)!;
    // DraftKings blocking a player from being drafted is a stronger statement
    // than any tag, and it is the one case where an empty tag still means out.
    const tag = livePlayer.isDisabled ? (livePlayer.status ?? "OUT") : livePlayer.status;
    statuses.set(key, tag);
    const before = (slate.index.get(key)!.dkStatus ?? "").trim().toUpperCase() || null;
    const after = (tag ?? "").trim().toUpperCase() || null;
    // Compare what a lineup would DO about the tag, not how it is spelled.
    // The salary file writes "OUT" and the live feed writes "O" for the same
    // player, so a raw string comparison announced three changes on the
    // Thursday ATL@GB slate where nothing had actually changed -- measured, not
    // hypothetical. A banner that cries wolf on spelling is worse than none.
    if (statusClass(before) !== statusClass(after))
      changes.push({ name: livePlayer.name, team: livePlayer.team, from: before, to: after });
  }

  return {
    version: LIVE_DK_STATUS_VERSION,
    applied: true,
    reason: `DraftKings' live pool, matched on ${shared.length} players.`,
    draftGroupId: pool.draftGroupId,
    capturedAt: pool.capturedAt.toISOString(),
    statuses,
    matched: shared.length,
    salaryAgreement: agreement,
    ambiguousNames: [...new Set([...slate.duplicates, ...live.duplicates])].sort(),
    changes: changes.sort((a, b) => a.name.localeCompare(b.name)),
  };
}

/** DraftKings tags that mean "not playing" — the same set the salary parser uses. */
export const LIVE_OUT_STATUSES = new Set(["O", "OUT", "IR", "PUP", "SUSP", "NA"]);

/**
 * What a lineup would DO about a tag.
 *
 * DraftKings spells the same fact differently in different places -- "OUT" in
 * the salary file, "O" in the live pool -- and IR, PUP and SUSP are all "he is
 * not playing" too. Grouping them is what lets the change list mean "something
 * happened" rather than "a string differs".
 */
export type StatusClass = "out" | "doubtful" | "questionable" | "none";

export function statusClass(status: string | null | undefined): StatusClass {
  const tag = (status ?? "").trim().toUpperCase();
  if (!tag) return "none";
  if (LIVE_OUT_STATUSES.has(tag)) return "out";
  if (tag === "D" || tag === "DOUBTFUL") return "doubtful";
  if (tag === "Q" || tag === "GTD" || tag === "QUESTIONABLE") return "questionable";
  return "none";
}

export const isLiveOutStatus = (status: string | null | undefined): boolean =>
  LIVE_OUT_STATUSES.has((status ?? "").trim().toUpperCase());
