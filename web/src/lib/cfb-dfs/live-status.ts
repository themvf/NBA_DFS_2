/**
 * DraftKings' live CFB player statuses, matched to an uploaded slate.
 *
 * Two public endpoints, no login (the same pair the NFL poller uses; the
 * api.draftkings.com draftables endpoint returns 403):
 *   www.draftkings.com/lobby/getcontests?sport=CFB       -> draft groups
 *   www.draftkings.com/lineup/getavailableplayers?draftGroupId=N -> the pool
 * The pool carries no draftable ID (`did` is 0), so players are matched to the
 * salary file by normalized name + team, and only when that pair is unique on
 * both sides. A status that cannot be matched is left alone, never guessed.
 *
 * Measured limit (2026-09-25): DraftKings left Braxton Woodson tagged Q through
 * a game he did not play. A tag says what DraftKings knows, not who starts.
 */
import { normalizeName } from "./projection";

export const CFB_LOBBY_URL = "https://www.draftkings.com/lobby/getcontests?sport=CFB";
export const cfbPoolUrl = (draftGroupId: number) => `https://www.draftkings.com/lineup/getavailableplayers?draftGroupId=${draftGroupId}`;
export const DK_HEADERS = { "User-Agent": "Mozilla/5.0", Accept: "application/json" };
/** DraftKings' contest type for multi-game CFB Classic (95 is single-game Showdown). */
export const CFB_CLASSIC_CONTEST_TYPE = 94;
/** A pool must contain at least this share of the slate's players to be the slate's draft group. */
export const MIN_POOL_OVERLAP = 0.8;
export const STATUS_MAX_AGE_MINUTES = 10;
/** Tags that mean "do not play him". */
export const UNAVAILABLE = new Set(["O", "OUT", "D", "DOUBTFUL", "IR"]);

export interface LobbyGroup { draftGroupId: number; contestTypeId: number; start: string | null; gameCount: number | null; suffix: string }
export interface PoolPlayer { key: string; team: string; status: string; name: string; salary: number | null; disabled: boolean }

export function parseLobby(payload: unknown): LobbyGroup[] {
  const groups = ((payload as { DraftGroups?: Array<Record<string, unknown>> })?.DraftGroups ?? []);
  return groups.filter((g) => Number(g.ContestTypeId) === CFB_CLASSIC_CONTEST_TYPE).map((g) => ({
    draftGroupId: Number(g.DraftGroupId), contestTypeId: Number(g.ContestTypeId),
    start: g.StartDate ? String(g.StartDate).replace(/\.(\d{3})\d*Z$/, ".$1Z") : null,
    gameCount: g.GameCount == null ? null : Number(g.GameCount), suffix: String(g.ContestStartTimeSuffix ?? "").trim(),
  }));
}

export function parsePool(payload: unknown): PoolPlayer[] {
  const list = ((payload as { playerList?: Array<Record<string, unknown>> })?.playerList ?? []);
  return list.map((p) => {
    const home = p.tid === p.htid;
    const name = [p.fn, p.ln].filter(Boolean).join(" ").trim();
    return { key: normalizeName(name), team: String((home ? p.htabbr : p.atabbr) ?? ""), name,
      status: canonicalStatus(String(p.i ?? "")), salary: typeof p.s === "number" ? p.s : null,
      disabled: Boolean(p.IsDisabledFromDrafting) };
  });
}

const pairKey = (key: string, team: string) => `${key}|${team}`;

/** Share of the slate's players found in this pool by name + team. */
export function poolOverlap(slate: ReadonlyArray<{ name: string; team: string }>, pool: readonly PoolPlayer[]): number {
  const inPool = new Set(pool.map((p) => pairKey(p.key, p.team)));
  if (!slate.length) return 0;
  return slate.filter((s) => inPool.has(pairKey(normalizeName(s.name), s.team))).length / slate.length;
}

export interface StatusMatch { dkId: number; status: string }

/** Live status for each slate player, only where name + team is unique on both sides. */
export function matchStatuses(slate: ReadonlyArray<{ dkId: number; name: string; team: string }>, pool: readonly PoolPlayer[]):
  { matches: StatusMatch[]; unmatched: number } {
  const count = <T,>(items: readonly T[], key: (t: T) => string) => {
    const m = new Map<string, number>();
    for (const item of items) m.set(key(item), (m.get(key(item)) ?? 0) + 1);
    return m;
  };
  const poolCounts = count(pool, (p) => pairKey(p.key, p.team));
  const slateCounts = count(slate, (s) => pairKey(normalizeName(s.name), s.team));
  const byPair = new Map(pool.map((p) => [pairKey(p.key, p.team), p] as const));
  const matches: StatusMatch[] = [];
  let unmatched = 0;
  for (const s of slate) {
    const k = pairKey(normalizeName(s.name), s.team);
    const hit = byPair.get(k);
    if (!hit || poolCounts.get(k) !== 1 || slateCounts.get(k) !== 1) { unmatched += 1; continue; }
    matches.push({ dkId: s.dkId, status: hit.status });
  }
  return { matches, unmatched };
}

/**
 * One spelling per status. The salary file writes OUT where the live feed
 * writes O; comparing them raw reported 8 false "changes" on 2026-09-25.
 */
const CANONICAL: Record<string, string> = {
  OUT: "O", O: "O", DOUBTFUL: "D", D: "D", QUESTIONABLE: "Q", Q: "Q", GTD: "Q", PROBABLE: "P", P: "P", IR: "IR",
};
export const canonicalStatus = (status: string | null | undefined) => {
  const s = (status ?? "").trim().toUpperCase();
  return CANONICAL[s] ?? s;
};

export const effectiveStatus = (csvStatus: string, liveStatus: string | null | undefined) =>
  canonicalStatus(liveStatus ?? csvStatus);

/** Only players whose game has not kicked off may take a new status: a played game is frozen. */
export const openForStatus = (kickoff: string | null, now: number) => kickoff == null || Date.parse(kickoff) > now;
