/**
 * Pull stat-api's projected GPP ownership for one of our NBA DraftKings
 * slates and store it on `dk_players.statapi_own_pct` (`nba-statapi-ownership-v1`).
 *
 * Server only; the key is `STAT_API_KEY` and goes nowhere but stat-api. Runs
 * when the optimizer starts on an NBA slate, at most once every
 * `REFRESH_MINUTES`, never on a schedule. Cost: one record per slate stat-api
 * lists on the date (the slate list is metered), the projections are free.
 *
 * The slate is matched by DraftKings draftable ids (our `dk_player_id`,
 * stat-api's `operator_player_export_id`), which are unique to a draft group,
 * so a slate with the same teams in another format cannot match.
 *
 * Which ownership: stat-api projects ownership per contest profile. We read
 * the large multi-entry GPP profile (gpp, 10k-100k entries, 20+ entries per
 * user), the one the 2026-11-14 winners study validated: Spearman 0.93 with
 * real flagship ownership on Oct 24, field-average forecast within 6 points.
 */
import { sql } from "drizzle-orm";
import { db } from "@/db";
import { parseStatApiNbaOwnership } from "./ownership-input";

export const NBA_STATAPI_OWNERSHIP_VERSION = "nba-statapi-ownership-v1";
const BASE = "https://api.stat-api.com/api/v1/dfs";
const REFRESH_MINUTES = 10;
const MIN_SLATE_OVERLAP = 0.8;
/**
 * The whole refresh (slate list plus every projections call) gets this long.
 * It runs before every NBA build, so a slow stat-api must not eat the build's
 * time; past the budget the build goes ahead on the fallback and says so.
 */
const TOTAL_BUDGET_MS = 12_000;
const PER_CALL_MS = 8_000;
const MAX_CANDIDATES = 6;

export type NbaStatApiOwnershipResult = {
  ok: boolean;
  /** True when a fresh stored pull was reused. */
  skipped?: boolean;
  message: string;
  statapiSlateId?: number;
  matched?: number;
  overlap?: number;
};

export function statApiConfigured(): boolean {
  return Boolean(process.env.STAT_API_KEY?.trim());
}

async function get(path: string, deadline: number): Promise<unknown> {
  const key = process.env.STAT_API_KEY?.trim();
  if (!key) throw new Error("STAT_API_KEY is not set here, so stat-api cannot be read.");
  const left = deadline - Date.now();
  if (left <= 250) throw new Error(`stat-api ownership ran past its ${TOTAL_BUDGET_MS / 1000} s budget.`);
  const response = await fetch(`${BASE}${path}`, {
    headers: { Authorization: `Bearer ${key}`, "User-Agent": "dfs-research-client" },
    cache: "no-store",
    signal: AbortSignal.timeout(Math.min(PER_CALL_MS, left)),
  });
  if (!response.ok) throw new Error(`stat-api answered ${response.status} for ${path.split("?")[0]}.`);
  return response.json();
}

export async function refreshNbaStatApiOwnership(slateId: number, options: { force?: boolean } = {}): Promise<NbaStatApiOwnershipResult> {
  if (!statApiConfigured()) return { ok: false, message: "STAT_API_KEY is not set, so stat-api ownership was not loaded." };

  const deadline = Date.now() + TOTAL_BUDGET_MS;
  const slateRows = await db.execute<{ slateDate: string; sport: string | null; gameCount: number | null; freshAt: string | null }>(sql`
    SELECT ds.slate_date::text AS "slateDate", ds.sport, ds.game_count AS "gameCount",
      (SELECT MAX(dp.statapi_own_at)::text FROM dk_players dp WHERE dp.slate_id = ds.id) AS "freshAt"
    FROM dk_slates ds WHERE ds.id = ${slateId}
  `);
  const slate = slateRows.rows[0];
  if (!slate) return { ok: false, message: `Slate ${slateId} not found.` };
  if ((slate.sport ?? "nba") !== "nba") return { ok: false, message: "stat-api ownership is wired for NBA slates only." };
  if (!options.force && slate.freshAt && Date.now() - Date.parse(slate.freshAt) < REFRESH_MINUTES * 60_000) {
    return { ok: true, skipped: true, message: `stat-api ownership is under ${REFRESH_MINUTES} minutes old; reused.` };
  }

  const players = await db.execute<{ dkPlayerId: number }>(sql`
    SELECT dk_player_id AS "dkPlayerId" FROM dk_players WHERE slate_id = ${slateId}
  `);
  const ourIds = new Set(players.rows.map((row) => Number(row.dkPlayerId)).filter((id) => Number.isFinite(id)));
  if (ourIds.size === 0) return { ok: false, message: "This slate has no players yet." };

  const listed = await get(`/slates?date=${slate.slateDate}&operator_id=1&sport=nba`, deadline);
  const slates = (Array.isArray(listed) ? listed : (listed as { slates?: unknown[] })?.slates ?? []) as Record<string, unknown>[];
  // Same game count first (when we know ours), then the biggest pools.
  const ourGames = slate.gameCount != null ? Number(slate.gameCount) : null;
  const candidates = slates
    .filter((s) => s.game_type === "classic" && s.part_of_game === "full")
    .sort((a, b) => {
      const aMatch = ourGames != null && Number(a.game_count) === ourGames ? 0 : 1;
      const bMatch = ourGames != null && Number(b.game_count) === ourGames ? 0 : 1;
      return aMatch - bMatch || Number(b.player_count ?? 0) - Number(a.player_count ?? 0);
    });
  if (!candidates.length) return { ok: false, message: `stat-api lists no NBA Classic slate on ${slate.slateDate}.` };

  let best = 0;
  for (const candidate of candidates.slice(0, MAX_CANDIDATES)) {
    const projections = await get(`/slates/${candidate.id}/projections`, deadline);
    const ownership = parseStatApiNbaOwnership(projections);
    const matched = [...ourIds].filter((id) => ownership.has(id));
    const overlap = matched.length / ourIds.size;
    best = Math.max(best, overlap);
    if (overlap < MIN_SLATE_OVERLAP) continue;

    const values = matched.map((id) => sql`(${id}::bigint, ${ownership.get(id) as number}::real)`);
    // One statement: neon-http commits each awaited statement separately.
    await db.execute(sql`
      UPDATE dk_players dp
      SET statapi_own_pct = v.own,
          statapi_own_at = NOW(),
          statapi_slate_id = ${Number(candidate.id)}
      FROM (SELECT dk.dk_player_id, x.own
            FROM dk_players dk
            LEFT JOIN (VALUES ${sql.join(values, sql`, `)}) AS x(dk_player_id, own) ON x.dk_player_id = dk.dk_player_id
            WHERE dk.slate_id = ${slateId}) v
      WHERE dp.slate_id = ${slateId} AND dp.dk_player_id = v.dk_player_id
    `);
    return {
      ok: true,
      message: `Loaded stat-api ownership for ${matched.length} of ${ourIds.size} players (stat-api slate ${String(candidate.id)}).`,
      statapiSlateId: Number(candidate.id),
      matched: matched.length,
      overlap,
    };
  }
  return { ok: false, message: `No stat-api NBA slate on ${slate.slateDate} has this slate's players (best match ${(best * 100).toFixed(0)}%). Nothing was saved.` };
}
