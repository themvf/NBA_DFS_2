/**
 * The stat-api pull behind `nfl-ownership-outside-v1`: find stat-api's slate
 * for one of ours and read its projected ownership. Server only; the key is
 * `STAT_API_KEY` and goes nowhere but stat-api. Run by hand (a button or the
 * pull script), never on a schedule.
 *
 * The slate is matched by DraftKings player ids, which are unique to a draft
 * group, so a slate with the same teams but another format cannot match.
 */
import { MIN_SLATE_OVERLAP, parseStatApiOwnership, slateOverlap, type OutsideOwnershipMeta, type OutsideOwnershipRow } from "./outside-ownership";

const BASE = "https://api.stat-api.com/api/v1/dfs";

export function statApiConfigured(): boolean {
  return Boolean(process.env.STAT_API_KEY?.trim());
}

async function get(path: string): Promise<unknown> {
  const key = process.env.STAT_API_KEY?.trim();
  if (!key) throw new Error("STAT_API_KEY is not set here, so stat-api cannot be read.");
  const response = await fetch(`${BASE}${path}`, { headers: { Authorization: `Bearer ${key}`, "User-Agent": "dfs-research-client" },
    cache: "no-store", signal: AbortSignal.timeout(45_000) });
  if (!response.ok) throw new Error(`stat-api answered ${response.status} for ${path.split("?")[0]}.`);
  return response.json();
}

export type StatApiPull = { rows: OutsideOwnershipRow[]; meta: OutsideOwnershipMeta; problems: string[]; overlap: number };

/**
 * `slateDate` is the slate's Eastern date (stat-api files a Thursday 8:15 PM
 * game under Thursday). Candidates are full-game DraftKings slates of the
 * same format and game count, nearest kickoff first.
 */
export async function pullStatApiOwnership(input: { slateDate: string; format: "classic" | "showdown"; gameCount: number; firstKickoff: string | null; dkPlayerIds: number[] }): Promise<StatApiPull> {
  const listed = await get(`/slates?date=${input.slateDate}&operator_id=1&sport=nfl`);
  const slates = (Array.isArray(listed) ? listed : (listed as { slates?: unknown[] })?.slates ?? []) as Record<string, unknown>[];
  const kickoff = input.firstKickoff ? Date.parse(input.firstKickoff) : NaN;
  const candidates = slates
    .filter((s) => s.game_type === input.format && s.part_of_game === "full" && Number(s.game_count) === input.gameCount)
    .sort((a, b) => Math.abs(Date.parse(String(a.start_time)) - kickoff) - Math.abs(Date.parse(String(b.start_time)) - kickoff))
    .slice(0, 12);
  if (!candidates.length) throw new Error(`stat-api lists no ${input.format} slate with ${input.gameCount} game(s) on ${input.slateDate}.`);
  let best = 0;
  for (const candidate of candidates) {
    const projections = await get(`/slates/${candidate.id}/projections`);
    const overlap = slateOverlap(input.dkPlayerIds, projections);
    best = Math.max(best, overlap);
    if (overlap < MIN_SLATE_OVERLAP) continue;
    const ownership = await get(`/slates/${candidate.id}/ownership`);
    return { ...parseStatApiOwnership(ownership, projections, input.format), overlap };
  }
  throw new Error(`No stat-api slate on ${input.slateDate} has this slate's players (best match ${(best * 100).toFixed(0)}% of them). Nothing was saved.`);
}
