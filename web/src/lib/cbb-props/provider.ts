import "server-only";
import { unstable_cache } from "next/cache";
import { CBB_PROP_MARKETS, normalizeCbbProps, parseCbbPropEvents, validCbbEventId } from "./model";

const BASE = "https://api.the-odds-api.com/v4/sports/basketball_ncaab";

async function request(path: string, params: Record<string, string> = {}): Promise<unknown> {
  const key = process.env.ODDS_API_KEY;
  if (!key) throw new Error("The CBB odds feed is not configured. Please try again after the feed is connected.");
  const query = new URLSearchParams({ apiKey: key, dateFormat: "iso", ...params });
  let response: Response;
  try { response = await fetch(`${BASE}${path}?${query}`, { cache: "no-store", signal: AbortSignal.timeout(12_000) }); }
  catch { throw new Error("The CBB odds provider could not be reached. Please try again shortly."); }
  if (!response.ok) {
    if (response.status === 401 || response.status === 403) throw new Error("The CBB odds feed could not authenticate. The feed connection needs attention.");
    if (response.status === 429) throw new Error("The odds feed is temporarily limited. Please try again later.");
    if (response.status === 404) throw new Error("That game is no longer available from the odds feed. Reload the schedule.");
    throw new Error("The CBB odds provider is temporarily unavailable. Please try again shortly.");
  }
  try { return await response.json(); } catch { throw new Error("The CBB odds provider returned an unreadable response."); }
}

export const getCbbPropSchedule = unstable_cache(async () => {
  const events = parseCbbPropEvents(await request("/events"));
  return { events, capturedAt: new Date().toISOString() };
}, ["cbb-props-schedule-v1"], { revalidate: 300 });

const cachedGame = unstable_cache(async (id: string) => {
  const { events } = await getCbbPropSchedule();
  const event = events.find(e => e.id === id);
  if (!event) throw new Error("That game is not in the current CBB schedule. Reload the schedule.");
  const raw = await request(`/events/${id}/odds`, { regions: "us", markets: Object.keys(CBB_PROP_MARKETS).join(","), oddsFormat: "american" });
  return normalizeCbbProps(raw, event, new Date().toISOString());
}, ["cbb-props-game-v1"], { revalidate: 60 });

export async function getCbbPropGame(id: string) {
  if (typeof id !== "string" || !validCbbEventId(id)) throw new Error("Select a valid college basketball game.");
  return cachedGame(id);
}
