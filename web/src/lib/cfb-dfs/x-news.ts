/**
 * Starter and injury news from X (twitterapi.io), for the CFB DFS page.
 *
 * Why: DraftKings left Braxton Woodson tagged Q through a game he did not
 * play. X had "Gutierrez expected to make his 1st career start" from two
 * national reporters 90 minutes before lock (tested 2026-09-26, 59 posts).
 *
 * This only gathers and flags posts. It never changes a status or a
 * projection: jokes, stale news and conflicting reports ("Woodson dressed and
 * warming up" came 50 minutes after "Gutierrez to start") need a human.
 */

export const X_SEARCH_URL = "https://api.twitterapi.io/twitter/tweet/advanced_search";
export const X_NEWS_LOOKBACK_HOURS = 72;

export interface XPost {
  id: string; at: string; user: string; followers: number | null; text: string; url: string | null;
  /** Phrases that suggest a lineup change: "will start", "doubtful", ... Only from sentences naming this team. */
  flags: string[];
  /** This team's slate players the post names (by surname). */
  mentions: string[];
}
export interface TeamNews {
  code: string; school: string; players: string[]; queries: string[];
  posts: XPost[]; error: string | null;
}

const STARTER = /\b(will start|to start|expected to start|named (?:the )?starter|(?:first|1st) (?:career )?start|makes? (?:his|the|a) (?:\w+ )?start|gets the start|starting (?:qb|quarterback)|start at (?:qb|quarterback))\b/i;
const OUT = /\b(ruled out|won'?t play|will not play|(?:not|isn't|isn’t|aren't|aren’t) expected to play|is out|out for|out with|sidelined|inactive)\b/i;
const DOUBT = /\b(doubtful|questionable|game[- ]time decision|gtd|limited|day[- ]to[- ]day)\b/i;
const INJURY = /\b(injur\w*|ankle|knee|concussion|sprain\w*|hamstring|shoulder)\b/i;

const SUFFIX = /^(jr\.?|sr\.?|ii|iii|iv|v)$/i;
/** Last name, skipping Jr./II/III: "Michael Hawkins Jr." -> "Hawkins". */
export function surnameOf(name: string): string {
  const parts = name.trim().split(/\s+/).filter((w) => !SUFFIX.test(w));
  return parts.at(-1) ?? name;
}
const escapeRe = (s: string) => s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
const wordRe = (term: string) => new RegExp(`(^|[^\\p{L}])${escapeRe(term)}($|[^\\p{L}])`, "iu");

/**
 * Flags from the sentences that name one of `terms` (surnames, school). A
 * multi-game preview saying "X is doubtful" about another team no longer flags
 * this one. With no terms, the whole post counts.
 */
export function flagPost(text: string, terms: readonly string[] = []): string[] {
  const res = terms.map(wordRe);
  const relevant = res.length
    ? text.split(/(?<=[.!?])\s+|\n+/).filter((sentence) => res.some((re) => re.test(sentence))).join(" ")
    : text;
  return phraseFlags(relevant);
}

function phraseFlags(text: string): string[] {
  const flags: string[] = [];
  if (STARTER.test(text)) flags.push("starter");
  if (OUT.test(text)) flags.push("out");
  if (DOUBT.test(text)) flags.push("doubtful/GTD");
  if (INJURY.test(text)) flags.push("injury");
  return flags;
}

const twitterTime = (d: Date) => d.toISOString().replace("T", "_").replace(/\.\d{3}Z$/, "_UTC");

/**
 * Two searches per team: the named players (quoted, so "Navy SEAL" posts do not
 * match), and the school with quarterback / availability words.
 */
export function teamQueries(school: string, players: readonly string[], until: Date): string[] {
  const since = new Date(until.getTime() - X_NEWS_LOOKBACK_HOURS * 3_600_000);
  const window = `since:${twitterTime(since)} until:${twitterTime(until)} -is:retweet`;
  const queries: string[] = [];
  if (players.length) queries.push(`(${players.map((p) => `"${p}"`).join(" OR ")}) ${window}`);
  queries.push(`"${school}" (QB OR quarterback) (start OR starting OR starter OR out OR injury OR injured OR doubtful OR "game-time") ${window}`);
  return queries;
}

export function parseXPosts(payload: unknown, players: readonly string[] = [], school: string | null = null): XPost[] {
  const surnames = [...new Set(players.map(surnameOf))];
  const terms = [...surnames, ...(school ? [school] : [])];
  const tweets = ((payload as { tweets?: Array<Record<string, unknown>> })?.tweets ?? []);
  return tweets.map((t) => {
    const author = (t.author ?? {}) as Record<string, unknown>;
    const text = String(t.text ?? "");
    const at = t.createdAt ? new Date(String(t.createdAt)) : null;
    return {
      id: String(t.id ?? ""), at: at && !Number.isNaN(at.getTime()) ? at.toISOString() : "",
      user: String(author.userName ?? ""), followers: typeof author.followers === "number" ? author.followers : null,
      text, url: t.url ? String(t.url) : null, flags: flagPost(text, terms),
      mentions: players.filter((name) => wordRe(surnameOf(name)).test(text)),
    };
  }).filter((p) => p.id && p.at);
}

/** Newest first, one copy of each post, flagged posts before unflagged ones of the same hour. */
export function mergePosts(lists: readonly XPost[][], limit = 12): XPost[] {
  const seen = new Set<string>();
  const out: XPost[] = [];
  for (const p of lists.flat()) {
    const key = p.id || p.text.slice(0, 80);
    if (seen.has(key)) continue;
    seen.add(key); out.push(p);
  }
  out.sort((a, b) => (b.flags.length > 0 ? 1 : 0) - (a.flags.length > 0 ? 1 : 0) || b.at.localeCompare(a.at));
  return out.slice(0, limit).sort((a, b) => b.at.localeCompare(a.at));
}
