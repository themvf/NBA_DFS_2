/**
 * Starter and injury news from X (twitterapi.io), shared by the CFB and NFL
 * DFS pages. Sport-neutral: each page supplies its own teams and players.
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
  /** Posted by an account on the page's trusted list. */
  trusted: boolean;
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
      trusted: false,
    };
  }).filter((p) => p.id && p.at);
}

/** One copy of each post. Flagged trusted, then flagged, then trusted posts win the `limit` places; shown newest first. */
export function mergePosts(lists: readonly XPost[][], limit = 12): XPost[] {
  const seen = new Set<string>();
  const out: XPost[] = [];
  for (const p of lists.flat()) {
    const key = p.id || p.text.slice(0, 80);
    if (seen.has(key)) continue;
    seen.add(key); out.push(p);
  }
  const rank = (p: XPost) => (p.flags.length ? 2 : 0) + (p.trusted ? 1 : 0);
  out.sort((a, b) => rank(b) - rank(a) || b.at.localeCompare(a.at));
  return out.slice(0, limit).sort((a, b) => b.at.localeCompare(a.at));
}

/**
 * Searches of the trusted accounts, restricted to posts naming a slate team:
 * (from:a OR from:b ...) (Team1 OR Team2 ...). Teams are split into groups so a
 * 14-team slate stays within X's query length.
 */
export function trustedQueries(handles: readonly string[], teamNames: readonly string[], until: Date, groupSize = 8): string[] {
  if (!handles.length || !teamNames.length) return [];
  const since = new Date(until.getTime() - X_NEWS_LOOKBACK_HOURS * 3_600_000);
  const from = `(${handles.map((h) => `from:${h}`).join(" OR ")})`;
  const window = `since:${twitterTime(since)} until:${twitterTime(until)} -is:retweet`;
  const names = [...new Set(teamNames)];
  const out: string[] = [];
  for (let i = 0; i < names.length; i += groupSize)
    out.push(`${from} (${names.slice(i, i + groupSize).map((n) => `"${n}"`).join(" OR ")}) ${window}`);
  return out;
}

const FOOTBALL = /\b(qb|quarterback|rb|running back|wr|receiver|te|tight end|football|cfb|nfl|starter|start(?:s|ed|ing)?|injury report|depth chart|touchdown|snaps?)\b/i;

/**
 * The teams a trusted post is about: it names one of the team's searched
 * players in full, or names the team in a sentence about football. Team names
 * alone are not enough (injury feeds also cover the Diamondbacks and Reds), and
 * neither are surnames (Johnson, Brown and Taylor are on every roster).
 */
export function teamsForPost(text: string, requests: readonly TeamNewsRequest[]): TeamNewsRequest[] {
  const sentences = text.split(/(?<=[.!?])\s+|\n+/);
  return requests.filter((r) => r.players.some((name) => wordRe(name).test(text))
    || sentences.some((s) => wordRe(r.school).test(s) && FOOTBALL.test(s)));
}

export interface XNewsResult { teams: TeamNews[]; searchedAt: string; postsRead: number; trustedAccounts: string[]; trustedError: string | null }

/** One team to search: `school` is how posts name the team ("Navy", "Chiefs"); `players` are named in quotes. */
export interface TeamNewsRequest { code: string; school: string; players: string[] }

/**
 * Run every team's searches, four at a time, and merge each team's posts.
 * A failed search is recorded on its team; the others still return.
 */
export async function searchTeamNews(key: string, requests: readonly TeamNewsRequest[],
  options: { trusted?: readonly string[]; until?: Date } = {}):
  Promise<XNewsResult> {
  const until = options.until ?? new Date();
  const trustedAccounts = [...new Set(options.trusted ?? [])];
  const trustedSet = new Set(trustedAccounts.map((h) => h.toLowerCase()));
  const mark = (posts: XPost[]) => posts.map((p) => ({ ...p, trusted: trustedSet.has(p.user.toLowerCase()) }));
  const teams: TeamNews[] = requests.map((r) => ({ ...r, queries: teamQueries(r.school, r.players, until), posts: [], error: null }));
  let postsRead = 0;
  const jobs = teams.flatMap((t) => t.queries.map((q) => ({ t, q })));
  const results = new Map<TeamNews, XPost[][]>();
  for (let i = 0; i < jobs.length; i += 4) {
    await Promise.all(jobs.slice(i, i + 4).map(async ({ t, q }) => {
      try {
        const url = `${X_SEARCH_URL}?${new URLSearchParams({ query: q, queryType: "Latest" })}`;
        const response = await fetch(url, { headers: { "X-API-Key": key }, cache: "no-store" });
        if (!response.ok) throw new Error(`X search ${response.status}`);
        const posts = mark(parseXPosts(await response.json(), t.players, t.school));
        postsRead += posts.length;
        results.set(t, [...(results.get(t) ?? []), posts]);
      } catch (reason) { t.error = reason instanceof Error ? reason.message : String(reason); }
    }));
  }
  // Trusted accounts: searched directly (two pages per query), each post routed to
  // every team it names and re-parsed so flags and mentions are that team's.
  let trustedError: string | null = null;
  for (const q of trustedQueries(trustedAccounts, requests.map((r) => r.school), until)) {
    try {
      let cursor = "";
      for (let page = 0; page < 2; page += 1) {
        const url = `${X_SEARCH_URL}?${new URLSearchParams({ query: q, queryType: "Latest", cursor })}`;
        const response = await fetch(url, { headers: { "X-API-Key": key }, cache: "no-store" });
        if (!response.ok) throw new Error(`X search ${response.status}`);
        const payload = (await response.json()) as { tweets?: unknown[]; has_next_page?: boolean; next_cursor?: string };
        postsRead += payload.tweets?.length ?? 0;
        for (const t of teams) {
          const posts = mark(parseXPosts(payload, t.players, t.school)).filter((p) => teamsForPost(p.text, [t]).length);
          if (posts.length) results.set(t, [...(results.get(t) ?? []), posts]);
        }
        if (!payload.has_next_page || !payload.next_cursor) break;
        cursor = payload.next_cursor;
      }
    } catch (reason) { trustedError = reason instanceof Error ? reason.message : String(reason); }
  }
  for (const t of teams) t.posts = mergePosts(results.get(t) ?? []);
  return { teams, searchedAt: until.toISOString(), postsRead, trustedAccounts, trustedError };
}
