/**
 * The pick'em live board: one frozen card, its pregame line trails, and the
 * finals, folded into what the page renders.
 *
 * Port of artifacts/pickem_live_board.py (the private claude.ai board this
 * page replaces), without its live scoreboard: the page shows pregame win
 * chances and grades each game once nfl_season_games records the final (the
 * same table pick'em settlement grades from). No in-game state, so no ESPN.
 * Pure: the reads live in db/queries.ts (getPickemLiveCard), so every rule
 * here -- the two flip tags above all -- can be tested without a database.
 */

export const TRAIL_POINTS = 16;

/** One frozen game as the ledger stores it, plus the season game's own result. */
export interface PickemLiveCardGame {
  gameId: number;
  away: string;
  home: string;
  pickHome: boolean;
  baselinePickHome: boolean;
  confidence: number;
  /** The ledger's frozen (defence-adjusted) home win chance. */
  pHome: number;
  kickoff: string;
  homeScore: number | null;
  awayScore: number | null;
  completed: boolean;
}

export interface PickemLiveCard {
  id: number;
  season: number;
  week: number;
  format: "confidence" | "straight";
  frozenAt: string;
  games: PickemLiveCardGame[];
  /** Pregame vig-free consensus per game, oldest first. Spread is the home line. */
  trails: Record<number, Array<{ pHome: number; homeSpread: number | null }>>;
}

/** Before kickoff; kicked off with no final recorded yet; final. */
export type GameState = "pre" | "pending" | "post";

export interface BoardGame {
  gameId: number;
  away: string;
  home: string;
  pick: string;
  /** Market favourite at the latest pregame capture (the ledger baseline when no line exists). */
  favorite: string;
  openFavorite: string | null;
  vegasFlip: boolean;
  ourFlip: boolean;
  ourFlipReason: "model" | "strategy" | null;
  confidence: number;
  kickoff: string;
  window: string;
  spread: string | null;
  /** Our pick's frozen chance, from the ledger. */
  frozen: number;
  /** Our pick's pregame chance, downsampled, first capture to last. */
  trail: number[];
  state: GameState;
  /** Final score from nfl_season_games; null until the game is final. */
  awayScore: number | null;
  homeScore: number | null;
}

export type Outcome = "won" | "lost" | "tie" | null;

const ET = "America/New_York";

/** Kickoff window in Eastern time, the way the schedule is talked about. */
export function windowLabel(kickoffIso: string): string {
  const kick = new Date(kickoffIso);
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: ET, weekday: "long", hour: "numeric", hourCycle: "h23",
  }).formatToParts(kick);
  const day = parts.find((p) => p.type === "weekday")?.value ?? "";
  const hour = Number(parts.find((p) => p.type === "hour")?.value ?? 0);
  if (day === "Thursday") return "Thursday night";
  if (day === "Monday") return "Monday night";
  if (day === "Sunday") {
    if (hour >= 19) return "Sunday night";
    if (hour >= 15) return "Sunday afternoon";
    return "Sunday 1:00 PM";
  }
  return day;
}

export function downsample<T>(rows: T[], n: number): T[] {
  if (rows.length <= n) return rows;
  const step = (rows.length - 1) / (n - 1);
  return Array.from({ length: n }, (_, i) => rows[Math.round(i * step)]);
}

/** A probability of exactly 0.5 has no favourite. */
function favouriteOf(pHome: number | null | undefined, home: string, away: string): string | null {
  if (pHome == null || pHome === 0.5) return null;
  return pHome > 0.5 ? home : away;
}

function spreadLabel(homeSpread: number | null | undefined, home: string, away: string): string | null {
  if (homeSpread == null) return null;
  if (homeSpread === 0) return "Pick'em";
  const fav = homeSpread < 0 ? home : away;
  return `${fav} −${Math.abs(homeSpread)}`;
}

export function buildBoardGames(card: PickemLiveCard, nowMs: number): BoardGame[] {
  return card.games.map((g) => {
    const trail = card.trails[g.gameId] ?? [];
    const side = (p: number) => (g.pickHome ? p : 1 - p);
    const pick = g.pickHome ? g.home : g.away;
    const openFav = trail.length ? favouriteOf(trail[0].pHome, g.home, g.away) : null;
    const marketFav = trail.length ? favouriteOf(trail[trail.length - 1].pHome, g.home, g.away) : null;
    const modelFav = favouriteOf(g.pHome, g.home, g.away);
    const ourFlip = Boolean(marketFav && pick !== marketFav);

    const final = g.completed && g.homeScore != null && g.awayScore != null;
    const state: GameState = final ? "post" : Date.parse(g.kickoff) <= nowMs ? "pending" : "pre";

    return {
      gameId: g.gameId,
      away: g.away,
      home: g.home,
      pick,
      favorite: marketFav ?? (g.baselinePickHome ? g.home : g.away),
      openFavorite: openFav,
      // Vegas flip: the market favourite changed between the first capture and the latest pregame one.
      vegasFlip: Boolean(openFav && marketFav && openFav !== marketFav),
      // Our flip: we picked the market underdog at the latest pregame capture.
      ourFlip,
      // Why: our frozen model also rates the pick the favourite, or we took the underdog anyway.
      ourFlipReason: ourFlip ? (modelFav === pick ? "model" : "strategy") : null,
      confidence: g.confidence,
      kickoff: g.kickoff,
      window: windowLabel(g.kickoff),
      spread: trail.length ? spreadLabel(trail[trail.length - 1].homeSpread, g.home, g.away) : null,
      frozen: side(g.pHome),
      trail: downsample(trail, TRAIL_POINTS).map((t) => side(t.pHome)),
      state,
      awayScore: final ? g.awayScore : null,
      homeScore: final ? g.homeScore : null,
    };
  });
}

export function outcomeOf(g: BoardGame): Outcome {
  if (g.state !== "post" || g.homeScore == null || g.awayScore == null) return null;
  const mine = g.pick === g.home ? g.homeScore : g.awayScore;
  const theirs = g.pick === g.home ? g.awayScore : g.homeScore;
  return mine > theirs ? "won" : mine < theirs ? "lost" : "tie";
}

/** Our pick's latest pregame chance; the frozen value when no line was captured. */
export function latestChance(g: BoardGame): number {
  return g.trail.length ? g.trail[g.trail.length - 1] : g.frozen;
}

export interface BoardSummary {
  won: number;
  lost: number;
  finals: number;
  /** Finals count 1 or 0; every other game counts its latest pregame chance. */
  expectedCorrect: number;
  /** Kicked off, final not recorded yet. */
  pending: number;
  ourFlips: string[];
  vegasFlips: string[];
}

export function summarize(games: BoardGame[]): BoardSummary {
  const finals = games.filter((g) => g.state === "post");
  return {
    won: finals.filter((g) => outcomeOf(g) === "won").length,
    lost: finals.filter((g) => outcomeOf(g) === "lost").length,
    finals: finals.length,
    expectedCorrect: games.reduce(
      (s, g) => s + (g.state === "post" ? (outcomeOf(g) === "won" ? 1 : 0) : latestChance(g)), 0),
    pending: games.filter((g) => g.state === "pending").length,
    ourFlips: games.filter((g) => g.ourFlip).map((g) => g.pick),
    vegasFlips: games.filter((g) => g.vegasFlip).map((g) => `${g.away} @ ${g.home}`),
  };
}

/** Games grouped by kickoff window, in kickoff order. */
export function groupByWindow(games: BoardGame[]): Array<{ window: string; games: BoardGame[] }> {
  const groups: Array<{ window: string; games: BoardGame[] }> = [];
  for (const g of games) {
    const last = groups.find((x) => x.window === g.window);
    if (last) last.games.push(g);
    else groups.push({ window: g.window, games: [g] });
  }
  return groups;
}

/** Team colours (background, text) and nickname, keyed by nfl_teams abbreviation. */
export const NFL_TEAM_STYLE: Record<string, [string, string, string]> = {
  ARI: ["#97233F", "#fff", "Cardinals"], ATL: ["#A71930", "#fff", "Falcons"], BAL: ["#241773", "#fff", "Ravens"],
  BUF: ["#00338D", "#fff", "Bills"], CAR: ["#0085CA", "#fff", "Panthers"], CHI: ["#0B162A", "#fff", "Bears"],
  CIN: ["#FB4F14", "#fff", "Bengals"], CLE: ["#FF3C00", "#fff", "Browns"], DAL: ["#003594", "#fff", "Cowboys"],
  DEN: ["#FB4F14", "#fff", "Broncos"], DET: ["#0076B6", "#fff", "Lions"], GB: ["#203731", "#fff", "Packers"],
  HOU: ["#03202F", "#fff", "Texans"], IND: ["#002C5F", "#fff", "Colts"], JAX: ["#006778", "#fff", "Jaguars"],
  KC: ["#E31837", "#fff", "Chiefs"], LAC: ["#0080C6", "#fff", "Chargers"], LAR: ["#003594", "#fff", "Rams"],
  LV: ["#A5ACAF", "#111", "Raiders"], MIA: ["#008E97", "#fff", "Dolphins"], MIN: ["#4F2683", "#fff", "Vikings"],
  NE: ["#002244", "#fff", "Patriots"], NO: ["#D3BC8D", "#111", "Saints"], NYG: ["#0B2265", "#fff", "Giants"],
  NYJ: ["#125740", "#fff", "Jets"], PHI: ["#004C54", "#fff", "Eagles"], PIT: ["#FFB612", "#111", "Steelers"],
  SEA: ["#69BE28", "#111", "Seahawks"], SF: ["#AA0000", "#fff", "49ers"], TB: ["#D50A0A", "#fff", "Buccaneers"],
  TEN: ["#4B92DB", "#fff", "Titans"], WSH: ["#5A1414", "#fff", "Commanders"],
};

export function teamStyle(abbr: string): [string, string, string] {
  return NFL_TEAM_STYLE[abbr] ?? ["#666", "#fff", abbr];
}
