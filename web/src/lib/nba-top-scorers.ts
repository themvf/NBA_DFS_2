/**
 * NBA top scorers from the books' player-points lines.
 *
 * Pure: no database or network. The page passes in the latest stored capture
 * per game (prop_odds_history, sport='nba', market='player_points') and last
 * season's per-game averages, and gets back a ranked board.
 *
 * What the numbers mean:
 * - `line` is the median of the books' points lines. A points line is the
 *   books' MEDIAN outcome, not their average; NBA scoring is right-skewed, so
 *   a player's mean is usually a little above his line. Ranking by line is
 *   ranking by the market's median, which is the right order for "who scores
 *   the most" but not an estimate of the average.
 * - `pOver` is the de-vigged chance of going over, taken only from books that
 *   quote the consensus line, because a probability at 24.5 and one at 25.5
 *   are different propositions and must not be averaged.
 * - Nothing here is fitted or validated. It is the market's number, shown as is.
 */

export type BookQuote = {
  line?: number | null;
  over?: number | null;
  under?: number | null;
  bookmakerTitle?: string | null;
  lastUpdate?: string | null;
};

export type PropRow = {
  eventId: string;
  commenceTime: string;
  homeTeam: string;
  awayTeam: string;
  player: string;
  books: Record<string, BookQuote>;
  capturedAt: string;
};

export type SeasonStat = {
  name: string;
  ppg: number | null;
  games: number | null;
  teamName: string | null;
  teamAbbrev: string | null;
};

export type BookLine = {
  book: string;
  line: number;
  over: number | null;
  under: number | null;
  pOver: number | null;
};

export type ScorerRow = {
  rank: number;
  player: string;
  eventId: string;
  game: string;
  commenceTime: string;
  /** Team the player belongs to, only when last season's team is one of the two in this game. */
  team: string | null;
  teamAbbrev: string | null;
  opponent: string | null;
  line: number;
  lineLow: number;
  lineHigh: number;
  pOver: number | null;
  books: BookLine[];
  lastSeasonPpg: number | null;
  lastSeasonGames: number | null;
  /** consensus line minus last season's points per game; null without a season row. */
  vsLastSeason: number | null;
  capturedAt: string;
};

export type GameSummary = {
  eventId: string;
  game: string;
  homeTeam: string;
  awayTeam: string;
  commenceTime: string;
  capturedAt: string;
  players: number;
  /** Sum of every listed player's line: a rough check against the game total, never a forecast. */
  lineSum: number;
};

const SUFFIXES = new Set(["jr", "sr", "ii", "iii", "iv", "v"]);

/** Accent-free, lowercase, suffix-free name key ("Nikola Jokić" and "Nikola Jokic" match). */
export function nameKey(name: string): string {
  const words = name
    .normalize("NFKD")
    .replace(/[̀-ͯ]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9 ]/g, " ")
    .split(/\s+/)
    .filter((w) => w && !SUFFIXES.has(w));
  return words.join("");
}

// Our teams table and The Odds API spell one team differently.
const TEAM_ALIASES: Record<string, string> = { laclippers: "losangelesclippers" };

export function teamKey(name: string | null | undefined): string {
  const key = nameKey(name ?? "");
  return TEAM_ALIASES[key] ?? key;
}

function implied(american: number): number {
  return american < 0 ? -american / (-american + 100) : 100 / (american + 100);
}

function validAmerican(price: unknown): price is number {
  return typeof price === "number" && Number.isFinite(price) && (price <= -100 || price >= 100);
}

/** Proportional de-vig of one book's over/under pair; null without a valid pair. */
export function devigOver(over: number | null | undefined, under: number | null | undefined): number | null {
  if (!validAmerican(over) || !validAmerican(under)) return null;
  const o = implied(over);
  const u = implied(under);
  return o / (o + u);
}

export function median(values: number[]): number {
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
}

export function bookLines(books: Record<string, BookQuote>): BookLine[] {
  const out: BookLine[] = [];
  for (const [book, quote] of Object.entries(books ?? {})) {
    const line = Number(quote?.line);
    if (quote?.line == null || !Number.isFinite(line)) continue;
    out.push({
      book,
      line,
      over: validAmerican(quote.over) ? quote.over : null,
      under: validAmerican(quote.under) ? quote.under : null,
      pOver: devigOver(quote.over, quote.under),
    });
  }
  return out.sort((a, b) => a.book.localeCompare(b.book));
}

/**
 * The line most books quote (the median breaks a tie), and the median de-vigged
 * P(over) of the books quoting exactly that line.
 */
export function consensus(lines: BookLine[]): { line: number; pOver: number | null } | null {
  if (lines.length === 0) return null;
  const counts = new Map<number, number>();
  for (const l of lines) counts.set(l.line, (counts.get(l.line) ?? 0) + 1);
  const top = Math.max(...counts.values());
  const modal = [...counts.entries()].filter(([, n]) => n === top).map(([v]) => v);
  const mid = median(lines.map((l) => l.line));
  const line = modal.reduce((best, v) => (Math.abs(v - mid) < Math.abs(best - mid) ? v : best), modal[0]);
  const probs = lines.filter((l) => l.line === line && l.pOver != null).map((l) => l.pOver as number);
  return { line, pOver: probs.length ? median(probs) : null };
}

export function buildBoard(
  rows: PropRow[],
  lastSeason: SeasonStat[],
): { scorers: ScorerRow[]; games: GameSummary[] } {
  const stats = new Map<string, SeasonStat | null>();
  for (const s of lastSeason) {
    const key = nameKey(s.name);
    // Two players sharing a name key are ambiguous: show neither rather than guess.
    stats.set(key, stats.has(key) ? null : s);
  }

  const scorers: Omit<ScorerRow, "rank">[] = [];
  const games = new Map<string, GameSummary>();
  for (const row of rows) {
    const lines = bookLines(row.books);
    const c = consensus(lines);
    if (!c) continue;
    const game = `${row.awayTeam} @ ${row.homeTeam}`;
    const stat = stats.get(nameKey(row.player)) ?? null;
    const statTeam = stat?.teamName ? teamKey(stat.teamName) : null;
    const onHome = statTeam != null && statTeam === teamKey(row.homeTeam);
    const onAway = statTeam != null && statTeam === teamKey(row.awayTeam);
    scorers.push({
      player: row.player,
      eventId: row.eventId,
      game,
      commenceTime: row.commenceTime,
      team: onHome ? row.homeTeam : onAway ? row.awayTeam : null,
      teamAbbrev: onHome || onAway ? stat?.teamAbbrev ?? null : null,
      opponent: onHome ? row.awayTeam : onAway ? row.homeTeam : null,
      line: c.line,
      lineLow: Math.min(...lines.map((l) => l.line)),
      lineHigh: Math.max(...lines.map((l) => l.line)),
      pOver: c.pOver,
      books: lines,
      lastSeasonPpg: stat?.ppg ?? null,
      lastSeasonGames: stat?.games ?? null,
      vsLastSeason: stat?.ppg != null ? c.line - stat.ppg : null,
      capturedAt: row.capturedAt,
    });
    const g = games.get(row.eventId) ?? {
      eventId: row.eventId,
      game,
      homeTeam: row.homeTeam,
      awayTeam: row.awayTeam,
      commenceTime: row.commenceTime,
      capturedAt: row.capturedAt,
      players: 0,
      lineSum: 0,
    };
    g.players += 1;
    g.lineSum += c.line;
    if (row.capturedAt > g.capturedAt) g.capturedAt = row.capturedAt;
    games.set(row.eventId, g);
  }

  scorers.sort(
    (a, b) => b.line - a.line || (b.pOver ?? 0) - (a.pOver ?? 0) || a.player.localeCompare(b.player),
  );
  return {
    scorers: scorers.map((s, i) => ({ ...s, rank: i + 1 })),
    games: [...games.values()].sort((a, b) => a.commenceTime.localeCompare(b.commenceTime) || a.game.localeCompare(b.game)),
  };
}
