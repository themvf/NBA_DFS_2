import type { grade } from "./nba-market-studies";
import type { gradeGame } from "./nfl/market-studies";
import type { MatchStudyId } from "./study-registry";
import { americanToImplied } from "./odds-consensus";

type Nba = ReturnType<typeof grade>;
type Nfl = ReturnType<typeof gradeGame>;
type Sport = "NBA" | "NFL" | "CFB";
type Market = "ATS" | "Outright win" | "Total";
export const STUDY_MATCH_PERCENT = 65;

export type MatchGame = {
  sport: Sport; id: string; date: string; start: string; home: string; away: string;
  finalObservedAt?: string | null;
};
type Observation = {
  game: MatchGame; id: MatchStudyId; cohort: string; market: Market; side: string;
  margin: number | null; observedAt?: string;
};
type Record = { wins: number; losses: number; pushes: number };
export type StudyMatch = Observation & { record: Record; decided: number; rate: number };

const signed = (number: number) => (number > 0 ? "+" : "") + number;
const hasNumber = (value: unknown): value is number => typeof value === "number" && Number.isFinite(value);
const resultAt = (actual: number | null, edge: number) => actual === null ? null : actual + edge;

export function qualifyingStudyMatches(observations: Observation[], date: string, now = Date.now()): StudyMatch[] {
  const prior = new Map<string, Map<string, Observation>>();
  const today = new Map<string, Observation>();
  const key = (row: Observation) => JSON.stringify([row.game.sport, row.id, row.cohort, row.market]);
  for (const row of observations) {
    if (row.game.date === date) {
      today.set(key(row) + row.game.id, row);
    } else if (row.game.date < date && row.margin !== null && row.game.finalObservedAt) {
      const cohort = prior.get(key(row)) ?? new Map();
      cohort.set(row.game.id, row);
      prior.set(key(row), cohort);
    }
  }
  return [...today.values()].flatMap(row => {
    const cutoff = Math.min(now, Date.parse(row.game.start));
    const record = { wins: 0, losses: 0, pushes: 0 };
    if (!row.observedAt || !(Date.parse(row.observedAt) < cutoff)) return [];
    for (const previous of prior.get(key(row))?.values() ?? []) {
      if (!(Date.parse(previous.game.finalObservedAt!) < cutoff)) continue;
      if (previous.margin! > 0) record.wins++;
      else if (previous.margin! < 0) record.losses++;
      else record.pushes++;
    }
    const decided = record.wins + record.losses;
    return decided && record.wins * 100 >= STUDY_MATCH_PERCENT * decided
      ? [{ ...row, record, decided, rate: record.wins / decided }]
      : [];
  }).sort((a, b) => Date.parse(a.game.start) - Date.parse(b.game.start) || a.game.id.localeCompare(b.game.id)
    || b.rate - a.rate || b.decided - a.decided || a.id.localeCompare(b.id));
}

function addSpread(rows: Observation[], game: MatchGame, id: MatchStudyId, cohort: string, line: number,
                   margin: number | null, observedAt: string, includeDog: boolean) {
  const sign = line < 0 ? 1 : -1;
  const favorite = sign === 1 ? game.home : game.away;
  const dog = sign === 1 ? game.away : game.home;
  const cover = margin === null ? null : (margin + line) * sign;
  rows.push({ game, id, cohort: cohort + " · favorite", market: "ATS", side: favorite + " " + signed(-Math.abs(line)), margin: cover, observedAt });
  if (includeDog) rows.push({ game, id, cohort: cohort + " · underdog", market: "ATS", side: dog + " " + signed(Math.abs(line)),
    margin: cover === null ? null : -cover, observedAt });
}

function addTotals(rows: Observation[], game: MatchGame, id: MatchStudyId, cohort: string, line: number,
                   total: number | null, time: string) {
  rows.push({ game, id, cohort: cohort + " · over", market: "Total", side: "Over " + line, margin: resultAt(total, -line), observedAt: time });
  rows.push({ game, id, cohort: cohort + " · under", market: "Total", side: "Under " + line,
    margin: total === null ? null : line - total, observedAt: time });
}

export function nbaObservations(row: Nba): Observation[] {
  const { game: source, margin, actualTotal, spread: line } = row;
  if (!source.start) return [];
  const game: MatchGame = { ...source, sport: "NBA", start: source.start, finalObservedAt: source.finalObservedAt };
  const rows: Observation[] = [];
  const time = row.spreadQuotes.at(-1)?.observedAt;
  if (line && time) {
    const size = Math.abs(line);
    const home = line < 0;
    const band = size < 4 ? "Under 4" : size < 7 ? "4 to under 7" : size < 10 ? "7 to under 10" : "10+";
    addSpread(rows, game, "NBA-01", band, line, margin, time, false);
    rows.push({ game, id: "NBA-01", cohort: band + " · favorite outright", market: "Outright win",
      side: home ? game.home : game.away, margin: margin === null ? null : home ? margin : -margin, observedAt: time });
    const favoriteRest = home ? source.homeBackToBack : source.awayBackToBack;
    const dogRest = home ? source.awayBackToBack : source.homeBackToBack;
    const rest = favoriteRest ? dogRest ? "Both" : "Favorite only" : dogRest ? "Underdog only" : "Neither";
    addSpread(rows, game, "NBA-02", rest + " on back-to-back", line, margin, time, false);
    addSpread(rows, game, "NBA-03", (home ? "Home" : "Away") + (size < 7 ? ", under 7" : ", 7+"), line, margin, time, false);
    const movement = row.movement === null ? "Single quote" : row.favoriteChanged
      ? "Favorite changed or opened pick’em" : row.movement > 0 ? "Favorite grew" : row.movement < 0 ? "Favorite shrank" : "Unchanged";
    addSpread(rows, game, "NBA-04", movement, line, margin, time, false);
    for (const [team, sign] of [[game.home, 1], [game.away, -1]] as const) {
      const teamLine = line * sign;
      const role = team + (teamLine < 0 ? " · favorite" : " · underdog");
      rows.push({ game, id: "NBA-06", cohort: role + " · last quote", market: "ATS",
        side: team + " " + signed(teamLine), margin: margin === null ? null : margin * sign + teamLine, observedAt: time });
      const first = row.spreadQuotes[0];
      if (first && hasNumber(row.firstSpread)) {
        const firstLine = row.firstSpread * sign;
        rows.push({ game, id: "NBA-06", cohort: role + " · first quote", market: "ATS",
          side: team + " " + signed(firstLine), margin: margin === null ? null : margin * sign + firstLine, observedAt: first.observedAt });
      }
    }
    const previous = home ? source.awayPriorVenue : source.homePriorVenue;
    if (dogRest && !favoriteRest && previous) {
      addSpread(rows, game, "NBA-11", "Underdog only on back-to-back · " + (previous === game.home ? "same venue" : "changed venue"),
        line, margin, time, false);
    }
  }
  if (row.lastTotal && hasNumber(row.total)) {
    const band = row.total < 225 ? "Under 225" : row.total < 235 ? "225 to under 235" : "235+";
    addTotals(rows, game, "NBA-05", band, row.total, actualTotal, row.lastTotal.observedAt);
    addTotals(rows, game, "NBA-05", "Last archived quote", row.total, actualTotal, row.lastTotal.observedAt);
  }
  if (row.firstTotal && hasNumber(row.firstTotal.books.pinnacle.total))
    addTotals(rows, game, "NBA-05", "First archived quote", row.firstTotal.books.pinnacle.total, actualTotal, row.firstTotal.observedAt);
  if (row.paperPlay && row.paperCapture && hasNumber(row.paperQuote?.total)) {
    rows.push({ game, id: "NBA-05", cohort: "Over 235+ paper trial", market: "Total", side: "Over " + row.paperQuote.total,
      margin: resultAt(actualTotal, -row.paperQuote.total), observedAt: row.paperCapture.observedAt });
  }
  return rows;
}

export function nflObservations(row: Nfl): Observation[] {
  const { game: source, margin } = row;
  if (!source.date || !source.kickoff) return [];
  const game: MatchGame = { ...source, sport: "NFL", date: source.date, start: source.kickoff };
  const rows: Observation[] = [];
  const spread = row.spreadQuotes.last;
  if (spread && spread.line !== 0) {
    const { line } = spread;
    const time = spread.quote.observedAt;
    const size = Math.abs(line);
    const home = line < 0;
    const bin = size < 3 ? "Under 3" : size === 3 ? "Exactly 3" : size < 7 ? "Over 3 to under 7" : size === 7 ? "Exactly 7" : "Over 7";
    addSpread(rows, game, "NFL-01", "All spread sizes", line, margin, time, true);
    addSpread(rows, game, "NFL-01", bin, line, margin, time, true);
    if (source.homeRest && source.awayRest) {
      const rest = home ? source.homeRest : source.awayRest;
      const difference = (source.homeRest - source.awayRest) * (home ? 1 : -1);
      addSpread(rows, game, "NFL-02", rest < 7 ? "Short rest" : rest === 7 ? "Seven days" : "Extended rest", line, margin, time, true);
      addSpread(rows, game, "NFL-11", difference > 0 ? "Favorite rest edge" : difference < 0 ? "Underdog rest edge" : "Equal rest",
        line, margin, time, true);
    }
    addSpread(rows, game, "NFL-03", home ? "Listed home" : "Listed away", line, margin, time, true);
    const first = row.spreadQuotes.first!;
    const move = (first.line - line) * (home ? 1 : -1);
    const movement = first === spread ? "One quote" : Math.sign(first.line) !== Math.sign(line) ? "Favorite changed or opened pick’em"
      : move > 0 ? "Favorite grew" : move < 0 ? "Favorite shrank" : "Unchanged";
    addSpread(rows, game, "NFL-04", movement, line, margin, time, true);
  }
  if (spread) {
    for (const [team, sign] of [[game.home, 1], [game.away, -1]] as const) {
      const line = spread.line * sign;
      rows.push({ game, id: "NFL-06", cohort: team + (line < 0 ? " · favorite" : line > 0 ? " · underdog" : " · pick’em"),
        market: "ATS", side: team + " " + signed(line), margin: margin === null ? null : margin * sign + line, observedAt: spread.quote.observedAt });
    }
  }
  const lastTotal = row.totalQuotes.last;
  if (lastTotal) {
    const band = lastTotal.line < 42 ? "Under 42" : lastTotal.line < 48 ? "42 to under 48" : "48+";
    addTotals(rows, game, "NFL-05", band, lastTotal.line, row.total, lastTotal.quote.observedAt);
    addTotals(rows, game, "NFL-05", "Last archived quote", lastTotal.line, row.total, lastTotal.quote.observedAt);
  }
  const firstTotal = row.totalQuotes.first;
  if (firstTotal && lastTotal && firstTotal !== lastTotal) {
    addTotals(rows, game, "NFL-05", "First quote · paired archive", firstTotal.line, row.total, firstTotal.quote.observedAt);
    addTotals(rows, game, "NFL-05", "Last quote · paired archive", lastTotal.line, row.total, lastTotal.quote.observedAt);
  }
  const ml = row.moneylineQuotes.last;
  const homeImplied = ml ? americanToImplied(ml.prices[0]) : null;
  const awayImplied = ml ? americanToImplied(ml.prices[1]) : null;
  if (ml && homeImplied !== null && awayImplied !== null && homeImplied !== awayImplied) {
    const home = homeImplied > awayImplied;
    const edge = margin === null ? null : home ? margin : -margin;
    rows.push({ game, id: "NFL-10", cohort: "Moneyline favorite", market: "Outright win", side: home ? game.home : game.away, margin: edge, observedAt: ml.quote.observedAt });
    rows.push({ game, id: "NFL-10", cohort: "Moneyline underdog", market: "Outright win", side: home ? game.away : game.home,
      margin: edge === null ? null : -edge, observedAt: ml.quote.observedAt });
  }
  return rows;
}

const cfbBet = {
  T1_g5_mid_fav_ml: ["T1", "Outright win"],
  T2_fcs_fav_spread: ["T2", "ATS"],
  T3_small_fav_dog_spread: ["T3", "ATS"],
  T4_big_fav_over: ["T4", "Total"],
  T5_sat_evening_g5_fav_ml: ["T5", "Outright win"],
  T6_mid_fav_spread: ["T6", "ATS"],
} as const;

export const cfbStudyTriggers = Object.keys(cfbBet);
export const matchAdapterStudyIds = [
  "NBA-01","NBA-02","NBA-03","NBA-04","NBA-05","NBA-06","NBA-11",
  "NFL-01","NFL-02","NFL-03","NFL-04","NFL-05","NFL-06","NFL-10","NFL-11",
  ...Object.values(cfbBet).map(([id]) => id),
] as const satisfies readonly MatchStudyId[];

export type CfbStudySelection = Omit<MatchGame, "sport"> & {
  trigger: string; frozenAt: string; selection: string; spread: number; total: number | null;
  homeMl: number; awayMl: number; homeScore: number | null; awayScore: number | null;
};

export function cfbObservations(rows: CfbStudySelection[]): Observation[] {
  return rows.flatMap(row => {
    const market = cfbBet[row.trigger as keyof typeof cfbBet];
    if (!market) return [];
    const [id, type] = market;
    const homeFav = row.spread !== 0 ? row.spread < 0 : row.homeMl < row.awayMl;
    let margin = null;
    if (row.homeScore !== null && row.awayScore !== null) {
      const favorite = (row.homeScore - row.awayScore) * (homeFav ? 1 : -1);
      if (type === "Outright win") margin = favorite;
      else if (type === "ATS") margin = id === "T3" ? Math.abs(row.spread) - favorite : favorite - Math.abs(row.spread);
      else if (row.total !== null) margin = row.homeScore + row.awayScore - row.total;
    }
    return [{ game: { sport: "CFB" as const, id: row.id, date: row.date, start: row.start, home: row.home, away: row.away,
      finalObservedAt: row.finalObservedAt }, id, market: type, cohort: "Frozen board entries", side: row.selection, margin, observedAt: row.frozenAt }];
  });
}
