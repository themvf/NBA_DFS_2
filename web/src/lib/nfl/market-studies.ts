import { americanToImplied } from "../odds-consensus";

export type Book = {
  last_update?: string;
  spread_home?: number; spread_away?: number;
  spread_home_price?: number; spread_away_price?: number;
  total_line?: number; over?: number; under?: number;
  ml_home?: number; ml_away?: number;
};

export type Quote = { id: string; observedAt: string; books: Record<string, Book | null> };
export type Game = {
  id: string; week: number; date: string | null; kickoff: string | null; boundary: string | null;
  home: string; away: string; homeScore: number | null; awayScore: number | null;
  finalObservedAt?: string | null;
  homeRest: number | null; awayRest: number | null;
  captures: Quote[];
};

type Market = "spread" | "total" | "moneyline";
type Offer = { line: number; prices: [number, number] };
type Selected = Offer & { quote: Quote };
export type Result = {
  label: string; wins: number; losses: number; pushes: number; units: number;
};

const BOOK = "pinnacle";
const markets = ["spread", "total", "moneyline"] as const;
const finite = (value: unknown): value is number => typeof value === "number" && Number.isFinite(value);
const price = (value: unknown): value is number => finite(value) && Math.abs(value) >= 100;
const profit = (odds: number) => 1 / americanToImplied(odds) - 1;
const average = (values: number[]) => values.length ? values.reduce((a, b) => a + b, 0) / values.length : null;

function offer(quote: Quote, bookName: string, market: Market): Offer | null {
  const book = quote.books[bookName];
  if (!book) return null;
  const updated = book.last_update == null ? null : Date.parse(book.last_update);
  if (updated !== null && !(updated <= Date.parse(quote.observedAt))) return null;

  switch (market) {
    case "spread":
      return finite(book.spread_home) && finite(book.spread_away) && book.spread_home === -book.spread_away
        && price(book.spread_home_price) && price(book.spread_away_price)
        ? { line: book.spread_home, prices: [book.spread_home_price, book.spread_away_price] } : null;
    case "total":
      return finite(book.total_line) && price(book.over) && price(book.under)
        ? { line: book.total_line, prices: [book.over, book.under] } : null;
    case "moneyline":
      return price(book.ml_home) && price(book.ml_away)
        ? { line: 0, prices: [book.ml_home, book.ml_away] } : null;
  }
}

function noVigHome({ prices }: Offer) {
  const [home, away] = prices.map(americanToImplied);
  return home / (home + away);
}

export function gradeGame(game: Game, asOf: number) {
  const boundary = game.boundary ? Date.parse(game.boundary) : NaN;
  const kickoff = game.kickoff ? Date.parse(game.kickoff) : NaN;
  const captures = game.captures.filter(quote => Date.parse(quote.observedAt) < Math.min(boundary, kickoff, asOf))
    .sort((a, b) => Date.parse(a.observedAt) - Date.parse(b.observedAt) || Number(a.id) - Number(b.id));
  const early = Math.min(kickoff - 60 * 60 * 1000, boundary);
  function select(market: Market) {
    const selections: Selected[] = captures.flatMap(quote => {
      const found = offer(quote, BOOK, market);
      return found ? [{ ...found, quote }] : [];
    });
    return {
      first: selections[0],
      last: selections.at(-1),
      early: selections.findLast(value => Date.parse(value.quote.observedAt) < early),
    };
  }
  const settled = game.homeScore !== null && game.awayScore !== null;
  const margin = settled ? game.homeScore! - game.awayScore! : null;
  const total = settled ? game.homeScore! + game.awayScore! : null;
  const spreadQuotes = select("spread");
  const totalQuotes = select("total");
  const moneylineQuotes = select("moneyline");
  const consensus = moneylineQuotes.last
    ? Object.keys(moneylineQuotes.last.quote.books).filter(name => name !== BOOK).flatMap(name => {
      const found = offer(moneylineQuotes.last!.quote, name, "moneyline");
      return found ? [noVigHome(found)] : [];
    })
    : [];

  return { game, margin, total, spreadQuotes, totalQuotes, moneylineQuotes, consensus: average(consensus) };
}

type Row = ReturnType<typeof gradeGame>;
type Observation = { margin: number; odds: number };

function result(label: string, values: Observation[]): Result {
  const output = { label, wins: 0, losses: 0, pushes: 0, units: 0 };
  for (const { margin, odds } of values) {
    if (margin > 0) {
      output.wins++;
      output.units += profit(odds);
    } else if (margin < 0) {
      output.losses++;
      output.units--;
    } else {
      output.pushes++;
    }
  }
  return output;
}

const spreadBin = (line: number) => {
  const size = Math.abs(line);
  return size < 3 ? "Under 3" : size === 3 ? "Exactly 3" : size < 7 ? "3.5 to under 7" : size === 7 ? "Exactly 7" : "Over 7";
};
const totalBin = (line: number) => line < 42 ? "Under 42" : line < 48 ? "42 to under 48" : "48+";
const restBin = (days: number) => days < 7 ? "Short rest" : days === 7 ? "Seven days" : "Extended rest";

export function summarize(rows: Row[]) {
  const ats = rows.flatMap(row => {
    const { margin, spreadQuotes, game } = row;
    const quote = spreadQuotes.last;
    if (margin === null || !quote || quote.line === 0) return [];
    const favorite = quote.line < 0 ? 0 : 1;
    const sign = favorite === 0 ? 1 : -1;
    const first = spreadQuotes.first!;
    const move = (first.line - quote.line) * sign;
    const movement = first === quote ? "One quote" : Math.sign(first.line) !== Math.sign(quote.line) ? "Favorite changed or opened pick’em"
      : move > 0 ? "Favorite grew" : move < 0 ? "Favorite shrank" : "Unchanged";
    const rest = game.homeRest && game.awayRest
      ? restBin(favorite === 0 ? game.homeRest : game.awayRest) : "Rest unavailable";
    const restDiff = game.homeRest && game.awayRest
      ? (game.homeRest - game.awayRest) * sign : null;
    return [{
      bin: spreadBin(quote.line), favoriteVenue: favorite === 0 ? "Listed home" : "Listed away",
      movement, rest, restDiff,
      favorite: { margin: (margin + quote.line) * sign, odds: quote.prices[favorite] },
      underdog: { margin: -(margin + quote.line) * sign, odds: quote.prices[1 - favorite] },
    }];
  });

  const spreadGroups = (labels: string[], group: (row: typeof ats[number]) => string) =>
    labels.flatMap(label => {
      const subset = ats.filter(row => group(row) === label);
      return [result(label + " · favorite", subset.map(row => row.favorite)),
        result(label + " · underdog", subset.map(row => row.underdog))];
    });

  const totals = (label: string, timing: "first" | "last", bin?: string, paired = false) =>
    [0, 1].map(side => result(label + (side === 0 ? " · over" : " · under"), rows.flatMap(row => {
      const quote = row.totalQuotes[timing];
      if (!quote || row.total === null || (bin && totalBin(quote.line) !== bin)
        || (paired && row.totalQuotes.first === row.totalQuotes.last)) return [];
      return [{ margin: (row.total - quote.line) * (side === 0 ? 1 : -1), odds: quote.prices[side] }];
    })));

  const teams = new Map<string, Observation[]>();

  const timing = {
    spread: { first: [] as number[], last: [] as number[] },
    total: { first: [] as number[], last: [] as number[] },
  };
  const shopping = { spread: { compared: 0, better: 0 }, total: { compared: 0, better: 0 }, moneyline: { compared: 0, better: 0 } };
  const calibration: { actual: number; probability: number; consensus: number | null }[] = [];
  const moneylineSides = { favorite: [] as Observation[], underdog: [] as Observation[] };
  for (const row of rows) {
    const { game, margin, total, spreadQuotes, totalQuotes, moneylineQuotes } = row;
    if (margin !== null && spreadQuotes.last) {
      const { line, prices } = spreadQuotes.last;
      for (const [team, side] of [[game.home, 0], [game.away, 1]] as const) {
        const residual = (margin + line) * (side === 0 ? 1 : -1);
        const teamLine = line * (side === 0 ? 1 : -1);
        const key = team + (teamLine < 0 ? " · favorite" : teamLine > 0 ? " · underdog" : " · pick’em");
        if (!teams.has(key)) {
          teams.set(key, []);

        }
        teams.get(key)!.push({ margin: residual, odds: prices[side] });

      }
    }

    for (const [market, actual, selections] of [["spread", margin, spreadQuotes], ["total", total, totalQuotes]] as const) {
      if (actual !== null && selections.first && selections.last && selections.first !== selections.last) {
        const sign = market === "spread" ? 1 : -1;
        timing[market].first.push(Math.abs(actual + sign * selections.first.line));
        timing[market].last.push(Math.abs(actual + sign * selections.last.line));
      }
    }

    if (margin !== null && moneylineQuotes.last) {
      const quote = moneylineQuotes.last;
      const probability = noVigHome(quote);
      if (probability !== 0.5) {
        const side = probability > 0.5 ? 0 : 1;
        const outcome = margin * (side === 0 ? 1 : -1);
        moneylineSides.favorite.push({ margin: outcome, odds: quote.prices[side] });
        moneylineSides.underdog.push({ margin: -outcome, odds: quote.prices[1 - side] });
      }
      if (margin !== 0) {
        calibration.push({ actual: margin > 0 ? 1 : 0, probability, consensus: row.consensus });
      }
    }

    for (const [market, selected] of [[markets[0], spreadQuotes.early], [markets[1], totalQuotes.early],
      [markets[2], moneylineQuotes.early]] as const) {
      if (!selected) continue;
      const alternatives = Object.keys(selected.quote.books).filter(name => name !== BOOK).flatMap(name => {
        const found = offer(selected.quote, name, market);
        return found && found.line === selected.line ? [found] : [];
      });
      if (!alternatives.length) continue;
      for (const side of [0, 1]) {
        shopping[market].compared++;
        if (alternatives.some(other => profit(other.prices[side]) > profit(selected.prices[side]))) {
          shopping[market].better++;
        }
      }
    }
  }
  const paired = calibration.flatMap(item => item.consensus === null ? [] : [{ ...item, consensus: item.consensus }]);
  const calibrationBins = [0, 0.2, 0.4, 0.6, 0.8].map((lo, i) => {
    const hi = (i + 1) / 5;
    const group = calibration.filter(item => item.probability >= lo && item.probability < hi);
    return { label: String(Math.round(lo * 100)) + "–" + String(Math.round(hi * 100)) + "%", n: group.length,
      probability: average(group.map(item => item.probability)), actual: average(group.map(item => item.actual)) };
  });

  return {
    coverage: {
      games: rows.length, final: rows.filter(row => row.margin !== null).length,
      ...Object.fromEntries(markets.map((market, i) => {
        const selections = rows.map(row => [row.spreadQuotes, row.totalQuotes, row.moneylineQuotes][i]);
        return [market, selections.filter(selection => selection.last).length];
      })),
    } as { games: number; final: number } & Record<Market, number>,
    spread: [...spreadGroups(["All spread favorites"], () => "All spread favorites"),
      ...spreadGroups(["Under 3", "Exactly 3", "3.5 to under 7", "Exactly 7", "Over 7"], row => row.bin)],
    rest: spreadGroups(["Short rest", "Seven days", "Extended rest", "Rest unavailable"], row => row.rest),
    venue: spreadGroups(["Listed home", "Listed away"], row => row.favoriteVenue),
    movement: spreadGroups(["Favorite grew", "Unchanged", "Favorite shrank", "Favorite changed or opened pick’em", "One quote"], row => row.movement),
    totals: [...totals("First archive, paired", "first", undefined, true),
      ...totals("Last archive, paired", "last", undefined, true), ...totals("Last archive, all", "last"),
      ...["Under 42", "42 to under 48", "48+"].flatMap(bin => totals(bin, "last", bin))],
    teams: [...teams.keys()].sort().map(label => result(label, teams.get(label)!)),
    timing: (["spread", "total"] as const).map(market => ({ market, n: timing[market].first.length,
      first: average(timing[market].first), last: average(timing[market].last) })),
    shopping,
    moneyline: [result("Moneyline favorite", moneylineSides.favorite), result("Moneyline underdog", moneylineSides.underdog)],
    calibration: { n: calibration.length, bins: calibrationBins, paired: paired.length,
      pinnacleBrier: average(paired.map(item => (item.probability - item.actual) ** 2)),
      consensusBrier: average(paired.map(item => (item.consensus - item.actual) ** 2)) },
    restEdge: spreadGroups(["Favorite rest edge", "Underdog rest edge", "Equal rest", "Rest unavailable"],
      row => row.restDiff === null ? "Rest unavailable" : row.restDiff > 0 ? "Favorite rest edge"
        : row.restDiff < 0 ? "Underdog rest edge" : "Equal rest"),
  };
}
