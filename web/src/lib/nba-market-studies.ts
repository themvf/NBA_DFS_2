export type BookQuote = {
  spread?: number; home_price?: number; away_price?: number;
  total?: number; over_price?: number; under_price?: number;
};

export type MarketCapture = {
  id: string; observedAt: string; marketStart: string;
  books: Record<string, BookQuote>;
};

export type StudyGame = {
  id: string; date: string; home: string; away: string;
  start: string | null; cutoff: string | null;
  homeScore: number | null; awayScore: number | null;
  homeBackToBack: boolean; awayBackToBack: boolean;
  homePriorVenue: string | null; awayPriorVenue: string | null;
  captures: MarketCapture[];
};

export const PAPER_START = "2026-11-15";
export const isNumber = (x: unknown): x is number => typeof x === "number" && Number.isFinite(x);
const isPrice = (x: unknown): x is number => isNumber(x) && Math.abs(x) >= 100;

function settlement(margin: number): "win" | "loss" | "push" {
  return margin > 0 ? "win" : margin < 0 ? "loss" : "push";
}

export function grade(game: StudyGame) {
  const ready = isNumber(game.homeScore) && isNumber(game.awayScore);
  const margin = ready ? game.homeScore! - game.awayScore! : null;
  const actualTotal = ready ? game.homeScore! + game.awayScore! : null;
  const pin = game.captures.filter(c => c.books.pinnacle);
  const spreadQuotes = pin.filter(c => isNumber(c.books.pinnacle.spread));
  const totalQuotes = pin.filter(c => isNumber(c.books.pinnacle.total));
  const firstSpread = spreadQuotes[0]?.books.pinnacle.spread;
  const spread = spreadQuotes.at(-1)?.books.pinnacle.spread;
  const total = totalQuotes.at(-1)?.books.pinnacle.total;
  const cutoff = game.cutoff ? Date.parse(game.cutoff) : NaN;
  const beforeCutoff = pin.filter(c => Date.parse(c.observedAt) < cutoff);
  const atCutoff = beforeCutoff.at(-1);
  const paperCapture = [...beforeCutoff].reverse().find(({ books }) => {
    const quote = books.pinnacle;
    return isNumber(quote.total) && isPrice(quote.over_price) && isPrice(quote.under_price);
  });
  const paperQuote = paperCapture?.books.pinnacle;
  const paperTotal = paperQuote?.total;
  const hasPaperQuote = paperCapture !== undefined;
  const homeFavorite = isNumber(spread) && spread < 0;
  const favoriteMargin = margin !== null && isNumber(spread) && spread !== 0 ? margin * (homeFavorite ? 1 : -1) : null;
  const coverMargin = favoriteMargin !== null && isNumber(spread) ? favoriteMargin - Math.abs(spread) : null;
  const movement = isNumber(spread) && isNumber(firstSpread) && spread !== 0 && spreadQuotes.length > 1
    ? (spread - firstSpread) * (homeFavorite ? -1 : 1) : null;
  return {
    game, margin, actualTotal, spread, firstSpread, total, homeFavorite, spreadQuotes,
    favoriteMargin, coverMargin, movement, lastTotal: totalQuotes.at(-1),
    firstTotal: totalQuotes[0], atCutoff, paperCapture, paperQuote, hasPaperQuote,
    paperPlay: game.date >= PAPER_START && hasPaperQuote && (paperTotal ?? 0) >= 235,
  };
}

type Graded = ReturnType<typeof grade>;
type Bucket = { label: string; games: number; win: number; loss: number; push: number; units: number; priced: number };

function bucket(label: string, observations: { margin: number; price?: number }[]): Bucket {
  const result = { label, games: observations.length, win: 0, loss: 0, push: 0, units: 0, priced: 0 };
  for (const { margin, price } of observations) {
    const status = settlement(margin);
    result[status]++;
    if (isPrice(price)) {
      result.priced++;
      result.units += status === "win" ? (price > 0 ? price / 100 : 100 / -price) : status === "loss" ? -1 : 0;
    }
  }
  return result;
}

export function summarize(rows: Graded[]) {
  const spreadRows = rows.filter(row => row.coverMargin !== null);
  const spreadBuckets = (labels: string[], group: (row: Graded) => string | null) =>
    labels.map(label => {
      const cohort = spreadRows.filter(row => group(row) === label);
      return {
        ...bucket(label, cohort.map(row => ({ margin: row.coverMargin! }))),
        outright: cohort.filter(row => row.favoriteMargin! > 0).length,
      };
    });
  const rest = (row: Graded) => {
    const favorite = row.homeFavorite ? row.game.homeBackToBack : row.game.awayBackToBack;
    const underdog = row.homeFavorite ? row.game.awayBackToBack : row.game.homeBackToBack;
    return favorite ? underdog ? "Both" : "Favorite only" : underdog ? "Underdog only" : "Neither";
  };
  const totals = (label: string, getQuote: (row: Graded) => BookQuote | undefined, side: "over" | "under", group?: string) =>
    bucket(label, rows.flatMap(row => {
      const quote = getQuote(row);
      if (row.actualTotal === null || !quote || !isNumber(quote.total)) return [];
      const bin = quote.total < 225 ? "Under 225" : quote.total < 235 ? "225 to under 235" : "235+";
      if (group && bin !== group) return [];
      return [{ margin: (row.actualTotal - quote.total) * (side === "over" ? 1 : -1), price: quote[side === "over" ? "over_price" : "under_price"] }];
    }));
  const paper = rows.filter(row => row.game.date >= PAPER_START);
  const paperPicks = paper.filter(row => row.paperPlay);
  const settled = paperPicks.filter(row => row.actualTotal !== null);
  const byTeam = new Map<string, { first: number[]; last: number[] }>();
  for (const row of rows) {
    if (row.margin === null || !isNumber(row.spread) || !isNumber(row.firstSpread) || row.spread === 0) continue;
    const { margin, spread, firstSpread } = row;
    for (const [team, sign] of [[row.game.home, 1], [row.game.away, -1]] as const) {
      const label = `${team} · ${spread * sign < 0 ? "Favorite" : "Underdog"}`;
      const values = byTeam.get(label) ?? { first: [], last: [] };
      values.first.push(sign * (margin + firstSpread));
      values.last.push(sign * (margin + spread));
      byTeam.set(label, values);
    }
  }
  const teams = [...byTeam.entries()].sort(([a], [b]) => a.localeCompare(b)).map(([label, v]) => {
    const sorted = [...v.last].sort((a, b) => a - b);
    const middle = (sorted[Math.floor((sorted.length - 1) / 2)] + sorted[Math.floor(sorted.length / 2)]) / 2;
    const mostExtreme = v.last.reduce((largest, value) => Math.abs(value) > Math.abs(largest) ? value : largest);
    const trimmedAverage = sorted.length > 1
      ? (v.last.reduce((sum, value) => sum + value, 0) - mostExtreme) / (sorted.length - 1) : null;
    return { label, median: middle, trimmedAverage,
      first: bucket(label, v.first.map(margin => ({ margin }))), last: bucket(label, v.last.map(margin => ({ margin }))) };
  });
  let comparisons = 0;
  let improvements = 0;
  for (const row of rows) {
    const capture = row.atCutoff;
    if (!capture) continue;
    const pin = capture.books.pinnacle;
    for (const [line, side, opposite] of [
      ["spread", "home_price", "away_price"], ["spread", "away_price", "home_price"],
      ["total", "over_price", "under_price"], ["total", "under_price", "over_price"],
    ] as const) {
      const pinPrice = pin[side];
      if (!isNumber(pin[line]) || !isPrice(pinPrice) || !isPrice(pin[opposite])) continue;
      const candidates = Object.entries(capture.books).flatMap(([name, quote]) =>
        name !== "pinnacle" && quote[line] === pin[line] && isPrice(quote[side]) ? [quote[side]] : []);
      if (!candidates.length) continue;
      const payout = (p: number) => p > 0 ? p / 100 : 100 / -p;
      comparisons++;
      if (candidates.some(p => payout(p) > payout(pinPrice))) improvements++;
    }
  }
  return {
    games: rows.length, spreadGraded: spreadRows.length,
    spread: spreadBuckets(["Under 4", "4 to under 7", "7 to under 10", "10+"], row => {
      const s = Math.abs(row.spread!);
      return s < 4 ? "Under 4" : s < 7 ? "4 to under 7" : s < 10 ? "7 to under 10" : "10+";
    }),
    rest: spreadBuckets(["Neither", "Underdog only", "Favorite only", "Both"], rest),
    venue: spreadBuckets(["Home, under 7", "Away, under 7", "Home, 7+", "Away, 7+"],
      row => `${row.homeFavorite ? "Home" : "Away"}, ${Math.abs(row.spread!) < 7 ? "under 7" : "7+"}`),
    movement: spreadBuckets(["Grew", "Unchanged", "Shrank", "Single quote"], row =>
      row.movement === null ? "Single quote" : row.movement > 0 ? "Grew" : row.movement < 0 ? "Shrank" : "Unchanged"),
    totals: [
      totals("First quote · over", row => row.firstTotal?.books.pinnacle, "over"),
      totals("First quote · under", row => row.firstTotal?.books.pinnacle, "under"),
      totals("Last quote · over", row => row.lastTotal?.books.pinnacle, "over"),
      totals("Last quote · under", row => row.lastTotal?.books.pinnacle, "under"),
      ...["Under 225", "225 to under 235", "235+"].flatMap(bin => (["over", "under"] as const).map(side =>
        totals(`${bin} · ${side}`, row => row.lastTotal?.books.pinnacle, side, bin))),
    ],
    paper: {
      games: paper.length, missing: paper.filter(row => !row.hasPaperQuote).length,
      pending: paperPicks.length - settled.length,
      result: bucket("Over 235+", settled.map(row => ({
        margin: row.actualTotal! - row.paperQuote!.total!, price: row.paperQuote!.over_price,
      }))),
    },
    teams, shopping: { comparisons, improvements },
    travel: spreadBuckets(["Underdog only on back-to-back · same venue", "Underdog only on back-to-back · changed venue"], row => {
      if (rest(row) !== "Underdog only") return null;
      const previous = row.homeFavorite ? row.game.awayPriorVenue : row.game.homePriorVenue;
      if (!previous) return null;
      return `Underdog only on back-to-back · ${previous === row.game.home ? "same" : "changed"} venue`;
    }),
  };
}
