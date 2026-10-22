// Writes tests/fixtures/nba_points_ladder_fixture.json from the TypeScript
// implementation, so model/nba_points_ladder.py can be pinned to it. From web/:
//   node --import tsx ./scripts/write-nba-points-ladder-fixture.ts
import { writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { buildDistribution } from "../src/lib/nba-points-ladder";
import { bookLines, consensus, nameKey, teamKey } from "../src/lib/nba-top-scorers";

function erf(x: number) {
  const s = Math.sign(x); x = Math.abs(x);
  const t = 1 / (1 + 0.3275911 * x);
  const y = 1 - ((((1.061405429 * t - 1.453152027) * t + 1.421413741) * t - 0.284496736) * t + 0.254829592) * t * Math.exp(-x * x);
  return s * y;
}
const S = (x: number) => 0.5 * (1 - erf((x - 25) / (7 * Math.SQRT2)));
const toAmerican = (p: number) => Math.round(p >= 0.5 ? (-100 * p) / (1 - p) : (100 * (1 - p)) / p);
const VIG = 1.045;
const ladder = [9.5, 14.5, 19.5, 29.5, 34.5, 39.5, 44.5].map((pt) => ({ point: pt, side: "over", price: toAmerican(Math.min(0.99, S(pt) * VIG)) }));

const cases = [
  {
    name: "single book normal ladder",
    books: { draftkings: { line: 24.5, over: toAmerican(S(24.5) * VIG), under: toAmerican((1 - S(24.5)) * VIG) } },
    ladders: { draftkings: { ladder } },
  },
  {
    name: "three books, modal tie broken by median, one book without main line skipped, paired rungs",
    books: {
      draftkings: { line: 24.5, over: -115, under: -105 },
      fanduel: { line: 25.5, over: -110, under: -110 },
      betmgm: { line: 24.5, over: -120, under: 100 },
      caesars: { line: 25.5, over: 105, under: -125 },
      fanatics: { line: 26.5, over: 120, under: -140 },
    },
    ladders: {
      draftkings: { ladder: [
        { point: 14.5, side: "over", price: -900 }, { point: 19.5, side: "over", price: -330 },
        { point: 29.5, side: "over", price: 190 }, { point: 34.5, side: "over", price: 500 }, { point: 39.5, side: "over", price: 1200 },
      ] },
      fanduel: { ladder: [
        { point: 19.5, side: "over", price: -300 }, { point: 19.5, side: "under", price: 240 },
        { point: 29.5, side: "over", price: 200 }, { point: 29.5, side: "under", price: -250 },
        { point: 24.5, side: "over", price: -105 },
      ] },
      espnbet: { ladder: [{ point: 19.5, side: "over", price: -280 }, { point: 29.5, side: "over", price: 210 }] },
      betmgm: { ladder: [{ point: 24.5, side: "under", price: -118 }, { point: 29.5, side: "over", price: 180 }] },
    },
  },
  {
    name: "out-of-order books pooled non-increasing",
    books: { draftkings: { line: 20.5, over: -110, under: -110 }, fanduel: { line: 20.5, over: -108, under: -112 } },
    ladders: {
      draftkings: { ladder: [{ point: 14.5, side: "over", price: -500 }, { point: 24.5, side: "over", price: 150 }, { point: 29.5, side: "over", price: 400 }] },
      fanduel: { ladder: [{ point: 14.5, side: "over", price: -450 }, { point: 24.5, side: "over", price: 140 }, { point: 29.5, side: "over", price: 120 }] },
    },
  },
  {
    name: "ladder without any main line: no distribution",
    books: {},
    ladders: { fanduel: { ladder } },
  },
  {
    name: "two rungs only: too few curve points",
    books: { draftkings: { line: 10.5, over: -110, under: -110 } },
    ladders: { draftkings: { ladder: [{ point: 14.5, side: "over", price: 150 }] } },
  },
  {
    name: "invalid prices ignored",
    books: { draftkings: { line: 18.5, over: -50, under: -110 }, fanduel: { line: 18.5, over: -112, under: -108 } },
    ladders: { fanduel: { ladder: [{ point: 9.5, side: "over", price: -2000 }, { point: 14.5, side: "over", price: -400 }, { point: 24.5, side: "over", price: 230 }] } },
  },
];

const out = cases.map((c) => {
  const lines = bookLines(c.books as never);
  return {
    name: c.name,
    books: c.books,
    ladders: c.ladders,
    expected: {
      bookLines: lines,
      consensus: consensus(lines),
      distribution: buildDistribution(
        Object.fromEntries(lines.map((l) => [l.book, { line: l.line, over: l.over, under: l.under }])),
        c.ladders as never,
      ),
    },
  };
});
const names = ["Nikola Jokić", "Jaren Jackson Jr.", "O.G. Anunoby", "Luka Dončić", "Shai Gilgeous-Alexander", "Michael Porter Jr", "LA Clippers", "Los Angeles Clippers"];
const fixture = {
  generatedBy: "web/src/lib/nba-points-ladder.ts + nba-top-scorers.ts (gen-ladder-fixture.ts)",
  nameKeys: Object.fromEntries(names.map((n) => [n, nameKey(n)])),
  teamKeys: Object.fromEntries(["LA Clippers", "Los Angeles Clippers", "Oklahoma City Thunder"].map((n) => [n, teamKey(n)])),
  cases: out,
};
const target = resolve(process.cwd(), "../tests/fixtures/nba_points_ladder_fixture.json");
writeFileSync(target, `${JSON.stringify(fixture, null, 2)}\n`);
console.log(`wrote ${target}`);
