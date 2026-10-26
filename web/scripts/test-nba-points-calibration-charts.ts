import assert from "node:assert/strict";
import {
  BIN_WIDTH,
  CLOSE_MINUTES,
  BASELINE_PRIOR_ROWS,
  MIN_BIN_N,
  READINGS,
  SHORT_MINUTES,
  coverageData,
  reliabilityBins,
  reliabilitySeries,
  scatterData,
  shortMinutesSummary,
  skillRow,
  skillTable,
  tailTrend,
  type GradedRow,
} from "../src/lib/nba-points-calibration-charts";

const row = (over: Partial<GradedRow> & { line: number; actual: number }): GradedRow => ({
  gameDate: "2026-10-21",
  player: "P",
  minutesBeforeTip: 50,
  minutes: 30,
  floor: null,
  ceiling: null,
  pOver: null,
  p20: null,
  p25: null,
  p30: null,
  p35: null,
  p40: null,
  ...over,
});

// Reliability: bins are plotted at the MEAN stated chance, observed = hit share.
const over = READINGS[0];
const rows = [
  row({ line: 20.5, actual: 25, pOver: 0.52 }),
  row({ line: 20.5, actual: 15, pOver: 0.58 }),
  row({ line: 20.5, actual: 30, pOver: 0.55 }),
  row({ line: 20.5, actual: 10, pOver: 0.31 }),
  row({ line: 20.5, actual: 12, pOver: 1.0 }), // 1.0 belongs to the top bin, not an 11th bin
  row({ line: 20.5, actual: 12, pOver: null }), // no stated chance: not binned
];
const bins = reliabilityBins(rows, over);
assert.deepEqual(bins.map((b) => [b.lo, b.n]), [[0.3, 1], [0.5, 3], [0.9, 1]]);
const mid = bins[1];
assert.ok(Math.abs(mid.stated - 0.55) < 1e-9, `mean stated ${mid.stated}`);
assert.ok(Math.abs(mid.observed - 2 / 3) < 1e-9, `observed ${mid.observed}`);
assert.ok(mid.thin && MIN_BIN_N > 3, "thin below the floor");
assert.equal(bins[2].hi, 1.0);
assert.equal(bins[2].observed, 0);

// A bin with enough rows is not thin, and the 20+ reading grades on actual >= 20.
const many = Array.from({ length: MIN_BIN_N }, (_, i) => row({ line: 18.5, actual: i % 2 ? 24 : 16, p20: 0.5 + (i % 5) * 0.01 }));
const twenty = reliabilityBins(many, READINGS[1]);
assert.equal(twenty.length, 1);
assert.equal(twenty[0].n, MIN_BIN_N);
assert.ok(!twenty[0].thin);
assert.equal(twenty[0].observed, 0.5);
assert.ok(twenty[0].stated >= 0.5 && twenty[0].stated < 0.5 + BIN_WIDTH);

// Series: one per reading, n counts only rows that carried that reading.
const series = reliabilitySeries([...rows, ...many]);
assert.equal(series.length, READINGS.length);
assert.equal(series.find((s) => s.key === "over")?.n, 5);
assert.equal(series.find((s) => s.key === "20")?.n, MIN_BIN_N);
assert.equal(series.find((s) => s.key === "40")?.n, 0);

// Scatter: newest nights only, close/early split at CLOSE_MINUTES, bias and MAE over shown points.
const nightly = [
  row({ gameDate: "2026-10-01", line: 20.5, actual: 30, minutesBeforeTip: 300 }),
  row({ gameDate: "2026-10-02", line: 20.5, actual: 18, minutesBeforeTip: CLOSE_MINUTES }),
  row({ gameDate: "2026-10-03", line: 25.5, actual: 31, minutesBeforeTip: CLOSE_MINUTES + 1 }),
];
const s = scatterData(nightly, 2);
assert.equal(s.nights, 2);
assert.deepEqual([s.firstDate, s.lastDate], ["2026-10-02", "2026-10-03"]);
assert.deepEqual(s.points.map((p) => p.group), ["close", "early"]);
assert.ok(Math.abs((s.bias ?? 0) - (-2.5 + 5.5) / 2) < 1e-9, `bias ${s.bias}`);
assert.ok(Math.abs((s.mae ?? 0) - (2.5 + 5.5) / 2) < 1e-9, `mae ${s.mae}`);
assert.equal(s.maxValue, 35);
const empty = scatterData([]);
assert.equal(empty.nights, 0);
assert.equal(empty.bias, null);

// Coverage: only rows with both floor and ceiling count; inside is inclusive.
const cov = coverageData([
  row({ line: 20.5, actual: 10, floor: 12, ceiling: 30 }), // below
  row({ line: 20.5, actual: 12, floor: 12, ceiling: 30 }), // on the floor: inside
  row({ line: 20.5, actual: 30, floor: 12, ceiling: 30 }), // on the ceiling: inside
  row({ line: 20.5, actual: 31, floor: 12, ceiling: 30 }), // above
  row({ line: 20.5, actual: 40, floor: null, ceiling: 30 }), // no floor: not counted
]);
assert.equal(cov.n, 4);
assert.deepEqual([cov.below, cov.inside, cov.above], [0.25, 0.5, 0.25]);
assert.deepEqual(cov.target, { below: 0.1, inside: 0.8, above: 0.1 });
assert.equal(coverageData([]).n, 0);

// Tail trend: one point per night that carried the reading, running means across nights.
const trend = tailTrend([
  row({ gameDate: "2026-10-01", line: 20.5, actual: 32, p30: 0.4 }),
  row({ gameDate: "2026-10-01", line: 20.5, actual: 12, p30: 0.2 }),
  row({ gameDate: "2026-10-02", line: 20.5, actual: 12, p30: null }), // no reading: night skipped for 30+
  row({ gameDate: "2026-10-03", line: 20.5, actual: 35, p30: 0.6 }),
]);
const thirty = trend.find((s) => s.key === "30")!;
assert.equal(thirty.n, 3);
assert.deepEqual(thirty.points.map((p) => p.date), ["2026-10-01", "2026-10-03"]);
assert.ok(Math.abs(thirty.points[0].statedNight - 0.3) < 1e-9 && thirty.points[0].observedNight === 0.5);
assert.equal(thirty.points[1].nCum, 3);
assert.ok(Math.abs(thirty.points[1].statedCum - 0.4) < 1e-9, `statedCum ${thirty.points[1].statedCum}`);
assert.ok(Math.abs(thirty.points[1].observedCum - 2 / 3) < 1e-9, `observedCum ${thirty.points[1].observedCum}`);
assert.equal(trend.find((s) => s.key === "40")!.points.length, 0);

// Skill: base rate in-sample, line-only leave-one-night-out with BASELINE_PRIOR_ROWS shrinkage.
assert.equal(BASELINE_PRIOR_ROWS, 10, "the hand-computed example below assumes K = 10");
const thirtyReading = READINGS.find((r) => r.key === "30")!;
const skillRows = [
  row({ gameDate: "2026-10-01", line: 27, actual: 32, p30: 0.5 }), // band 9, hit
  row({ gameDate: "2026-10-01", line: 27, actual: 20, p30: 0.3 }), // band 9, miss
  row({ gameDate: "2026-10-02", line: 27, actual: 35, p30: 0.6 }), // band 9, hit
  row({ gameDate: "2026-10-02", line: 15, actual: 10, p30: 0.05 }), // band 5, miss
  row({ gameDate: "2026-10-02", line: 15, actual: 10, p30: null }), // no reading: not scored
];
const sk = skillRow(skillRows, thirtyReading);
assert.equal(sk.n, 4);
assert.equal(sk.nights, 2);
assert.ok(Math.abs((sk.brier ?? 0) - 0.5025 / 4) < 1e-12, `brier ${sk.brier}`);
assert.equal(sk.baseRate, 0.5);
assert.equal(sk.brierBase, 0.25);
assert.ok(Math.abs((sk.skillVsBase ?? 0) - (1 - 0.5025 / 4 / 0.25)) < 1e-12);
// Night 1 rows: other night has 1 of 2 hits (pool 0.5) and band 9 there is 1 of 1 -> (1 + 5) / 11.
// Night 2 band 9: other night band 9 is 1 of 2 -> (1 + 5) / 12 = 0.5; band 5: empty -> 5 / 10 = 0.5.
const expectedLine = ((5 / 11) ** 2 + (6 / 11) ** 2 + 0.25 + 0.25) / 4;
assert.equal(sk.lineN, 4);
assert.ok(Math.abs((sk.brierLine ?? 0) - expectedLine) < 1e-12, `line ${sk.brierLine} vs ${expectedLine}`);
assert.ok(Math.abs((sk.brierPaired ?? 0) - (sk.brier ?? 0)) < 1e-12, "every row paired here");
assert.ok(Math.abs((sk.skillVsLine ?? 0) - (1 - (sk.brierPaired ?? 0) / expectedLine)) < 1e-12);
// One night only: no line-only guess, base rate still defined.
const oneNight = skillRow(skillRows.filter((r) => r.gameDate === "2026-10-01"), thirtyReading);
assert.equal(oneNight.lineN, 0);
assert.equal(oneNight.brierLine, null);
assert.equal(oneNight.skillVsLine, null);
assert.equal(oneNight.brierBase, 0.25);
// No rows carry the reading: everything null, n 0.
const none = skillRow(skillRows, READINGS.find((r) => r.key === "40")!);
assert.equal(none.n, 0);
assert.equal(none.brier, null);
assert.equal(skillTable(skillRows).length, READINGS.length);

// Short nights: under SHORT_MINUTES, counted and drawn hollow, never dropped.
assert.equal(SHORT_MINUTES, 15);
const shortRows = [
  row({ line: 14.5, actual: 2, minutes: 7 }),
  row({ line: 14.5, actual: 16, minutes: 15 }), // exactly 15 is not short
  row({ line: 14.5, actual: 16, minutes: null }), // unknown minutes is not short
];
assert.deepEqual(shortMinutesSummary(shortRows), { n: 3, short: 1, withMinutes: 2 });
assert.deepEqual(scatterData(shortRows).points.map((p) => p.short), [true, false, false]);
assert.equal(scatterData(shortRows).points.length, 3, "short nights stay on the scatter");

console.log("test-nba-points-calibration-charts: all assertions passed");
