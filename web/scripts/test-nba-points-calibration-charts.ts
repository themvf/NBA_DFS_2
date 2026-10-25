import assert from "node:assert/strict";
import {
  BIN_WIDTH,
  CLOSE_MINUTES,
  MIN_BIN_N,
  READINGS,
  reliabilityBins,
  reliabilitySeries,
  scatterData,
  type GradedRow,
} from "../src/lib/nba-points-calibration-charts";

const row = (over: Partial<GradedRow> & { line: number; actual: number }): GradedRow => ({
  gameDate: "2026-10-21",
  player: "P",
  minutesBeforeTip: 50,
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

console.log("test-nba-points-calibration-charts: all assertions passed");
