import assert from "node:assert/strict";
import {
  CONTROL_ARM_ALERT_TYPE,
  ENROLLED_DETECTOR_VERSION,
  LIVE_ARM_ALERT_TYPE,
  applyFilters,
  bookLabel,
  classifyLifecycle,
  controlArm,
  formatAmerican,
  formatCountdown,
  liveArmHistory,
  liveBoard,
  marketLabel,
  minutesUntil,
  toPropPlay,
  urgency,
} from "../src/lib/mlb-prop-board";

const NOW = new Date("2026-08-23T18:00:00Z");

type Raw = Parameters<typeof toPropPlay>[0];

function alert(over: Partial<Raw> & { details?: Record<string, unknown> }): Raw {
  return {
    matchupId: 1,
    createdAt: "2026-08-23T15:00:00Z",
    matchup: "NYY @ BOS",
    commenceTime: "2026-08-23T23:05:00Z",
    alertType: LIVE_ARM_ALERT_TYPE,
    side: "Player K O5.5",
    alertProb: 0.5,
    sharpProb: 0.54,
    clvPp: null,
    outcome: null,
    ...over,
    details: {
      market: "pitcher_strikeouts",
      player: "Roki Sasaki",
      line: 5.5,
      bet: "Over",
      exec_book: "fanduel",
      exec_odds: -108,
      exec_gain_pct: 1.8,
      dk_odds: -115,
      ev_pct: 4.2,
      books_qualifying: 1,
      detector_version: ENROLLED_DETECTOR_VERSION,
      ...(over.details ?? {}),
    },
  } as Raw;
}

/* ── lifecycle: the split the old page could not make ────────────────────── */

assert.equal(minutesUntil("2026-08-23T19:00:00Z", NOW), 60);
assert.equal(minutesUntil("2026-08-23T17:00:00Z", NOW), -60);
assert.equal(minutesUntil(null, NOW), null, "no commence time yields null, not 0");
assert.equal(minutesUntil("not-a-date", NOW), null);

assert.equal(classifyLifecycle(null, 60), "live");
assert.equal(classifyLifecycle(null, -1), "started");
assert.equal(classifyLifecycle(null, 0), "started", "first pitch exactly now is not bettable");
// A missing start time must NOT be folded into "started" — we cannot tell, and
// the board says so rather than guessing.
assert.equal(classifyLifecycle(null, null), "unknown");
// A graded alert is settled regardless of clock.
assert.equal(classifyLifecycle("won", 60), "settled");
assert.equal(classifyLifecycle("lost", null), "settled");

/* ── arm classification ──────────────────────────────────────────────────── */

assert.equal(toPropPlay(alert({}), NOW).arm, "live");
assert.equal(
  toPropPlay(alert({ alertType: CONTROL_ARM_ALERT_TYPE }), NOW).arm,
  "control",
);

/* ── execution price is the ACTION; DK's price is the selection price ────── */

const p = toPropPlay(alert({}), NOW);
assert.equal(p.execBook, "fanduel");
assert.equal(p.execOdds, -108);
assert.equal(p.dkOdds, -115, "DraftKings' price is kept: it is what CLV grades on");
assert.equal(p.execGainPct, 1.8);
assert.equal(p.player, "Roki Sasaki");
assert.equal(p.marketLabel, "Strikeouts");
assert.equal(p.enrolled, true);
assert.equal(p.anchoredMarket, true);

// A detector generation that is not the enrolled cohort is not poolable.
assert.equal(toPropPlay(alert({ details: { detector_version: "prop-value-v2-multibook" } }), NOW).enrolled, false);
assert.equal(toPropPlay(alert({ details: { detector_version: null } }), NOW).enrolled, false);
// A market outside the census set is flagged, not hidden.
assert.equal(toPropPlay(alert({ details: { market: "pitcher_earned_runs" } }), NOW).anchoredMarket, false);

/* ── the board: live arm only, un-started only, soonest first ────────────── */

const plays = [
  toPropPlay(alert({ matchupId: 1, commenceTime: "2026-08-23T23:05:00Z", details: { ev_pct: 3.1 } }), NOW),
  toPropPlay(alert({ matchupId: 2, commenceTime: "2026-08-23T19:10:00Z", details: { ev_pct: 3.2 } }), NOW),
  toPropPlay(alert({ matchupId: 3, commenceTime: "2026-08-23T19:10:00Z", details: { ev_pct: 9.9 } }), NOW),
  toPropPlay(alert({ matchupId: 4, commenceTime: "2026-08-23T16:00:00Z" }), NOW),       // started
  toPropPlay(alert({ matchupId: 5, outcome: "won", clvPp: 1.4 }), NOW),                  // settled
  toPropPlay(alert({ matchupId: 6, alertType: CONTROL_ARM_ALERT_TYPE }), NOW),           // control
  toPropPlay(alert({ matchupId: 7, commenceTime: null }), NOW),                          // unknown
];

const board = liveBoard(plays);
assert.deepEqual(board.map((b) => b.key.split(":")[0]), ["3", "2", "1"],
  "soonest first pitch first; EV only breaks a tie within the same start time");
assert.ok(!board.some((b) => b.arm === "control"), "the control arm never reaches the board");
assert.ok(!board.some((b) => b.lifecycle !== "live"), "started/settled/unknown never reach the board");

// The highest-EV row is NOT first overall — a board ranked by claimed edge
// reads as a ranked recommendation, which the evidence does not support.
assert.notEqual(liveBoard(plays)[0].evPct, 3.1);
assert.equal(board[0].evPct, 9.9, "9.9 wins only because it ties 19:10 with the 3.2 row");

const history = liveArmHistory(plays);
assert.equal(history.length, 3, "started + settled + unknown, all live-arm");
assert.ok(!history.some((h) => h.arm === "control"));
assert.equal(controlArm(plays).length, 1);

/* ── filters ─────────────────────────────────────────────────────────────── */

assert.equal(applyFilters(board, { market: "all", minEvPct: 0 }).length, 3);
assert.equal(applyFilters(board, { market: "all", minEvPct: 5 }).length, 1);
assert.equal(applyFilters(board, { market: "batter_total_bases", minEvPct: 0 }).length, 0);
assert.equal(applyFilters(board, { market: "pitcher_strikeouts", minEvPct: 0 }).length, 3);

/* ── formatting: a wrong book name sends someone to the wrong sportsbook ─── */

assert.equal(bookLabel("williamhill_us"), "Caesars");
assert.equal(bookLabel("draftkings"), "DraftKings");
assert.equal(bookLabel("unknown_book"), "unknown_book", "unmapped keys pass through, never blank");
assert.equal(bookLabel(null), "—");

assert.equal(formatAmerican(-108), "-108");
assert.equal(formatAmerican(145), "+145");
assert.equal(formatAmerican(null), "—");
assert.equal(marketLabel("pitcher_outs"), "Outs Recorded");
assert.equal(marketLabel(null), "—");

assert.equal(formatCountdown(null), "start time unknown");
assert.equal(formatCountdown(-5), "started");
assert.equal(formatCountdown(45), "45m to first pitch");
assert.equal(formatCountdown(150), "2h 30m to first pitch");
assert.equal(formatCountdown(60 * 30), "1d 6h to first pitch");

assert.equal(urgency(30), "soon");
assert.equal(urgency(300), "today");
assert.equal(urgency(60 * 20), "later");
assert.equal(urgency(null), "later");

console.log("MLB prop board tests passed");
