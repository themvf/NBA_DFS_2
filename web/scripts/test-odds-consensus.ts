// Consensus American prices must be averaged in probability space. The old
// arithmetic mean produced impossible prices for mixed-sign near-even pairs.
import assert from "node:assert/strict";
import { americanToImplied, consensusAmerican, impliedToAmerican, isImpossibleAmerican, noVigHomeProbability } from "../src/lib/odds-consensus";

// Round trips.
assert.equal(impliedToAmerican(americanToImplied(-150)), -150);
assert.equal(impliedToAmerican(americanToImplied(130)), 130);
assert.equal(impliedToAmerican(americanToImplied(-100)), -100);

// The bug: -105 and +105 averaged arithmetically is 0, an impossible price.
const arithmetic = Math.round((-105 + 105) / 2);
assert.ok(isImpossibleAmerican(arithmetic), "arithmetic mean lands in the impossible band");
const consensus = consensusAmerican([-105, 105]);
assert.ok(consensus != null && !isImpossibleAmerican(consensus), `probability-space mean is quotable: ${consensus}`);
assert.ok(Math.abs(consensus!) <= 105 && Math.abs(consensus!) >= 100, `near even money: ${consensus}`);

// A -110/-110 book and a -110/-110 book agree with themselves.
assert.equal(consensusAmerican([-110, -110, -110]), -110);
// One book at -200 and one at -100 sit between them, not at the arithmetic -150 exactly, but close and quotable.
const mid = consensusAmerican([-200, -100])!;
assert.ok(mid < -100 && mid > -200 && !isImpossibleAmerican(mid), `between the two: ${mid}`);
// Longshot and favourite mixed: arithmetic mean of +400 and -400 is 0; consensus is a real price.
const mixed = consensusAmerican([400, -400])!;
assert.ok(!isImpossibleAmerican(mixed), `mixed extremes stay quotable: ${mixed}`);
// Nothing usable => null, never 0.
assert.equal(consensusAmerican([]), null);
assert.equal(consensusAmerican([0, NaN, 50]), null, "impossible inputs are dropped, not averaged");

// No-vig probability needs both sides; a one-sided quote is not a probability.
assert.equal(noVigHomeProbability(-110, null), null);
const p = noVigHomeProbability(-110, -110)!;
assert.ok(Math.abs(p - 0.5) < 1e-9);
const fav = noVigHomeProbability(-200, 170)!;
assert.ok(fav > 0.6 && fav < 0.7, `favourite around 64%: ${fav}`);

console.log("RESULT: odds-consensus checks passed");
