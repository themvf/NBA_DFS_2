/**
 * /cfb/pattern-watch is private: the page opens only with the configured key,
 * and every other case (no key set, a short key, missing or wrong input) fails closed.
 */
import assert from "node:assert/strict";
import { MIN_KEY_LENGTH, patternWatchKeyValid } from "../src/lib/cfb-pattern-watch-access";

const good = "k".repeat(MIN_KEY_LENGTH) + "-pattern";

assert.equal(patternWatchKeyValid(good, good), true, "the configured key opens the page");
assert.equal(patternWatchKeyValid(good + "x", good), false, "a different key is refused");
assert.equal(patternWatchKeyValid("", good), false, "an empty key is refused");
assert.equal(patternWatchKeyValid(undefined, good), false, "no key is refused");
assert.equal(patternWatchKeyValid(good, undefined), false, "no configured key means the page stays closed");
assert.equal(patternWatchKeyValid("short", "short"), false, "a configured key under the minimum length keeps the page closed");

console.log("CFB pattern watch access: 6 checks passed.");
