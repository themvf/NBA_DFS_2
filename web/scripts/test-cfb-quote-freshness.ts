import assert from "node:assert/strict";
import { isCfbQuoteFresh } from "../src/lib/cfb-quote-freshness";

const quote = "2026-10-03T18:55:00Z";
const capture = "2026-10-03T18:56:00Z";
const kickoff = "2026-10-03T19:10:00Z";

assert.equal(isCfbQuoteFresh(quote, capture, kickoff, Date.parse("2026-10-03T18:59:59Z")), true);
assert.equal(isCfbQuoteFresh(quote, capture, kickoff, Date.parse("2026-10-03T19:00:01Z")), false);
assert.equal(isCfbQuoteFresh(quote, capture, "2026-10-03T18:59:00Z", Date.parse("2026-10-03T18:59:00Z")), false);
assert.equal(isCfbQuoteFresh("2026-10-03T19:01:00Z", capture, kickoff, Date.parse("2026-10-03T18:59:00Z")), false);
assert.equal(isCfbQuoteFresh(quote, null, kickoff, Date.parse("2026-10-03T18:59:00Z")), false);

console.log("CFB paper quote freshness checks passed");
