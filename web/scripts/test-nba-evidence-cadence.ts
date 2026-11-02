import assert from "node:assert/strict";
import {nbaGhostFreezeDue,nbaGhostGradeDue,nbaInjuryRefreshDue} from "../src/lib/cron-dispatch";
for(const [yes,no] of [["2026-10-31T15:37:00Z","2026-10-31T16:37:00Z"],["2026-11-02T16:37:00Z","2026-11-02T15:37:00Z"]]){assert(nbaGhostFreezeDue(new Date(yes)));assert(!nbaGhostFreezeDue(new Date(no)));}
assert(nbaGhostGradeDue(new Date("2026-11-02T12:37:00Z")));assert(nbaInjuryRefreshDue(new Date("2026-11-02T23:52:00Z")));assert(!nbaInjuryRefreshDue(new Date("2026-11-02T22:52:00Z")));assert(!nbaGhostFreezeDue(new Date("2026-07-02T15:37:00Z")));
console.log("PASS: Eastern freeze/grade and bounded injury checks across DST and off-season");
