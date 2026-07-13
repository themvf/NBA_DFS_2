import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

import {
  canPromoteTennisSurfaceElo,
  tennisSurfaceActionMessage,
} from "../src/lib/tennis-elo-policy";

assert.equal(canPromoteTennisSurfaceElo([]), false);
assert.equal(canPromoteTennisSurfaceElo([{ tour: "ATP", status: "PASS" }]), false);
assert.equal(
  canPromoteTennisSurfaceElo([
    { tour: "ATP", status: "PASS" },
    { tour: "WTA", status: "FAIL" },
  ]),
  false,
);
assert.equal(
  canPromoteTennisSurfaceElo([
    { tour: "ATP", status: "PASS" },
    { tour: "WTA", status: "PASS" },
  ]),
  true,
);
assert.match(
  tennisSurfaceActionMessage([
    { tour: "ATP", status: "FAIL" },
    { tour: "WTA", status: "FAIL" },
  ]),
  /do not place a bet/i,
);

const querySource = readFileSync(new URL("../src/db/queries.ts", import.meta.url), "utf8");
assert.match(querySource, /tennis_elo_promotion_gates/);
assert.match(querySource, /WHERE e\.eligible/);
assert.match(querySource, /fs\.provenance ->> 'run_id' = r\.run_id::text/);
assert.match(querySource, /source_availability ->> 'serve_return'/);

const routeSource = readFileSync(
  new URL("../src/app/api/tennis/surface-evidence/route.ts", import.meta.url),
  "utf8",
);
assert.match(routeSource, /promotionStatus/);
assert.match(routeSource, /getTennisEloDashboard/);

console.log("Tennis surface-Elo policy tests passed");
