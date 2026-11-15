import assert from "node:assert/strict";
import { isFrozenDeploymentHost, lockCountdown } from "../src/lib/nfl-dfs/lock-countdown";

const kickoff = "2026-11-15T18:00:00Z"; // 1:00 PM ET
const at = (iso: string) => Date.parse(iso);
assert.equal(lockCountdown(null, at(kickoff)), null);
assert.deepEqual(lockCountdown(kickoff, at("2026-11-15T15:00:00Z")), { state: "open", text: "Locks in 3h 00m · first game's inactives about 11:30 AM ET (in 1h 30m)" });
assert.deepEqual(lockCountdown(kickoff, at("2026-11-15T17:00:00Z")), { state: "open", text: "Locks in 1h 00m · first game's inactives posted about 11:30 AM ET" });
assert.equal(lockCountdown(kickoff, at("2026-11-15T17:45:00Z"))!.state, "closing");
assert.equal(lockCountdown(kickoff, at("2026-11-15T18:00:01Z"))!.state, "locked");

const prod = "nbadfs.vercel.app";
assert.equal(isFrozenDeploymentHost("nbadfs.vercel.app", prod), false);
assert.equal(isFrozenDeploymentHost("nbadfs-josh-bandes-projects.vercel.app", prod), false, "the project alias follows the newest deployment");
assert.equal(isFrozenDeploymentHost("nbadfs-git-main-josh-bandes-projects.vercel.app", prod), false, "the branch alias follows the newest deployment");
assert.equal(isFrozenDeploymentHost("nbadfs-9s2a7jr7v-josh-bandes-projects.vercel.app", prod), true, "a per-deployment address never updates");
assert.equal(isFrozenDeploymentHost("localhost:3000", prod), false);
assert.equal(isFrozenDeploymentHost("nbadfs-9s2a7jr7v-josh-bandes-projects.vercel.app", null), false, "no production host known: say nothing");
console.log("Lock countdown and frozen-address check: all assertions passed.");
