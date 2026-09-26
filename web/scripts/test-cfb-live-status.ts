/**
 * CFB live DraftKings status: lobby filtering, pool parsing, slate matching
 * (unique name + team on both sides only), and the Questionable cap policy.
 */
import assert from "node:assert/strict";
import { canonicalStatus, effectiveStatus, matchStatuses, openForStatus, parseLobby, parsePool, poolOverlap } from "../src/lib/cfb-dfs/live-status";

const lobby = parseLobby({ DraftGroups: [
  { DraftGroupId: 153951, ContestTypeId: 94, StartDate: "2026-09-26T23:00:00.0000000Z", GameCount: 7, ContestStartTimeSuffix: " (Night) " },
  { DraftGroupId: 153952, ContestTypeId: 95, StartDate: "2026-09-26T23:30:00.0000000Z", GameCount: 1, ContestStartTimeSuffix: "(TA&M @ LSU)" },
] });
assert.deepEqual(lobby.map((g) => g.draftGroupId), [153951], "Showdown (95) groups are not CFB Classic");
assert.equal(lobby[0].start, "2026-09-26T23:00:00.000Z");
assert.equal(lobby[0].suffix, "(Night)");

// Pool rows in DraftKings' getavailableplayers shape.
const row = (fn: string, ln: string, home: boolean, i: string) =>
  ({ fn, ln, i, s: 5000, tid: home ? 1 : 2, htid: 1, atid: 2, htabbr: "UAB", atabbr: "NAVY", IsDisabledFromDrafting: false });
const pool = parsePool({ playerList: [
  row("Braxton", "Woodson", false, "Q"), row("Jackson", "Gutierrez", false, ""), row("Khobie", "Martin", true, "O"),
  row("Twin", "Name", true, ""), row("Twin", "Name", true, "O"),
] });
assert.deepEqual(pool.find((p) => p.name === "Braxton Woodson"), { key: "braxtonwoodson", team: "NAVY", status: "Q", name: "Braxton Woodson", salary: 5000, disabled: false });
assert.equal(pool.find((p) => p.name === "Khobie Martin")!.team, "UAB", "home player takes the home abbreviation");

const slate = [
  { dkId: 1, name: "Braxton Woodson", team: "NAVY" }, { dkId: 2, name: "Jackson Gutierrez", team: "NAVY" },
  { dkId: 3, name: "Khobie Martin", team: "UAB" }, { dkId: 4, name: "Twin Name", team: "UAB" },
  { dkId: 5, name: "Not In Pool", team: "UAB" },
];
const { matches, unmatched } = matchStatuses(slate, pool);
assert.deepEqual(matches, [{ dkId: 1, status: "Q" }, { dkId: 2, status: "" }, { dkId: 3, status: "O" }]);
assert.equal(unmatched, 2, "a name+team DK lists twice is ambiguous, and a missing player is unmatched: both left alone");
assert.equal(poolOverlap(slate, pool), 0.8);

assert.equal(effectiveStatus("Q", "O"), "O", "the live status wins");
assert.equal(effectiveStatus("Q", ""), "", "live 'no tag' clears the file's Q");
assert.equal(effectiveStatus("Q", null), "Q", "never checked: the file stands");
assert.equal(effectiveStatus("OUT", "O"), "O"); assert.equal(canonicalStatus("OUT"), canonicalStatus("O"), "OUT and O are one status");
assert.equal(canonicalStatus("Doubtful"), "D"); assert.equal(canonicalStatus(" q "), "Q");
const now = Date.parse("2026-09-26T20:00:00Z");
assert.equal(openForStatus("2026-09-26T23:00:00Z", now), true, "later game: may still change");
assert.equal(openForStatus("2026-09-26T19:00:00Z", now), false, "game started: frozen");

console.log("CFB live status: classic lobby only, home/away teams, unique-only matching, live status wins when known.");
