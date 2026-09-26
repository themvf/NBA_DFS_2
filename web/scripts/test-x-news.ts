/** X news (CFB and NFL): phrase flags on the real Navy posts, query shape, merge order. */
import assert from "node:assert/strict";
import { flagPost, mergePosts, parseXPosts, surnameOf, teamQueries, teamsForPost, trustedQueries } from "../src/lib/x-news";

assert.deepEqual(flagPost("Update: Navy back-up QB Jackson Gutierrez is expected to make his 1st career start tonight at UAB"), ["starter"]);
assert.deepEqual(flagPost("QB Braxton Woodson (ankle) is downgraded to doubtful Friday."), ["doubtful/GTD", "injury"]);
assert.ok(flagPost("Starting quarterback Braxton Woodson, who is recovering from an ankle injury, isn't expected to play").includes("out"), "isn't expected to play is out");
assert.ok(flagPost("Woodson isn't expected to play; not expected to play tonight").includes("out"));
assert.deepEqual(flagPost("Britain was fundamental for the defeat of Nazi Germany. Without the Royal Navy there were no supplies"), []);

const q = teamQueries("Navy", ["Braxton Woodson", "Jackson Gutierrez"], new Date("2026-09-25T23:00:00Z"));
assert.equal(q.length, 2);
assert.ok(q[0].startsWith('("Braxton Woodson" OR "Jackson Gutierrez") since:2026-09-22_23:00:00_UTC until:2026-09-25_23:00:00_UTC'));
assert.ok(q[1].startsWith('"Navy" (QB OR quarterback)'));
assert.equal(teamQueries("UAB", [], new Date()).length, 1, "no named players: school search only");

const posts = parseXPosts({ tweets: [
  { id: "1", createdAt: "Fri Sep 25 21:24:08 +0000 2026", text: "Gutierrez expected to start", author: { userName: "Brett_McMurphy", followers: 331347 }, url: "https://x.com/a/1" },
  { id: "2", createdAt: "Fri Sep 25 22:19:51 +0000 2026", text: "Woodson is dressed and warming up", author: { userName: "SteveIrvine04", followers: 9009 } },
  { id: "", createdAt: "Fri Sep 25 22:19:51 +0000 2026", text: "no id" },
] });
assert.equal(posts.length, 2, "posts without an id are dropped");
assert.equal(posts[0].at, "2026-09-25T21:24:08.000Z");
const merged = mergePosts([posts, posts]);
assert.deepEqual(merged.map((p) => p.id), ["2", "1"], "deduplicated, newest first");
// Sentence scoping: another team's injury in a multi-game preview does not flag this team.
const preview = "Oregon at USC tonight. Texas RB Hollywood Smothers is doubtful. Maiava has been sharp.";
assert.deepEqual(flagPost(preview, ["Maiava", "USC"]), [], "doubtful is about Smothers, not USC");
assert.deepEqual(flagPost("Sources: Navy is expected to start Gutierrez at UAB tonight.", ["Gutierrez", "Navy"]), ["starter"]);
assert.equal(surnameOf("Michael Hawkins Jr."), "Hawkins");
assert.equal(surnameOf("Jordan McCord III"), "McCord");
const scoped = parseXPosts({ tweets: [{ id: "9", createdAt: "Fri Sep 25 21:29:53 +0000 2026",
  text: "Navy is expected to start back-up quarterback Jackson Gutierrez. Braxton Woodson isn't expected to play.", author: { userName: "PeteThamel", followers: 410742 } }] },
  ["Braxton Woodson", "Jackson Gutierrez"], "Navy");
assert.deepEqual(scoped[0].mentions, ["Braxton Woodson", "Jackson Gutierrez"]);
assert.deepEqual(scoped[0].flags, ["starter", "out"]);
// Trusted accounts: grouped queries, routing to named teams, ranking ahead of others.
const tq = trustedQueries(["PeteThamel", "Brett_McMurphy"], ["Navy", "UAB", "LSU", "USC", "Oregon", "Alabama", "Missouri", "Kansas State", "Cincinnati"], new Date("2026-09-25T23:00:00Z"));
assert.equal(tq.length, 2, "9 teams in groups of 8");
assert.ok(tq[0].startsWith('(from:PeteThamel OR from:Brett_McMurphy) ("Navy" OR "UAB"'));
const reqs = [{ code: "NAVY", school: "Navy", players: ["Jackson Gutierrez"] }, { code: "UAB", school: "UAB", players: ["Ryder Burton"] }];
assert.deepEqual(teamsForPost("Sources: Navy is expected to start Gutierrez at UAB tonight.", reqs).map((r) => r.code), ["NAVY", "UAB"]);
assert.deepEqual(teamsForPost("Jackson Gutierrez gets the nod.", reqs).map((r) => r.code), ["NAVY"], "a full player name routes the post");
assert.deepEqual(teamsForPost("Gutierrez gets the nod.", reqs), [], "a surname alone does not");
assert.deepEqual(teamsForPost("Arizona - C Gabriel Moreno (hamstring) is probable today versus San Diego.",
  [{ code: "ARIZ", school: "Arizona", players: ["Noah Fifita"] }]), [], "a baseball injury note is not the football team");
const fan = { ...posts[1], id: "fan", at: "2026-09-25T22:50:00.000Z", flags: ["starter"], trusted: false };
const insider = { ...posts[0], trusted: true };
assert.deepEqual(mergePosts([[fan, { ...insider, flags: ["starter"] }]], 1).map((p) => p.id), ["1"], "a flagged trusted post beats a flagged fan post");
assert.deepEqual(mergePosts([[fan, { ...insider, flags: [] }]], 1).map((p) => p.id), ["fan"], "flagged news beats an unflagged trusted post");
console.log("X news: flags, queries, trusted routing and merge ok.");
