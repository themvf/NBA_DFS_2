/**
 * Rolling projections explorer (/nfl/projections): filtering, counts, sorting,
 * stat lines, the defense's implied total, and URL state. Pure; no database.
 */
import assert from "node:assert/strict";
import {
  classicSalaryIndex, environmentByTeam, filterPlayers, pointsPerThousand, salaryKey, filterSearch, hiddenCounts, parseFilters, positionCounts, rangeMax,
  relevantImplied, sortPlayers, STAT_KEYS, statLine, type ExplorerState, type RollingGame, type RollingPlayer, type StatMeans,
} from "../src/lib/nfl-dfs/rolling-projection-view";

const noStats = Object.fromEntries(STAT_KEYS.map(key => [key, null])) as StatMeans;
let nextId = 1;
function player(patch: Partial<RollingPlayer> & Pick<RollingPlayer, "name" | "position" | "team">): RollingPlayer {
  return {
    id: nextId++, opponent: "OPP", status: "historical", projected: 10, floor: 4, median: 9, ceiling: 20,
    boomRate: 0.1, historyGames: 30, salary: null, kickoff: "2026-11-01T17:00:00.000Z",
    stats: noStats, teamContext: null, ...patch,
  };
}

const defaults: ExplorerState = {
  position: "ALL", team: null, query: "", includeOut: false, includeEstimates: false, sort: "projected", view: "cards",
};

const allen = player({ name: "Josh Allen", position: "QB", team: "BUF", projected: 24.1, ceiling: 41 });
const backup = player({ name: "Mitchell Trubisky", position: "QB", team: "BUF", status: "position_prior", projected: 17.5, historyGames: 0 });
const cook = player({ name: "James Cook", position: "RB", team: "BUF", projected: 16.2 });
const injured = player({ name: "Keon Coleman", position: "WR", team: "BUF", status: "out", projected: 0 });
const kraft = player({ name: "Tucker Kraft", position: "TE", team: "GB", projected: 12.2, boomRate: 0.3 });
const bills = player({ name: "Bills", position: "DST", team: "BUF", projected: 7.5 });
const accents = player({ name: "Amon-Ra St. Brown", position: "WR", team: "DET", projected: 18 });
const all = [allen, backup, cook, injured, kraft, bills, accents];

// --- Default view hides ruled-out players and position estimates, and says how many ---
assert.deepEqual(filterPlayers(all, defaults).map(p => p.name).sort(),
  ["Amon-Ra St. Brown", "Bills", "James Cook", "Josh Allen", "Tucker Kraft"]);
assert.deepEqual(hiddenCounts(all, defaults), { out: 1, estimates: 1 });
assert.ok(filterPlayers(all, { ...defaults, includeEstimates: true }).includes(backup), "toggle brings the estimate back");
assert.ok(filterPlayers(all, { ...defaults, includeOut: true }).includes(injured), "toggle brings the ruled-out player back");

// --- Position filters, FLEX, and counts that ignore the position filter itself ---
assert.deepEqual(filterPlayers(all, { ...defaults, position: "FLEX" }).map(p => p.position).sort(), ["RB", "TE", "WR"]);
const counts = positionCounts(all, { ...defaults, position: "QB" });
assert.equal(counts.ALL, 5, "counts describe every tab, not just the selected one");
assert.equal(counts.QB, 1, "the position-estimate backup is not counted while hidden");
assert.equal(counts.FLEX, 3);
assert.equal(counts.K, 0);
assert.equal(positionCounts(all, { ...defaults, includeEstimates: true }).QB, 2);
assert.deepEqual(hiddenCounts(all, { ...defaults, position: "WR" }), { out: 1, estimates: 0 }, "hidden counts follow the position tab");

// --- Team and search ---
assert.deepEqual(filterPlayers(all, { ...defaults, team: "GB" }).map(p => p.name), ["Tucker Kraft"]);
assert.deepEqual(filterPlayers(all, { ...defaults, query: "st. brown" }).map(p => p.name), ["Amon-Ra St. Brown"]);
assert.deepEqual(filterPlayers(all, { ...defaults, query: "det" }).map(p => p.name), ["Amon-Ra St. Brown"], "a team code searches the team");
const accented = player({ name: "Zoë Müller", position: "K", team: "GB" });
assert.deepEqual(filterPlayers([accented], { ...defaults, query: "zoe muller" }).map(p => p.name), ["Zoë Müller"], "accents are ignored");

// --- Sorting: descending by value, missing values last, names break ties ---
const missing = player({ name: "Aaron Missing", position: "WR", team: "DET", boomRate: null });
const tie = player({ name: "Aaron Tie", position: "TE", team: "GB", projected: 12.2, boomRate: 0.3 });
assert.deepEqual(sortPlayers([cook, allen, kraft], "projected").map(p => p.name), ["Josh Allen", "James Cook", "Tucker Kraft"]);
assert.deepEqual(sortPlayers([missing, kraft, tie], "boom").map(p => p.name), ["Aaron Tie", "Tucker Kraft", "Aaron Missing"]);
assert.deepEqual(sortPlayers([kraft, allen], "name").map(p => p.name), ["Josh Allen", "Tucker Kraft"]);

// --- One shared bar scale, rounded up to a clean tick ---
assert.equal(rangeMax([allen, cook]), 45);
assert.equal(rangeMax([]), 10);

// --- Stat lines: a stat the run did not store is left out, never shown as zero ---
const receiver = player({ name: "WR", position: "WR", team: "DET",
  stats: { ...noStats, receptions: 6.2, receiving_yards: 78.4, receiving_tds: 0.45, rushing_tds: 0.05 } });
assert.deepEqual(statLine(receiver), [
  { label: "rec", value: 6.2, digits: 1 },
  { label: "rec yds", value: 78.4, digits: 0 },
  { label: "TD", value: 0.5, digits: 1 },
]);
const kicker = player({ name: "K", position: "K", team: "GB",
  stats: { ...noStats, fg_made: 1.8, fg_att: 2.1, fg_made_50_59: 0.3, fg_made_60_: 0.02, pat_made: 2.4 } });
assert.equal(statLine(kicker).find(item => item.label === "50+")?.value.toFixed(2), "0.32");
assert.deepEqual(statLine(bills), [], "defenses have no stat line");
assert.deepEqual(statLine(player({ name: "QB", position: "QB", team: "BUF" })), []);

// --- A defense's relevant total is the opponent's implied points ---
const game: RollingGame = { gameId: "g1", kickoff: "2026-11-01T17:00:00.000Z", home: "BUF", away: "MIA",
  homeImplied: 27.5, awayImplied: 20, homeSpread: -7.5, total: 47.5 };
const environments = environmentByTeam([game]);
assert.deepEqual(environments.get("MIA"), { implied: 20, opponentImplied: 27.5, spread: 7.5, total: 47.5 });
assert.deepEqual(relevantImplied("DST", environments.get("BUF")), { value: 20, label: "Opp implied" });
assert.deepEqual(relevantImplied("WR", environments.get("BUF")), { value: 27.5, label: "Team implied" });
assert.deepEqual(relevantImplied("WR", undefined), { value: null, label: "Team implied" });

// --- URL state round-trips, defaults stay off the URL, and unknown values fall back ---
const state: ExplorerState = { ...defaults, position: "FLEX", team: "BUF", query: " cook ", sort: "ceiling",
  view: "table", includeOut: true, includeEstimates: true };
const search = filterSearch(state, "2026-8");
assert.equal(search, "?week=2026-8&position=FLEX&team=BUF&q=cook&sort=ceiling&view=table&out=1&est=1");
assert.deepEqual(parseFilters(Object.fromEntries(new URLSearchParams(search)), ["BUF"]), { ...state, query: "cook" });
assert.equal(filterSearch(defaults, null), "");
assert.deepEqual(parseFilters({ position: "LB", team: "XXX", sort: "ownership", view: "grid" }, ["BUF"]), defaults);

// --- Classic salaries: one per player, conflicts dropped, defenses paired by team ---
const index = classicSalaryIndex([
  { name: "Amon-Ra St. Brown", team: "DET", position: "WR", salary: 8100 },
  { name: "Amon-Ra St. Brown", team: "DET", position: "WR", salary: 8100 },
  { name: "Marvin Harrison Jr.", team: "AZ", position: "WR", salary: 6200 },
  { name: "Josh Allen", team: "BUF", position: "QB", salary: 7700 },
  { name: "Josh Allen", team: "BUF", position: "QB", salary: 7500 },
  { name: "Bills", team: "BUF", position: "DST", salary: 3100 },
]);
assert.equal(index.get(salaryKey("Amon-Ra St. Brown", "DET", "WR")), 8100, "the same salary in two groups is one salary");
assert.equal(index.get(salaryKey("Marvin Harrison", "ARI", "WR")), 6200, "suffixes and team aliases match the slate identity rule");
assert.equal(index.has(salaryKey("Josh Allen", "BUF", "QB")), false, "two salaries for one key are dropped, not guessed");
assert.equal(index.get(salaryKey("Buffalo Bills", "BUF", "DST")), 3100, "a defense matches by team, not by name");
assert.equal(index.has(salaryKey("Buffalo Bills", "BUF", "WR")), false);

assert.equal(pointsPerThousand({ projected: 24.5, salary: 7000 }), 3.5);
assert.equal(pointsPerThousand({ projected: 24.5, salary: null }), null);
assert.equal(pointsPerThousand({ projected: null, salary: 7000 }), null);
const cheap = player({ name: "Cheap", position: "WR", team: "DET", projected: 12, salary: 3000 });
const pricey = player({ name: "Pricey", position: "WR", team: "DET", projected: 20, salary: 8000 });
const unpriced = player({ name: "Unpriced", position: "K", team: "DET", projected: 9 });
assert.deepEqual(sortPlayers([cheap, unpriced, pricey], "salary").map(p => p.name), ["Pricey", "Cheap", "Unpriced"]);
assert.deepEqual(sortPlayers([pricey, unpriced, cheap], "value").map(p => p.name), ["Cheap", "Pricey", "Unpriced"]);

console.log("nfl rolling projection view: all assertions passed");
