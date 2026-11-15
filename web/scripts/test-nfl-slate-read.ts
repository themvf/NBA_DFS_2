import assert from "node:assert/strict";
import { buildClassicSlateRead, buildShowdownSlateRead, totalBand, type GameLine, type SlateReadPlayer } from "../src/lib/nfl-dfs/slate-read";

const p = (name: string, position: string, team: string, gameKey: string, ownPct: number | null, extra: Partial<SlateReadPlayer> = {}): SlateReadPlayer =>
  ({ name, position, team, gameKey, salary: 5000, ourProj: 10, ownPct, ...extra });
assert.equal(totalBand(42.5), "under 43"); assert.equal(totalBand(43), "43-47.5"); assert.equal(totalBand(48), "48+");

const lines: GameLine[] = [
  { home: "SF", away: "DAL", total: 51.5, homeSpread: -3, capturedAt: "2026-11-15T14:00:00.000Z" },
  { home: "IND", away: "MIA", total: 38, homeSpread: -6.5, capturedAt: "2026-11-15T13:00:00.000Z" },
  { home: "WSH", away: "LA", total: 45, homeSpread: 2.5, capturedAt: null }, // schedule codes; the slate says WAS and LAR
];
const players = [
  p("Purdy", "QB", "SF", "DAL@SF", 12), p("Dak", "QB", "DAL", "DAL@SF", 8), p("Lamb", "WR", "DAL", "DAL@SF", 30),
  p("Jones", "QB", "IND", "MIA@IND", 2), p("Taylor", "RB", "IND", "MIA@IND", 18),
  p("Stafford", "QB", "LAR", "LAR@WAS", 3), p("Nacua", "WR", "LAR", "LAR@WAS", 27), p("Hurt", "WR", "LAR", "LAR@WAS", 40, { isOut: true }),
];
const read = buildClassicSlateRead(players, lines);
assert.deepEqual(read.games.map((g) => g.key), ["DAL@SF", "LAR@WAS", "MIA@IND"], "highest total first");
const [top, mid, low] = read.games;
assert.equal(top.favorite, "SF"); assert.equal(top.band, "48+");
assert.equal(mid.total, 45, "LA/WSH on the schedule match LAR/WAS on the slate"); assert.equal(mid.favorite, "LAR");
assert.equal(low.band, "under 43");
assert.equal(Math.round(top.fieldSharePct!), 50, "50 of 100 ownership points; the player who is out is not counted");
assert.equal(Math.round(top.fieldQbPct!), 80, "20 of 25 quarterback ownership points");
assert.deepEqual(top.chalk.map((c) => c.name), ["Lamb", "Purdy", "Dak"]);
assert.match(read.statements[0], /Highest total: DAL at SF, 51\.5\. We estimate 80% of the field's quarterbacks and 50% of all its roster spots come from this game; an even split over 3 games would be 33%\./);
assert.ok(read.statements.some((s) => /Totals of 48 or more: DAL@SF\..*1\.04 per \$1.*1\.22/.test(s)));
assert.ok(read.statements.some((s) => /Totals under 43: MIA@IND\..*0\.52 per \$1.*8%/.test(s)));
assert.equal(read.linesAsOf, "2026-11-15T14:00:00.000Z");

// No ownership and no lines: it says so and invents nothing.
const bare = buildClassicSlateRead(players.map((x) => ({ ...x, ownPct: null })), []);
assert.equal(bare.ownershipKnown, false);
assert.equal(bare.games[0].fieldSharePct, null);
assert.match(bare.statements[0], /No game totals were found/);

// Showdown
const sd = buildShowdownSlateRead([
  p("Burrow", "QB", "CIN", "PIT@CIN", 70, { captainOwnPct: 22 }), p("Chase", "WR", "CIN", "PIT@CIN", 75, { captainOwnPct: 30 }),
  p("Rodgers", "QB", "PIT", "PIT@CIN", 55, { captainOwnPct: 9 }), p("Metcalf", "WR", "PIT", "PIT@CIN", 40, { captainOwnPct: 8 }),
], [{ home: "CIN", away: "PIT", total: 44.5, homeSpread: -3, capturedAt: null }]);
assert.match(sd.statements[0], /Line: total 44\.5, CIN favored by 3\./);
assert.match(sd.statements[1], /Field captains \(estimated\): Chase 30%, Burrow 22%/);
assert.ok(sd.statements.some((s) => /five players from the underdog \(PIT today\): 0\.77 per \$1/.test(s)));
assert.equal(sd.teamSharePct[0].team, "CIN");
console.log("Slate read: all assertions passed.");
