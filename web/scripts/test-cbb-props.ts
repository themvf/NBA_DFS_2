import assert from "node:assert/strict";
import { cbbPropFreshness, normalizeCbbProps, parseCbbPropEvents, validCbbEventId } from "../src/lib/cbb-props/model";

const id = "a".repeat(32), updated = "2026-11-01T12:00:00Z", captured = "2026-11-01T12:01:00Z";
const game = { id, sport_key: "basketball_ncaab", home_team: "Home College", away_team: "Away University", commence_time: "2026-11-01T20:00:00Z" };
const event = parseCbbPropEvents([game])[0];
const over = (player: string, point: number, price = -110) => ({ name: "Over", description: player, point, price });
const under = (player: string, point: number, price = -110) => ({ ...over(player, point, price), name: "Under" });
const market = (key: string, outcomes: unknown[], last_update: string | null = updated) => ({ key, outcomes, last_update });
const book = (key: string, markets: unknown[]) => ({ key, title: key === "draftkings" ? "DraftKings" : key, markets });
const payload = (bookmakers: unknown[]) => ({ ...game, bookmakers });
const board = normalizeCbbProps(payload([
  book("draftkings", [market("player_points", [over("Player A", 19.5, -115), under("Player A", 19.5, -105)]), market("player_rebounds", [over("Player A", 7.5), under("Player A", 7.5)])]),
  book("fanduel", [market("player_points", [over("Player A", 20.5, 100), under("Player A", 20.5, -120)]), market("player_assists", [over("Player B", 4.5), under("Player B", 4.5)])]),
]), event, captured);
assert.equal(board.lines.length, 4);
const dkPoints = board.lines.find(row => row.book === "draftkings" && row.market === "player_points")!;
assert.equal(dkPoints.player, "Player A"); assert.equal(dkPoints.line, 19.5); assert.equal(dkPoints.over, -115); assert.equal(dkPoints.under, -105);
assert.equal(board.capturedAt, captured); assert.equal(dkPoints.updatedAt, "2026-11-01T12:00:00.000Z");
assert.equal(board.lines.find(row => row.book === "fanduel" && row.market === "player_points")?.line, 20.5, "do not merge books or lines");
assert.equal(board.skipped, 0);

const mismatch = normalizeCbbProps(payload([book("draftkings", [market("player_points", [over("A", 10.5), under("A", 11.5)])])]), event, captured);
assert.equal(mismatch.lines.length, 2, "different thresholds never form an over/under pair");
assert.equal(mismatch.lines.find(row => row.line === 10.5)?.under, null);
assert.equal(mismatch.lines.find(row => row.line === 11.5)?.over, null);
const sameName = normalizeCbbProps(payload([book("draftkings", [market("player_points", [over("A Jr.", 10.5), under("A", 10.5)])])]), event, captured);
assert.equal(sameName.lines.length, 2, "do not strip suffixes and silently merge identities");
const duplicates = normalizeCbbProps(payload([book("draftkings", [market("player_points", [over("A", 10.5), over("A", 10.5), under("A", 10.5)])])]), event, captured);
assert.equal(duplicates.lines.length, 1);
const conflicts = normalizeCbbProps(payload([book("draftkings", [market("player_points", [over("A", 10.5, -110), over("A", 10.5, -120), under("A", 10.5)])])]), event, captured);
assert.equal(conflicts.lines.length, 0); assert.ok(conflicts.skipped > 0);
const malformed = normalizeCbbProps(payload([book("draftkings", [market("player_points", [{ name: "Over", point: 5, price: -110 }, over("A", NaN), over("A", 10.5, 1.9)]), market("player_threes", [over("A", 2.5)])])]), event, captured);
assert.equal(malformed.lines.length, 0); assert.equal(malformed.skipped, 3);
assert.equal(normalizeCbbProps(payload([]), event, captured).lines.length, 0, "unavailable is empty, never fabricated zero lines");
assert.throws(() => normalizeCbbProps({ ...payload([]), id: "b".repeat(32) }, event, captured), /do not match/);
assert.throws(() => normalizeCbbProps({ ...payload([]), home_team: "Other College" }, event, captured), /do not match/);
assert.throws(() => normalizeCbbProps({ ...payload([]), sport_key: "basketball_nba" }, event, captured), /invalid game/);
assert.throws(() => normalizeCbbProps({ ...game }, event, captured), /invalid response/);
assert.throws(() => parseCbbPropEvents({}), /unavailable/);
assert.throws(() => parseCbbPropEvents([game, game]), /conflicting game/);
assert.equal(validCbbEventId("../secret"), false); assert.equal(validCbbEventId(id), true);
const unknownTime = normalizeCbbProps(payload([book("draftkings", [market("player_points", [over("A", 10.5)], null)])]), event, captured);
assert.equal(unknownTime.lines[0].updatedAt, null, "capture time cannot fill a missing source timestamp");
assert.equal(cbbPropFreshness(updated, Date.parse(captured)), "recent");
assert.equal(cbbPropFreshness(updated, Date.parse("2026-11-01T12:16:00Z")), "stale");
assert.equal(cbbPropFreshness(null, Date.parse(captured)), "unknown");
assert.equal(cbbPropFreshness("2026-11-02T12:00:00Z", Date.parse(captured)), "unknown");
console.log("CBB player props: book/player/stat/threshold pairing, missing sides, conflicts, event/sport identity, source timestamps, freshness, and missing coverage passed.");
