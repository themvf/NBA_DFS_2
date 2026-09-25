/**
 * CFB DraftKings entries export: fills the Edit Entries template in place,
 * preserves everything else, and refuses files that are not CFB Classic.
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { parseCfbSalaryCsv, splitCsvLine } from "../src/lib/cfb-dfs/salary-csv";
import { DEFAULT_CFB_SETTINGS, optimizeCfbLineups, type CfbPoolPlayer } from "../src/lib/cfb-dfs/optimizer";
import { exportCfbDkEntries } from "../src/lib/cfb-dfs/entry-export";

const slate = parseCfbSalaryCsv(readFileSync(new URL("./fixtures/cfb-dk-salaries-2026-09-25.csv", import.meta.url), "utf8"));
const pool: CfbPoolPlayer[] = slate.players.filter((p) => !["OUT", "O", "D"].includes(p.status))
  .map((p) => ({ dkId: p.dkId, name: p.name, position: p.position, team: p.team, game: p.game, salary: p.salary, proj: p.dkAvg }));
const lineups = optimizeCfbLineups(pool, { ...DEFAULT_CFB_SETTINGS, nLineups: 2 }).lineups;
assert.equal(lineups.length, 2);

const template = [
  "Entry ID,Contest Name,Contest ID,Entry Fee,QB,RB,RB,WR,WR,WR,FLEX,S-FLEX,,Instructions",
  '4812000001,"CFB $5 Friday Night Blitz, 3-Game",191000001,$5,,,,,,,,,,1. Locate the player you want to select',
  '4812000002,"CFB $5 Friday Night Blitz, 3-Game",191000001,$5,,,,,,,,,,2. Copy the ID of your player',
  '4812000003,"CFB $5 Friday Night Blitz, 3-Game",191000001,$5,,,,,,,,,,3. Paste the ID into the roster position',
].join("\r\n");

const result = exportCfbDkEntries(template, lineups);
assert.deepEqual([result.filled, result.entries], [2, 3]);
const out = result.csv.trimEnd().split("\r\n").map(splitCsvLine);
assert.deepEqual(out[0], splitCsvLine(template.split("\r\n")[0]), "header untouched");
for (const [i, lineup] of lineups.entries()) {
  const row = out[i + 1];
  assert.equal(row[0], `481200000${i + 1}`, "entry order kept");
  assert.equal(row[1], "CFB $5 Friday Night Blitz, 3-Game", "the quoted contest name survives");
  assert.deepEqual(row.slice(4, 12), lineup.slots.map((s) => `${s.player.name} (${s.player.dkId})`), "slot columns, DK order");
  assert.equal(row[13], template.split("\r\n")[i + 1].split(",").pop(), "instructions column untouched");
}
assert.deepEqual(out[3].slice(4, 12), ["", "", "", "", "", "", "", ""], "an entry with no lineup is left empty");

// DraftKings has written the slot as SUPER FLEX too.
assert.equal(exportCfbDkEntries(template.replace("S-FLEX", "SUPER FLEX"), lineups).filled, 2);

// Refusals: an NFL template, too many lineups, a file with no header.
const nfl = "Entry ID,Contest Name,Contest ID,Entry Fee,QB,RB,RB,WR,WR,WR,TE,FLEX,DST\r\n1,NFL,2,$3,,,,,,,,,";
assert.throws(() => exportCfbDkEntries(nfl, lineups), /not a CFB Classic entries file/);
assert.throws(() => exportCfbDkEntries(template.split("\r\n").slice(0, 2).join("\r\n"), lineups), /only 1 entries/);
assert.throws(() => exportCfbDkEntries("Position,Name\r\nQB,A", lineups), /Entry ID header/);

console.log("CFB entry export: fills entries in order, keeps every other cell, refuses non-CFB and short files.");
