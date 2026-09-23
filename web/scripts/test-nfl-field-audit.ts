/**
 * Field audit, browser side — and the pin that keeps it honest.
 *
 * This logic exists twice: here and in `model/nfl_dfs_field_audit.py`. The
 * duplication is deliberate (a 64 MB export has to be parsed in the browser,
 * and a weekly terminal command is a chore nobody runs), but it is a real
 * maintenance hazard. The last block reads the Python constants off disk and
 * asserts they match, so a one-sided threshold edit fails loudly here rather
 * than silently producing two different answers to the same question.
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import {
  auditSlate, parseContestExport, normalizeName,
  IGNORED_BY_FIELD_PCT, NO_PRODUCTION_FPTS, ROSTERABLE_PROJECTION_SHARE,
  POSITION_DEPTH, DESCRIPTIVE_ONLY_BELOW, FIELD_AUDIT_VERSION,
  type AuditSlatePlayer,
} from "../src/lib/nfl-dfs/field-audit";

const HEADER = "Rank,EntryId,EntryName,TimeRemaining,Points,Lineup,,Player,Roster Position,%Drafted,FPTS";

function main() {
  // --- Parsing --------------------------------------------------------------
  const classic = parseContestExport([HEADER,
    `1,id,name,0,243.26,"DST Panthers  QB Dak Prescott",,Bijan Robinson,RB,37.91%,11.1`,
    `2,id,name,0,236.96,"QB Brock Purdy",,Derrick Henry,RB,32.44%,17.7`,
    `3,id,name,0,232.66,"QB Josh Allen"`,
  ].join("\n"));
  assert.equal(classic.entryCount, 3, "narrow rows past the ownership table are still entries");
  assert.equal(classic.players.length, 2);
  assert.equal(classic.winningScore, 243.26);
  assert.equal(classic.medianScore, 236.96);
  assert.equal(classic.format, "classic");

  // A lineup cell contains commas; the parser must honour the quoting.
  assert.equal(classic.players.find((p) => p.name === "Bijan Robinson")!.draftedPct, 37.91);

  // --- The multiplier trap --------------------------------------------------
  // CPT FPTS is 1.5x FLEX FPTS for the same player. Reading whichever row came
  // last inflated every captain by 50% and corrupted a whole analysis before
  // it was caught. Adams scored 42.5; his CPT row says 63.8.
  const showdown = parseContestExport([HEADER,
    `1,id,n,0,149.33,"CPT Davante Adams",,Davante Adams,FLEX,45.59%,42.5`,
    `2,id,n,0,140.00,"CPT Matthew Stafford",,Davante Adams,CPT,15.68%,63.8`,
  ].join("\n"));
  const adams = showdown.players.find((p) => p.normalizedName === normalizeName("Davante Adams"))!;
  assert.equal(adams.fpts, 42.5, "base comes from the FLEX row");
  assert.equal(adams.draftedPct, 61.27, "ownership is the total across slots");
  assert.equal(showdown.format, "showdown");

  // Only ever captained: divide back out rather than guess.
  const cptOnly = parseContestExport([HEADER, `1,id,n,0,100,"x",,Only Captained,CPT,0.5%,30.0`].join("\n"));
  assert.equal(cptOnly.players[0].fpts, 20);

  // Garbage in is an error, not a silent empty audit.
  assert.throws(() => parseContestExport("just one line"), /empty/i);
  assert.throws(() => parseContestExport([HEADER, "no,entries,here"].join("\n")), /contest entries/i);

  // --- Verdicts -------------------------------------------------------------
  const wr = (i: number): AuditSlatePlayer => ({
    dkPlayerId: 1000 + i, name: `WR${String.fromCharCode(97 + Math.floor(i / 26))}${String.fromCharCode(97 + (i % 26))}`,
    position: "WR", salary: 8000 - 100 * i, ourProj: 20 - 0.4 * i, isOut: false,
  });
  const pool = Array.from({ length: 40 }, (_, i) => wr(i));
  const field = (over: Record<string, { draftedPct: number; fpts: number | null }> = {}) => {
    const m = new Map<string, { draftedPct: number; fpts: number | null }>();
    for (const p of pool) m.set(normalizeName(p.name), { draftedPct: 12, fpts: 11 });
    for (const [k, v] of Object.entries(over)) m.set(normalizeName(k), v);
    return m;
  };
  const target = pool[2].name;

  const blind = auditSlate(pool, field({ [target]: { draftedPct: 0.01, fpts: 0 } }));
  assert.equal(blind.summary.marketKnew, 1);
  assert.equal(blind.flagged[0].verdict, "MARKET_KNEW");
  assert.equal(blind.summary.projectedPointsOnMarketKnew, blind.flagged[0].ourProj);

  assert.equal(auditSlate(pool, field({ [target]: { draftedPct: 0.5, fpts: 20 } })).summary.realEdge, 1);
  assert.equal(auditSlate(pool, field({ [target]: { draftedPct: IGNORED_BY_FIELD_PCT, fpts: 0 } })).summary.flagged, 0,
    "at the threshold he is not ignored");

  // A player we ruled out is agreement, not a blind spot.
  const withOut = pool.map((p, i) => (i === 2 ? { ...p, isOut: true } : p));
  assert.equal(auditSlate(withOut, field({ [target]: { draftedPct: 0.01, fpts: 0 } })).summary.flagged, 0);

  // Small slate: an absolute depth cut alone flags $200 bodies projected at 0.3.
  const tiny: AuditSlatePlayer[] = [
    { dkPlayerId: 1, name: "Star", position: "WR", salary: 11400, ourProj: 25, isOut: false },
    { dkPlayerId: 2, name: "Scrub", position: "WR", salary: 200, ourProj: 0.3, isOut: false },
  ];
  const tinyField = new Map([
    [normalizeName("Star"), { draftedPct: 40, fpts: 30 }],
    [normalizeName("Scrub"), { draftedPct: 0.01, fpts: 0 }],
  ]);
  assert.equal(auditSlate(tiny, tinyField).summary.flagged, 0);
  assert.equal(auditSlate(tiny, tinyField, { projectionShare: 0 }).summary.marketKnew, 1, "...without the floor");

  // Absent from the field table is 0% owned, but with no outcome there is no verdict.
  const missing = field(); missing.delete(normalizeName(target));
  const gap = auditSlate(pool, missing);
  assert.equal(gap.summary.unmatchedToFieldTable, 1);
  assert.ok(gap.flagged.every((r) => r.name !== target));

  // --- The cross-language pin ----------------------------------------------
  const python = readFileSync(join(process.cwd(), "..", "model", "nfl_dfs_field_audit.py"), "utf8");
  const constant = (name: string) => {
    const match = python.match(new RegExp(`^${name}\\s*=\\s*([0-9.]+)`, "m"));
    assert.ok(match, `${name} not found in model/nfl_dfs_field_audit.py`);
    return Number(match![1]);
  };
  assert.equal(constant("IGNORED_BY_FIELD_PCT"), IGNORED_BY_FIELD_PCT);
  assert.equal(constant("NO_PRODUCTION_FPTS"), NO_PRODUCTION_FPTS);
  assert.equal(constant("ROSTERABLE_PROJECTION_SHARE"), ROSTERABLE_PROJECTION_SHARE);
  const depth = python.match(/^POSITION_DEPTH\s*=\s*\{([^}]*)\}/m);
  assert.ok(depth, "POSITION_DEPTH not found in the Python module");
  for (const [, pos, value] of depth![1].matchAll(/"(\w+)":\s*(\d+)/g)) {
    assert.equal(POSITION_DEPTH[pos], Number(value), `POSITION_DEPTH.${pos} differs across languages`);
  }
  assert.ok(python.includes(`VERSION = "${FIELD_AUDIT_VERSION}"`), "version string differs across languages");
  assert.ok(python.includes(`total < ${DESCRIPTIVE_ONLY_BELOW}`), "descriptive-only floor differs across languages");

  console.log("Field audit (browser):");
  console.log("  - the two side-by-side tables split by row width, quoted lineups intact");
  console.log("  - a showdown CPT row is 1.5x FLEX and is never read as the base");
  console.log("  - verdicts, the rosterable floor, and ruled-out exclusion match the CLI");
  console.log("  - thresholds are pinned against model/nfl_dfs_field_audit.py");
}

main();
