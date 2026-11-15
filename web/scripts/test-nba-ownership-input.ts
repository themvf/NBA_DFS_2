/**
 * nba-ownership-input-v1 + nba-gpp-scoring-v2 (2026-11-14).
 *
 * Pins: no leverage edge without a field projection; stat-api ownership wins
 * only when it covers the slate and the mode is not gpp_ls; the projected
 * field average; the relative lineup-ownership target; the stat-api parser's
 * profile choice; and that the optimizer still builds valid lineups on
 * stat-api-scale ownership.
 */
import {
  applyNbaOwnershipInput,
  computeNbaLeverage,
  describeNbaOwnershipInput,
  nbaFieldProjection,
  parseStatApiNbaOwnership,
  projectedFieldOwnershipSum,
  STATAPI_MIN_COVERAGE,
} from "../src/lib/nba-dfs/ownership-input";
import * as optimizerModule from "../src/app/dfs/optimizer";
import * as fixtureModule from "./nba-optimizer-fixtures";
import type { OptimizerPlayer, OptimizerSettings } from "../src/app/dfs/optimizer";

const { optimizeLineupsWithDebug, passesCheapLeverageGate, resolveLineupOwnershipTarget, NBA_FIELD_OWNERSHIP_MARGIN } = optimizerModule;
const { assertValidLineups, createSyntheticNbaPool } = fixtureModule;

let failures = 0;
function check(name: string, condition: unknown, detail = "") {
  if (condition) console.log(`PASS ${name}`);
  else { failures++; console.log(`FAIL ${name}${detail ? `: ${detail}` : ""}`); }
}
const close = (a: number | null | undefined, b: number, tol = 1e-6) => a != null && Math.abs(a - b) <= tol;

// ── computeNbaLeverage ───────────────────────────────────────────────
check("no field projection gives zero leverage", computeNbaLeverage(25, 10, null) === 0);
check("a DraftKings average of 0 counts as no field projection", computeNbaLeverage(25, 10, 0) === 0);
check(
  "field edge x (1-own)^0.7 matches the old formula when a field projection exists",
  close(computeNbaLeverage(30, 20, 25), Math.round(5 * Math.pow(0.8, 0.7) * 1000) / 1000),
);
check("negative edge stays negative", computeNbaLeverage(20, 10, 25) < 0);
check("ceiling bonus from steals and blocks", close(computeNbaLeverage(30, 0, 25, 2, 1), Math.round(5 * 1.14 * 1000) / 1000));

// ── projectedFieldOwnershipSum ───────────────────────────────────────
check("field sum is sum(own^2)/100", close(projectedFieldOwnershipSum([50, 30, 20, null, 0]), 38));

// ── applyNbaOwnershipInput ───────────────────────────────────────────
function player(id: number, overrides: Partial<OptimizerPlayer> = {}): OptimizerPlayer {
  return {
    id, dkPlayerId: 1000 + id, name: `P${id}`, teamAbbrev: "AAA", teamId: 1, matchupId: 1,
    eligiblePositions: "PG", salary: 6000, avgFptsDk: 25, ourProj: 30, ourLeverage: 99,
    linestarProj: null, projOwnPct: 12, projCeiling: 40, boomRate: 0.1, propPts: null,
    isOut: false, gameInfo: null, teamLogo: null, teamName: null, homeTeamId: 1,
    ...overrides,
  } as OptimizerPlayer;
}

{
  const pool = [
    player(1, { statapiOwnPct: 60 }),
    player(2, { statapiOwnPct: 30, avgFptsDk: null }),
    player(3, { statapiOwnPct: 5 }),
    player(4, { statapiOwnPct: 2 }),
    player(5, {}), // playable but not listed by stat-api
    player(6, { isOut: true, statapiOwnPct: 40 }),
  ];
  const { pool: out, summary } = applyNbaOwnershipInput(pool, { mode: "gpp", tournament: true });
  check("stat-api covering 80% of playable players is used", summary.source === "statapi", summary.source);
  check("stat-api ownership replaces the pool's", out[0].projOwnPct === 60);
  check("a playable player stat-api does not list reads 0%", out[4].projOwnPct === 0);
  check("an out player reads 0% and no leverage", out[5].projOwnPct === 0 && out[5].ourLeverage == null);
  check("stored leverage is recomputed, not kept", out[0].ourLeverage !== 99);
  check("no DK average means no leverage edge", out[1].ourLeverage === 0);
  const zeroAvg = applyNbaOwnershipInput([player(9, { avgFptsDk: 0, linestarProj: null, statapiOwnPct: 3 })], { mode: "gpp", tournament: true });
  check("a DK average of 0 gives no leverage edge", zeroAvg.pool[0].ourLeverage === 0);
  check(
    "field average is stamped from the ownership used",
    close(summary.fieldOwnershipSum, projectedFieldOwnershipSum([60, 30, 5, 2, 0]), 0.05)
      && out.every((p) => p.fieldOwnershipSum === summary.fieldOwnershipSum),
  );
  check("no warning when stat-api is used", summary.warning == null);
}

{
  const pool = [player(1, { statapiOwnPct: 50 }), player(2), player(3), player(4)];
  const { pool: out, summary } = applyNbaOwnershipInput(pool, { mode: "gpp", tournament: true });
  check(`stat-api under ${STATAPI_MIN_COVERAGE * 100}% coverage is not used`, summary.source === "internal" && out[0].projOwnPct === 12);
  check("thin stat-api coverage explains itself", (summary.warning ?? "").includes("internal estimate"));
  check("no field-average stamp on internal ownership", summary.fieldOwnershipSum == null);
}

{
  const pool = [
    player(1, { statapiOwnPct: 50, rawLsProjOwnPct: 30, linestarOwnPct: 30, projOwnPct: 31 }),
    player(2, { statapiOwnPct: 20, rawLsProjOwnPct: 10, linestarOwnPct: 10, projOwnPct: 11 }),
  ];
  const { pool: out, summary } = applyNbaOwnershipInput(pool, { mode: "gpp_ls", tournament: true });
  check("gpp_ls keeps LineStar ownership", summary.source === "linestar" && out[0].projOwnPct === 31);
  check("LineStar ownership is calibrated, so the field average is stamped", summary.fieldOwnershipSum != null);
}

{
  // The Python slate loader writes a baseline estimate into proj_own_pct when
  // LineStar is missing; with no linestar_own_pct it must read as internal.
  const pool = [player(1, { rawLsProjOwnPct: 12 }), player(2, { rawLsProjOwnPct: 9 })];
  const { summary } = applyNbaOwnershipInput(pool, { mode: "gpp", tournament: true });
  check("a baseline estimate in proj_own_pct is not taken for LineStar", summary.source === "internal" && summary.fieldOwnershipSum == null);
  check("so the flat-ownership warning still shows", (summary.warning ?? "").includes("internal estimate"));
}

{
  const { summary } = applyNbaOwnershipInput([player(1), player(2)], { mode: "gpp_ls", tournament: true });
  check("gpp_ls advice names LineStar only", (summary.warning ?? "").includes("Load LineStar") && !(summary.warning ?? "").includes("stat-api"));
  const note = describeNbaOwnershipInput(summary, { ok: false, message: "boom" }, "gpp_ls");
  check("gpp_ls gets no stat-api note", !(note ?? "").includes("stat-api:"));
}

{
  const { summary } = applyNbaOwnershipInput([player(1), player(2)], { mode: "cash", tournament: false });
  check("cash mode gets no ownership warning", summary.warning == null);
  const note = describeNbaOwnershipInput(summary, { ok: false, message: "STAT_API_KEY is not set" }, "cash");
  check("a failed stat-api pull is reported when stat-api was not used", (note ?? "").includes("STAT_API_KEY"));
}

{
  // Stored stat-api ownership and a failed refresh.
  const now = Date.parse("2026-11-14T23:00:00Z");
  const stored = (minutesAgo: number) => new Date(now - minutesAgo * 60_000).toISOString();
  const pool = (minutesAgo: number) => [1, 2, 3, 4, 5].map((id) => player(id, { statapiOwnPct: 10 * id, statapiOwnAt: stored(minutesAgo) }));
  const failed = { ok: false, message: "stat-api answered 503" };

  const fresh = applyNbaOwnershipInput(pool(30), { mode: "gpp", tournament: true, statApiPull: failed, now });
  const freshNote = describeNbaOwnershipInput(fresh.summary, failed, "gpp") ?? "";
  check("a failed refresh with recent stored values still uses stat-api", fresh.summary.source === "statapi" && fresh.summary.statapiAgeMinutes === 30);
  check("and the warning names the failure and the age", freshNote.includes("503") && freshNote.includes("30 minutes"));

  const stale = applyNbaOwnershipInput(pool(240), { mode: "gpp", tournament: true, statApiPull: failed, now });
  check("a failed refresh with values over the limit falls back", stale.summary.source === "internal");
  check("and says why", (stale.summary.warning ?? "").includes("240 minutes old"));

  const ok = applyNbaOwnershipInput(pool(240), { mode: "gpp", tournament: true, statApiPull: { ok: true, skipped: true, message: "reused" }, now });
  check("old values with a healthy refresh are used", ok.summary.source === "statapi");
}

{
  // The cheap-player leverage gate: no field projection means it does not apply.
  const noField = player(20, { salary: 3700, avgFptsDk: null, linestarProj: null, ourLeverage: 0 });
  const zeroAvg = player(21, { salary: 3700, avgFptsDk: 0, linestarProj: null, ourLeverage: 0 });
  const belowField = player(22, { salary: 3700, avgFptsDk: 18, ourLeverage: -1.5 });
  const aboveField = player(23, { salary: 3700, avgFptsDk: 12, ourLeverage: 4 });
  check("no field projection passes the leverage gate", passesCheapLeverageGate(noField, "gpp"));
  check("a DK average of 0 passes the leverage gate", passesCheapLeverageGate(zeroAvg, "gpp"));
  check("below the field still fails the gate", !passesCheapLeverageGate(belowField, "gpp"));
  check("above the field passes the gate", passesCheapLeverageGate(aboveField, "gpp"));
  check("the page and optimizer share one field-projection rule", nbaFieldProjection(0, 14) === 14 && nbaFieldProjection(null, null) == null && nbaFieldProjection(9, 14) === 9);
}

// ── resolveLineupOwnershipTarget ─────────────────────────────────────
check("no field average keeps the fixed GPP target", resolveLineupOwnershipTarget("gpp", null) === 125);
check("a real-scale field average moves the target above the field", resolveLineupOwnershipTarget("gpp", 215) === 215 + NBA_FIELD_OWNERSHIP_MARGIN);
check("a flat field average never lowers the fixed target", resolveLineupOwnershipTarget("gpp2", 80) === 112);

// ── parseStatApiNbaOwnership ─────────────────────────────────────────
{
  const parsed = parseStatApiNbaOwnership({
    projections: [
      { player: { operator_player_export_id: "44635823" }, ownership: [
        { contest_kind: "cash", size_tier: "10k-100k", multi_entry_per_user_band: "20+", projected_ownership: "0.90" },
        { contest_kind: "gpp", size_tier: "1k-10k", multi_entry_per_user_band: "20+", projected_ownership: "0.40" },
        { contest_kind: "gpp", size_tier: "10k-100k", multi_entry_per_user_band: "20+", projected_ownership: "0.4512" },
      ] },
      { player: { operator_player_export_id: "2" }, ownership: [{ contest_kind: "gpp", size_tier: "3-100", multi_entry_per_user_band: "1", projected_ownership: "0.05" }] },
      { player: { operator_player_export_id: "3" }, ownership: [{ contest_kind: "cash", projected_ownership: "0.5" }] },
      { player: { operator_player_export_id: null }, ownership: [] },
    ],
  });
  check("parser reads the large multi-entry GPP profile, in percent", parsed.get(44635823) === 45.1);
  check("parser falls back to another GPP profile", parsed.get(2) === 5);
  check("parser skips players with no GPP profile", !parsed.has(3) && parsed.size === 2);
  const nullShare = parseStatApiNbaOwnership({ projections: [{ player: { operator_player_export_id: "7" }, ownership: [
    { contest_kind: "gpp", size_tier: "10k-100k", multi_entry_per_user_band: "20+", projected_ownership: null },
    { contest_kind: "gpp", size_tier: "1k-10k", multi_entry_per_user_band: "20+", projected_ownership: "0.22" },
  ] }] });
  check("a missing share on the preferred profile falls back instead of reading 0%", nullShare.get(7) === 22);
}

// ── optimizer on stat-api-scale ownership ────────────────────────────
{
  const base = createSyntheticNbaPool({ gameCount: 6, valuePlayersPerTeam: 3 });
  // Real-scale ownership: concentrate on the best projections, like a real slate.
  const ranked = [...base].sort((a, b) => (b.ourProj ?? 0) - (a.ourProj ?? 0));
  const ownById = new Map(ranked.map((p, i) => [p.id, Math.max(0.5, 60 * Math.exp(-i / 8))]));
  const { pool, summary } = applyNbaOwnershipInput(
    base.map((p) => ({ ...p, statapiOwnPct: ownById.get(p.id) ?? 0 })),
    { mode: "gpp", tournament: true },
  );
  const settings: OptimizerSettings = { mode: "gpp", nLineups: 4, minStack: 2, teamStackCount: 1, maxExposure: 0.75 };
  const { lineups, debug } = optimizeLineupsWithDebug(pool, settings);
  check("optimizer builds lineups on stat-api-scale ownership", lineups.length === settings.nLineups, `${lineups.length} lineups, ${debug.terminationReason}`);
  try {
    assertValidLineups(lineups, pool, settings, { salaryFloor: debug.effectiveSettings.salaryFloor ?? 49_000 });
    check("those lineups are valid", true);
  } catch (error) {
    check("those lineups are valid", false, String(error));
  }
  check("summary records stat-api as the source", summary.source === "statapi" && (summary.fieldOwnershipSum ?? 0) > 100);
}

if (failures) {
  console.log(`${failures} check(s) failed`);
  process.exit(1);
}
console.log("PASS nba ownership input");
