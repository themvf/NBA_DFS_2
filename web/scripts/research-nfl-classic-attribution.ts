/**
 * Classic attribution study (read-only). Three stages over the files written by
 * `research/nfl_classic_attribution_export.py`:
 *
 *   exclusions  Winners-shape pool exclusions as the PRODUCT computes them
 *               (`applyWinnersShape` on the whole frozen pool, thin-position
 *               skips and missing-data skips included) plus the rule
 *               configurations every solver shares -> `exclusions.json`.
 *               Week 5 is checked against the exclusions its Winners run
 *               actually recorded.
 *   (then)      `python -m research.nfl_classic_attribution_cbc <dir>` solves
 *               every configuration with CBC and records its certificate.
 *   report      The optimizer with a neutralized objective solves the same
 *               configurations and is compared with CBC; original portfolios,
 *               paired 20-lineup replays, the top-100 premise and forecasts are
 *               measured; `report.md` and `results.json` are written.
 *
 * Usage: node -r ./scripts/server-only-stub.cjs --import tsx scripts/research-nfl-classic-attribution.ts <exclusions|report> <dir>
 */
import { readdirSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { optimizeNflLineups, type NflOptimizerPlayer, type NflOptimizerSettings } from "../src/app/dfs/nfl/nfl-optimizer";
import { applyWinnersShape } from "../src/lib/nfl-dfs/winners-shape";

const [, , mode, dir] = process.argv;
if (!dir || !["exclusions", "report"].includes(mode)) {
  console.error("usage: research-nfl-classic-attribution.ts <exclusions|report> <dir>");
  process.exit(1);
}
// eslint-disable-next-line @typescript-eslint/no-explicit-any -- untyped JSON from the exporter
type Raw = Record<string, any>;
const slates: Raw[] = readdirSync(dir).filter((f) => /^\d{4}-w\d+-classic\.json$/.test(f))
  .sort((a, b) => Number(a.split("-w")[1].split("-")[0]) - Number(b.split("-w")[1].split("-")[0]))
  .map((f) => JSON.parse(readFileSync(join(dir, f), "utf8")));
const FLOOR = 49_500;
const r2 = (x: number | null | undefined) => (x == null || !Number.isFinite(x) ? null : Math.round(x * 100) / 100);
const norm = (s: string) => s.normalize("NFKD").toLowerCase().replace(/[^a-z0-9]/g, "").replace(/(jr|sr|ii|iii|iv|v)$/g, "");

type Rules = { rbPool: boolean; wrPool: boolean; tePool: boolean; dstPool: boolean; floor: boolean; stack: boolean;
  bringBack: boolean; noDstVsOffense: boolean; dkOnly: boolean; standardFloor: boolean };
const NONE: Rules = { rbPool: false, wrPool: false, tePool: false, dstPool: false, floor: false, stack: false, bringBack: false, noDstVsOffense: false, dkOnly: false, standardFloor: false };
const WINNERS_KEYS = ["rbPool", "wrPool", "tePool", "dstPool", "floor", "stack"] as const;
const NAMES: Record<string, string> = { rbPool: "RB pool rule", wrPool: "WR pool rule", tePool: "TE pool rule", dstPool: "DST pool rule", floor: "$49.5k floor", stack: "QB + WR/TE stack" };
const FULL: Rules = { ...NONE, rbPool: true, wrPool: true, tePool: true, dstPool: true, floor: true, stack: true };
function configs(): Array<{ key: string; label: string; group: string; rules: Rules }> {
  const out = [
    { key: "dk_only", label: "DraftKings rules only (CBC only)", group: "baseline", rules: { ...NONE, dkOnly: true } },
    { key: "unrestricted", label: "Unrestricted (DraftKings rules + product invariants)", group: "baseline", rules: { ...NONE } },
    { key: "standard", label: "Standard defaults (stack + bring-back, run's salary floor)", group: "baseline", rules: { ...NONE, stack: true, bringBack: true, standardFloor: true } },
  ];
  for (const k of WINNERS_KEYS) out.push({ key: `only_${k}`, label: `${NAMES[k]} only`, group: "single", rules: { ...NONE, [k]: true } });
  out.push({ key: "winners", label: "Full Winners shape (single-lineup rules)", group: "full", rules: { ...FULL } });
  for (const k of WINNERS_KEYS) out.push({ key: `winners_minus_${k}`, label: `Winners without ${NAMES[k]}`, group: "leave_one_out", rules: { ...FULL, [k]: false } });
  out.push({ key: "winners_strict_dst", label: "Diagnostic: Winners + no DST against own offense in this lineup", group: "diagnostic", rules: { ...FULL, noDstVsOffense: true } });
  out.push({ key: "winners_strict_bringback", label: "Diagnostic: Winners + bring-back required in this lineup", group: "diagnostic", rules: { ...FULL, bringBack: true } });
  return out;
}

function shapeExclusions(s: Raw) {
  const settings = { format: "classic", mode: "gpp", nLineups: 20, minSalary: 45_000, maxExposure: 0.6, minUnique: 2,
    stackPassCatchers: 1, bringBack: true, lockedPlayerIds: [], excludedPlayerIds: [], buildShape: "winners", projectionSource: "our",
    allowDkFallback: false, randomness: 0, minExposureByPlayer: {}, maxExposureByPlayer: {} } as unknown as NflOptimizerSettings;
  const shape = applyWinnersShape(settings, s.players.map((p: Raw) => ({ dkPlayerId: p.dkPlayerId, name: p.name, position: p.position,
    salary: p.salary, isOut: !!p.isOut, projectionStatus: p.projectionStatus ?? "historical", priorOpportunity: p.priorOpportunity ?? null })));
  const pos = new Map(s.players.map((p: Raw) => [p.dkPlayerId, p.position]));
  const byRule: Record<string, number[]> = { rbPool: [], wrPool: [], tePool: [], dstPool: [] };
  for (const id of shape.excluded.keys()) byRule[{ RB: "rbPool", WR: "wrPool", TE: "tePool", DST: "dstPool" }[pos.get(id) as string] as string].push(id);
  return { byRule, notes: shape.notes.filter((n) => /pool kept|not applied/.test(n)) };
}

if (mode === "exclusions") {
  const out: Raw = { configs: configs(), slates: {} };
  for (const s of slates) {
    const ex = shapeExclusions(s);
    const recorded: number[] = s.recordedWinnersExclusions?.excluded ?? [];
    const mine = new Set(Object.values(ex.byRule).flat());
    const fidelity = recorded.length ? { recordedRun: s.recordedWinnersExclusions.runId, recorded: recorded.length, computed: mine.size,
      both: recorded.filter((id) => mine.has(id)).length, onlyRecorded: recorded.filter((id) => !mine.has(id)).length,
      onlyComputed: [...mine].filter((id) => !recorded.includes(id)).length } : null;
    out.slates[s.week] = { ...ex, fidelity, priorSource: s.audit.priorOpportunity };
    console.log(`week ${s.week}: excluded RB ${ex.byRule.rbPool.length}, WR ${ex.byRule.wrPool.length}, TE ${ex.byRule.tePool.length}, DST ${ex.byRule.dstPool.length}` +
      (fidelity ? ` | vs recorded Winners run: ${fidelity.both} match, ${fidelity.onlyRecorded} only recorded, ${fidelity.onlyComputed} only computed` : ""));
    for (const n of ex.notes) console.log(`   ${n}`);
  }
  writeFileSync(join(dir, "exclusions.json"), JSON.stringify(out, null, 1));
  process.exit(0);
}

// ================================================================ report
const ex = JSON.parse(readFileSync(join(dir, "exclusions.json"), "utf8")) as Raw;
const cbc = JSON.parse(readFileSync(join(dir, "cbc.json"), "utf8")) as Record<string, Raw>;
const CONFIGS = ex.configs as Array<{ key: string; label: string; group: string; rules: Rules }>;
const report: string[] = [];
const say = (s = "") => report.push(s);
const results: Raw = { generatedAt: new Date().toISOString() };

function declared(s: Raw, punts: boolean): Raw[] {
  return s.players.filter((p: Raw) => p.actual != null && (p.eligibilityClass === "available" || (punts && p.eligibilityClass === "punt")));
}
function excludedFor(s: Raw, r: Rules): Set<number> {
  const by = ex.slates[s.week].byRule;
  return new Set([...(r.rbPool ? by.rbPool : []), ...(r.wrPool ? by.wrPool : []), ...(r.tePool ? by.tePool : []), ...(r.dstPool ? by.dstPool : [])]);
}
function floorFor(s: Raw, r: Rules) { return r.floor ? FLOOR : r.standardFloor ? (s.run.settings.minSalary ?? 0) : 0; }

/** Legality under one rule set, recomputed independently of either solver. */
function legal(lineup: Raw[], s: Raw, r: Rules): string[] {
  const bad: string[] = [];
  const count = (pos: string) => lineup.filter((p) => p.position === pos).length;
  if (lineup.length !== 9) bad.push("size");
  if (count("QB") !== 1 || count("DST") !== 1 || count("RB") < 2 || count("WR") < 3 || count("TE") < 1 || count("RB") + count("WR") + count("TE") !== 7) bad.push("positions");
  const sal = lineup.reduce((a, p) => a + p.salary, 0);
  if (sal > 50_000) bad.push("cap");
  if (sal < floorFor(s, r)) bad.push("floor");
  const games = new Map<string, number>(); for (const p of lineup) games.set(p.gameKey, (games.get(p.gameKey) ?? 0) + 1);
  if ([...games.values()].some((n) => n > 8)) bad.push("one game");
  const exc = excludedFor(s, r); if (lineup.some((p) => exc.has(p.dkPlayerId))) bad.push("pool rule");
  const qb = lineup.find((p) => p.position === "QB"), dst = lineup.find((p) => p.position === "DST");
  const opp = (p: Raw) => p.gameKey?.split("@").find((t: string) => t !== p.team);
  const dstOpp = dst ? opp(dst) : null;
  const facing = lineup.filter((p) => ["QB", "RB", "WR", "TE"].includes(p.position) && p.team === dstOpp).length;
  if (!r.dkOnly && facing > 3) bad.push("dst script");
  if (r.noDstVsOffense && facing > 0) bad.push("dst vs offense");
  if (r.stack && qb) {
    if (!lineup.some((p) => ["WR", "TE"].includes(p.position) && p.team === qb.team)) bad.push("stack");
    if (dst && dst.team === opp(qb)) bad.push("anti qb-dst");
  }
  if (r.bringBack && qb && !lineup.some((p) => ["RB", "WR", "TE"].includes(p.position) && p.team === opp(qb))) bad.push("bring-back");
  return bad;
}

function toOptimizer(players: Raw[], actualAsProjection: boolean): NflOptimizerPlayer[] {
  const shift = actualAsProjection ? 0.01 - Math.min(0, ...players.map((p) => p.actual ?? 0)) : 0;
  return players.map((p) => {
    const own = p.ownership && typeof p.ownership === "object" ? p.ownership : null;
    const b: Raw = { ...p, id: p.id ?? p.dkPlayerId, floorFpts: p.floor ?? null, ceilingFpts: p.ceiling ?? null, avgFptsDk: p.dkAvg ?? null,
      fantasyprosProj: p.fantasypros ?? null, linestarProj: p.linestar ?? null, linestarOwnPct: null, customProj: p.custom ?? null,
      ownPct: own?.total ?? (typeof p.ownership === "number" ? p.ownership : null), ownSource: own?.source ?? null,
      isOut: !!p.isOut, projectionStatus: p.projectionStatus ?? "historical", captainSalary: null };
    if (actualAsProjection) {
      for (const k of ["specialTeams", "calibrated", "workload", "positionWorkload", "defensiveForecast", "availability", "availabilityStatus",
        "availabilityState", "dkStatus", "situationEvidence", "absenceCommit", "marketAnchor"]) delete b[k];
      if (b.position === "K") b.depthRole = "Listed K1";
      Object.assign(b, { customProj: p.actual + shift, ourProj: p.actual + shift, floorFpts: p.actual + shift, ceilingFpts: p.actual + shift,
        boomRate: 0, isOut: false, projectionStatus: "historical", historyGames: 10, ownPct: null });
    }
    return b as NflOptimizerPlayer;
  });
}
function optimizerOracle(s: Raw, r: Rules, punts: boolean) {
  const pool = declared(s, punts);
  const settings = { format: "classic", mode: "gpp", projectionSource: "custom", allowDkFallback: false, nLineups: 1, minSalary: floorFor(s, r),
    minPlayerSalary: 0, requireObservedHistory: false, maxExposure: 1, minUnique: 1, stackPassCatchers: r.stack ? 1 : 0, bringBack: r.bringBack,
    randomness: 0, buildShape: "standard", archetypeMode: "standard", topProjectedCoverage: false, lockedPlayerIds: [], excludedPlayerIds: [...excludedFor(s, r)],
    minExposureByPlayer: {}, maxExposureByPlayer: {}, puntOverrides: [], ownershipCapability: "unavailable", ownershipLeverageEnabled: false,
    puntPolicy: { mode: "custom", absoluteMinSalary: 0, denylistedPlayerIds: [], allowlistedPlayerIds: [], minimumRoleConfidence: 0, minimumProjectedOpportunities: null,
      maxSalaryReliefPlayersPerLineup: 9, roleEvidenceRequiredBelowSalary: 0 },
    defensiveAdjustments: { mode: "off", profile: "gpp-integrated" }, situations: { enabled: false, teams: {} },
    ...(r.noDstVsOffense ? { maxDstVsOffenseShare: 0 } : {}) } as unknown as NflOptimizerSettings;
  const t0 = Date.now();
  try {
    const res = optimizeNflLineups(toOptimizer(pool, true), settings);
    const l = res.lineups[0];
    if (!l) return { points: null, ms: Date.now() - t0, error: res.warnings.find((w) => /Stopped|cannot/i.test(w)) ?? "no lineup" };
    const byId = new Map(pool.map((p) => [p.dkPlayerId, p]));
    const players = l.slots.map((x) => byId.get(x.player.dkPlayerId)!);
    return { points: r2(players.reduce((a, p) => a + p.actual, 0)), players, ms: Date.now() - t0, illegal: legal(players, s, r) };
  } catch (e) { return { points: null, ms: Date.now() - t0, error: (e as Error).message }; }
}

function rankOf(score: number, curve: number[][]): number | null {
  if (!curve?.length) return null;
  let above = -1;
  for (let i = 0; i < curve.length; i++) { if (curve[i][1] > score) above = i; else break; }
  if (above === -1) return 1;
  const [r0, s0] = curve[above];
  if (above === curve.length - 1) return Math.round(r0) + 1;
  const [r1, s1] = curve[above + 1];
  if (r1 - r0 === 1 || s0 === s1) return Math.round(r0) + 1;
  return Math.round(r0 + ((s0 - score) / (s0 - s1)) * (r1 - 1 - r0)) + 1;
}
function measures(points: number[], s: Raw) {
  const sorted = [...points].sort((a, b) => b - a);
  const curve = s.contest.curve, entries = s.contest.entries, median = s.contest.median;
  const ranks = sorted.map((p) => rankOf(p, curve));
  return { n: points.length, best: r2(sorted[0]), bestRank: ranks[0] ?? null, median: r2(sorted[Math.floor((sorted.length - 1) / 2)]),
    top1pct: ranks.filter((x) => x != null && x <= Math.ceil(entries * 0.01)).length, aboveFieldMedian: median == null ? null : sorted.filter((p) => p > median).length };
}

say("# Classic attribution study (read-only)");
say("");
say(`Generated ${results.generatedAt}. Inputs: \`research/nfl_classic_attribution_export.py\`; exclusions and report: \`web/scripts/research-nfl-classic-attribution.ts\`; independent oracle: \`research/nfl_classic_attribution_cbc.py\` (CBC via pulp). No setting, optimizer code or stored record was changed.`);
say("");
say("## 1. Coverage and input completeness");
say("");
say("| Week | Slate | Contest | Entries | Score curve | Top-100 entries | Build used (lineups) | Shape | Model | Optimizer then | Eligibility report | Eligible pool | Eligible with a recorded score | Prior-window plays labelled before the run | Pool-rule inputs |");
say("|---:|---|---|---:|---|---:|---|---|---|---|---|---:|---:|---|---|");
for (const s of slates) {
  const elig = s.players.filter((p: Raw) => p.eligibilityClass !== "availability");
  say(`| ${s.week} | ${s.games.length} games | ${s.contest.id} | ${s.contest.entries.toLocaleString()} | ${s.contest.curve.length ? "yes" : "no"} | ${s.contest.topEntries.length} | ${s.run.runId.slice(0, 8)} (${s.run.lineups}) | ${s.run.settings.buildShape ?? "standard"} | ${s.run.modelVersion} | ${s.run.optimizerVersion} | ${s.run.eligibilityReport ? "yes" : "no"} | ${elig.length} | ${elig.filter((p: Raw) => p.actual != null).length} | ${s.week === 1 ? "- (no prior weeks)" : `${s.audit.priorWindowPlaysLabelledBeforeRun[0].toLocaleString()} / ${s.audit.priorWindowPlaysLabelledBeforeRun[1].toLocaleString()}`} | ${s.week === 1 ? "none: RB/WR/TE rules skipped by the product" : s.audit.priorWindowPlaysLabelledBeforeRun[0] === 0 ? "**unavailable**: RB/WR/TE rules skipped" : s.audit.priorWindowPlaysLabelledBeforeRun[0] < s.audit.priorWindowPlaysLabelledBeforeRun[1] ? "**partial** reconstruction" : "complete reconstruction"} |`);
}
say("");
say("- **Build used** is the largest optimizer run created before the slate's first kickoff (latest on ties), i.e. the entered build rather than a test run.");
say("- **Prior opportunity** (the input to the RB/WR/TE pool rules) was not stored in any of these frozen pools. It is **reconstructed** with the workspace's own query: play-by-play weeks w-3..w-1 labelled no later than the run, team targets for the share. Plays relabelled after a run carry the later label time, so the gate drops them rather than use later information: weeks 2-3 have none left (the product would have skipped the three rules, and so does this study), week 4 keeps 90%. Week 1 has no prior weeks.");
const fw = Object.entries(ex.slates as Record<string, Raw>).find(([, v]) => v.fidelity);
if (fw) {
  const f = (fw[1] as Raw).fidelity;
  say(`- **Reconstruction check (week ${fw[0]}):** that week's Winners run recorded ${f.recorded} pool exclusions. Today's rules on the reconstructed inputs exclude ${f.computed}: ${f.both} in common, ${f.onlyRecorded} only in the run's record, ${f.onlyComputed} only in the reconstruction.`);
}
say(`- Players with no recorded score are excluded from every pool and counted (eligible minus scored, above); they are mostly depth players who did not play. Defence scores where DraftKings' contest number and the results table differ: ${slates.map((s) => s.audit.sourceDisagreements.map((d: Raw) => `${d.name} ${d.contest} vs ${d.results}`).join(", ")).filter(Boolean).join("; ") || "none"} (DraftKings' number is used).`);
say("");
say("### What the Winners pool rules exclude, as the product computes them");
say("");
say("| Week | RB excluded | WR excluded | TE excluded | DST excluded | Product notes |");
say("|---:|---:|---:|---:|---:|---|");
for (const s of slates) {
  const e = ex.slates[s.week];
  say(`| ${s.week} | ${e.byRule.rbPool.length} | ${e.byRule.wrPool.length} | ${e.byRule.tePool.length} | ${e.byRule.dstPool.length} | ${e.notes.join(" ").replace(/\|/g, "/").slice(0, 400)} |`);
}

// ---- hindsight
say("");
say("## 2. Hindsight optimum (perfect information, one lineup)");
say("");
say("Both solvers maximize actual DraftKings points under DraftKings' Classic rules plus the optimizer's always-on invariants (at most 8 players from one game; a defence with at most 3 opposing offensive players), except 'DraftKings rules only', which CBC alone solves without the second invariant. The defence-against-own-offense limit (5%) and the bring-back share (50%) are portfolio rules: a single Winners lineup may have either, so full Winners leaves both open; strict single-lineup versions appear only as labelled diagnostics. CBC's certificate (status, objective, bound, gap, time) is required before its number is used.");
say("");
const hindsight: Raw[] = [];
let agree = 0, total = 0;
const disagree: Raw[] = [];
for (const s of slates) {
  for (const punts of [false, true]) {
    for (const c of CONFIGS) {
      const cb = cbc[`${s.week}|${punts ? "punts" : "noPunts"}|${c.key}`];
      const row: Raw = { week: s.week, punts, key: c.key, label: c.label, group: c.group, cbc: cb };
      if (c.key !== "dk_only") {
        const o = optimizerOracle(s, c.rules, punts);
        row.optimizer = { points: o.points, ms: o.ms, illegal: o.illegal ?? null, error: o.error ?? null };
        const ok = cb?.status === "Optimal" && o.points != null && Math.abs(o.points - cb.objective) < 0.011 && (o.illegal ?? []).length === 0;
        total++; if (ok) agree++; else disagree.push(row);
      }
      hindsight.push(row);
    }
  }
}
results.hindsight = hindsight;
say(`**CBC certificates:** ${Object.values(cbc).filter((x) => x.status === "Optimal" && x.gap != null && x.gap <= 1e-6).length} of ${Object.keys(cbc).length} solves proved optimal with zero gap; max solve time ${r2(Math.max(...Object.values(cbc).map((x) => x.seconds)))} s.`);
say("");
say(`**Optimizer vs CBC:** ${agree} of ${total} solves agree on the optimum and legality.`);
if (disagree.length) {
  say("");
  say("| Week | Cheap players | Configuration | CBC | Optimizer | Optimizer lineup legal? | Note |");
  say("|---:|---|---|---:|---:|---|---|");
  for (const d of disagree) say(`| ${d.week} | ${d.punts ? "allowed" : "No punts"} | ${d.label} | ${d.cbc?.status === "Optimal" ? d.cbc.objective : d.cbc?.status} | ${d.optimizer.points ?? "-"} | ${d.optimizer.illegal ? (d.optimizer.illegal.length ? d.optimizer.illegal.join(", ") : "yes") : "-"} | ${(d.optimizer.error ?? "").slice(0, 120)} |`);
}
say("");
say("### Cost of each restriction (points below Unrestricted, CBC, No punts)");
say("");
const val = (week: number, key: string, punts = false) => cbc[`${week}|${punts ? "punts" : "noPunts"}|${key}`]?.objective ?? null;
say("| Week | Unrestricted | DST script invariant | " + WINNERS_KEYS.map((k) => NAMES[k] + " alone").join(" | ") + " | Full Winners | Sum of singles | Interaction | Standard defaults |");
say("|---:|---:|---:|" + WINNERS_KEYS.map(() => "---:").join("|") + "|---:|---:|---:|---:|");
for (const s of slates) {
  const u = val(s.week, "unrestricted");
  const singles = WINNERS_KEYS.map((k) => r2(u - val(s.week, `only_${k}`)));
  const full = r2(u - val(s.week, "winners"));
  const sum = r2(singles.reduce((a: number, b) => a + (b ?? 0), 0));
  say(`| ${s.week} | ${u} | ${r2(val(s.week, "dk_only") - u)} | ${singles.join(" | ")} | ${full} | ${sum} | ${r2((full ?? 0) - (sum ?? 0))} | ${r2(u - val(s.week, "standard"))} |`);
}
say("");
say("### Leave-one-out: what removing each restriction from full Winners recovers (CBC, No punts)");
say("");
say("| Week | Full Winners | " + WINNERS_KEYS.map((k) => `without ${NAMES[k]}`).join(" | ") + " | Diagnostic: strict DST | Diagnostic: bring-back required |");
say("|---:|---:|" + WINNERS_KEYS.map(() => "---:").join("|") + "|---:|---:|");
for (const s of slates) {
  const w = val(s.week, "winners");
  say(`| ${s.week} | ${w} | ${WINNERS_KEYS.map((k) => `+${r2(val(s.week, `winners_minus_${k}`) - w)}`).join(" | ")} | ${r2(val(s.week, "winners_strict_dst") - w)} | ${r2(val(s.week, "winners_strict_bringback") - w)} |`);
}
say("");
say("Weeks 1-3 have no usable prior opportunity, so their RB/WR/TE rules exclude nothing (the product skips them) and only the DST pool rule binds; week 4 rests on a partial reconstruction, week 5 on a complete one. Punts-allowed results are in results.json. Hindsight optima are not achievable; they bound what a restriction can cost.");

// ---- premise
say("");
say("## 3. Rule premise: what the verified top-100 entries did");
say("");
say("| Week | Entries (matched) | Unique lineups | Uses a player the pool rules exclude (entries / unique) | RB | WR | TE | DST | Under $49.5k | No WR/TE stack | Bring-back | DST vs own offense |");
say("|---:|---:|---:|---|---|---|---|---|---|---|---|---|");
const premise: Raw[] = [];
for (const s of slates) {
  const byName = new Map(s.players.map((p: Raw) => [norm(p.name), p]));
  const by = ex.slates[s.week].byRule;
  const sets: Record<string, Set<number>> = { RB: new Set(by.rbPool), WR: new Set(by.wrPool), TE: new Set(by.tePool), DST: new Set(by.dstPool) };
  const rows: Raw[] = [];
  let matched = 0;
  const tops = s.contest.topEntries.filter((e: Raw) => e.rank <= 100);
  for (const e of tops) {
    const ps = e.players.map((x: Raw) => byName.get(norm(x.name)));
    if (ps.length !== 9 || ps.some((x: Raw) => !x)) continue;
    matched++;
    const qb = ps.find((p: Raw) => p.position === "QB"), dst = ps.find((p: Raw) => p.position === "DST");
    const opp = (p: Raw) => p?.gameKey?.split("@").find((t: string) => t !== p.team);
    const hit = (pos: string) => ps.some((p: Raw) => sets[pos].has(p.dkPlayerId));
    rows.push({ key: ps.map((p: Raw) => p.dkPlayerId).sort().join("|"), any: ["RB", "WR", "TE", "DST"].some(hit), RB: hit("RB"), WR: hit("WR"), TE: hit("TE"), DST: hit("DST"),
      under: ps.reduce((a: number, p: Raw) => a + p.salary, 0) < FLOOR, noStack: !!qb && !ps.some((p: Raw) => ["WR", "TE"].includes(p.position) && p.team === qb.team),
      bring: !!qb && ps.some((p: Raw) => ["RB", "WR", "TE"].includes(p.position) && p.team === opp(qb)),
      dstVs: !!dst && ps.some((p: Raw) => ["QB", "RB", "WR", "TE"].includes(p.position) && p.team === opp(dst)) });
  }
  const uniq = [...new Map(rows.map((x) => [x.key, x])).values()];
  const f = (xs: Raw[], k: string) => `${xs.filter((x) => x[k]).length}/${xs.length}`;
  premise.push({ week: s.week, entries: tops.length, matched, unique: uniq.length, rows: rows.length });
  say(`| ${s.week} | ${tops.length} (${matched}) | ${uniq.length} | ${f(rows, "any")} / ${f(uniq, "any")} | ${f(rows, "RB")} | ${f(rows, "WR")} | ${f(rows, "TE")} | ${f(rows, "DST")} | ${f(rows, "under")} | ${f(rows, "noStack")} | ${f(rows, "bring")} | ${f(rows, "dstVs")} |`);
}
results.premise = premise;
say("");
say("Per-position columns are entry-weighted. Entries are those ranked 1-100 (ties kept whole), not the top 100 users. Pool-rule columns use the exclusions in section 1, so weeks 2-4 inherit the reconstruction caveat.");

// ---- pregame
say("");
say("## 4. Selection regret (original build at its own size)");
say("");
say("| Week | Lineups (unscored) | Best | Rank | Median | Top-1% | Above field median | Rules it used | Hindsight optimum, those rules | Regret | Sensitivity: no-score players as 0 (best, rank) | Players with no recorded score |");
say("|---:|---:|---:|---:|---:|---:|---:|---|---:|---:|---|---|");
const originals: Raw[] = [];
for (const s of slates) {
  const byId = new Map<number, Raw>(s.players.map((p: Raw) => [p.dkPlayerId, p]));
  const pts: number[] = [], asZero: number[] = []; let unscored = 0;
  const noScore = new Set<string>();
  for (const l of s.portfolio) {
    let t = 0, z = 0, ok = true;
    for (const x of l) { const a = byId.get(x.dkPlayerId)?.actual; if (a == null) { ok = false; noScore.add(x.name); } else { t += a; z += a; } }
    asZero.push(z);
    if (ok) pts.push(t); else unscored++;
  }
  const m = measures(pts, s);
  const z = measures(asZero, s);
  const winners = s.run.settings.buildShape === "winners";
  const key = winners ? "winners" : "standard";
  const opt = val(s.week, key);
  originals.push({ week: s.week, ...m, unscored, rules: key, optimum: opt, regret: m.best == null ? null : r2(opt - m.best), asZero: z, noScore: [...noScore] });
  say(`| ${s.week} | ${m.n} (${unscored}) | ${m.best ?? "-"} | ${m.bestRank?.toLocaleString() ?? "-"} | ${m.median ?? "-"} | ${m.n ? m.top1pct : "-"} | ${m.n ? m.aboveFieldMedian ?? "-" : "-"} | ${winners ? "Winners shape" : `Standard (min $${(s.run.settings.minSalary ?? 0).toLocaleString()})`} | ${opt} | ${m.best == null ? "-" : r2(opt - m.best)} | ${z.best} (${z.bestRank?.toLocaleString() ?? "-"}) | ${[...noScore].join(", ") || "-"} |`);
}
results.originals = originals;
say("");
say("Regret mixes forecast uncertainty with the selection objective; it is not projection error. For a Winners build the comparison optimum is the single-lineup Winners optimum. The sensitivity column scores players with no recorded score as 0, which is what DraftKings pays a player who did not play; it is labelled, not used elsewhere.");

say("");
say("## 5. Paired replays: 20 lineups from each frozen pool, today's code");
say("");
say("Same frozen pool and the run's other settings; arms Standard vs Winners x ownership leverage on vs off. Randomness is a fixed-seed hash, so arms are paired. Weeks 1-3 Winners arms apply only the DST pool rule; week 4 reads a partial reconstruction.");
say("");
say("| Week | Arm | Requested / built | Best | Rank | Median | Top-1% | Above field median | Sensitivity: no-score as 0 (best, rank) | Note |");
say("|---:|---|---|---:|---:|---:|---:|---:|---|---|");
const replays: Raw[] = [];
for (const s of slates) {
  const hasOwn = s.players.some((p: Raw) => p.ownership != null);
  const noHist = s.players.every((p: Raw) => p.historyGames == null);
  for (const shape of ["standard", "winners"]) for (const lev of [false, true]) {
    const note: string[] = [];
    let over: Raw = { nLineups: 20, buildShape: shape, ownershipCapability: hasOwn ? "heuristic_uncalibrated" : "unavailable", ownershipLeverageEnabled: lev && hasOwn };
    if (lev && !hasOwn) note.push("no ownership in snapshot: leverage inert");
    if (noHist) { over.requireObservedHistory = false; note.push("snapshot predates the observed-games field: that rule off"); }
    const run = (o: Raw) => optimizeNflLineups(toOptimizer(s.players, false), { ...s.run.settings, ...o } as NflOptimizerSettings);
    let res;
    try { res = run(over); }
    catch (e) {
      note.push(`run settings refused (${(e as Error).message.replace(/\s+/g, " ").slice(0, 90)}); retried with defensive off, projection source 'our', no DK-average fallback`);
      over = { ...over, defensiveAdjustments: { mode: "off", profile: "gpp-integrated" }, projectionSource: "our", allowDkFallback: false };
      try { res = run(over); } catch (e2) { replays.push({ week: s.week, shape, lev, error: (e2 as Error).message }); say(`| ${s.week} | ${shape}, leverage ${lev ? "on" : "off"} | 20 / 0 | - | - | - | - | - | - | failed: ${(e2 as Error).message.replace(/\s+/g, " ").slice(0, 140)} |`); continue; }
    }
    const byId = new Map<number, Raw>(s.players.map((p: Raw) => [p.dkPlayerId, p]));
    const pts: number[] = [], zeros: number[] = []; let unscored = 0;
    for (const l of res.lineups) {
      let t = 0, ok = true;
      for (const x of l.slots) { const a = byId.get(x.player.dkPlayerId)?.actual; if (a == null) ok = false; else t += a; }
      zeros.push(t); if (ok) pts.push(t); else unscored++;
    }
    const zb = measures(zeros, s);
    if (unscored) note.push(`${unscored} unscored`);
    if (res.lineups.length < 20) note.push(`stopped: ${(res.warnings.find((w) => /Stopped|cannot|infeasible|could not/i.test(w)) ?? res.warnings.slice(-1)[0] ?? "no reason given").replace(/\s+/g, " ").slice(0, 160)}`);
    const m = measures(pts, s);
    replays.push({ week: s.week, shape, lev, built: res.lineups.length, ...m, unscored, asZero: zb, note });
    const show = (v: unknown) => (v == null ? "-" : String(v));
    say(`| ${s.week} | ${shape}, leverage ${lev ? "on" : "off"} | 20 / ${res.lineups.length} | ${show(m.best)} | ${m.bestRank?.toLocaleString() ?? "-"} | ${show(m.median)} | ${m.n ? m.top1pct : "-"} | ${m.n ? show(m.aboveFieldMedian) : "-"} | ${zb.n ? `${zb.best} (${zb.bestRank?.toLocaleString() ?? "-"})` : "-"} | ${note.join("; ")} |`);
  }
}
results.replays = replays;
say("");
say("Replays measure the strategy on frozen inputs with today's code, not the build that was entered; their best-of-20 is not comparable with an original build of another size.");

// ---- forecasts
say("");
say("## 6. Player forecasts (descriptive)");
say("");
say("| Week | Players scored | MAE | Bias (actual - projected) | Inside P10-P90 | Mean P10-P90 width |");
say("|---:|---:|---:|---:|---:|---:|");
const fc: Raw[] = [];
for (const s of slates) {
  const xs = s.players.filter((p: Raw) => p.eligibilityClass === "available" && p.actual != null && (p.ourProj ?? 0) > 0);
  const wr = xs.filter((p: Raw) => p.floor != null && p.ceiling != null);
  const mae = xs.reduce((a: number, p: Raw) => a + Math.abs(p.actual - p.ourProj), 0) / (xs.length || 1);
  const bias = xs.reduce((a: number, p: Raw) => a + (p.actual - p.ourProj), 0) / (xs.length || 1);
  const inside = wr.filter((p: Raw) => p.actual >= p.floor && p.actual <= p.ceiling).length;
  fc.push({ week: s.week, n: xs.length, mae: r2(mae), bias: r2(bias), inside, ranged: wr.length });
  say(`| ${s.week} | ${xs.length} | ${r2(mae)} | ${r2(bias)} | ${inside}/${wr.length} | ${r2(wr.reduce((a: number, p: Raw) => a + (p.ceiling - p.floor), 0) / (wr.length || 1))} |`);
}
results.forecasts = fc;
say("");
say("Coverage is description only; players in one game move together, and pooled players with no recorded score are excluded.");
say("");
say("## 7. What this can and cannot say");
say("");
say(`- ${slates.length} slates identify mechanical exclusions, solver shortfalls and retrospective selection regret. They cannot say which strategy has better future returns, nor set an exposure level.`);
say("- The RB/WR/TE pool rules are measurable on week 5 (complete, and exact against that week's recorded exclusions) and week 4 (partial); weeks 1-3 cannot test them.");
say("- Hindsight optima bound what a restriction can cost, not what removing it would have won.");
writeFileSync(join(dir, "report.md"), report.join("\n"));
writeFileSync(join(dir, "results.json"), JSON.stringify(results, null, 1));
console.log(report.join("\n"));
