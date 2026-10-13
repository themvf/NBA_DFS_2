/**
 * Showdown attribution study (read-only): where do our Showdown losses come
 * from -- construction rules, the selection objective, or ownership leverage?
 *
 * Inputs are the frozen per-game files written by
 * `research/nfl_showdown_attribution_export.py`. Nothing here writes to the
 * database or changes a setting. Output: `report.md` and `results.json` next
 * to the inputs.
 *
 * 1. Hindsight (single lineup, perfect information). Two solvers, the
 *    optimizer with its objective neutralized (custom projection = actual DK
 *    points, leverage off, randomness 0, no portfolio rules) and an
 *    independent exhaustive search, each find the best legal lineup under all
 *    16 combinations of the four Winners-shape rules (no kicker, no 4-2,
 *    $49,500 floor, captain QB/RB/WR), with the cheap-player rule on and
 *    relaxed. They must agree on the optimal score and on legality.
 * 2. Rule premise: how often the verified top-100 ENTRIES used a kicker, a 4-2
 *    split, both quarterbacks; entry-weighted and per unique construction.
 * 3. Selection regret: hindsight optimum under the rules a run used, minus the
 *    best lineup of (a) the original saved portfolio at its own size and
 *    (b) a controlled 20-lineup replay of the same frozen pool. Never compared
 *    with each other as equals.
 * 4. Controlled replay arms on the frozen pregame pool: Standard vs Winners
 *    shape x ownership leverage on vs off. The optimizer's randomness is a
 *    fixed-seed hash, so arms are paired by construction.
 * 5. Player forecasts, descriptive only: error, P10-P90 coverage and width;
 *    for week 5, the books' calibrated DK-point mean (the market anchor's own
 *    documented conversion) for players that had a full line set.
 *
 * Usage: node -r ./scripts/server-only-stub.cjs --import tsx scripts/research-nfl-showdown-attribution.ts <dir>
 */
import { readdirSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { optimizeNflLineups, type NflOptimizerPlayer, type NflOptimizerSettings } from "../src/app/dfs/nfl/nfl-optimizer";

const dir = process.argv[2];
if (!dir) { console.error("usage: research-nfl-showdown-attribution.ts <dir>"); process.exit(1); }

// eslint-disable-next-line @typescript-eslint/no-explicit-any -- untyped JSON from the exporter
type Raw = Record<string, any>;
type Game = Raw & { game: string; week: number; players: Raw[]; contest: Raw; run: Raw | null; portfolio: Raw[][] };
const games: Game[] = readdirSync(dir).filter((f) => /^\d{4}-w\d+-.+\.json$/.test(f)).sort((a, b) => {
  const wa = Number(a.split("-w")[1].split("-")[0]), wb = Number(b.split("-w")[1].split("-")[0]);
  return wa - wb || a.localeCompare(b);
}).map((f) => JSON.parse(readFileSync(join(dir, f), "utf8")));

const CAP = 50_000, FLOOR = 49_500, CPT = 1.5;
const norm = (s: string) => s.normalize("NFKD").toLowerCase().replace(/[^a-z0-9]/g, "").replace(/(jr|sr|ii|iii|iv|v)$/g, "");
const r2 = (x: number | null | undefined) => (x == null || !Number.isFinite(x) ? null : Math.round(x * 100) / 100);

// ---------------------------------------------------------------- rules
type Rules = { noKicker: boolean; noFourTwo: boolean; floor: boolean; captainQbRbWr: boolean };
const RULE_KEYS: (keyof Rules)[] = ["noKicker", "noFourTwo", "floor", "captainQbRbWr"];
const CONFIGS: Rules[] = Array.from({ length: 16 }, (_, i) => ({
  noKicker: !!(i & 1), noFourTwo: !!(i & 2), floor: !!(i & 4), captainQbRbWr: !!(i & 8) }));
const label = (r: Rules) => RULE_KEYS.filter((k) => r[k]).map((k) => ({ noKicker: "no K", noFourTwo: "no 4-2", floor: "$49.5k floor", captainQbRbWr: "CPT QB/RB/WR" })[k]).join(" + ") || "unrestricted";
const configIndex = (r: Rules) => (r.noKicker ? 1 : 0) | (r.noFourTwo ? 2 : 0) | (r.floor ? 4 : 0) | (r.captainQbRbWr ? 8 : 0);

type P = { id: number; cptId: number; name: string; pos: string; team: string; salary: number; cptSalary: number; actual: number };
type Lineup = { captain: P; flex: P[]; points: number; salary: number };

function legal(l: Lineup, r: Rules, teams: string[]): string[] {
  const all = [l.captain, ...l.flex];
  const bad: string[] = [];
  if (l.salary > CAP) bad.push("over cap");
  if (r.floor && l.salary < FLOOR) bad.push("under floor");
  if (r.noKicker && all.some((p) => p.pos === "K")) bad.push("kicker");
  const a = all.filter((p) => p.team === teams[0]).length;
  if (a === 0 || a === 6) bad.push("one team");
  if (r.noFourTwo && (a === 2 || a === 4)) bad.push("4-2");
  if (r.captainQbRbWr && !["QB", "RB", "WR"].includes(l.captain.pos)) bad.push("captain position");
  if (new Set(all.map((p) => p.id)).size !== 6) bad.push("duplicate");
  return bad;
}

/** Exhaustive: every 6-subset, every captain inside it; best per rule combination at once. */
function exactAll(pool: P[], teams: string[]): (Lineup | null)[] {
  const best: (Lineup | null)[] = Array(16).fill(null);
  const bestPts = Array(16).fill(-Infinity);
  const n = pool.length, idx = [0, 1, 2, 3, 4, 5];
  const t0 = teams[0];
  const rec = (start: number, depth: number) => {
    if (depth === 6) {
      let sal = 0, pts = 0, kick = false, a = 0;
      for (const i of idx) { const p = pool[i]; sal += p.salary; pts += p.actual; if (p.pos === "K") kick = true; if (p.team === t0) a++; }
      if (a === 0 || a === 6) return;
      const fourTwo = a === 2 || a === 4;
      for (const c of idx) {
        const cp = pool[c];
        const salary = sal - cp.salary + cp.cptSalary;
        if (salary > CAP) continue;
        const points = pts + cp.actual * (CPT - 1);
        const cptOk = cp.pos === "QB" || cp.pos === "RB" || cp.pos === "WR";
        for (let k = 0; k < 16; k++) {
          if ((k & 1) && kick) continue;
          if ((k & 2) && fourTwo) continue;
          if ((k & 4) && salary < FLOOR) continue;
          if ((k & 8) && !cptOk) continue;
          if (points > bestPts[k] + 1e-9) {
            bestPts[k] = points;
            best[k] = { captain: cp, flex: idx.filter((i) => i !== c).map((i) => pool[i]), points, salary };
          }
        }
      }
      return;
    }
    for (let i = start; i <= n - (6 - depth); i++) { idx[depth] = i; rec(i + 1, depth + 1); }
  };
  rec(0, 0);
  return best;
}

function toOptimizerPlayers(players: Raw[], customFromActual: boolean): NflOptimizerPlayer[] {
  // The optimizer drops a player whose projection is not positive. Adding one
  // constant to every player keeps 0- and negative-point players in the pool and
  // adds the same 6.5 x shift to every lineup (5 flex + 1.5 captain), so the
  // ranking of lineups is unchanged; scores are always recomputed from actuals.
  const shift = customFromActual ? 0.01 - Math.min(0, ...players.map((p) => p.actual ?? 0)) : 0;
  return players.map((p) => {
    const own = p.ownership && typeof p.ownership === "object" ? p.ownership : null;
    const base: Raw = {
      ...p, id: p.id ?? p.dkPlayerId, floorFpts: p.floor ?? p.floorFpts ?? null, ceilingFpts: p.ceiling ?? p.ceilingFpts ?? null,
      avgFptsDk: p.dkAvg ?? p.avgFptsDk ?? null, fantasyprosProj: p.fantasypros ?? null, linestarProj: p.linestar ?? null,
      linestarOwnPct: null, customProj: p.custom ?? null, ownPct: own?.total ?? null, flexOwnPct: own?.flex ?? null,
      captainOwnPct: own?.captain ?? null, ownSource: own?.source ?? null, boomRate: p.boomRate ?? null,
      ourProj: p.ourProj ?? null, isOut: !!p.isOut, projectionStatus: p.projectionStatus ?? "historical",
      captainSalary: p.captainSalary ?? Math.round((p.salary ?? 0) * 1.5),
    };
    if (customFromActual) {
      // Neutralized objective: custom projection = actual points; every
      // adjustment, availability gate and range removed.
      for (const k of ["specialTeams", "calibrated", "workload", "positionWorkload", "defensiveForecast", "availability",
        "availabilityStatus", "availabilityState", "dkStatus", "situationEvidence", "absenceCommit", "marketAnchor"]) delete base[k];
      // Kicker role is part of declared eligibility (KICKER_ROLE is an availability exclusion upstream),
      // so every kicker that reaches the oracle pool is the listed K1.
      if (base.position === "K") base.depthRole = "Listed K1";
      Object.assign(base, { customProj: p.actual + shift, ourProj: p.actual + shift, floorFpts: p.actual + shift, ceilingFpts: p.actual + shift, boomRate: 0, isOut: false,
        projectionStatus: "historical", historyGames: 10, ownPct: null, flexOwnPct: null, captainOwnPct: null });
    }
    return base as NflOptimizerPlayer;
  });
}

function oracleViaOptimizer(players: Raw[], r: Rules): { lineup: Lineup | null; error?: string } {
  const settings = {
    format: "showdown", mode: "gpp", projectionSource: "custom", allowDkFallback: false, nLineups: 1,
    minSalary: r.floor ? FLOOR : 0, minPlayerSalary: 0, requireObservedHistory: false, maxExposure: 1, minUnique: 1,
    stackPassCatchers: 0, bringBack: false, randomness: 0, buildShape: "standard", archetypeMode: "standard",
    lockedPlayerIds: [], excludedPlayerIds: [], minExposureByPlayer: {}, maxExposureByPlayer: {},
    puntPolicy: { mode: "custom", absoluteMinSalary: 0, denylistedPlayerIds: [], allowlistedPlayerIds: [], minimumRoleConfidence: 0,
      minimumProjectedOpportunities: null, maxSalaryReliefPlayersPerLineup: 6, roleEvidenceRequiredBelowSalary: 0 },
    puntOverrides: [], ownershipCapability: "unavailable", ownershipLeverageEnabled: false,
    defensiveAdjustments: { mode: "off", profile: "gpp-integrated" }, situations: { enabled: false, teams: {} },
    ...(r.noKicker ? { excludeKickers: true } : {}), ...(r.noFourTwo ? { forbidFourTwoSplit: true } : {}),
    ...(r.captainQbRbWr ? { captainPositions: ["QB", "RB", "WR"] } : {}),
  } as unknown as NflOptimizerSettings;
  try {
    const res = optimizeNflLineups(toOptimizerPlayers(players, true), settings);
    const l = res.lineups[0];
    if (!l) return { lineup: null, error: res.warnings.find((w) => /Stopped|cannot|No /i.test(w)) ?? "no lineup" };
    const byId = new Map(players.map((p) => [p.dkPlayerId, p]));
    const cs = l.slots.find((s) => s.multiplier === CPT)!;
    const asP = (q: Raw): P => ({ id: q.dkPlayerId, cptId: q.captainDkPlayerId, name: q.name, pos: q.position, team: q.team, salary: q.salary,
      cptSalary: q.captainSalary ?? Math.round(q.salary * 1.5), actual: q.actual });
    const captain = asP(byId.get(cs.player.dkPlayerId)!);
    const flex = l.slots.filter((s) => s !== cs).map((s) => asP(byId.get(s.player.dkPlayerId)!));
    return { lineup: { captain, flex, points: captain.actual * CPT + flex.reduce((a, p) => a + p.actual, 0), salary: l.totalSalary } };
  } catch (e) { return { lineup: null, error: (e as Error).message }; }
}

// ---------------------------------------------------------------- contest measures
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
function portfolioMeasures(points: number[], game: Game) {
  const curve = game.contest.curve as number[][];
  const entries = game.contest.entries as number;
  const median = (game.contest.median as number | null) ?? null;
  const sorted = [...points].sort((a, b) => b - a);
  const ranks = sorted.map((p) => rankOf(p, curve));
  const top1 = curve?.length ? ranks.filter((r) => r != null && r <= Math.ceil(entries * 0.01)).length : null;
  const med = sorted.length ? sorted[Math.floor((sorted.length - 1) / 2)] : null;
  return { n: points.length, best: r2(sorted[0]), bestRank: ranks[0] ?? null, median: r2(med),
    top1pct: top1, aboveFieldMedian: median == null ? null : sorted.filter((p) => p > median).length,
    bestMinusFieldMedian: median == null || sorted[0] == null ? null : r2(sorted[0] - median) };
}

// ---------------------------------------------------------------- study
const report: string[] = [];
const results: Raw = { generatedAt: new Date().toISOString(), games: [] };
const say = (s = "") => report.push(s);

say("# Showdown attribution study (read-only)");
say("");
say(`Generated ${results.generatedAt}. Inputs: \`research/nfl_showdown_attribution_export.py\` files in this folder; analysis: \`web/scripts/research-nfl-showdown-attribution.ts\`. No setting or optimizer code was changed.`);
say("");
say("## 1. Coverage");
say("");
say("| Week | Game | Contest | Entries | Score curve | Top-100 entries | Selected saved build (lineups) | Model | Optimizer then | Eligibility report | Pool | No recorded score |");
say("|---:|---|---|---:|---|---:|---|---|---|---|---:|---:|");
for (const g of games) {
  const a = g.audit;
  say(`| ${g.week} | ${g.game} | ${g.contest.id} | ${g.contest.entries?.toLocaleString()} | ${g.contest.curve?.length ? "yes" : "**no**"} | ${g.contest.topEntries.length} | ${g.run ? `yes (${g.run.lineups})` : "**none**"} | ${g.run?.modelVersion ?? "-"} | ${g.run?.optimizerVersion ?? "-"} | ${g.run ? (g.run.eligibilityReport ? "yes" : "no (salary-only punt rule)") : "-"} | ${a.pool} | ${a.missingActual.length} |`);
}
say("");
say("Players with no recorded score (no DraftKings contest number and no stat row) are excluded from every pool and counted above; most are $200 depth players. This study did not verify their participation, so the oracle's optimum is over the scored pool only. Source disagreements between the contest's DraftKings points and the results table: " +
  games.reduce((a, g) => a + g.audit.sourceDisagreements.length, 0) + ".");
say("");
say("**Declared eligibility** (shared by both solvers): the frozen run's pool, minus pregame availability exclusions (inactive, doubtful, backup-kicker role), which are never relaxed. The cheap-player rule (absolute $1,000 block and the role checks below $3,000) is applied in the *No punts* column and lifted in *punts allowed*. Kicker bans, the Winners-shape pool rule and manual exclusions are not part of declared eligibility; the study switches the kicker ban itself. Runs without an eligibility report (weeks 1-3) get only the $1,000 salary block as their cheap-player rule. TB@DAL has no saved run: its pool is the uploaded salary file and it is hindsight-only. Every recorded score enters the oracle pool, zeros and negatives included: under the split and salary-floor rules a $200 player who scored 0 can be what makes a lineup legal (column 'zero-point gain' in results.json).");

// ---- 2. hindsight
say("");
say("## 2. Hindsight optimum (perfect information, one lineup)");
say("");
say("Both solvers maximize actual DK points with the captain's 1.5x applied once and DraftKings' cap and both-teams rule. Agreement is required on the optimal score and on legality, for all 16 rule combinations and both cheap-player settings.");
say("");
const hindsight: Raw[] = [];
for (const g of games) {
  const teams = [...new Set(g.players.map((p) => p.team))].filter(Boolean).sort();
  const gh: Raw = { game: g.game, week: g.week, variants: {} };
  for (const variant of ["noPunts", "puntsAllowed"] as const) {
    const declared = g.players.filter((p) => p.actual != null && (p.eligibilityClass === "available" || (variant === "puntsAllowed" && p.eligibilityClass === "punt")));
    const pool: P[] = declared.map((p) => ({ id: p.dkPlayerId, cptId: p.captainDkPlayerId, name: p.name, pos: p.position, team: p.team,
      salary: p.salary, cptSalary: p.captainSalary ?? Math.round(p.salary * 1.5), actual: p.actual }));
    const exact = exactAll(pool, teams);
    const positiveOnly = exactAll(pool.filter((p) => p.actual > 0), teams);
    const rows: Raw[] = [];
    for (const r of CONFIGS) {
      const k = configIndex(r);
      const ex = exact[k];
      const opt = oracleViaOptimizer(declared, r);
      const optLegal = opt.lineup ? legal(opt.lineup, r, teams) : ["none"];
      const agree = !!ex && !!opt.lineup && Math.abs(ex.points - opt.lineup.points) < 0.011 && optLegal.length === 0 && legal(ex, r, teams).length === 0;
      const zeroGain = r2((ex?.points ?? 0) - (positiveOnly[k]?.points ?? 0));
      rows.push({ rules: label(r), k, exact: r2(ex?.points), optimizer: r2(opt.lineup?.points), agree, zeroGain, optimizerError: opt.error ?? null,
        optimizerIllegal: opt.lineup ? optLegal : null,
        lineup: ex ? `CPT ${ex.captain.name} + ${ex.flex.map((p) => p.name).join(", ")}` : null,
        hasKicker: ex ? [ex.captain, ...ex.flex].some((p) => p.pos === "K") : null,
        split: ex ? (() => { const a = [ex.captain, ...ex.flex].filter((p) => p.team === teams[0]).length; return `${Math.max(a, 6 - a)}-${Math.min(a, 6 - a)}`; })() : null,
        rank: ex ? rankOf(ex.points, g.contest.curve) : null });
    }
    gh.variants[variant] = { poolSize: pool.length, rows };
  }
  hindsight.push(gh);
}
results.hindsight = hindsight;
const disagreements = hindsight.flatMap((h) => Object.entries(h.variants as Record<string, Raw>).flatMap(([v, d]) => d.rows.filter((x: Raw) => !x.agree).map((x: Raw) => ({ game: h.game, variant: v, ...x }))));
say(`**Solver agreement: ${disagreements.length === 0 ? "all 16 x 2 x " + games.length + " cases agree on score and legality" : `${16 * 2 * games.length - disagreements.length} of ${16 * 2 * games.length} cases agree; ${disagreements.length} do not`}.**`);
if (disagreements.length) {
  say("");
  say("| Game | Cheap players | Rules | Exhaustive search | Optimizer | Optimizer lineup legal? | Optimizer error |");
  say("|---|---|---|---:|---:|---|---|");
  for (const d of disagreements) say(`| ${d.game} | ${d.variant === "noPunts" ? "No punts" : "allowed"} | ${d.rules} | ${d.exact} | ${d.optimizer ?? "-"} | ${d.optimizerIllegal ? (d.optimizerIllegal.length ? d.optimizerIllegal.join(", ") : "yes") : "-"} | ${d.optimizerError ?? ""} |`);
  say("");
  say("Where the optimizer's legal lineup scores below the exhaustive optimum, its integer solver (javascript-lp-solver, default settings) stopped short of the true optimum. Every cost below uses the exhaustive search.");
}
say("");
say("### Cost of each rule (points below the unrestricted optimum, No punts)");
say("");
const pick = (rows: Raw[], k: number): Raw => rows.find((x) => x.k === k)!;
say("| Week | Game | Unrestricted | no K | no 4-2 | $49.5k floor | CPT QB/RB/WR | All four (Winners shape) | Sum of single costs | Interaction | Unrestricted optimum: K? split | Punts allowed: unrestricted (gain) |");
say("|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---|---:|");
for (const h of hindsight) {
  const rows = h.variants.noPunts.rows, base = pick(rows, 0);
  const cost = (k: number) => (base.exact == null || pick(rows, k).exact == null ? null : r2(base.exact - pick(rows, k).exact));
  const singles = [1, 2, 4, 8].map(cost);
  const all = cost(15);
  const sum = singles.every((x) => x != null) ? r2(singles.reduce((a, b) => a + (b as number), 0)) : null;
  const pa = pick(h.variants.puntsAllowed.rows, 0);
  say(`| ${h.week} | ${h.game} | ${base.exact} | ${singles[0]} | ${singles[1]} | ${singles[2]} | ${singles[3]} | ${all} | ${sum} | ${all != null && sum != null ? r2(all - sum) : "-"} | ${base.hasKicker ? "K" : "no K"}, ${base.split} | ${pa.exact} (${r2(pa.exact - base.exact)}) |`);
}
say("");
say("Interaction = cost of all four together minus the sum of the four single costs (0 means the costs add). Rank of the unrestricted optimum against each contest is in results.json; it is not achievable pregame and is kept apart from every pregame number below.");

// ---- 3. premise
say("");
say("## 3. Rule premise: what the verified top-100 entries did");
say("");
say("| Week | Game | Top-100 entries (matched) | Unique constructions | Kicker (entries / unique) | 4-2 split (entries / unique) | Both QBs (entries / unique) | Captain QB/RB/WR (entries) |");
say("|---:|---|---:|---:|---|---|---|---|");
const premise: Raw[] = [];
for (const g of games) {
  const byName = new Map(g.players.map((p) => [norm(p.name), p]));
  const teams = [...new Set(g.players.map((p) => p.team))].filter(Boolean).sort();
  const tops = (g.contest.topEntries as Raw[]).filter((e) => e.rank <= 100);
  let matched = 0;
  const rows: Raw[] = [];
  for (const e of tops) {
    const ps = (e.players as Raw[]).map((x) => ({ slot: x.slot, p: byName.get(norm(x.name)) }));
    if (ps.some((x) => !x.p) || ps.length !== 6) continue;
    matched++;
    const key = ps.map((x) => `${x.slot}:${x.p!.dkPlayerId}`).sort().join("|");
    const a = ps.filter((x) => x.p!.team === teams[0]).length;
    const cpt = ps.find((x) => x.slot === "CPT")?.p;
    rows.push({ key, kicker: ps.some((x) => x.p!.position === "K"), fourTwo: a === 2 || a === 4,
      bothQb: ps.filter((x) => x.p!.position === "QB").length >= 2, cptOk: cpt ? ["QB", "RB", "WR"].includes(cpt.position) : null });
  }
  const uniq = [...new Map(rows.map((r) => [r.key, r])).values()];
  const frac = (xs: Raw[], f: string) => (xs.length ? `${xs.filter((x) => x[f]).length}/${xs.length}` : "-");
  premise.push({ game: g.game, entries: tops.length, matched, unique: uniq.length,
    kicker: [rows.filter((x) => x.kicker).length, uniq.filter((x) => x.kicker).length],
    fourTwo: [rows.filter((x) => x.fourTwo).length, uniq.filter((x) => x.fourTwo).length],
    bothQb: [rows.filter((x) => x.bothQb).length, uniq.filter((x) => x.bothQb).length] });
  say(`| ${g.week} | ${g.game} | ${tops.length ? `${tops.length} (${matched})` : "none stored"} | ${uniq.length || "-"} | ${frac(rows, "kicker")} / ${frac(uniq, "kicker")} | ${frac(rows, "fourTwo")} / ${frac(uniq, "fourTwo")} | ${frac(rows, "bothQb")} / ${frac(uniq, "bothQb")} | ${frac(rows, "cptOk")} |`);
}
results.premise = premise;
say("");
say("Entries are those ranked 1-100 in the contest (`nfl_dfs_field_top_entries`), not the top 100 users. 'Matched' = every player resolved to the slate pool by name. Games with no stored top entries (IND@WAS) have nothing to report.");

// ---- 4/5. pregame
say("");
say("## 4. Selection regret and controlled replays (pregame, achievable)");
say("");
function runSettings(g: Game, over: Raw): NflOptimizerSettings {
  const s = { ...g.run!.settings } as Raw;
  return { ...s, nLineups: 20, ...over } as NflOptimizerSettings;
}
const scoreLineups = (lineups: { slots: { player: { dkPlayerId: number }; multiplier: number }[] }[], g: Game) => {
  const byId = new Map(g.players.map((p) => [p.dkPlayerId, p]));
  const scored: number[] = []; let unscored = 0;
  for (const l of lineups) {
    let t = 0, ok = true;
    for (const s of l.slots) { const a = byId.get(s.player.dkPlayerId)?.actual; if (a == null) ok = false; else t += a * s.multiplier; }
    if (ok) scored.push(t); else unscored++;
  }
  return { scored, unscored };
};
const regret: Raw[] = [];
say("### 4a. Original saved portfolio (its own size) and regret against the hindsight optimum under the rules it used");
say("");
say("| Week | Game | Lineups (unscored) | Best | Rank | Median | Top-1% | Above field median | Rules it used | Hindsight optimum, same rules | Regret (optimum - our best) |");
say("|---:|---|---:|---:|---:|---:|---:|---:|---|---:|---:|");
for (const g of games) {
  if (!g.run) continue;
  const byId = new Map(g.players.map((p) => [p.dkPlayerId, p]));
  const pts: number[] = []; let unscored = 0;
  for (const l of g.portfolio) {
    let t = 0, ok = true;
    for (const s of l) { const a = byId.get(s.dkPlayerId)?.actual; if (a == null) { ok = false; break; } t += a * (String(s.slot).startsWith("CPT") ? CPT : 1); }
    if (ok) pts.push(t); else unscored++;
  }
  const m = portfolioMeasures(pts, g);
  const s = g.run.settings;
  const winners = s.buildShape === "winners";
  const used: Rules = { noKicker: winners, noFourTwo: winners, floor: winners || (s.minSalary ?? 0) >= FLOOR, captainQbRbWr: winners };
  const h = hindsight.find((x) => x.game === g.game)!;
  const opt = pick(h.variants.noPunts.rows, configIndex(used));
  regret.push({ game: g.game, ...m, unscored, rules: label(used), optimum: opt.exact, regret: m.best == null ? null : r2(opt.exact - m.best) });
  say(`| ${g.week} | ${g.game} | ${m.n} (${unscored}) | ${m.best} | ${m.bestRank ?? "-"} | ${m.median} | ${m.top1pct ?? "-"} | ${m.aboveFieldMedian ?? "-"} | ${label(used)}${(s.minSalary ?? 0) < FLOOR && !winners ? ` (min $${(s.minSalary ?? 0).toLocaleString()})` : ""} | ${opt.exact} | ${m.best == null ? "-" : r2(opt.exact - m.best)} |`);
}
results.originalPortfolios = regret;
say("");
say("Regret mixes forecast uncertainty and the selection objective; it is not projection error. The optimum uses the No punts pool, the cheap-player rule the runs used.");
say("");
say("### 4b. Controlled replay: 20 lineups from each frozen pregame pool, today's optimizer code");
say("");
say("Same frozen pool, the run's own other settings (plan, exposure, salary floor, defensive mode), 20 lineups. Arms: Standard or Winners shape, ownership leverage on or off. Randomness is a fixed-seed hash, so arms are paired. Weeks 1-3 snapshots carry no ownership, so their leverage arms are identical by construction.");
say("");
say("| Week | Game | Arm | Best | Rank | Median | Top-1% | Above field median | Regret vs hindsight (same rules) | Note |");
say("|---:|---|---|---:|---:|---:|---:|---:|---:|---|");
const replays: Raw[] = [];
for (const g of games) {
  if (!g.run) continue;
  const hasOwn = g.players.some((p) => p.ownership && typeof p.ownership === "object");
  // Snapshots written before the observed-games field existed store it as null for
  // everyone; today's rule reads null as 0 and removes the whole pool. Those runs did
  // not apply the rule in this form, so the replay switches it off and says so.
  const noHistoryField = g.players.every((p) => p.historyGames == null);
  for (const shape of ["standard", "winners"] as const) {
    for (const lev of [false, true]) {
      const note: string[] = [];
      let over: Raw = { buildShape: shape, ownershipCapability: hasOwn ? "heuristic_uncalibrated" : "unavailable", ownershipLeverageEnabled: lev && hasOwn };
      if (lev && !hasOwn) note.push("no ownership in snapshot: leverage inert");
      if (noHistoryField) { over.requireObservedHistory = false; note.push("snapshot predates the observed-games field: that rule off"); }
      let res;
      try { res = optimizeNflLineups(toOptimizerPlayers(g.players, false), runSettings(g, over)); }
      catch (e) {
        note.push(`run settings refused (${(e as Error).message.slice(0, 80)}); retried with defensive off and projection source 'our'`);
        over = { ...over, defensiveAdjustments: { mode: "off", profile: "gpp-integrated" }, projectionSource: "our" };
        try { res = optimizeNflLineups(toOptimizerPlayers(g.players, false), runSettings(g, over)); }
        catch (e2) { replays.push({ game: g.game, shape, lev, error: (e2 as Error).message }); say(`| ${g.week} | ${g.game} | ${shape}, leverage ${lev ? "on" : "off"} | - | - | - | - | - | - | failed: ${(e2 as Error).message.replace(/\s+/g, " ").slice(0, 160)} |`); continue; }
      }
      const { scored, unscored } = scoreLineups(res.lineups, g);
      if (res.lineups.length < 20) note.push(`built ${res.lineups.length}/20`);
      if (unscored) note.push(`${unscored} unscored`);
      const m = portfolioMeasures(scored, g);
      const used: Rules = shape === "winners" ? { noKicker: true, noFourTwo: true, floor: true, captainQbRbWr: true }
        : { noKicker: false, noFourTwo: false, floor: (g.run.settings.minSalary ?? 0) >= FLOOR, captainQbRbWr: false };
      const opt = pick(hindsight.find((x) => x.game === g.game)!.variants.noPunts.rows, configIndex(used));
      replays.push({ game: g.game, shape, lev, ...m, unscored, regret: m.best == null ? null : r2(opt.exact - m.best), note });
      say(`| ${g.week} | ${g.game} | ${shape}, leverage ${lev ? "on" : "off"} | ${m.best} | ${m.bestRank ?? "-"} | ${m.median} | ${m.top1pct ?? "-"} | ${m.aboveFieldMedian ?? "-"} | ${m.best == null ? "-" : r2(opt.exact - m.best)} | ${note.join("; ")} |`);
    }
  }
}
results.replays = replays;
say("");
say("Replays measure the strategy on frozen inputs with today's code, not the build that was entered. Best-lineup numbers from 20 lineups and from the original portfolio size are not comparable.");

// ---- attribution summary
say("");
say("## 5. Attribution table (one row per game)");
say("");
say("Hindsight columns bound what a rule can cost; pregame columns are achievable. Replay columns are best-of-20 actual points (rank in parentheses where the contest has a score curve).");
say("");
say("| Week | Game | Hindsight optimum (unrestricted) | Winners rules cost (hindsight) | Original best (lineups) | Regret, original | Standard, lev off | Standard, lev on | Winners, lev off | Winners, lev on |");
say("|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|");
for (const g of games) {
  const h = hindsight.find((x) => x.game === g.game)!;
  const base = pick(h.variants.noPunts.rows, 0).exact, all = pick(h.variants.noPunts.rows, 15).exact;
  const o = regret.find((x) => x.game === g.game);
  const arm = (shape: string, lev: boolean) => {
    const x = replays.find((y) => y.game === g.game && y.shape === shape && y.lev === lev);
    if (!x) return "-";
    if (x.error) return "failed";
    return x.best == null ? "-" : `${x.best}${x.bestRank ? ` (${x.bestRank.toLocaleString()})` : ""}`;
  };
  say(`| ${g.week} | ${g.game} | ${base} | ${r2(base - all)} | ${o ? `${o.best} (${o.n})` : "no build"} | ${o?.regret ?? "-"} | ${arm("standard", false)} | ${arm("standard", true)} | ${arm("winners", false)} | ${arm("winners", true)} |`);
}

// ---- 6. forecasts
say("");
say("## 6. Player forecasts (descriptive)");
say("");
say("| Week | Game | Players scored | MAE | Bias (actual - projected) | Inside P10-P90 | Mean P10-P90 width |");
say("|---:|---|---:|---:|---:|---:|---:|");
const fc: Raw[] = [];
for (const g of games) {
  if (!g.run) continue;
  const xs = g.players.filter((p) => p.eligibilityClass === "available" && p.actual != null && p.ourProj != null && p.ourProj > 0);
  const withRange = xs.filter((p) => p.floor != null && p.ceiling != null);
  const mae = xs.reduce((a, p) => a + Math.abs(p.actual - p.ourProj), 0) / (xs.length || 1);
  const bias = xs.reduce((a, p) => a + (p.actual - p.ourProj), 0) / (xs.length || 1);
  const inside = withRange.filter((p) => p.actual >= p.floor && p.actual <= p.ceiling).length;
  const width = withRange.reduce((a, p) => a + (p.ceiling - p.floor), 0) / (withRange.length || 1);
  fc.push({ game: g.game, n: xs.length, mae: r2(mae), bias: r2(bias), inside: `${inside}/${withRange.length}`, width: r2(width) });
  say(`| ${g.week} | ${g.game} | ${xs.length} | ${r2(mae)} | ${r2(bias)} | ${inside}/${withRange.length} | ${r2(width)} |`);
}
results.forecasts = fc;
const b = games.find((g) => g.game === "BUF@LAR");
if (b) {
  const anch = b.players.filter((p) => p.marketAnchor && p.actual != null && p.eligibilityClass === "available");
  const e = (f: (p: Raw) => number) => r2(anch.reduce((a, p) => a + Math.abs(p.actual - f(p)), 0) / (anch.length || 1));
  say("");
  say(`Week 5 BUF@LAR, ${anch.length} players with a full set of pregame lines: books' calibrated DK-point mean (market anchor) MAE ${e((p) => p.marketAnchor.after.mean)}, our model's own number before the anchor MAE ${e((p) => p.marketAnchor.before.mean)}. Per-market lines against their own stats for weeks 1-5 are in \`research/nfl_prop_outcomes.py\` (rushing overs 44.6% vs 49.8% implied; receptions and receiving yards calibrated); weeks 1-4 lines are pregame snapshots captured after the fact.`);
}
say("");
say("P10-P90 coverage is a description, not a calibration test: a nominal 80% interval is judged over many player-games, and players in one game move together.");

say("");
say("## 7. What this can and cannot say");
say("");
say(`- ${games.length} games (${games.filter((g) => g.run).length} with a saved pregame build). That is enough to see which rules cost points in hindsight and how our selections behave, not to establish an exposure percentage, a profitable strategy, or a winner between arms.`);
say("- Hindsight optima are not achievable; they bound what a rule can cost, not what removing it would have won.");
say("- Replays use today's code on old inputs; earlier weeks ran older projection versions (coverage table).");

writeFileSync(join(dir, "report.md"), report.join("\n"), "utf8");
writeFileSync(join(dir, "results.json"), JSON.stringify(results, null, 1), "utf8");
console.log(report.join("\n"));
