import { evOptimalEntry, makeRng, outcomePoints, resolveField, tieProbability,
  type Entry, type FieldModel, type GameOutcome, type PickemGame } from "./pickem-strategy";

export const CONTEST_POLICY_VERSION = "pickem-contest-v2";
export type PoolConfig = {
  version: 1;
  entries: number | null;
  gameTieRule: "zero" | "half" | "point" | null;
  prizeTieRule: "split" | "tiebreaker" | null;
  weeklyPayouts: number[] | null;
  seasonPayouts: number[] | null;
  ownScore: number | null;
  rivalScores: number[] | null;
  remainingWeeks: number | null;
  sameCard: boolean | null;
  lockRule: "first_kickoff" | "per_game" | null;
  sharePopulation: "rivals" | "all" | "unknown";
  standingsCapturedAt: string | null;
  settledWeek?: { capturedAt: string; gameIds: number[]; ownPoints: number; rivalPoints: number[] } | null;
};
export const EMPTY_POOL_CONFIG: PoolConfig = { version: 1, entries: null, gameTieRule: null,
  prizeTieRule: null, weeklyPayouts: null, seasonPayouts: null, ownScore: null, rivalScores: null,
  remainingWeeks: null, sameCard: null, lockRule: null, sharePopulation: "unknown", standingsCapturedAt: null };

export function validatePoolConfig(raw: unknown): PoolConfig {
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) throw new Error("Invalid pool rules");
  const p = { ...EMPTY_POOL_CONFIG, ...raw } as PoolConfig;
  if (p.version !== 1) throw new Error("Unknown pool configuration version");
  for (const k of ["entries", "remainingWeeks"] as const)
    if (p[k] != null && (!Number.isInteger(p[k]) || p[k]! < (k === "entries" ? 1 : 0))) throw new Error(`Invalid ${k}`);
  for (const k of ["weeklyPayouts", "seasonPayouts", "rivalScores"] as const)
    if (p[k] != null && (!Array.isArray(p[k]) || p[k]!.some(n => !Number.isFinite(n) || n < 0))) throw new Error(`Invalid ${k}`);
  if (p.ownScore != null && (!Number.isFinite(p.ownScore) || p.ownScore < 0)) throw new Error("Invalid own score");
  if (![null, "zero", "half", "point"].includes(p.gameTieRule) ||
      ![null, "split", "tiebreaker"].includes(p.prizeTieRule) ||
      ![null, "first_kickoff", "per_game"].includes(p.lockRule) ||
      !["rivals", "all", "unknown"].includes(p.sharePopulation) ||
      ![null, true, false].includes(p.sameCard)) throw new Error("Invalid pool rule");
  if (p.standingsCapturedAt != null && !Number.isFinite(Date.parse(p.standingsCapturedAt))) throw new Error("Invalid standings timestamp");
  if (p.settledWeek != null) {
    const s = p.settledWeek;
    if (!Number.isFinite(Date.parse(s.capturedAt)) || !Array.isArray(s.gameIds) || !s.gameIds.length ||
        s.gameIds.some(id => !Number.isInteger(id)) || new Set(s.gameIds).size !== s.gameIds.length ||
        !Number.isFinite(s.ownPoints) || s.ownPoints < 0 || s.ownPoints > s.gameIds.length ||
        !Array.isArray(s.rivalPoints) || s.rivalPoints.some(x => !Number.isFinite(x) || x < 0 || x > s.gameIds.length))
      throw new Error("Invalid completed-game score snapshot");
    const scoreStep = p.gameTieRule === "half" ? .5 : 1;
    if(p.gameTieRule != null && [s.ownPoints,...s.rivalPoints].some(x=>Math.abs(x/scoreStep-Math.round(x/scoreStep))>1e-9))
      throw new Error("Completed-game points do not match the selected scoring rule");
  }
  return p;
}

export function splitPayout(score: number, rivals: ArrayLike<number>, payouts: number[]): { payout: number; first: boolean; outright: boolean } {
  let above = 0, equal = 0;
  for (let i = 0; i < rivals.length; i++) { if (rivals[i] > score) above++; else if (rivals[i] === score) equal++; }
  let prize = 0;
  for (let rank = above; rank <= above + equal; rank++) prize += payouts[rank] ?? 0;
  return { payout: prize / (equal + 1), first: above === 0, outright: above === 0 && equal === 0 };
}

type Draw = { outcomes: GameOutcome[]; rivals: Float64Array; seasonRivals: Float64Array; ownFuture: number };
type Bank = { draws: Draw[]; seed: number; warnings: string[] };
const tieScore = (p: PoolConfig) => p.gameTieRule === "half" ? .5 : p.gameTieRule === "point" ? 1 : 0;
function expectedScore(games: PickemGame[], entry: Entry, p: PoolConfig) {
  return games.reduce((s,g,i) => s + (p.settledWeek?.gameIds.includes(g.gameId) ? 0 :
    (1-tieProbability(g))*(entry.pickHome[i] ? g.pHome : 1-g.pHome)+tieProbability(g)*tieScore(p)), p.settledWeek?.ownPoints ?? 0);
}

function buildBank(games: PickemGame[], future: PickemGame[], p: PoolConfig, model: FieldModel, sims: number, seed: number): Bank {
  const rng = makeRng(seed), rivals = (p.entries ?? 1) - 1, tie = tieScore(p);
  const settled = new Set(p.settledWeek?.gameIds ?? []);
  const activeGames = games.filter(g=>!settled.has(g.gameId));
  const activeIndex = new Map(activeGames.map((g,i)=>[g.gameId,i]));
  const nowField = resolveField(activeGames, model, p.entries ?? 1);
  const byWeek = new Map<number, PickemGame[]>();
  for (const g of future) byWeek.set(g.week, [...(byWeek.get(g.week) ?? []), g]);
  const futureFields = [...byWeek.values()].map(gs => ({ games: gs, field: resolveField(gs, model, p.entries ?? 1) }));
  const warnings = [...nowField.warnings];
  if(p.rivalScores) warnings.push("Rival strategy types are exchangeable across standings; no observed entrant-by-entrant strategy mapping was supplied.");
  if (future.length) warnings.push("Future weeks use stored forecast scenarios and a highest-expected-correct future card; these are not future observed market prices.");
  const sample = (g: PickemGame): GameOutcome => {
    const u = rng(), t = tieProbability(g);
    return u < t ? null : u < t + (1 - t) * g.pHome;
  };
  const draws: Draw[] = [];
  for (let s = 0; s < sims; s++) {
    // A user's order of rival scores must not assign the strongest opponents
    // permanently to the all-favorite block. Keep sampled type ranks through
    // future weeks within each world, but randomize their identities per world.
    const typeRank = Array.from({length:rivals},(_,i)=>i);
    for(let i=rivals-1;i>0;i--) { const j=Math.floor(rng()*(i+1)); [typeRank[i],typeRank[j]]=[typeRank[j],typeRank[i]]; }
    const outcomes = games.map(sample), scores = new Float64Array(rivals), seasonScores = new Float64Array(rivals);
    for (let r = 0; r < rivals; r++) {
      scores[r] = p.settledWeek?.rivalPoints[r] ?? 0;
      for (let i = 0; i < games.length; i++) {
        if (settled.has(games[i].gameId)) continue;
        const pick = typeRank[r] < nowField.chalkRivals ? games[i].pHome >= .5 : rng() < nowField.nonChalkShares[activeIndex.get(games[i].gameId)!];
        scores[r] += outcomePoints(pick, outcomes[i], tie);
      }
      seasonScores[r] = scores[r] + (p.rivalScores?.[r] ?? 0);
    }
    let ownFuture = 0;
    for (const { games: gs, field } of futureFields) {
      const outcomesFuture = gs.map(sample);
      for (let i = 0; i < gs.length; i++) ownFuture += outcomePoints(gs[i].pHome >= .5, outcomesFuture[i], tie);
      for (let r = 0; r < rivals; r++) for (let i = 0; i < gs.length; i++) {
        const pick = typeRank[r] < field.chalkRivals ? gs[i].pHome >= .5 : rng() < field.nonChalkShares[i];
        seasonScores[r] += outcomePoints(pick, outcomesFuture[i], tie);
      }
    }
    draws.push({ outcomes, rivals: scores, seasonRivals: seasonScores, ownFuture });
  }
  return { draws, seed, warnings: [...new Set(warnings)] };
}

export type ContestEvaluation = { expectedCorrect: number; weeklyPayout: number | null; seasonPayout: number | null;
  combinedPayout: number | null; weeklyFirstOrTied: number | null; seasonFirstOrTied: number | null;
  weeklyOutrightFirst: number | null; seasonOutrightFirst: number | null };
type Eligibility = { weekly: boolean; season: boolean; combined: boolean; reasons: string[] };
function eligibility(games: PickemGame[], future: PickemGame[], p: PoolConfig): Eligibility {
  const reasons: string[] = [];
  if (p.entries == null) reasons.push("Pool size is unknown");
  if ((p.entries ?? 0) > 1000) reasons.push("This explicit-rival evaluator supports at most 1,000 entries");
  if (p.gameTieRule == null) reasons.push("Game-tie scoring is unknown");
  if (p.prizeTieRule !== "split") reasons.push("Prize ties require known split rules; tiebreaker models are unavailable");
  if (p.lockRule == null) reasons.push("Lock rules are unknown");
  const started = games.filter(g => g.completed || (g.kickoff != null && Date.parse(g.kickoff) <= Date.now()));
  const settled = p.settledWeek;
  if (games.some(g => !g.kickoff || !Number.isFinite(Date.parse(g.kickoff)))) reasons.push("Kickoff times are missing");
  if (started.length || settled) {
    if (p.lockRule !== "per_game" || !settled || started.some(g => !g.completed) ||
        settled.gameIds.length !== started.length || started.some(g => !settled.gameIds.includes(g.gameId)) ||
        settled.rivalPoints.length !== (p.entries ?? 0)-1 || Date.parse(settled.capturedAt) > Date.now() ||
        started.some(g => !g.kickoff || Date.parse(settled.capturedAt) < Date.parse(g.kickoff)))
      reasons.push("Midweek comparison needs per-game locks and a timestamped own/rival score snapshot for every completed game; in-progress games are unavailable.");
  }
  if (games.some(g => g.pTie == null)) reasons.push("Tie probabilities are missing");
  const base = reasons.length === 0;
  const weekly = base && p.weeklyPayouts != null && p.weeklyPayouts.length > 0;
  const futureWeeks = new Set(future.map(g => g.week)).size;
  const season = base && p.seasonPayouts != null && p.seasonPayouts.length > 0 && p.ownScore != null &&
    p.rivalScores?.length === (p.entries ?? 0) - 1 && p.remainingWeeks != null &&
    futureWeeks === p.remainingWeeks && future.every(g => g.pTie != null) && p.standingsCapturedAt != null;
  if (!weekly) reasons.push("Weekly payout comparison is unavailable until its inputs are complete");
  if (!season) reasons.push("Season comparison requires payouts, timestamped scores for every rival, and the complete remaining-week scenario");
  const combined = weekly && season && p.sameCard === true;
  if (!combined) reasons.push("Combined comparison requires complete weekly/season inputs and the same card serving both prizes");
  return { weekly, season, combined, reasons };
}

function evaluate(games: PickemGame[], entry: Entry, p: PoolConfig, bank: Bank, e: Eligibility) {
  const weekly: number[] = [], season: number[] = [], combined: number[] = [];
  let wf = 0, sf = 0, wo = 0, so = 0;
  for (const d of bank.draws) {
    const score = games.reduce((a, g, i) => a + (p.settledWeek?.gameIds.includes(g.gameId) ? 0 : outcomePoints(entry.pickHome[i], d.outcomes[i], tieScore(p))), p.settledWeek?.ownPoints ?? 0);
    const w = splitPayout(score, d.rivals, p.weeklyPayouts ?? []);
    const s = splitPayout(score + (p.ownScore ?? 0) + d.ownFuture, d.seasonRivals, p.seasonPayouts ?? []);
    weekly.push(w.payout); season.push(s.payout); combined.push(w.payout + s.payout);
    wf += Number(w.first); sf += Number(s.first); wo += Number(w.outright); so += Number(s.outright);
  }
  const n = bank.draws.length, avg = (a: number[]) => a.reduce((s, x) => s + x, 0) / n;
  const result: ContestEvaluation = { expectedCorrect: expectedScore(games, entry, p),
    weeklyPayout: e.weekly ? avg(weekly) : null, seasonPayout: e.season ? avg(season) : null,
    combinedPayout: e.combined ? avg(combined) : null, weeklyFirstOrTied: e.weekly ? wf / n : null,
    seasonFirstOrTied: e.season ? sf / n : null, weeklyOutrightFirst: e.weekly ? wo / n : null, seasonOutrightFirst: e.season ? so / n : null };
  return { result, weekly, season, combined };
}

export type ContestComparison = { version: string; config: PoolConfig; eligibility: Eligibility; selectionSeed: number;
  evaluationSeed: number; sims: number; warnings: string[]; baseline: ContestEvaluation;
  fieldSensitivity: Array<{ scenario: string; objective: string; pairedPayoutGain: number }>;
  candidates: Array<{ objective: "weekly" | "season" | "combined"; entry: Entry; evaluation: ContestEvaluation;
    expectedCorrectCost: number; pairedPayoutGain: number; monteCarlo95: [number, number]; }>; futureGameIds: number[];
  futureScenario: Array<{gameId: number; week: number; pHome: number; pTie: number | null; provenance: string}> };

/** Search only on selection draws; all reported gains use independent paired evaluation draws. */
export function compareContestCards(games: PickemGame[], future: PickemGame[], raw: PoolConfig, model: FieldModel,
  options: { sims?: number; selectionSeed?: number; evaluationSeed?: number } = {}): ContestComparison {
  const config = validatePoolConfig(raw), e = eligibility(games, future, config);
  const selectionSeed = options.selectionSeed ?? 2026092701, evaluationSeed = options.evaluationSeed ?? 2026092702;
  if (selectionSeed === evaluationSeed) throw new Error("Selection and evaluation seeds must differ");
  const workLimit = Math.floor(20000000 / Math.max(1, ((config.entries ?? 1)-1) * (games.length+future.length)));
  const sims = Math.min(10000, Math.max(100, Math.min(options.sims ?? 1000, workLimit)));
  const baseline = evOptimalEntry(games, "straight");
  const fallback: ContestEvaluation = { expectedCorrect: expectedScore(games, baseline, config),
    weeklyPayout: null, seasonPayout: null, combinedPayout: null, weeklyFirstOrTied: null, seasonFirstOrTied: null,
    weeklyOutrightFirst: null, seasonOutrightFirst: null };
  const report: ContestComparison = { version: CONTEST_POLICY_VERSION, config, eligibility: e, selectionSeed, evaluationSeed, sims,
    warnings: [...e.reasons], baseline: fallback, candidates: [], fieldSensitivity: [], futureGameIds: future.map(g => g.gameId),
    futureScenario: future.map(g => ({ gameId: g.gameId, week: g.week, pHome: g.pHome, pTie: g.pTie ?? null, provenance: g.provenance })) };
  if (!e.weekly && !e.season) return report;
  const selection = buildBank(games, e.season ? future : [], config, model, sims, selectionSeed);
  const evaluation = buildBank(games, e.season ? future : [], config, model, sims, evaluationSeed);
  report.warnings.push(...selection.warnings, "Field behavior is a sensitivity assumption; matching pick shares does not identify full-card dependence.",
    "Candidates are the baseline plus one- and two-game changes; this is not an exhaustive global optimum.");
  const candidates = [baseline];
  for (let i = 0; i < games.length; i++) for (let j = i; j < games.length; j++) {
    if (config.settledWeek?.gameIds.includes(games[i].gameId) || config.settledWeek?.gameIds.includes(games[j].gameId)) continue;
    const entry = { pickHome: [...baseline.pickHome], confidence: [...baseline.confidence] };
    entry.pickHome[i] = !entry.pickHome[i]; if (j !== i) entry.pickHome[j] = !entry.pickHome[j]; candidates.push(entry);
  }
  const selected = candidates.map(entry => ({ entry, score: evaluate(games, entry, config, selection, e) }));
  const base = evaluate(games, baseline, config, evaluation, e); report.baseline = base.result;
  for (const objective of ["weekly", "season", "combined"] as const) {
    if (!e[objective]) continue;
    const key = `${objective}Payout` as "weeklyPayout" | "seasonPayout" | "combinedPayout";
    const chosen = selected.reduce((a, b) => (b.score.result[key] ?? -Infinity) > (a.score.result[key] ?? -Infinity) ? b : a);
    const measured = evaluate(games, chosen.entry, config, evaluation, e);
    const delta = measured[objective].map((x, i) => x - base[objective][i]);
    const mean = delta.reduce((a, x) => a + x, 0) / sims;
    const se = Math.sqrt(delta.reduce((a, x) => a + (x - mean) ** 2, 0) / (sims - 1) / sims);
    report.candidates.push({ objective, entry: chosen.entry, evaluation: measured.result,
      expectedCorrectCost: base.result.expectedCorrect - measured.result.expectedCorrect,
      pairedPayoutGain: mean, monteCarlo95: [mean - 1.96 * se, mean + 1.96 * se] });
  }
  // Re-evaluate the already selected cards under alternative field assumptions;
  // do not use these evaluation banks to search or select a replacement.
  for (const [i, scenario] of [{ label: "No all-favorite block; neutral favorite bias", chalkFraction: 0, favoriteBias: 1 },
    { label: "Half all-favorite block; stronger favorite bias", chalkFraction: .5, favoriteBias: 1.6 }].entries()) {
    const sensitivityBank = buildBank(games, e.season ? future : [], config, { ...model, ...scenario }, sims, evaluationSeed+11+i);
    const sensitivityBase = evaluate(games, baseline, config, sensitivityBank, e);
    for (const c of report.candidates) {
      const measured = evaluate(games, c.entry, config, sensitivityBank, e);
      const key = `${c.objective}Payout` as "weeklyPayout" | "seasonPayout" | "combinedPayout";
      report.fieldSensitivity.push({ scenario: scenario.label, objective: c.objective,
        pairedPayoutGain: (measured.result[key] ?? 0)-(sensitivityBase.result[key] ?? 0) });
    }
  }
  return report;
}
