/** Shared evidence contracts and decision arithmetic. No inferred injury adjustments. */
export type MarketQuote = {
  capturedAt: string;
  pHome: number | null;
  homeSpread: number | null;
  source: string;
};
export type PickemNews = {
  id: string;
  team: string;
  category: "quarterback" | "offensive-line" | "playmaker" | "defense" | "weather" | "coaching" | "other";
  headline: string;
  detail: string;
  status: "confirmed" | "uncertain" | "reported";
  source: string;
  url: string | null;
  publishedAt: string | null;
  observedAt: string;
};
export type Performance = {
  gameId: number;
  team: string;
  week: number;
  plays: number;
  epaPerPlay: number | null;
  successRate: number | null;
  rushYards: number | null;
  turnovers: number;
  fieldGoalsMade: number;
  defensiveReturnTdsAllowed: number;
  kickReturnTdsAllowed: number;
};
export type GameEvidence = {
  opening: MarketQuote | null;
  latest: MarketQuote | null;
  news: PickemNews[];
  performance: Performance[];
  recentForm?: Array<{ team: string; games: number; offenseEpa: number | null; defenseEpa: number | null; rushYardsAllowed: number | null }>;
};
export type PickemEvidence = {
  loadedAt: string;
  games: Record<number, GameEvidence>;
  warnings: string[];
};
export type PickemScenario = { pHome: number; reason: string };
export type FrozenEvidence = GameEvidence & {
  version: 1;
  recordedAt: string;
  probabilityComputedAt: string | null;
  narrative: "crowded" | "contrarian" | "quiet";
  favoriteHome: boolean;
  scenario: PickemScenario | null;
  marketBaselinePickHome: boolean | null;
  marketBaselineConfidence: number | null;
  coverageWarnings?: string[];
};
export const EMPTY_EVIDENCE: GameEvidence = { opening: null, latest: null, news: [], performance: [] };

export function timestamp(value: string | null | undefined): number {
  if (!value) return NaN;
  return Date.parse(value.replace(" ", "T").replace(/([+-]\d{2})$/, "$1:00"));
}

export function noVigHome(home: number | null, away: number | null): number | null {
  if (home == null || away == null || !Number.isFinite(home) || !Number.isFinite(away) ||
      Math.abs(home) < 100 || Math.abs(away) < 100) return null;
  const implied = (odds: number) => odds < 0 ? -odds / (-odds + 100) : 100 / (odds + 100);
  const h = implied(home), a = implied(away);
  return h / (h + a);
}

export function safeSourceUrl(value: string): string | null {
  try {
    const url = new URL(value);
    return url.protocol === "https:" && !url.username && !url.password ? url.href : null;
  } catch { return null; }
}

export function marketReview(evidence: GameEvidence, pHome: number, kickoff: string | null, now: string) {
  const at = timestamp(now), starts = timestamp(kickoff);
  const captured = timestamp(evidence.latest?.capturedAt);
  // Tighten freshness inside 24 hours. This is an operational rule, not a model parameter.
  const maxAgeHours = Number.isFinite(starts) && starts - at <= 24 * 3600_000 ? 2 : 24;
  const closed = Number.isFinite(starts) && starts <= at;
  const stale = !closed && (!Number.isFinite(captured) || captured > at || at - captured > maxAgeHours * 3600_000);
  const favorite = (q: MarketQuote | null) => q?.pHome != null ? Math.sign(q.pHome - 0.5)
    : q?.homeSpread != null ? -Math.sign(q.homeSpread) : 0;
  const favoriteChanged = favorite(evidence.opening) * favorite(evidence.latest) < 0;
  const probabilityConflict = Math.sign(pHome - 0.5) * favorite(evidence.latest) < 0;
  const newsAfterQuote = evidence.news.filter(n => !Number.isFinite(captured) ||
    timestamp(n.publishedAt ?? n.observedAt) > captured).length;
  return { stale, closed, maxAgeHours, favoriteChanged, probabilityConflict, newsAfterQuote };
}

export function scenarioDecision(pHome: number, pickHome: boolean, confidence: number) {
  const p = pickHome ? pHome : 1 - pHome;
  return { p, expectedPoints: confidence * p, switchCost: confidence * (2 * p - 1), preferredHome: pHome >= 0.5 };
}

/** Only earlier weeks; retain sample size so one result cannot look like a trend. */
export function recentForm(performance: Performance[], team: string, beforeWeek: number) {
  const own = performance.filter(p => p.team === team && p.week < beforeWeek).sort((a, b) => b.week - a.week).slice(0, 3);
  const opponents = own.map(p => performance.find(o => o.gameId === p.gameId && o.team !== team));
  const mean = (values: Array<number | null | undefined>) => {
    const valid = values.filter((v): v is number => v != null && Number.isFinite(v));
    return valid.length === own.length && valid.length > 0 ? valid.reduce((a, b) => a + b, 0) / valid.length : null;
  };
  return { team, games: own.length, offenseEpa: mean(own.map(p => p.epaPerPlay)),
    defenseEpa: mean(opponents.map(p => p?.epaPerPlay)), rushYardsAllowed: mean(opponents.map(p => p?.rushYards)) };
}

/** Paired sample only; missing market evidence is never replaced with model probability. */
export function evidenceGrades(rows: Array<{
  gameId: number; pHome: number; homeWon: boolean | null;
  recommendedPickHome: boolean; recommendedConfidence: number;
  fieldHomeShare: number | null; fieldSource: string; evidence?: FrozenEvidence | null;
}>) {
  let n = 0, brier = 0, marketBrier = 0, points = 0, marketPoints = 0;
  const narrative = { crowded: { n: 0, sum: 0 }, contrarian: { n: 0, sum: 0 }, quiet: { n: 0, sum: 0 } };
  for (const row of rows) {
    const e = row.evidence;
    if (!e) continue;
    if (row.fieldSource === "observed" && row.fieldHomeShare != null) {
      const bucket = narrative[e.narrative];
      bucket.n++;
      bucket.sum += 100 * (e.favoriteHome ? row.fieldHomeShare - row.pHome : row.pHome - row.fieldHomeShare);
    }
    if (row.homeWon == null || e.latest?.pHome == null || e.marketBaselinePickHome == null || e.marketBaselineConfidence == null) continue;
    const y = row.homeWon ? 1 : 0;
    n++;
    brier += (row.pHome - y) ** 2;
    marketBrier += (e.latest.pHome - y) ** 2;
    if (row.recommendedPickHome === row.homeWon) points += row.recommendedConfidence;
    if (e.marketBaselinePickHome === row.homeWon) marketPoints += e.marketBaselineConfidence;
  }
  return { n, brier: n ? brier / n : null, marketBrier: n ? marketBrier / n : null,
    points, marketPoints, narrative };
}
