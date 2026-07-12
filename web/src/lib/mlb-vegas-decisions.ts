import type { MlbActionabilityDecision } from "./mlb-vegas-trust";

export const MLB_DECISION_POLICY_VERSION = "mlb-decision-ux-v3";
export const MLB_MIN_MEAN_ROI = 0.02;
export const MLB_MIN_POSITIVE_RESAMPLE_RATE = 0.8;
export const MLB_MAX_QUOTE_AGE_MINUTES = 10;
export const MLB_START_BUFFER_MINUTES = 5;
export const MLB_WATCH_PRICE_DISTANCE = 0.02;

export type MlbDecisionMarket = "moneyline" | "total";
export type MlbPrimaryStatus = "take_now" | "watch" | "pass" | "blocked" | "closed";
export type MlbDecisionRelationship =
  | "agree_model_stronger"
  | "agree_market_stronger"
  | "disagree"
  | "at_market"
  | "model_above_line"
  | "model_below_line";
export type MlbPriceSupport = "passes" | "near_target" | "too_expensive" | "unavailable";
export type MlbCompleteness = "complete" | "optional_missing" | "required_missing";
export type MlbFragility = "low" | "medium" | "high" | "blocked";
export type MlbProbabilityKind = "calibrated" | "raw" | "unavailable";

export type MlbBookQuote = Record<string, unknown>;

export type MlbDecisionMatchup = {
  matchupId: number;
  gameDate?: string;
  gameId?: string | null;
  gameStatus?: string | null;
  doubleheaderGameNumber?: number | null;
  awayAbbrev: string;
  homeAbbrev: string;
  awaySpName?: string | null;
  homeSpName?: string | null;
  ballpark?: string | null;
  commenceTime: string | null;
  oddsSnapshotId?: number | null;
  oddsCaptureKey?: string | null;
  oddsCapturedAt: string | null;
  oddsBooks: Record<string, MlbBookQuote> | null;
  openingOddsBooks: Record<string, MlbBookQuote> | null;
  moneylinePredictionSnapshotId?: number | null;
  moneylineReferenceOddsSnapshotId?: number | null;
  moneylinePredictionEventCommence?: string | null;
  moneylineFeatureAvailableAt?: string | null;
  moneylineReferenceMarketProbability?: number | null;
  moneylinePrediction: number | null;
  moneylineCalibratedProbability?: number | null;
  moneylinePredictionAt: string | null;
  moneylineModelVersion: string | null;
  moneylineRunConfig: Record<string, unknown> | null;
  moneylineFeatureValues: Record<string, unknown> | null;
  moneylineMissingness: Record<string, boolean> | null;
  totalPredictionSnapshotId?: number | null;
  totalReferenceOddsSnapshotId?: number | null;
  totalPredictionEventCommence?: string | null;
  totalFeatureAvailableAt?: string | null;
  totalReferenceMarketLine?: number | null;
  totalPrediction: number | null;
  totalPredictionAt: string | null;
  totalModelVersion: string | null;
  totalRunConfig: Record<string, unknown> | null;
  totalFeatureValues: Record<string, unknown> | null;
  totalMissingness: Record<string, boolean> | null;
};

export type MlbDecisionReason = {
  label: string;
  detail: string;
  source: "Model snapshot" | "Sportsbook quote" | "Trust policy" | "Data health";
  direction?: "for" | "against" | "neutral";
};

export type MlbMarketDecision = {
  decisionId: string;
  key: string;
  evaluatedAt: string;
  eventRevisionId: string;
  matchupId: number;
  gameId: string | null;
  gameDate: string | null;
  gameStatus: string | null;
  doubleheaderGameNumber: number | null;
  matchup: string;
  homeAbbrev: string;
  awayAbbrev: string;
  homeSpName: string | null;
  awaySpName: string | null;
  ballpark: string | null;
  commenceTime: string | null;
  market: MlbDecisionMarket;
  primaryStatus: MlbPrimaryStatus;
  headline: string;
  primaryReason: string;
  nextAction: string | null;
  watchTrigger: string | null;
  selection: string | null;
  side: "home" | "away" | "over" | "under" | null;
  line: number | null;
  modelProbability: number | null;
  probabilityKind: MlbProbabilityKind;
  modelTotal: number | null;
  referenceProbability: number | null;
  referenceLine: number | null;
  offeredBreakEven: number | null;
  priceMargin: number | null;
  estimatedRoi: number | null;
  targetDecimalPrice: number | null;
  targetAmericanPrice: number | null;
  resamplePositiveRate: number | null;
  uncertaintyLow: number | null;
  uncertaintyHigh: number | null;
  relationship: MlbDecisionRelationship;
  relationshipLabel: string;
  priceSupport: MlbPriceSupport;
  completeness: MlbCompleteness;
  fragility: MlbFragility;
  fragilityReasons: string[];
  bookKey: string | null;
  bookLabel: string | null;
  price: number | null;
  pairedPrice: number | null;
  decimalPrice: number | null;
  bookmakerUpdatedAt: string | null;
  oddsCapturedAt: string | null;
  quoteAgeMinutes: number | null;
  validUntil: string | null;
  observedOddsSnapshotId: number | null;
  referenceOddsSnapshotId: number | null;
  predictionSnapshotId: number | null;
  predictionAt: string | null;
  modelVersion: string | null;
  canonicalHorizon: string | null;
  trustEvaluationId: string | null;
  trustState: MlbActionabilityDecision["state"] | "unavailable";
  trustOpenGates: number;
  policyVersion: string;
  reasons: MlbDecisionReason[];
  blockers: string[];
  missingInformation: string[];
  parlayEligible: boolean;
};

export type BuildMlbDecisionOptions = {
  evaluatedAt: string;
  trustDecisions?: MlbActionabilityDecision[];
};

export const MLB_BOOK_LABELS: Record<string, string> = {
  draftkings: "DraftKings",
  fanduel: "FanDuel",
  betmgm: "BetMGM",
  williamhill_us: "Caesars",
  fanatics: "Fanatics",
  betrivers: "BetRivers",
  espnbet: "ESPN BET",
  hardrockbet: "Hard Rock Bet",
  bet365: "bet365",
  ballybet: "Bally Bet",
  betfred: "Betfred",
  betparx: "betPARX",
  fliff: "Fliff",
};

export const MLB_DEFAULT_BOOKS = [
  "draftkings",
  "fanduel",
  "betmgm",
  "williamhill_us",
  "fanatics",
] as const;

const REFERENCE_ONLY_BOOKS = new Set(["pinnacle"]);

const FEATURE_LABELS: Record<string, string> = {
  market_home_prob: "Reference market probability",
  sp_xfip_adv: "Starting-pitcher xFIP difference",
  sp_k9_adv: "Starting-pitcher strikeout difference",
  wrc_adv: "Team offense difference",
  iso_adv: "Team power difference",
  bullpen_adv: "Bullpen quality difference",
  vegas_total: "Reference total",
  home_implied: "Home implied runs",
  away_implied: "Away implied runs",
  abs_spread: "Run expectation gap",
  home_win_prob: "Reference home-win probability",
  sp_xfip_avg: "Starting-pitcher run prevention",
  sp_xfip_diff: "Starting-pitcher quality gap",
  sp_k9_avg: "Starting-pitcher strikeout environment",
  park_runs_factor: "Park run factor",
  temp_delta: "Temperature context",
  wind_component: "Wind context",
  wrc_avg: "Combined offense quality",
  iso_avg: "Combined power",
  bullpen_fip_avg: "Bullpen run prevention",
};

function finiteNumber(value: unknown): number | null {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function finiteProbability(value: unknown): number | null {
  const n = finiteNumber(value);
  return n != null && n >= 0 && n <= 1 ? n : null;
}

function parseTimestamp(value: unknown): number | null {
  if (typeof value !== "string") return null;
  const parsed = Date.parse(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function sameInstant(a: string | null | undefined, b: string | null | undefined): boolean {
  const left = parseTimestamp(a);
  const right = parseTimestamp(b);
  return left != null && right != null && left === right;
}

export function isValidAmericanPrice(value: unknown): value is number {
  return typeof value === "number"
    && Number.isFinite(value)
    && Number.isInteger(value)
    && (value <= -100 || value >= 100);
}

export function americanToDecimal(price: number): number {
  if (!isValidAmericanPrice(price)) throw new Error(`Invalid American price: ${price}`);
  return price < 0 ? 1 + 100 / Math.abs(price) : 1 + price / 100;
}

export function americanToImpliedProbability(price: number): number {
  return 1 / americanToDecimal(price);
}

export function minimumAmericanPrice(decimalPrice: number): number {
  if (!Number.isFinite(decimalPrice) || decimalPrice <= 1) {
    throw new Error(`Invalid decimal price: ${decimalPrice}`);
  }
  const exact = decimalPrice >= 2
    ? (decimalPrice - 1) * 100
    : -100 / (decimalPrice - 1);
  const integer = Math.ceil(exact - 1e-10);
  if (integer > -100 && integer < 100) return 100;
  return integer;
}

export function removeTwoWayVig(homePrice: number, awayPrice: number): number {
  const home = americanToImpliedProbability(homePrice);
  const away = americanToImpliedProbability(awayPrice);
  return home / (home + away);
}

function configString(config: Record<string, unknown> | null, key: string): string | null {
  const value = config?.[key];
  return typeof value === "string" && value.trim() ? value.trim() : null;
}

function hasSourceAwareMissingness(config: Record<string, unknown> | null): boolean {
  return config?.missingness_policy === "source-aware-v1";
}

function hasRealCalibration(config: Record<string, unknown> | null): boolean {
  const method = configString(config, "calibration_method")?.toLowerCase();
  return Boolean(method && !["none", "raw", "identity", "uncalibrated"].includes(method));
}

function missingSourceInputs(missingness: Record<string, boolean> | null): string[] {
  if (!missingness) return ["Source-aware feature coverage is unavailable"];
  return Object.entries(missingness)
    .filter(([, missing]) => missing)
    .map(([feature]) => `${FEATURE_LABELS[feature] ?? feature.replaceAll("_", " ")} unavailable`);
}

function contributionReasons(featureValues: Record<string, unknown> | null): MlbDecisionReason[] {
  const raw = featureValues?.contributions;
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) return [];
  return Object.entries(raw as Record<string, unknown>)
    .map(([feature, value]) => ({ feature, value: finiteNumber(value) }))
    .filter((row): row is { feature: string; value: number } => row.value != null)
    .filter((row) => row.feature !== "market_home_prob" && row.feature !== "vegas_total")
    .sort((a, b) => Math.abs(b.value) - Math.abs(a.value))
    .slice(0, 4)
    .map((row) => ({
      label: FEATURE_LABELS[row.feature] ?? row.feature.replaceAll("_", " "),
      detail: `${row.value >= 0 ? "Raised" : "Lowered"} the fitted model score in this frozen run`,
      source: "Model snapshot" as const,
      direction: row.value >= 0 ? "for" as const : "against" as const,
    }));
}

function numberArray(value: unknown): number[] {
  if (!Array.isArray(value)) return [];
  return value.map(finiteNumber).filter((n): n is number => n != null);
}

function percentile(values: number[], q: number): number | null {
  if (values.length === 0) return null;
  const sorted = [...values].sort((a, b) => a - b);
  const index = Math.min(sorted.length - 1, Math.max(0, Math.floor(q * (sorted.length - 1))));
  return sorted[index];
}

type TotalDistribution = {
  line: number;
  pOver: number;
  pPush: number;
  pUnder: number;
  resamples: Array<{ pOver: number; pPush: number; pUnder: number }>;
};

function parseTotalDistribution(featureValues: Record<string, unknown> | null): TotalDistribution | null {
  const raw = featureValues?.total_distribution;
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) return null;
  const row = raw as Record<string, unknown>;
  const line = finiteNumber(row.line);
  const pOver = finiteProbability(row.p_over ?? row.pOver);
  const pPush = finiteProbability(row.p_push ?? row.pPush);
  const pUnder = finiteProbability(row.p_under ?? row.pUnder);
  if (line == null || pOver == null || pPush == null || pUnder == null) return null;
  if (Math.abs(pOver + pPush + pUnder - 1) > 1e-6) return null;
  const rawResamples = Array.isArray(row.resamples) ? row.resamples : [];
  const resamples = rawResamples.flatMap((sample) => {
    if (Array.isArray(sample) && sample.length === 3) {
      const over = finiteProbability(sample[0]);
      const push = finiteProbability(sample[1]);
      const under = finiteProbability(sample[2]);
      return over != null && push != null && under != null && Math.abs(over + push + under - 1) <= 1e-6
        ? [{ pOver: over, pPush: push, pUnder: under }]
        : [];
    }
    if (!sample || typeof sample !== "object") return [];
    const record = sample as Record<string, unknown>;
    const over = finiteProbability(record.p_over ?? record.pOver);
    const push = finiteProbability(record.p_push ?? record.pPush);
    const under = finiteProbability(record.p_under ?? record.pUnder);
    return over != null && push != null && under != null && Math.abs(over + push + under - 1) <= 1e-6
      ? [{ pOver: over, pPush: push, pUnder: under }]
      : [];
  });
  return { line, pOver, pPush, pUnder, resamples };
}

function relationshipLabel(relationship: MlbDecisionRelationship): string {
  switch (relationship) {
    case "agree_model_stronger": return "Agree - model stronger";
    case "agree_market_stronger": return "Agree - market stronger";
    case "disagree": return "Disagree on winner";
    case "model_above_line": return "Model above line";
    case "model_below_line": return "Model below line";
    default: return "At market";
  }
}

function moneylineRelationship(modelHome: number | null, referenceHome: number | null): MlbDecisionRelationship {
  if (modelHome == null || referenceHome == null || Math.abs(modelHome - referenceHome) < 0.005) {
    return "at_market";
  }
  const modelFavoriteHome = modelHome >= 0.5;
  const marketFavoriteHome = referenceHome >= 0.5;
  if (modelFavoriteHome !== marketFavoriteHome) return "disagree";
  return Math.abs(modelHome - 0.5) > Math.abs(referenceHome - 0.5)
    ? "agree_model_stronger"
    : "agree_market_stronger";
}

function bookKeys(books: Record<string, MlbBookQuote> | null): Array<string | null> {
  if (!books) return [null];
  const keys = Object.keys(books).filter(
    (key) => !REFERENCE_ONLY_BOOKS.has(key) && Object.hasOwn(MLB_BOOK_LABELS, key),
  );
  return keys.length > 0 ? keys : [null];
}

function bookLabel(key: string | null): string | null {
  return key == null ? null : MLB_BOOK_LABELS[key] ?? key;
}

function eventRevisionId(matchup: MlbDecisionMatchup): string {
  return [matchup.gameId ?? `matchup-${matchup.matchupId}`, matchup.commenceTime ?? "commence-missing"].join(":");
}

function isClosedStatus(status: string | null | undefined): boolean {
  const normalized = status?.toLowerCase() ?? "";
  return ["final", "game over", "completed", "postponed", "cancelled", "canceled"].some((value) => normalized.includes(value));
}

function effectiveQuoteTimestamp(capturedAt: string | null, bookmakerUpdatedAt: string | null): number | null {
  const capture = parseTimestamp(capturedAt);
  const update = parseTimestamp(bookmakerUpdatedAt);
  if (capture == null) return null;
  return update == null ? capture : Math.min(capture, update);
}

function validUntilIso(quoteAt: number | null, commenceAt: number | null): string | null {
  if (quoteAt == null || commenceAt == null) return null;
  const quoteExpiry = quoteAt + MLB_MAX_QUOTE_AGE_MINUTES * 60_000;
  const startBuffer = commenceAt - MLB_START_BUFFER_MINUTES * 60_000;
  return new Date(Math.min(quoteExpiry, startBuffer)).toISOString();
}

function trustForMarket(options: BuildMlbDecisionOptions, market: MlbDecisionMarket): MlbActionabilityDecision | null {
  return options.trustDecisions?.find((decision) => decision.market === market) ?? null;
}

function commonDecision(
  matchup: MlbDecisionMatchup,
  market: MlbDecisionMarket,
  bookKey: string | null,
  options: BuildMlbDecisionOptions,
) {
  const predictionSnapshotId = market === "moneyline"
    ? matchup.moneylinePredictionSnapshotId ?? null
    : matchup.totalPredictionSnapshotId ?? null;
  const referenceOddsSnapshotId = market === "moneyline"
    ? matchup.moneylineReferenceOddsSnapshotId ?? null
    : matchup.totalReferenceOddsSnapshotId ?? null;
  const predictionAt = market === "moneyline" ? matchup.moneylinePredictionAt : matchup.totalPredictionAt;
  const modelVersion = market === "moneyline" ? matchup.moneylineModelVersion : matchup.totalModelVersion;
  const config = market === "moneyline" ? matchup.moneylineRunConfig : matchup.totalRunConfig;
  const canonicalHorizon = configString(config, "canonical_horizon");
  const trust = trustForMarket(options, market);
  const revision = eventRevisionId(matchup);
  const decisionId = [
    revision,
    market,
    bookKey ?? "no-book",
    matchup.oddsSnapshotId ?? "no-quote",
    predictionSnapshotId ?? "no-prediction",
    MLB_DECISION_POLICY_VERSION,
  ].join(":");
  return {
    decisionId,
    key: decisionId,
    evaluatedAt: options.evaluatedAt,
    eventRevisionId: revision,
    matchupId: matchup.matchupId,
    gameId: matchup.gameId ?? null,
    gameDate: matchup.gameDate ?? null,
    gameStatus: matchup.gameStatus ?? null,
    doubleheaderGameNumber: matchup.doubleheaderGameNumber ?? null,
    matchup: `${matchup.awayAbbrev} @ ${matchup.homeAbbrev}`,
    homeAbbrev: matchup.homeAbbrev,
    awayAbbrev: matchup.awayAbbrev,
    homeSpName: matchup.homeSpName ?? null,
    awaySpName: matchup.awaySpName ?? null,
    ballpark: matchup.ballpark ?? null,
    commenceTime: matchup.commenceTime,
    market,
    bookKey,
    bookLabel: bookLabel(bookKey),
    oddsCapturedAt: matchup.oddsCapturedAt,
    observedOddsSnapshotId: matchup.oddsSnapshotId ?? null,
    referenceOddsSnapshotId,
    predictionSnapshotId,
    predictionAt,
    modelVersion,
    canonicalHorizon,
    trustEvaluationId: trust?.trustEvaluationId ?? null,
    trustState: trust?.state ?? "unavailable" as const,
    trustOpenGates: trust ? trust.total - trust.passed : 0,
    policyVersion: MLB_DECISION_POLICY_VERSION,
  };
}

type StatusInputs = {
  matchup: MlbDecisionMatchup;
  market: MlbDecisionMarket;
  options: BuildMlbDecisionOptions;
  blockers: string[];
  selection: string | null;
  bookLabel: string | null;
  price: number | null;
  estimatedRoi: number | null;
  priceSupport: MlbPriceSupport;
  resamplePositiveRate: number | null;
  requiresRecalculation: boolean;
  canonicalHorizon: string | null;
  modelVersion: string | null;
};

function classifyStatus(inputs: StatusInputs): Pick<MlbMarketDecision, "primaryStatus" | "headline" | "primaryReason" | "nextAction" | "watchTrigger"> {
  const evaluatedAt = parseTimestamp(inputs.options.evaluatedAt);
  const commenceAt = parseTimestamp(inputs.matchup.commenceTime);
  const selection = inputs.selection ?? (inputs.market === "moneyline" ? "moneyline" : "total");
  const quote = inputs.price != null && inputs.bookLabel ? `${inputs.bookLabel} ${inputs.price > 0 ? "+" : ""}${inputs.price}` : inputs.bookLabel;
  if (isClosedStatus(inputs.matchup.gameStatus) || (evaluatedAt != null && commenceAt != null && evaluatedAt >= commenceAt)) {
    const postponed = inputs.matchup.gameStatus?.toLowerCase().includes("postpon") ?? false;
    return {
      primaryStatus: "closed",
      headline: postponed ? "CLOSED / VOID" : "CLOSED",
      primaryReason: postponed
        ? "The event was postponed; a makeup game requires a new event revision and decision."
        : "The game has started, so no new pregame action is available.",
      nextAction: null,
      watchTrigger: null,
    };
  }
  if (inputs.blockers.length > 0) {
    return {
      primaryStatus: "blocked",
      headline: `BLOCKED - ${inputs.blockers[0]}`,
      primaryReason: inputs.blockers[0],
      nextAction: inputs.blockers[0].toLowerCase().includes("quote")
        ? "Reload pipeline output after the next quote capture."
        : "Open details to see the missing requirement and source evidence.",
      watchTrigger: null,
    };
  }
  if (inputs.requiresRecalculation) {
    return {
      primaryStatus: "watch",
      headline: `WATCH - recalculate ${selection} at the current price`,
      primaryReason: "The observed quote is newer than the market input used by this prediction.",
      nextAction: "Run a fresh prediction against the current quote snapshot.",
      watchTrigger: "Fresh prediction linked to the current odds snapshot",
    };
  }

  const trust = trustForMarket(inputs.options, inputs.market);
  const trustMatches = Boolean(
    trust
    && trust.modelVersion === inputs.modelVersion
    && trust.canonicalHorizon === inputs.canonicalHorizon,
  );
  const localTake = inputs.estimatedRoi != null
    && inputs.estimatedRoi >= MLB_MIN_MEAN_ROI
    && inputs.resamplePositiveRate != null
    && inputs.resamplePositiveRate >= MLB_MIN_POSITIVE_RESAMPLE_RATE
    && inputs.priceSupport === "passes";

  if (localTake && trust?.state === "actionable" && trustMatches) {
    return {
      primaryStatus: "take_now",
      headline: `TAKE NOW - ${selection}${quote ? ` at ${quote}` : ""}`,
      primaryReason: "The exact price, model-stability, integrity, and promoted-cohort rules all pass.",
      nextAction: `Verify the exact ${quote ?? "sportsbook quote"} before placing the bet.`,
      watchTrigger: null,
    };
  }
  if (localTake) {
    return {
      primaryStatus: "watch",
      headline: `WATCH - ${selection} clears the local price rule`,
      primaryReason: trustMatches
        ? `${trust?.total ?? 0} trust requirements are evaluated; ${trust?.total != null ? trust.total - trust.passed : "promotion"} remain open.`
        : "The validation evidence does not match this exact model version and canonical horizon.",
      nextAction: "Keep tracking this exact price while the prospective cohort completes.",
      watchTrigger: "Exact cohort promotion gates pass",
    };
  }
  if (
    (inputs.estimatedRoi != null && inputs.estimatedRoi > 0)
    || inputs.priceSupport === "near_target"
    || (inputs.estimatedRoi != null && inputs.estimatedRoi >= MLB_MIN_MEAN_ROI)
  ) {
    return {
      primaryStatus: "watch",
      headline: `WATCH - ${selection} is close but does not qualify`,
      primaryReason: inputs.priceSupport === "near_target"
        ? "The displayed price is within the locked watch distance of the qualifying price."
        : inputs.resamplePositiveRate != null && inputs.resamplePositiveRate < MLB_MIN_POSITIVE_RESAMPLE_RATE
          ? `${(inputs.resamplePositiveRate * 100).toFixed(0)}% of model resamples stay positive; policy requires ${(MLB_MIN_POSITIVE_RESAMPLE_RATE * 100).toFixed(0)}%.`
          : `Modeled ROI is ${inputs.estimatedRoi == null ? "unavailable" : `${(inputs.estimatedRoi * 100).toFixed(1)}%`}; policy requires ${(MLB_MIN_MEAN_ROI * 100).toFixed(1)}%.`,
      nextAction: "Track the target price shown on this card and require a fresh decision when it is reached.",
      watchTrigger: "Target price or stability threshold is reached",
    };
  }
  return {
    primaryStatus: "pass",
    headline: `PASS - ${selection}${quote ? ` at ${quote}` : ""}`,
    primaryReason: inputs.estimatedRoi == null
      ? "The current snapshot does not support a positive price margin."
      : `Modeled ROI is ${(inputs.estimatedRoi * 100).toFixed(1)}%, below the locked price rule.`,
    nextAction: "No action at the current price; reassess only after a new quote or material input change.",
    watchTrigger: null,
  };
}

function baseBlockers(
  matchup: MlbDecisionMatchup,
  market: MlbDecisionMarket,
  options: BuildMlbDecisionOptions,
  bookKey: string | null,
  bookmakerUpdatedAt: string | null,
): { blockers: string[]; quoteAgeMinutes: number | null; validUntil: string | null } {
  const blockers: string[] = [];
  const evaluatedAt = parseTimestamp(options.evaluatedAt);
  const commenceAt = parseTimestamp(matchup.commenceTime);
  const predictionAt = parseTimestamp(market === "moneyline" ? matchup.moneylinePredictionAt : matchup.totalPredictionAt);
  const predictionCommence = market === "moneyline"
    ? matchup.moneylinePredictionEventCommence
    : matchup.totalPredictionEventCommence;
  const featureAvailableAt = parseTimestamp(market === "moneyline"
    ? matchup.moneylineFeatureAvailableAt
    : matchup.totalFeatureAvailableAt);
  const effectiveQuoteAt = effectiveQuoteTimestamp(matchup.oddsCapturedAt, bookmakerUpdatedAt);
  const quoteAgeMinutes = evaluatedAt != null && effectiveQuoteAt != null
    ? (evaluatedAt - effectiveQuoteAt) / 60_000
    : null;

  if (!matchup.gameId) blockers.push("MLB game identity is missing");
  if (commenceAt == null) blockers.push("game start time is missing or invalid");
  if (bookKey == null) blockers.push("no configured sportsbook has an exact paired quote");
  if (matchup.oddsSnapshotId == null) blockers.push("observed quote snapshot identity is missing");
  if (effectiveQuoteAt == null) blockers.push("quote timestamp is missing or invalid");
  if (quoteAgeMinutes != null && quoteAgeMinutes > MLB_MAX_QUOTE_AGE_MINUTES + 1e-9) {
    blockers.push(`quote is ${Math.floor(quoteAgeMinutes)} minutes old; policy maximum is ${MLB_MAX_QUOTE_AGE_MINUTES}`);
  }
  if (quoteAgeMinutes != null && quoteAgeMinutes < -1) blockers.push("quote timestamp is later than the decision time");
  if (effectiveQuoteAt != null && commenceAt != null && effectiveQuoteAt >= commenceAt) {
    blockers.push("quote was captured after the scheduled start");
  }
  if (evaluatedAt != null && commenceAt != null && evaluatedAt < commenceAt
      && commenceAt - evaluatedAt < MLB_START_BUFFER_MINUTES * 60_000) {
    blockers.push(`inside the ${MLB_START_BUFFER_MINUTES}-minute manual verification buffer`);
  }
  if (predictionAt == null) blockers.push("immutable prospective prediction is missing");
  if (predictionAt != null && commenceAt != null && predictionAt >= commenceAt) {
    blockers.push("prediction was written after the scheduled start");
  }
  if (!sameInstant(predictionCommence, matchup.commenceTime)) {
    blockers.push("prediction belongs to a different event revision or start time");
  }
  if (featureAvailableAt == null) blockers.push("feature availability timestamp is missing");
  if (featureAvailableAt != null && predictionAt != null && featureAvailableAt > predictionAt) {
    blockers.push("a required feature became available after the prediction cutoff");
  }
  return { blockers, quoteAgeMinutes, validUntil: validUntilIso(effectiveQuoteAt, commenceAt) };
}

function decisionFragility(status: MlbPrimaryStatus, reasons: string[]): MlbFragility {
  if (status === "blocked") return "blocked";
  if (status === "watch" || reasons.length >= 2) return "high";
  if (status === "take_now" && reasons.length === 0) return "low";
  return "medium";
}

function buildMoneylineDecisionForBook(
  matchup: MlbDecisionMatchup,
  bookKey: string | null,
  options: BuildMlbDecisionOptions,
): MlbMarketDecision {
  const common = commonDecision(matchup, "moneyline", bookKey, options);
  const quote = bookKey ? matchup.oddsBooks?.[bookKey] : null;
  const homePrice = quote?.ml_home;
  const awayPrice = quote?.ml_away;
  const validPair = isValidAmericanPrice(homePrice) && isValidAmericanPrice(awayPrice);
  const bookmakerUpdatedAt = typeof quote?.last_update === "string" ? quote.last_update : null;
  const health = baseBlockers(matchup, "moneyline", options, bookKey, bookmakerUpdatedAt);
  const blockers = [...health.blockers];
  if (!validPair) blockers.push("exact two-sided moneyline price is missing or invalid");
  if (matchup.moneylinePredictionSnapshotId == null) blockers.push("moneyline prediction snapshot identity is missing");
  if (matchup.moneylineReferenceOddsSnapshotId == null) blockers.push("prediction reference-quote identity is missing");
  if (!hasSourceAwareMissingness(matchup.moneylineRunConfig)) {
    blockers.push("prediction predates source-aware feature tracking");
  }
  blockers.push(...missingSourceInputs(matchup.moneylineMissingness));

  const rawModelHome = finiteProbability(matchup.moneylinePrediction);
  const calibratedModelHome = hasRealCalibration(matchup.moneylineRunConfig)
    ? finiteProbability(matchup.moneylineCalibratedProbability)
    : null;
  const modelHome = calibratedModelHome ?? rawModelHome;
  const probabilityKind: MlbProbabilityKind = calibratedModelHome != null
    ? "calibrated"
    : rawModelHome != null
      ? "raw"
      : "unavailable";
  if (rawModelHome == null) blockers.push("moneyline model probability is missing or invalid");
  if (calibratedModelHome == null) blockers.push("out-of-fold calibrated moneyline probability is unavailable");
  const homeResamples = numberArray(
    matchup.moneylineFeatureValues?.probability_resamples
      ?? matchup.moneylineRunConfig?.probability_resamples,
  ).filter((p) => p >= 0 && p <= 1);
  if (homeResamples.length === 0) blockers.push("moneyline model-stability resamples are unavailable");
  const canonicalHorizon = configString(matchup.moneylineRunConfig, "canonical_horizon");
  if (!canonicalHorizon) blockers.push("canonical prediction horizon is missing");

  const referenceHome = finiteProbability(matchup.moneylineReferenceMarketProbability);
  if (referenceHome == null) blockers.push("fixed reference-market probability is missing");

  let side: "home" | "away" | null = null;
  let selection: string | null = null;
  let price: number | null = null;
  let pairedPrice: number | null = null;
  let modelProbability: number | null = null;
  let referenceProbability: number | null = null;
  let offeredBreakEven: number | null = null;
  let estimatedRoi: number | null = null;
  let targetDecimalPrice: number | null = null;
  let targetAmericanPrice: number | null = null;
  let resamplePositiveRate: number | null = null;
  let uncertaintyLow: number | null = null;
  let uncertaintyHigh: number | null = null;

  if (validPair && modelHome != null) {
    const homeDecimal = americanToDecimal(homePrice);
    const awayDecimal = americanToDecimal(awayPrice);
    const homeRoi = modelHome * homeDecimal - 1;
    const awayRoi = (1 - modelHome) * awayDecimal - 1;
    side = homeRoi >= awayRoi ? "home" : "away";
    selection = side === "home" ? matchup.homeAbbrev : matchup.awayAbbrev;
    price = side === "home" ? homePrice : awayPrice;
    pairedPrice = side === "home" ? awayPrice : homePrice;
    modelProbability = side === "home" ? modelHome : 1 - modelHome;
    referenceProbability = referenceHome == null ? null : side === "home" ? referenceHome : 1 - referenceHome;
    const decimal = americanToDecimal(price);
    offeredBreakEven = 1 / decimal;
    estimatedRoi = modelProbability * decimal - 1;
    if (calibratedModelHome != null && homeResamples.length > 0) {
      const selectedSamples = side === "home" ? homeResamples : homeResamples.map((p) => 1 - p);
      const sampleRois = selectedSamples.map((p) => p * decimal - 1);
      resamplePositiveRate = sampleRois.filter((roi) => roi > 0).length / sampleRois.length;
      uncertaintyLow = percentile(selectedSamples, 0.1);
      uncertaintyHigh = percentile(selectedSamples, 0.9);
      targetDecimalPrice = (1 + MLB_MIN_MEAN_ROI) / modelProbability;
      targetAmericanPrice = minimumAmericanPrice(targetDecimalPrice);
    }
  }

  const decimalPrice = price != null ? americanToDecimal(price) : null;
  const priceDistance = decimalPrice != null && targetDecimalPrice != null
    ? (targetDecimalPrice - decimalPrice) / targetDecimalPrice
    : null;
  const priceSupport: MlbPriceSupport = targetDecimalPrice == null || decimalPrice == null
    ? "unavailable"
    : decimalPrice + 1e-12 >= targetDecimalPrice
      ? "passes"
      : priceDistance != null && priceDistance > 0 && priceDistance <= MLB_WATCH_PRICE_DISTANCE
        ? "near_target"
        : "too_expensive";
  const requiresRecalculation = matchup.oddsSnapshotId != null
    && matchup.moneylineReferenceOddsSnapshotId != null
    && matchup.oddsSnapshotId !== matchup.moneylineReferenceOddsSnapshotId;
  const status = classifyStatus({
    matchup,
    market: "moneyline",
    options,
    blockers: [...new Set(blockers)],
    selection,
    bookLabel: common.bookLabel,
    price,
    estimatedRoi,
    priceSupport,
    resamplePositiveRate,
    requiresRecalculation,
    canonicalHorizon,
    modelVersion: matchup.moneylineModelVersion,
  });
  const relationship = moneylineRelationship(modelHome, referenceHome);
  const reasons = contributionReasons(matchup.moneylineFeatureValues);
  if (referenceHome != null && modelHome != null) {
    reasons.unshift({
      label: relationshipLabel(relationship),
      detail: `${matchup.homeAbbrev} model ${Math.round(modelHome * 1000) / 10}% vs fixed reference ${Math.round(referenceHome * 1000) / 10}%`,
      source: "Model snapshot",
      direction: relationship === "disagree" ? "against" : "neutral",
    });
  }
  const blockerList = [...new Set(blockers)];
  const fragilityReasons = [
    ...blockerList.slice(0, 3),
    ...(requiresRecalculation ? ["Current quote is newer than the model's market input"] : []),
  ];
  return {
    ...common,
    ...status,
    selection,
    side,
    line: null,
    modelProbability,
    probabilityKind,
    modelTotal: null,
    referenceProbability,
    referenceLine: null,
    offeredBreakEven,
    priceMargin: modelProbability != null && offeredBreakEven != null ? modelProbability - offeredBreakEven : null,
    estimatedRoi,
    targetDecimalPrice,
    targetAmericanPrice,
    resamplePositiveRate,
    uncertaintyLow,
    uncertaintyHigh,
    relationship,
    relationshipLabel: relationshipLabel(relationship),
    priceSupport,
    completeness: blockerList.some((reason) => reason.includes("feature") || reason.includes("prediction")) ? "required_missing" : "complete",
    fragility: decisionFragility(status.primaryStatus, fragilityReasons),
    fragilityReasons,
    price,
    pairedPrice,
    decimalPrice,
    bookmakerUpdatedAt,
    quoteAgeMinutes: health.quoteAgeMinutes,
    validUntil: health.validUntil,
    reasons,
    blockers: blockerList,
    missingInformation: blockerList,
    parlayEligible: status.primaryStatus === "take_now" && price != null && price < 0,
  };
}

function buildTotalDecisionForBook(
  matchup: MlbDecisionMatchup,
  bookKey: string | null,
  options: BuildMlbDecisionOptions,
): MlbMarketDecision {
  const common = commonDecision(matchup, "total", bookKey, options);
  const quote = bookKey ? matchup.oddsBooks?.[bookKey] : null;
  const line = finiteNumber(quote?.total_line);
  const overPrice = quote?.over;
  const underPrice = quote?.under;
  const validPair = line != null
    && Number.isInteger(line * 2)
    && isValidAmericanPrice(overPrice)
    && isValidAmericanPrice(underPrice);
  const bookmakerUpdatedAt = typeof quote?.last_update === "string" ? quote.last_update : null;
  const health = baseBlockers(matchup, "total", options, bookKey, bookmakerUpdatedAt);
  const blockers = [...health.blockers];
  if (!validPair) blockers.push("exact paired total line and prices are missing or invalid");
  if (matchup.totalPredictionSnapshotId == null) blockers.push("total prediction snapshot identity is missing");
  if (matchup.totalReferenceOddsSnapshotId == null) blockers.push("total prediction reference-quote identity is missing");
  if (!hasSourceAwareMissingness(matchup.totalRunConfig)) {
    blockers.push("prediction predates source-aware feature tracking");
  }
  blockers.push(...missingSourceInputs(matchup.totalMissingness));
  const modelTotal = finiteNumber(matchup.totalPrediction);
  if (modelTotal == null) blockers.push("predicted game total is missing or invalid");
  const canonicalHorizon = configString(matchup.totalRunConfig, "canonical_horizon");
  if (!canonicalHorizon) blockers.push("canonical prediction horizon is missing");
  const hasDistributionMethod = Boolean(configString(matchup.totalRunConfig, "distribution_method"));
  const distribution = hasDistributionMethod ? parseTotalDistribution(matchup.totalFeatureValues) : null;
  if (!distribution) blockers.push("calibrated win/push/loss distribution is unavailable");
  if (distribution && line != null && Math.abs(distribution.line - line) > 1e-9) {
    blockers.push(`total distribution is for ${distribution.line}, not the offered ${line}`);
  }
  if (distribution && distribution.resamples.length === 0) {
    blockers.push("total model-stability resamples are unavailable");
  }

  let side: "over" | "under" | null = null;
  let selection: string | null = null;
  let price: number | null = null;
  let pairedPrice: number | null = null;
  let modelProbability: number | null = null;
  let estimatedRoi: number | null = null;
  let targetDecimalPrice: number | null = null;
  let targetAmericanPrice: number | null = null;
  let resamplePositiveRate: number | null = null;
  let uncertaintyLow: number | null = null;
  let uncertaintyHigh: number | null = null;

  if (validPair && distribution && line != null && Math.abs(distribution.line - line) <= 1e-9) {
    const overDecimal = americanToDecimal(overPrice);
    const underDecimal = americanToDecimal(underPrice);
    const overRoi = distribution.pOver * (overDecimal - 1) - distribution.pUnder;
    const underRoi = distribution.pUnder * (underDecimal - 1) - distribution.pOver;
    side = overRoi >= underRoi ? "over" : "under";
    selection = `${side === "over" ? "Over" : "Under"} ${line}`;
    price = side === "over" ? overPrice : underPrice;
    pairedPrice = side === "over" ? underPrice : overPrice;
    const pWin = side === "over" ? distribution.pOver : distribution.pUnder;
    const pLoss = side === "over" ? distribution.pUnder : distribution.pOver;
    modelProbability = pWin;
    const decimal = americanToDecimal(price);
    estimatedRoi = pWin * (decimal - 1) - pLoss;
    const sampleWins = distribution.resamples.map((sample) => side === "over" ? sample.pOver : sample.pUnder);
    const sampleLosses = distribution.resamples.map((sample) => side === "over" ? sample.pUnder : sample.pOver);
    const sampleRois = sampleWins.map((p, index) => p * (decimal - 1) - sampleLosses[index]);
    resamplePositiveRate = sampleRois.filter((roi) => roi > 0).length / sampleRois.length;
    uncertaintyLow = percentile(sampleWins, 0.1);
    uncertaintyHigh = percentile(sampleWins, 0.9);
    targetDecimalPrice = 1 + (MLB_MIN_MEAN_ROI + pLoss) / pWin;
    targetAmericanPrice = minimumAmericanPrice(targetDecimalPrice);
  } else if (modelTotal != null && line != null) {
    side = modelTotal >= line ? "over" : "under";
    selection = `${side === "over" ? "Over" : "Under"} ${line}`;
    if (validPair) {
      price = side === "over" ? overPrice : underPrice;
      pairedPrice = side === "over" ? underPrice : overPrice;
    }
  }

  const decimalPrice = price != null ? americanToDecimal(price) : null;
  const offeredBreakEven = decimalPrice == null ? null : 1 / decimalPrice;
  const priceDistance = decimalPrice != null && targetDecimalPrice != null
    ? (targetDecimalPrice - decimalPrice) / targetDecimalPrice
    : null;
  const priceSupport: MlbPriceSupport = targetDecimalPrice == null || decimalPrice == null
    ? "unavailable"
    : decimalPrice + 1e-12 >= targetDecimalPrice
      ? "passes"
      : priceDistance != null && priceDistance > 0 && priceDistance <= MLB_WATCH_PRICE_DISTANCE
        ? "near_target"
        : "too_expensive";
  const requiresRecalculation = matchup.oddsSnapshotId != null
    && matchup.totalReferenceOddsSnapshotId != null
    && matchup.oddsSnapshotId !== matchup.totalReferenceOddsSnapshotId;
  const status = classifyStatus({
    matchup,
    market: "total",
    options,
    blockers: [...new Set(blockers)],
    selection,
    bookLabel: common.bookLabel,
    price,
    estimatedRoi,
    priceSupport,
    resamplePositiveRate,
    requiresRecalculation,
    canonicalHorizon,
    modelVersion: matchup.totalModelVersion,
  });
  const relationship: MlbDecisionRelationship = modelTotal == null || line == null || Math.abs(modelTotal - line) < 0.1
    ? "at_market"
    : modelTotal > line
      ? "model_above_line"
      : "model_below_line";
  const reasons = contributionReasons(matchup.totalFeatureValues);
  if (modelTotal != null && line != null) {
    reasons.unshift({
      label: relationshipLabel(relationship),
      detail: `Model mean ${modelTotal.toFixed(1)} vs exact ${common.bookLabel ?? "book"} line ${line.toFixed(1)}`,
      source: "Model snapshot",
      direction: "neutral",
    });
  }
  const blockerList = [...new Set(blockers)];
  const fragilityReasons = [
    ...blockerList.slice(0, 3),
    ...(requiresRecalculation ? ["Current quote is newer than the model's market input"] : []),
  ];
  return {
    ...common,
    ...status,
    selection,
    side,
    line,
    modelProbability,
    probabilityKind: distribution ? "calibrated" : "unavailable",
    modelTotal,
    referenceProbability: null,
    referenceLine: matchup.totalReferenceMarketLine ?? null,
    offeredBreakEven,
    priceMargin: modelProbability != null && offeredBreakEven != null ? modelProbability - offeredBreakEven : null,
    estimatedRoi,
    targetDecimalPrice,
    targetAmericanPrice,
    resamplePositiveRate,
    uncertaintyLow,
    uncertaintyHigh,
    relationship,
    relationshipLabel: relationshipLabel(relationship),
    priceSupport,
    completeness: blockerList.some((reason) => reason.includes("feature") || reason.includes("prediction") || reason.includes("distribution")) ? "required_missing" : "complete",
    fragility: decisionFragility(status.primaryStatus, fragilityReasons),
    fragilityReasons,
    price,
    pairedPrice,
    decimalPrice,
    bookmakerUpdatedAt,
    quoteAgeMinutes: health.quoteAgeMinutes,
    validUntil: health.validUntil,
    reasons,
    blockers: blockerList,
    missingInformation: blockerList,
    parlayEligible: false,
  };
}

export function buildMoneylineDecisions(
  matchup: MlbDecisionMatchup,
  options: BuildMlbDecisionOptions,
): MlbMarketDecision[] {
  return bookKeys(matchup.oddsBooks).map((key) => buildMoneylineDecisionForBook(matchup, key, options));
}

export function buildTotalDecisions(
  matchup: MlbDecisionMatchup,
  options: BuildMlbDecisionOptions,
): MlbMarketDecision[] {
  return bookKeys(matchup.oddsBooks).map((key) => buildTotalDecisionForBook(matchup, key, options));
}

export function buildMlbDecisionBoard(
  matchups: MlbDecisionMatchup[],
  options: BuildMlbDecisionOptions,
): MlbMarketDecision[] {
  return matchups.flatMap((matchup) => [
    ...buildMoneylineDecisions(matchup, options),
    ...buildTotalDecisions(matchup, options),
  ]);
}
