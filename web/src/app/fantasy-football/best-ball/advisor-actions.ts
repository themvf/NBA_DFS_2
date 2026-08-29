"use server";

import { createHash } from "node:crypto";
import { getFantasyRankings, getRankingSetSummary, getTeammateCorrelations } from "@/db/queries-fantasy-football";
import {
  buildBestBallAdvisorSnapshot,
  enrichBestBallAdvisorResult,
  getValidatedBestBallAdvisorOutput,
  type BestBallAdvisorProvider,
  type BestBallAdvisorRequest,
  type BestBallAdvisorResult,
  type BestBallAdvisorSnapshot,
} from "@/lib/fantasy-football/ai-draft-advisor";
import {
  callBestBallAdvisorProvider,
  DEEPSEEK_BEST_BALL_MODEL,
  OPENAI_BEST_BALL_MODEL,
} from "@/lib/fantasy-football/ai-draft-advisor-providers";
import { BEST_BALL_POSITIONS } from "@/lib/fantasy-football/best-ball";
import { requireProjectionModelVersion } from "@/lib/fantasy-football/projection-model";

const CACHE_TTL_MS = 5 * 60_000;
const RATE_WINDOW_MS = 60_000;
const RATE_LIMIT_PER_PROVIDER = 6;
// News search is a slower, costlier OpenAI-only path (web search + reasoning +
// structured output) -- throttled separately and more tightly than the base path.
const RATE_LIMIT_NEWS = 3;

type AdvisorActionResponse =
  | { ok: true; result: BestBallAdvisorResult }
  | { ok: false; message: string };

type CachedResult = { expiresAt: number; result: BestBallAdvisorResult };
const responseCache = new Map<string, CachedResult>();
const inFlight = new Map<string, Promise<BestBallAdvisorResult>>();
const rateBuckets = new Map<string, number[]>();

function requestHash(snapshot: BestBallAdvisorSnapshot): string {
  return createHash("sha256").update(JSON.stringify(snapshot)).digest("hex").slice(0, 20);
}

function enforceRateLimit(provider: BestBallAdvisorProvider, withNews: boolean): void {
  const key = withNews ? `${provider}:news` : provider;
  const limit = withNews ? RATE_LIMIT_NEWS : RATE_LIMIT_PER_PROVIDER;
  const now = Date.now();
  const recent = (rateBuckets.get(key) ?? []).filter((timestamp) => now - timestamp < RATE_WINDOW_MS);
  if (recent.length >= limit) {
    const label = provider === "openai" ? (withNews ? "OpenAI with news" : "OpenAI") : "DeepSeek";
    throw new Error(`Please wait a minute before asking ${label} again.`);
  }
  recent.push(now);
  rateBuckets.set(key, recent);
}

async function createRecommendation(
  provider: BestBallAdvisorProvider,
  snapshot: BestBallAdvisorSnapshot,
  hash: string,
  withNews: boolean,
): Promise<BestBallAdvisorResult> {
  enforceRateLimit(provider, withNews);
  const { output } = await getValidatedBestBallAdvisorOutput(
    snapshot,
    (correction) => callBestBallAdvisorProvider(provider, snapshot, correction, withNews),
  );
  return {
    provider,
    providerLabel: provider === "openai" ? "OpenAI" : "DeepSeek",
    model: provider === "openai" ? OPENAI_BEST_BALL_MODEL : DEEPSEEK_BEST_BALL_MODEL,
    requestHash: hash,
    generatedAt: new Date().toISOString(),
    ...enrichBestBallAdvisorResult(output, snapshot),
  };
}

export async function requestBestBallAdvice(input: BestBallAdvisorRequest): Promise<AdvisorActionResponse> {
  const withNews = input.withNews === true;
  try {
    if (input.provider !== "openai" && input.provider !== "deepseek") return { ok: false, message: "Choose OpenAI or DeepSeek." };
    if (withNews && input.provider !== "openai") return { ok: false, message: "News search is only available for OpenAI." };
    const rankingSet = await getRankingSetSummary(Number(input.rankingSetId));
    if (!rankingSet) return { ok: false, message: "This ranking snapshot no longer exists." };
    const projectionModel = requireProjectionModelVersion(rankingSet.modelVersion);
    const rankings = (await getFantasyRankings(Number(input.rankingSetId)))
      .filter((player) => BEST_BALL_POSITIONS.includes(player.position as "QB" | "RB" | "WR" | "TE"))
      .slice(0, 260);
    if (!rankings.length) return { ok: false, message: "This ranking snapshot has no eligible Best Ball players." };
    const correlations = await getTeammateCorrelations(rankings.map((player) => player.playerId));
    const snapshot = buildBestBallAdvisorSnapshot(rankings, input, projectionModel, correlations);
    if (snapshot.draft.completed) return { ok: false, message: "The draft is complete." };
    if (snapshot.candidates.length < 3) return { ok: false, message: "Fewer than three legal candidates remain." };

    const hash = requestHash(snapshot);
    const cacheKey = `${input.provider}:${withNews ? "news:" : ""}${hash}`;
    const cached = responseCache.get(cacheKey);
    if (cached && cached.expiresAt > Date.now()) return { ok: true, result: cached.result };
    if (cached) responseCache.delete(cacheKey);

    let pending = inFlight.get(cacheKey);
    if (!pending) {
      pending = createRecommendation(input.provider, snapshot, hash, withNews);
      inFlight.set(cacheKey, pending);
    }
    try {
      const result = await pending;
      responseCache.set(cacheKey, { result, expiresAt: Date.now() + CACHE_TTL_MS });
      return { ok: true, result };
    } finally {
      if (inFlight.get(cacheKey) === pending) inFlight.delete(cacheKey);
    }
  } catch (error) {
    if (error instanceof DOMException && error.name === "TimeoutError") {
      const seconds = withNews ? 90 : 45;
      return { ok: false, message: `The provider took longer than ${seconds} seconds. Try again.` };
    }
    if (error instanceof SyntaxError) return { ok: false, message: "The provider returned malformed JSON. Try again." };
    return { ok: false, message: error instanceof Error ? error.message : "The advisor request failed." };
  }
}
