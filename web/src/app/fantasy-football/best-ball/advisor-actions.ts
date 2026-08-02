"use server";

import { createHash } from "node:crypto";
import { getFantasyRankings } from "@/db/queries-fantasy-football";
import {
  buildBestBallAdvisorSnapshot,
  enrichBestBallAdvisorResult,
  validateBestBallAdvisorOutput,
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

const CACHE_TTL_MS = 5 * 60_000;
const RATE_WINDOW_MS = 60_000;
const RATE_LIMIT_PER_PROVIDER = 6;

type AdvisorActionResponse =
  | { ok: true; result: BestBallAdvisorResult }
  | { ok: false; message: string };

type CachedResult = { expiresAt: number; result: BestBallAdvisorResult };
const responseCache = new Map<string, CachedResult>();
const inFlight = new Map<string, Promise<BestBallAdvisorResult>>();
const rateBuckets = new Map<BestBallAdvisorProvider, number[]>();

function requestHash(snapshot: BestBallAdvisorSnapshot): string {
  return createHash("sha256").update(JSON.stringify(snapshot)).digest("hex").slice(0, 20);
}

function enforceRateLimit(provider: BestBallAdvisorProvider): void {
  const now = Date.now();
  const recent = (rateBuckets.get(provider) ?? []).filter((timestamp) => now - timestamp < RATE_WINDOW_MS);
  if (recent.length >= RATE_LIMIT_PER_PROVIDER) throw new Error(`Please wait a minute before asking ${provider === "openai" ? "OpenAI" : "DeepSeek"} again.`);
  recent.push(now);
  rateBuckets.set(provider, recent);
}

async function createRecommendation(
  provider: BestBallAdvisorProvider,
  snapshot: BestBallAdvisorSnapshot,
  hash: string,
): Promise<BestBallAdvisorResult> {
  enforceRateLimit(provider);
  const raw = await callBestBallAdvisorProvider(provider, snapshot);
  const output = validateBestBallAdvisorOutput(raw, snapshot);
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
  try {
    if (input.provider !== "openai" && input.provider !== "deepseek") return { ok: false, message: "Choose OpenAI or DeepSeek." };
    const rankings = (await getFantasyRankings(Number(input.rankingSetId)))
      .filter((player) => BEST_BALL_POSITIONS.includes(player.position as "QB" | "RB" | "WR" | "TE"))
      .slice(0, 260);
    if (!rankings.length) return { ok: false, message: "This ranking snapshot has no eligible Best Ball players." };
    const snapshot = buildBestBallAdvisorSnapshot(rankings, input);
    if (snapshot.draft.completed) return { ok: false, message: "The draft is complete." };
    if (snapshot.candidates.length < 3) return { ok: false, message: "Fewer than three legal candidates remain." };

    const hash = requestHash(snapshot);
    const cacheKey = `${input.provider}:${hash}`;
    const cached = responseCache.get(cacheKey);
    if (cached && cached.expiresAt > Date.now()) return { ok: true, result: cached.result };
    if (cached) responseCache.delete(cacheKey);

    let pending = inFlight.get(cacheKey);
    if (!pending) {
      pending = createRecommendation(input.provider, snapshot, hash);
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
      return { ok: false, message: "The provider took longer than 45 seconds. Try again." };
    }
    if (error instanceof SyntaxError) return { ok: false, message: "The provider returned malformed JSON. Try again." };
    return { ok: false, message: error instanceof Error ? error.message : "The advisor request failed." };
  }
}
