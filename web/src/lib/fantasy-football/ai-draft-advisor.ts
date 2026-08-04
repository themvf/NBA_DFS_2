import type { FantasyRankingRow } from "@/db/queries-fantasy-football";
import {
  BEST_BALL_POSITIONS,
  BEST_BALL_ROUNDS,
  BEST_BALL_TEAM_COUNT,
  canAddBestBallPlayer,
  getBestBallRosterStatus,
  type BestBallPosition,
} from "./best-ball";
import { buildSnakeSlots, nextControlledPick } from "./draft-engine";

export const BEST_BALL_ADVISOR_PROJECTION_MODEL = "ff-independent-v1.6";
export const BEST_BALL_ADVISOR_CANDIDATE_LIMIT = 40;
// News search only covers players actually under consideration for this pick, not
// the full 40-candidate board -- keeps latency/cost bounded (OpenAI-only feature;
// DeepSeek's API has no equivalent web-search tool).
export const BEST_BALL_ADVISOR_NEWS_CANDIDATE_LIMIT = 8;

export type BestBallAdvisorProvider = "openai" | "deepseek";

export type BestBallAdvisorRequest = {
  provider: BestBallAdvisorProvider;
  rankingSetId: number;
  userSlot: number;
  playerIds: number[];
  // OpenAI-only (validated server-side): search recent news for the top candidates
  // under consideration before answering. Slower and costlier, so it's a distinct
  // opt-in action rather than the default path.
  withNews?: boolean;
};

export type BestBallAdvisorCorrelationPair = {
  withPlayerId: number;
  withName: string;
  relationshipType: string;
  shrunkCorrelation: number;
  sampleWeeks: number;
};

// Duck-typed to match TeammateCorrelationRow in db/queries-fantasy-football.ts
// without this lib module importing the db layer.
export type BestBallAdvisorCorrelationInput = {
  playerAId: number;
  playerBId: number;
  relationshipType: string;
  sampleWeeks: number;
  shrunkCorrelation: number;
};

export type BestBallAdvisorCandidate = {
  candidateKey: string | null;
  playerId: number;
  name: string;
  position: string;
  team: string | null;
  byeWeek: number | null;
  ourRank: number | null;
  positionRank: number | null;
  adp: number | null;
  ourProjectedPoints: number | null;
  fantasyProsProjectedPoints: number | null;
  games2025: number | null;
  fantasyPoints2025: number | null;
  confidence: number | null;
  signals: string[];
  projectionDetails: Record<string, unknown> | null;
  correlationsWithRoster: BestBallAdvisorCorrelationPair[];
};

export type BestBallAdvisorSnapshot = {
  contractVersion: "best-ball-advisor-v1";
  projectionModel: typeof BEST_BALL_ADVISOR_PROJECTION_MODEL;
  rankingSetId: number;
  rules: {
    teams: 12;
    rounds: 20;
    rosterSize: 20;
    positions: readonly ["QB", "RB", "WR", "TE"];
    weeklyLineup: { QB: 1; RB: 2; WR: 3; TE: 1; FLEX: 1 };
    scoring: string[];
    tournament: string[];
  };
  draft: {
    userSlot: number;
    currentOverallPick: number | null;
    currentRound: number | null;
    currentTeamSlot: number | null;
    targetOverallPick: number | null;
    picksUntilUser: number | null;
    isUserOnClock: boolean;
    draftedCount: number;
    completed: boolean;
  };
  draftedPicks: Array<{
    overallPick: number;
    round: number;
    teamSlot: number;
    playerId: number;
    name: string;
    position: string;
    nflTeam: string | null;
  }>;
  rosters: Array<{
    teamSlot: number;
    isUser: boolean;
    playerIds: number[];
    counts: Record<BestBallPosition, number>;
  }>;
  userRoster: BestBallAdvisorCandidate[];
  userByeWeeks: Partial<Record<BestBallPosition, number[]>>;
  rosterTeamConcentration: Record<string, number>;
  candidates: BestBallAdvisorCandidate[];
  instructions: string[];
};

export type BestBallAdvisorNewsSource = {
  player: string;
  url: string;
  title: string;
  publishedAt: string | null;
  summary: string;
};

export type BestBallAdvisorModelOutput = {
  recommendedPlayerId: number;
  confidence: number;
  confidenceProvided: boolean;
  whyNow: string;
  rosterFit: string;
  evidence: string[];
  risks: string[];
  alternatives: Array<{ playerId: number; reason: string }>;
  strategyUntilNextTurn: string;
  whatWouldChange: string;
  newsSources: BestBallAdvisorNewsSource[];
};

export type BestBallAdvisorCorrection = {
  validationError: string;
  previousOutput: unknown;
};

export type BestBallAdvisorPick = BestBallAdvisorCandidate & { reason?: string };

export type BestBallAdvisorResult = {
  provider: BestBallAdvisorProvider;
  providerLabel: "OpenAI" | "DeepSeek";
  model: "gpt-5.6-luna" | "deepseek-v4-flash";
  projectionModel: typeof BEST_BALL_ADVISOR_PROJECTION_MODEL;
  requestHash: string;
  generatedAt: string;
  currentOverallPick: number | null;
  targetOverallPick: number | null;
  draftedCount: number;
  recommendation: BestBallAdvisorPick;
  alternatives: BestBallAdvisorPick[];
  confidence: number;
  confidenceProvided: boolean;
  whyNow: string;
  rosterFit: string;
  evidence: string[];
  risks: string[];
  strategyUntilNextTurn: string;
  whatWouldChange: string;
  newsSources: BestBallAdvisorNewsSource[];
};

const DRAFT_SLOTS = buildSnakeSlots(BEST_BALL_TEAM_COUNT, BEST_BALL_ROUNDS);

function finiteOrNull(value: number | null): number | null {
  return typeof value === "number" && Number.isFinite(value) ? Number(value.toFixed(2)) : null;
}

function advisorCandidate(player: FantasyRankingRow): BestBallAdvisorCandidate {
  return {
    candidateKey: null,
    playerId: player.playerId,
    name: player.name,
    position: player.position,
    team: player.team,
    byeWeek: player.byeWeek,
    ourRank: player.ourRank,
    positionRank: player.positionRank,
    adp: finiteOrNull(player.adp),
    ourProjectedPoints: finiteOrNull(player.ourProjectedPoints),
    fantasyProsProjectedPoints: finiteOrNull(player.fantasyProsProjectedPoints),
    games2025: player.games2025,
    fantasyPoints2025: finiteOrNull(player.fantasyPoints2025),
    confidence: finiteOrNull(player.confidence),
    signals: player.indicators.slice(0, 8).map((indicator) => indicator.label),
    projectionDetails: player.projectionDetails,
    correlationsWithRoster: [],
  };
}

export function bestBallAdvisorDraftSignature(input: Pick<BestBallAdvisorRequest, "rankingSetId" | "userSlot" | "playerIds">): string {
  return `${input.rankingSetId}:${input.userSlot}:${input.playerIds.join(",")}`;
}

// The candidates actually worth searching news for -- top of the board, sorted by
// ourRank ascending. snapshot.candidates is already ranked-order by construction.
export function bestBallAdvisorNewsCandidates(snapshot: BestBallAdvisorSnapshot): BestBallAdvisorCandidate[] {
  return snapshot.candidates.slice(0, BEST_BALL_ADVISOR_NEWS_CANDIDATE_LIMIT);
}

export function buildBestBallAdvisorProviderSnapshot(snapshot: BestBallAdvisorSnapshot): unknown {
  return JSON.parse(JSON.stringify(snapshot, (key, value) => (
    key === "playerId" || key === "playerIds" ? undefined : value
  )));
}

export function buildBestBallAdvisorSnapshot(
  rankings: FantasyRankingRow[],
  input: Pick<BestBallAdvisorRequest, "rankingSetId" | "userSlot" | "playerIds">,
  correlations: BestBallAdvisorCorrelationInput[] = [],
): BestBallAdvisorSnapshot {
  if (!Number.isInteger(input.rankingSetId) || input.rankingSetId <= 0) throw new Error("The ranking snapshot is invalid.");
  if (!Number.isInteger(input.userSlot) || input.userSlot < 1 || input.userSlot > BEST_BALL_TEAM_COUNT) {
    throw new Error("The user's draft slot must be between 1 and 12.");
  }
  if (!Array.isArray(input.playerIds) || input.playerIds.length > DRAFT_SLOTS.length) throw new Error("The draft history is invalid.");
  if (new Set(input.playerIds).size !== input.playerIds.length || input.playerIds.some((id) => !Number.isInteger(id) || id <= 0)) {
    throw new Error("The draft history contains an invalid or duplicate player.");
  }

  const playerById = new Map(rankings.map((player) => [player.playerId, player]));
  const unknownId = input.playerIds.find((id) => !playerById.has(id));
  if (unknownId) throw new Error(`Drafted player ${unknownId} is not in this ranking snapshot.`);

  const draftedSet = new Set(input.playerIds);
  const rosterMap = new Map<number, FantasyRankingRow[]>(
    Array.from({ length: BEST_BALL_TEAM_COUNT }, (_, index) => [index + 1, []]),
  );
  const draftedPicks = input.playerIds.map((playerId, index) => {
    const slot = DRAFT_SLOTS[index];
    const player = playerById.get(playerId)!;
    rosterMap.get(slot.teamSlot)!.push(player);
    return {
      overallPick: slot.overallPick,
      round: slot.round,
      teamSlot: slot.teamSlot,
      playerId,
      name: player.name,
      position: player.position,
      nflTeam: player.team,
    };
  });

  const currentSlot = DRAFT_SLOTS[input.playerIds.length] ?? null;
  const currentOverallPick = currentSlot?.overallPick ?? null;
  const targetOverallPick = currentOverallPick === null
    ? null
    : nextControlledPick(currentOverallPick, input.userSlot, BEST_BALL_TEAM_COUNT, BEST_BALL_ROUNDS);
  const userRosterRows = rosterMap.get(input.userSlot) ?? [];
  const rosterPlayerIds = new Set(userRosterRows.map((player) => player.playerId));
  const rosterNameById = new Map(userRosterRows.map((player) => [player.playerId, player.name]));
  const correlationByPair = new Map<string, BestBallAdvisorCorrelationInput>();
  for (const row of correlations) {
    correlationByPair.set(`${Math.min(row.playerAId, row.playerBId)}:${Math.max(row.playerAId, row.playerBId)}`, row);
  }
  const correlationsFor = (playerId: number): BestBallAdvisorCorrelationPair[] => {
    const pairs: BestBallAdvisorCorrelationPair[] = [];
    for (const rosterId of rosterPlayerIds) {
      const row = correlationByPair.get(`${Math.min(playerId, rosterId)}:${Math.max(playerId, rosterId)}`);
      if (row) {
        pairs.push({
          withPlayerId: rosterId,
          withName: rosterNameById.get(rosterId) ?? `Player ${rosterId}`,
          relationshipType: row.relationshipType,
          shrunkCorrelation: row.shrunkCorrelation,
          sampleWeeks: row.sampleWeeks,
        });
      }
    }
    return pairs;
  };
  const candidates = rankings
    .filter((player) => !draftedSet.has(player.playerId) && canAddBestBallPlayer(userRosterRows, player))
    .slice(0, BEST_BALL_ADVISOR_CANDIDATE_LIMIT)
    .map((player, index) => ({
      ...advisorCandidate(player),
      candidateKey: `C${String(index + 1).padStart(2, "0")}`,
      correlationsWithRoster: correlationsFor(player.playerId),
    }));

  const rosterTeamConcentration: Record<string, number> = {};
  for (const player of userRosterRows) {
    if (!player.team) continue;
    rosterTeamConcentration[player.team] = (rosterTeamConcentration[player.team] ?? 0) + 1;
  }

  const userByeWeeks: Partial<Record<BestBallPosition, number[]>> = {};
  for (const position of BEST_BALL_POSITIONS) {
    userByeWeeks[position] = userRosterRows
      .filter((player) => player.position === position && player.byeWeek !== null)
      .map((player) => player.byeWeek as number)
      .sort((a, b) => a - b);
  }

  return {
    contractVersion: "best-ball-advisor-v1",
    projectionModel: BEST_BALL_ADVISOR_PROJECTION_MODEL,
    rankingSetId: input.rankingSetId,
    rules: {
      teams: 12,
      rounds: 20,
      rosterSize: 20,
      positions: BEST_BALL_POSITIONS,
      weeklyLineup: { QB: 1, RB: 2, WR: 3, TE: 1, FLEX: 1 },
      scoring: [
        "Full PPR: reception +1",
        "Passing: 25 yards +1, touchdown +4, interception -1, 300-yard game +3",
        "Rushing: 10 yards +1, touchdown +6, 100-yard game +3",
        "Receiving: 10 yards +1, touchdown +6, 100-yard game +3",
        "Two-point conversion +2, lost fumble -1, return or offensive fumble-recovery touchdown +6",
        "DraftKings automatically counts the highest-scoring legal eight-player lineup each week",
      ],
      tournament: ["Weeks 1-14 opening round", "Week 15 round two", "Week 16 semifinal", "Week 17 final"],
    },
    draft: {
      userSlot: input.userSlot,
      currentOverallPick,
      currentRound: currentSlot?.round ?? null,
      currentTeamSlot: currentSlot?.teamSlot ?? null,
      targetOverallPick,
      picksUntilUser: currentOverallPick === null || targetOverallPick === null ? null : targetOverallPick - currentOverallPick,
      isUserOnClock: currentSlot?.teamSlot === input.userSlot,
      draftedCount: input.playerIds.length,
      completed: currentSlot === null,
    },
    draftedPicks,
    rosters: Array.from({ length: BEST_BALL_TEAM_COUNT }, (_, index) => {
      const teamSlot = index + 1;
      const roster = rosterMap.get(teamSlot) ?? [];
      return {
        teamSlot,
        isUser: teamSlot === input.userSlot,
        playerIds: roster.map((player) => player.playerId),
        counts: getBestBallRosterStatus(roster).counts,
      };
    }),
    userRoster: userRosterRows.map(advisorCandidate),
    userByeWeeks,
    rosterTeamConcentration,
    candidates,
    instructions: [
      "Recommend for the user's target pick, not for the team currently on the clock.",
      "Balance best available value with roster construction; do not force immediate bye-week backup when later value is likely.",
      "Prioritize spike-week upside and paths to a weekly starting slot, but never invent air-yard, injury, role, matchup, or correlation evidence absent from this snapshot.",
      "Use ADP to judge whether a candidate is likely to survive until the user's following pick.",
      "The active point projection is V1.6. V2 opportunity and weekly-distribution modeling is not active.",
      "Select only candidateKey values contained in candidates. Candidate keys are the sole selection identifiers.",
      "Correlation is a variance lever, not an expected-points lever: it does not change a player's projected points, only how the roster's weekly total moves together. rules.tournament shows two different phases with opposite needs -- Weeks 1-14 is cumulative accumulation against a large field (favor diversification: uncorrelated players give the weekly-best-lineup selector more independent chances to pop), while Weeks 15-17 are single-week knockout rounds (favor 1-2 deliberate correlated stacks for ceiling, since a shared QB+WR spike is how an outlier single-week score gets made).",
      "Use each candidate's correlationsWithRoster (shrunkCorrelation, already shrunk toward a league-wide prior by sample size -- do not treat a low-sample_weeks pair as equally reliable as a high one) to judge whether this pick would add a genuine stack, add unwanted concentration, or is unrelated to the current roster. Never invent a correlation value for a pair not present there.",
      "Use rosterTeamConcentration to flag when a pick would be the 3rd+ player from one NFL team -- note explicitly whether that concentration looks like an intentional stack or an accidental one.",
    ],
  };
}

// A DeepSeek response can include a key with an empty string value (observed live,
// e.g. strategyUntilNextTurn: "") -- typeof x === "string" is true for "" too, so a
// naive typeof check would forward it into requireString() and throw instead of
// falling back. Treat blank/whitespace-only strings the same as "field absent".
function optionalNonEmptyString(value: unknown): string | null {
  return typeof value === "string" && value.trim() ? value : null;
}

function requireString(value: unknown, field: string): string {
  if (typeof value !== "string" || !value.trim()) throw new Error(`The advisor response is missing ${field}.`);
  return value.trim().slice(0, 1_200);
}

function requireStringArray(value: unknown, field: string, min: number, max: number): string[] {
  const values = typeof value === "string" ? [value] : value;
  if (!Array.isArray(values)) throw new Error(`The advisor response has invalid ${field}.`);
  const result = values.map((item) => requireString(item, field)).slice(0, max);
  if (result.length < min) throw new Error(`The advisor response needs at least ${min} ${field} item(s).`);
  return result;
}

function normalizedCandidateKey(raw: unknown, candidates: BestBallAdvisorCandidate[]): string | null {
  if (Array.isArray(raw)) return normalizedCandidateKey(raw[0], candidates);
  if (raw && typeof raw === "object" && !Array.isArray(raw)) {
    return normalizedCandidateKey(candidateReference(raw as Record<string, unknown>, [
      "candidateKey", "candidate_key", "playerName", "player_name", "player", "name", "pick",
      "primary", "recommendation", "recommended", "choice", "selection", "candidate", "key",
    ]), candidates);
  }
  if (typeof raw === "number" && Number.isInteger(raw) && raw > 0) {
    return `C${String(raw).padStart(2, "0")}`;
  }
  if (typeof raw !== "string") return null;
  const trimmed = raw.trim();
  const direct = candidates.find((candidate) => candidate.candidateKey?.toUpperCase() === trimmed.toUpperCase());
  if (direct?.candidateKey) return direct.candidateKey;
  const embeddedKey = trimmed.match(/\bC0?(\d{1,2})\b/i);
  if (embeddedKey) return `C${embeddedKey[1].padStart(2, "0")}`;
  const normalizedName = trimmed.toLowerCase().replace(/[^a-z0-9]/g, "");
  const byName = candidates.find((candidate) => candidate.name.toLowerCase().replace(/[^a-z0-9]/g, "") === normalizedName);
  return byName?.candidateKey ?? null;
}

function collectCandidateKeys(raw: unknown, candidates: BestBallAdvisorCandidate[], depth = 0, found: string[] = []): string[] {
  if (depth > 5 || raw === null || raw === undefined) return found;
  const direct = normalizedCandidateKey(raw, candidates);
  if (direct && !found.includes(direct)) found.push(direct);
  if (Array.isArray(raw)) {
    for (const item of raw) collectCandidateKeys(item, candidates, depth + 1, found);
  } else if (typeof raw === "object") {
    const record = raw as Record<string, unknown>;
    const priorityKeys = ["winnerPick", "winner_pick", "primary", "recommendation", "recommended", "selection", "pick", "choice"];
    for (const key of priorityKeys) if (key in record) collectCandidateKeys(record[key], candidates, depth + 1, found);
    for (const [key, child] of Object.entries(record)) {
      if (!priorityKeys.includes(key)) collectCandidateKeys(child, candidates, depth + 1, found);
    }
  }
  return found;
}

function deepAdvisorField(raw: unknown, keys: string[], depth = 0): unknown {
  if (depth > 5 || !raw || typeof raw !== "object") return null;
  if (!Array.isArray(raw)) {
    const record = raw as Record<string, unknown>;
    const direct = candidateReference(record, keys);
    if (direct !== null) return direct;
    for (const child of Object.values(record)) {
      const nested = deepAdvisorField(child, keys, depth + 1);
      if (nested !== null) return nested;
    }
  } else {
    for (const child of raw) {
      const nested = deepAdvisorField(child, keys, depth + 1);
      if (nested !== null) return nested;
    }
  }
  return null;
}

function candidateReference(value: Record<string, unknown>, keys: string[]): unknown {
  for (const key of keys) {
    if (value[key] !== undefined && value[key] !== null) return value[key];
  }
  return null;
}

function nestedRecommendation(value: Record<string, unknown>): Record<string, unknown> {
  const recommendedPicks = value.recommendedPicks ?? value.recommended_picks;
  const firstRecommendedPick = Array.isArray(recommendedPicks) ? recommendedPicks[0] : null;
  const recommendation = value.recommendation ?? value.selection ?? value.winnerPick ?? value.winner_pick ?? firstRecommendedPick;
  return recommendation && typeof recommendation === "object" && !Array.isArray(recommendation)
    ? recommendation as Record<string, unknown>
    : {};
}

function advisorField(value: Record<string, unknown>, keys: string[]): unknown {
  return candidateReference(value, keys) ?? candidateReference(nestedRecommendation(value), keys);
}

export function validateBestBallAdvisorOutput(raw: unknown, snapshot: BestBallAdvisorSnapshot): BestBallAdvisorModelOutput {
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) throw new Error("The advisor returned an invalid response.");
  const value = raw as Record<string, unknown>;
  const candidateByKey = new Map(snapshot.candidates.map((candidate) => [candidate.candidateKey, candidate]));
  const recommendedReference = advisorField(
    value,
    [
      "recommendedCandidateKey", "recommended_candidate_key", "candidateKey", "candidate_key",
      "winnerPick", "winner_pick", "recommendedPlayerName", "recommended_player_name", "recommendedPlayer", "recommended_player", "playerName", "player_name", "pick", "selection", "recommendedPicks", "recommended_picks",
    ],
  );
  const collectedCandidateKeys = collectCandidateKeys(value, snapshot.candidates);
  const recommendedCandidateKey = normalizedCandidateKey(recommendedReference, snapshot.candidates) ?? collectedCandidateKeys[0] ?? "";
  const recommendedCandidate = candidateByKey.get(recommendedCandidateKey);
  if (!recommendedCandidate) {
    const received = JSON.stringify(recommendedReference)?.slice(0, 120) ?? "missing";
    const fields = Object.keys(value).slice(0, 12).join(", ") || "none";
    throw new Error(`The advisor recommended a player who is no longer legal or available (received ${received}; fields: ${fields}).`);
  }
  const recommendedPlayerId = recommendedCandidate.playerId;
  const confidenceValue = advisorField(value, ["confidence"]);
  const confidenceProvided = confidenceValue !== null && confidenceValue !== undefined;
  const rawConfidence = confidenceProvided ? Number(confidenceValue) : 0;
  if (!Number.isFinite(rawConfidence) || rawConfidence < 0 || rawConfidence > 100) throw new Error("The advisor returned invalid confidence.");
  const confidence = rawConfidence > 0 && rawConfidence <= 1 ? rawConfidence * 100 : rawConfidence;
  const suppliedAlternatives = advisorField(value, ["alternatives", "alternatePicks", "alternate_picks", "advisablePicks", "advisable_picks"]);
  const recommendedPicks = value.recommendedPicks ?? value.recommended_picks;
  const rawAlternatives = Array.isArray(suppliedAlternatives)
    ? suppliedAlternatives.slice(0, 2)
    : Array.isArray(recommendedPicks) && recommendedPicks.length >= 3
      ? recommendedPicks.slice(1, 3)
    : collectedCandidateKeys.filter((candidateKey) => candidateKey !== recommendedCandidateKey).length >= 2
      ? collectedCandidateKeys.filter((candidateKey) => candidateKey !== recommendedCandidateKey).slice(0, 2)
      : snapshot.candidates.filter((candidate) => candidate.candidateKey !== recommendedCandidateKey).slice(0, 2).map((candidate) => candidate.candidateKey);
  if (rawAlternatives.length !== 2) {
    throw new Error("The advisor must return exactly two alternatives.");
  }
  const seen = new Set([recommendedCandidateKey]);
  const alternatives = rawAlternatives.map((item) => {
    if (!item || (typeof item !== "object" && typeof item !== "string")) throw new Error("The advisor returned an invalid alternative.");
    const alternative = typeof item === "object" && !Array.isArray(item) ? item as Record<string, unknown> : {};
    const candidateKey = normalizedCandidateKey(
      typeof item === "string" ? item : candidateReference(alternative, ["candidateKey", "candidate_key", "playerName", "player_name", "player", "name", "pick"]),
      snapshot.candidates,
    ) ?? "";
    const candidate = candidateByKey.get(candidateKey);
    if (!candidate || seen.has(candidateKey)) {
      throw new Error("The advisor returned a duplicate, unavailable, or illegal alternative.");
    }
    seen.add(candidateKey);
    const reason = candidateReference(alternative, ["reason", "rationale", "why"]);
    return {
      playerId: candidate.playerId,
      reason: typeof reason === "string" && reason.trim()
        ? requireString(reason, "alternative reason")
        : "DeepSeek listed this player as an alternative.",
    };
  });
  const rationale = advisorField(value, ["whyNow", "why_now", "rationale", "reason", "analysis", "explanation"])
    ?? deepAdvisorField(value, ["whyNow", "why_now", "rationale", "reason", "analysis", "explanation"]);
  // News grounding is best-effort and additive -- a missing or malformed
  // newsSources entry never fails the whole recommendation (unlike whyNow).
  // "No recent news found" is a legitimate, honest result, not an error.
  const rawNewsSources = advisorField(value, ["newsSources", "news_sources", "news"]);
  const newsSources: BestBallAdvisorNewsSource[] = Array.isArray(rawNewsSources)
    ? rawNewsSources
      .filter((item): item is Record<string, unknown> => !!item && typeof item === "object" && !Array.isArray(item))
      .map((item) => ({
        player: optionalNonEmptyString(item.player ?? item.playerName ?? item.player_name) ?? "",
        url: optionalNonEmptyString(item.url ?? item.link) ?? "",
        title: optionalNonEmptyString(item.title ?? item.headline) ?? "",
        publishedAt: optionalNonEmptyString(item.publishedAt ?? item.published_at ?? item.date),
        summary: optionalNonEmptyString(item.summary ?? item.description) ?? "",
      }))
      .filter((item) => item.url && item.title)
      .slice(0, 10)
    : [];
  const suppliedEvidence = advisorField(value, ["evidence"]) ?? deepAdvisorField(value, ["evidence"]);
  const evidence = suppliedEvidence === null
    ? [
      `V1.6 projection: ${recommendedCandidate.ourProjectedPoints?.toFixed(1) ?? "not available"} PPR points.`,
      `Current ADP: ${recommendedCandidate.adp?.toFixed(1) ?? "not available"}; our rank: ${recommendedCandidate.ourRank ?? "not available"}.`,
    ]
    : requireStringArray(suppliedEvidence, "evidence", 2, 5);
  return {
    recommendedPlayerId,
    confidence: Math.round(confidence),
    confidenceProvided,
    whyNow: requireString(rationale, "why-now explanation"),
    rosterFit: optionalNonEmptyString(advisorField(value, ["rosterFit", "roster_fit"]))
      ? requireString(advisorField(value, ["rosterFit", "roster_fit"]), "roster-fit explanation")
      : "DeepSeek did not provide a separate roster-fit explanation.",
    evidence,
    risks: (advisorField(value, ["risks", "risk", "caveats"]) ?? deepAdvisorField(value, ["risks", "risk", "caveats"])) === null
      ? ["DeepSeek did not provide a separate risk statement."]
      : requireStringArray(advisorField(value, ["risks", "risk", "caveats"]) ?? deepAdvisorField(value, ["risks", "risk", "caveats"]), "risk", 1, 4),
    alternatives,
    strategyUntilNextTurn: optionalNonEmptyString(advisorField(value, ["strategyUntilNextTurn", "strategy_until_next_turn", "strategy"]))
      ? requireString(advisorField(value, ["strategyUntilNextTurn", "strategy_until_next_turn", "strategy"]), "next-turn strategy")
      : "Re-run DeepSeek after the next recorded pick so it receives the updated legal board.",
    whatWouldChange: optionalNonEmptyString(advisorField(value, ["whatWouldChange", "what_would_change"]))
      ? requireString(advisorField(value, ["whatWouldChange", "what_would_change"]), "change condition")
      : "A draft pick that removes this player or materially changes the available-player tier.",
    newsSources,
  };
}

export async function getValidatedBestBallAdvisorOutput(
  snapshot: BestBallAdvisorSnapshot,
  getOutput: (correction?: BestBallAdvisorCorrection) => Promise<unknown>,
): Promise<{ output: BestBallAdvisorModelOutput; retried: boolean }> {
  const first = await getOutput();
  try {
    return { output: validateBestBallAdvisorOutput(first, snapshot), retried: false };
  } catch (firstError) {
    const correction: BestBallAdvisorCorrection = {
      validationError: firstError instanceof Error ? firstError.message : "The first response failed validation.",
      previousOutput: first,
    };
    const second = await getOutput(correction);
    try {
      return { output: validateBestBallAdvisorOutput(second, snapshot), retried: true };
    } catch (secondError) {
      const reason = secondError instanceof Error ? secondError.message : "The corrected response was invalid.";
      throw new Error(`The advisor rechecked the live board but its response still failed validation: ${reason}`);
    }
  }
}

export function enrichBestBallAdvisorResult(
  output: BestBallAdvisorModelOutput,
  snapshot: BestBallAdvisorSnapshot,
): Omit<BestBallAdvisorResult, "provider" | "providerLabel" | "model" | "requestHash" | "generatedAt"> {
  const candidateById = new Map(snapshot.candidates.map((candidate) => [candidate.playerId, candidate]));
  return {
    projectionModel: snapshot.projectionModel,
    currentOverallPick: snapshot.draft.currentOverallPick,
    targetOverallPick: snapshot.draft.targetOverallPick,
    draftedCount: snapshot.draft.draftedCount,
    recommendation: candidateById.get(output.recommendedPlayerId)!,
    alternatives: output.alternatives.map((alternative) => ({
      ...candidateById.get(alternative.playerId)!,
      reason: alternative.reason,
    })),
    confidence: output.confidence,
    confidenceProvided: output.confidenceProvided,
    whyNow: output.whyNow,
    rosterFit: output.rosterFit,
    evidence: output.evidence,
    risks: output.risks,
    strategyUntilNextTurn: output.strategyUntilNextTurn,
    whatWouldChange: output.whatWouldChange,
    newsSources: output.newsSources,
  };
}

export const BEST_BALL_ADVISOR_JSON_SCHEMA = {
  type: "object",
  additionalProperties: false,
  properties: {
    recommendedCandidateKey: { type: "string" },
    confidence: { type: "number", minimum: 0, maximum: 100 },
    whyNow: { type: "string" },
    rosterFit: { type: "string" },
    evidence: { type: "array", items: { type: "string" }, minItems: 2, maxItems: 5 },
    risks: { type: "array", items: { type: "string" }, minItems: 1, maxItems: 4 },
    alternatives: {
      type: "array",
      minItems: 2,
      maxItems: 2,
      items: {
        type: "object",
        additionalProperties: false,
        properties: { candidateKey: { type: "string" }, reason: { type: "string" } },
        required: ["candidateKey", "reason"],
      },
    },
    strategyUntilNextTurn: { type: "string" },
    whatWouldChange: { type: "string" },
  },
  required: [
    "recommendedCandidateKey",
    "confidence",
    "whyNow",
    "rosterFit",
    "evidence",
    "risks",
    "alternatives",
    "strategyUntilNextTurn",
    "whatWouldChange",
  ],
} as const;

// News-augmented variant, used only for the OpenAI "with news" call. OpenAI's
// strict structured-output mode requires every schema property to be listed in
// required (no true optional fields), so this is a distinct schema rather than
// making newsSources optional on the base one -- the base (no-news) call should
// never be asked to produce a field it has no tool access to populate.
export const BEST_BALL_ADVISOR_JSON_SCHEMA_WITH_NEWS = {
  ...BEST_BALL_ADVISOR_JSON_SCHEMA,
  properties: {
    ...BEST_BALL_ADVISOR_JSON_SCHEMA.properties,
    newsSources: {
      type: "array",
      minItems: 0,
      maxItems: 10,
      items: {
        type: "object",
        additionalProperties: false,
        properties: {
          player: { type: "string" },
          url: { type: "string" },
          title: { type: "string" },
          publishedAt: { type: ["string", "null"] },
          summary: { type: "string" },
        },
        required: ["player", "url", "title", "publishedAt", "summary"],
      },
    },
  },
  required: [...BEST_BALL_ADVISOR_JSON_SCHEMA.required, "newsSources"],
} as const;
