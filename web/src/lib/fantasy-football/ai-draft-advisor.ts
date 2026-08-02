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

export const BEST_BALL_ADVISOR_PROJECTION_MODEL = "ff-independent-v1.4";
export const BEST_BALL_ADVISOR_CANDIDATE_LIMIT = 40;

export type BestBallAdvisorProvider = "openai" | "deepseek";

export type BestBallAdvisorRequest = {
  provider: BestBallAdvisorProvider;
  rankingSetId: number;
  userSlot: number;
  playerIds: number[];
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
  candidates: BestBallAdvisorCandidate[];
  instructions: string[];
};

export type BestBallAdvisorModelOutput = {
  recommendedPlayerId: number;
  confidence: number;
  whyNow: string;
  rosterFit: string;
  evidence: string[];
  risks: string[];
  alternatives: Array<{ playerId: number; reason: string }>;
  strategyUntilNextTurn: string;
  whatWouldChange: string;
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
  whyNow: string;
  rosterFit: string;
  evidence: string[];
  risks: string[];
  strategyUntilNextTurn: string;
  whatWouldChange: string;
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
  };
}

export function bestBallAdvisorDraftSignature(input: Pick<BestBallAdvisorRequest, "rankingSetId" | "userSlot" | "playerIds">): string {
  return `${input.rankingSetId}:${input.userSlot}:${input.playerIds.join(",")}`;
}

export function buildBestBallAdvisorProviderSnapshot(snapshot: BestBallAdvisorSnapshot): unknown {
  return JSON.parse(JSON.stringify(snapshot, (key, value) => (
    key === "playerId" || key === "playerIds" ? undefined : value
  )));
}

export function buildBestBallAdvisorSnapshot(
  rankings: FantasyRankingRow[],
  input: Pick<BestBallAdvisorRequest, "rankingSetId" | "userSlot" | "playerIds">,
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
  const candidates = rankings
    .filter((player) => !draftedSet.has(player.playerId) && canAddBestBallPlayer(userRosterRows, player))
    .slice(0, BEST_BALL_ADVISOR_CANDIDATE_LIMIT)
    .map((player, index) => ({
      ...advisorCandidate(player),
      candidateKey: `C${String(index + 1).padStart(2, "0")}`,
    }));

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
    candidates,
    instructions: [
      "Recommend for the user's target pick, not for the team currently on the clock.",
      "Balance best available value with roster construction; do not force immediate bye-week backup when later value is likely.",
      "Prioritize spike-week upside and paths to a weekly starting slot, but never invent air-yard, injury, role, matchup, or correlation evidence absent from this snapshot.",
      "Use ADP to judge whether a candidate is likely to survive until the user's following pick.",
      "The active point projection is V1.4. V2 opportunity and weekly-distribution modeling is not active.",
      "Select only candidateKey values contained in candidates. Candidate keys are the sole selection identifiers.",
    ],
  };
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

function candidateReference(value: Record<string, unknown>, keys: string[]): unknown {
  for (const key of keys) {
    if (value[key] !== undefined && value[key] !== null) return value[key];
  }
  return null;
}

function nestedRecommendation(value: Record<string, unknown>): Record<string, unknown> {
  const recommendation = value.recommendation;
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
      "recommendedPlayerName", "recommended_player_name", "recommendedPlayer", "recommended_player", "playerName", "player_name",
    ],
  );
  const recommendedCandidateKey = normalizedCandidateKey(recommendedReference, snapshot.candidates) ?? "";
  const recommendedCandidate = candidateByKey.get(recommendedCandidateKey);
  if (!recommendedCandidate) {
    const received = JSON.stringify(recommendedReference)?.slice(0, 120) ?? "missing";
    throw new Error(`The advisor recommended a player who is no longer legal or available (received ${received}).`);
  }
  const recommendedPlayerId = recommendedCandidate.playerId;
  const rawConfidence = Number(advisorField(value, ["confidence"]));
  if (!Number.isFinite(rawConfidence) || rawConfidence < 0 || rawConfidence > 100) throw new Error("The advisor returned invalid confidence.");
  const confidence = rawConfidence > 0 && rawConfidence <= 1 ? rawConfidence * 100 : rawConfidence;
  const rawAlternatives = advisorField(value, ["alternatives"]);
  if (!Array.isArray(rawAlternatives) || rawAlternatives.length !== 2) {
    throw new Error("The advisor must return exactly two alternatives.");
  }
  const seen = new Set([recommendedCandidateKey]);
  const alternatives = rawAlternatives.map((item) => {
    if (!item || typeof item !== "object" || Array.isArray(item)) throw new Error("The advisor returned an invalid alternative.");
    const alternative = item as Record<string, unknown>;
    const candidateKey = normalizedCandidateKey(
      candidateReference(alternative, ["candidateKey", "candidate_key", "playerName", "player_name", "player"]),
      snapshot.candidates,
    ) ?? "";
    const candidate = candidateByKey.get(candidateKey);
    if (!candidate || seen.has(candidateKey)) {
      throw new Error("The advisor returned a duplicate, unavailable, or illegal alternative.");
    }
    seen.add(candidateKey);
    return { playerId: candidate.playerId, reason: requireString(alternative.reason, "alternative reason") };
  });
  return {
    recommendedPlayerId,
    confidence: Math.round(confidence),
    whyNow: requireString(advisorField(value, ["whyNow", "why_now"]), "why-now explanation"),
    rosterFit: requireString(advisorField(value, ["rosterFit", "roster_fit"]), "roster-fit explanation"),
    evidence: requireStringArray(advisorField(value, ["evidence"]), "evidence", 2, 5),
    risks: requireStringArray(advisorField(value, ["risks", "risk"]), "risk", 1, 4),
    alternatives,
    strategyUntilNextTurn: requireString(advisorField(value, ["strategyUntilNextTurn", "strategy_until_next_turn"]), "next-turn strategy"),
    whatWouldChange: requireString(advisorField(value, ["whatWouldChange", "what_would_change"]), "change condition"),
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
    whyNow: output.whyNow,
    rosterFit: output.rosterFit,
    evidence: output.evidence,
    risks: output.risks,
    strategyUntilNextTurn: output.strategyUntilNextTurn,
    whatWouldChange: output.whatWouldChange,
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
