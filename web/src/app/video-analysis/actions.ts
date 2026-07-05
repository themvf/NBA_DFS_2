"use server";

/**
 * Server actions for the Video Analysis feature (2026-07-05).
 *
 * analyzeVideo — paste a YouTube URL, fetch its transcript, ask DeepSeek for
 *   a structured per-team/per-player breakdown. Sport-agnostic by design.
 * getRecentVideoAnalyses — list of previously analyzed videos for the page.
 *
 * Results are cached by video_id (UNIQUE) -- re-analyzing the same video
 * returns the stored result instead of re-fetching/re-calling the LLM,
 * unless forceRefresh is passed.
 */

import { db } from "@/db";
import { ensureVideoAnalysisTables } from "@/db/ensure-schema";
import { videoAnalysis } from "@/db/schema";
import { eq, desc } from "drizzle-orm";
import {
  extractVideoId,
  fetchTranscript,
  formatTimestamp,
  TranscriptUnavailableError,
} from "@/lib/youtube-transcript";

const MODEL_VERSION = "video-analysis-deepseek-v1";
const DEEPSEEK_API_URL = "https://api.deepseek.com/chat/completions";
const DEEPSEEK_MODEL = "deepseek-chat";

// Conservative cap so very long videos (multi-hour podcasts) don't blow the
// model's context window. ~4 chars/token, so this leaves plenty of headroom
// under a 64K-token context. Longer videos get analyzed on a truncated
// transcript -- documented, not hidden.
const MAX_TRANSCRIPT_CHARS = 60_000;

export interface VideoAnalysisSubject {
  name: string;
  type: "player" | "team";
  sport: string | null;
  summary: string;
  timestamp: string | null;
}

export interface VideoAnalysisResult {
  videoSummary: string;
  subjects: VideoAnalysisSubject[];
}

const SYSTEM_PROMPT = `You are a sports video analysis tool. You are given a transcript of a \
YouTube video with approximate timestamps, and asked to identify every distinct team and \
player discussed. This works across any sport (NBA, MLB, NFL, NHL, soccer, tennis, etc.) -- \
do not assume a single sport.

For each team or player discussed:
- name: the team or player's name as stated in the transcript
- type: "player" or "team"
- sport: your best guess (e.g. "nba", "mlb", "nfl", "nhl", "soccer", "tennis"), or null if unclear
- summary: 1-3 sentences, in your own words, of what was actually said about them (analysis, \
  predictions, injury/lineup notes, performance commentary, etc.)
- timestamp: the approximate "M:SS" or "MM:SS" timestamp of their first substantive mention, \
  taken from the transcript's own timestamps -- do not invent a timestamp not grounded in the \
  transcript

Only include a subject if the video meaningfully discusses them -- a passing one-word mention \
does not qualify. Do not fabricate opinions or facts not present in the transcript.

Respond with ONLY a JSON object in this exact shape, no other text:
{"videoSummary": "2-3 sentence overall summary of the video", "subjects": [{"name": "...", \
"type": "player"|"team", "sport": "..."|null, "summary": "...", "timestamp": "M:SS"|null}]}`;

async function callDeepSeek(transcriptWithTimestamps: string): Promise<VideoAnalysisResult> {
  const apiKey = process.env.DEEPSEEK_API_KEY;
  if (!apiKey) {
    throw new Error("DEEPSEEK_API_KEY not set in Vercel env vars");
  }

  const resp = await fetch(DEEPSEEK_API_URL, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      model: DEEPSEEK_MODEL,
      messages: [
        { role: "system", content: SYSTEM_PROMPT },
        { role: "user", content: `Transcript:\n\n${transcriptWithTimestamps}` },
      ],
      temperature: 0.2,
      response_format: { type: "json_object" },
    }),
  });
  if (!resp.ok) {
    throw new Error(`DeepSeek call failed (HTTP ${resp.status})`);
  }
  const data = await resp.json();
  const content = data?.choices?.[0]?.message?.content;
  if (!content) throw new Error("DeepSeek returned no content");

  const parsed = JSON.parse(content);
  const subjects: VideoAnalysisSubject[] = Array.isArray(parsed.subjects)
    ? parsed.subjects
        .filter((s: unknown): s is Record<string, unknown> => typeof s === "object" && s !== null)
        .map((s: Record<string, unknown>) => ({
          name: String(s.name ?? "Unknown"),
          type: s.type === "team" ? "team" : "player",
          sport: typeof s.sport === "string" ? s.sport : null,
          summary: String(s.summary ?? ""),
          timestamp: typeof s.timestamp === "string" ? s.timestamp : null,
        }))
    : [];

  return {
    videoSummary: typeof parsed.videoSummary === "string" ? parsed.videoSummary : "",
    subjects,
  };
}

export async function analyzeVideo(
  urlOrId: string,
  forceRefresh = false,
): Promise<{ ok: boolean; message: string; result?: VideoAnalysisResult & { title: string | null; videoId: string; cached: boolean } }> {
  await ensureVideoAnalysisTables();

  const videoId = extractVideoId(urlOrId);
  if (!videoId) {
    return { ok: false, message: "Could not parse a YouTube video ID from that URL." };
  }

  if (!forceRefresh) {
    const [existing] = await db
      .select()
      .from(videoAnalysis)
      .where(eq(videoAnalysis.videoId, videoId))
      .limit(1);
    if (existing) {
      const analysis = existing.analysisJson as VideoAnalysisResult;
      return {
        ok: true,
        message: "Loaded from cache.",
        result: { ...analysis, title: existing.title, videoId, cached: true },
      };
    }
  }

  let transcript;
  try {
    transcript = await fetchTranscript(videoId);
  } catch (err) {
    if (err instanceof TranscriptUnavailableError) {
      return { ok: false, message: err.message };
    }
    return { ok: false, message: `Failed to fetch transcript: ${(err as Error).message}` };
  }

  const fullText = transcript.segments
    .map((s) => `[${formatTimestamp(s.start)}] ${s.text}`)
    .join(" ");
  const truncated = fullText.length > MAX_TRANSCRIPT_CHARS;
  const transcriptForModel = truncated ? fullText.slice(0, MAX_TRANSCRIPT_CHARS) : fullText;

  let result: VideoAnalysisResult;
  try {
    result = await callDeepSeek(transcriptForModel);
  } catch (err) {
    return { ok: false, message: `Analysis failed: ${(err as Error).message}` };
  }

  await db
    .insert(videoAnalysis)
    .values({
      videoUrl: urlOrId,
      videoId,
      title: transcript.title,
      channelName: transcript.channelName,
      transcriptText: fullText,
      analysisJson: result,
      modelVersion: MODEL_VERSION,
    })
    .onConflictDoUpdate({
      target: videoAnalysis.videoId,
      set: {
        videoUrl: urlOrId,
        title: transcript.title,
        channelName: transcript.channelName,
        transcriptText: fullText,
        analysisJson: result,
        modelVersion: MODEL_VERSION,
        createdAt: new Date(),
      },
    });

  return {
    ok: true,
    message: truncated
      ? "Analyzed (transcript was long and truncated for analysis)."
      : "Analyzed.",
    result: { ...result, title: transcript.title, videoId, cached: false },
  };
}

export interface RecentVideoAnalysis {
  videoId: string;
  videoUrl: string;
  title: string | null;
  channelName: string | null;
  createdAt: Date | null;
  subjectCount: number;
}

export async function getRecentVideoAnalyses(limit = 20): Promise<RecentVideoAnalysis[]> {
  await ensureVideoAnalysisTables();
  const rows = await db
    .select()
    .from(videoAnalysis)
    .orderBy(desc(videoAnalysis.createdAt))
    .limit(limit);
  return rows.map((r) => ({
    videoId: r.videoId,
    videoUrl: r.videoUrl,
    title: r.title,
    channelName: r.channelName,
    createdAt: r.createdAt,
    subjectCount: Array.isArray((r.analysisJson as VideoAnalysisResult)?.subjects)
      ? (r.analysisJson as VideoAnalysisResult).subjects.length
      : 0,
  }));
}
