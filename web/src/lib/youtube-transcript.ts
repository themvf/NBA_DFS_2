/**
 * Fetches a YouTube video's transcript without an API key.
 *
 * There is no official public API for downloading caption tracks (the Data
 * API v3 caption endpoints require OAuth as the video owner). This uses the
 * same technique as the widely-used youtube-transcript-api Python library:
 * impersonate YouTube's internal "innertube" ANDROID client, which returns
 * caption track URLs that work without the bot-detection "PO token" the web
 * client's caption URLs require. Verified working against a live video
 * before writing this (see CLAUDE.md "YouTube Video Analysis" section).
 *
 * This is unofficial and could break if YouTube changes the innertube
 * contract -- that risk is accepted and documented, not hidden.
 */

const WATCH_URL = "https://www.youtube.com/watch?v=";
const INNERTUBE_CONTEXT = { client: { clientName: "ANDROID", clientVersion: "20.10.38" } };
const USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36";

export interface TranscriptSegment {
  start: number;
  text: string;
}

export interface FetchedTranscript {
  segments: TranscriptSegment[];
  title: string | null;
  channelName: string | null;
}

export class TranscriptUnavailableError extends Error {}

/** Extracts an 11-char YouTube video ID from any common URL shape, or a bare ID. */
export function extractVideoId(input: string): string | null {
  const trimmed = input.trim();
  if (/^[a-zA-Z0-9_-]{11}$/.test(trimmed)) return trimmed;

  try {
    const url = new URL(trimmed);
    if (url.hostname === "youtu.be") {
      const id = url.pathname.slice(1);
      return /^[a-zA-Z0-9_-]{11}$/.test(id) ? id : null;
    }
    if (url.hostname.endsWith("youtube.com")) {
      const v = url.searchParams.get("v");
      if (v && /^[a-zA-Z0-9_-]{11}$/.test(v)) return v;
      const shortsMatch = url.pathname.match(/^\/(shorts|embed)\/([a-zA-Z0-9_-]{11})/);
      if (shortsMatch) return shortsMatch[2];
    }
  } catch {
    return null;
  }
  return null;
}

function decodeXmlEntities(s: string): string {
  return s
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&#(\d+);/g, (_, code) => String.fromCharCode(Number(code)));
}

function parseTimedText(xml: string): TranscriptSegment[] {
  const segments: TranscriptSegment[] = [];
  const re = /<p t="(\d+)"[^>]*>([\s\S]*?)<\/p>/g;
  let match: RegExpExecArray | null;
  while ((match = re.exec(xml)) !== null) {
    const startMs = Number(match[1]);
    const text = decodeXmlEntities(match[2].replace(/<[^>]+>/g, "")).replace(/\s+/g, " ").trim();
    if (text) segments.push({ start: startMs / 1000, text });
  }
  return segments;
}

export async function fetchTranscript(videoId: string): Promise<FetchedTranscript> {
  const htmlResp = await fetch(`${WATCH_URL}${videoId}`, {
    headers: { "User-Agent": USER_AGENT },
  });
  if (!htmlResp.ok) {
    throw new TranscriptUnavailableError(`Could not load video page (HTTP ${htmlResp.status})`);
  }
  const html = await htmlResp.text();

  const keyMatch = html.match(/"INNERTUBE_API_KEY":\s*"([a-zA-Z0-9_-]+)"/);
  if (!keyMatch) {
    throw new TranscriptUnavailableError("Could not find video (it may be private, deleted, or age-restricted)");
  }

  const titleMatch = html.match(/"title":"([^"]*)".*?"channelName":"([^"]*)"/) ||
                      html.match(/<meta name="title" content="([^"]*)"/);
  const title = titleMatch ? decodeXmlEntities(titleMatch[1]) : null;
  const channelNameMatch = html.match(/"author":"([^"]*)"/);
  const channelName = channelNameMatch ? decodeXmlEntities(channelNameMatch[1]) : null;

  const innertubeResp = await fetch(
    `https://www.youtube.com/youtubei/v1/player?key=${keyMatch[1]}`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ context: INNERTUBE_CONTEXT, videoId }),
    },
  );
  if (!innertubeResp.ok) {
    throw new TranscriptUnavailableError(`Transcript lookup failed (HTTP ${innertubeResp.status})`);
  }
  const data = await innertubeResp.json();

  const status = data?.playabilityStatus?.status;
  if (status && status !== "OK") {
    throw new TranscriptUnavailableError(
      `Video is not playable (${status}: ${data.playabilityStatus?.reason ?? "unknown reason"})`,
    );
  }

  const tracks = data?.captions?.playerCaptionsTracklistRenderer?.captionTracks;
  if (!tracks || tracks.length === 0) {
    throw new TranscriptUnavailableError("This video has no captions/transcript available");
  }

  const track =
    tracks.find((t: { languageCode?: string }) => t.languageCode === "en") ?? tracks[0];
  const transcriptResp = await fetch(track.baseUrl, { headers: { "User-Agent": USER_AGENT } });
  if (!transcriptResp.ok) {
    throw new TranscriptUnavailableError(`Could not download transcript (HTTP ${transcriptResp.status})`);
  }
  const xml = await transcriptResp.text();
  const segments = parseTimedText(xml);
  if (segments.length === 0) {
    throw new TranscriptUnavailableError("Transcript was empty after parsing");
  }

  return { segments, title, channelName };
}

export function formatTimestamp(seconds: number): string {
  const m = Math.floor(seconds / 60);
  const s = Math.floor(seconds % 60);
  return `${m}:${s.toString().padStart(2, "0")}`;
}
