"use server";

/**
 * Server actions for managing tracked YouTube picks channels.
 *
 * addYoutubeChannel resolves a channel @handle or URL to its canonical
 * channel_id (the same "read externalId off the channel page" technique
 * verified live before building web/src/lib/youtube-transcript.ts and
 * ingest/youtube_picks_videos.py) and registers it in
 * youtube_pick_channels. The Python ingest script picks up newly-added
 * channels on its next scheduled run -- this action only resolves and
 * registers, it never scrapes videos itself.
 */

import { db } from "@/db";
import { ensureYoutubePickChannelsTable } from "@/db/ensure-schema";
import { youtubePickChannels } from "@/db/schema";
import { desc } from "drizzle-orm";

const USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36";

/** Accepts "@handle", "youtube.com/@handle", or a full channel URL. */
function normalizeChannelUrl(input: string): string | null {
  const trimmed = input.trim();
  if (!trimmed) return null;
  if (trimmed.startsWith("@")) return `https://www.youtube.com/${trimmed}`;
  if (/^[A-Za-z0-9_.-]+$/.test(trimmed)) return `https://www.youtube.com/@${trimmed}`;
  try {
    const url = new URL(trimmed.startsWith("http") ? trimmed : `https://${trimmed}`);
    if (!url.hostname.endsWith("youtube.com")) return null;
    return url.toString();
  } catch {
    return null;
  }
}

export async function addYoutubeChannel(
  input: string,
): Promise<{ ok: boolean; message: string; channel?: { channelId: string; channelName: string } }> {
  await ensureYoutubePickChannelsTable();

  const channelUrl = normalizeChannelUrl(input);
  if (!channelUrl) {
    return { ok: false, message: "Enter a channel @handle or a youtube.com channel URL." };
  }

  let html: string;
  try {
    const resp = await fetch(channelUrl, { headers: { "User-Agent": USER_AGENT } });
    if (!resp.ok) {
      return { ok: false, message: `Could not load that channel (HTTP ${resp.status}).` };
    }
    html = await resp.text();
  } catch (err) {
    return { ok: false, message: `Failed to fetch channel page: ${(err as Error).message}` };
  }

  const idMatch = html.match(/"externalId":"([A-Za-z0-9_-]+)"/);
  if (!idMatch) {
    return { ok: false, message: "Could not find a channel ID on that page — check the handle/URL." };
  }
  const channelId = idMatch[1];

  const nameMatch = html.match(/<meta property="og:title" content="([^"]*)"/) ||
                     html.match(/"channelMetadataRenderer":\{"title":"([^"]*)"/);
  const channelName = nameMatch ? nameMatch[1] : channelId;

  const handleMatch = channelUrl.match(/youtube\.com\/(@[\w.-]+)/);

  await db
    .insert(youtubePickChannels)
    .values({ channelId, channelName, handle: handleMatch?.[1] ?? null })
    .onConflictDoUpdate({
      target: youtubePickChannels.channelId,
      set: { channelName, handle: handleMatch?.[1] ?? null },
    });

  return { ok: true, message: `Added ${channelName}.`, channel: { channelId, channelName } };
}

export async function getTrackedYoutubeChannels() {
  await ensureYoutubePickChannelsTable();
  return db.select().from(youtubePickChannels).orderBy(desc(youtubePickChannels.addedAt));
}
