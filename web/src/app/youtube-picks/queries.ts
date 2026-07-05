import "server-only";

import { db } from "@/db";
import { youtubePicks, youtubePickVideos } from "@/db/schema";
import { and, desc, eq, ne } from "drizzle-orm";

export interface YoutubePickRow {
  id: number;
  videoDbId: number;
  youtubeVideoId: string;
  videoTitle: string;
  channelName: string;
  publishedAt: Date | null;
  sport: string;
  betType: string;
  subject: string;
  opponent: string | null;
  selection: string;
  oddsAmerican: number | null;
  gameContext: string | null;
  confidenceLabel: string | null;
  quote: string;
  status: string;
}

/** Recent extracted picks, newest video first. Excludes the internal
 * '_none' sentinel rows used to mark a video as processed-with-no-picks. */
export async function getRecentYoutubePicks(limit = 100): Promise<YoutubePickRow[]> {
  const rows = await db
    .select({
      id: youtubePicks.id,
      videoDbId: youtubePickVideos.id,
      youtubeVideoId: youtubePickVideos.videoId,
      videoTitle: youtubePickVideos.title,
      channelName: youtubePickVideos.channelName,
      publishedAt: youtubePickVideos.publishedAt,
      sport: youtubePicks.sport,
      betType: youtubePicks.betType,
      subject: youtubePicks.subject,
      opponent: youtubePicks.opponent,
      selection: youtubePicks.selection,
      oddsAmerican: youtubePicks.oddsAmerican,
      gameContext: youtubePicks.gameContext,
      confidenceLabel: youtubePicks.confidenceLabel,
      quote: youtubePicks.quote,
      status: youtubePicks.status,
    })
    .from(youtubePicks)
    .innerJoin(youtubePickVideos, eq(youtubePickVideos.id, youtubePicks.videoId))
    .where(and(ne(youtubePicks.sport, "_none"), ne(youtubePicks.status, "_none")))
    .orderBy(desc(youtubePickVideos.publishedAt), desc(youtubePicks.id))
    .limit(limit);

  return rows;
}
