import "server-only";

import { db } from "@/db";
import { youtubePicks, youtubePickVideos } from "@/db/schema";
import { and, desc, eq, inArray, ne, sql } from "drizzle-orm";

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
  resultDetail: string | null;
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
      resultDetail: youtubePicks.resultDetail,
    })
    .from(youtubePicks)
    .innerJoin(youtubePickVideos, eq(youtubePickVideos.id, youtubePicks.videoId))
    .where(and(ne(youtubePicks.sport, "_none"), ne(youtubePicks.status, "_none")))
    .orderBy(desc(youtubePickVideos.publishedAt), desc(youtubePicks.id))
    .limit(limit);

  return rows;
}

export interface ChannelSportRecord {
  channelName: string;
  sport: string;
  wins: number;
  losses: number;
  pushes: number;
}

/** Settled win-loss-push record per (channel, sport) -- powers the
 * channel-vs-sport scoreboard. Only counts graded outcomes (won/lost/push).
 * Deduped to one row per UNIQUE POSITION (channel + resolved game +
 * selection) so a capper repeating the same pick across multiple videos
 * counts once, while genuinely different bets on different games (distinct
 * matchup_ref) stay separate. */
export async function getChannelSportRecords(): Promise<ChannelSportRecord[]> {
  const deduped = db
    .selectDistinct({
      channelName: youtubePickVideos.channelName,
      sport: youtubePicks.sport,
      matchupRef: youtubePicks.matchupRef,
      selection: youtubePicks.selection,
      status: youtubePicks.status,
    })
    .from(youtubePicks)
    .innerJoin(youtubePickVideos, eq(youtubePickVideos.id, youtubePicks.videoId))
    .where(and(inArray(youtubePicks.status, ["won", "lost", "push"]), ne(youtubePicks.sport, "_none")))
    .as("deduped");

  const rows = await db
    .select({
      channelName: deduped.channelName,
      sport: deduped.sport,
      wins: sql<number>`count(*) filter (where ${deduped.status} = 'won')`.mapWith(Number),
      losses: sql<number>`count(*) filter (where ${deduped.status} = 'lost')`.mapWith(Number),
      pushes: sql<number>`count(*) filter (where ${deduped.status} = 'push')`.mapWith(Number),
    })
    .from(deduped)
    .groupBy(deduped.channelName, deduped.sport);

  return rows;
}
