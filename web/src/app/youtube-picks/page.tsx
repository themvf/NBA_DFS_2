export const dynamic = "force-dynamic";

import type { Metadata } from "next";
import { getTrackedYoutubeChannels } from "./actions";
import { getChannelSportRecords, getRecentYoutubePicks } from "./queries";
import { YoutubePicksClient } from "./youtube-picks-client";

export const metadata: Metadata = {
  title: "YouTube Picks Tracker",
  description: "Extracted betting picks from tracked YouTube channels.",
};

export default async function YoutubePicksPage() {
  const [picks, channels, records] = await Promise.all([
    getRecentYoutubePicks(200),
    getTrackedYoutubeChannels(),
    getChannelSportRecords(),
  ]);
  return <YoutubePicksClient picks={picks} initialChannels={channels} records={records} />;
}
