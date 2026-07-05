export const dynamic = "force-dynamic";

import type { Metadata } from "next";
import { getTrackedYoutubeChannels } from "./actions";
import { getRecentYoutubePicks } from "./queries";
import { YoutubePicksClient } from "./youtube-picks-client";

export const metadata: Metadata = {
  title: "YouTube Picks Tracker",
  description: "Extracted betting picks from tracked YouTube channels.",
};

export default async function YoutubePicksPage() {
  const [picks, channels] = await Promise.all([
    getRecentYoutubePicks(200),
    getTrackedYoutubeChannels(),
  ]);
  return <YoutubePicksClient picks={picks} initialChannels={channels} />;
}
