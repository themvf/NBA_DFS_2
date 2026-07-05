export const dynamic = "force-dynamic";

import type { Metadata } from "next";
import { getRecentVideoAnalyses } from "./actions";
import { VideoAnalysisClient } from "./video-analysis-client";

export const metadata: Metadata = {
  title: "Video Analysis",
  description: "Paste a YouTube video URL to get a per-team/per-player analysis breakdown.",
};

export default async function VideoAnalysisPage() {
  const recent = await getRecentVideoAnalyses(20);
  return <VideoAnalysisClient initialRecent={recent} />;
}
