import { StudyPerformancePanel } from "@/app/sports/market-studies/study-performance-panel";
import { getCfbStudySelections } from "@/db/cfb-study-matches";
import { cfbObservations } from "@/lib/daily-study-matches";
import { studyDate } from "@/lib/daily-market-studies";
import { easternDateString } from "@/lib/eastern-date";
import type { Metadata } from "next";


import { getLatestPatternWatchDataset, type PatternWatchDataset } from "@/db/cfb-pattern-watch";

import PatternWatchClient from "./pattern-watch-client";

export const dynamic = "force-dynamic";

export const metadata: Metadata = {
  title: "CFB Pattern Watch",
  robots: { index: false, follow: false },
};

export default async function PatternWatchPage({ searchParams }: { searchParams: Promise<{ date?: string; cohort?: string }> }) {
  const params = await searchParams;
  const date = studyDate(params.date, easternDateString());
  const entriesPromise = getCfbStudySelections(date).then(cfbObservations).catch(() => null);
  let dataset: PatternWatchDataset | null = null;
  let error: string | null = null;
  try {
    dataset = await getLatestPatternWatchDataset();
  } catch (e) {
    console.error("CFB pattern watch dataset unavailable", e);
    error = "The pattern watch data could not be read from the database.";
  }
  const entries = await entriesPromise;
  const performance = entries ? <StudyPerformancePanel entries={entries} date={date} initial={params.cohort} />
    : <p role="status" id="study-performance">Frozen CFB entries are temporarily unavailable.</p>;
  return <PatternWatchClient dataset={dataset} error={error} performance={performance} />;
}
