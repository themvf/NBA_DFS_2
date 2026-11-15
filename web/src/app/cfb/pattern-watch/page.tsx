import type { Metadata } from "next";


import { getLatestPatternWatchDataset, type PatternWatchDataset } from "@/db/cfb-pattern-watch";

import PatternWatchClient from "./pattern-watch-client";

export const dynamic = "force-dynamic";

export const metadata: Metadata = {
  title: "CFB Pattern Watch",
  robots: { index: false, follow: false },
};

export default async function PatternWatchPage() {
  let dataset: PatternWatchDataset | null = null;
  let error: string | null = null;
  try {
    dataset = await getLatestPatternWatchDataset();
  } catch (e) {
    console.error("CFB pattern watch dataset unavailable", e);
    error = "The pattern watch data could not be read from the database.";
  }
  return <PatternWatchClient dataset={dataset} error={error} />;
}
