import type { Metadata } from "next";
import { notFound } from "next/navigation";
import { getLatestPatternWatchDataset, type PatternWatchDataset } from "@/db/cfb-pattern-watch";
import { patternWatchKeyValid } from "@/lib/cfb-pattern-watch-access";
import PatternWatchClient from "./pattern-watch-client";

export const dynamic = "force-dynamic";

export const metadata: Metadata = {
  title: "CFB Pattern Watch",
  robots: { index: false, follow: false },
};

export default async function PatternWatchPage({
  searchParams,
}: {
  searchParams: Promise<{ key?: string }>;
}) {
  const { key } = await searchParams;
  // Private until shared: without the key the page does not exist.
  if (!patternWatchKeyValid(key)) notFound();

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
