import type { Metadata } from "next";
import CfbDfsClient from "./cfb-dfs-client";

export const dynamic = "force-dynamic";

export const metadata: Metadata = {
  title: "CFB DFS",
  description: "DraftKings College Football Classic: projections from box scores and a lineup builder.",
};

/** College football DFS. Separate from the NFL workspace by design: its own rules, data and tables. */
export default async function CfbDfsPage({ searchParams }: { searchParams: Promise<{ upload?: string }> }) {
  const { upload } = await searchParams;
  return <main className="mx-auto max-w-[1500px] space-y-4 p-4">
    <CfbDfsClient initialUploadId={upload ?? null} />
  </main>;
}
