import type { Metadata } from "next";
import CbbDfsClient from "./cbb-dfs-client";

export const metadata: Metadata = { title: "CBB DFS", description: "DraftKings men's college basketball Classic and Showdown: player projections, ownership inputs, and lineup builder." };
export const maxDuration = 60;
export default function CbbDfsPage() {
  return <main className="mx-auto max-w-[1500px] space-y-6 p-4 md:p-6"><CbbDfsClient /></main>;
}
