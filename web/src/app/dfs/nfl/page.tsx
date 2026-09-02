import type { Metadata } from "next";
import NflDfsClient from "./nfl-dfs-client";

export const metadata: Metadata = {
  title: "NFL DFS Workspace",
  description: "DraftKings NFL Classic and Showdown slate intake and model-readiness workspace.",
};

export default function NflDfsPage() {
  return <NflDfsClient />;
}
