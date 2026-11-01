import type { Metadata } from "next";
import Link from "next/link";
import CbbPropsBoard from "./props-board";

export const metadata: Metadata = { title: "CBB Player Props", description: "College basketball player-specific points, rebounds, and assists lines and sportsbook odds." };
export const maxDuration = 60;
export default function CbbPlayerPropsPage() {
  return <main className="mx-auto max-w-[1500px] space-y-5 p-4 md:p-6"><header><Link href="/cbb" className="text-sm text-muted-foreground hover:underline">College Basketball</Link><h1 className="mt-1 text-3xl font-bold">CBB Player Props</h1><p className="mt-2 text-sm text-muted-foreground">See the actual line and price for each named player, stat, and sportsbook.</p></header><CbbPropsBoard /></main>;
}
