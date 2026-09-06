import Link from "next/link";
import MlbDiagnosticsContent from "../mlb-diagnostics-content";
import { normalizeMlbDate } from "@/lib/mlb-terminal";
export const dynamic = "force-dynamic";
export default async function Page({ searchParams }: { searchParams: Promise<{ date?: string }> }) {
  const date = normalizeMlbDate((await searchParams).date);
  return <><Link className="block p-4 text-sm underline" href={`/vegas?sport=mlb&date=${date}`}>Back to MLB terminal</Link><MlbDiagnosticsContent date={date} /></>;
}
