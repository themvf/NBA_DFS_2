export const dynamic = "force-dynamic";

import CheatSheetPrintPage from "@/components/fantasy-football/cheat-sheet-print-page";

export default async function BestBallCheatSheetPage({
  searchParams,
}: {
  searchParams: Promise<{ scoring?: string }>;
}) {
  const { scoring } = await searchParams;
  return <CheatSheetPrintPage
    variant="bestball"
    scoring={scoring}
    printHref="/fantasy-football/best-ball/print"
    backHref="/fantasy-football/best-ball"
    backLabel="Back to Best Ball"
  />;
}
