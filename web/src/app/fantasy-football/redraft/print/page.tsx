export const dynamic = "force-dynamic";

import CheatSheetPrintPage from "@/components/fantasy-football/cheat-sheet-print-page";

export default async function RedraftCheatSheetPage({
  searchParams,
}: {
  searchParams: Promise<{ scoring?: string }>;
}) {
  const { scoring } = await searchParams;
  return <CheatSheetPrintPage
    variant="redraft"
    scoring={scoring}
    printHref="/fantasy-football/redraft/print"
    backHref="/fantasy-football/redraft"
    backLabel="Back to Redraft"
  />;
}
