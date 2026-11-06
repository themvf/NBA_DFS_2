import { getTdComparison } from "@/db/nfl-td-comparison";
import { scores } from "@/lib/nfl/td-comparison";
export const dynamic = "force-dynamic";
export async function GET(request: Request) {
  try {
    const data = await getTdComparison(Number(new URL(request.url).searchParams.get("season")));
    return Response.json({ ...data, scores: scores(data.rows), readAt: new Date().toISOString() }, { headers: { "Cache-Control": "no-store", "Content-Disposition": `attachment; filename="nfl-td-comparison-${data.season}.json"` } });
  } catch (error) { console.error("TD evidence download unavailable", error); return Response.json({ error: "Comparison data unavailable" }, { status: 503 }); }
}
