import { createHash } from "node:crypto";
import { getContestRunGames, saveContestRun } from "@/db/nba-contest-runs";
import { RUN_LIMITS, checkRunRequest, runPayload } from "@/lib/nba-contest-runs";

/**
 * Saves one run of the contest builder. The browser sends the settings and the entries
 * it showed; the games' teams and tip times come from the stored lines, and the database
 * clock decides whether the run was saved before the first tip. A refusal always says why.
 */
export async function POST(request: Request) {
  const text = await request.text();
  if (text.length > RUN_LIMITS.maxBytes) return Response.json({ error: "Run too large to save." }, { status: 400 });
  let body: unknown;
  try {
    body = JSON.parse(text);
  } catch {
    return Response.json({ error: "Run could not be read." }, { status: 400 });
  }
  const checked = checkRunRequest(body);
  if (!checked.ok) return Response.json({ error: `Run not saved: ${checked.error}.` }, { status: 400 });
  try {
    const games = await getContestRunGames(checked.value.date, checked.value.eventIds);
    if (games.length !== checked.value.eventIds.length) {
      return Response.json({ error: "Run not saved: a chosen game has no stored lines for this date. Reload the page." }, { status: 409 });
    }
    const payload = runPayload(checked.value, games);
    const firstTip = games.map((g) => g.commenceTime).sort()[0];
    const sha256 = createHash("sha256").update(JSON.stringify(payload)).digest("hex");
    const saved = await saveContestRun(payload, sha256, firstTip);
    return Response.json({ ...saved, firstTip });
  } catch (error) {
    console.error("Contest run save failed", error instanceof Error ? error.message : "unknown");
    return Response.json({ error: "Run not saved: the database did not answer. The entries on screen are still good; press Optimize again to retry." }, { status: 503 });
  }
}
