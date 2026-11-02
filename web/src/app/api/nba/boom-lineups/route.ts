import { createHash } from "node:crypto";
import { sql } from "drizzle-orm";
import { db } from "@/db";
import { getBoomBoard } from "@/db/nba-boom-board";
import { ensureNbaBoomLineupTables } from "@/db/ensure-schema";
import {getNbaAvailability} from "@/db/nba-availability";
import {availabilityBoard,AVAILABILITY_POLICY} from "@/lib/nba-availability";
import { buildBoomLineups } from "@/lib/nba-boom-lineups";
export async function POST(request:Request) {
  const text=await request.text();
  if(text.length>2000) return Response.json({error:"Request too large"},{status:400});
  try {
    const body=JSON.parse(text);
    if(!body || typeof body!=="object" || Array.isArray(body)) throw new Error("Invalid controls");
    const [originalBoard,evidence]=await Promise.all([getBoomBoard(String(body.date)),getNbaAvailability(String(body.date))]);
    const board=availabilityBoard(originalBoard,evidence);
    if((body.evidenceSha??null)!==(evidence?.sha??null))return Response.json({error:"Availability evidence changed. Reload to review the latest report before saving."},{status:409});
    if(!board || body.snapshot!==board.snapshot) return Response.json({error:"Frozen snapshot missing or changed. Reload the page."},{status:409});
    if(body.selected!==null && typeof body.selected!=="string") throw new Error("Invalid player");
    const result=buildBoomLineups(board,body.selected,body.requested,body.maxDrop);
    if(!result.feasible) return Response.json({error:result.reason},{status:400});
    const settings={selected:body.selected,requested:body.requested,maxDrop:body.maxDrop};
    const id=createHash("sha256").update(JSON.stringify({policy:AVAILABILITY_POLICY,snapshot:board.snapshot,evidence:evidence?.sha??null,settings})).digest("hex");
    const payload=JSON.stringify({version:AVAILABILITY_POLICY,originalBoard,board,availabilityEvidence:evidence,settings,...result});
    await ensureNbaBoomLineupTables();
    // Database clock enforces cutoff even if the request began before lock.
    const saved=await db.execute(sql`INSERT INTO nba_boom_lineup_records(id,game_date,snapshot_sha256,policy,lock_at,payload)
      SELECT ${id},${board.date}::date,${board.snapshot},${AVAILABILITY_POLICY},${board.lockAt}::timestamptz,${payload}::jsonb
      WHERE clock_timestamp()<${board.lockAt}::timestamptz
      AND COALESCE((SELECT report_sha256 FROM nba_injury_observations WHERE observed_at<=clock_timestamp() AND published_at<=observed_at
        AND EXISTS(SELECT 1 FROM jsonb_array_elements(report->'team_submissions') t WHERE t->>'game_date'=${board.date})
        ORDER BY published_at DESC,observed_at DESC LIMIT 1),'')=${evidence?.sha??""}
      ON CONFLICT(id) DO NOTHING RETURNING id,created_at`);
    let row=saved.rows[0];
    if(!row) row=(await db.execute(sql`SELECT id,created_at FROM nba_boom_lineup_records WHERE id=${id}`)).rows[0];
    if(!row) return Response.json({error:"Comparison not recorded: cutoff passed or availability changed. Reload to review current evidence."},{status:409});
    return Response.json({id:row.id,createdAt:row.created_at});
  } catch(e) {
    if(e instanceof SyntaxError || (e instanceof Error && /Invalid|no frozen boom/i.test(e.message))) return Response.json({error:"Invalid lineup controls"},{status:400});
    console.error("Boom comparison save failed",e instanceof Error?e.message:"unknown");
    return Response.json({error:"Comparison could not be saved. Download a copy; it is not a confirmed pregame recording."},{status:503});
  }
}
