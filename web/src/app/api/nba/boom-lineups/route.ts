import { createHash } from "node:crypto";
import { sql } from "drizzle-orm";
import { db } from "@/db";
import { getBoomBoard } from "@/db/nba-boom-board";
import { ensureNbaBoomLineupTables } from "@/db/ensure-schema";
import { BOOM_POLICY, buildBoomLineups } from "@/lib/nba-boom-lineups";
export async function POST(request:Request) {
  const text=await request.text();
  if(text.length>2000) return Response.json({error:"Request too large"},{status:400});
  try {
    const body=JSON.parse(text);
    const board=await getBoomBoard(String(body.date));
    if(!board || body.snapshot!==board.snapshot) return Response.json({error:"Frozen snapshot missing or changed. Reload the page."},{status:409});
    if(body.selected!==null && typeof body.selected!=="string") throw new Error("Invalid player");
    const result=buildBoomLineups(board,body.selected,body.requested,body.maxDrop);
    if(!result.feasible) return Response.json({error:result.reason},{status:400});
    const settings={selected:body.selected,requested:body.requested,maxDrop:body.maxDrop};
    const id=createHash("sha256").update(JSON.stringify({policy:BOOM_POLICY,snapshot:board.snapshot,settings})).digest("hex");
    const payload=JSON.stringify({version:BOOM_POLICY,board,settings,...result});
    await ensureNbaBoomLineupTables();
    // Database clock enforces cutoff even if the request began before lock.
    const saved=await db.execute(sql`INSERT INTO nba_boom_lineup_records(id,game_date,snapshot_sha256,policy,lock_at,payload)
      SELECT ${id},${board.date}::date,${board.snapshot},${BOOM_POLICY},${board.lockAt}::timestamptz,${payload}::jsonb
      WHERE clock_timestamp()<${board.lockAt}::timestamptz ON CONFLICT(id) DO NOTHING RETURNING id,created_at`);
    let row=saved.rows[0];
    if(!row) row=(await db.execute(sql`SELECT id,created_at FROM nba_boom_lineup_records WHERE id=${id}`)).rows[0];
    if(!row) return Response.json({error:"First-game cutoff has passed. This comparison was not recorded."},{status:409});
    return Response.json({id:row.id,createdAt:row.created_at});
  } catch(e) {
    if(e instanceof SyntaxError || (e instanceof Error && /Invalid|no frozen boom/i.test(e.message))) return Response.json({error:"Invalid lineup controls"},{status:400});
    console.error("Boom comparison save failed",e instanceof Error?e.message:"unknown");
    return Response.json({error:"Comparison could not be saved. Download a copy; it is not a confirmed pregame recording."},{status:503});
  }
}
