import { NextRequest, NextResponse } from 'next/server';
import { withHeartbeat } from "@/lib/cron-heartbeat";
import { captureDuePools } from '@/lib/nfl-dfs/pool-capture-service';
export const dynamic='force-dynamic';
export const maxDuration=300;
async function handle(request: NextRequest) {
  if(!process.env.CRON_SECRET)return NextResponse.json({error:'Cron authentication is not configured'},{status:500});
  if(request.headers.get('authorization')!==`Bearer ${process.env.CRON_SECRET}`)return NextResponse.json({error:'Unauthorized'},{status:401});
  const result=await captureDuePools();
  return NextResponse.json(result,{status:result.errors.length?500:200});
}

// Every run records its outcome for /health (lib/cron-heartbeat): Vercel's own
// logs are not visible from here, so a failing or stopped cron would be silent.
export async function GET(request: NextRequest) {
  return withHeartbeat("nfl-pool-capture", () => handle(request));
}
