import { NextRequest, NextResponse } from 'next/server';
import { captureDuePools } from '@/lib/nfl-dfs/pool-capture-service';
export const dynamic='force-dynamic';
export const maxDuration=300;
export async function GET(request:NextRequest) {
  if(!process.env.CRON_SECRET)return NextResponse.json({error:'Cron authentication is not configured'},{status:500});
  if(request.headers.get('authorization')!==`Bearer ${process.env.CRON_SECRET}`)return NextResponse.json({error:'Unauthorized'},{status:401});
  const result=await captureDuePools();
  return NextResponse.json(result,{status:result.errors.length?500:200});
}
