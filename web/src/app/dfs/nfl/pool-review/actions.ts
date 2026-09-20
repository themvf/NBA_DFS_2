'use server';
import { capturePool } from '@/lib/nfl-dfs/pool-capture-service';
import { revalidatePath } from 'next/cache';
export async function captureCurrentPool(uploadId:string) {
  const result=await capturePool(uploadId);
  revalidatePath('/dfs/nfl/pool-review');
  return result;
}
