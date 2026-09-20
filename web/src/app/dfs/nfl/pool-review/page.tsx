import Link from 'next/link';
import { poolAuditIndex, readPoolReview } from '@/db/nfl-dfs-pool-audit';
import PoolReview from './pool-review';
export const dynamic='force-dynamic';
export const maxDuration=300;
export const metadata={title:'NFL DFS · Full Pool Audit'};
export default async function Page({searchParams}:{searchParams:Promise<{upload?:string;view?:string}>}) {
  const {upload,view}=await searchParams;
  const latest=view==='latest';
  try {
    const index=await poolAuditIndex();
    const selected=index.uploads.find(u=>u.id===upload)??index.uploads[0];
    const review=selected?await readPoolReview(selected.id,latest):null;
    return <PoolReview key={`${selected?.id}:${latest}`} index={index} selected={selected??null} review={review} latest={latest}/>;
  } catch(error) {
    console.error('NFL pool audit unavailable',error);
    return <main className="space-y-4 p-8"><Link href="/dfs/nfl">← NFL DFS</Link><h1 className="text-2xl font-bold">Full Pool Audit</h1>
      <p role="alert">Saved audit evidence could not be loaded. This is not an empty pool or a zero-result report. Retry after checking the capture job.</p></main>;
  }
}
