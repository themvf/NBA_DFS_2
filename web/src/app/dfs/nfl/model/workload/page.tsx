import Report, { type VolumeShareReportView } from './report';
import { getLatestVolumeShareReport } from '@/db/nfl-volume-share';
export const dynamic = 'force-dynamic';
export const metadata = { title: 'NFL DFS - Pass Volume and Target Shares' };
export default async function Page() {
  // This dynamic server page captures one request timestamp for consistent client hydration.
  // eslint-disable-next-line react-hooks/purity
  const viewedAt = Date.now();
  // The newest stored run (research workflow); a missing table or run renders an explained empty state.
  const latest = await getLatestVolumeShareReport().catch(() => null);
  return <Report viewedAt={viewedAt} report={(latest?.report as unknown as VolumeShareReportView | undefined) ?? null} />;
}
