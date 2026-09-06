import Report from './report';
export const dynamic = 'force-dynamic';
export const metadata = { title: 'NFL DFS - Pass Volume and Target Shares' };
export default async function Page() {
  // This dynamic server page captures one request timestamp for consistent client hydration.
  // eslint-disable-next-line react-hooks/purity
  const viewedAt = Date.now();
  return <Report viewedAt={viewedAt} />;
}
