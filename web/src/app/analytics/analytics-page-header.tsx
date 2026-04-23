export default function AnalyticsPageHeader({
  title,
  description,
}: {
  title: string;
  description: string;
}) {
  return (
    <div className="mx-auto mb-6 max-w-5xl">
      <h1 className="text-xl font-bold">{title}</h1>
      <p className="mt-1 text-sm text-slate-700">{description}</p>
    </div>
  );
}
