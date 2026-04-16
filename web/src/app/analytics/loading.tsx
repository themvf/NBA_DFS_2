export default function AnalyticsLoading() {
  return (
    <div className="mx-auto max-w-5xl space-y-8 p-6 text-slate-900">
      <div>
        <h1 className="text-xl font-bold">Model Calibration Analytics</h1>
        <p className="mt-1 text-sm text-slate-700">Loading analytics...</p>
      </div>
      <div className="rounded-lg border bg-card p-6 text-sm text-slate-700">
        Loading accuracy trends, position breakdowns, and leverage calibration...
      </div>
    </div>
  );
}
