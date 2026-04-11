export default function AnalyticsLoading() {
  return (
    <div className="space-y-8 p-6 max-w-5xl mx-auto">
      <div>
        <h1 className="text-xl font-bold">Model Calibration Analytics</h1>
        <p className="text-sm text-gray-500 mt-1">Loading analytics…</p>
      </div>
      <div className="rounded-lg border bg-card p-6 text-sm text-gray-400">
        Loading accuracy trends, position breakdowns, and leverage calibration…
      </div>
    </div>
  );
}
