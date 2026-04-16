"use client";

export default function AnalyticsError({
  error,
  reset,
}: {
  error: Error & { digest?: string };
  reset: () => void;
}) {
  return (
    <div className="space-y-4 p-6 max-w-5xl mx-auto">
      <h1 className="text-xl font-bold">Model Calibration Analytics</h1>
      <div className="rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-800">
        <p className="font-medium mb-1">Failed to load analytics data</p>
        <p className="font-mono text-xs">{error.message}</p>
        {error.digest && (
          <p className="mt-1 text-xs text-red-600">Digest: {error.digest}</p>
        )}
      </div>
      <button
        onClick={reset}
        className="rounded bg-primary px-3 py-1.5 text-sm text-primary-foreground hover:bg-primary/90"
      >
        Retry
      </button>
    </div>
  );
}
