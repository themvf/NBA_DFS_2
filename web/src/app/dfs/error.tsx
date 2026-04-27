"use client";

export default function DfsError({
  error,
  reset,
}: {
  error: Error & { digest?: string };
  reset: () => void;
}) {
  return (
    <div className="rounded-lg border border-red-200 bg-red-50 p-6">
      <h2 className="mb-2 text-sm font-semibold text-red-800">Page Load Error</h2>
      <p className="mb-4 text-xs text-red-700">
        {error.message || "A server-side error occurred while loading the DFS page."}
        {error.digest && (
          <span className="ml-2 font-mono text-red-500">Digest: {error.digest}</span>
        )}
      </p>
      <button
        onClick={reset}
        className="rounded bg-red-600 px-3 py-1.5 text-xs font-medium text-white hover:bg-red-700"
      >
        Try again
      </button>
    </div>
  );
}
