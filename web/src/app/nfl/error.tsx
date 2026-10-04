"use client";

// Error boundary for the NFL pages (pick'em, survivor, specials, vegas, pbp).
// A page-level read that throws lands here with its message, instead of
// Next's default "a server-side exception has occurred" or, worse, the
// swallowed-into-[] behaviour the pick'em pools/ledger queries used to have.
export default function NflError({
  error,
  reset,
}: {
  error: Error & { digest?: string };
  reset: () => void;
}) {
  return (
    <div className="mx-auto mt-8 max-w-4xl rounded-lg border border-red-200 bg-red-50 p-6">
      <h2 className="mb-2 text-sm font-semibold text-red-800">Could not load this NFL page</h2>
      <p className="mb-4 text-xs text-red-700">
        {error.message || "A server-side error occurred while reading the NFL data."}
        {error.digest && (
          <span className="ml-2 font-mono text-red-500">Digest: {error.digest}</span>
        )}
      </p>
      <p className="mb-4 text-xs text-red-700">
        Nothing below this line is missing because it is empty; it is missing because the read failed.
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
