"use client";

/**
 * The DFS workspace's error boundary.
 *
 * Without one, a failure anywhere in this route hit Next's default handler and
 * rendered "An error occurred in the Server Components render. The specific
 * message is omitted in production builds to avoid leaking sensitive details."
 * -- a full-page card with no message, no digest in view, and no way back
 * except a manual reload.
 *
 * That is what a real slate failure looked like on 2026-09-19, and it is why
 * diagnosing it needed a local dev build to see the message at all. The digest
 * IS the key to the server log, so it is shown here rather than buried.
 */

import { useEffect } from "react";
import { AlertTriangle, RotateCw } from "lucide-react";

export default function NflDfsError({ error, reset }: { error: Error & { digest?: string }; reset: () => void }) {
  useEffect(() => { console.error("NFL DFS workspace error", error); }, [error]);

  return (
    <div className="mx-auto max-w-3xl p-6">
      <section className="rounded-2xl border border-rose-300 bg-rose-50 p-6">
        <h1 className="flex items-center gap-2 text-lg font-black text-rose-950">
          <AlertTriangle className="h-5 w-5" aria-hidden />
          The DFS workspace could not load
        </h1>

        <p className="mt-3 rounded-lg border border-rose-200 bg-white p-3 font-mono text-sm text-rose-900">
          {error.message || "No message was attached to this error."}
        </p>

        {error.digest ? (
          <p className="mt-2 text-xs text-rose-800">
            Server log reference: <span className="font-mono font-bold">{error.digest}</span>. Production hides the
            message itself, so quote this digest when checking the deployment logs.
          </p>
        ) : null}

        <p className="mt-3 text-sm text-rose-900">
          Your saved slates and lineups are untouched — this failed while reading them, not while writing.
        </p>

        <button
          onClick={reset}
          className="mt-4 inline-flex min-h-11 items-center gap-2 rounded-lg bg-rose-700 px-4 text-sm font-bold text-white hover:bg-rose-600"
        >
          <RotateCw className="h-4 w-4" aria-hidden />
          Try again
        </button>
      </section>
    </div>
  );
}
