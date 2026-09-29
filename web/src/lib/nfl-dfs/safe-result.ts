/**
 * A server action's outcome as data.
 *
 * In production Next.js replaces a thrown server-action message with a
 * generic "An error occurred in the Server Components render" digest, so
 * every reason the NFL DFS actions give ("Refresh the slate", "This lineup set
 * belongs to a different slate", ...) reached the page as that one banner
 * (PHI@CHI 2026-09-28). Returning the reason keeps it readable; `unwrap`
 * turns it back into a thrown Error on the client, where existing
 * try/catch blocks show `error.message`.
 */
export type SafeResult<T> = { ok: true; value: T } | { ok: false; error: string };

export function unwrap<T>(result: SafeResult<T>): T {
  if (!result.ok) throw new Error(result.error);
  return result.value;
}
