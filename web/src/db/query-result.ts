type QueryResultWithRows = {
  rows?: unknown;
};

/** Normalize Drizzle/Neon query results without assuming a driver-specific shape. */
export function queryRows<T>(result: unknown): T[] {
  if (Array.isArray(result)) return result as T[];
  if (result && typeof result === "object") {
    const nestedRows = (result as QueryResultWithRows).rows;
    if (Array.isArray(nestedRows)) return nestedRows as T[];
  }
  return [];
}
