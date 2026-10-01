const MAX_QUOTE_AGE_MS = 5 * 60_000;

export function isCfbQuoteFresh(
  updatedAt: string | null,
  capturedAt: string | null,
  commenceTime: string | null,
  nowMs: number,
): boolean {
  if (!updatedAt || !capturedAt || !commenceTime || !Number.isFinite(nowMs)) return false;
  const updatedMs = Date.parse(updatedAt);
  const capturedMs = Date.parse(capturedAt);
  const kickoffMs = Date.parse(commenceTime);
  if (![updatedMs, capturedMs, kickoffMs].every(Number.isFinite)) return false;
  return nowMs < kickoffMs
    && updatedMs <= nowMs && capturedMs <= nowMs
    && nowMs - updatedMs <= MAX_QUOTE_AGE_MS
    && nowMs - capturedMs <= MAX_QUOTE_AGE_MS;
}
