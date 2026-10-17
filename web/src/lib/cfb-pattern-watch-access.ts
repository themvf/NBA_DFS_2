import { createHash, timingSafeEqual } from "node:crypto";

/**
 * The /cfb/pattern-watch page is private until the owner chooses to share it.
 * The site has no login, so the page opens only with ?key= matching the
 * CFB_PATTERN_PAGE_KEY environment variable. It fails closed: no variable, a
 * variable shorter than MIN_KEY_LENGTH, or a wrong key all answer "not found".
 */
export const MIN_KEY_LENGTH = 16;

function digest(value: string): Buffer {
  return createHash("sha256").update(value, "utf8").digest();
}

export function patternWatchKeyValid(
  supplied: string | null | undefined,
  expected: string | undefined = process.env.CFB_PATTERN_PAGE_KEY,
): boolean {
  if (!expected || expected.length < MIN_KEY_LENGTH) return false;
  if (!supplied) return false;
  // Compare fixed-length digests so the check takes the same time for any input.
  return timingSafeEqual(digest(supplied), digest(expected));
}
