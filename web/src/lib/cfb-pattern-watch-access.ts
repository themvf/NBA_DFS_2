import { createHash, createHmac, timingSafeEqual } from "node:crypto";

/**
 * The /cfb/pattern-watch page is private until the owner chooses to share it.
 * Access requires the configured key or a signed session issued after key
 * verification. Both checks fail closed when no valid key is configured.
 *
 * Setting or changing the variable takes effect only after a NEW build. A
 * Vercel "Redeploy" of the same commit is cancelled by vercel.json's
 * ignoreCommand (nothing under web/ changed), so it never picks the key up.
 */
export const MIN_KEY_LENGTH = 16;
export const CFB_PATTERN_SESSION = "cfb-pattern-session";
const SESSION_DURATION_MS = 8 * 60 * 60 * 1000;

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

function sessionSignature(key: string, expiry: number) {
  return createHmac("sha256", key).update("cfb-pattern-watch-session:" + expiry).digest("hex");
}

export function createPatternWatchSession(key: string, now: number, expected = process.env.CFB_PATTERN_PAGE_KEY) {
  if (!patternWatchKeyValid(key, expected)) return null;
  const expiresAt = now + SESSION_DURATION_MS;
  return { expiresAt, value: expiresAt + "." + sessionSignature(key, expiresAt) };
}

export function patternWatchSessionValid(
  token: string | undefined,
  now = Date.now(),
  expected = process.env.CFB_PATTERN_PAGE_KEY,
): boolean {
  if (!expected || expected.length < MIN_KEY_LENGTH || !token || !/^\d{13}\.[a-f0-9]{64}$/.test(token)) return false;
  const [stamp, signature] = token.split(".");
  const expiry = Number(stamp);
  if (expiry <= now || expiry > now + SESSION_DURATION_MS) return false;
  return timingSafeEqual(Buffer.from(signature, "hex"), Buffer.from(sessionSignature(expected, expiry), "hex"));
}
