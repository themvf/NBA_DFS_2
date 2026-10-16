import { timingSafeEqual } from "node:crypto";

/** The ghost entry points run only for a caller holding CRON_SECRET. */
export function assertGhostSecret(secret: string | null | undefined): void {
  const expected = process.env.CRON_SECRET;
  if (!expected) throw new Error("CRON_SECRET is not configured");
  const a = Buffer.from(String(secret ?? "")), b = Buffer.from(expected);
  if (a.length !== b.length || !timingSafeEqual(a, b)) throw new Error("Unauthorized");
}
